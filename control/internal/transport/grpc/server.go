package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"fluxforge-control/internal/autoscaler"
	"fluxforge-control/internal/catalog"
	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/execution"
	"fluxforge-control/internal/runnerregistry"
	"fluxforge-control/internal/scheduler"
	"fluxforge-control/internal/telemetry"
	"fluxforge-control/internal/workflow"
	pb "fluxforge-control/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type PolicyRepository interface {
	GetPolicy(context.Context, string) (int64, []byte, error)
	SetConfigPolicy(context.Context, string, int64, []byte) (int64, error)
}
type Server struct {
	pb.UnimplementedCatalogServiceServer
	pb.UnimplementedWorkflowServiceServer
	pb.UnimplementedRunnerQueueServiceServer
	pb.UnimplementedQueueAdminServiceServer
	pb.UnimplementedObservabilityServiceServer
	Catalog             *catalog.Service
	Workflow            *workflow.Service
	Execution           *execution.Service
	Runners             *runnerregistry.Service
	Metrics             *telemetry.Aggregator
	Queue               *scheduler.Scheduler
	Policies            PolicyRepository
	Pool                autoscaler.SnapshotProvider
	RuntimeDigest       string
	ApplyScheduling     func([]byte) error
	ApplyAutoscaling    func([]byte) error
	ValidateScheduling  func([]byte) error
	ValidateAutoscaling func([]byte) error
	policyMu            sync.Mutex
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	var d *domain.Error
	if errors.As(err, &d) {
		c := codes.Internal
		switch d.Code {
		case domain.Invalid:
			c = codes.InvalidArgument
		case domain.NotFound:
			c = codes.NotFound
		case domain.Conflict:
			c = codes.AlreadyExists
		case domain.Stale:
			c = codes.FailedPrecondition
		case domain.Exhausted:
			c = codes.ResourceExhausted
		case domain.Unavailable:
			c = codes.Unavailable
		}
		return status.Error(c, d.Message)
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}
func (s *Server) PublishFunction(ctx context.Context, r *pb.PublishFunctionRequest) (*pb.PublishFunctionResponse, error) {
	f, e := s.Catalog.PublishFunction(ctx, function(r.Spec))
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.PublishFunctionResponse{Success: true, Spec: functionPB(f)}, nil
}
func (s *Server) GetFunction(ctx context.Context, r *pb.GetFunctionRequest) (*pb.GetFunctionResponse, error) {
	f, e := s.Catalog.GetFunction(ctx, r.FunctionId, r.Version)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.GetFunctionResponse{Spec: functionPB(f)}, nil
}
func (s *Server) PublishWorkflow(ctx context.Context, r *pb.PublishWorkflowRequest) (*pb.PublishWorkflowResponse, error) {
	if r.Spec == nil {
		return nil, status.Error(codes.InvalidArgument, "typed spec is required")
	}
	w, e := s.Catalog.PublishWorkflow(ctx, workflowSpec(r.Spec))
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.PublishWorkflowResponse{Success: true, Spec: workflowPB(w)}, nil
}
func (s *Server) GetWorkflow(ctx context.Context, r *pb.GetWorkflowRequest) (*pb.GetWorkflowResponse, error) {
	w, e := s.Catalog.GetWorkflow(ctx, r.WorkflowId, r.Version)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.GetWorkflowResponse{WorkflowId: w.ID, Spec: workflowPB(w)}, nil
}
func (s *Server) SubmitRun(ctx context.Context, r *pb.SubmitRunRequest) (*pb.SubmitRunResponse, error) {
	p := payload(r.Input)
	if r.Input == nil && len(r.InputPayload) > 0 {
		p.Inline = r.InputPayload
	}
	c := domain.SubmitCommand{WorkflowID: r.WorkflowId, Version: r.WorkflowVersion, IdempotencyKey: r.IdempotencyKey, Input: p, Priority: r.Priority, SLOMS: r.SloMs}
	if r.DeadlineUnixMs > 0 {
		c.Deadline = time.UnixMilli(r.DeadlineUnixMs)
	}
	traceContext := r.TraceContext
	if traceContext == nil {
		md, _ := metadata.FromIncomingContext(ctx)
		parents := md.Get("traceparent")
		states := md.Get("tracestate")
		if len(parents) == 1 {
			if _, _, ok := telemetry.ParseTraceParent(parents[0]); ok {
				traceContext = &pb.TraceContext{Traceparent: parents[0]}
				if len(states) > 0 {
					traceContext.Tracestate = states[0]
				}
			}
		}
	}
	if traceContext != nil {
		if _, _, ok := telemetry.ParseTraceParent(traceContext.Traceparent); !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid W3C traceparent")
		}
		c.TraceParent = traceContext.Traceparent
		c.TraceState = traceContext.Tracestate
	}
	v, _, e := s.Workflow.Submit(ctx, c)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.SubmitRunResponse{RunId: v.ID, TraceId: telemetry.TraceID(v.TraceParent)}, nil
}
func (s *Server) GetRun(ctx context.Context, r *pb.GetRunRequest) (*pb.GetRunResponse, error) {
	v, e := s.Workflow.GetRun(ctx, r.RunId)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.GetRunResponse{RunId: v.ID, Status: v.State, Run: runPB(v)}, nil
}
func (s *Server) CancelRun(ctx context.Context, r *pb.CancelRunRequest) (*pb.CancelRunResponse, error) {
	v, e := s.Workflow.CancelRun(ctx, r.RunId, r.Reason)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.CancelRunResponse{Success: true, State: v.State}, nil
}
func pageSize(n int32) (int, error) {
	if n == 0 {
		return 100, nil
	}
	if n < 1 || n > 1000 {
		return 0, status.Error(codes.InvalidArgument, "limit must be 1..1000")
	}
	return int(n), nil
}
func (s *Server) ListRuns(ctx context.Context, r *pb.ListRunsRequest) (*pb.ListRunsResponse, error) {
	n, e := pageSize(r.Limit)
	if e != nil {
		return nil, e
	}
	v, e := s.Workflow.ListRuns(ctx, r.WorkflowId, r.State, n+1, r.PageToken)
	if e != nil {
		return nil, rpcError(e)
	}
	p := &pb.ListRunsResponse{}
	if len(v) > n {
		v = v[:n]
		p.NextPageToken = v[n-1].ID
	}
	for _, x := range v {
		p.Runs = append(p.Runs, runPB(x))
	}
	return p, nil
}
func (s *Server) ListInvocations(ctx context.Context, r *pb.ListInvocationsRequest) (*pb.ListInvocationsResponse, error) {
	n, e := pageSize(r.Limit)
	if e != nil {
		return nil, e
	}
	v, e := s.Workflow.ListInvocations(ctx, r.RunId, n+1, r.PageToken)
	if e != nil {
		return nil, rpcError(e)
	}
	p := &pb.ListInvocationsResponse{}
	if len(v) > n {
		v = v[:n]
		p.NextPageToken = v[n-1].ID
	}
	for _, x := range v {
		p.Invocations = append(p.Invocations, invocationPB(x))
	}
	return p, nil
}
func (s *Server) ListAttempts(ctx context.Context, r *pb.ListAttemptsRequest) (*pb.ListAttemptsResponse, error) {
	n, e := pageSize(r.Limit)
	if e != nil {
		return nil, e
	}
	v, e := s.Workflow.ListAttempts(ctx, r.InvocationId, n+1, r.PageToken)
	if e != nil {
		return nil, rpcError(e)
	}
	p := &pb.ListAttemptsResponse{}
	if len(v) > n {
		v = v[:n]
		p.NextPageToken = v[n-1].ID
	}
	for _, x := range v {
		p.Attempts = append(p.Attempts, attemptPB(x))
	}
	return p, nil
}
func (s *Server) RegisterRunner(ctx context.Context, r *pb.RegisterRunnerRequest) (*pb.RegisterRunnerResponse, error) {
	v := domain.RunnerSession{RunnerID: r.RunnerId, BootID: r.BootId, Slots: r.MaxSlots, MemoryBytes: r.MemoryCapacityBytes, CPUMillis: r.CpuCapacityMillis, ContainerID: r.ContainerId, Architecture: r.Architecture, RuntimeDigest: r.RuntimeDigest}
	if v.Architecture == "" {
		v.Architecture = "linux/amd64"
	}
	if v.RuntimeDigest == "" {
		v.RuntimeDigest = s.RuntimeDigest
	}
	for _, rt := range r.SupportedRuntimes {
		if rt != pb.RuntimeType_RUNTIME_CPP20 {
			return nil, status.Error(codes.InvalidArgument, "only CPP20 runtime is supported")
		}
		v.Runtimes = append(v.Runtimes, "cpp20")
	}
	v, e := s.Runners.Register(ctx, v)
	if e != nil {
		return nil, rpcError(e)
	}
	if _, e = s.Execution.RefreshRunner(ctx, v.ID); e != nil {
		return nil, rpcError(e)
	}
	return &pb.RegisterRunnerResponse{Success: true, HeartbeatIntervalSec: 3, Policy: policyPB(v.Policy), SessionId: v.ID}, nil
}
func (s *Server) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	v, ids, e := s.Runners.Heartbeat(ctx, domain.RunnerSession{ID: r.SessionId, RunnerID: r.RunnerId, AvailableSlots: r.AvailableSlots, UsedMemoryBytes: r.UsedMemoryBytes, CachedKeys: r.CachedAffinityKeys, ObservedPolicy: policy(r.ObservedPolicy), ActiveAttempts: r.ActiveAttempts, PendingCompletions: r.PendingCompletions})
	if e != nil {
		return nil, rpcError(e)
	}
	if _, e = s.Execution.RefreshRunner(ctx, v.ID); e != nil {
		return nil, rpcError(e)
	}
	return &pb.HeartbeatResponse{Acknowledged: true, Policy: policyPB(v.Policy), CancelAttemptIds: ids}, nil
}
func (s *Server) AcquireTasks(ctx context.Context, r *pb.AcquireTasksRequest) (*pb.AcquireTasksResponse, error) {
	v, e := s.Execution.Acquire(ctx, domain.ClaimCommand{SessionID: r.SessionId, RequestID: r.RequestId, RequestedSlots: r.RequestedSlots, WaitTimeoutMS: r.WaitTimeoutMs})
	if e != nil {
		return nil, rpcError(e)
	}
	p := &pb.AcquireTasksResponse{Replayed: v.Duplicate, ExpiredAttemptIds: v.ExpiredAttemptIDs}
	for _, a := range v.Assignments {
		p.Tasks = append(p.Tasks, assignmentPB(a))
	}
	return p, nil
}
func (s *Server) ReportStarted(ctx context.Context, r *pb.ReportStartedRequest) (*pb.ReportStartedResponse, error) {
	a, e := s.Execution.Started(ctx, r.SessionId, r.AttemptId, r.LeaseToken)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.ReportStartedResponse{Valid: true, ExecutionDeadlineUnixMs: optionalMS(a.ExecutionDeadline), ServerUnixMs: ms(time.Now())}, nil
}
func (s *Server) ReportExecutionFinished(ctx context.Context, r *pb.ReportExecutionFinishedRequest) (*pb.ReportExecutionFinishedResponse, error) {
	a, e := s.Execution.ExecutionFinished(ctx, r.SessionId, r.AttemptId, r.LeaseToken, r.DurationMs)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.ReportExecutionFinishedResponse{Valid: true, UploadDeadlineUnixMs: optionalMS(a.UploadDeadline), ServerUnixMs: ms(time.Now())}, nil
}
func (s *Server) RenewLeases(ctx context.Context, r *pb.RenewLeasesRequest) (*pb.RenewLeasesResponse, error) {
	if len(r.Items) > 128 {
		return nil, status.Error(codes.InvalidArgument, "at most 128 leases per renewal")
	}
	p := &pb.RenewLeasesResponse{}
	for _, item := range r.Items {
		if item == nil {
			continue
		}
		a, e := s.Execution.Renew(ctx, r.SessionId, item.AttemptId, item.LeaseToken)
		result := &pb.LeaseRenewResult{AttemptId: item.AttemptId, Valid: e == nil}
		if e != nil {
			result.Reason = e.Error()
			p.RevokedAttemptIds = append(p.RevokedAttemptIds, item.AttemptId)
		} else {
			result.ExpiresUnixMs = ms(a.LeaseExpiresAt)
			result.RemainingMs = time.Until(a.LeaseExpiresAt).Milliseconds()
			p.RenewedAttemptIds = append(p.RenewedAttemptIds, item.AttemptId)
		}
		p.Results = append(p.Results, result)
	}
	p.ServerUnixMs = ms(time.Now())
	return p, nil
}
func outcome(p pb.AttemptStatus) string {
	switch p {
	case pb.AttemptStatus_ATTEMPT_STATUS_SUCCESS:
		return "SUCCEEDED"
	case pb.AttemptStatus_ATTEMPT_STATUS_USER_FAILED:
		return "USER_FAILED"
	case pb.AttemptStatus_ATTEMPT_STATUS_TIMEOUT:
		return "TIMEOUT"
	case pb.AttemptStatus_ATTEMPT_STATUS_OOM:
		return "OOM"
	case pb.AttemptStatus_ATTEMPT_STATUS_CRASHED:
		return "CRASHED"
	case pb.AttemptStatus_ATTEMPT_STATUS_CANCELLED:
		return "CANCELLED"
	case pb.AttemptStatus_ATTEMPT_STATUS_RESOURCE_ERROR:
		return "RESOURCE_ERROR"
	case pb.AttemptStatus_ATTEMPT_STATUS_INFRA_ERROR:
		return "INFRA_ERROR"
	}
	return ""
}
func (s *Server) ReportCompletion(ctx context.Context, r *pb.ReportCompletionRequest) (*pb.ReportCompletionResponse, error) {
	c := domain.Completion{SessionID: r.SessionId, AttemptID: r.AttemptId, LeaseToken: r.LeaseToken, Outcome: outcome(r.FinalStatus), DurationMS: r.DurationMs, Error: r.ErrorMessage}
	for _, v := range r.Stages {
		if v != nil {
			c.Stages = append(c.Stages, domain.StageTiming{Name: v.Name, OffsetMS: v.OffsetMs, DurationMS: v.DurationMs})
		}
	}
	if r.Output != nil {
		p := payload(r.Output)
		c.Output = &p
	} else if len(r.OutputPayload) > 0 {
		c.Output = &domain.PayloadRef{Inline: r.OutputPayload}
	}
	v, e := s.Execution.Complete(ctx, c)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.ReportCompletionResponse{Acknowledged: true, IsDuplicate: v.Duplicate}, nil
}
func (s *Server) ReleaseTask(ctx context.Context, r *pb.ReleaseTaskRequest) (*pb.ReleaseTaskResponse, error) {
	_, e := s.Execution.Release(ctx, r.SessionId, r.AttemptId, r.LeaseToken, r.Reason)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.ReleaseTaskResponse{Acknowledged: true}, nil
}
func (s *Server) ReportMetrics(ctx context.Context, r *pb.ReportMetricsRequest) (*pb.ReportMetricsResponse, error) {
	session, e := s.Runners.GetSession(ctx, r.SessionId)
	if e != nil {
		return nil, rpcError(e)
	}
	if session.RunnerID != r.RunnerId || session.State != "ACTIVE" {
		return nil, status.Error(codes.FailedPrecondition, "metrics session is inactive")
	}
	report := telemetry.Report{RunnerID: r.RunnerId, SessionID: r.SessionId, Sequence: r.Sequence, ObservedAt: time.UnixMilli(r.ObservedUnixMs)}
	for _, f := range r.Functions {
		if f == nil || f.Execution == nil {
			return nil, status.Error(codes.InvalidArgument, "histogram required")
		}
		report.Functions = append(report.Functions, telemetry.FunctionMetrics{Key: telemetry.FunctionKey{FunctionID: f.FunctionId, Version: f.Version, RuntimeDigest: f.RuntimeDigest}, Execution: telemetry.Histogram{Bounds: f.Execution.Bounds, Counts: f.Execution.Counts, Count: f.Execution.Count, Sum: f.Execution.Sum}, Errors: f.Errors, ColdStarts: f.ColdStarts, WarmStarts: f.WarmStarts})
	}
	accepted, e := s.Metrics.Ingest(report)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	return &pb.ReportMetricsResponse{Accepted: accepted}, nil
}
func (s *Server) GetFunctionStats(ctx context.Context, r *pb.GetFunctionStatsRequest) (*pb.GetFunctionStatsResponse, error) {
	window := time.Duration(r.WindowMs) * time.Millisecond
	if window == 0 {
		window = 60 * time.Second
	}
	if window < time.Second || window > s.Metrics.MaxWindow() {
		return nil, status.Error(codes.InvalidArgument, "window must be 1s..retained window")
	}
	snap := s.Metrics.SnapshotWindow(window)
	p := &pb.GetFunctionStatsResponse{}
	for _, f := range snap.Functions {
		if r.FunctionId != "" && r.FunctionId != f.Key.FunctionID || r.Version != "" && r.Version != f.Key.Version {
			continue
		}
		p.Functions = append(p.Functions, &pb.FunctionStats{FunctionId: f.Key.FunctionID, Version: f.Key.Version, RuntimeDigest: f.Key.RuntimeDigest, Count: f.Execution.Count, AverageMs: float64(f.Execution.Avg) / float64(time.Millisecond), P50Ms: float64(f.Execution.P50) / float64(time.Millisecond), P95Ms: float64(f.Execution.P95) / float64(time.Millisecond), P99Ms: float64(f.Execution.P99) / float64(time.Millisecond), Errors: f.Errors, ColdStarts: f.ColdStarts, WarmStarts: f.WarmStarts, UpdatedUnixMs: ms(snap.UpdatedAt), WindowMs: snap.Window.Milliseconds(), InvocationCount: f.Invocation.Count, InvocationAverageMs: float64(f.Invocation.Avg) / float64(time.Millisecond), InvocationP50Ms: float64(f.Invocation.P50) / float64(time.Millisecond), InvocationP95Ms: float64(f.Invocation.P95) / float64(time.Millisecond), InvocationP99Ms: float64(f.Invocation.P99) / float64(time.Millisecond)})
	}
	return p, nil
}
func (s *Server) ListRunners(ctx context.Context, _ *pb.ListRunnersRequest) (*pb.ListRunnersResponse, error) {
	rs, e := s.Runners.ListSessions(ctx)
	if e != nil {
		return nil, rpcError(e)
	}
	p := &pb.ListRunnersResponse{}
	for _, r := range rs {
		used, e := s.Runners.Capacity(ctx, r.ID)
		if e != nil {
			return nil, rpcError(e)
		}
		p.Runners = append(p.Runners, &pb.RunnerSummary{RunnerId: r.RunnerID, SessionId: r.ID, State: r.State, Policy: policyPB(r.Policy), MaxSlots: r.Slots, OccupiedSlots: used.Slots, LastSeenUnixMs: ms(r.LastSeen), CachedAffinityKeys: r.CachedKeys, ContainerId: r.ContainerID, ActiveAttempts: r.ActiveAttempts, PendingCompletions: r.PendingCompletions, DrainAcknowledged: r.DrainAcknowledged})
	}
	return p, nil
}
func (s *Server) SetRunnerPolicy(ctx context.Context, r *pb.SetRunnerPolicyRequest) (*pb.SetRunnerPolicyResponse, error) {
	v, e := s.Runners.SetPolicy(ctx, r.RunnerId, policy(r.Policy))
	if e != nil {
		return nil, rpcError(e)
	}
	_, e = s.Execution.RefreshRunner(ctx, v.ID)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.SetRunnerPolicyResponse{Success: true}, nil
}
func (s *Server) GetPoolSnapshot(ctx context.Context, _ *pb.GetPoolSnapshotRequest) (*pb.GetPoolSnapshotResponse, error) {
	p, e := s.Pool.PoolSnapshot(ctx)
	if e != nil {
		return nil, rpcError(e)
	}
	var draining int32
	for _, r := range p.Runners {
		if r.State == "DRAINING" {
			draining++
		}
	}
	drain := 0.0
	if p.TotalSlots > 0 {
		drain = float64((p.ReadyWork + p.RunningWork).Milliseconds()) / float64(p.TotalSlots)
	}
	return &pb.GetPoolSnapshotResponse{ReadyTasksCount: int32(p.ReadyTasks), RunningTasksCount: int32(p.RunningTasks), LeasedTasksCount: int32(p.LeasedTasks), CompletingTasksCount: int32(p.CompletingTasks), OldestPendingAgeMs: p.OldestReady.Milliseconds(), TotalActiveSlots: int32(p.TotalSlots), UsedSlots: int32(p.OccupiedSlots), EstimatedDrainTimeMs: drain, StartingRunners: int32(p.StartingRunners), DrainingRunners: draining}, nil
}
func (s *Server) getPolicy(ctx context.Context, key string) (*pb.PolicyResponse, error) {
	version, b, e := s.Policies.GetPolicy(ctx, key)
	if e != nil {
		return nil, rpcError(e)
	}
	return &pb.PolicyResponse{Version: version, Json: b}, nil
}
func (s *Server) setPolicy(ctx context.Context, key string, r *pb.SetPolicyRequest, apply func([]byte) error) (*pb.PolicyResponse, error) {
	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	if len(r.Json) > 16384 || !json.Valid(r.Json) {
		return nil, status.Error(codes.InvalidArgument, "policy must be JSON <=16KiB")
	}
	var obj map[string]json.RawMessage
	if e := json.Unmarshal(r.Json, &obj); e != nil || obj == nil {
		return nil, status.Error(codes.InvalidArgument, "policy must be an object")
	}
	// Persist the effective configuration, not a partial patch whose omitted
	// settings would silently revert on controller restart.
	currentVersion, current, e := s.Policies.GetPolicy(ctx, key)
	if e != nil {
		return nil, rpcError(e)
	}
	if currentVersion != r.ExpectedVersion {
		return nil, status.Error(codes.AlreadyExists, "policy version changed; reload before updating")
	}
	merged := map[string]json.RawMessage{}
	if e = json.Unmarshal(current, &merged); e != nil {
		return nil, rpcError(e)
	}
	for k, v := range obj {
		merged[k] = v
	}
	effective, e := json.Marshal(merged)
	if e != nil {
		return nil, rpcError(e)
	}
	if apply == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime policy updates are disabled")
	}
	validate := s.ValidateScheduling
	if key == "autoscaling" {
		validate = s.ValidateAutoscaling
	}
	if validate == nil {
		return nil, status.Error(codes.FailedPrecondition, "policy validation is unavailable")
	}
	if e := validate(effective); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	v, e := s.Policies.SetConfigPolicy(ctx, key, r.ExpectedVersion, effective)
	if e != nil {
		return nil, rpcError(e)
	}
	if e := apply(effective); e != nil {
		return nil, status.Error(codes.Unavailable, "policy persisted; application failed: "+e.Error())
	}
	return &pb.PolicyResponse{Version: v, Json: effective}, nil
}
func (s *Server) GetSchedulingPolicy(ctx context.Context, _ *pb.GetPolicyRequest) (*pb.PolicyResponse, error) {
	return s.getPolicy(ctx, "scheduling")
}
func (s *Server) SetSchedulingPolicy(ctx context.Context, r *pb.SetPolicyRequest) (*pb.PolicyResponse, error) {
	return s.setPolicy(ctx, "scheduling", r, s.ApplyScheduling)
}
func (s *Server) GetAutoscalingPolicy(ctx context.Context, _ *pb.GetPolicyRequest) (*pb.PolicyResponse, error) {
	return s.getPolicy(ctx, "autoscaling")
}
func (s *Server) SetAutoscalingPolicy(ctx context.Context, r *pb.SetPolicyRequest) (*pb.PolicyResponse, error) {
	return s.setPolicy(ctx, "autoscaling", r, s.ApplyAutoscaling)
}
