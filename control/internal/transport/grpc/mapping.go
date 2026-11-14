// Package grpc translates transport DTOs without implementing execution state.
package grpc

import (
	"fluxforge-control/internal/domain"
	pb "fluxforge-control/pkg/api/v1"
	"time"
)

func object(p *pb.ObjectRef) domain.ObjectRef {
	if p == nil {
		return domain.ObjectRef{}
	}
	return domain.ObjectRef{ID: p.ObjectId, SHA256: p.Sha256, Size: p.SizeBytes, ContentType: p.ContentType}
}
func objectPB(p domain.ObjectRef) *pb.ObjectRef {
	return &pb.ObjectRef{ObjectId: p.ID, Sha256: p.SHA256, SizeBytes: p.Size, ContentType: p.ContentType}
}
func payload(p *pb.PayloadRef) domain.PayloadRef {
	if p == nil {
		return domain.PayloadRef{}
	}
	switch v := p.Value.(type) {
	case *pb.PayloadRef_InlineJson:
		return domain.PayloadRef{Inline: v.InlineJson}
	case *pb.PayloadRef_Object:
		o := object(v.Object)
		return domain.PayloadRef{Object: &o}
	}
	return domain.PayloadRef{}
}
func payloadPB(p *domain.PayloadRef) *pb.PayloadRef {
	if p == nil {
		return nil
	}
	if p.Object != nil {
		return &pb.PayloadRef{Value: &pb.PayloadRef_Object{Object: objectPB(*p.Object)}}
	}
	return &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: p.Inline}}
}
func function(p *pb.FunctionSpec) domain.FunctionSpec {
	if p == nil {
		return domain.FunctionSpec{}
	}
	runtime := "unsupported"
	if p.Runtime == pb.RuntimeType_RUNTIME_CPP20 || p.Runtime == pb.RuntimeType_RUNTIME_UNSPECIFIED {
		runtime = "cpp20"
	}
	return domain.FunctionSpec{ID: p.FunctionId, Version: p.Version, Runtime: runtime, Architecture: p.Architecture, RuntimeDigest: p.RuntimeDigest, Artifact: object(p.Artifact), DependencyDigest: p.Digest, ABIVersion: p.AbiVersion, Entrypoint: p.Entrypoint, InputSchema: []byte(p.InputSchema), OutputSchema: []byte(p.OutputSchema), TimeoutMS: p.TimeoutMs, MemoryBytes: p.MemoryLimitBytes, CPUMillis: p.CpuMillis, SLOMS: p.SloMs}
}
func functionPB(f domain.FunctionSpec) *pb.FunctionSpec {
	return &pb.FunctionSpec{FunctionId: f.ID, Version: f.Version, Runtime: pb.RuntimeType_RUNTIME_CPP20, Digest: f.DependencyDigest, Artifact: objectPB(f.Artifact), AbiVersion: f.ABIVersion, Architecture: f.Architecture, RuntimeDigest: f.RuntimeDigest, Entrypoint: f.Entrypoint, InputSchema: string(f.InputSchema), OutputSchema: string(f.OutputSchema), TimeoutMs: f.TimeoutMS, MemoryLimitBytes: f.MemoryBytes, CpuMillis: f.CPUMillis, SloMs: f.SLOMS}
}
func source(p *pb.InputSource) domain.InputSource {
	if p == nil {
		return domain.InputSource{Kind: "run_input"}
	}
	s := domain.InputSource{Pointer: p.JsonPointer}
	switch v := p.Source.(type) {
	case *pb.InputSource_RunInput:
		if v.RunInput {
			s.Kind = "run_input"
		}
	case *pb.InputSource_StepOutput:
		s.Kind = "step_output"
		s.Step = v.StepOutput
	case *pb.InputSource_LiteralJson:
		s.Kind = "literal"
		s.Literal = v.LiteralJson
	}
	return s
}
func binding(p *pb.InputBinding) domain.InputPlan {
	out := domain.InputPlan{}
	if p == nil {
		return out
	}
	switch v := p.Binding.(type) {
	case *pb.InputBinding_Source:
		s := source(v.Source)
		out.Source = &s
	case *pb.InputBinding_ObjectFields:
		out.Fields = map[string]domain.InputSource{}
		if v.ObjectFields != nil {
			for k, s := range v.ObjectFields.Fields {
				out.Fields[k] = source(s)
			}
		}
	}
	return out
}
func sourcePB(s domain.InputSource) *pb.InputSource {
	p := &pb.InputSource{JsonPointer: s.Pointer}
	switch s.Kind {
	case "run_input":
		p.Source = &pb.InputSource_RunInput{RunInput: true}
	case "step_output":
		p.Source = &pb.InputSource_StepOutput{StepOutput: s.Step}
	case "literal":
		p.Source = &pb.InputSource_LiteralJson{LiteralJson: s.Literal}
	}
	return p
}
func bindingPB(plan domain.InputPlan) *pb.InputBinding {
	p := &pb.InputBinding{}
	if plan.Source != nil {
		p.Binding = &pb.InputBinding_Source{Source: sourcePB(*plan.Source)}
	} else {
		fields := &pb.InputFields{Fields: map[string]*pb.InputSource{}}
		for k, s := range plan.Fields {
			fields.Fields[k] = sourcePB(s)
		}
		p.Binding = &pb.InputBinding_ObjectFields{ObjectFields: fields}
	}
	return p
}
func workflowSpec(p *pb.WorkflowSpec) domain.WorkflowSpec {
	if p == nil {
		return domain.WorkflowSpec{}
	}
	w := domain.WorkflowSpec{ID: p.WorkflowId, Version: p.Version, OutputStep: p.OutputStep, DefaultSLOMS: p.DefaultSloMs}
	for _, s := range p.Steps {
		if s == nil {
			continue
		}
		v := domain.StepSpec{Name: s.Name, DependsOn: s.DependsOn, Input: binding(s.Input)}
		if s.Function != nil {
			v.Function = domain.FunctionRef{ID: s.Function.FunctionId, Version: s.Function.Version}
		}
		if s.Retry != nil {
			v.Retry = domain.RetryPolicy{MaxAttempts: s.Retry.MaxAttempts, InitialBackoffMS: s.Retry.InitialBackoffMs, MaxBackoffMS: s.Retry.MaxBackoffMs, RetryUserErrors: s.Retry.RetryUserErrors}
		}
		w.Steps = append(w.Steps, v)
	}
	return w
}
func workflowPB(w domain.WorkflowSpec) *pb.WorkflowSpec {
	p := &pb.WorkflowSpec{WorkflowId: w.ID, Version: w.Version, OutputStep: w.OutputStep, DefaultSloMs: w.DefaultSLOMS}
	for _, s := range w.Steps {
		p.Steps = append(p.Steps, &pb.WorkflowStep{Name: s.Name, Function: &pb.FunctionRef{FunctionId: s.Function.ID, Version: s.Function.Version}, DependsOn: s.DependsOn, Input: bindingPB(s.Input), Retry: &pb.RetryPolicy{MaxAttempts: s.Retry.MaxAttempts, InitialBackoffMs: s.Retry.InitialBackoffMS, MaxBackoffMs: s.Retry.MaxBackoffMS, RetryUserErrors: s.Retry.RetryUserErrors}})
	}
	return p
}
func ms(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}
func optionalMS(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return ms(*t)
}
func runPB(r domain.Run) *pb.RunSummary {
	return &pb.RunSummary{RunId: r.ID, WorkflowId: r.WorkflowID, WorkflowVersion: r.WorkflowVersion, State: r.State, Input: payloadPB(&r.Input), Output: payloadPB(r.Output), Priority: r.Priority, SloMs: r.SLOMS, CreatedUnixMs: ms(r.CreatedAt), FinishedUnixMs: optionalMS(r.FinishedAt), DeadlineUnixMs: ms(r.Deadline), ErrorMessage: r.Error, TraceContext: &pb.TraceContext{Traceparent: r.TraceParent, Tracestate: r.TraceState}}
}
func invocationPB(v domain.Invocation) *pb.InvocationSummary {
	return &pb.InvocationSummary{InvocationId: v.ID, RunId: v.RunID, StepName: v.StepName, Function: &pb.FunctionRef{FunctionId: v.Function.ID, Version: v.Function.Version}, State: v.State, RemainingDeps: v.RemainingDeps, AttemptCount: v.AttemptNo, ReadyUnixMs: optionalMS(v.FirstReadyAt), StartedUnixMs: optionalMS(v.StartedAt), FinishedUnixMs: optionalMS(v.FinishedAt), Output: payloadPB(v.Output), ErrorMessage: v.Error}
}
func attemptPB(a domain.Attempt) *pb.AttemptSummary {
	return &pb.AttemptSummary{AttemptId: a.ID, InvocationId: a.InvocationID, Number: a.Number, SessionId: a.SessionID, State: a.State, CreatedUnixMs: ms(a.CreatedAt), StartedUnixMs: optionalMS(a.StartedAt), FinishedUnixMs: optionalMS(a.FinishedAt), DurationMs: a.DurationMS, ErrorMessage: a.Error, Output: payloadPB(a.Output)}
}
func assignmentPB(a domain.Assignment) *pb.TaskAssignment {
	now := time.Now()
	bound := func(s domain.InputSource) *pb.BoundInput {
		return &pb.BoundInput{Payload: payloadPB(s.Payload), JsonPointer: s.Pointer}
	}
	plan := &pb.InputPlan{}
	if a.Input.Source != nil {
		plan.Binding = &pb.InputPlan_Source{Source: bound(*a.Input.Source)}
	} else {
		f := &pb.BoundInputFields{Fields: map[string]*pb.BoundInput{}}
		for k, s := range a.Input.Fields {
			f.Fields[k] = bound(s)
		}
		plan.Binding = &pb.InputPlan_ObjectFields{ObjectFields: f}
	}
	remaining := a.Attempt.LeaseExpiresAt.Sub(now).Milliseconds()
	if remaining < 0 {
		remaining = 0
	}
	return &pb.TaskAssignment{RunId: a.Run.ID, InvocationId: a.Invocation.ID, AttemptId: a.Attempt.ID, StepName: a.Invocation.StepName, AffinityKey: a.Function.AffinityKey(), FunctionSpec: functionPB(a.Function), InputPlan: plan, DeadlineUnixMs: ms(a.Run.Deadline), PrepareTimeoutMs: 30000, CreatedUnixMs: ms(a.Attempt.CreatedAt), TraceContext: &pb.TraceContext{Traceparent: a.Run.TraceParent, Tracestate: a.Run.TraceState}, InitialLease: &pb.TaskLease{AttemptId: a.Attempt.ID, LeaseToken: a.Attempt.LeaseToken, LeaseDurationMs: remaining, RenewIntervalMs: 5000, ExpiresUnixMs: ms(a.Attempt.LeaseExpiresAt), ServerUnixMs: ms(now)}}
}
func policy(v pb.RunnerStatePolicy) string {
	switch v {
	case pb.RunnerStatePolicy_RUNNER_POLICY_DRAINING:
		return "DRAINING"
	case pb.RunnerStatePolicy_RUNNER_POLICY_OFFLINE:
		return "OFFLINE"
	default:
		return "ACTIVE"
	}
}
func policyPB(v string) pb.RunnerStatePolicy {
	switch v {
	case "DRAINING":
		return pb.RunnerStatePolicy_RUNNER_POLICY_DRAINING
	case "OFFLINE":
		return pb.RunnerStatePolicy_RUNNER_POLICY_OFFLINE
	default:
		return pb.RunnerStatePolicy_RUNNER_POLICY_ACTIVE
	}
}
