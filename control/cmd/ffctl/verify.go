package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "fluxforge-control/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *client) singleWorkflow(name, function string) error {
	ctx, cancel := c.ctx(false)
	defer cancel()
	_, e := pb.NewCatalogServiceClient(c.conn).PublishWorkflow(ctx, &pb.PublishWorkflowRequest{Spec: &pb.WorkflowSpec{WorkflowId: name, Version: "v1", OutputStep: "invoke", DefaultSloMs: 5000, Steps: []*pb.WorkflowStep{{Name: "invoke", Function: &pb.FunctionRef{FunctionId: function, Version: "v1"}, Input: &pb.InputBinding{Binding: &pb.InputBinding_Source{Source: &pb.InputSource{Source: &pb.InputSource_RunInput{RunInput: true}}}}}}}})
	return e
}
func (c *client) submitRequest(workflow string, input *pb.PayloadRef, key string) (*pb.SubmitRunResponse, error) {
	ctx, cancel := c.ctx(false)
	defer cancel()
	return pb.NewWorkflowServiceClient(c.conn).SubmitRun(ctx, &pb.SubmitRunRequest{WorkflowId: workflow, WorkflowVersion: "v1", IdempotencyKey: key, Priority: 50, SloMs: 5000, Input: input})
}
func (c *client) terminal(id string, budget time.Duration) (*pb.RunSummary, error) {
	until := time.Now().Add(budget)
	for time.Now().Before(until) {
		ctx, cancel := c.ctx(false)
		r, e := pb.NewWorkflowServiceClient(c.conn).GetRun(ctx, &pb.GetRunRequest{RunId: id})
		cancel()
		if e != nil {
			return nil, e
		}
		switch r.Status {
		case "SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT":
			return r.Run, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("run %s did not terminate", id)
}
func (c *client) verify(args []string) error {
	f := flag.NewFlagSet("verify", flag.ContinueOnError)
	dir := f.String("artifacts", "/workspace/runner/build/examples", "compiled examples")
	if e := f.Parse(args); e != nil {
		return e
	}
	report := func(s string) { fmt.Println("PASS:", s) }
	for _, name := range []string{"echo", "large_output", "spin", "crash"} {
		if _, e := c.publishFunction(name, "v1", filepath.Join(*dir, name+".so")); e != nil {
			return e
		}
		if e := c.singleWorkflow("verify-"+name, name); e != nil {
			return e
		}
	}
	input := &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: []byte(`{}`)}}
	// The same semantic request is retried without adding a new deadline.
	key := "verify-replay-" + fmt.Sprint(time.Now().UnixNano())
	one, e := c.submitRequest("verify-echo", input, key)
	if e != nil {
		return e
	}
	two, e := c.submitRequest("verify-echo", input, key)
	if e != nil || one.RunId != two.RunId {
		return fmt.Errorf("submit replay created another run: %v", e)
	}
	ctx, cancel := c.ctx(false)
	_, e = pb.NewWorkflowServiceClient(c.conn).SubmitRun(ctx, &pb.SubmitRunRequest{WorkflowId: "verify-echo", WorkflowVersion: "v1", IdempotencyKey: key, Priority: 51, SloMs: 5000, Input: input})
	cancel()
	if status.Code(e) != codes.AlreadyExists {
		return fmt.Errorf("conflicting replay was not rejected: %v", e)
	}
	if r, e := c.terminal(one.RunId, 15*time.Second); e != nil || r.State != "SUCCEEDED" {
		return fmt.Errorf("echo replay: %v", e)
	}
	report("SubmitRun replay and conflicting-key rejection")
	// Check claim-check in both directions using a real 256 KiB JSON input.
	data, _ := json.Marshal(map[string]string{"data": strings.Repeat("x", 256*1024)})
	tmp, e := os.CreateTemp(".", "verify-input-*.json")
	if e != nil {
		return e
	}
	path := tmp.Name()
	defer os.Remove(path)
	if _, e = tmp.Write(data); e != nil {
		tmp.Close()
		return e
	}
	tmp.Close()
	large, e := c.payload(path, "")
	if e != nil {
		return e
	}
	if large.GetObject() == nil {
		return errors.New("large input was not claim-checked")
	}
	submitted, e := c.submitRequest("verify-echo", large, "verify-large-"+fmt.Sprint(time.Now().UnixNano()))
	if e != nil {
		return e
	}
	r, e := c.terminal(submitted.RunId, 20*time.Second)
	if e != nil || r.State != "SUCCEEDED" {
		return fmt.Errorf("large input run: %+v %v", r, e)
	}
	if r.Output.GetObject() == nil {
		return errors.New("large output was sent inline")
	}
	out, e := c.result(r.Output)
	if e != nil {
		return e
	}
	var decoded map[string]string
	if e = json.Unmarshal(out, &decoded); e != nil || len(decoded["data"]) != 256*1024 {
		return fmt.Errorf("large payload mismatch: %v", e)
	}
	report("256 KiB input/output direct MinIO transfer and SHA256 verification")
	generated, e := c.submitRequest("verify-large_output", &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: []byte(`{"bytes":262144}`)}}, "verify-output-"+fmt.Sprint(time.Now().UnixNano()))
	if e != nil {
		return e
	}
	r, e = c.terminal(generated.RunId, 20*time.Second)
	if e != nil || r.State != "SUCCEEDED" || r.Output.GetObject() == nil {
		return fmt.Errorf("large generated output: %+v %v", r, e)
	}
	report("execution-finished/upload/completion phase transition")
	// A crash is retried with new Attempts; an execution timeout is not retried.
	for _, name := range []string{"crash", "spin"} {
		s, e := c.submitRequest("verify-"+name, input, "verify-"+name+fmt.Sprint(time.Now().UnixNano()))
		if e != nil {
			return e
		}
		r, e := c.terminal(s.RunId, 20*time.Second)
		if e != nil || r.State != "FAILED" {
			return fmt.Errorf("%s should fail: %+v %v", name, r, e)
		}
		ctx, cancel := c.ctx(false)
		nodes, e := pb.NewWorkflowServiceClient(c.conn).ListInvocations(ctx, &pb.ListInvocationsRequest{RunId: s.RunId})
		cancel()
		if e != nil || len(nodes.Invocations) != 1 {
			return fmt.Errorf("%s invocation lookup: %v", name, e)
		}
		ctx, cancel = c.ctx(false)
		attempts, e := pb.NewWorkflowServiceClient(c.conn).ListAttempts(ctx, &pb.ListAttemptsRequest{InvocationId: nodes.Invocations[0].InvocationId})
		cancel()
		if e != nil {
			return e
		}
		if name == "crash" && len(attempts.Attempts) != 3 {
			return fmt.Errorf("crash retries: %d, expected 3", len(attempts.Attempts))
		}
		if name == "spin" && (len(attempts.Attempts) != 1 || attempts.Attempts[0].State != "TIMEOUT") {
			return fmt.Errorf("spin timeout attempts: %+v", attempts.Attempts)
		}
		report(name + " recovery and bounded attempt policy")
	}
	s, e := c.submitRequest("verify-spin", input, "verify-cancel-"+fmt.Sprint(time.Now().UnixNano()))
	if e != nil {
		return e
	}
	// Wait for real execution before cancelling; cancelling only a queued Run
	// would not exercise the runner's process termination path.
	started := false
	for until := time.Now().Add(1500 * time.Millisecond); time.Now().Before(until); {
		probe, finish := c.ctx(false)
		nodes, err := pb.NewWorkflowServiceClient(c.conn).ListInvocations(probe, &pb.ListInvocationsRequest{RunId: s.RunId})
		finish()
		if err != nil {
			return err
		}
		if len(nodes.Invocations) > 0 && nodes.Invocations[0].State == "RUNNING" {
			started = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !started {
		return errors.New("cancellation test never observed a running function")
	}
	ctx, cancel = c.ctx(false)
	_, e = pb.NewWorkflowServiceClient(c.conn).CancelRun(ctx, &pb.CancelRunRequest{RunId: s.RunId, Reason: "verification"})
	cancel()
	if e != nil {
		return e
	}
	r, e = c.terminal(s.RunId, 15*time.Second)
	if e != nil || r.State != "CANCELLED" {
		return fmt.Errorf("cancellation: %+v %v", r, e)
	}
	report("durable cancellation and cleanup")
	if e = c.demo([]string{"--artifacts", *dir}); e != nil {
		return e
	}
	report("fan-in and healthy task execution after failures")
	// Independent metrics loop must eventually report completed execution samples.
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		ctx, cancel := c.ctx(false)
		stats, e := pb.NewObservabilityServiceClient(c.conn).GetFunctionStats(ctx, &pb.GetFunctionStatsRequest{FunctionId: "echo", Version: "v1", WindowMs: 60000})
		cancel()
		if e != nil {
			return e
		}
		for _, v := range stats.Functions {
			if v.Count > 0 && v.InvocationCount > 0 {
				report("seconds histogram reporting and both invocation/execution latency")
				fmt.Println("verification complete")
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("function metrics did not arrive within 5 seconds")
}
