// ffctl is the container-native FluxForge command line client.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	pb "fluxforge-control/pkg/api/v1"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type client struct{ conn, internal *grpc.ClientConn }

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func (c *client) ctx(internal bool) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	key := "FF_PUBLIC_TOKEN"
	if internal {
		key = "FF_INTERNAL_TOKEN"
	}
	if v := os.Getenv(key); v != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+v)
	}
	return ctx, cancel
}
func connect() (*client, error) {
	dial := func(addr string) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	}
	a, e := dial(env("FF_ADDR", "control-plane:9090"))
	if e != nil {
		return nil, e
	}
	b, e := dial(env("FF_INTERNAL_ADDR", "control-plane:9091"))
	if e != nil {
		a.Close()
		return nil, e
	}
	return &client{a, b}, nil
}
func printMessage(m proto.Message) error {
	b, e := (protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}).Marshal(m)
	if e == nil {
		fmt.Println(string(b))
	}
	return e
}
func flags(name string, args []string) (*flag.FlagSet, *string, *string) {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	id := f.String("id", "", "resource ID")
	version := f.String("version", "v1", "immutable version")
	return f, id, version
}
func (c *client) upload(path, purpose string) (*pb.ObjectRef, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return nil, e
	}
	if _, e = f.Seek(0, 0); e != nil {
		return nil, e
	}
	ctx, cancel := c.ctx(true)
	defer cancel()
	api := pb.NewObjectServiceClient(c.internal)
	content := "application/json"
	if purpose == "FUNCTION_ARTIFACT" {
		content = "application/octet-stream"
	}
	grant, e := api.BeginUpload(ctx, &pb.BeginUploadRequest{Purpose: purpose, SizeBytes: info.Size(), Sha256: hex.EncodeToString(h.Sum(nil)), ContentType: content})
	if e != nil {
		return nil, e
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPut, grant.Url, f)
	if e != nil {
		return nil, e
	}
	req.ContentLength = info.Size()
	for k, v := range grant.Headers {
		req.Header.Set(k, v)
	}
	resp, e := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("upload: HTTP %d", resp.StatusCode)
	}
	out, e := api.CommitUpload(ctx, &pb.CommitUploadRequest{UploadId: grant.UploadId})
	if e != nil {
		return nil, e
	}
	return out.Object, nil
}
func (c *client) payload(path, raw string) (*pb.PayloadRef, error) {
	if path == "" {
		if raw == "" {
			raw = "{}"
		}
		if !json.Valid([]byte(raw)) {
			return nil, errors.New("input must be JSON")
		}
		return &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: []byte(raw)}}, nil
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	if !json.Valid(b) {
		return nil, errors.New("input file must contain JSON")
	}
	if len(b) <= 64*1024 {
		return &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: b}}, nil
	}
	o, e := c.upload(path, "RUN_INPUT")
	if e != nil {
		return nil, e
	}
	return &pb.PayloadRef{Value: &pb.PayloadRef_Object{Object: o}}, nil
}
func (c *client) publishFunction(id, version, path string) (*pb.PublishFunctionResponse, error) {
	data, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	sum := sha256.Sum256(data)
	probe, finish := c.ctx(false)
	existing, lookup := pb.NewCatalogServiceClient(c.conn).GetFunction(probe, &pb.GetFunctionRequest{FunctionId: id, Version: version})
	finish()
	if lookup == nil {
		if existing.GetSpec().GetArtifact().GetSha256() != hex.EncodeToString(sum[:]) {
			return nil, errors.New("function version already exists with another artifact; use a new version")
		}
		return &pb.PublishFunctionResponse{Success: true, Spec: existing.Spec, Message: "existing immutable version reused"}, nil
	}
	if status.Code(lookup) != codes.NotFound {
		return nil, lookup
	}
	o, e := c.upload(path, "FUNCTION_ARTIFACT")
	if e != nil {
		return nil, e
	}
	ctx, cancel := c.ctx(false)
	defer cancel()
	digest := env("FF_RUNTIME_DIGEST", "cpp20-linux-amd64-v1")
	return pb.NewCatalogServiceClient(c.conn).PublishFunction(ctx, &pb.PublishFunctionRequest{Spec: &pb.FunctionSpec{FunctionId: id, Version: version, Runtime: pb.RuntimeType_RUNTIME_CPP20, Digest: digest, RuntimeDigest: digest, Architecture: "linux/amd64", AbiVersion: 1, Entrypoint: "ff_invoke_v1", Artifact: o, InputSchema: `{}`, OutputSchema: `{}`, TimeoutMs: 2000, MemoryLimitBytes: 256 << 20, CpuMillis: 1000, SloMs: 1000}})
}
func (c *client) wait(id string, budget time.Duration) (*pb.GetRunResponse, error) {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		ctx, cancel := c.ctx(false)
		r, e := pb.NewWorkflowServiceClient(c.conn).GetRun(ctx, &pb.GetRunRequest{RunId: id})
		cancel()
		if e != nil {
			return nil, e
		}
		switch r.Status {
		case "SUCCEEDED":
			return r, nil
		case "FAILED", "CANCELLED", "TIMED_OUT":
			return r, fmt.Errorf("run %s: %s (%s)", id, r.Status, r.GetRun().GetErrorMessage())
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, fmt.Errorf("wait timeout; run %s continues; use run get", id)
}
func (c *client) result(p *pb.PayloadRef) ([]byte, error) {
	if p == nil {
		return nil, errors.New("missing output")
	}
	if b := p.GetInlineJson(); b != nil {
		return b, nil
	}
	ctx, cancel := c.ctx(true)
	defer cancel()
	g, e := pb.NewObjectServiceClient(c.internal).GetDownloadURL(ctx, &pb.GetDownloadURLRequest{ObjectId: p.GetObject().GetObjectId()})
	if e != nil {
		return nil, e
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, g.Url, nil)
	if e != nil {
		return nil, e
	}
	r, e := http.DefaultClient.Do(req)
	if e != nil {
		return nil, e
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("download HTTP %d", r.StatusCode)
	}
	b, e := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if e != nil {
		return nil, e
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != g.Object.Sha256 {
		return nil, errors.New("output checksum mismatch")
	}
	return b, nil
}
func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 || args[0] == "help" {
		fmt.Println("ffctl function publish/get | workflow publish/get | run submit/get/wait/cancel | invocation list | attempt list | function stats | runner list/drain | pool status/pause/resume/shutdown | demo | verify")
		return nil
	}
	c, e := connect()
	if e != nil {
		return e
	}
	defer c.conn.Close()
	defer c.internal.Close()
	if args[0] == "demo" {
		return c.demo(args[1:])
	}
	if args[0] == "verify" {
		return c.verify(args[1:])
	}
	if args[0] == "benchmark" {
		return c.benchmark(args[1:])
	}
	if len(args) < 2 {
		return errors.New("expected resource and command")
	}
	kind, verb := args[0], args[1]
	tail := args[2:]
	f, id, version := flags(kind+" "+verb, tail)
	file := f.String("file", "", "input/spec/artifact file")
	raw := f.String("input", "{}", "inline JSON input")
	key := f.String("key", "", "submission idempotency key")
	wait := f.Bool("wait", false, "wait for completion")
	timeout := f.Duration("timeout", 2*time.Minute, "wait budget")
	slo := f.Int64("slo-ms", 5000, "soft Run SLO")
	priority := f.Int("priority", 50, "priority 0..100")
	window := f.Duration("window", 60*time.Second, "statistics window")
	if e = f.Parse(tail); e != nil {
		return e
	}
	internal := kind == "runner" || kind == "pool"
	ctx, cancel := c.ctx(internal)
	defer cancel()
	catalog := pb.NewCatalogServiceClient(c.conn)
	flows := pb.NewWorkflowServiceClient(c.conn)
	admin := pb.NewQueueAdminServiceClient(c.internal)
	switch kind + " " + verb {
	case "pool pause", "pool resume", "pool shutdown":
		return c.fleet(verb, *timeout)
	case "function publish":
		if *id == "" || *file == "" {
			return errors.New("--id and --file required")
		}
		r, e := c.publishFunction(*id, *version, *file)
		if e != nil {
			return e
		}
		return printMessage(r)
	case "function get":
		r, e := catalog.GetFunction(ctx, &pb.GetFunctionRequest{FunctionId: *id, Version: *version})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "workflow publish":
		b, e := os.ReadFile(*file)
		if e != nil {
			return e
		}
		var spec pb.WorkflowSpec
		if e = protojson.Unmarshal(b, &spec); e != nil {
			return e
		}
		r, e := catalog.PublishWorkflow(ctx, &pb.PublishWorkflowRequest{Spec: &spec})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "workflow get":
		r, e := catalog.GetWorkflow(ctx, &pb.GetWorkflowRequest{WorkflowId: *id, Version: *version})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "run submit":
		p, e := c.payload(*file, *raw)
		if e != nil {
			return e
		}
		if *key == "" {
			*key = "cli-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		r, e := flows.SubmitRun(ctx, &pb.SubmitRunRequest{WorkflowId: *id, WorkflowVersion: *version, Input: p, IdempotencyKey: *key, Priority: int32(*priority), SloMs: *slo})
		if e != nil {
			return e
		}
		if !*wait {
			return printMessage(r)
		}
		out, e := c.wait(r.RunId, *timeout)
		if out != nil {
			printMessage(out)
		}
		return e
	case "run get":
		r, e := flows.GetRun(ctx, &pb.GetRunRequest{RunId: *id})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "run wait":
		r, e := c.wait(*id, *timeout)
		if r != nil {
			printMessage(r)
		}
		return e
	case "run cancel":
		r, e := flows.CancelRun(ctx, &pb.CancelRunRequest{RunId: *id, Reason: "CLI cancellation"})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "invocation list":
		r, e := flows.ListInvocations(ctx, &pb.ListInvocationsRequest{RunId: *id, Limit: 100})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "attempt list":
		r, e := flows.ListAttempts(ctx, &pb.ListAttemptsRequest{InvocationId: *id, Limit: 100})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "function stats":
		r, e := pb.NewObservabilityServiceClient(c.conn).GetFunctionStats(ctx, &pb.GetFunctionStatsRequest{FunctionId: *id, Version: *version, WindowMs: window.Milliseconds()})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "runner list":
		r, e := admin.ListRunners(ctx, &pb.ListRunnersRequest{})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "runner drain":
		r, e := admin.SetRunnerPolicy(ctx, &pb.SetRunnerPolicyRequest{RunnerId: *id, Policy: pb.RunnerStatePolicy_RUNNER_POLICY_DRAINING})
		if e != nil {
			return e
		}
		return printMessage(r)
	case "pool status":
		r, e := admin.GetPoolSnapshot(ctx, &pb.GetPoolSnapshotRequest{})
		if e != nil {
			return e
		}
		return printMessage(r)
	default:
		return fmt.Errorf("unknown command: %s %s", kind, verb)
	}
}
func (c *client) demo(args []string) error {
	f := flag.NewFlagSet("demo", flag.ContinueOnError)
	dir := f.String("artifacts", "/workspace/runner/build/examples", "compiled example .so directory")
	if e := f.Parse(args); e != nil {
		return e
	}
	for _, name := range []string{"echo", "add", "fan_in"} {
		if _, e := c.publishFunction(name, "v1", filepath.Join(*dir, name+".so")); e != nil {
			return fmt.Errorf("publish %s: %w", name, e)
		}
	}
	workflow, e := os.ReadFile("/workspace/examples/workflows/fan-in.json")
	if e != nil {
		return e
	}
	var spec pb.WorkflowSpec
	if e = protojson.Unmarshal(workflow, &spec); e != nil {
		return e
	}
	ctx, cancel := c.ctx(false)
	_, e = pb.NewCatalogServiceClient(c.conn).PublishWorkflow(ctx, &pb.PublishWorkflowRequest{Spec: &spec})
	cancel()
	if e != nil {
		return e
	}
	ctx, cancel = c.ctx(false)
	submitted, e := pb.NewWorkflowServiceClient(c.conn).SubmitRun(ctx, &pb.SubmitRunRequest{WorkflowId: spec.WorkflowId, WorkflowVersion: spec.Version, IdempotencyKey: "demo-" + strconv.FormatInt(time.Now().UnixNano(), 10), Priority: 50, SloMs: 5000, DeadlineUnixMs: time.Now().Add(2 * time.Minute).UnixMilli(), Input: &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: []byte(`{"a":2,"b":3}`)}}})
	cancel()
	if e != nil {
		return e
	}
	fmt.Println("run_id:", submitted.RunId)
	r, e := c.wait(submitted.RunId, 2*time.Minute)
	if e != nil {
		return e
	}
	out, e := c.result(r.Run.Output)
	if e != nil {
		return e
	}
	var result struct {
		Value float64 `json:"value"`
	}
	if e = json.NewDecoder(bytes.NewReader(out)).Decode(&result); e != nil {
		return e
	}
	if result.Value != 10 {
		return fmt.Errorf("fan-in expected value=10, received %s", out)
	}
	fmt.Println("fan-in result:", string(out))
	return nil
}
