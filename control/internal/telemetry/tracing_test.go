package telemetry

import (
	"context"
	"encoding/hex"
	"fluxforge-control/internal/domain"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestOTLPExportsStableRunInvocationAttemptParents(t *testing.T) {
	var mu sync.Mutex
	var spans []*tracepb.Span
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, e := io.ReadAll(r.Body)
		if e != nil {
			t.Error(e)
		}
		var req collector.ExportTraceServiceRequest
		if e = proto.Unmarshal(b, &req); e != nil {
			t.Error(e)
		}
		mu.Lock()
		for _, resource := range req.ResourceSpans {
			for _, scope := range resource.ScopeSpans {
				spans = append(spans, scope.Spans...)
			}
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer server.Close()
	ctx := context.Background()
	tracer, e := NewTracing(ctx, server.URL)
	if e != nil {
		t.Fatal(e)
	}
	start := time.Now().Add(-3 * time.Second)
	ready := start.Add(100 * time.Millisecond)
	called := ready.Add(100 * time.Millisecond)
	end := start.Add(2 * time.Second)
	run := domain.Run{ID: "run", WorkflowID: "workflow", State: "SUCCEEDED", CreatedAt: start, FinishedAt: &end, TraceParent: "00-0123456789abcdef0123456789abcdef-1122334455667788-01", ParentSpanID: "8877665544332211"}
	inv := domain.Invocation{ID: "invoke", RunID: run.ID, StepName: "A", State: "SUCCEEDED", FirstReadyAt: &ready, StartedAt: &called, FinishedAt: &end}
	a := domain.Assignment{Run: run, Invocation: inv, Function: domain.FunctionSpec{ID: "echo"}, Attempt: domain.Attempt{ID: "attempt", CreatedAt: ready, StartedAt: &called, FinishedAt: &end}}
	tracer.Completed(ctx, a, domain.Completion{Outcome: "SUCCEEDED", Stages: []domain.StageTiming{{Name: "artifact.fetch", DurationMS: 5}, {Name: "worker.execute", OffsetMS: 100, DurationMS: 50}}})
	tracer.Invocation(ctx, run, inv, a.Function)
	tracer.Run(ctx, run)
	if e = tracer.Shutdown(ctx); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	defer mu.Unlock()
	byName := map[string]*tracepb.Span{}
	for _, s := range spans {
		byName[s.Name] = s
		if hex.EncodeToString(s.TraceId) != "0123456789abcdef0123456789abcdef" {
			t.Fatal("trace identity changed")
		}
	}
	root := byName["workflow.run"]
	logical := byName["function.invoke"]
	attempt := byName["function.attempt"]
	if root == nil || logical == nil || attempt == nil || byName["artifact.fetch"] == nil {
		t.Fatalf("missing spans: %v", byName)
	}
	if hex.EncodeToString(root.SpanId) != "1122334455667788" || hex.EncodeToString(root.ParentSpanId) != "8877665544332211" {
		t.Fatal("caller/Run context not preserved")
	}
	if hex.EncodeToString(logical.ParentSpanId) != hex.EncodeToString(root.SpanId) || hex.EncodeToString(attempt.ParentSpanId) != hex.EncodeToString(logical.SpanId) {
		t.Fatal("incorrect asynchronous hierarchy")
	}
	if root.EndTimeUnixNano-root.StartTimeUnixNano != uint64(2*time.Second) {
		t.Fatal("Run latency did not include whole request")
	}
}
