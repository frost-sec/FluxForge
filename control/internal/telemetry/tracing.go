package telemetry

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"time"

	"fluxforge-control/internal/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Stable span IDs let terminal events reconstruct the same logical parent after
// controller recovery; telemetry remains independent of transaction success.
type fixedIdentity struct {
	traceID trace.TraceID
	spanID  trace.SpanID
}
type identityKey struct{}
type identityGenerator struct{}

func (identityGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if id, ok := ctx.Value(identityKey{}).(fixedIdentity); ok {
		return id.traceID, id.spanID
	}
	var t trace.TraceID
	var s trace.SpanID
	rand.Read(t[:])
	rand.Read(s[:])
	return t, s
}
func (identityGenerator) NewSpanID(ctx context.Context, _ trace.TraceID) trace.SpanID {
	if id, ok := ctx.Value(identityKey{}).(fixedIdentity); ok {
		return id.spanID
	}
	var s trace.SpanID
	rand.Read(s[:])
	return s
}

type Tracing struct {
	provider *sdk.TracerProvider
	tracer   trace.Tracer
}

func NewTracing(ctx context.Context, endpoint string) (*Tracing, error) {
	if endpoint == "" {
		return &Tracing{}, nil
	}
	u, e := url.Parse(endpoint)
	if e != nil {
		return nil, e
	}
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host), otlptracehttp.WithTimeout(2 * time.Second)}
	if u.Scheme != "https" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exporter, e := otlptracehttp.New(ctx, opts...)
	if e != nil {
		return nil, e
	}
	p := sdk.NewTracerProvider(sdk.WithIDGenerator(identityGenerator{}), sdk.WithSampler(sdk.AlwaysSample()), sdk.WithResource(resource.NewSchemaless(attribute.String("service.name", "fluxforge-control"))), sdk.WithBatcher(exporter, sdk.WithBatchTimeout(time.Second), sdk.WithMaxQueueSize(2048)))
	return &Tracing{provider: p, tracer: p.Tracer("fluxforge/workflow")}, nil
}
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}
func spanID(name string) trace.SpanID {
	h := sha256.Sum256([]byte(name))
	var id trace.SpanID
	copy(id[:], h[:8])
	return id
}
func rootIdentity(r domain.Run) (fixedIdentity, bool) {
	tid, sid, ok := ParseTraceParent(r.TraceParent)
	if !ok {
		return fixedIdentity{}, false
	}
	a, e := trace.TraceIDFromHex(tid)
	if e != nil {
		return fixedIdentity{}, false
	}
	b, e := trace.SpanIDFromHex(sid)
	return fixedIdentity{a, b}, e == nil
}
func (t *Tracing) emit(ctx context.Context, name string, r domain.Run, id, parent trace.SpanID, start, end time.Time, attrs []attribute.KeyValue, failed bool) {
	if t == nil || t.tracer == nil || start.IsZero() || end.IsZero() {
		return
	}
	root, ok := rootIdentity(r)
	if !ok {
		return
	}
	if end.Before(start) {
		end = start
	}
	ctx = context.WithValue(ctx, identityKey{}, fixedIdentity{root.traceID, id})
	if parent.IsValid() {
		state, _ := trace.ParseTraceState(r.TraceState)
		ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: root.traceID, SpanID: parent, TraceState: state, TraceFlags: trace.FlagsSampled, Remote: true}))
	} else {
		ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	}
	_, s := t.tracer.Start(ctx, name, trace.WithTimestamp(start), trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	if failed {
		s.SetStatus(codes.Error, "execution failed")
	}
	s.End(trace.WithTimestamp(end))
}
func (t *Tracing) Run(ctx context.Context, r domain.Run) {
	if r.FinishedAt == nil {
		return
	}
	root, ok := rootIdentity(r)
	if !ok {
		return
	}
	var parent trace.SpanID
	if r.ParentSpanID != "" {
		parent, _ = trace.SpanIDFromHex(r.ParentSpanID)
	}
	t.emit(ctx, "workflow.run", r, root.spanID, parent, r.CreatedAt, *r.FinishedAt, []attribute.KeyValue{attribute.String("fluxforge.run_id", r.ID), attribute.String("fluxforge.workflow", r.WorkflowID), attribute.String("fluxforge.state", r.State)}, r.State != "SUCCEEDED")
}
func (t *Tracing) Invocation(ctx context.Context, r domain.Run, i domain.Invocation, f domain.FunctionSpec) {
	if i.FirstReadyAt == nil || i.FinishedAt == nil {
		return
	}
	root, ok := rootIdentity(r)
	if !ok {
		return
	}
	id := spanID("invoke:" + i.ID)
	attrs := []attribute.KeyValue{attribute.String("fluxforge.run_id", r.ID), attribute.String("fluxforge.invocation_id", i.ID), attribute.String("fluxforge.step", i.StepName), attribute.String("faas.name", f.ID), attribute.String("faas.version", f.Version), attribute.String("fluxforge.state", i.State)}
	t.emit(ctx, "function.invoke", r, id, root.spanID, *i.FirstReadyAt, *i.FinishedAt, attrs, i.State != "SUCCEEDED")
}
func (t *Tracing) Started(context.Context, domain.Assignment) {}
func (t *Tracing) Completed(ctx context.Context, a domain.Assignment, c domain.Completion) {
	end := time.Now().UTC()
	if a.Attempt.FinishedAt != nil {
		end = *a.Attempt.FinishedAt
	}
	attrs := []attribute.KeyValue{attribute.String("fluxforge.run_id", a.Run.ID), attribute.String("fluxforge.invocation_id", a.Invocation.ID), attribute.String("fluxforge.attempt_id", a.Attempt.ID), attribute.String("faas.name", a.Function.ID), attribute.String("fluxforge.outcome", c.Outcome)}
	id := spanID("attempt:" + a.Attempt.ID)
	t.emit(ctx, "function.attempt", a.Run, id, spanID("invoke:"+a.Invocation.ID), a.Attempt.CreatedAt, end, attrs, c.Outcome != "SUCCEEDED")
	if a.Invocation.ReadyAt != nil {
		t.emit(ctx, "queue.wait", a.Run, spanID("queue:"+a.Attempt.ID), spanID("invoke:"+a.Invocation.ID), *a.Invocation.ReadyAt, a.Attempt.CreatedAt, attrs, false)
	}
	if len(c.Stages) > 0 {
		for n, v := range c.Stages {
			start := a.Attempt.CreatedAt.Add(time.Duration(v.OffsetMS) * time.Millisecond)
			finish := start.Add(time.Duration(v.DurationMS) * time.Millisecond)
			if finish.After(end) {
				finish = end
			}
			if start.After(end) {
				continue
			}
			t.emit(ctx, v.Name, a.Run, spanID(v.Name+":"+a.Attempt.ID+":"+fmt.Sprint(n)), id, start, finish, attrs, false)
		}
		return
	}
	if a.Attempt.StartedAt != nil {
		start := *a.Attempt.StartedAt
		execEnd := start.Add(time.Duration(c.DurationMS) * time.Millisecond)
		if execEnd.After(end) {
			execEnd = end
		}
		t.emit(ctx, "worker.prepare", a.Run, spanID("prepare:"+a.Attempt.ID), id, a.Attempt.CreatedAt, start, attrs, false)
		t.emit(ctx, "worker.execute", a.Run, spanID("execute:"+a.Attempt.ID), id, start, execEnd, attrs, c.Outcome != "SUCCEEDED")
		if end.After(execEnd) {
			t.emit(ctx, "output.commit", a.Run, spanID("output:"+a.Attempt.ID), id, execEnd, end, attrs, false)
		}
	}
}

// TraceID returns the stable request trace identifier for the API/CLI.
func TraceID(traceparent string) string {
	id, _, ok := ParseTraceParent(traceparent)
	if !ok {
		return ""
	}
	_, e := hex.DecodeString(id)
	if e != nil {
		return ""
	}
	return id
}
