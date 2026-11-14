package telemetry

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func histogram(duration time.Duration, count int) Histogram {
	h := emptyHistogram()
	for i := 0; i < count; i++ {
		observe(&h, duration)
	}
	return h
}
func report(runner string, sequence uint64, h Histogram) Report {
	return Report{RunnerID: runner, SessionID: "boot", Sequence: sequence, Functions: []FunctionMetrics{{Key: FunctionKey{FunctionID: "echo", Version: "v1"}, Execution: h, WarmStarts: h.Count}}}
}

func TestMergedQuantileIsNotAverageOfRunnerQuantiles(t *testing.T) {
	a := New(Config{})
	if _, err := a.Ingest(report("fast", 1, histogram(10*time.Millisecond, 99))); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Ingest(report("slow", 1, histogram(time.Second, 1))); err != nil {
		t.Fatal(err)
	}
	s := a.Snapshot()
	if len(s.Functions) != 1 || s.Functions[0].Execution.Count != 100 {
		t.Fatalf("unexpected stats %+v", s)
	}
	if s.Functions[0].Execution.P99 != 10*time.Millisecond {
		t.Fatalf("p99 averaged rather than merged: %v", s.Functions[0].Execution.P99)
	}
	if got := s.Functions[0].Execution.Avg; got < 19*time.Millisecond || got > 20*time.Millisecond {
		t.Fatalf("incorrect weighted avg: %v", got)
	}
}

func TestCumulativeReplayAndSessionRestart(t *testing.T) {
	a := New(Config{})
	r := report("r", 1, histogram(time.Millisecond, 2))
	ok, err := a.Ingest(r)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	ok, err = a.Ingest(r)
	if ok || err != nil {
		t.Fatalf("duplicate not ignored: %v %v", ok, err)
	}
	r.Sequence = 2
	r.Functions[0].Execution = histogram(time.Millisecond, 3)
	r.Functions[0].WarmStarts = 3
	if _, err = a.Ingest(r); err != nil {
		t.Fatal(err)
	}
	r.Sequence = 1
	r.SessionID = "restarted"
	r.Functions[0].Execution = histogram(time.Millisecond, 1)
	r.Functions[0].WarmStarts = 1
	if _, err = a.Ingest(r); err != nil {
		t.Fatal(err)
	}
	if got := a.Snapshot().Functions[0].Execution.Count; got != 4 {
		t.Fatalf("count=%d", got)
	}
}

func TestInvalidReportIsAtomic(t *testing.T) {
	a := New(Config{})
	r := report("r", 1, histogram(time.Millisecond, 1))
	broken := r.Functions[0]
	broken.Key.FunctionID = "bad"
	broken.Execution.Count++
	r.Functions = append(r.Functions, broken)
	if _, err := a.Ingest(r); err == nil {
		t.Fatal("accepted invalid histogram")
	}
	if got := len(a.Snapshot().Functions); got != 0 {
		t.Fatal("partially applied report")
	}
	if _, err := a.Ingest(report("r", 1, histogram(time.Millisecond, 1))); err != nil {
		t.Fatal(err)
	}
}

func TestWindowExpiryAndArrivalRate(t *testing.T) {
	now := time.Unix(100, 0)
	a := New(Config{Now: func() time.Time { return now }, Window: 10 * time.Second})
	key := FunctionKey{FunctionID: "echo", Version: "v1"}
	a.ObserveArrival(key)
	a.ObserveArrival(key)
	a.ObserveInvocation(key, time.Millisecond, true)
	a.ObserveRun(time.Second, true)
	s := a.Snapshot()
	if s.Functions[0].ArrivalRate != .2 || s.Functions[0].Invocation.Count != 1 || s.Runs.Count != 1 {
		t.Fatalf("bad counters %+v", s)
	}
	now = now.Add(11 * time.Second)
	if s = a.Snapshot(); len(s.Functions) != 0 || s.Runs.Count != 0 {
		t.Fatalf("window retained expired samples %+v", s)
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), "fluxforge_run_seconds_count 1") {
		t.Fatal("Prometheus cumulative counter expired with window")
	}
}

func TestLongGapDoesNotInventRecentTraffic(t *testing.T) {
	now := time.Unix(100, 0)
	a := New(Config{Now: func() time.Time { return now }, Window: 10 * time.Second, MaxReportGap: 5 * time.Second})
	r := report("r", 1, histogram(time.Millisecond, 1))
	r.ObservedAt = now
	_, _ = a.Ingest(r)
	now = now.Add(20 * time.Second)
	r.Sequence = 2
	r.ObservedAt = now
	r.Functions[0].Execution = histogram(time.Millisecond, 100)
	r.Functions[0].WarmStarts = 100
	if _, err := a.Ingest(r); err != nil {
		t.Fatal(err)
	}
	if len(a.Snapshot().Functions) != 0 {
		t.Fatal("placed stale accumulated traffic into current window")
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), "runtime_digest=\"\"} 100") {
		t.Fatal("lifetime metrics lost recovered cumulative samples")
	}
}

func TestTraceParentValidation(t *testing.T) {
	if _, _, ok := ParseTraceParent("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"); !ok {
		t.Fatal("valid traceparent rejected")
	}
	if _, _, ok := ParseTraceParent("00-00000000000000000000000000000000-0123456789abcdef-01"); ok {
		t.Fatal("zero trace id accepted")
	}
}

func TestRequestedWindowAndQuantiles(t *testing.T) {
	now := time.Unix(100, 0)
	a := New(Config{Now: func() time.Time { return now }})
	key := FunctionKey{FunctionID: "echo", Version: "v1"}
	a.ObserveInvocation(key, time.Millisecond, true)
	now = now.Add(10 * time.Second)
	a.ObserveInvocation(key, time.Second, true)
	short := a.SnapshotWindow(5 * time.Second)
	long := a.SnapshotWindow(30 * time.Second)
	if short.Functions[0].Invocation.Count != 1 || long.Functions[0].Invocation.Count != 2 {
		t.Fatal("requested metric window was ignored")
	}
	if long.Functions[0].Invocation.P50 != time.Millisecond || long.Functions[0].Invocation.P95 != time.Second || long.Functions[0].Invocation.P99 != time.Second {
		t.Fatalf("invalid quantiles %+v", long.Functions[0].Invocation)
	}
}

func TestCardinalityAndRetiredSessionLimits(t *testing.T) {
	now := time.Unix(100, 0)
	a := New(Config{Now: func() time.Time { return now }, MaxFunctions: 1, MaxSessions: 1, SessionTTL: time.Second})
	if _, err := a.Ingest(report("one", 1, histogram(time.Millisecond, 1))); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Ingest(report("two", 1, histogram(time.Millisecond, 1))); err == nil {
		t.Fatal("accepted more sessions than the configured bound")
	}
	r := report("one", 2, histogram(time.Millisecond, 1))
	r.Functions[0].Key.FunctionID = "another"
	if _, err := a.Ingest(r); err == nil {
		t.Fatal("accepted more function series than the configured bound")
	}
	now = now.Add(2 * time.Second)
	if _, err := a.Ingest(report("two", 1, histogram(time.Millisecond, 1))); err != nil {
		t.Fatalf("retired session not reclaimed: %v", err)
	}
	if a.Snapshot().Sessions != 1 {
		t.Fatal("expired session survived pruning")
	}
}

func TestExistingRunnerFirstSnapshotSeedsWindowBaseline(t *testing.T) {
	a := New(Config{})
	if _, err := a.Ingest(report("existing", 500, histogram(time.Millisecond, 100))); err != nil {
		t.Fatal(err)
	}
	if len(a.Snapshot().Functions) != 0 {
		t.Fatal("control-plane restart treated lifetime history as recent traffic")
	}
	r := report("existing", 501, histogram(time.Millisecond, 101))
	if _, err := a.Ingest(r); err != nil {
		t.Fatal(err)
	}
	if a.Snapshot().Functions[0].Execution.Count != 1 {
		t.Fatal("did not observe execution delta after baseline")
	}
}
