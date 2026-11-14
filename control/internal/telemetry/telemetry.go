// Package telemetry merges bounded-cardinality execution histograms.
package telemetry

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func DefaultBounds() []float64 {
	return []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
}

// Counts contains non-cumulative bins, including one final +Inf bin. Times are seconds.
type Histogram struct {
	Bounds []float64
	Counts []uint64
	Count  uint64
	Sum    float64
}
type FunctionKey struct{ FunctionID, Version, RuntimeDigest string }
type FunctionMetrics struct {
	Key                            FunctionKey
	Execution                      Histogram
	Errors, ColdStarts, WarmStarts uint64
}
type Report struct {
	RunnerID, SessionID string
	Sequence            uint64
	ObservedAt          time.Time
	Functions           []FunctionMetrics
}
type Config struct {
	Window, Retention, MaxReportGap, SessionTTL, IdleFunctionTTL time.Duration
	MaxFunctions, MaxSessions                                    int
	Now                                                          func() time.Time
}
type Summary struct {
	Count              uint64
	Sum                time.Duration
	Avg, P50, P95, P99 time.Duration
}
type FunctionSnapshot struct {
	Key                            FunctionKey
	Execution, Invocation          Summary
	Errors, ColdStarts, WarmStarts uint64
	ArrivalRate                    float64
}
type Snapshot struct {
	Window     time.Duration
	Functions  []FunctionSnapshot
	Runs       Summary
	FailedRuns uint64
	Sessions   int
	UpdatedAt  time.Time
}

type counters struct {
	execution, invocation        Histogram
	errors, cold, warm, arrivals uint64
}
type timeBucket struct {
	second     int64
	functions  map[FunctionKey]*counters
	runs       Histogram
	failedRuns uint64
}
type session struct {
	sequence       uint64
	at, receivedAt time.Time
	functions      map[FunctionKey]FunctionMetrics
	// After a baseline is evicted, an unknown cumulative series is seeded, not
	// interpreted as newly completed work. This is intentionally conservative.
	baselineEvicted bool
}

type Aggregator struct {
	mu            sync.Mutex
	config        Config
	sessions      map[string]*session
	buckets       map[int64]*timeBucket
	totals        map[FunctionKey]*counters
	runTotals     Histogram
	failedRuns    uint64
	updatedAt     time.Time
	lastSeen      map[FunctionKey]time.Time
	droppedSeries uint64
}

func New(config Config) *Aggregator {
	if config.Window <= 0 {
		config.Window = 60 * time.Second
	}
	if config.Retention <= 0 {
		config.Retention = 5 * time.Minute
	}
	if config.Retention < config.Window {
		config.Retention = config.Window
	}
	if config.MaxReportGap <= 0 {
		config.MaxReportGap = 5 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.MaxFunctions <= 0 {
		config.MaxFunctions = 1024
	}
	if config.MaxSessions <= 0 {
		config.MaxSessions = 256
	}
	if config.SessionTTL <= 0 {
		config.SessionTTL = 10 * time.Minute
	}
	if config.IdleFunctionTTL <= 0 {
		config.IdleFunctionTTL = 30 * time.Minute
	}
	if config.IdleFunctionTTL < config.Retention {
		config.IdleFunctionTTL = config.Retention
	}
	return &Aggregator{config: config, sessions: make(map[string]*session), buckets: make(map[int64]*timeBucket), totals: make(map[FunctionKey]*counters), runTotals: emptyHistogram(), lastSeen: make(map[FunctionKey]time.Time)}
}
func emptyHistogram() Histogram {
	return Histogram{Bounds: DefaultBounds(), Counts: make([]uint64, len(DefaultBounds())+1)}
}
func newCounters() *counters {
	return &counters{execution: emptyHistogram(), invocation: emptyHistogram()}
}
func cloneHistogram(h Histogram) Histogram {
	h.Bounds = append([]float64(nil), h.Bounds...)
	h.Counts = append([]uint64(nil), h.Counts...)
	return h
}
func validHistogram(h Histogram) error {
	expected := DefaultBounds()
	if len(h.Bounds) != len(expected) || len(h.Counts) != len(expected)+1 {
		return errors.New("histogram boundaries do not match protocol")
	}
	var count uint64
	for i, b := range expected {
		if h.Bounds[i] != b {
			return errors.New("histogram boundaries do not match protocol")
		}
	}
	for _, n := range h.Counts {
		if ^uint64(0)-count < n {
			return errors.New("histogram count overflow")
		}
		count += n
	}
	if count != h.Count || math.IsNaN(h.Sum) || math.IsInf(h.Sum, 0) || h.Sum < 0 {
		return errors.New("invalid histogram count or sum")
	}
	return nil
}
func add(dst *Histogram, src Histogram) {
	for i, n := range src.Counts {
		dst.Counts[i] += n
	}
	dst.Count += src.Count
	dst.Sum += src.Sum
}
func difference(next, prev Histogram) (Histogram, error) {
	result := emptyHistogram()
	if next.Count < prev.Count || next.Sum+1e-12 < prev.Sum {
		return result, errors.New("cumulative histogram reset within session")
	}
	result.Count = next.Count - prev.Count
	result.Sum = next.Sum - prev.Sum
	for i, n := range next.Counts {
		if n < prev.Counts[i] {
			return result, errors.New("cumulative histogram bucket decreased")
		}
		result.Counts[i] = n - prev.Counts[i]
	}
	return result, nil
}
func observe(h *Histogram, d time.Duration) {
	seconds := d.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	i := sort.SearchFloat64s(h.Bounds, seconds)
	h.Counts[i]++
	h.Count++
	h.Sum += seconds
}
func summarize(h Histogram) Summary {
	if h.Count == 0 {
		return Summary{}
	}
	result := Summary{Count: h.Count, Sum: time.Duration(h.Sum * float64(time.Second)), Avg: time.Duration(h.Sum / float64(h.Count) * float64(time.Second))}
	result.P50 = quantile(h, .50)
	result.P95 = quantile(h, .95)
	result.P99 = quantile(h, .99)
	return result
}
func quantile(h Histogram, q float64) time.Duration {
	if h.Count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(float64(h.Count) * q))
	var cumulative uint64
	for i, count := range h.Counts {
		cumulative += count
		if cumulative >= rank {
			bound := h.Bounds[len(h.Bounds)-1]
			if i < len(h.Bounds) {
				bound = h.Bounds[i]
			}
			return time.Duration(bound * float64(time.Second))
		}
	}
	return 0
}
func (a *Aggregator) prune(now time.Time) {
	cutoff := now.Add(-a.config.Retention).Unix()
	for second := range a.buckets {
		if second <= cutoff {
			delete(a.buckets, second)
		}
	}
	for key, value := range a.sessions {
		if now.Sub(value.receivedAt) > a.config.SessionTTL {
			delete(a.sessions, key)
		}
	}
	for key, last := range a.lastSeen {
		if now.Sub(last) > a.config.IdleFunctionTTL {
			delete(a.lastSeen, key)
			delete(a.totals, key)
			for _, value := range a.sessions {
				if _, exists := value.functions[key]; exists {
					delete(value.functions, key)
					value.baselineEvicted = true
				}
			}
		}
	}
}
func (a *Aggregator) bucket(now time.Time) *timeBucket {
	a.prune(now)
	second := now.Unix()
	b := a.buckets[second]
	if b == nil {
		b = &timeBucket{second: second, functions: make(map[FunctionKey]*counters), runs: emptyHistogram()}
		a.buckets[second] = b
	}
	a.updatedAt = now
	return b
}
func functionCounters(m map[FunctionKey]*counters, key FunctionKey) *counters {
	c := m[key]
	if c == nil {
		c = newCounters()
		m[key] = c
	}
	return c
}

// Ingest is atomic per report. Session validity must be checked by the RPC service.
// Duplicate/out-of-order sequence numbers are ignored. It never averages quantiles.
func (a *Aggregator) Ingest(report Report) (bool, error) {
	if report.RunnerID == "" || report.SessionID == "" || report.Sequence == 0 {
		return false, errors.New("missing metrics session or sequence")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.config.Now()
	a.prune(now)
	if report.ObservedAt.IsZero() {
		report.ObservedAt = now
	}
	if report.ObservedAt.After(now.Add(5 * time.Second)) {
		return false, errors.New("metrics timestamp is too far in the future")
	}
	key := report.RunnerID + "\x00" + report.SessionID
	previous := a.sessions[key]
	if previous == nil && len(a.sessions) >= a.config.MaxSessions {
		return false, errors.New("metrics session limit reached")
	}
	if len(report.Functions) > a.config.MaxFunctions {
		return false, errors.New("metrics function limit reached")
	}
	if previous != nil && report.Sequence <= previous.sequence {
		return false, nil
	}
	if previous != nil && report.ObservedAt.Before(previous.at) {
		return false, errors.New("metrics timestamp moved backwards")
	}
	next := make(map[FunctionKey]FunctionMetrics, len(report.Functions))
	deltas := make([]FunctionMetrics, 0, len(report.Functions))
	newFunctions := 0
	for _, item := range report.Functions {
		if item.Key.FunctionID == "" || item.Key.Version == "" {
			return false, errors.New("metrics function identity is required")
		}
		if _, exists := next[item.Key]; exists {
			return false, errors.New("duplicate function in metrics report")
		}
		if _, known := a.totals[item.Key]; !known {
			newFunctions++
		}
		if len(a.totals)+newFunctions > a.config.MaxFunctions {
			return false, errors.New("metrics function limit reached")
		}
		if err := validHistogram(item.Execution); err != nil {
			return false, err
		}
		old := FunctionMetrics{Execution: emptyHistogram()}
		knownBaseline := false
		if previous != nil {
			if found, ok := previous.functions[item.Key]; ok {
				old = found
				knownBaseline = true
			}
		}
		delta, err := difference(item.Execution, old.Execution)
		if err != nil {
			return false, err
		}
		if item.Errors < old.Errors || item.ColdStarts < old.ColdStarts || item.WarmStarts < old.WarmStarts {
			return false, errors.New("cumulative function counter decreased")
		}
		if item.Errors > item.Execution.Count || item.ColdStarts+item.WarmStarts > item.Execution.Count {
			return false, errors.New("function counters exceed completed executions")
		}
		newDelta := FunctionMetrics{Key: item.Key, Execution: delta, Errors: item.Errors - old.Errors, ColdStarts: item.ColdStarts - old.ColdStarts, WarmStarts: item.WarmStarts - old.WarmStarts}
		if (previous == nil && report.Sequence != 1) || (previous != nil && previous.baselineEvicted && !knownBaseline) {
			newDelta = FunctionMetrics{Key: item.Key, Execution: emptyHistogram()}
		}
		deltas = append(deltas, newDelta)
		item.Execution = cloneHistogram(item.Execution)
		next[item.Key] = item
	}
	// Omitted functions retain their cumulative baseline instead of double counting
	// when a runner reports only functions that changed in a later batch.
	if previous != nil {
		for k, v := range previous.functions {
			if _, ok := next[k]; !ok {
				next[k] = v
			}
		}
	}
	current := a.bucket(now)
	// A new control-plane process may first see an old runner's sequence 1000.
	// Import its lifetime totals, but do not call the whole history recent traffic.
	windowEligible := now.Sub(report.ObservedAt) <= a.config.MaxReportGap && ((previous == nil && report.Sequence == 1) || (previous != nil && report.ObservedAt.Sub(previous.at) <= a.config.MaxReportGap))
	for _, delta := range deltas {
		a.lastSeen[delta.Key] = now
		total := functionCounters(a.totals, delta.Key)
		add(&total.execution, delta.Execution)
		total.errors += delta.Errors
		total.cold += delta.ColdStarts
		total.warm += delta.WarmStarts
		if windowEligible {
			window := functionCounters(current.functions, delta.Key)
			add(&window.execution, delta.Execution)
			window.errors += delta.Errors
			window.cold += delta.ColdStarts
			window.warm += delta.WarmStarts
		}
	}
	baselineEvicted := previous != nil && previous.baselineEvicted
	a.sessions[key] = &session{sequence: report.Sequence, at: report.ObservedAt, receivedAt: now, functions: next, baselineEvicted: baselineEvicted}
	return true, nil
}

func (a *Aggregator) RemoveSession(runnerID, sessionID string) {
	a.mu.Lock()
	delete(a.sessions, runnerID+"\x00"+sessionID)
	a.mu.Unlock()
}

// ObserveArrival records a newly READY logical invocation; retry generations must
// not be counted again. Arrival rate is distinct from completion rate.
func (a *Aggregator) trackKey(key FunctionKey, now time.Time) bool {
	a.prune(now)
	if _, known := a.totals[key]; !known && len(a.totals) >= a.config.MaxFunctions {
		a.droppedSeries++
		return false
	}
	a.lastSeen[key] = now
	return true
}
func (a *Aggregator) ObserveArrival(key FunctionKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.config.Now()
	if !a.trackKey(key, now) {
		return
	}
	b := a.bucket(now)
	functionCounters(b.functions, key).arrivals++
	functionCounters(a.totals, key).arrivals++
}
func (a *Aggregator) ObserveInvocation(key FunctionKey, duration time.Duration, success bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.config.Now()
	if !a.trackKey(key, now) {
		return
	}
	b := a.bucket(now)
	observe(&functionCounters(b.functions, key).invocation, duration)
	observe(&functionCounters(a.totals, key).invocation, duration)
}
func (a *Aggregator) ObserveRun(duration time.Duration, success bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.bucket(a.config.Now())
	observe(&b.runs, duration)
	observe(&a.runTotals, duration)
	if !success {
		b.failedRuns++
		a.failedRuns++
	}
}

func (a *Aggregator) Snapshot() Snapshot       { return a.SnapshotWindow(a.config.Window) }
func (a *Aggregator) MaxWindow() time.Duration { return a.config.Retention }
func (a *Aggregator) SnapshotWindow(window time.Duration) Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.config.Now()
	a.prune(now)
	if window <= 0 {
		window = a.config.Window
	}
	if window > a.config.Retention {
		window = a.config.Retention
	}
	if window < time.Second {
		window = time.Second
	}
	cutoff := now.Add(-window).Unix()
	merged := make(map[FunctionKey]*counters)
	runs := emptyHistogram()
	var failed uint64
	for _, b := range a.buckets {
		if b.second <= cutoff {
			continue
		}
		add(&runs, b.runs)
		failed += b.failedRuns
		for key, c := range b.functions {
			dst := functionCounters(merged, key)
			add(&dst.execution, c.execution)
			add(&dst.invocation, c.invocation)
			dst.errors += c.errors
			dst.cold += c.cold
			dst.warm += c.warm
			dst.arrivals += c.arrivals
		}
	}
	result := Snapshot{Window: window, Runs: summarize(runs), FailedRuns: failed, Sessions: len(a.sessions), UpdatedAt: a.updatedAt}
	for key, c := range merged {
		result.Functions = append(result.Functions, FunctionSnapshot{Key: key, Execution: summarize(c.execution), Invocation: summarize(c.invocation), Errors: c.errors, ColdStarts: c.cold, WarmStarts: c.warm, ArrivalRate: float64(c.arrivals) / window.Seconds()})
	}
	sort.Slice(result.Functions, func(i, j int) bool {
		return functionKeyString(result.Functions[i].Key) < functionKeyString(result.Functions[j].Key)
	})
	return result
}
func functionKeyString(k FunctionKey) string {
	return k.FunctionID + "\x00" + k.Version + "\x00" + k.RuntimeDigest
}
func labels(k FunctionKey) string {
	return "function=" + strconv.Quote(k.FunctionID) + ",version=" + strconv.Quote(k.Version) + ",runtime_digest=" + strconv.Quote(k.RuntimeDigest)
}
func writeHistogram(w io.Writer, name, ls string, h Histogram) {
	var cumulative uint64
	for i, n := range h.Counts {
		cumulative += n
		bound := "+Inf"
		if i < len(h.Bounds) {
			bound = strconv.FormatFloat(h.Bounds[i], 'g', -1, 64)
		}
		all := ls
		if all != "" {
			all += ","
		}
		fmt.Fprintf(w, "%s_bucket{%sle=%q} %d\n", name, all, bound, cumulative)
	}
	if ls != "" {
		ls = "{" + ls + "}"
	}
	fmt.Fprintf(w, "%s_count%s %d\n%s_sum%s %g\n", name, ls, h.Count, name, ls, h.Sum)
}

// ServeHTTP exports process-lifetime cumulative Prometheus histograms and counters.
// IDs of runs and attempts are deliberately not metric labels.
func (a *Aggregator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	a.mu.Lock()
	a.prune(a.config.Now())
	var output strings.Builder
	fmt.Fprintln(&output, "# TYPE fluxforge_execution_seconds histogram\n# TYPE fluxforge_invocation_seconds histogram\n# TYPE fluxforge_run_seconds histogram")
	keys := make([]FunctionKey, 0, len(a.totals))
	for k := range a.totals {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return functionKeyString(keys[i]) < functionKeyString(keys[j]) })
	for _, key := range keys {
		c := a.totals[key]
		ls := labels(key)
		writeHistogram(&output, "fluxforge_execution_seconds", ls, c.execution)
		writeHistogram(&output, "fluxforge_invocation_seconds", ls, c.invocation)
		fmt.Fprintf(&output, "fluxforge_execution_errors_total{%s} %d\nfluxforge_cold_starts_total{%s} %d\nfluxforge_warm_starts_total{%s} %d\n", ls, c.errors, ls, c.cold, ls, c.warm)
	}
	writeHistogram(&output, "fluxforge_run_seconds", "", a.runTotals)
	fmt.Fprintf(&output, "fluxforge_failed_runs_total %d\nfluxforge_metrics_dropped_series_total %d\n", a.failedRuns, a.droppedSeries)
	a.mu.Unlock()
	_, _ = io.WriteString(w, output.String())
}

// ParseTraceParent performs the minimum W3C traceparent validation needed by
// adapters. Tracing itself should use OpenTelemetry, never a custom span RPC.
func ParseTraceParent(value string) (traceID, spanID string, ok bool) {
	parts := strings.Split(value, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", "", false
	}
	for _, part := range parts {
		for _, c := range part {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return "", "", false
			}
		}
	}
	if parts[1] == strings.Repeat("0", 32) || parts[2] == strings.Repeat("0", 16) {
		return "", "", false
	}
	return parts[1], parts[2], true
}
