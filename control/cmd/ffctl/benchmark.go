package main

import (
	"encoding/json"
	"flag"
	pb "fluxforge-control/pkg/api/v1"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"time"
)

type quantiles struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
}

func summarizeSamples(values []float64) quantiles {
	sort.Float64s(values)
	q := quantiles{Count: len(values)}
	if len(values) == 0 {
		return q
	}
	for _, v := range values {
		q.Mean += v
	}
	q.Mean /= float64(len(values))
	pick := func(p float64) float64 { return values[int(math.Ceil(p*float64(len(values))))-1] }
	q.P50 = pick(.5)
	q.P95 = pick(.95)
	q.P99 = pick(.99)
	return q
}
func (c *client) benchmark(args []string) error {
	f := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	count := f.Int("count", 40, "Run count 1..200")
	dir := f.String("artifacts", "/workspace/runner/build/examples", "example artifacts")
	check := f.Bool("verify-scale", false, "require live scale-out and later idle shrink")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *count < 1 || *count > 200 {
		return fmt.Errorf("count must be 1..200")
	}
	if _, e := c.publishFunction("delay", "v1", filepath.Join(*dir, "delay.so")); e != nil {
		return e
	}
	if e := c.singleWorkflow("benchmark-delay", "delay"); e != nil {
		return e
	}
	api := pb.NewWorkflowServiceClient(c.conn)
	admin := pb.NewQueueAdminServiceClient(c.internal)
	peak := int32(0)
	requests := make(map[string]bool, *count)
	for n := 0; n < *count; n++ {
		r, e := c.submitRequest("benchmark-delay", &pb.PayloadRef{Value: &pb.PayloadRef_InlineJson{InlineJson: []byte(`{"milliseconds":200}`)}}, fmt.Sprintf("benchmark-%d-%d", time.Now().UnixNano(), n))
		if e != nil {
			return e
		}
		requests[r.RunId] = true
	}
	var runTimes, invokeTimes, executionTimes []float64
	deadline := time.Now().Add(60 * time.Second)
	for len(requests) > 0 && time.Now().Before(deadline) {
		ctx, cancel := c.ctx(true)
		pool, e := admin.GetPoolSnapshot(ctx, &pb.GetPoolSnapshotRequest{})
		cancel()
		if e != nil {
			return e
		}
		if pool.TotalActiveSlots > peak {
			peak = pool.TotalActiveSlots
		}
		for id := range requests {
			ctx, cancel := c.ctx(false)
			r, e := api.GetRun(ctx, &pb.GetRunRequest{RunId: id})
			cancel()
			if e != nil {
				return e
			}
			if r.Status == "FAILED" || r.Status == "TIMED_OUT" {
				return fmt.Errorf("benchmark run %s failed: %s", id, r.Run.ErrorMessage)
			}
			if r.Status != "SUCCEEDED" {
				continue
			}
			delete(requests, id)
			runTimes = append(runTimes, float64(r.Run.FinishedUnixMs-r.Run.CreatedUnixMs))
			ctx, cancel = c.ctx(false)
			nodes, e := api.ListInvocations(ctx, &pb.ListInvocationsRequest{RunId: id})
			cancel()
			if e != nil {
				return e
			}
			for _, v := range nodes.Invocations {
				invokeTimes = append(invokeTimes, float64(v.FinishedUnixMs-v.ReadyUnixMs))
				ctx, cancel = c.ctx(false)
				attempts, e := api.ListAttempts(ctx, &pb.ListAttemptsRequest{InvocationId: v.InvocationId})
				cancel()
				if e != nil {
					return e
				}
				for _, a := range attempts.Attempts {
					executionTimes = append(executionTimes, float64(a.DurationMs))
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(requests) > 0 {
		return fmt.Errorf("benchmark %d runs remain", len(requests))
	}
	report := struct {
		Run, Invocation, Execution quantiles
		PeakSlots                  int32 `json:"peak_slots"`
		Shrunk                     bool  `json:"idle_shrink_verified"`
	}{Run: summarizeSamples(runTimes), Invocation: summarizeSamples(invokeTimes), Execution: summarizeSamples(executionTimes), PeakSlots: peak}
	if *check {
		if peak <= 2 {
			return fmt.Errorf("load did not demonstrate scale-out; peak slots=%d", peak)
		}
		fmt.Println("scale-out verified; waiting for 60s idle window and drain acknowledgments")
		until := time.Now().Add(90 * time.Second)
		for time.Now().Before(until) {
			ctx, cancel := c.ctx(true)
			p, e := admin.GetPoolSnapshot(ctx, &pb.GetPoolSnapshotRequest{})
			cancel()
			if e != nil {
				return e
			}
			if p.TotalActiveSlots == 2 && p.DrainingRunners == 0 && p.StartingRunners == 0 {
				report.Shrunk = true
				break
			}
			time.Sleep(time.Second)
		}
		if !report.Shrunk {
			return fmt.Errorf("idle pool did not shrink to default minimum within 90s")
		}
	}
	b, e := json.MarshalIndent(report, "", "  ")
	if e == nil {
		fmt.Println(string(b))
	}
	return e
}
