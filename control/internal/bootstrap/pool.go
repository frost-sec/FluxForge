package bootstrap

import (
	"context"
	"fluxforge-control/internal/adapters/postgres"
	"fluxforge-control/internal/autoscaler"
	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/runnerregistry"
	"fluxforge-control/internal/telemetry"
	"fluxforge-control/internal/workflow"
	"sync"
	"time"
)

type poolView struct {
	store     *postgres.Store
	runners   *runnerregistry.Service
	metrics   *telemetry.Aggregator
	mu        sync.Mutex
	history   map[string]time.Duration
	historyAt time.Time
	workflows map[string]domain.WorkflowSpec
	driver    autoscaler.Driver
}

func (p *poolView) PoolSnapshot(ctx context.Context) (autoscaler.PoolSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if p.history == nil || now.Sub(p.historyAt) > 30*time.Second {
		data, e := p.store.HistoricalEstimates(ctx)
		if e != nil {
			return autoscaler.PoolSnapshot{}, e
		}
		p.history = map[string]time.Duration{}
		for _, x := range data {
			p.history[x.Function.ID+"\x00"+x.Function.Version] = x.Duration
		}
		p.historyAt = now
	}
	estimates := make(map[string]time.Duration, len(p.history))
	for k, v := range p.history {
		estimates[k] = v
	}
	stats := p.metrics.Snapshot()
	var arrival, weighted float64
	var count uint64
	for _, f := range stats.Functions {
		if f.Execution.Count > 0 {
			estimates[f.Key.FunctionID+"\x00"+f.Key.Version] = f.Execution.Avg
			weighted += f.Execution.Avg.Seconds() * float64(f.Execution.Count)
			count += f.Execution.Count
		}
		arrival += f.ArrivalRate
	}
	estimate := func(id, version string) time.Duration {
		v := estimates[id+"\x00"+version]
		if v < time.Millisecond {
			v = 100 * time.Millisecond
		}
		return v
	}
	result := autoscaler.PoolSnapshot{ArrivalRate: arrival, MeanExecutionSeconds: .1}
	setBudget := func(budget time.Duration) {
		if budget < 10*time.Millisecond {
			budget = 10 * time.Millisecond
		}
		if result.QueueBudget == 0 || budget < result.QueueBudget {
			result.QueueBudget = budget
		}
	}
	if count > 0 {
		result.MeanExecutionSeconds = weighted / float64(count)
	}
	tasks, e := p.store.ListReady(ctx)
	if e != nil {
		return result, e
	}
	result.ReadyTasks = len(tasks)
	for _, t := range tasks {
		cost := estimate(t.FunctionID, t.FunctionVersion)
		result.ReadyWork += cost
		if !t.SLODeadline.IsZero() {
			setBudget(t.SLODeadline.Sub(now) - cost)
		}
		age := now.Sub(t.ReadyAt)
		if age > result.OldestReady {
			result.OldestReady = age
		}
	}
	work, e := p.store.ActiveWork(ctx)
	if e != nil {
		return result, e
	}
	for _, w := range work {
		switch w.State {
		case "LEASED":
			result.LeasedTasks++
		case "RUNNING":
			result.RunningTasks++
		case "COMPLETING":
			result.CompletingTasks++
		}
		d := estimate(w.FunctionID, w.FunctionVersion)
		if w.StartedAt != nil {
			d -= now.Sub(*w.StartedAt)
		}
		if d < time.Millisecond {
			d = time.Millisecond
		}
		result.RunningWork += d
	}
	rs, e := p.runners.ListSessions(ctx)
	if e != nil {
		return result, e
	}
	known := map[string]bool{}
	for _, r := range rs {
		if r.State == "ACTIVE" {
			known[r.RunnerID] = true
		}
		used, e := p.runners.Capacity(ctx, r.ID)
		if e != nil {
			return result, e
		}
		state := r.State
		if r.Policy == "DRAINING" {
			state = "DRAINING"
		}
		pending := int(r.PendingCompletions)
		if state == "DRAINING" && !r.DrainAcknowledged && pending == 0 {
			pending = 1
		}
		load := autoscaler.RunnerLoad{ID: r.RunnerID, SessionID: r.ID, State: state, ActiveAttempts: int(used.Slots), PendingCompletions: pending, WarmKeys: len(r.CachedKeys)}
		result.Runners = append(result.Runners, load)
		if r.State == "ACTIVE" && r.Policy == "ACTIVE" && now.Sub(r.LastSeen) < p.store.HeartbeatTimeout {
			result.ActiveRunners++
			result.TotalSlots += int(r.Slots)
		}
		result.OccupiedSlots += int(used.Slots)
	}
	if p.driver != nil {
		managed, err := p.driver.List(ctx)
		if err == nil {
			for _, m := range managed {
				if !known[m.ID] && (m.State == "running" || m.State == "created") {
					result.StartingRunners++
				}
			}
		}
	}
	views, e := p.store.ActiveSLOViews(ctx)
	if e != nil {
		return result, e
	}
	if p.workflows == nil {
		p.workflows = map[string]domain.WorkflowSpec{}
	}
	for _, v := range views {
		key := v.Run.WorkflowID + "\x00" + v.Run.WorkflowVersion
		spec, ok := p.workflows[key]
		if !ok {
			spec, e = p.store.GetWorkflow(ctx, v.Run.WorkflowID, v.Run.WorkflowVersion)
			if e != nil {
				return result, e
			}
			if len(p.workflows) >= 1024 {
				p.workflows = map[string]domain.WorkflowSpec{}
			}
			p.workflows[key] = spec
		}
		path := workflow.RemainingCriticalPath(spec, v.Nodes, func(f domain.FunctionRef) time.Duration { return estimate(f.ID, f.Version) }, now)
		budget := v.Run.CreatedAt.Add(time.Duration(v.Run.SLOMS)*time.Millisecond).Sub(now) - path
		if budget < 0 {
			result.RunSLORisk = true
		}
		setBudget(budget)
	}
	return result, nil
}
