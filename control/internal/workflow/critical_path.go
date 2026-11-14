package workflow

import (
	"fluxforge-control/internal/domain"
	"time"
)

// RemainingCriticalPath uses max(parent path), not the sum of parallel branches.
// Estimates are advisory; soft SLO risk never changes authoritative task state.
func RemainingCriticalPath(w domain.WorkflowSpec, nodes []domain.Invocation, estimate func(domain.FunctionRef) time.Duration, now time.Time) time.Duration {
	states := make(map[string]domain.Invocation, len(nodes))
	for _, n := range nodes {
		states[n.StepName] = n
	}
	steps := make(map[string]domain.StepSpec, len(w.Steps))
	for _, s := range w.Steps {
		steps[s.Name] = s
	}
	memo := make(map[string]time.Duration, len(w.Steps))
	var visit func(string) time.Duration
	visit = func(name string) time.Duration {
		if v, ok := memo[name]; ok {
			return v
		}
		s := steps[name]
		n, ok := states[name]
		if ok && domain.TerminalInvocation(n.State) {
			memo[name] = 0
			return 0
		}
		cost := estimate(s.Function)
		if cost < time.Millisecond {
			cost = time.Millisecond
		}
		if ok && n.State == "RUNNING" && n.StartedAt != nil {
			cost -= now.Sub(*n.StartedAt)
			if cost < time.Millisecond {
				cost = time.Millisecond
			}
		}
		if ok && n.State == "RETRY_WAIT" && n.NotBefore != nil && n.NotBefore.After(now) {
			cost += n.NotBefore.Sub(now)
		}
		var upstream time.Duration
		for _, parent := range s.DependsOn {
			if d := visit(parent); d > upstream {
				upstream = d
			}
		}
		memo[name] = cost + upstream
		return memo[name]
	}
	var longest time.Duration
	for _, s := range w.Steps {
		if d := visit(s.Name); d > longest {
			longest = d
		}
	}
	return longest
}
