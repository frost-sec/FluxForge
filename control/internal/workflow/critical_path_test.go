package workflow

import (
	"fluxforge-control/internal/domain"
	"testing"
	"time"
)

func TestCriticalPathPreservesParallelismAndRemainingWork(t *testing.T) {
	now := time.Now()
	w := domain.WorkflowSpec{Steps: []domain.StepSpec{{Name: "A"}, {Name: "B", DependsOn: []string{"A"}}, {Name: "C", DependsOn: []string{"A"}}, {Name: "D", DependsOn: []string{"B", "C"}}}}
	estimate := func(domain.FunctionRef) time.Duration { return time.Second }
	if d := RemainingCriticalPath(w, nil, estimate, now); d != 3*time.Second {
		t.Fatalf("parallel branches summed: %s", d)
	}
	started := now.Add(-500 * time.Millisecond)
	nodes := []domain.Invocation{{StepName: "A", State: "SUCCEEDED"}, {StepName: "B", State: "RUNNING", StartedAt: &started}, {StepName: "C", State: "SUCCEEDED"}}
	if d := RemainingCriticalPath(w, nodes, estimate, now); d != 1500*time.Millisecond {
		t.Fatalf("completed work retained: %s", d)
	}
}
