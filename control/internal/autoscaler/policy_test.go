package autoscaler

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNoCapacityBacklogScalesImmediately(t *testing.T) {
	p := &provider{s: PoolSnapshot{ReadyTasks: 1, ReadyWork: 10 * time.Millisecond}}
	d := &driver{}
	c := New(Config{ScaleUpConsecutive: 10}, p, &policy{}, d)
	r, err := c.Reconcile(context.Background())
	if err != nil || len(r.Created) != 1 {
		t.Fatalf("backlog has no serving capacity but was delayed: %+v %v", r, err)
	}
}

func TestExitedUnregisteredRunnerDoesNotCountAsStarting(t *testing.T) {
	now := time.Unix(100, 0)
	p := &provider{s: PoolSnapshot{ReadyTasks: 1, ReadyWork: time.Second}}
	d := &driver{runners: []ManagedRunner{{ID: "failed", ContainerID: "failed-container", State: "exited", CreatedAt: now}}}
	c := New(Config{Now: func() time.Time { return now }}, p, &policy{}, d)
	r, err := c.Reconcile(context.Background())
	if err != nil || len(d.removed) != 1 || len(r.Created) == 0 || r.StartingRunners != 0 {
		t.Fatalf("failed startup consumed capacity: %+v %v", r, err)
	}
}

func TestAutoscalingPolicyUpdateChangesDesiredCapacity(t *testing.T) {
	p := &provider{}
	d := &driver{}
	c := New(Config{}, p, &policy{}, d)
	policy := c.Config()
	policy.MinRunners = 3
	policy.ScaleUpConsecutive = 1
	if err := c.UpdateConfig(policy); err != nil {
		t.Fatal(err)
	}
	r, err := c.Reconcile(context.Background())
	if err != nil || r.DesiredRunners != 3 || len(r.Created) != 3 {
		t.Fatalf("policy was persisted but not applied: %+v %v", r, err)
	}
	policy.MaxRunners = 2
	if err := c.UpdateConfig(policy); err == nil {
		t.Fatal("accepted max below minimum")
	}
	policy = c.Config()
	policy.SlotsPerRunner = 129
	if err := c.UpdateConfig(policy); err == nil {
		t.Fatal("accepted slots the Runner cannot register")
	}
}

func TestConcurrentConfigReadsAndUpdates(t *testing.T) {
	c := New(Config{}, &provider{}, &policy{}, &driver{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p := c.Config()
			p.QueueBudget = time.Duration(i+1) * time.Millisecond
			if err := c.UpdateConfig(p); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.Config()
		}
	}()
	wg.Wait()
}
