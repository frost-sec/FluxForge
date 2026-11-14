package autoscaler

import (
	"context"
	"errors"
	"testing"
	"time"
)

type provider struct {
	s   PoolSnapshot
	err error
}

func (p *provider) PoolSnapshot(context.Context) (PoolSnapshot, error) { return p.s, p.err }

type policy struct{ drained []string }

func (p *policy) SetDraining(_ context.Context, id, session string) error {
	p.drained = append(p.drained, id)
	return nil
}

type driver struct {
	runners []ManagedRunner
	created []RunnerSpec
	removed []string
}

func (d *driver) List(context.Context) ([]ManagedRunner, error) {
	return append([]ManagedRunner(nil), d.runners...), nil
}
func (d *driver) Create(_ context.Context, s RunnerSpec) (ManagedRunner, error) {
	d.created = append(d.created, s)
	r := ManagedRunner{ID: s.ID, ContainerID: s.ID, CreatedAt: time.Now()}
	d.runners = append(d.runners, r)
	return r, nil
}
func (d *driver) Remove(_ context.Context, id string) error {
	d.removed = append(d.removed, id)
	return nil
}

func TestScaleUpHysteresisAndStartingCapacity(t *testing.T) {
	p := &provider{s: PoolSnapshot{ActiveRunners: 1, TotalSlots: 2, ReadyTasks: 10, ReadyWork: 2 * time.Second, Runners: []RunnerLoad{{ID: "one", State: "ACTIVE"}}}}
	d := &driver{runners: []ManagedRunner{{ID: "one", ContainerID: "one"}}}
	c := New(Config{}, p, &policy{}, d)
	first, err := c.Reconcile(context.Background())
	if err != nil || len(first.Created) != 0 {
		t.Fatal(first, err)
	}
	second, err := c.Reconcile(context.Background())
	if err != nil || len(second.Created) != 1 {
		t.Fatalf("expected target 2: %+v %v", second, err)
	}
	third, err := c.Reconcile(context.Background())
	if err != nil || len(third.Created) != 0 || third.StartingRunners != 1 {
		t.Fatalf("starting capacity double-created: %+v %v", third, err)
	}
}

func TestFullPoolUsesTotalCapacityAndSoftRiskDoesNotCancel(t *testing.T) {
	p := &provider{s: PoolSnapshot{ActiveRunners: 1, TotalSlots: 2, OccupiedSlots: 2, ReadyTasks: 2, ReadyWork: time.Second, RunningWork: time.Second, RunSLORisk: true, Runners: []RunnerLoad{{ID: "one", State: "ACTIVE", ActiveAttempts: 2}}}}
	d := &driver{runners: []ManagedRunner{{ID: "one", ContainerID: "one"}}}
	policies := &policy{}
	c := New(Config{}, p, policies, d)
	decision, err := c.Reconcile(context.Background())
	if err != nil || decision.EstimatedDrainTime != time.Second || len(decision.Created) != 1 {
		t.Fatalf("bad full capacity estimate: %+v %v", decision, err)
	}
	if len(policies.drained) != 0 || len(d.removed) != 0 {
		t.Fatal("soft SLO risk cancelled or removed active work")
	}
}

func TestScaleDownRequiresIdleWindowAndDrainAcknowledgement(t *testing.T) {
	now := time.Unix(100, 0)
	p := &provider{s: PoolSnapshot{ActiveRunners: 2, TotalSlots: 4, Runners: []RunnerLoad{{ID: "warm", SessionID: "warm-boot", State: "ACTIVE", WarmKeys: 3}, {ID: "cold", SessionID: "cold-boot", State: "ACTIVE"}}}}
	d := &driver{runners: []ManagedRunner{{ID: "warm", ContainerID: "warm"}, {ID: "cold", ContainerID: "cold"}}}
	policies := &policy{}
	c := New(Config{Now: func() time.Time { return now }}, p, policies, d)
	_, _ = c.Reconcile(context.Background())
	now = now.Add(59 * time.Second)
	_, _ = c.Reconcile(context.Background())
	if len(policies.drained) != 0 {
		t.Fatal("scaled down too early")
	}
	now = now.Add(time.Second)
	decision, err := c.Reconcile(context.Background())
	if err != nil || len(decision.Draining) != 1 || decision.Draining[0] != "cold" || len(d.removed) != 0 {
		t.Fatalf("unsafe drain %+v %v", decision, err)
	}
	p.s.ActiveRunners = 1
	p.s.Runners[1].State = "DRAINING"
	p.s.Runners[1].PendingCompletions = 1
	_, _ = c.Reconcile(context.Background())
	if len(d.removed) != 0 {
		t.Fatal("removed runner with pending completion")
	}
	p.s.Runners[1].PendingCompletions = 0
	_, err = c.Reconcile(context.Background())
	if err != nil || len(d.removed) != 1 || d.removed[0] != "cold" {
		t.Fatal(d.removed, err)
	}
}

func TestSnapshotFailureHasNoSideEffects(t *testing.T) {
	p := &provider{err: errors.New("database unavailable")}
	d := &driver{runners: []ManagedRunner{{ID: "stale", ContainerID: "stale", CreatedAt: time.Unix(1, 0)}}}
	_, err := New(Config{}, p, &policy{}, d).Reconcile(context.Background())
	if err == nil || len(d.removed) != 0 || len(d.created) != 0 {
		t.Fatal("snapshot failure caused effects")
	}
}

func TestRunningWorkAloneDoesNotCauseUselessExpansion(t *testing.T) {
	p := &provider{s: PoolSnapshot{ActiveRunners: 1, TotalSlots: 2, OccupiedSlots: 1, RunningWork: time.Minute, RunSLORisk: true}}
	d := &driver{}
	decision, err := New(Config{}, p, &policy{}, d).Reconcile(context.Background())
	if err != nil || decision.DesiredRunners != 1 {
		t.Fatalf("expanded without distributable backlog %+v %v", decision, err)
	}
}
