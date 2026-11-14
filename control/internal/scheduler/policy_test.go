package scheduler

import (
	"sync"
	"testing"
	"time"
)

func TestPolicyUpdateAppliesDelayToExistingEntries(t *testing.T) {
	now := time.Unix(100, 0)
	s := New(Config{Now: func() time.Time { return now }, AffinityDelay: time.Second})
	owner, borrower := active("owner"), active("borrower")
	owner.CachedKeys = []string{"echo@1"}
	s.UpsertRunner(owner)
	s.UpsertRunner(borrower)
	v := task("ready")
	v.ReadyAt = now.Add(-50 * time.Millisecond)
	if err := s.Enqueue(v); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Reserve("borrower", 1); len(r.Tasks) != 0 {
		t.Fatal("stole before initial delay")
	}
	policy := s.Config()
	policy.AffinityDelay = time.Millisecond
	policy.Now = nil // Update preserves the injected clock.
	if err := s.UpdateConfig(policy); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Reserve("borrower", 1); len(r.Tasks) != 1 {
		t.Fatal("new delay did not affect already queued work")
	}
	policy.MaxWait = -time.Second
	if err := s.UpdateConfig(policy); err == nil {
		t.Fatal("accepted invalid policy")
	}
}

func TestOfflineRecoveryWorksWithBalancingDisabled(t *testing.T) {
	s := New(Config{DisableStealing: true})
	owner, borrower := active("owner"), active("borrower")
	owner.CachedKeys = []string{"echo@1"}
	s.UpsertRunner(owner)
	s.UpsertRunner(borrower)
	if err := s.Enqueue(task("ready")); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Reserve("borrower", 1); len(r.Tasks) != 0 {
		t.Fatal("load balancing was not disabled")
	}
	s.RemoveRunner("owner")
	r, err := s.Reserve("borrower", 1)
	if err != nil || len(r.Tasks) != 1 {
		t.Fatalf("offline owner stranded work: %+v %v", r, err)
	}
	if err := s.Abort(r.ID); err != nil {
		t.Fatal(err)
	}
	r, err = s.Reserve("borrower", 1)
	if err != nil || len(r.Tasks) != 1 {
		t.Fatalf("abort stranded recovered work: %+v %v", r, err)
	}
}

func TestConcurrentPolicyUpdatesAndReservations(t *testing.T) {
	s := New(Config{})
	s.UpsertRunner(active("runner"))
	if err := s.Enqueue(task("one")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p := s.Config()
			p.AffinityDelay = time.Duration(i+1) * time.Millisecond
			if err := s.UpdateConfig(p); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			r, e := s.Reserve("runner", 1)
			if e != nil {
				t.Error(e)
				return
			}
			if r.ID != "" {
				if e = s.Abort(r.ID); e != nil {
					t.Error(e)
				}
			}
			s.Snapshot()
		}
	}()
	wg.Wait()
}
