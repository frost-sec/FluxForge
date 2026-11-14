package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func active(id string) Runner {
	return Runner{ID: id, SessionID: id + "-boot", Active: true, Runtimes: []string{"cpp20"}, AvailableSlots: 4, AvailableCPUMillis: 4000, AvailableMemoryBytes: 1024}
}
func task(id string) Task {
	return Task{ID: id, Version: 1, Runtime: "cpp20", AffinityKey: "echo@1", Priority: 10, CPUMillis: 100, MemoryBytes: 100, EstimatedDuration: 10 * time.Millisecond}
}

func TestAffinityDelayAndOfflineStealing(t *testing.T) {
	now := time.Unix(100, 0)
	s := New(Config{Now: func() time.Time { return now }, AffinityDelay: 20 * time.Millisecond})
	a, b := active("a"), active("b")
	a.CachedKeys = []string{"echo@1"}
	s.UpsertRunner(a)
	s.UpsertRunner(b)
	if err := s.Enqueue(task("one")); err != nil {
		t.Fatal(err)
	}
	r, err := s.Reserve("b", 1)
	if err != nil || len(r.Tasks) != 0 {
		t.Fatalf("stole during grace: %+v %v", r, err)
	}
	now = now.Add(21 * time.Millisecond)
	r, err = s.Reserve("b", 1)
	if err != nil || len(r.Tasks) != 1 {
		t.Fatalf("did not steal after grace: %+v %v", r, err)
	}
	if err = s.Abort(r.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(-20 * time.Millisecond)
	s.RemoveRunner("a")
	r, err = s.Reserve("b", 1)
	if err != nil || len(r.Tasks) != 1 {
		t.Fatalf("offline owner blocked steal: %+v %v", r, err)
	}
}

func TestResourcesIdempotencyAndGeneration(t *testing.T) {
	s := New(Config{})
	a := active("a")
	a.AvailableSlots = 2
	a.AvailableMemoryBytes = 150
	s.UpsertRunner(a)
	if err := s.Enqueue(task("one")); err != nil {
		t.Fatal(err)
	}
	_ = s.Enqueue(task("one"))
	_ = s.Enqueue(task("two"))
	r, err := s.Reserve("a", 2)
	if err != nil || len(r.Tasks) != 1 {
		t.Fatalf("overcommitted memory: %+v %v", r, err)
	}
	r2, _ := s.Reserve("a", 2)
	if len(r2.Tasks) != 0 {
		t.Fatal("reserved memory was not charged")
	}
	newTask := task(r.Tasks[0].ID)
	newTask.Version = 2
	_ = s.Enqueue(newTask)
	if err = s.Abort(r.ID); err != nil {
		t.Fatal(err)
	}
	r3, _ := s.Reserve("a", 2)
	if len(r3.Tasks) != 1 {
		t.Fatalf("expected one task: %+v", r3)
	}
	if r3.Tasks[0].ID == newTask.ID && r3.Tasks[0].Version != 2 {
		t.Fatal("old version resurrected")
	}
	_ = s.Commit(r3.ID, []string{r3.Tasks[0].ID})
	if err = s.Abort(r3.ID); err != ErrReservationUnknown {
		t.Fatalf("double resolve accepted: %v", err)
	}
}

func TestPriorityAgingAndGlobalFallback(t *testing.T) {
	now := time.Unix(1000, 0)
	s := New(Config{Now: func() time.Time { return now }, AgingInterval: time.Second})
	old := task("old")
	old.Priority = 0
	old.ReadyAt = now.Add(-101 * time.Second)
	_ = s.Enqueue(old)
	newer := task("new")
	newer.Priority = 100
	newer.ReadyAt = now
	_ = s.Enqueue(newer)
	s.UpsertRunner(active("a"))
	r, err := s.Reserve("a", 1)
	if err != nil || len(r.Tasks) != 1 || r.Tasks[0].ID != "old" {
		t.Fatalf("old task starved: %+v %v", r, err)
	}
}

func TestConcurrentReservationsNeverDuplicate(t *testing.T) {
	s := New(Config{})
	r := active("runner")
	r.AvailableSlots = 100
	r.AvailableCPUMillis = 100000
	r.AvailableMemoryBytes = 100000
	s.UpsertRunner(r)
	for i := 0; i < 100; i++ {
		_ = s.Enqueue(task(fmt.Sprint(i)))
	}
	var wg sync.WaitGroup
	seen := make(map[string]bool)
	var mu sync.Mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservation, err := s.Reserve("runner", 10)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, item := range reservation.Tasks {
				if seen[item.ID] {
					t.Errorf("duplicate %s", item.ID)
				}
				seen[item.ID] = true
			}
		}()
	}
	wg.Wait()
	if len(seen) != 100 {
		t.Fatalf("got %d tasks", len(seen))
	}
	if s.Snapshot().ReservedTasks != 100 {
		t.Fatal("invalid reserved snapshot")
	}
}

func TestWaitWakesOnEnqueueAndCancellation(t *testing.T) {
	s := New(Config{})
	s.UpsertRunner(active("a"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Wait(ctx, "a", time.Second) }()
	time.Sleep(10 * time.Millisecond)
	_ = s.Enqueue(task("wake"))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("lost enqueue notification")
	}
	s.Remove("wake", 1)
	cancel()
	if err := s.Wait(ctx, "a", time.Second); err != context.Canceled {
		t.Fatalf("got %v", err)
	}
}

func TestReservationExpiryReturnsCandidate(t *testing.T) {
	now := time.Unix(100, 0)
	s := New(Config{Now: func() time.Time { return now }, ReservationTTL: time.Second})
	s.UpsertRunner(active("a"))
	_ = s.Enqueue(task("one"))
	r, _ := s.Reserve("a", 1)
	now = now.Add(2 * time.Second)
	s.ReapExpired()
	next, _ := s.Reserve("a", 1)
	if len(next.Tasks) != 1 || next.Tasks[0].ID != "one" {
		t.Fatal("expired candidate lost")
	}
	if err := s.Commit(r.ID, []string{"one"}); err != ErrReservationUnknown {
		t.Fatalf("late reservation commit: %v", err)
	}
}
