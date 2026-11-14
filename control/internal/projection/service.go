// Package projection reconstructs the disposable scheduler index from durable events.
package projection

import (
	"context"
	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/scheduler"
	"fluxforge-control/internal/telemetry"
	"log/slog"
	"sync"
	"time"
)

type Repository interface {
	ListReady(context.Context) ([]domain.ReadyTask, error)
	LookupReady(context.Context, string) ([]domain.ReadyTask, error)
	ListOutbox(context.Context, int) ([]domain.Event, error)
	MarkDelivered(context.Context, int64) error
	Finalize(context.Context, string) (domain.Run, error)
	GetRun(context.Context, string) (domain.Run, error)
	GetInvocation(context.Context, string) (domain.Invocation, error)
	GetFunction(context.Context, string, string) (domain.FunctionSpec, error)
}
type Hooks struct {
	Invocation func(context.Context, domain.Run, domain.Invocation, domain.FunctionSpec)
	Run        func(context.Context, domain.Run)
}
type Service struct {
	repo     Repository
	queue    *scheduler.Scheduler
	metrics  *telemetry.Aggregator
	hooks    Hooks
	mu       sync.Mutex
	wake     chan struct{}
	arrivals map[string]bool
}

func New(repo Repository, queue *scheduler.Scheduler, metrics *telemetry.Aggregator, hooks Hooks) *Service {
	return &Service{repo: repo, queue: queue, metrics: metrics, hooks: hooks, wake: make(chan struct{}, 1), arrivals: map[string]bool{}}
}
func Task(v domain.ReadyTask) scheduler.Task {
	return scheduler.Task{ID: v.InvocationID, RunID: v.RunID, Version: v.Generation, FunctionID: v.FunctionID, FunctionVersion: v.FunctionVersion, AffinityKey: v.FunctionKey, Runtime: v.Runtime, Priority: int(v.Priority), ReadyAt: v.ReadyAt, Deadline: v.Deadline, CPUMillis: v.CPUMillis, MemoryBytes: v.MemoryBytes, EstimatedDuration: v.EstimatedDuration}
}
func (s *Service) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *Service) Rebuild(ctx context.Context) error {
	tasks, e := s.repo.ListReady(ctx)
	if e != nil {
		return e
	}
	for _, task := range tasks {
		if e = s.queue.ReconcileReady(Task(task)); e != nil {
			return e
		}
	}
	return s.Sync(ctx)
}
func (s *Service) Sync(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for batch := 0; batch < 20; batch++ {
		events, e := s.repo.ListOutbox(ctx, 100)
		if e != nil {
			return e
		}
		if len(events) == 0 {
			return nil
		}
		for _, event := range events {
			switch event.Kind {
			case "READY":
				tasks, e := s.repo.LookupReady(ctx, event.InvocationID)
				if e != nil {
					return e
				}
				for _, task := range tasks {
					if e = s.queue.Enqueue(Task(task)); e != nil {
						return e
					}
					if s.metrics != nil && !s.arrivals[task.InvocationID] {
						v, e := s.repo.GetInvocation(ctx, task.InvocationID)
						if e != nil {
							return e
						}
						if v.AttemptNo == 0 {
							s.metrics.ObserveArrival(telemetry.FunctionKey{FunctionID: task.FunctionID, Version: task.FunctionVersion, RuntimeDigest: task.RuntimeDigest})
							s.arrivals[task.InvocationID] = true
						}
					}
				}
			case "FINALIZE":
				if _, e = s.repo.Finalize(ctx, event.RunID); e != nil {
					return e
				}
			case "INVOCATION_FINISHED":
				v, e := s.repo.GetInvocation(ctx, event.InvocationID)
				if e != nil {
					return e
				}
				s.queue.Remove(v.ID, v.Generation)
				delete(s.arrivals, v.ID)
				r, e := s.repo.GetRun(ctx, v.RunID)
				if e != nil {
					return e
				}
				f, e := s.repo.GetFunction(ctx, v.Function.ID, v.Function.Version)
				if e != nil {
					return e
				}
				if s.metrics != nil && v.FirstReadyAt != nil && v.FinishedAt != nil {
					s.metrics.ObserveInvocation(telemetry.FunctionKey{FunctionID: f.ID, Version: f.Version, RuntimeDigest: f.RuntimeDigest}, v.FinishedAt.Sub(*v.FirstReadyAt), v.State == "SUCCEEDED")
				}
				if s.hooks.Invocation != nil {
					s.hooks.Invocation(ctx, r, v, f)
				}
			case "RUN_FINISHED":
				r, e := s.repo.GetRun(ctx, event.RunID)
				if e != nil {
					return e
				}
				if s.metrics != nil && r.FinishedAt != nil {
					s.metrics.ObserveRun(r.FinishedAt.Sub(r.CreatedAt), r.State == "SUCCEEDED")
				}
				if s.hooks.Run != nil {
					s.hooks.Run(ctx, r)
				}
			}
			if e = s.repo.MarkDelivered(ctx, event.ID); e != nil {
				return e
			}
			if event.Kind == "INVOCATION_FINISHED" {
				s.queue.Forget(event.InvocationID)
			}
		}
	}
	s.Wake()
	return nil
}
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	reconcile := time.NewTicker(5 * time.Second)
	defer reconcile.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		case <-reconcile.C:
			if e := s.Rebuild(ctx); e != nil && ctx.Err() == nil {
				slog.Error("scheduler index reconciliation failed", "error", e)
			}
			continue
		}
		if e := s.Sync(ctx); e != nil && ctx.Err() == nil {
			slog.Error("scheduler projection failed", "error", e)
		}
	}
}
