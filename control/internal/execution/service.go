// Package execution coordinates queue reservations with authoritative leases.
package execution

import (
	"context"
	"errors"
	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/scheduler"
	"time"
)

type Repository interface {
	Claim(context.Context, domain.ClaimCommand) (domain.ClaimResult, error)
	ReplayClaim(context.Context, domain.ClaimCommand) (domain.ClaimResult, bool, error)
	GetSession(context.Context, string) (domain.RunnerSession, error)
	Capacity(context.Context, string) (domain.Capacity, error)
	Started(context.Context, string, string, string) (domain.Attempt, error)
	ExecutionFinished(context.Context, string, string, string, int64) (domain.Attempt, error)
	Renew(context.Context, string, string, string) (domain.Attempt, error)
	Complete(context.Context, domain.Completion) (domain.CompletionResult, error)
	Release(context.Context, string, string, string, string) (domain.CompletionResult, error)
	LookupReady(context.Context, string) ([]domain.ReadyTask, error)
	GetAssignment(context.Context, string) (domain.Assignment, error)
}
type Projection interface {
	Wake()
	Sync(context.Context) error
}
type PayloadValidator interface {
	ValidatePayload(context.Context, *domain.PayloadRef) error
}
type Hooks struct {
	Started   func(context.Context, domain.Assignment)
	Completed func(context.Context, domain.Assignment, domain.Completion)
}
type Service struct {
	repo       Repository
	queue      *scheduler.Scheduler
	projection Projection
	objects    PayloadValidator
	hooks      Hooks
}

func New(repo Repository, queue *scheduler.Scheduler, projection Projection, objects PayloadValidator, hooks Hooks) *Service {
	return &Service{repo: repo, queue: queue, projection: projection, objects: objects, hooks: hooks}
}

func (s *Service) RefreshRunner(ctx context.Context, id string) (domain.RunnerSession, error) {
	r, e := s.repo.GetSession(ctx, id)
	if e != nil {
		return r, e
	}
	used, e := s.repo.Capacity(ctx, id)
	if e != nil {
		return r, e
	}
	slots := r.Slots - used.Slots
	if slots < 0 {
		slots = 0
	}
	s.queue.UpsertRunner(scheduler.Runner{ID: r.RunnerID, SessionID: r.ID, Active: r.State == "ACTIVE" && r.Policy == "ACTIVE", Runtimes: r.Runtimes, CachedKeys: r.CachedKeys, AvailableSlots: int(slots), AvailableCPUMillis: r.CPUMillis - used.CPUMillis, AvailableMemoryBytes: r.MemoryBytes - used.MemoryBytes})
	return r, nil
}

func (s *Service) Acquire(ctx context.Context, c domain.ClaimCommand) (domain.ClaimResult, error) {
	if c.SessionID == "" || c.RequestID == "" || len(c.RequestID) > 128 || c.RequestedSlots < 1 || c.RequestedSlots > 128 || c.WaitTimeoutMS < 0 || c.WaitTimeoutMS > 10000 {
		return domain.ClaimResult{}, domain.E(domain.Invalid, "invalid acquire request")
	}
	if r, found, e := s.repo.ReplayClaim(ctx, c); e != nil || found {
		return r, e
	}
	runner, e := s.RefreshRunner(ctx, c.SessionID)
	if e != nil {
		return domain.ClaimResult{}, e
	}
	if runner.State != "ACTIVE" {
		return domain.ClaimResult{}, domain.E(domain.Stale, "runner session is not active")
	}
	waitUntil := time.Now().Add(time.Duration(c.WaitTimeoutMS) * time.Millisecond)
	for {
		reservation, e := s.queue.Reserve(runner.RunnerID, int(c.RequestedSlots))
		if e != nil && !errors.Is(e, scheduler.ErrRunnerUnavailable) {
			return domain.ClaimResult{}, e
		}
		if len(reservation.Tasks) > 0 {
			c.Candidates = nil
			for _, task := range reservation.Tasks {
				c.Candidates = append(c.Candidates, domain.ReadyTask{InvocationID: task.ID, RunID: task.RunID, Generation: task.Version})
			}
			result, e := s.repo.Claim(ctx, c)
			if e != nil {
				_ = s.queue.Abort(reservation.ID)
				return result, e
			}
			claimed := []string{}
			for _, a := range result.Assignments {
				claimed = append(claimed, a.Invocation.ID)
			}
			if e = s.queue.Commit(reservation.ID, claimed); e != nil {
				return result, e
			}
			for _, task := range reservation.Tasks {
				ready, e := s.repo.LookupReady(ctx, task.ID)
				if e == nil && len(ready) == 0 {
					s.queue.Remove(task.ID, task.Version)
				}
			}
			if s.projection != nil {
				s.projection.Wake()
			}
			return result, nil
		}
		if reservation.ID != "" {
			_ = s.queue.Abort(reservation.ID)
		}
		if runner.Policy != "ACTIVE" || c.WaitTimeoutMS == 0 || !time.Now().Before(waitUntil) {
			c.Candidates = nil
			return s.repo.Claim(ctx, c)
		}
		if e = s.queue.Wait(ctx, runner.RunnerID, time.Until(waitUntil)); e != nil {
			if ctx.Err() != nil {
				return domain.ClaimResult{}, ctx.Err()
			}
			if !errors.Is(e, context.DeadlineExceeded) {
				return domain.ClaimResult{}, e
			}
		}
		runner, e = s.RefreshRunner(ctx, c.SessionID)
		if e != nil {
			return domain.ClaimResult{}, e
		}
	}
}

func (s *Service) Started(ctx context.Context, session, attempt, token string) (domain.Attempt, error) {
	a, e := s.repo.Started(ctx, session, attempt, token)
	if e == nil && s.hooks.Started != nil {
		assignment, loadErr := s.repo.GetAssignment(ctx, attempt)
		if loadErr == nil {
			s.hooks.Started(ctx, assignment)
		}
	}
	return a, e
}
func (s *Service) ExecutionFinished(ctx context.Context, session, attempt, token string, duration int64) (domain.Attempt, error) {
	if duration < 0 {
		return domain.Attempt{}, domain.E(domain.Invalid, "duration cannot be negative")
	}
	return s.repo.ExecutionFinished(ctx, session, attempt, token, duration)
}
func (s *Service) Renew(ctx context.Context, session, attempt, token string) (domain.Attempt, error) {
	return s.repo.Renew(ctx, session, attempt, token)
}

func (s *Service) Complete(ctx context.Context, c domain.Completion) (domain.CompletionResult, error) {
	if len(c.Stages) > 16 {
		return domain.CompletionResult{}, domain.E(domain.Invalid, "too many stage timings")
	}
	for _, v := range c.Stages {
		if len(v.Name) > 64 || v.OffsetMS < 0 || v.DurationMS < 0 || v.OffsetMS > 86400000 || v.DurationMS > 86400000 {
			return domain.CompletionResult{}, domain.E(domain.Invalid, "invalid stage timing")
		}
	}
	allowed := map[string]bool{"SUCCEEDED": true, "USER_FAILED": true, "TIMEOUT": true, "OOM": true, "CRASHED": true, "CANCELLED": true, "RESOURCE_ERROR": true, "INFRA_ERROR": true}
	if !allowed[c.Outcome] || c.DurationMS < 0 || len(c.Error) > 4096 {
		return domain.CompletionResult{}, domain.E(domain.Invalid, "invalid completion outcome, duration, or error")
	}
	if c.Outcome == "SUCCEEDED" {
		if c.Output == nil {
			return domain.CompletionResult{}, domain.E(domain.Invalid, "successful completion requires JSON output")
		}
		if e := domain.ValidatePayload(*c.Output); e != nil {
			return domain.CompletionResult{}, e
		}
		if s.objects != nil {
			if e := s.objects.ValidatePayload(ctx, c.Output); e != nil {
				return domain.CompletionResult{}, e
			}
		}
	}
	r, e := s.repo.Complete(ctx, c)
	if e == nil && !r.Duplicate {
		if s.projection != nil {
			s.projection.Wake()
		}
		if s.hooks.Completed != nil {
			a, loadErr := s.repo.GetAssignment(ctx, c.AttemptID)
			if loadErr == nil {
				s.hooks.Completed(ctx, a, c)
			}
		}
	}
	return r, e
}
func (s *Service) Release(ctx context.Context, session, attempt, token, reason string) (domain.CompletionResult, error) {
	if len(reason) > 4096 {
		return domain.CompletionResult{}, domain.E(domain.Invalid, "release reason is too long")
	}
	r, e := s.repo.Release(ctx, session, attempt, token, reason)
	if e == nil && s.projection != nil {
		s.projection.Wake()
	}
	return r, e
}
