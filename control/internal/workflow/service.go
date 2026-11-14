// Package workflow owns workflow submission, query, cancellation and input contracts.
package workflow

import (
	"context"
	"fluxforge-control/internal/domain"
	"strings"
)

type Repository interface {
	Submit(context.Context, domain.SubmitCommand) (domain.Run, bool, error)
	GetRun(context.Context, string) (domain.Run, error)
	CancelRun(context.Context, string, string) (domain.Run, error)
	ListRuns(context.Context, string, string, int, string) ([]domain.Run, error)
	ListInvocations(context.Context, string, int, string) ([]domain.Invocation, error)
	ListAttempts(context.Context, string, int, string) ([]domain.Attempt, error)
}
type PayloadValidator interface {
	ValidatePayload(context.Context, *domain.PayloadRef) error
}
type Wakeup interface{ Wake() }
type Service struct {
	repo       Repository
	objects    PayloadValidator
	projection Wakeup
}

func New(repo Repository, objects PayloadValidator, projection Wakeup) *Service {
	return &Service{repo: repo, objects: objects, projection: projection}
}
func (s *Service) Submit(ctx context.Context, c domain.SubmitCommand) (domain.Run, bool, error) {
	if c.WorkflowID == "" || c.Version == "" {
		return domain.Run{}, false, domain.E(domain.Invalid, "workflow ID and version are required")
	}
	if len(c.IdempotencyKey) < 1 || len(c.IdempotencyKey) > 128 {
		return domain.Run{}, false, domain.E(domain.Invalid, "idempotency key must contain 1..128 bytes")
	}
	if c.Priority < 0 || c.Priority > 100 || c.SLOMS < 0 || c.SLOMS > 86400000 {
		return domain.Run{}, false, domain.E(domain.Invalid, "invalid priority or SLO")
	}
	if e := domain.ValidatePayload(c.Input); e != nil {
		return domain.Run{}, false, e
	}
	if s.objects != nil {
		if e := s.objects.ValidatePayload(ctx, &c.Input); e != nil {
			return domain.Run{}, false, e
		}
	}
	if c.TraceParent == "" {
		trace := strings.ReplaceAll(domain.NewID(), "-", "")
		span := strings.ReplaceAll(domain.NewID(), "-", "")[:16]
		c.TraceParent = "00-" + trace + "-" + span + "-01"
	} else {
		// The incoming context is the caller's span, not our asynchronous Run.
		parts := strings.Split(c.TraceParent, "-")
		if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
			return domain.Run{}, false, domain.E(domain.Invalid, "invalid trace context")
		}
		c.ParentSpanID = parts[2]
		parts[2] = strings.ReplaceAll(domain.NewID(), "-", "")[:16]
		c.TraceParent = strings.Join(parts, "-")
	}
	r, duplicate, e := s.repo.Submit(ctx, c)
	if e == nil && s.projection != nil {
		s.projection.Wake()
	}
	return r, duplicate, e
}
func (s *Service) GetRun(ctx context.Context, id string) (domain.Run, error) {
	return s.repo.GetRun(ctx, id)
}
func (s *Service) CancelRun(ctx context.Context, id, reason string) (domain.Run, error) {
	if reason == "" {
		reason = "cancelled by client"
	}
	if len(reason) > 4096 {
		return domain.Run{}, domain.E(domain.Invalid, "cancellation reason is too long")
	}
	r, e := s.repo.CancelRun(ctx, id, reason)
	if e == nil && s.projection != nil {
		s.projection.Wake()
	}
	return r, e
}
func (s *Service) ListRuns(ctx context.Context, workflow, state string, limit int, after string) ([]domain.Run, error) {
	return s.repo.ListRuns(ctx, workflow, state, limit, after)
}
func (s *Service) ListInvocations(ctx context.Context, id string, limit int, after string) ([]domain.Invocation, error) {
	if _, e := s.repo.GetRun(ctx, id); e != nil {
		return nil, e
	}
	return s.repo.ListInvocations(ctx, id, limit, after)
}
func (s *Service) ListAttempts(ctx context.Context, id string, limit int, after string) ([]domain.Attempt, error) {
	return s.repo.ListAttempts(ctx, id, limit, after)
}
