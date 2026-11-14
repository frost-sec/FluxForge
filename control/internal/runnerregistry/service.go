// Package runnerregistry owns runner identity, boot fencing and heartbeat policy.
package runnerregistry

import (
	"context"
	"fluxforge-control/internal/domain"
)

type Repository interface {
	Register(context.Context, domain.RunnerSession) (domain.RunnerSession, error)
	Heartbeat(context.Context, domain.RunnerSession) (domain.RunnerSession, []string, error)
	GetSession(context.Context, string) (domain.RunnerSession, error)
	ListSessions(context.Context) ([]domain.RunnerSession, error)
	SetPolicy(context.Context, string, string) (domain.RunnerSession, error)
	Capacity(context.Context, string) (domain.Capacity, error)
}
type Service struct{ repo Repository }

func New(repo Repository) *Service { return &Service{repo: repo} }
func (s *Service) Register(ctx context.Context, r domain.RunnerSession) (domain.RunnerSession, error) {
	if len(r.RunnerID) < 1 || len(r.RunnerID) > 128 || len(r.BootID) < 1 || len(r.BootID) > 128 {
		return r, domain.E(domain.Invalid, "runner ID and unique boot ID are required")
	}
	if r.Slots < 1 || r.Slots > 128 || r.MemoryBytes < 16*1024*1024 {
		return r, domain.E(domain.Invalid, "invalid runner capacity")
	}
	if r.CPUMillis == 0 {
		r.CPUMillis = int64(r.Slots) * 1000
	}
	if r.CPUMillis < 1 {
		return r, domain.E(domain.Invalid, "runner CPU capacity must be positive")
	}
	for _, runtime := range r.Runtimes {
		if runtime != "cpp20" {
			return r, domain.E(domain.Invalid, "only cpp20 runners are supported")
		}
	}
	return s.repo.Register(ctx, r)
}
func (s *Service) Heartbeat(ctx context.Context, r domain.RunnerSession) (domain.RunnerSession, []string, error) {
	current, e := s.repo.GetSession(ctx, r.ID)
	if e != nil {
		return r, nil, e
	}
	if current.RunnerID != r.RunnerID {
		return r, nil, domain.E(domain.Stale, "session belongs to another runner")
	}
	if len(r.CachedKeys) > 10000 {
		return r, nil, domain.E(domain.Invalid, "too many cached affinity keys")
	}
	r.BootID = current.BootID
	return s.repo.Heartbeat(ctx, r)
}
func (s *Service) GetSession(ctx context.Context, id string) (domain.RunnerSession, error) {
	return s.repo.GetSession(ctx, id)
}
func (s *Service) ListSessions(ctx context.Context) ([]domain.RunnerSession, error) {
	return s.repo.ListSessions(ctx)
}
func (s *Service) Capacity(ctx context.Context, id string) (domain.Capacity, error) {
	return s.repo.Capacity(ctx, id)
}
func (s *Service) SetPolicy(ctx context.Context, runnerID, policy string) (domain.RunnerSession, error) {
	if policy != "ACTIVE" && policy != "DRAINING" && policy != "OFFLINE" {
		return domain.RunnerSession{}, domain.E(domain.Invalid, "unknown runner policy")
	}
	sessions, e := s.repo.ListSessions(ctx)
	if e != nil {
		return domain.RunnerSession{}, e
	}
	for _, r := range sessions {
		if r.RunnerID == runnerID && r.State == "ACTIVE" {
			return s.repo.SetPolicy(ctx, r.ID, policy)
		}
	}
	return domain.RunnerSession{}, domain.E(domain.NotFound, "active runner not found")
}
func (s *Service) SetDraining(ctx context.Context, runnerID, sessionID string) error {
	r, e := s.repo.GetSession(ctx, sessionID)
	if e != nil {
		return e
	}
	if r.RunnerID != runnerID {
		return domain.E(domain.Stale, "session belongs to another runner")
	}
	_, e = s.repo.SetPolicy(ctx, sessionID, "DRAINING")
	return e
}
