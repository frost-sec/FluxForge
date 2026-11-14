// Package recovery reconciles persistent state after crashes and lease expiry.
package recovery

import (
	"context"
	"log/slog"
	"time"
)

type Repository interface{ Sweep(context.Context) error }
type Projection interface {
	Rebuild(context.Context) error
	Sync(context.Context) error
	Wake()
}
type Service struct {
	repo       Repository
	projection Projection
}

func New(repo Repository, projection Projection) *Service {
	return &Service{repo: repo, projection: projection}
}
func (s *Service) Recover(ctx context.Context) error {
	if e := s.repo.Sweep(ctx); e != nil {
		return e
	}
	return s.projection.Rebuild(ctx)
}
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if e := s.repo.Sweep(ctx); e != nil && ctx.Err() == nil {
			slog.Error("state reconciliation failed", "error", e)
		}
		s.projection.Wake()
	}
}
