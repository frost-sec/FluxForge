package postgres

import (
	"context"
	"errors"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) InitPolicies(ctx context.Context) error {
	_, e := s.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS policies(name text PRIMARY KEY,version bigint NOT NULL,data jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())`)
	return e
}
func (s *Store) GetPolicy(ctx context.Context, name string) (int64, []byte, error) {
	var v int64
	var b []byte
	e := s.Pool.QueryRow(ctx, `SELECT version,data FROM policies WHERE name=$1`, name).Scan(&v, &b)
	if e != nil {
		var exists bool
		if x := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM policies WHERE name=$1)`, name).Scan(&exists); x == nil && !exists {
			return 0, []byte(`{}`), nil
		}
	}
	return v, b, e
}
func (s *Store) SetConfigPolicy(ctx context.Context, name string, expected int64, b []byte) (int64, error) {
	var v int64
	var e error
	if expected == 0 {
		e = s.Pool.QueryRow(ctx, `INSERT INTO policies(name,version,data) VALUES($1,1,$2) ON CONFLICT DO NOTHING RETURNING version`, name, b).Scan(&v)
	} else {
		e = s.Pool.QueryRow(ctx, `UPDATE policies SET version=version+1,data=$3,updated_at=clock_timestamp() WHERE name=$1 AND version=$2 RETURNING version`, name, expected, b).Scan(&v)
	}
	if errors.Is(e, pgx.ErrNoRows) {
		return 0, domain.E(domain.Conflict, "policy version changed; reload before updating")
	}
	return v, e
}
