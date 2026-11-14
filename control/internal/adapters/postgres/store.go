// Package postgres implements the authoritative persistence and transaction layer.
package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	Pool             *pgxpool.Pool
	LeaseDuration    time.Duration
	PrepareTimeout   time.Duration
	UploadTimeout    time.Duration
	HeartbeatTimeout time.Duration
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{Pool: pool, LeaseDuration: 15 * time.Second, PrepareTimeout: 30 * time.Second, UploadTimeout: 30 * time.Second, HeartbeatTimeout: 12 * time.Second}
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(721465981)`); err != nil {
		return err
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		b, e := migrations.ReadFile("migrations/" + entry.Name())
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, string(b)); e != nil {
			return fmt.Errorf("migration %s: %w", entry.Name(), e)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) Tx(ctx context.Context, fn func(pgx.Tx) error) error {
	for i := 0; i < 4; i++ {
		tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
		var pe *pgconn.PgError
		if err != nil && errors.As(err, &pe) && (pe.Code == "40P01" || pe.Code == "40001") {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(i+1) * 10 * time.Millisecond):
			}
			continue
		}
		return err
	}
	return domain.E(domain.Unavailable, "database transaction retry limit exceeded")
}

func encoded(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func missing(err error, what string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.E(domain.NotFound, "%s not found", what)
	}
	return err
}

func lockRun(ctx context.Context, tx pgx.Tx, id string, allowCancelling bool) (string, error) {
	var state string
	err := tx.QueryRow(ctx, `SELECT state FROM runs WHERE id=$1 FOR SHARE`, id).Scan(&state)
	if err != nil {
		return state, missing(err, "run")
	}
	if !domain.ActiveRun(state) && !(allowCancelling && state == "CANCELLING") {
		return state, domain.E(domain.Stale, "run is %s", state)
	}
	return state, nil
}

func lockSession(ctx context.Context, tx pgx.Tx, id string, exclusive bool) (domain.RunnerSession, error) {
	query := sessionSelect + ` WHERE id=$1 FOR SHARE`
	if exclusive {
		query = sessionSelect + ` WHERE id=$1 FOR UPDATE`
	}
	session, err := scanSession(tx.QueryRow(ctx, query, id))
	if err != nil {
		return session, missing(err, "runner session")
	}
	if session.State != "ACTIVE" || time.Since(session.LastSeen) > time.Minute {
		return session, domain.E(domain.Stale, "runner session is inactive")
	}
	return session, nil
}

func outbox(ctx context.Context, tx pgx.Tx, kind, runID, invID string, generation int64) error {
	if kind == "FINALIZE" {
		invID = ""
		generation = 0
	}
	_, err := tx.Exec(ctx, `INSERT INTO scheduler_outbox(kind,run_id,invocation_id,generation) VALUES($1,$2,NULLIF($3,'')::uuid,$4) ON CONFLICT DO NOTHING`, kind, runID, invID, generation)
	return err
}

type rowScanner interface{ Scan(...any) error }
