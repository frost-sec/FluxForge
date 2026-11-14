package postgres

import (
	"context"
	"encoding/json"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
	"time"
)

const sessionSelect = `SELECT id::text,runner_id,boot_id,policy,state,slots,memory_bytes,cpu_millis,available_slots,used_memory_bytes,cached_keys,last_seen,container_id,runtimes,architecture,runtime_digest,active_attempts,pending_completions,drain_acknowledged FROM runner_sessions`

func scanSession(row rowScanner) (domain.RunnerSession, error) {
	var r domain.RunnerSession
	var keys, runtimes []byte
	err := row.Scan(&r.ID, &r.RunnerID, &r.BootID, &r.Policy, &r.State, &r.Slots, &r.MemoryBytes, &r.CPUMillis, &r.AvailableSlots, &r.UsedMemoryBytes, &keys, &r.LastSeen, &r.ContainerID, &runtimes, &r.Architecture, &r.RuntimeDigest, &r.ActiveAttempts, &r.PendingCompletions, &r.DrainAcknowledged)
	if err == nil {
		err = json.Unmarshal(keys, &r.CachedKeys)
	}
	if err == nil {
		err = json.Unmarshal(runtimes, &r.Runtimes)
	}
	return r, err
}

func (s *Store) Register(ctx context.Context, r domain.RunnerSession) (domain.RunnerSession, error) {
	var result domain.RunnerSession
	err := s.Tx(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,9))`, r.RunnerID); e != nil {
			return e
		}
		existing, e := scanSession(tx.QueryRow(ctx, sessionSelect+` WHERE runner_id=$1 AND boot_id=$2`, r.RunnerID, r.BootID))
		if e == nil {
			if existing.State != "ACTIVE" {
				return domain.E(domain.Stale, "runner boot has been retired")
			}
			if _, e = tx.Exec(ctx, `UPDATE runner_sessions SET last_seen=clock_timestamp() WHERE id=$1`, existing.ID); e != nil {
				return e
			}
			result = existing
			return nil
		}
		if e != pgx.ErrNoRows {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE runner_sessions SET state='RETIRED',policy='OFFLINE' WHERE runner_id=$1 AND state='ACTIVE'`, r.RunnerID); e != nil {
			return e
		}
		if r.ID == "" {
			r.ID = domain.NewID()
		}
		if len(r.Runtimes) == 0 {
			r.Runtimes = []string{"cpp20"}
		}
		if r.Architecture == "" {
			r.Architecture = "linux/amd64"
		}
		if r.RuntimeDigest == "" {
			r.RuntimeDigest = "cpp20-linux-amd64-v1"
		}
		if _, e = tx.Exec(ctx, `INSERT INTO runner_sessions(id,runner_id,boot_id,state,policy,slots,memory_bytes,cpu_millis,available_slots,cached_keys,container_id,runtimes,architecture,runtime_digest) VALUES($1,$2,$3,'ACTIVE','ACTIVE',$4,$5,$6,$4,'[]',$7,$8,$9,$10)`, r.ID, r.RunnerID, r.BootID, r.Slots, r.MemoryBytes, r.CPUMillis, r.ContainerID, encoded(r.Runtimes), r.Architecture, r.RuntimeDigest); e != nil {
			return e
		}
		result, e = scanSession(tx.QueryRow(ctx, sessionSelect+` WHERE id=$1`, r.ID))
		return e
	})
	return result, err
}

func (s *Store) Heartbeat(ctx context.Context, r domain.RunnerSession) (domain.RunnerSession, []string, error) {
	var result domain.RunnerSession
	cancelIDs := []string{}
	err := s.Tx(ctx, func(tx pgx.Tx) error {
		var state string
		if e := tx.QueryRow(ctx, `SELECT state FROM runner_sessions WHERE id=$1 AND runner_id=$2 AND boot_id=$3 FOR UPDATE`, r.ID, r.RunnerID, r.BootID).Scan(&state); e != nil {
			return missing(e, "runner session")
		}
		if state != "ACTIVE" {
			return domain.E(domain.Stale, "runner session is not active")
		}
		keys := r.CachedKeys
		if keys == nil {
			keys = []string{}
		}
		if _, e := tx.Exec(ctx, `UPDATE runner_sessions SET last_seen=clock_timestamp(),available_slots=GREATEST(0,LEAST(slots,$2)),used_memory_bytes=GREATEST(0,$3),cached_keys=$4,active_attempts=GREATEST(0,$5),pending_completions=GREATEST(0,$6),drain_acknowledged=(policy='DRAINING' AND $7='DRAINING' AND $5=0 AND $6=0) WHERE id=$1`, r.ID, r.AvailableSlots, r.UsedMemoryBytes, encoded(keys), r.ActiveAttempts, r.PendingCompletions, r.ObservedPolicy); e != nil {
			return e
		}
		var e error
		result, e = scanSession(tx.QueryRow(ctx, sessionSelect+` WHERE id=$1`, r.ID))
		if e != nil {
			return e
		}
		rows, e := tx.Query(ctx, `SELECT a.id::text FROM attempts a JOIN runs r ON r.id=a.run_id WHERE a.session_id=$1 AND a.state IN ('LEASED','RUNNING','COMPLETING') AND (r.state NOT IN ('PENDING','RUNNING') OR a.lease_expires_at<=clock_timestamp()) ORDER BY a.id`, r.ID)
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				return e
			}
			cancelIDs = append(cancelIDs, id)
		}
		return rows.Err()
	})
	return result, cancelIDs, err
}

func (s *Store) GetSession(ctx context.Context, id string) (domain.RunnerSession, error) {
	r, e := scanSession(s.Pool.QueryRow(ctx, sessionSelect+` WHERE id=$1`, id))
	return r, missing(e, "runner session")
}
func (s *Store) ListSessions(ctx context.Context) ([]domain.RunnerSession, error) {
	rows, e := s.Pool.Query(ctx, sessionSelect+` ORDER BY runner_id,last_seen`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []domain.RunnerSession{}
	for rows.Next() {
		r, e := scanSession(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) SetPolicy(ctx context.Context, id, policy string) (domain.RunnerSession, error) {
	var r domain.RunnerSession
	err := s.Tx(ctx, func(tx pgx.Tx) error {
		tag, e := tx.Exec(ctx, `UPDATE runner_sessions SET policy=$2,state=CASE WHEN $2='OFFLINE' THEN 'OFFLINE' ELSE state END,drain_acknowledged=CASE WHEN policy=$2 THEN drain_acknowledged ELSE false END WHERE id=$1 AND state='ACTIVE'`, id, policy)
		if e != nil {
			return e
		}
		if tag.RowsAffected() == 0 {
			return domain.E(domain.NotFound, "active runner session not found")
		}
		r, e = scanSession(tx.QueryRow(ctx, sessionSelect+` WHERE id=$1`, id))
		return e
	})
	return r, err
}

type Capacity = domain.Capacity

func capacity(ctx context.Context, q querier, sessionID string) (Capacity, error) {
	var c Capacity
	e := q.QueryRow(ctx, `SELECT count(*)::integer,COALESCE(sum((f.spec->>'memory_bytes')::bigint),0)::bigint,COALESCE(sum((f.spec->>'cpu_millis')::bigint),0)::bigint FROM attempts a JOIN invocations i ON i.id=a.invocation_id JOIN function_versions f ON f.function_id=i.function_id AND f.version=i.function_version WHERE a.session_id=$1 AND a.state IN ('LEASED','RUNNING','COMPLETING')`, sessionID).Scan(&c.Slots, &c.MemoryBytes, &c.CPUMillis)
	return c, e
}
func (s *Store) Capacity(ctx context.Context, id string) (Capacity, error) {
	return capacity(ctx, s.Pool, id)
}

func (s *Store) RetireMissing(ctx context.Context) error {
	_, e := s.Pool.Exec(ctx, `UPDATE runner_sessions SET state='OFFLINE',policy='OFFLINE' WHERE state='ACTIVE' AND last_seen<clock_timestamp()-($1::bigint*interval '1 millisecond')`, s.HeartbeatTimeout.Milliseconds())
	return e
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
