package postgres

import (
	"context"
	"crypto/subtle"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
	"time"
)

type attemptIdentity struct{ RunID, InvocationID string }

func (s *Store) attemptIdentity(ctx context.Context, id string) (attemptIdentity, error) {
	var x attemptIdentity
	e := s.Pool.QueryRow(ctx, `SELECT run_id::text,invocation_id::text FROM attempts WHERE id=$1`, id).Scan(&x.RunID, &x.InvocationID)
	return x, missing(e, "attempt")
}

func validToken(a domain.Attempt, session, token string) bool {
	return a.SessionID == session && subtle.ConstantTimeCompare([]byte(a.LeaseToken), []byte(token)) == 1
}

func lockAttemptRows(ctx context.Context, tx pgx.Tx, identity attemptIdentity, attemptID string, children bool) (domain.Invocation, domain.Attempt, error) {
	query := `SELECT id::text FROM invocations WHERE id=$1 ORDER BY id FOR UPDATE`
	if children {
		query = `SELECT id::text FROM invocations WHERE id=$1 OR id IN (SELECT child_id FROM invocation_edges WHERE parent_id=$1) ORDER BY id FOR UPDATE`
	}
	rows, e := tx.Query(ctx, query, identity.InvocationID)
	if e != nil {
		return domain.Invocation{}, domain.Attempt{}, e
	}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return domain.Invocation{}, domain.Attempt{}, e
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return domain.Invocation{}, domain.Attempt{}, e
	}
	v, e := scanInvocation(tx.QueryRow(ctx, invocationSelect+` WHERE id=$1`, identity.InvocationID))
	if e != nil {
		return v, domain.Attempt{}, e
	}
	a, e := scanAttempt(tx.QueryRow(ctx, attemptSelect+` WHERE id=$1 FOR UPDATE`, attemptID))
	return v, a, e
}

func checkLease(a domain.Attempt, v domain.Invocation, session, token string, now time.Time) error {
	if !validToken(a, session, token) || !domain.ActiveAttempt(a.State) || v.CurrentAttemptID != a.ID || !a.LeaseExpiresAt.After(now) {
		return domain.E(domain.Stale, "lease is revoked, expired, or belongs to another session")
	}
	if a.State == "RUNNING" && a.ExecutionDeadline != nil && !a.ExecutionDeadline.After(now) {
		return domain.E(domain.Stale, "execution deadline exceeded")
	}
	if a.State == "COMPLETING" && a.UploadDeadline != nil && !a.UploadDeadline.After(now) {
		return domain.E(domain.Stale, "completion upload deadline exceeded")
	}
	return nil
}

func (s *Store) Started(ctx context.Context, session, attemptID, token string) (domain.Attempt, error) {
	identity, e := s.attemptIdentity(ctx, attemptID)
	if e != nil {
		return domain.Attempt{}, e
	}
	var result domain.Attempt
	e = s.Tx(ctx, func(tx pgx.Tx) error {
		if _, e := lockRun(ctx, tx, identity.RunID, false); e != nil {
			return e
		}
		if _, e := lockSession(ctx, tx, session, false); e != nil {
			return e
		}
		v, a, e := lockAttemptRows(ctx, tx, identity, attemptID, false)
		if e != nil {
			return e
		}
		var now time.Time
		if e = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); e != nil {
			return e
		}
		if e = checkLease(a, v, session, token, now); e != nil {
			return e
		}
		if a.State != "LEASED" {
			result = a
			return nil
		}
		var timeout int64
		var deadline time.Time
		if e = tx.QueryRow(ctx, `SELECT (f.spec->>'timeout_ms')::bigint,r.deadline FROM function_versions f JOIN runs r ON r.id=$3 WHERE f.function_id=$1 AND f.version=$2`, v.Function.ID, v.Function.Version, v.RunID).Scan(&timeout, &deadline); e != nil {
			return e
		}
		deadline = minTime(deadline, now.Add(time.Duration(timeout)*time.Millisecond))
		expiry := minTime(deadline, now.Add(s.LeaseDuration))
		if _, e = tx.Exec(ctx, `UPDATE attempts SET state='RUNNING',started_at=$2,execution_deadline=$3,lease_expires_at=$4 WHERE id=$1`, a.ID, now, deadline, expiry); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE invocations SET state='RUNNING',started_at=COALESCE(started_at,$2) WHERE id=$1`, v.ID, now); e != nil {
			return e
		}
		result, e = scanAttempt(tx.QueryRow(ctx, attemptSelect+` WHERE id=$1`, a.ID))
		return e
	})
	return result, e
}

func (s *Store) ExecutionFinished(ctx context.Context, session, attemptID, token string, durationMS int64) (domain.Attempt, error) {
	identity, e := s.attemptIdentity(ctx, attemptID)
	if e != nil {
		return domain.Attempt{}, e
	}
	var result domain.Attempt
	e = s.Tx(ctx, func(tx pgx.Tx) error {
		if _, e := lockRun(ctx, tx, identity.RunID, false); e != nil {
			return e
		}
		if _, e := lockSession(ctx, tx, session, false); e != nil {
			return e
		}
		v, a, e := lockAttemptRows(ctx, tx, identity, attemptID, false)
		if e != nil {
			return e
		}
		var now, runDeadline time.Time
		if e = tx.QueryRow(ctx, `SELECT clock_timestamp(),deadline FROM runs WHERE id=$1`, v.RunID).Scan(&now, &runDeadline); e != nil {
			return e
		}
		if e = checkLease(a, v, session, token, now); e != nil {
			return e
		}
		if a.State == "COMPLETING" {
			result = a
			return nil
		}
		if a.State != "RUNNING" {
			return domain.E(domain.Stale, "attempt has not started")
		}
		deadline := minTime(runDeadline, now.Add(s.UploadTimeout))
		expiry := minTime(deadline, now.Add(s.LeaseDuration))
		if _, e = tx.Exec(ctx, `UPDATE attempts SET state='COMPLETING',execution_finished_at=$2,upload_deadline=$3,lease_expires_at=$4,duration_ms=$5 WHERE id=$1`, a.ID, now, deadline, expiry, durationMS); e != nil {
			return e
		}
		result, e = scanAttempt(tx.QueryRow(ctx, attemptSelect+` WHERE id=$1`, a.ID))
		return e
	})
	return result, e
}

func (s *Store) Renew(ctx context.Context, session, attemptID, token string) (domain.Attempt, error) {
	identity, e := s.attemptIdentity(ctx, attemptID)
	if e != nil {
		return domain.Attempt{}, e
	}
	var result domain.Attempt
	e = s.Tx(ctx, func(tx pgx.Tx) error {
		if _, e := lockRun(ctx, tx, identity.RunID, false); e != nil {
			return e
		}
		if _, e := lockSession(ctx, tx, session, false); e != nil {
			return e
		}
		v, a, e := lockAttemptRows(ctx, tx, identity, attemptID, false)
		if e != nil {
			return e
		}
		var now, deadline time.Time
		if e = tx.QueryRow(ctx, `SELECT clock_timestamp(),deadline FROM runs WHERE id=$1`, v.RunID).Scan(&now, &deadline); e != nil {
			return e
		}
		if e = checkLease(a, v, session, token, now); e != nil {
			return e
		}
		limit := a.CreatedAt.Add(s.PrepareTimeout)
		if a.State == "RUNNING" && a.ExecutionDeadline != nil {
			limit = *a.ExecutionDeadline
		}
		if a.State == "COMPLETING" && a.UploadDeadline != nil {
			limit = *a.UploadDeadline
		}
		deadline = minTime(deadline, limit)
		if !deadline.After(now) {
			return domain.E(domain.Stale, "attempt phase deadline exceeded")
		}
		expiry := minTime(deadline, now.Add(s.LeaseDuration))
		if _, e = tx.Exec(ctx, `UPDATE attempts SET lease_expires_at=$2 WHERE id=$1`, a.ID, expiry); e != nil {
			return e
		}
		result, e = scanAttempt(tx.QueryRow(ctx, attemptSelect+` WHERE id=$1`, a.ID))
		return e
	})
	return result, e
}
