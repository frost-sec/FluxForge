package postgres

import (
	"context"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
	"time"
)

// ReapOne uses the same lock hierarchy as a client completion. A stale session
// is never modified while holding an Invocation lock.
func (s *Store) ReapOne(ctx context.Context, id string) error {
	identity, e := s.attemptIdentity(ctx, id)
	if e != nil {
		return e
	}
	return s.Tx(ctx, func(tx pgx.Tx) error {
		var runState string
		if e := tx.QueryRow(ctx, `SELECT state FROM runs WHERE id=$1 FOR SHARE`, identity.RunID).Scan(&runState); e != nil {
			return e
		}
		var session string
		if e := tx.QueryRow(ctx, `SELECT session_id::text FROM attempts WHERE id=$1`, id).Scan(&session); e != nil {
			return e
		}
		runner, e := scanSession(tx.QueryRow(ctx, sessionSelect+` WHERE id=$1 FOR SHARE`, session))
		if e != nil {
			return e
		}
		v, a, e := lockAttemptRows(ctx, tx, identity, id, false)
		if e != nil {
			return e
		}
		if !domain.ActiveAttempt(a.State) {
			return nil
		}
		var now time.Time
		if e = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); e != nil {
			return e
		}
		outcome, message := "LOST", "task lease expired"
		revoke := !domain.ActiveRun(runState) || runner.State != "ACTIVE" || !a.LeaseExpiresAt.After(now)
		if a.State == "LEASED" && a.CreatedAt.Add(s.PrepareTimeout).Before(now) {
			revoke = true
			message = "task preparation deadline exceeded"
		}
		if a.State == "RUNNING" && a.ExecutionDeadline != nil && !a.ExecutionDeadline.After(now) {
			revoke = true
			outcome = "TIMEOUT"
			message = "function execution deadline exceeded"
		}
		if a.State == "COMPLETING" && a.UploadDeadline != nil && !a.UploadDeadline.After(now) {
			revoke = true
			outcome = "INFRA_ERROR"
			message = "output upload deadline exceeded"
		}
		if !revoke {
			return nil
		}
		if !domain.ActiveRun(runState) {
			outcome = "CANCELLED"
			message = "run is terminating"
		}
		if runner.State != "ACTIVE" {
			message = "runner session is offline or superseded"
		}
		if _, e = tx.Exec(ctx, `UPDATE attempts SET state=$2,finished_at=$3,error=$4 WHERE id=$1`, id, outcome, now, message); e != nil {
			return e
		}
		if v.CurrentAttemptID == id {
			terminal := false
			if !domain.ActiveRun(runState) {
				_, e = tx.Exec(ctx, `UPDATE invocations SET state='CANCELLED',finished_at=$2,current_attempt_id=NULL,error=$3 WHERE id=$1`, v.ID, now, message)
				terminal = true
			} else {
				terminal, e = s.failInvocation(ctx, tx, v, outcome, message, now)
			}
			if e != nil {
				return e
			}
			if terminal {
				if e = outbox(ctx, tx, "INVOCATION_FINISHED", v.RunID, v.ID, v.Generation); e != nil {
					return e
				}
			}
		}
		return outbox(ctx, tx, "FINALIZE", v.RunID, v.ID, v.Generation)
	})
}

func (s *Store) Sweep(ctx context.Context) error {
	if e := s.RetireMissing(ctx); e != nil {
		return e
	}
	rows, e := s.Pool.Query(ctx, `SELECT a.id::text FROM attempts a JOIN runs r ON r.id=a.run_id JOIN runner_sessions s ON s.id=a.session_id WHERE a.state IN ('LEASED','RUNNING','COMPLETING') AND (a.lease_expires_at<=clock_timestamp() OR s.state<>'ACTIVE' OR r.state NOT IN ('PENDING','RUNNING') OR (a.state='RUNNING' AND a.execution_deadline<=clock_timestamp()) OR (a.state='COMPLETING' AND a.upload_deadline<=clock_timestamp()) OR (a.state='LEASED' AND a.created_at<clock_timestamp()-($1::bigint*interval '1 millisecond'))) ORDER BY a.id LIMIT 1000`, s.PrepareTimeout.Milliseconds())
	if e != nil {
		return e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		if e = s.ReapOne(ctx, id); e != nil {
			return e
		}
	}
	rows, e = s.Pool.Query(ctx, `SELECT id::text,run_id::text FROM invocations WHERE state='RETRY_WAIT' AND not_before<=clock_timestamp() ORDER BY id LIMIT 1000`)
	if e != nil {
		return e
	}
	type pair struct{ id, run string }
	pending := []pair{}
	for rows.Next() {
		var p pair
		if e = rows.Scan(&p.id, &p.run); e != nil {
			rows.Close()
			return e
		}
		pending = append(pending, p)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, p := range pending {
		e = s.Tx(ctx, func(tx pgx.Tx) error {
			var state string
			if e := tx.QueryRow(ctx, `SELECT state FROM runs WHERE id=$1 FOR SHARE`, p.run).Scan(&state); e != nil {
				return e
			}
			if !domain.ActiveRun(state) {
				return nil
			}
			v, e := scanInvocation(tx.QueryRow(ctx, invocationSelect+` WHERE id=$1 FOR UPDATE`, p.id))
			if e != nil {
				return e
			}
			if v.State != "RETRY_WAIT" || v.NotBefore == nil || v.NotBefore.After(time.Now()) {
				return nil
			}
			if _, e = tx.Exec(ctx, `UPDATE invocations SET state='READY',ready_generation=ready_generation+1,ready_at=clock_timestamp(),first_ready_at=COALESCE(first_ready_at,clock_timestamp()),not_before=NULL WHERE id=$1`, p.id); e != nil {
				return e
			}
			return outbox(ctx, tx, "READY", p.run, p.id, v.Generation+1)
		})
		if e != nil {
			return e
		}
	}
	return s.sweepRuns(ctx)
}

func (s *Store) sweepRuns(ctx context.Context) error {
	rows, e := s.Pool.Query(ctx, `SELECT id::text FROM runs WHERE state IN ('PENDING','RUNNING','CANCELLING') ORDER BY created_at LIMIT 10000`)
	if e != nil {
		return e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		r, e := s.Finalize(ctx, id)
		if e != nil {
			return e
		}
		if r.State == "CANCELLING" {
			if e = s.cancelWaiting(ctx, id); e != nil {
				return e
			}
			if _, e = s.Finalize(ctx, id); e != nil {
				return e
			}
		}
	}
	return nil
}

func (s *Store) cancelWaiting(ctx context.Context, runID string) error {
	rows, e := s.Pool.Query(ctx, `SELECT id::text FROM invocations WHERE run_id=$1 AND state IN ('WAITING','READY','RETRY_WAIT') ORDER BY id`, runID)
	if e != nil {
		return e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		e = s.Tx(ctx, func(tx pgx.Tx) error {
			var state string
			if e := tx.QueryRow(ctx, `SELECT state FROM runs WHERE id=$1 FOR SHARE`, runID).Scan(&state); e != nil {
				return e
			}
			if state != "CANCELLING" {
				return nil
			}
			v, e := scanInvocation(tx.QueryRow(ctx, invocationSelect+` WHERE id=$1 FOR UPDATE`, id))
			if e != nil {
				return e
			}
			if v.State != "WAITING" && v.State != "READY" && v.State != "RETRY_WAIT" {
				return nil
			}
			if _, e = tx.Exec(ctx, `UPDATE invocations SET state='CANCELLED',finished_at=clock_timestamp(),error='run cancelled' WHERE id=$1`, id); e != nil {
				return e
			}
			return outbox(ctx, tx, "INVOCATION_FINISHED", runID, id, v.Generation)
		})
		if e != nil {
			return e
		}
	}
	return nil
}
