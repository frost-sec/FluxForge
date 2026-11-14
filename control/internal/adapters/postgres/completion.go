package postgres

import (
	"context"
	"encoding/json"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
	"time"
)

type CompletionResult = domain.CompletionResult

func (s *Store) Complete(ctx context.Context, c domain.Completion) (CompletionResult, error) {
	identity, e := s.attemptIdentity(ctx, c.AttemptID)
	if e != nil {
		return CompletionResult{}, e
	}
	var result CompletionResult
	hash := domain.Hash(struct {
		Outcome    string
		Output     *domain.PayloadRef
		DurationMS int64
		Error      string
	}{c.Outcome, c.Output, c.DurationMS, c.Error})
	e = s.Tx(ctx, func(tx pgx.Tx) error {
		result = CompletionResult{RunID: identity.RunID}
		var runState string
		if e := tx.QueryRow(ctx, `SELECT state FROM runs WHERE id=$1 FOR SHARE`, identity.RunID).Scan(&runState); e != nil {
			return e
		}
		runner, e := scanSession(tx.QueryRow(ctx, sessionSelect+` WHERE id=$1 FOR SHARE`, c.SessionID))
		if e != nil {
			return missing(e, "runner session")
		}
		v, a, e := lockAttemptRows(ctx, tx, identity, c.AttemptID, true)
		if e != nil {
			return e
		}
		if !validToken(a, c.SessionID, c.LeaseToken) {
			return domain.E(domain.Stale, "completion belongs to another lease")
		}
		if !domain.ActiveAttempt(a.State) {
			var oldHash *string
			if e = tx.QueryRow(ctx, `SELECT completion_hash FROM attempts WHERE id=$1`, a.ID).Scan(&oldHash); e != nil {
				return e
			}
			if oldHash != nil && *oldHash == hash {
				result.Duplicate = true
				result.Invocation = v
				return nil
			}
			if oldHash != nil {
				return domain.E(domain.Conflict, "attempt already has a different completion")
			}
			return domain.E(domain.Stale, "attempt is no longer active")
		}
		if !domain.ActiveRun(runState) || runner.State != "ACTIVE" {
			return domain.E(domain.Stale, "run or runner session is no longer active")
		}
		var now time.Time
		var deadline time.Time
		if e = tx.QueryRow(ctx, `SELECT clock_timestamp(),deadline FROM runs WHERE id=$1`, identity.RunID).Scan(&now, &deadline); e != nil {
			return e
		}
		if !deadline.After(now) {
			return domain.E(domain.Stale, "run deadline exceeded")
		}
		if e = checkLease(a, v, c.SessionID, c.LeaseToken, now); e != nil {
			return e
		}
		if c.Outcome == "SUCCEEDED" && a.State == "LEASED" {
			return domain.E(domain.Stale, "successful attempt must have started")
		}
		var output any
		if c.Output != nil {
			output = encoded(c.Output)
		}
		if _, e = tx.Exec(ctx, `UPDATE attempts SET state=$2,finished_at=$3,output=$4,duration_ms=$5,error=$6,completion_hash=$7 WHERE id=$1`, a.ID, c.Outcome, now, output, c.DurationMS, c.Error, hash); e != nil {
			return e
		}
		var fb []byte
		if e = tx.QueryRow(ctx, `SELECT spec FROM function_versions WHERE function_id=$1 AND version=$2`, v.Function.ID, v.Function.Version).Scan(&fb); e != nil {
			return e
		}
		if e = json.Unmarshal(fb, &result.Function); e != nil {
			return e
		}
		if c.Outcome == "SUCCEEDED" {
			if _, e = tx.Exec(ctx, `UPDATE invocations SET state='SUCCEEDED',finished_at=$2,output=$3,error='',current_attempt_id=NULL WHERE id=$1`, v.ID, now, output); e != nil {
				return e
			}
			rows, e := tx.Query(ctx, `WITH satisfied AS (UPDATE invocation_edges SET satisfied_at=$3 WHERE run_id=$1 AND parent_id=$2 AND satisfied_at IS NULL RETURNING child_id),delta AS (SELECT child_id,count(*)::int AS n FROM satisfied GROUP BY child_id) UPDATE invocations c SET remaining_deps=c.remaining_deps-d.n,state=CASE WHEN c.remaining_deps=d.n THEN 'READY' ELSE c.state END,ready_generation=CASE WHEN c.remaining_deps=d.n THEN c.ready_generation+1 ELSE c.ready_generation END,first_ready_at=CASE WHEN c.remaining_deps=d.n THEN COALESCE(c.first_ready_at,$3) ELSE c.first_ready_at END,ready_at=CASE WHEN c.remaining_deps=d.n THEN $3 ELSE c.ready_at END FROM delta d WHERE c.id=d.child_id AND c.state='WAITING' AND c.remaining_deps>=d.n RETURNING c.id::text,c.remaining_deps,c.ready_generation`, v.RunID, v.ID, now)
			if e != nil {
				return e
			}
			type unlocked struct {
				id         string
				deps       int
				generation int64
			}
			children := []unlocked{}
			for rows.Next() {
				var u unlocked
				if e = rows.Scan(&u.id, &u.deps, &u.generation); e != nil {
					rows.Close()
					return e
				}
				children = append(children, u)
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return e
			}
			for _, u := range children {
				if u.deps == 0 {
					if e = outbox(ctx, tx, "READY", v.RunID, u.id, u.generation); e != nil {
						return e
					}
				}
			}
			result.InvocationTerminal = true
		} else {
			terminal, e := s.failInvocation(ctx, tx, v, c.Outcome, c.Error, now)
			if e != nil {
				return e
			}
			result.InvocationTerminal = terminal
		}
		if result.InvocationTerminal {
			if e = outbox(ctx, tx, "INVOCATION_FINISHED", v.RunID, v.ID, v.Generation); e != nil {
				return e
			}
		}
		if e = outbox(ctx, tx, "FINALIZE", v.RunID, v.ID, v.Generation); e != nil {
			return e
		}
		result.Invocation, e = scanInvocation(tx.QueryRow(ctx, invocationSelect+` WHERE id=$1`, v.ID))
		return e
	})
	return result, e
}

func (s *Store) failInvocation(ctx context.Context, tx pgx.Tx, v domain.Invocation, outcome, message string, now time.Time) (bool, error) {
	var b []byte
	if e := tx.QueryRow(ctx, `SELECT w.spec FROM workflow_versions w JOIN runs r ON r.workflow_id=w.workflow_id AND r.workflow_version=w.version WHERE r.id=$1`, v.RunID).Scan(&b); e != nil {
		return false, e
	}
	var w domain.WorkflowSpec
	if e := json.Unmarshal(b, &w); e != nil {
		return false, e
	}
	var retry domain.RetryPolicy
	for _, step := range w.Steps {
		if step.Name == v.StepName {
			retry = step.Retry
			break
		}
	}
	retryable := outcome == "LOST" || outcome == "CRASHED" || outcome == "INFRA_ERROR" || outcome == "RELEASED" || (outcome == "USER_FAILED" && retry.RetryUserErrors)
	if retryable && v.AttemptNo < retry.MaxAttempts {
		backoff := retry.InitialBackoffMS
		for n := int32(1); n < v.AttemptNo && backoff < retry.MaxBackoffMS; n++ {
			backoff *= 2
		}
		if backoff > retry.MaxBackoffMS {
			backoff = retry.MaxBackoffMS
		}
		_, e := tx.Exec(ctx, `UPDATE invocations SET state='RETRY_WAIT',not_before=$2,error=$3,current_attempt_id=NULL WHERE id=$1`, v.ID, now.Add(time.Duration(backoff)*time.Millisecond), message)
		return false, e
	}
	_, e := tx.Exec(ctx, `UPDATE invocations SET state='FAILED',finished_at=$2,error=$3,current_attempt_id=NULL WHERE id=$1`, v.ID, now, message)
	return true, e
}

func (s *Store) Release(ctx context.Context, session, attemptID, token, reason string) (CompletionResult, error) {
	return s.Complete(ctx, domain.Completion{SessionID: session, AttemptID: attemptID, LeaseToken: token, Outcome: "RELEASED", Error: reason})
}
