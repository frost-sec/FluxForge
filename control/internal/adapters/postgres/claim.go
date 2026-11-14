package postgres

import (
	"context"
	"encoding/json"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
	"sort"
	"time"
)

type ClaimCommand = domain.ClaimCommand
type ClaimResult = domain.ClaimResult

func (s *Store) ReplayClaim(ctx context.Context, c ClaimCommand) (ClaimResult, bool, error) {
	var exists bool
	e := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM acquire_requests WHERE session_id=$1 AND request_id=$2)`, c.SessionID, c.RequestID).Scan(&exists)
	if e != nil || !exists {
		return ClaimResult{}, exists, e
	}
	c.Candidates = nil
	r, e := s.Claim(ctx, c)
	return r, true, e
}

func (s *Store) GetAssignment(ctx context.Context, id string) (domain.Assignment, error) {
	var result domain.Assignment
	e := s.Tx(ctx, func(tx pgx.Tx) error { var e error; result, e = loadAssignment(ctx, tx, id); return e })
	return result, e
}

func (s *Store) Claim(ctx context.Context, c ClaimCommand) (ClaimResult, error) {
	result := ClaimResult{Assignments: []domain.Assignment{}, ExpiredAttemptIDs: []string{}}
	requestHash := domain.Hash(struct {
		Slots int32
		Wait  int64
	}{c.RequestedSlots, c.WaitTimeoutMS})
	err := s.Tx(ctx, func(tx pgx.Tx) error {
		result = ClaimResult{Assignments: []domain.Assignment{}, ExpiredAttemptIDs: []string{}}
		runIDs := map[string]bool{}
		for _, v := range c.Candidates {
			runIDs[v.RunID] = true
		}
		ordered := []string{}
		for id := range runIDs {
			ordered = append(ordered, id)
		}
		sort.Strings(ordered)
		activeRuns := map[string]bool{}
		for _, id := range ordered {
			var state string
			var deadline time.Time
			if e := tx.QueryRow(ctx, `SELECT state,deadline FROM runs WHERE id=$1 FOR SHARE`, id).Scan(&state, &deadline); e != nil {
				return missing(e, "run")
			}
			activeRuns[id] = domain.ActiveRun(state) && deadline.After(time.Now())
		}
		runner, e := lockSession(ctx, tx, c.SessionID, true)
		if e != nil {
			return e
		}
		result.Policy = runner.Policy
		var hash string
		var previous []byte
		e = tx.QueryRow(ctx, `SELECT request_hash,attempt_ids FROM acquire_requests WHERE session_id=$1 AND request_id=$2`, c.SessionID, c.RequestID).Scan(&hash, &previous)
		if e == nil {
			if hash != requestHash {
				return domain.E(domain.Conflict, "request ID was used with different acquire parameters")
			}
			result.Duplicate = true
			var ids []string
			if e = json.Unmarshal(previous, &ids); e != nil {
				return e
			}
			for _, id := range ids {
				a, e := loadAssignment(ctx, tx, id)
				if e != nil {
					return e
				}
				if domain.ActiveAttempt(a.Attempt.State) && domain.ActiveRun(a.Run.State) && a.Attempt.LeaseExpiresAt.After(time.Now()) {
					result.Assignments = append(result.Assignments, a)
				} else {
					result.ExpiredAttemptIDs = append(result.ExpiredAttemptIDs, id)
				}
			}
			return nil
		}
		if e != pgx.ErrNoRows {
			return e
		}
		used, e := capacity(ctx, tx, c.SessionID)
		if e != nil {
			return e
		}
		free := runner.Slots - used.Slots
		if c.RequestedSlots < free {
			free = c.RequestedSlots
		}
		if runner.Policy != "ACTIVE" {
			free = 0
		}
		remainingMemory := runner.MemoryBytes - used.MemoryBytes
		remainingCPU := runner.CPUMillis - used.CPUMillis
		candidates := append([]domain.ReadyTask(nil), c.Candidates...)
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].InvocationID < candidates[j].InvocationID })
		locked := map[string]domain.Invocation{}
		for _, candidate := range candidates {
			if !activeRuns[candidate.RunID] {
				continue
			}
			v, e := scanInvocation(tx.QueryRow(ctx, invocationSelect+` WHERE id=$1 FOR UPDATE`, candidate.InvocationID))
			if e == pgx.ErrNoRows {
				continue
			}
			if e != nil {
				return e
			}
			locked[v.ID] = v
		}
		ids := []string{}
		var now time.Time
		if e = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); e != nil {
			return e
		}
		for _, candidate := range c.Candidates {
			if free <= 0 {
				break
			}
			v, ok := locked[candidate.InvocationID]
			if !ok || v.State != "READY" || v.Generation != candidate.Generation || (v.NotBefore != nil && v.NotBefore.After(now)) {
				continue
			}
			var fb []byte
			if e = tx.QueryRow(ctx, `SELECT spec FROM function_versions WHERE function_id=$1 AND version=$2`, v.Function.ID, v.Function.Version).Scan(&fb); e != nil {
				return e
			}
			var f domain.FunctionSpec
			if e = json.Unmarshal(fb, &f); e != nil {
				return e
			}
			if f.MemoryBytes > remainingMemory || f.CPUMillis > remainingCPU || f.Architecture != runner.Architecture || f.RuntimeDigest != runner.RuntimeDigest {
				continue
			}
			supported := false
			for _, runtime := range runner.Runtimes {
				if runtime == f.Runtime {
					supported = true
				}
			}
			if !supported {
				continue
			}
			r, e := getRun(ctx, tx, v.RunID)
			if e != nil {
				return e
			}
			attemptID := domain.NewID()
			expiry := minTime(now.Add(s.LeaseDuration), r.Deadline)
			if _, e = tx.Exec(ctx, `INSERT INTO attempts(id,invocation_id,run_id,attempt_no,session_id,lease_token,state,lease_expires_at,created_at) VALUES($1,$2,$3,$4,$5,$6,'LEASED',$7,$8)`, attemptID, v.ID, v.RunID, v.AttemptNo+1, c.SessionID, domain.Token(), expiry, now); e != nil {
				return e
			}
			if _, e = tx.Exec(ctx, `UPDATE invocations SET state='LEASED',attempt_no=attempt_no+1,current_attempt_id=$2 WHERE id=$1`, v.ID, attemptID); e != nil {
				return e
			}
			ids = append(ids, attemptID)
			free--
			remainingMemory -= f.MemoryBytes
			remainingCPU -= f.CPUMillis
			if e = outbox(ctx, tx, "FINALIZE", v.RunID, v.ID, v.Generation); e != nil {
				return e
			}
		}
		if _, e = tx.Exec(ctx, `INSERT INTO acquire_requests(session_id,request_id,request_hash,attempt_ids) VALUES($1,$2,$3,$4)`, c.SessionID, c.RequestID, requestHash, encoded(ids)); e != nil {
			return e
		}
		for _, id := range ids {
			a, e := loadAssignment(ctx, tx, id)
			if e != nil {
				return e
			}
			result.Assignments = append(result.Assignments, a)
		}
		return nil
	})
	return result, err
}

const attemptSelect = `SELECT id::text,invocation_id::text,run_id::text,attempt_no,session_id::text,state,lease_token,lease_expires_at,execution_deadline,upload_deadline,execution_finished_at,created_at,started_at,finished_at,duration_ms,error,output FROM attempts`

func scanAttempt(row rowScanner) (domain.Attempt, error) {
	var a domain.Attempt
	var b []byte
	e := row.Scan(&a.ID, &a.InvocationID, &a.RunID, &a.Number, &a.SessionID, &a.State, &a.LeaseToken, &a.LeaseExpiresAt, &a.ExecutionDeadline, &a.UploadDeadline, &a.ExecutionFinishedAt, &a.CreatedAt, &a.StartedAt, &a.FinishedAt, &a.DurationMS, &a.Error, &b)
	if e == nil && b != nil {
		a.Output = &domain.PayloadRef{}
		e = json.Unmarshal(b, a.Output)
	}
	return a, e
}

func loadAssignment(ctx context.Context, tx pgx.Tx, id string) (domain.Assignment, error) {
	var result domain.Assignment
	var e error
	result.Attempt, e = scanAttempt(tx.QueryRow(ctx, attemptSelect+` WHERE id=$1`, id))
	if e != nil {
		return result, missing(e, "attempt")
	}
	result.Invocation, e = scanInvocation(tx.QueryRow(ctx, invocationSelect+` WHERE id=$1`, result.Attempt.InvocationID))
	if e != nil {
		return result, e
	}
	result.Run, e = getRun(ctx, tx, result.Attempt.RunID)
	if e != nil {
		return result, e
	}
	var fb, wb []byte
	e = tx.QueryRow(ctx, `SELECT f.spec,w.spec FROM function_versions f JOIN workflow_versions w ON w.workflow_id=$3 AND w.version=$4 WHERE f.function_id=$1 AND f.version=$2`, result.Invocation.Function.ID, result.Invocation.Function.Version, result.Run.WorkflowID, result.Run.WorkflowVersion).Scan(&fb, &wb)
	if e != nil {
		return result, e
	}
	if e = json.Unmarshal(fb, &result.Function); e != nil {
		return result, e
	}
	var w domain.WorkflowSpec
	if e = json.Unmarshal(wb, &w); e != nil {
		return result, e
	}
	outputs := map[string]domain.PayloadRef{}
	rows, e := tx.Query(ctx, `SELECT step_name,output FROM invocations WHERE run_id=$1 AND state='SUCCEEDED'`, result.Run.ID)
	if e != nil {
		return result, e
	}
	for rows.Next() {
		var name string
		var b []byte
		if e = rows.Scan(&name, &b); e != nil {
			rows.Close()
			return result, e
		}
		var p domain.PayloadRef
		if e = json.Unmarshal(b, &p); e != nil {
			rows.Close()
			return result, e
		}
		outputs[name] = p
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return result, e
	}
	for _, step := range w.Steps {
		if step.Name == result.Invocation.StepName {
			result.Input, e = domain.ResolveInput(step.Input, result.Run.Input, outputs)
			break
		}
	}
	return result, e
}

func (s *Store) ListAttempts(ctx context.Context, invocationID string, limit int, after string) ([]domain.Attempt, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, e := s.Pool.Query(ctx, attemptSelect+` WHERE invocation_id=$1 AND ($2='' OR id::text>$2) ORDER BY id LIMIT $3`, invocationID, after, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []domain.Attempt{}
	for rows.Next() {
		a, e := scanAttempt(rows)
		if e != nil {
			return nil, e
		}
		a.LeaseToken = ""
		out = append(out, a)
	}
	return out, rows.Err()
}
