package postgres

import (
	"context"
	"encoding/json"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
	"time"
)

type SubmitCommand = domain.SubmitCommand

func (s *Store) Submit(ctx context.Context, c SubmitCommand) (domain.Run, bool, error) {
	var result domain.Run
	duplicate := false
	requestHash := domain.Hash(struct {
		WorkflowID, Version string
		Input               domain.PayloadRef
		Priority            int32
		SLOMS               int64
		Deadline            time.Time
	}{c.WorkflowID, c.Version, c.Input, c.Priority, c.SLOMS, c.Deadline})
	err := s.Tx(ctx, func(tx pgx.Tx) error {
		var previousID, previousHash string
		lookup := tx.QueryRow(ctx, `SELECT id::text,request_hash FROM runs WHERE idempotency_key=$1`, c.IdempotencyKey).Scan(&previousID, &previousHash)
		if lookup == nil {
			if previousHash != requestHash {
				return domain.E(domain.Conflict, "idempotency key was used for another request")
			}
			duplicate = true
			var e error
			result, e = getRun(ctx, tx, previousID)
			return e
		}
		if lookup != pgx.ErrNoRows {
			return lookup
		}
		var wb []byte
		if err := tx.QueryRow(ctx, `SELECT spec FROM workflow_versions WHERE workflow_id=$1 AND version=$2`, c.WorkflowID, c.Version).Scan(&wb); err != nil {
			return missing(err, "workflow version")
		}
		var w domain.WorkflowSpec
		if err := json.Unmarshal(wb, &w); err != nil {
			return err
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		slo := c.SLOMS
		if slo == 0 {
			slo = w.DefaultSLOMS
		}
		deadline := c.Deadline
		if deadline.IsZero() {
			timeout := 60 * time.Second
			if d := time.Duration(slo) * 10 * time.Millisecond; d > timeout {
				timeout = d
			}
			deadline = now.Add(timeout)
		}
		if !deadline.After(now) {
			return domain.E(domain.Invalid, "run deadline has already passed")
		}
		id := domain.NewID()
		tag, err := tx.Exec(ctx, `INSERT INTO runs(id,workflow_id,workflow_version,idempotency_key,request_hash,state,input,priority,slo_ms,deadline,created_at,trace_parent,trace_state,parent_span_id) VALUES($1,$2,$3,$4,$5,'PENDING',$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(idempotency_key) DO NOTHING`, id, c.WorkflowID, c.Version, c.IdempotencyKey, requestHash, encoded(c.Input), c.Priority, slo, deadline, now, c.TraceParent, c.TraceState, c.ParentSpanID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var hash string
			if err = tx.QueryRow(ctx, `SELECT id::text,request_hash FROM runs WHERE idempotency_key=$1`, c.IdempotencyKey).Scan(&id, &hash); err != nil {
				return err
			}
			if hash != requestHash {
				return domain.E(domain.Conflict, "idempotency key was used for another request")
			}
			duplicate = true
			result, err = getRun(ctx, tx, id)
			return err
		}
		ids := map[string]string{}
		for _, step := range w.Steps {
			ids[step.Name] = domain.NewID()
		}
		for _, step := range w.Steps {
			state := "WAITING"
			var ready *time.Time
			if len(step.DependsOn) == 0 {
				state = "READY"
				ready = &now
			}
			_, err = tx.Exec(ctx, `INSERT INTO invocations(id,run_id,step_name,function_id,function_version,state,remaining_deps,first_ready_at,ready_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8)`, ids[step.Name], id, step.Name, step.Function.ID, step.Function.Version, state, len(step.DependsOn), ready)
			if err != nil {
				return err
			}
			if ready != nil {
				if err = outbox(ctx, tx, "READY", id, ids[step.Name], 1); err != nil {
					return err
				}
			}
		}
		for _, step := range w.Steps {
			for _, dep := range step.DependsOn {
				if _, err = tx.Exec(ctx, `INSERT INTO invocation_edges(run_id,parent_id,child_id) VALUES($1,$2,$3)`, id, ids[dep], ids[step.Name]); err != nil {
					return err
				}
			}
		}
		result, err = getRun(ctx, tx, id)
		return err
	})
	return result, duplicate, err
}

const runSelect = `SELECT id::text,workflow_id,workflow_version,state,input,output,priority,slo_ms,created_at,deadline,finished_at,error,trace_parent,trace_state,parent_span_id FROM runs`

type querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func getRun(ctx context.Context, q querier, id string) (domain.Run, error) {
	var r domain.Run
	var input, output []byte
	err := q.QueryRow(ctx, runSelect+` WHERE id=$1`, id).Scan(&r.ID, &r.WorkflowID, &r.WorkflowVersion, &r.State, &input, &output, &r.Priority, &r.SLOMS, &r.CreatedAt, &r.Deadline, &r.FinishedAt, &r.Error, &r.TraceParent, &r.TraceState, &r.ParentSpanID)
	if err != nil {
		return r, missing(err, "run")
	}
	if err = json.Unmarshal(input, &r.Input); err != nil {
		return r, err
	}
	if output != nil {
		r.Output = &domain.PayloadRef{}
		err = json.Unmarshal(output, r.Output)
	}
	return r, err
}
func (s *Store) GetRun(ctx context.Context, id string) (domain.Run, error) {
	return getRun(ctx, s.Pool, id)
}

const invocationSelect = `SELECT id::text,run_id::text,step_name,function_id,function_version,state,remaining_deps,ready_generation,attempt_no,COALESCE(current_attempt_id::text,''),first_ready_at,ready_at,not_before,started_at,finished_at,output,error FROM invocations`

func scanInvocation(row rowScanner) (domain.Invocation, error) {
	var v domain.Invocation
	var b []byte
	err := row.Scan(&v.ID, &v.RunID, &v.StepName, &v.Function.ID, &v.Function.Version, &v.State, &v.RemainingDeps, &v.Generation, &v.AttemptNo, &v.CurrentAttemptID, &v.FirstReadyAt, &v.ReadyAt, &v.NotBefore, &v.StartedAt, &v.FinishedAt, &b, &v.Error)
	if err == nil && b != nil {
		v.Output = &domain.PayloadRef{}
		err = json.Unmarshal(b, v.Output)
	}
	return v, err
}
func (s *Store) ListInvocations(ctx context.Context, runID string, limit int, after string) ([]domain.Invocation, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.Pool.Query(ctx, invocationSelect+` WHERE run_id=$1 AND ($2='' OR id::text>$2) ORDER BY id LIMIT $3`, runID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Invocation{}
	for rows.Next() {
		v, e := scanInvocation(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) CancelRun(ctx context.Context, id, reason string) (domain.Run, error) {
	var r domain.Run
	err := s.Tx(ctx, func(tx pgx.Tx) error {
		var state string
		if e := tx.QueryRow(ctx, `SELECT state FROM runs WHERE id=$1 FOR UPDATE`, id).Scan(&state); e != nil {
			return missing(e, "run")
		}
		if domain.ActiveRun(state) {
			if _, e := tx.Exec(ctx, `UPDATE runs SET state='CANCELLING',terminal_target='CANCELLED',error=$2 WHERE id=$1`, id, reason); e != nil {
				return e
			}
			if e := outbox(ctx, tx, "FINALIZE", id, "", 0); e != nil {
				return e
			}
		}
		var e error
		r, e = getRun(ctx, tx, id)
		return e
	})
	return r, err
}

// Finalize is deliberately a separate transaction. It never upgrades a SHARE
// lock taken by a completion transaction and never locks a runner session.
func (s *Store) Finalize(ctx context.Context, id string) (domain.Run, error) {
	var r domain.Run
	err := s.Tx(ctx, func(tx pgx.Tx) error {
		var state, target string
		var deadline time.Time
		var wb []byte
		if e := tx.QueryRow(ctx, `SELECT r.state,COALESCE(r.terminal_target,''),r.deadline,w.spec FROM runs r JOIN workflow_versions w ON w.workflow_id=r.workflow_id AND w.version=r.workflow_version WHERE r.id=$1 FOR UPDATE OF r`, id).Scan(&state, &target, &deadline, &wb); e != nil {
			return missing(e, "run")
		}
		if domain.TerminalRun(state) {
			var e error
			r, e = getRun(ctx, tx, id)
			return e
		}
		var failed, nonterminal, active int
		if e := tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='FAILED'),count(*) FILTER(WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')),count(*) FILTER(WHERE state IN ('LEASED','RUNNING')) FROM invocations WHERE run_id=$1`, id).Scan(&failed, &nonterminal, &active); e != nil {
			return e
		}
		var now time.Time
		if e := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); e != nil {
			return e
		}
		if domain.ActiveRun(state) && (failed > 0 || !deadline.After(now)) {
			target = "FAILED"
			reason := "a workflow step failed"
			if !deadline.After(now) {
				target = "TIMED_OUT"
				reason = "run deadline exceeded"
			}
			state = "CANCELLING"
			if _, e := tx.Exec(ctx, `UPDATE runs SET state='CANCELLING',terminal_target=$2,error=$3 WHERE id=$1`, id, target, reason); e != nil {
				return e
			}
		}
		if state == "PENDING" && active > 0 {
			if _, e := tx.Exec(ctx, `UPDATE runs SET state='RUNNING' WHERE id=$1`, id); e != nil {
				return e
			}
			state = "RUNNING"
		}
		if state == "CANCELLING" && nonterminal == 0 && active == 0 {
			if target == "" {
				target = "CANCELLED"
			}
			if _, e := tx.Exec(ctx, `UPDATE runs SET state=$2,finished_at=clock_timestamp() WHERE id=$1`, id, target); e != nil {
				return e
			}
			if e := outbox(ctx, tx, "RUN_FINISHED", id, "", 0); e != nil {
				return e
			}
		}
		if domain.ActiveRun(state) && nonterminal == 0 && failed == 0 {
			var w domain.WorkflowSpec
			if e := json.Unmarshal(wb, &w); e != nil {
				return e
			}
			if _, e := tx.Exec(ctx, `UPDATE runs SET state='SUCCEEDED',finished_at=clock_timestamp(),output=(SELECT output FROM invocations WHERE run_id=$1 AND step_name=$2) WHERE id=$1`, id, w.OutputStep); e != nil {
				return e
			}
			if e := outbox(ctx, tx, "RUN_FINISHED", id, "", 0); e != nil {
				return e
			}
		}
		var e error
		r, e = getRun(ctx, tx, id)
		return e
	})
	return r, err
}
