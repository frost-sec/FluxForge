package postgres

import (
	"context"
	"encoding/json"
	"fluxforge-control/internal/domain"
	"time"
)

func (s *Store) ListReady(ctx context.Context) ([]domain.ReadyTask, error) { return s.ready(ctx, "") }
func (s *Store) LookupReady(ctx context.Context, id string) ([]domain.ReadyTask, error) {
	return s.ready(ctx, id)
}
func (s *Store) ready(ctx context.Context, id string) ([]domain.ReadyTask, error) {
	rows, e := s.Pool.Query(ctx, `SELECT i.id::text,i.run_id::text,i.ready_generation,i.ready_at,r.priority,r.deadline,f.spec,COALESCE(i.first_ready_at,i.ready_at) FROM invocations i JOIN runs r ON r.id=i.run_id JOIN function_versions f ON f.function_id=i.function_id AND f.version=i.function_version WHERE i.state='READY' AND r.state IN ('PENDING','RUNNING') AND r.deadline>clock_timestamp() AND ($1='' OR i.id::text=$1) ORDER BY i.id`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []domain.ReadyTask{}
	for rows.Next() {
		var task domain.ReadyTask
		var b []byte
		var firstReady time.Time
		if e = rows.Scan(&task.InvocationID, &task.RunID, &task.Generation, &task.ReadyAt, &task.Priority, &task.Deadline, &b, &firstReady); e != nil {
			return nil, e
		}
		var f domain.FunctionSpec
		if e = json.Unmarshal(b, &f); e != nil {
			return nil, e
		}
		task.FunctionKey = f.AffinityKey()
		task.FunctionID = f.ID
		task.FunctionVersion = f.Version
		task.Runtime = f.Runtime
		task.RuntimeDigest = f.RuntimeDigest
		task.MemoryBytes = f.MemoryBytes
		task.CPUMillis = f.CPUMillis
		task.EstimatedDuration = 100 * time.Millisecond
		task.SLODeadline = firstReady.Add(time.Duration(f.SLOMS) * time.Millisecond)
		out = append(out, task)
	}
	return out, rows.Err()
}

func (s *Store) ListOutbox(ctx context.Context, limit int) ([]domain.Event, error) {
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	rows, e := s.Pool.Query(ctx, `SELECT id,kind,COALESCE(invocation_id::text,''),run_id::text,generation,created_at FROM scheduler_outbox WHERE delivered_at IS NULL ORDER BY id LIMIT $1`, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []domain.Event{}
	for rows.Next() {
		var v domain.Event
		if e = rows.Scan(&v.ID, &v.Kind, &v.InvocationID, &v.RunID, &v.Generation, &v.CreatedAt); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) MarkDelivered(ctx context.Context, id int64) error {
	_, e := s.Pool.Exec(ctx, `UPDATE scheduler_outbox SET delivered_at=clock_timestamp() WHERE id=$1`, id)
	return e
}
func (s *Store) GetInvocation(ctx context.Context, id string) (domain.Invocation, error) {
	v, e := scanInvocation(s.Pool.QueryRow(ctx, invocationSelect+` WHERE id=$1`, id))
	return v, missing(e, "invocation")
}
func (s *Store) ListRuns(ctx context.Context, workflow, state string, limit int, after string) ([]domain.Run, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, e := s.Pool.Query(ctx, runSelect+` WHERE ($1='' OR workflow_id=$1) AND ($2='' OR state=$2) AND ($3='' OR id::text>$3) ORDER BY id LIMIT $4`, workflow, state, after, limit)
	if e != nil {
		return nil, e
	}
	ids := []string{}
	for rows.Next() {
		var r domain.Run
		var input, output []byte
		if e = rows.Scan(&r.ID, &r.WorkflowID, &r.WorkflowVersion, &r.State, &input, &output, &r.Priority, &r.SLOMS, &r.CreatedAt, &r.Deadline, &r.FinishedAt, &r.Error, &r.TraceParent, &r.TraceState, &r.ParentSpanID); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, r.ID)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	out := []domain.Run{}
	for _, id := range ids {
		r, e := s.GetRun(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, nil
}
