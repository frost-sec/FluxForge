package postgres

import (
	"context"
	"fluxforge-control/internal/domain"
)

type SLOView struct {
	Run   domain.Run
	Nodes []domain.Invocation
}

// Two bounded metadata queries avoid an N+1 query per active Run and never load payload bytes.
func (s *Store) ActiveSLOViews(ctx context.Context) ([]SLOView, error) {
	rows, e := s.Pool.Query(ctx, `SELECT id::text,workflow_id,workflow_version,created_at,deadline,slo_ms FROM runs WHERE state IN ('PENDING','RUNNING') ORDER BY created_at LIMIT 1000`)
	if e != nil {
		return nil, e
	}
	var out []SLOView
	byID := map[string]int{}
	ids := []string{}
	for rows.Next() {
		var v SLOView
		if e = rows.Scan(&v.Run.ID, &v.Run.WorkflowID, &v.Run.WorkflowVersion, &v.Run.CreatedAt, &v.Run.Deadline, &v.Run.SLOMS); e != nil {
			rows.Close()
			return nil, e
		}
		byID[v.Run.ID] = len(out)
		ids = append(ids, v.Run.ID)
		out = append(out, v)
	}
	e = rows.Err()
	rows.Close()
	if e != nil || len(ids) == 0 {
		return out, e
	}
	rows, e = s.Pool.Query(ctx, `SELECT i.run_id::text,i.id::text,i.step_name,i.state,COALESCE(a.started_at,i.started_at),i.not_before FROM invocations i LEFT JOIN attempts a ON a.id=i.current_attempt_id WHERE i.run_id=ANY($1::uuid[]) ORDER BY i.run_id,i.id LIMIT 20000`, ids)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	for rows.Next() {
		var n domain.Invocation
		if e = rows.Scan(&n.RunID, &n.ID, &n.StepName, &n.State, &n.StartedAt, &n.NotBefore); e != nil {
			return nil, e
		}
		if at, ok := byID[n.RunID]; ok {
			out[at].Nodes = append(out[at].Nodes, n)
		}
	}
	return out, rows.Err()
}
