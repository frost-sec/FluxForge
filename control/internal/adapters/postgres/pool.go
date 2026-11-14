package postgres

import (
	"context"
	"fluxforge-control/internal/domain"
	"time"
)

type ActiveWork struct {
	FunctionID, FunctionVersion string
	State                       string
	StartedAt                   *time.Time
	CreatedAt                   time.Time
	TimeoutMS                   int64
}

func (s *Store) ActiveWork(ctx context.Context) ([]ActiveWork, error) {
	rows, e := s.Pool.Query(ctx, `SELECT i.function_id,i.function_version,a.state,a.started_at,a.created_at,(f.spec->>'timeout_ms')::bigint FROM attempts a JOIN invocations i ON i.id=a.invocation_id JOIN function_versions f ON f.function_id=i.function_id AND f.version=i.function_version WHERE a.state IN ('LEASED','RUNNING','COMPLETING')`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []ActiveWork
	for rows.Next() {
		var v ActiveWork
		if e = rows.Scan(&v.FunctionID, &v.FunctionVersion, &v.State, &v.StartedAt, &v.CreatedAt, &v.TimeoutMS); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) RunSLORisk(ctx context.Context) (bool, error) {
	var v bool
	e := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runs WHERE state IN ('PENDING','RUNNING') AND created_at+(slo_ms*interval '1 millisecond')<clock_timestamp()+interval '500 milliseconds')`).Scan(&v)
	return v, e
}

type HistoricalEstimate struct {
	Function domain.FunctionRef
	Duration time.Duration
}

func (s *Store) HistoricalEstimates(ctx context.Context) ([]HistoricalEstimate, error) {
	rows, e := s.Pool.Query(ctx, `SELECT function_id,function_version,percentile_cont(0.99) WITHIN GROUP(ORDER BY duration_ms) FROM (SELECT i.function_id,i.function_version,a.duration_ms FROM attempts a JOIN invocations i ON i.id=a.invocation_id WHERE a.state='SUCCEEDED' AND a.finished_at>clock_timestamp()-interval '24 hours' ORDER BY a.finished_at DESC LIMIT 10000) recent GROUP BY function_id,function_version`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []HistoricalEstimate
	for rows.Next() {
		var v HistoricalEstimate
		var ms float64
		if e = rows.Scan(&v.Function.ID, &v.Function.Version, &ms); e != nil {
			return nil, e
		}
		v.Duration = time.Duration(ms * float64(time.Millisecond))
		out = append(out, v)
	}
	return out, rows.Err()
}
