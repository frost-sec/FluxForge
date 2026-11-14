package postgres

import (
	"context"
	"encoding/json"
	"fluxforge-control/internal/domain"
	"github.com/jackc/pgx/v5"
)

func (s *Store) PublishFunction(ctx context.Context, f domain.FunctionSpec) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO function_versions(function_id,version,spec,digest) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, f.ID, f.Version, encoded(f), domain.Hash(f))
	if err != nil {
		return err
	}
	var hash string
	if err = s.Pool.QueryRow(ctx, `SELECT digest FROM function_versions WHERE function_id=$1 AND version=$2`, f.ID, f.Version).Scan(&hash); err != nil {
		return err
	}
	if hash != domain.Hash(f) {
		return domain.E(domain.Conflict, "function version already contains different content")
	}
	return nil
}
func (s *Store) GetFunction(ctx context.Context, id, version string) (domain.FunctionSpec, error) {
	var f domain.FunctionSpec
	var b []byte
	err := s.Pool.QueryRow(ctx, `SELECT spec FROM function_versions WHERE function_id=$1 AND version=$2`, id, version).Scan(&b)
	if err != nil {
		return f, missing(err, "function version")
	}
	err = json.Unmarshal(b, &f)
	return f, err
}

func (s *Store) PublishWorkflow(ctx context.Context, w domain.WorkflowSpec) error {
	return s.Tx(ctx, func(tx pgx.Tx) error {
		for _, step := range w.Steps {
			var present bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM function_versions WHERE function_id=$1 AND version=$2)`, step.Function.ID, step.Function.Version).Scan(&present); err != nil {
				return err
			}
			if !present {
				return domain.E(domain.NotFound, "function %s:%s not found", step.Function.ID, step.Function.Version)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_versions(workflow_id,version,spec,digest) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, w.ID, w.Version, encoded(w), domain.Hash(w)); err != nil {
			return err
		}
		var hash string
		if err := tx.QueryRow(ctx, `SELECT digest FROM workflow_versions WHERE workflow_id=$1 AND version=$2`, w.ID, w.Version).Scan(&hash); err != nil {
			return err
		}
		if hash != domain.Hash(w) {
			return domain.E(domain.Conflict, "workflow version already contains different content")
		}
		return nil
	})
}
func (s *Store) GetWorkflow(ctx context.Context, id, version string) (domain.WorkflowSpec, error) {
	var w domain.WorkflowSpec
	var b []byte
	err := s.Pool.QueryRow(ctx, `SELECT spec FROM workflow_versions WHERE workflow_id=$1 AND version=$2`, id, version).Scan(&b)
	if err != nil {
		return w, missing(err, "workflow version")
	}
	err = json.Unmarshal(b, &w)
	return w, err
}
