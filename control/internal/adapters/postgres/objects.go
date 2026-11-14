package postgres

import (
	"context"
	"errors"
	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/objects"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ObjectRepository struct{ Pool *pgxpool.Pool }

func (r *ObjectRepository) Init(ctx context.Context) error {
	_, e := r.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS objects(
 id uuid PRIMARY KEY,purpose text NOT NULL,bucket text NOT NULL,key text NOT NULL UNIQUE,
 size_bytes bigint NOT NULL CHECK(size_bytes>0),sha256 text NOT NULL,content_type text NOT NULL,
 version_id text NOT NULL DEFAULT '',committed boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(),committed_at timestamptz)`)
	return e
}
func (r *ObjectRepository) Create(ctx context.Context, u objects.Upload) error {
	_, e := r.Pool.Exec(ctx, `INSERT INTO objects(id,purpose,bucket,key,size_bytes,sha256,content_type)VALUES($1,$2,$3,$4,$5,$6,$7)`, u.ID, u.Purpose, u.Ref.Bucket, u.Ref.Key, u.Ref.Size, u.Ref.SHA256, u.Ref.ContentType)
	return e
}
func (r *ObjectRepository) Find(ctx context.Context, id string) (objects.Upload, error) {
	var u objects.Upload
	e := r.Pool.QueryRow(ctx, `SELECT id::text,purpose,bucket,key,size_bytes,sha256,content_type,version_id,committed,created_at FROM objects WHERE id::text=$1`, id).Scan(&u.ID, &u.Purpose, &u.Ref.Bucket, &u.Ref.Key, &u.Ref.Size, &u.Ref.SHA256, &u.Ref.ContentType, &u.Ref.VersionID, &u.Committed, &u.CreatedAt)
	if errors.Is(e, pgx.ErrNoRows) {
		e = domain.E(domain.NotFound, "object not found")
	}
	u.Ref.ID = u.ID
	return u, e
}
func (r *ObjectRepository) Commit(ctx context.Context, id, version string) (domain.ObjectRef, error) {
	_, e := r.Pool.Exec(ctx, `UPDATE objects SET committed=true,version_id=$2,committed_at=clock_timestamp() WHERE id::text=$1 AND NOT committed`, id, version)
	if e != nil {
		return domain.ObjectRef{}, e
	}
	u, e := r.Find(ctx, id)
	return u.Ref, e
}
