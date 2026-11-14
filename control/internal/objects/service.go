// Package objects owns claim-check references and short-lived transfer grants.
package objects

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"fluxforge-control/internal/domain"
)

type Upload struct {
	ID        string
	Purpose   string
	Ref       domain.ObjectRef
	Committed bool
	CreatedAt time.Time
}
type Repository interface {
	Create(context.Context, Upload) error
	Find(context.Context, string) (Upload, error)
	Commit(context.Context, string, string) (domain.ObjectRef, error)
}
type Backend interface {
	UploadURL(context.Context, string, bool) (string, error)
	UploadedVersion(context.Context, string, int64) (string, error)
	DownloadURL(context.Context, domain.ObjectRef, bool) (string, error)
}
type Service struct {
	repo     Repository
	backend  Backend
	bucket   string
	maxBytes int64
}

func New(repo Repository, backend Backend, bucket string) *Service {
	return &Service{repo: repo, backend: backend, bucket: bucket, maxBytes: 64 << 20}
}
func (s *Service) Begin(ctx context.Context, purpose string, size int64, digest, contentType string, internal bool) (Upload, string, error) {
	if purpose != "FUNCTION_ARTIFACT" && purpose != "RUN_INPUT" && purpose != "ATTEMPT_OUTPUT" {
		return Upload{}, "", domain.E(domain.Invalid, "unknown upload purpose")
	}
	b, e := hex.DecodeString(digest)
	if e != nil || len(b) != 32 || size <= 0 || size > s.maxBytes {
		return Upload{}, "", domain.E(domain.Invalid, "sha256 and object size (1..64MiB) required")
	}
	if contentType == "" {
		contentType = "application/json"
	}
	if len(contentType) > 128 {
		return Upload{}, "", domain.E(domain.Invalid, "invalid content type")
	}
	id := domain.NewID()
	u := Upload{ID: id, Purpose: purpose, Ref: domain.ObjectRef{ID: id, Bucket: s.bucket, Key: strings.ToLower(purpose) + "/" + id, SHA256: strings.ToLower(digest), Size: size, ContentType: contentType}, CreatedAt: time.Now().UTC()}
	// Generate the grant outside the persistence transaction.
	grant, e := s.backend.UploadURL(ctx, u.Ref.Key, internal)
	if e != nil {
		return Upload{}, "", e
	}
	if e = s.repo.Create(ctx, u); e != nil {
		return Upload{}, "", e
	}
	return u, grant, nil
}
func (s *Service) Commit(ctx context.Context, id string) (domain.ObjectRef, error) {
	u, e := s.repo.Find(ctx, id)
	if e != nil {
		return domain.ObjectRef{}, e
	}
	if u.Committed {
		return u.Ref, nil
	}
	version, e := s.backend.UploadedVersion(ctx, u.Ref.Key, u.Ref.Size)
	if e != nil {
		return domain.ObjectRef{}, e
	}
	if version == "" || version == "null" {
		return domain.ObjectRef{}, errors.New("object store must enable bucket versioning")
	}
	return s.repo.Commit(ctx, id, version)
}
func (s *Service) Resolve(ctx context.Context, ref domain.ObjectRef) (domain.ObjectRef, error) {
	if ref.ID == "" {
		return domain.ObjectRef{}, domain.E(domain.Invalid, "object_id required")
	}
	u, e := s.repo.Find(ctx, ref.ID)
	if e != nil {
		return domain.ObjectRef{}, e
	}
	if !u.Committed {
		return domain.ObjectRef{}, domain.E(domain.Conflict, "object not committed")
	}
	if (ref.SHA256 != "" && !strings.EqualFold(ref.SHA256, u.Ref.SHA256)) || (ref.Size != 0 && ref.Size != u.Ref.Size) {
		return domain.ObjectRef{}, domain.E(domain.Invalid, "object metadata mismatch")
	}
	return u.Ref, nil
}
func (s *Service) ValidatePayload(ctx context.Context, p *domain.PayloadRef) error {
	if p == nil {
		return domain.E(domain.Invalid, "payload required")
	}
	if p.Object == nil {
		return nil
	}
	if len(p.Inline) > 0 {
		return domain.E(domain.Invalid, "inline and object are exclusive")
	}
	ref, e := s.Resolve(ctx, *p.Object)
	if e != nil {
		return e
	}
	p.Object = &ref
	return nil
}
func (s *Service) Download(ctx context.Context, id string, internal bool) (domain.ObjectRef, string, error) {
	ref, e := s.Resolve(ctx, domain.ObjectRef{ID: id})
	if e != nil {
		return ref, "", e
	}
	u, e := s.backend.DownloadURL(ctx, ref, internal)
	return ref, u, e
}
