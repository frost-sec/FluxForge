// Package minio provides version-pinned, direct S3 transfer grants.
package minio

import (
	"context"
	"fluxforge-control/internal/domain"
	"fmt"
	sdk "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/url"
	"strings"
	"time"
)

type Config struct{ Endpoint, PublicEndpoint, AccessKey, SecretKey, Bucket string }
type Backend struct {
	internal, public *sdk.Client
	bucket           string
}

func New(c Config) (*Backend, error) {
	mk := func(endpoint string) (*sdk.Client, error) {
		if !strings.Contains(endpoint, "://") {
			endpoint = "http://" + endpoint
		}
		u, e := url.Parse(endpoint)
		if e != nil {
			return nil, e
		}
		if u.Host == "" {
			return nil, fmt.Errorf("invalid MinIO endpoint")
		}
		return sdk.New(u.Host, &sdk.Options{Creds: credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""), Secure: u.Scheme == "https"})
	}
	a, e := mk(c.Endpoint)
	if e != nil {
		return nil, e
	}
	if c.PublicEndpoint == "" {
		c.PublicEndpoint = c.Endpoint
	}
	b, e := mk(c.PublicEndpoint)
	if e != nil {
		return nil, e
	}
	return &Backend{a, b, c.Bucket}, nil
}
func (b *Backend) Ensure(ctx context.Context) error {
	ok, e := b.internal.BucketExists(ctx, b.bucket)
	if e != nil {
		return e
	}
	if !ok {
		if e = b.internal.MakeBucket(ctx, b.bucket, sdk.MakeBucketOptions{}); e != nil {
			return e
		}
	}
	return b.internal.SetBucketVersioning(ctx, b.bucket, sdk.BucketVersioningConfiguration{Status: "Enabled"})
}
func (b *Backend) client(internal bool) *sdk.Client {
	if internal {
		return b.internal
	}
	return b.public
}
func (b *Backend) UploadURL(ctx context.Context, key string, internal bool) (string, error) {
	u, e := b.client(internal).PresignedPutObject(ctx, b.bucket, key, 15*time.Minute)
	if e != nil {
		return "", e
	}
	return u.String(), nil
}
func (b *Backend) UploadedVersion(ctx context.Context, key string, size int64) (string, error) {
	o, e := b.internal.StatObject(ctx, b.bucket, key, sdk.StatObjectOptions{})
	if e != nil {
		return "", e
	}
	if o.Size != size {
		return "", fmt.Errorf("uploaded size %d does not match %d", o.Size, size)
	}
	return o.VersionID, nil
}
func (b *Backend) DownloadURL(ctx context.Context, ref domain.ObjectRef, internal bool) (string, error) {
	q := make(url.Values)
	q.Set("versionId", ref.VersionID)
	u, e := b.client(internal).PresignedGetObject(ctx, ref.Bucket, ref.Key, 15*time.Minute, q)
	if e != nil {
		return "", e
	}
	return u.String(), nil
}
