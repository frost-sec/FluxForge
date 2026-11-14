# Claim-check objects

Purpose: begin direct uploads, freeze an uploaded MinIO object version, resolve opaque references and issue expiring download grants. Implemented Begin/Commit/Download and metadata normalization. Large bytes never pass through the control service. Maximum stored object 64MiB; execution payload limits are enforced by Runner separately.

Interfaces: Service.Begin/Commit/Resolve/ValidatePayload/Download; Repository and Backend are injected. The gRPC adapter exposes ObjectService on public/internal endpoints with the appropriate signing host. A commit checks size/existence; consumers verify SHA256. Bucket versioning pins committed references even if an unexpired PUT URL is reused.

Repository owns metadata, backend owns credentials; user functions receive neither credentials nor arbitrary remote object locations. Temporary uploads are retained until explicit lifecycle cleanup; automatic object GC is not yet implemented. Build/test with the Go Docker tool service. See the root README for commands and MINIO_* configuration.
