# MinIO adapter

Implements objects.Backend using minio-go/v7. Ensure creates the deployment bucket and enables versioning. Public and container-network endpoints use separate signing clients; download grants include the committed versionId. Credentials remain in the control process.

Configuration: MINIO_ENDPOINT, MINIO_PUBLIC_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET. Startup fails readiness if the bucket cannot be prepared. Integration tests exercise upload/commit/download in Docker; no host SDK is required.
