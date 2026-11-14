# Go control module

One Go module/process contains the catalog, workflow/execution services, runner registry, scheduler, projection/recovery, object grants, telemetry and autoscaler. Each internal business package has its own README. The app entry point only assembles focused services/adapters.

Public/internal gRPC and health/metrics ports are configured by Compose. PostgreSQL owns state; scheduler indexes are reconstructible. Bulk data uses direct MinIO transfer. Docker adapter alone has container-management access.

All commands run in Docker: root scripts/ff.cmd build/test/up/integration/demo/verify. `test` includes Go race checks, `integration` additionally enables real PostgreSQL schema-isolated tests. CLI binaries are packaged in their own image; no Go compiler is required on the host.

See docs/IMPLEMENTATION_PLAN.md, docs/IMPLEMENTATION_STATUS.md and docs/API.md for accepted boundaries, verified evidence, APIs and known limitations.
