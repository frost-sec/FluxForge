# Bootstrap

Assemble infrastructure, business services, policy hooks, background loops and public/internal listeners.

## Interfaces and dependencies

Run(ctx, config.Config). Owns pool, fencing connection, tracer and listener lifetimes.

## Lifecycle and failures

Startup migrates PostgreSQL, pins controller advisory lock, prepares MinIO versioning, recovers queues, then opens readiness. Loss of fencing connection stops the process. HTTP shutdown and gRPC draining have bounded waits.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
