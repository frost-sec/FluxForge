# Projection

Project durable Ready/outcome events into the disposable scheduler, metrics and trace hooks.

## Interfaces and dependencies

Rebuild, Sync, Wake, Run; repository reads current state, not stale event payloads.

## Lifecycle and failures

One projection mutex serializes event consumption; queue locks are internal and never span I/O. Repeated Ready generations deduplicate. Observability is best effort; delivery replay may repeat a terminal metric after a crash.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
