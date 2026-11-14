# Runner registry

Runner boot sessions, heartbeats, capacity queries and ACTIVE/DRAINING/OFFLINE policies.

## Interfaces and dependencies

Register, Heartbeat, GetSession, ListSessions, Capacity, SetPolicy, SetDraining.

## Lifecycle and failures

Persistent sessions fence restarts. Drain acknowledgment requires observed DRAINING and zero local active/pending work. Expiry handling is performed by recovery.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
