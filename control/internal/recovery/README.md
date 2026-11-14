# Recovery

Lease/session expiry, delayed retries, cancellation cleanup and Run finalization; initial scheduler rebuild before readiness.

## Interfaces and dependencies

Recover, Run; delegates authoritative mutations to repository Sweep and projection.

## Lifecycle and failures

A periodic loop uses the documented Run/Session/Invocation/Attempt lock order. Unexpired leases survive controller restart. Failed database iterations are logged and retried.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
