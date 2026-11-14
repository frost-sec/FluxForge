# Execution

Coordinate scheduler reservations with PostgreSQL claims and Attempt ownership, including lease renewals and result submission.

## Interfaces and dependencies

Acquire, Started, ExecutionFinished, Renew, Complete, Release, RefreshRunner; injected Projection and PayloadValidator.

## Lifecycle and failures

Never hold heap locks across SQL. A replay keeps original slots/wait parameters. Completion retries do not rerun functions; large results enter COMPLETING before upload.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
