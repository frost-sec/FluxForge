# Workflow

Submit, query, pagination and cancellation of durable Runs. Input binding and graph normalization live in domain; transactions live in the repository.

## Interfaces and dependencies

Submit, GetRun, ListRuns, ListInvocations, ListAttempts, CancelRun.

## Lifecycle and failures

Submission persists before acknowledgment; missing trace context is generated independently of request identity. Cancellation is durable and completes asynchronously through recovery.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
