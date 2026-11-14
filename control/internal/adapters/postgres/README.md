# PostgreSQL adapter

Authoritative immutable catalog, Run/Invocation/Attempt state, replay records, fan-in transactions, recovery, object metadata and policies.

## Interfaces and dependencies

Store.New, Migrate and repository methods. Objects use ObjectRepository.Init separately. SQL is confined to this adapter.

## Lifecycle and failures

READ COMMITTED plus explicit lock ordering and uniqueness constraints. Fan-in consumes edges and decrements child indegree in one transaction. SQL deadlock/serialization retries are bounded. Integration tests create/drop only randomly named ff_test_* schemas with explicit FF_TEST_DSN.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
