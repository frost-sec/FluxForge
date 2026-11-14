# Catalog

Immutable function/workflow publication, Draft7 schema compilation, function-reference checks and DAG normalization.

## Interfaces and dependencies

PublishFunction, GetFunction, PublishWorkflow, GetWorkflow; adapters implement Repository and object Resolver.

## Lifecycle and failures

No background goroutines. Publication resolves canonical committed ObjectRefs before writing metadata. Invalid definitions and version conflicts do not create execution state.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
