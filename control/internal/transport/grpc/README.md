# Business gRPC adapters

Map protobuf messages, domain validation/errors and service calls. Public Catalog/Workflow/Observability; internal RunnerQueue/QueueAdmin.

## Interfaces and dependencies

All RPCs are defined in proto/fluxforge_v1.proto; ObjectService is a separate objectgrpc adapter. Limits, page bounds, lease outcomes and policy CAS are enforced here.

## Lifecycle and failures

Listener authentication is the bootstrap Bearer interceptor. Transport does not issue SQL or Docker calls. QueueAdmin policies validate before durable CAS and apply to running scheduler/autoscaler.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
