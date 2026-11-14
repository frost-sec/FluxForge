# Control executable

Read configuration and run the modular application with signal-controlled shutdown.

## Interfaces and dependencies

main -> config.Load -> bootstrap.Run.

## Lifecycle and failures

No catalog, queue or execution implementation belongs in this entry point. Run using the control runtime image; HTTP /livez /readyz /metrics and legacy /ping are separate from business gRPC.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
