# Configuration

Read container environment without starting external resources.

## Interfaces and dependencies

Load() Config. DB_DSN; MINIO_ENDPOINT/PUBLIC_ENDPOINT/ACCESS_KEY/SECRET_KEY/BUCKET; PUBLIC_ADDR; INTERNAL_ADDR; HTTP_ADDR; FF_PUBLIC_TOKEN; FF_INTERNAL_TOKEN; FF_PROJECT; RUNNER_IMAGE; NETWORK_NAME; AUTOSCALING_ENABLED; MIN_RUNNERS/MAX_RUNNERS; RUNNER_SLOTS; AFFINITY_DELAY_MS; OTEL_EXPORTER_OTLP_ENDPOINT.

## Lifecycle and failures

The Compose file is the supported local default configuration. No host compiler or host SDK is used. Runtime policy changes are versioned separately in PostgreSQL.

## Current support and validation

Implementation is present; verified evidence and remaining limitations are tracked in docs/IMPLEMENTATION_STATUS.md. Build and test inside fluxforge-go-tools:dev, working directory /workspace/control, with `go test -race ./...`. PostgreSQL integration additionally requires FF_TEST_DSN and an isolated schema. Use the root PowerShell/Docker commands; do not install tools on the host.

Container configuration and startup commands are in the root README and compose.yml. Architectural boundaries are fixed in docs/IMPLEMENTATION_PLAN.md.
