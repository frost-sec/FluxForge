# Implementation status

Accepted baseline: 2026-11-02. Verification updated 2026-11-03 (Asia/Hong_Kong). See IMPLEMENTATION_PLAN.md for the accepted contract.

| Stage | Status | Evidence |
| --- | --- | --- |
| A: modules, contracts, Docker toolchain, docs | Implemented | Go/C++ tool and runtime images built; per-module READMEs; Compose validated |
| B: persistent execution and leases | Verified | Docker `go test -race ./...`, real PG claim/completion replay and expiry fencing |
| C: DAG and recovery | Verified | Parallel fan-in, cancellation, retired boots, recovery rebuild and missing-index repair integration tests |
| D: native runtime and claim-check | Verified | CTest 2/2; full A->[B,C]->D yields value=10; 256KiB input/output; crash retries; running cancellation; execution timeout |
| E: scheduler | Verified | Race tests: independent shards, local/global/steal, resources, delay, priority aging, generation and session fencing |
| F: observability and autoscaling | Verified | OTLP parent/stage export and W3C propagation tests; live load scaled to 8 slots then shrank to 2 after idle window and acknowledgments |

## Reproducible verification

- All compilation, generation, formatting, tests and packaging used Docker; no host compiler was installed.
- Final source was rebuilt into Control/CLI images, rolled into the development deployment, and verified through `scripts/ff.cmd demo` (value=10). The standalone function SDK image also compiled echo.cpp successfully.
- `go test -race ./...` with FF_TEST_DSN passes against PostgreSQL. Tests own random ff_test_* schemas and clean up only those schemas.
- The documented Compose `go-test` entry point passes the full race suite and real PostgreSQL integration tests with deployment-provided configuration.
- Runner release-image build runs CTest (IPC framing plus worker lifecycle: warm reuse, schemas, timeout, cancellation, crash, log flood, large output and output bounds).
- `ffctl verify` passes Submit replay/conflict, direct MinIO large input/output and digest verification, COMPLETING upload, 3 crash attempts, one timeout attempt, cancellation of a running function, recovery to healthy fan-in, and independent seconds metrics.
- OpenTelemetry HTTP/protobuf exporter test verifies caller -> Run -> Invocation -> Attempt parent IDs, root timestamps and stage spans.
- Controller/Runner code and policy changes use public/internal listeners and the project-owned Docker labels. New deployment uses fluxforge-v1 resources; legacy ff-postgres/ff-minio volumes were not erased.
- Full safe down/up lifecycle passed: pause -> drain -> stop -> restart -> resume, existing catalog/results retained, fan-in still returns value=10.
- Synthetic 40-Run/200ms workload completed all Runs; peak 4 Runners/8 slots, idle shrink verified. Run p99=2048ms; execution p99=201ms. See BENCHMARK.md for scope and exact samples.

## Explicit boundaries

- Team-owned C++20 ABI-v1 functions, Linux/amd64, one Docker host. Not a hostile multi-tenant sandbox or an HA control plane.
- Inline JSON 64KiB; Runner input/output default limit 16MiB. Function artifact/object upload maximum 64MiB. Resource settings and timeouts are explicit.
- Histograms produce approximate bucket-bound quantiles; SQL history is limited to recent 24h / 10000 samples with 30s estimate caching. Soft SLO is a capacity risk, not an execution cancellation deadline or an achieved latency guarantee.
- Telemetry is best effort; lost terminal-event delivery acknowledgments may replay a metric after a crash. Business state and graph progression remain durable/idempotent.
- Committed objects are version-pinned and retained. Automatic object garbage collection is not implemented; no background deletion of user results is performed.
- Optional Collector/Jaeger/Prometheus Compose configuration is provided. OTLP is verified with a real HTTP test receiver; the optional external observability images have not been launched during this validation.

