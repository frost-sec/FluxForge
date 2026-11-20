# Reproducible local load sample

Measured 2026-11-03 using Docker Desktop Linux/amd64. Command:

```powershell
docker compose -f compose.yml --profile tools run --rm --no-deps ffctl benchmark --count 40 --verify-scale
```

40 one-node Runs, each calling the bounded `delay` fixture for 200ms. Default pool: min1/max4 Runners, 2 slots each. The test started with existing local image/tool caches; it is not an isolated cold-machine startup test.

| Interval | Count | Mean ms | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Run durable accept -> terminal | 40 | 1390.25 | 1474 | 1951 | 2048 |
| Invocation Ready -> terminal | 40 | 1381.95 | 1454 | 1945 | 2040 |
| Worker execution | 40 | 200.275 | 200 | 201 | 201 |

All 40 Runs succeeded. Live pool reached 8 slots (4 Runners), then returned to 2 slots after the default60s low-load window and drain acknowledgment; `idle_shrink_verified=true`.

These are exact sample quantiles from stored timestamps/durations, distinct from approximate histogram bucket quantiles in the live metrics API. The difference between execution and end-to-end latency includes queueing, cold preparation and control/persistence work. Exceeding the function soft SLO did not cancel accepted work.

This validates functional scaling behavior for one synthetic burst. It is not a throughput benchmark, a production SLO guarantee or evidence of microsecond/zero-copy execution. CPU-heavy workloads, cold images, external object-storage latency and larger dependency graphs require their own measurements.

## Zygote preparation sample

Measured 2026-11-21 on Docker Desktop Linux/amd64, with 2 CPUs, 1 GiB memory, a read-only root filesystem, and UID 65532. The `worker_lifecycle` test in `runner/tests/worker_test.cpp` uses a fixture whose library constructor initializes an 8 MiB heap buffer. Twenty pairs alternate a new environment key with an immediate hit on that key. Times cover the local Host request through `PREPARED`, including the leaf fork on both paths and library initialization on a miss.

| Prepare path | p50 (ms) | p95 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: |
| Function-Zygote miss | 10.467 | 18.827 | 28.735 |
| Function-Zygote hit | 1.689 | 2.317 | 15.914 |

With two parked Workers from one seed, Linux `smaps_rollup` reported 144 KiB private for the seed and 52 KiB private per Worker. The sample was taken before either Worker ran the handler. The same test verifies that each call starts from the initialized state after writes in another Worker.

These are small local samples from the new runtime's miss and hit paths. They exclude artifact download, queueing, function execution and database work. The previous release has no directly comparable sample here.
