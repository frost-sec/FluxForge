# app

Sources: src/main.cpp; include/fluxforge/config.hpp; include/fluxforge/agent.hpp.

## Purpose and implementation

Entry point, configuration and lifecycle wiring. Starts the independent Host before gRPC/curl threads; initializes signals and closes services in order.

## Interfaces, dependencies and ownership

Linux/amd64 only. Docker runtime supplies resource limits and init; no shell or host compiler is required.

## Configuration

QUEUE_ADDR=control-plane:9091, FF_INTERNAL_TOKEN, FF_RUNTIME_DIGEST=cpp20-linux-amd64-v1, RUNNER_ID (container hostname by default), RUNNER_SLOTS=2, RUNNER_MEMORY_BYTES=1073741824, RUNNER_CPU_MILLIS=2000, RUNNER_SHUTDOWN_GRACE_MS=45000, FF_WORK_DIR=/tmp/fluxforge.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
