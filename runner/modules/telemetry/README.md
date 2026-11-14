# telemetry

Sources: include/fluxforge/metrics.hpp; include/fluxforge/agent.hpp.

## Purpose and implementation

Per-function cumulative execution histograms, outcomes and cold/warm counts; approximately once per second ReportMetrics.

## Interfaces, dependencies and ownership

Retries preserve report sequence and cumulative contents; new snapshots advance sequence. Control merges buckets and deduplicates. Detailed spans are reconstructed in the control telemetry hooks; Runner logs carry Run/Invocation/Attempt/Trace IDs.

## Configuration

Fixed 14 finite bucket boundaries in seconds plus +Inf, non-cumulative bin counts. At most 1024 function/version/runtime series per Agent.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
