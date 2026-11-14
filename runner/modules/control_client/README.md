# control_client

Sources: include/fluxforge/control_client.hpp.

## Purpose and implementation

Deadline-bounded Register/Heartbeat/Acquire/Started/ExecutionFinished/Renew/Completion/Release/Metrics gRPC calls with Bearer metadata.

## Interfaces, dependencies and ownership

Owns the gRPC stub, not execution state. Lease/validation errors revoke work; retryable failures are bounded by the current lease.

## Configuration

FF_INTERNAL_TOKEN and QUEUE_ADDR. Transport failure retries use the same semantic request; per-RPC deadlines are explicit.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
