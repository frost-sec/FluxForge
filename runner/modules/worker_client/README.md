# worker_client

Sources: include/fluxforge/worker_client.hpp.

## Purpose and implementation

Creates the Host using socketpair/fork/exec before network threads and correlates concurrent requests through futures.

## Interfaces, dependencies and ownership

Only the reader thread fulfills promises. A mutex serializes frame writes. A separate promise map mutex protects completion; Host disconnect rejects outstanding futures.

## Configuration

Host/Worker paths default to siblings of the Agent executable; FF_WORKER_HOST and FF_WORKER_BINARY override them.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
