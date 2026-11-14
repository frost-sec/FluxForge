# artifact_cache

Sources: include/fluxforge/object_io.hpp (ArtifactCache).

## Purpose and implementation

Downloads .so artifacts once per SHA256, verifies size/hash, publishes by rename, shares concurrent requests and evicts unpinned LRU entries.

## Interfaces, dependencies and ownership

A Handle pins an entry during preparation/execution. Idle Worker mappings remain valid if a cache file is evicted. Transfer failure removes temporary files and permits retry.

## Configuration

Maximum artifact 64MiB; in-process cache target 128MiB. Files live under FF_WORK_DIR/artifacts.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
