# worker_runtime

Sources: src/worker/main.cpp; ../sdk/cpp/include/fluxforge/function.h.

## Purpose and implementation

After exec, dlopen one immutable function library, validate C ABI, invoke serially, validate Draft7 input/output and return only metadata.

## Interfaces, dependencies and ownership

Output writers are host-owned and bounded; no allocator ownership crosses ABI. Invalid JSON/schema, ABI failure, crash or timeout causes Worker replacement.

## Configuration

Schema validator pboettch/json-schema-validator 2.3.0; no remote reference loader. Worker address space is constrained before exec.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
