# worker_runtime

Sources: src/worker/main.cpp, src/worker/runtime.cpp and sdk/cpp/include/fluxforge/function.h.

## Purpose and implementation

Base-Zygote starts the C++ process and serves fork commands. Function-Zygote loads one immutable `.so`, validates the ABI and serves fork commands with that library mapped. Each leaf inherits initialized state, handles one invocation, validates Draft7 input/output, returns metadata and exits with `_exit`.

## Interfaces, dependencies and ownership

Output writers are host-owned and bounded. Invalid JSON or schema becomes a user failure. An invalid ABI or loader error invalidates seed preparation. A leaf crash or timeout leaves the seed eligible for reuse. Parent-death signals cascade from Host to Base-Zygote, seed and leaf. See the SDK README for constructor and per-invocation cleanup requirements.

## Configuration

Schema validator pboettch/json-schema-validator 2.3.0 uses local schemas. Base starts with a clean environment. A Function-Zygote applies `RLIMIT_AS`, `RLIMIT_FSIZE`, `RLIMIT_NOFILE` and `RLIMIT_CORE` before loading its library, and leaves inherit these limits.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
