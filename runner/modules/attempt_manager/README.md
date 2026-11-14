# attempt_manager

Sources: include/fluxforge/attempt_manager.hpp; include/fluxforge/agent.hpp.

## Purpose and implementation

Owns local Attempt records, execution threads, logical slots, monotonic lease safety and acknowledgement handling.

## Interfaces, dependencies and ownership

Phases are Preparing, Running, Uploading, Completing and Done. Revocation cancels the Host execution; stale outputs are discarded.

## Configuration

Slot count comes from RUNNER_SLOTS. A slot stays occupied until acknowledged or revoked. One job per Attempt ID prevents replay from executing twice.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
