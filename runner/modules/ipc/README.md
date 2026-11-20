# ipc

Sources: include/fluxforge/ipc.hpp; proto/worker.proto.

## Purpose and implementation

Four-byte network-endian body length followed by WorkerEnvelope protobuf. The envelope carries protocol/correlation/Attempt/generation; the operation-specific body is JSON.

The private Zygote launch channel uses bounded Unix seqpackets with `SCM_RIGHTS` for socket and log descriptors. The new child sends its pidfd over its dedicated channel and waits for Host to grant launch. Worker result frames travel directly to Host over the per-Attempt stream. A closed channel before the grant makes the child exit.

## Interfaces, dependencies and ownership

Handles split/coalesced frames, short writes, EINTR/EAGAIN and disconnects. Both envelope and body identities must agree; stale Worker generations cannot complete a new Attempt.

## Configuration

Protocol version 1; max encoded frame 1MiB; decoder buffer and Host pending writes are bounded.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
