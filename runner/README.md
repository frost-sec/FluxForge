# C++ execution layer

Linux/amd64 C++20 Agent -> independent single-threaded Worker Host -> warm Worker processes. Each business module is documented in modules/<name>/README.md. Actual implementation is in small include/fluxforge headers and src/worker_host, src/worker entry points. Worker Host and Worker do not link gRPC; only Agent does. All targets and examples are compiled in Docker.

## Build and test

Use fluxforge-toolchain:dev with the repository mounted at /workspace. Run cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON -DCMAKE_BUILD_TYPE=Debug, cmake --build /workspace/runner/build-agent -j 4 and ctest --test-dir /workspace/runner/build-agent --output-on-failure. FF_BUILD_AGENT=OFF still builds/tests the standalone execution layer and generates the local Protobuf IPC envelope.

Tests cover split/coalesced/oversized/forged frames, warm PID reuse, prepare reservation, schema failure, infinite-loop timeout, cancellation, crash/recovery, log flooding, large local output and output-size enforcement. Full gRPC/database/MinIO/recovery/scaling behavior requires integration tests; CTest does not claim those guarantees.

## Resource and failure boundary

The container must set CPU, memory and PID limits and provide an executable bounded /tmp (dlopen needs executable mappings). RLIMIT_AS is address space, not strict RSS. Only team-owned functions are supported; no privileged container or host cgroup delegation is required. Function code must not fork or retain ABI input pointers. Static state persists across warm calls; worker recycling bounds lifetime, not perfect per-call memory isolation.

The host schema dependency is pinned to json-schema-validator 2.3.0. gRPC is located through CMake config or distro pkg-config. Protocols are generated into the build directory. See ../docs/IMPLEMENTATION_STATUS.md for integration evidence and limitations.
