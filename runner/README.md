# C++ execution layer

Linux/amd64 C++20 Agent -> independent single-threaded Worker Host -> Base-Zygote -> Function-Zygote -> one-shot Worker. Each business module is documented in modules/<name>/README.md. The Host and Worker binaries use local UDS and link no gRPC. All targets and examples are compiled in Docker.

## Build and test

Use fluxforge-toolchain:dev with the repository mounted at /workspace. Run cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON -DCMAKE_BUILD_TYPE=Debug, cmake --build /workspace/runner/build-agent -j 4 and ctest --test-dir /workspace/runner/build-agent --output-on-failure. FF_BUILD_AGENT=OFF still builds/tests the standalone execution layer and generates the local Protobuf IPC envelope.

Tests cover split/coalesced/oversized/forged frames, the three-level process tree, concurrent preparation from one seed, pristine per-call state, prepared ownership, schema failure, timeout, cancellation, crash/recovery, log flooding and bounded large output. Full gRPC, database, MinIO and scaling coverage uses the separate integration suite.

## Resource and failure boundary

Docker must provide Linux pidfd and close_range support, CPU/memory/PID budgets and an executable, bounded `/tmp` for `dlopen`. `RLIMIT_AS` caps address space. Total PID and memory budgets include both the cached templates and active leaves. Function initialization must be fork-safe. See the [SDK contract](../sdk/cpp/README.md) for details.

The host schema dependency is pinned to json-schema-validator 2.3.0. gRPC is located through CMake config or distro pkg-config. Protocols are generated into the build directory. See ../docs/IMPLEMENTATION_STATUS.md for integration evidence and limitations.
