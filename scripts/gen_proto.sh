#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
docker run --rm -v "${PROJECT_ROOT}:/workspace" fluxforge-go-tools:dev bash /workspace/scripts/codegen.sh
docker run --rm -v "${PROJECT_ROOT}:/workspace" fluxforge-toolchain:dev bash -c 'mkdir -p /workspace/runner/include/proto && protoc -I/workspace/proto --cpp_out=/workspace/runner/include/proto --grpc_out=/workspace/runner/include/proto --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin /workspace/proto/fluxforge_v1.proto'
echo "Go and C++ contracts generated in disposable Docker tools."
