#!/usr/bin/env bash
set -euo pipefail
cd /workspace
mkdir -p control/pkg/api/v1
protoc -I proto --go_out=control/pkg/api/v1 --go_opt=paths=source_relative --go-grpc_out=control/pkg/api/v1 --go-grpc_opt=paths=source_relative proto/fluxforge_v1.proto
