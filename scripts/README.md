# Docker command entry points

All scripts delegate compilation/code generation/testing to containers. No host Go, C++, protoc or Bash installation is required when using ff.ps1. Codegen uses a disposable tool container and does not mutate a running control service. PostgreSQL integration tests use random isolated schemas and clean up only their own schemas; end-to-end tests use the new deployment. Normal down operations preserve volumes.

ff.ps1 implements tools/generate/build/up/down/test/integration/demo/verify/cli/status/logs/function-build. ff.cmd runs the same entry point with a process-local PowerShell execution policy on hosts that disable direct script loading. It never changes system-wide policy. The down command pauses/drains first, only manages exact project-labeled containers, and retains all volumes. Progress and measured evidence are recorded in docs/IMPLEMENTATION_STATUS.md.
