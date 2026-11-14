# FluxForge engineering rules

Read `docs/IMPLEMENTATION_PLAN.md` before changing architecture or contracts. Track actual verified progress in `docs/IMPLEMENTATION_STATUS.md`; do not present planned or stubbed behavior as implemented.

- Build, code generation, formatting, tests and release packaging run in Docker. Host requirements are Docker Desktop/Linux containers and PowerShell only.
- Preserve the agreed architecture: one modular Go control process, PostgreSQL authoritative state, reconstructible sharded in-memory scheduler, MinIO claim-check, C++ Agent / single-threaded Worker Host / warm Worker processes.
- First runtime is Linux/amd64 C++20 shared libraries using the versioned C JSON ABI. Scope is team-owned functions on one Docker host, not an untrusted multi-tenant sandbox.
- Domain types do not import Protobuf or infrastructure SDKs. Scheduler does not access PostgreSQL, gRPC, MinIO or Docker. Infrastructure is behind focused interfaces. Never hold heap locks across I/O.
- Every business module requires a README with purpose, implementation status, interfaces, dependencies, lifecycle/resource ownership, configuration, Docker commands, and failure behavior.
- Do not reuse protobuf field numbers for new meanings. Keep business state, metrics and traces separate. Merge histograms; never average p99 values.
- Persist claim/completion deduplication with state changes. Fan-in completion, edge consumption, atomic indegree decrement and outbox events share a transaction. Follow the documented database lock order.
- Do not delete existing database/object volumes or manage containers without this deployment's ownership labels. Use isolated test databases/projects.

