# Docker runbook

Windows host: Docker Desktop Linux containers and PowerShell only. If direct .ps1 execution is disabled, use `scripts\ff.cmd`, which sets script policy for its child process only and does not change global settings.

```powershell
.\scripts\ff.cmd build
.\scripts\ff.cmd test
.\scripts\ff.cmd up
.\scripts\ff.cmd integration
.\scripts\ff.cmd demo
.\scripts\ff.cmd verify
.\scripts\ff.cmd cli pool status
```

Build sequence: Go/protoc tools -> Ubuntu C++ base/tools -> Runner image (CTest runs) -> function SDK -> Control/CLI images. Existing Docker tool layers are cached. Codegen uses disposable tool containers; it never apt-installs into the running control service.

Default endpoints: public gRPC localhost:19090, health/metrics18080, MinIO19000, Console19001. Internals resolve control-plane:9091 and minio:9000 on the project network. Settings are in .env.example; Compose reads .env. Dev credentials are localhost-only examples. Pin immutable images and supply deployment credentials for a release.

Benchmark synthetic controlled sleep functions:

```powershell
.\scripts\ff.cmd cli benchmark --count 40 --verify-scale
```

This measures request/invocation/execution samples, observes live scale-out and waits for the default60s idle window and acknowledged scale-in. It is a reproducible functional/load sample, not a production capacity or SLO guarantee.

Shutdown:

```powershell
.\scripts\ff.cmd down
```

The wrapper pauses capacity creation, drains before stopping control, then inspects exact io.fluxforge.managed/project labels before stopping/removing runners. It preserves volumes. Drain timeout fails safely with tasks still running; inspect/cancel or wait and retry. `up` resumes the persisted pause. Never use volume deletion or broad Docker prune commands for routine lifecycle.

Diagnostics: CLI run get, invocation list, attempt list, function stats, runner list; Docker logs for specific owned containers; HTTP /readyz and /metrics. If MinIO transfer fails, check internal/public signing endpoint and committed version. Cached .so files require an explicitly executable tmpfs mount: removing `exec` can silently make dlopen fail. Docker resource limits are whole-container limits; RLIMIT_AS is address space, not RSS/OOM proof.

Optional tracing UI uses compose.observability.yml (Collector -> Jaeger; Prometheus scrapes control). The core platform runs without these external services. Backend unavailability may lose/drop telemetry but must not affect business commits.
