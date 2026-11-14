# FluxForge implementation contract

Accepted 2026-11-02. This file records the agreed plan and decisions, not a claim of completion. Progress and limitations are recorded separately in IMPLEMENTATION_STATUS.md.

## Scope and acceptance

Single Docker host with multiple runners; extensible runner protocol. Team-owned functions, Linux/amd64 C++20 `.so` and versioned C JSON ABI. Go/C++20/Docker/gRPC/PostgreSQL/MinIO/UDS. All compilation, generation, formatting, testing and releases run in Docker; host compiler installation is unnecessary. Business gRPC and container CLI; HTTP only health/metrics. Every business module has its own README.

## Architecture and module boundaries

One modular Go control process: domain, catalog, workflow, execution, runnerregistry, scheduler, projection, recovery, objects, telemetry, autoscaler, adapters/postgres, adapters/minio, adapters/docker, transport/grpc, config/bootstrap. Entry points only assemble components.

- domain: plain types, state/validation and errors; no protobuf/infrastructure imports.
- catalog: immutable function/workflow versions, DAG compile/validation.
- workflow: submit/query/cancel, binding, dependency transitions and retry decisions.
- execution: acquire/start/renew/finish/release orchestration and ownership checks.
- runnerregistry: persistent boot sessions, health, cache profile and desired policy.
- scheduler: pure in-memory per-runner locked heaps, global heap, locality, delay scheduling, work stealing and bounded waits.
- projection: durable outbox -> current-state reload -> idempotent scheduler index.
- recovery: expired leases/retries/cancellation/finalization/startup rebuild.
- objects: claim-check upload/download grants and immutable object metadata.
- telemetry: run/invocation/attempt tracing, histogram aggregation and metrics.
- autoscaler: risk/desired capacity/reconciliation; Docker adapter only manages owned containers.
- adapters: SQL and infrastructure APIs; transport: mapping/auth/validation/status only.

C++ modules: app, control_client, attempt_manager, artifact_cache, payload_io, worker_client, worker_host, worker_runtime, ipc, telemetry. Separate sdk/cpp provides ABI/templates. Agent starts independent single-threaded Worker Host before gRPC threads. Host never loads user code; it fork/execs Workers, which load `.so` only after exec. A Worker handles one Attempt at a time and remains warm for matching environment keys.

Dependency direction is inward toward domain and focused ports; no cyclic business modules, universal managers, SQL in handlers or Docker calls in scheduler. Every module README describes purpose, current support, interfaces, dependencies, lifecycle/ownership, config, Docker tests and failures.

## Data and public contracts

Immutable FunctionVersion: ID/version, code SHA256, dependency/runtime digest, ABI/architecture, artifact ObjectRef, CPU/memory/timeouts/SLO, Draft7 input/output schemas. Immutable WorkflowVersion: explicit function versions, typed nodes/dependencies/bindings/retries/output_step. No `latest` resolution during a Run.

PayloadRef is a oneof inline JSON/object reference. ObjectRef has stable object ID, byte size, SHA256 and content type; backend records key and fixed object version. URL grants expire and are never the persistent reference. Large bytes flow client/runner <-> MinIO, never through control gRPC. Control distributes InputPlans containing references.

Bindings support one whole source or a top-level object field map. Sources: literal JSON, RunInput+JSON Pointer, StepOutput+JSON Pointer. Source references imply deduplicated dependency edges. No expression language. Draft7 schemas compile at publish, hydrated I/O validates on runner; remote schema refs disabled. Workflow final output forwards the chosen output_step PayloadRef; aggregate in a final function when needed.

CatalogService: Publish/GetFunction, Publish/GetWorkflow. ObjectService: BeginUpload, CommitUpload, GetDownloadURL. WorkflowService: SubmitRun, GetRun, ListRuns, ListInvocations, ListAttempts, CancelRun. RunnerQueueService: RegisterRunner, Heartbeat, AcquireTasks, ReportStarted, ReportExecutionFinished, RenewLeases, ReportCompletion, ReleaseTask, ReportMetrics. Admin: GetPoolSnapshot, ListRunners, SetRunnerPolicy, Get/SetSchedulingPolicy, Get/SetAutoscalingPolicy. Observability: GetFunctionStats. HTTP: /livez /readyz /metrics and legacy /ping. Errors use gRPC status; paging bounds large responses.

Submit idempotency key + normalized request hash: same request returns original Run; conflicting reuse errors. Runner updates carry persistent session identity; tokens bind to Session. Acquire retry freezes entire request, not only ID; replay returns original still-valid batch and never allocates replacements or renews leases. Renew returns each item's actual validity. Completion replay checks already-committed result first: identical duplicate ACKs even after lease expiry, conflicting payload errors. Only a never-committed expired result is rejected.

## Persistent state and transactions

PostgreSQL is the only authoritative execution state; no custom WAL/Redis/Kafka in v1. Tables: function_versions, workflow_versions, runs, invocations, invocation_edges, attempts, runner_sessions, acquire_requests, objects, scheduler_outbox, policies. Unique constraints cover versions, submit keys, Run+step, Invocation+attempt number, active Attempt per Invocation and outbox generations. remaining_deps is nonnegative.

Run: ACCEPTED -> RUNNING -> SUCCEEDED or CANCELLING -> CANCELLED/FAILED/TIMED_OUT. Invocation: BLOCKED -> READY -> LEASED -> RUNNING -> SUCCEEDED; retryable failure -> RETRY_WAIT -> new READY generation; terminal failure/cancel/skip. Attempt separates preparation/execution/completing and terminal outcomes. Invocation identity is stable; retries use new Attempt IDs.

Submit transaction stores Run, nodes, unique edges, root Ready states and outbox with idempotency. Claim reserves in memory, releases heap lock, then verifies READY+generation+session+ACTIVE+resources in PG, stores Attempt/lease/acquire replay atomically. Claim counts active Attempts under session lock in a fresh READ COMMITTED statement.

Completion transaction: first-time ownership CAS, parent outcome, consume each previously unconsumed outgoing edge, single SQL atomically decrement child indegrees; only 1->0 transitions enqueue Ready outbox. Duplicate completion never consumes edges again. Finalize events share this transaction. No external I/O inside SQL transactions.

Lock order: Run FOR SHARE -> Session (Claim FOR UPDATE; other updates FOR SHARE) -> all affected Invocations in sorted order -> Attempts. No Run SHARE->UPDATE upgrade and no session occupancy-counter write by shared holders. Registration only changes Session; old Attempts cleaned later in correct order. Cancellation/Run Finalizer take Run FOR UPDATE in independent transactions. Finalizer reads nodes and writes Run only; post-commit immediate invocation plus durable event/background recovery. FAIL_FAST linearizes at persisted CANCELLING; a small pre-finalizer claim window is allowed and subsequently revoked.

Startup acquires single-controller advisory lock, loads sessions/valid leases/Ready and delayed states, rebuilds scheduler before readiness. Valid leases survive control restart. Outbox at-least-once projection re-reads current state and uses generation idempotency; periodic reconciliation repairs missing indexes.

## Scheduler

Independent per-runner shard locks plus global shard; container/heap entries contain Invocation/Run/generation/environment key/resources/priority/Ready time/estimate/SLO/deadline. Route to cached compatible runner, otherwise stable compatible owner; unavailable owner -> global. Acquire: local -> global -> delay-expired stealable Ready entries. Offline owner opens immediate stealing. Never steal leased work.

Reserve -> unlock -> PG Claim -> Commit/Abort reservation. No heap lock across DB/network/wait. Bounded search skips oversized heads. Aging updates heap keys explicitly. Wait subscribes before checking to avoid lost wakeups. Server ownership/resource accounting constrains allocation independently of reported free slots.

## ABI, IPC and execution lifecycle

C ABI exports ff_abi_version and ff_invoke_v1(call, output_writer, error_writer). Versioned structs, JSON pointer+length, Run/Invocation/Attempt IDs, host-owned writer callbacks. No STL/exceptions/allocator ownership across ABI; SDK catches exceptions. Matching code/runtime/ABI/resource configuration is the warm key. Recycle after limits/lifetime/memory growth; failed workers are destroyed.

UDS socketpair, 4-byte network-endian length + WorkerEnvelope protobuf; version/correlation/Attempt/generation. Messages cover warm/execute/terminate/drain/ready/result/exit. Bound frames and output queues, handle partial I/O/EINTR/EAGAIN/disconnect. Large local payloads use private temporary files, never credentials in worker.

Host uses level-triggered epoll, monotonic timerfd, signalfd, waitpid drain and per-worker process groups. Timeout/cancel/revoked lease kills process group and reaps; callbacks do not perform unsafe asynchronous signal operations. Container init and parent-death cleanup handle orphaning. Docker CPU/memory/PID limits + worker RLIMIT_AS/FSIZE/NOFILE/CORE and bounded logs/files; RLIMIT_AS is not RSS and SIGKILL is not proof of OOM. No privileged/container cgroup delegation requirement. Scope is not a malicious multi-tenant sandbox.

Agent holds slot from assignment through final ACK/revocation. Independent heartbeats/acquire/renew/I/O. Preparation and upload have their own deadlines; actual function timeout excludes upload. Large result: ReportExecutionFinished -> upload -> Completion; small result may combine. Continue leases through preparation and result ACK. DRAINING stops acquisitions but preserves existing execution/renew/completion until drained.

## Observability and autoscaling

Run latency: durable accepted -> durable Run terminal. Invocation latency: READY -> logical terminal including queue/retries. Attempt execute: Worker call start -> return. Trace parent workflow.run -> function.invoke -> function.attempt, with queue/artifact/input/worker/output subspans; submit RPC is not long-running Run. Context travels with assignments. Telemetry is best effort and cannot block business; restart spans link recovery, no false lossless trace claim.

Every ~1s per-function cumulative count/sum/histograms/outcomes/cache/cold-stage metrics, session+monotonic seq dedup/restart handling. Control measures its own queue/Run durations. Merge buckets for p99, never average p99. Low-cardinality metrics; IDs only traces/logs. SQL historical P99 is bounded diagnostic/estimate backfill, realtime memory window+live queue drives scaling.

Soft SLO misses record risk, hard deadlines stop execution. T_drain=(Ready predicted work+inflight residual)/healthy ACTIVE total service capacity, not instantaneous free slots. Target combines arrival rate and work/budget, includes STARTING planned capacity, bounds resources/min/max. Defaults 1s reconcile, 2 bad intervals to grow, immediate grow with no capacity+Ready, 60s low-load before shrinking, min1/max4/slots2 configurable. Persist DRAINING before notification; no new claims, wait for both control and agent drain, then stop/remove. Ordinary scale-down does not kill active work; forced removal explicit. Discover existing labeled containers after restart, use API negotiation and immutable release image tags/digests. Never mount Docker socket into runner/worker.

## Docker delivery, stages and tests

Separate codegen/control-dev/control-runtime/runner-builder/runner-runtime/function-sdk/test/ffctl targets. PowerShell host wrapper invokes Docker only. Codegen does not require running control container. Root README, architecture/protocol/runbook and every business module README. Defaults preserve existing volumes; data deletion is explicit.

A: module/contracts/docs/toolchain/migrations/CLI skeleton. B: PG single-function execution/idempotent lease loop. C: DAG fan-in/retry/cancel/outbox/recovery. D: .so/UDS/warm workers/claim-check. E: sharded three-tier scheduler/delay/stealing/fairness. F: dual latency/OTel/seconds metrics/autoscaling.

Acceptance: Docker-only generate/build/test; reply loss at Submit/Claim/Complete; conflicting IDs; stale sessions/tokens/generations; duplicate/parallel fan-in; cancellation races; DB commit before projection crash; worker loop/segfault/allocation/log/output faults; revoked leases in download/execute/upload/ACK; draining continues leases; metrics duplicate/reorder/reset; controller restart no duplicated fleet. Benchmark cold/warm/IPC/end-to-end p50/p95/p99, record measured evidence rather than promise millisecond latency.

