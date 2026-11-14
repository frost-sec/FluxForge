# API and execution contracts

The source of truth for wire types is proto/fluxforge_v1.proto. Use protobuf JSON for workflow files, e.g. examples/workflows/fan-in.json. Business messages use bounded inline JSON or opaque, committed ObjectRefs. Deprecated fields are compatibility placeholders, not the new input path.

## Listeners and authentication

Public gRPC :9090 (host localhost:19090): CatalogService, WorkflowService, ObjectService, ObservabilityService. Internal :9091 (Docker network only): RunnerQueueService, QueueAdminService, ObjectService. Each listener validates `authorization: Bearer <FF_PUBLIC_TOKEN or FF_INTERNAL_TOKEN>`. Object grants use the appropriate externally reachable/internal MinIO endpoint. Health/metrics HTTP :8080 (localhost:18080) is independent.

## Public APIs

- Catalog Publish/GetFunction: immutable function_id+version, cpp20, Linux/amd64, ABI1, ff_invoke_v1, committed .so artifact, dependency/runtime digest, Draft7 schemas, memory/CPU/timeout/SLO. Same version cannot change content.
- Catalog Publish/GetWorkflow: typed immutable WorkflowSpec with explicit function versions, nodes, deduplicated dependencies, input bindings, bounded retries and output_step. A step-output binding implies its dependency.
- Object BeginUpload: purpose FUNCTION_ARTIFACT/RUN_INPUT/ATTEMPT_OUTPUT, size, SHA256 and content type -> upload_id and 15-minute URL. PUT bytes directly to MinIO. CommitUpload -> fixed object version. GetDownloadURL grants a GET for that version; consumer verifies digest.
- Workflow SubmitRun: workflow_id/version, idempotency_key, PayloadRef, priority0..100, optional soft SLO and hard deadline. Missing deadline uses server default. Same key and semantic request returns original Run even after completion; trace context is not part of request identity.
- Workflow GetRun / ListRuns / ListInvocations / ListAttempts: state, timestamps, output references and errors; paginated lists. CancelRun is asynchronous and idempotent. Wait timeouts never cancel a Run implicitly.
- Observability GetFunctionStats: function/version and 1s..5min retained window -> execution/invocation count, avg/p50/p95/p99, error/cold/warm counts and freshness. Long-term history is in PG/Prometheus.

## Internal APIs

RegisterRunner declares stable runner_id, boot UUID, runtime/architecture, memory/CPU and slots; receives persistent session_id. Heartbeat publishes capacity/cache profile and acknowledged policy, receives cancellation/policy. All Attempt mutations use session_id+attempt_id+lease_token.

AcquireTasks uses a frozen request_id/requested_slots/wait_timeout request, max wait10s. Reply loss is retried with the exact same request. Replay returns only the original still-valid batch, identifies expired entries, never allocates replacements and never renews implicitly.

ReportStarted records first execution deadline. Large result uses ReportExecutionFinished to enter COMPLETING with a separate upload deadline, then direct upload, then ReportCompletion. Small results combine finishing/completion. RenewLeases returns validity and actual remaining lifetime for every item. ReportCompletion is durable and duplicate-safe; already-committed identical completion ACKs even after expiry. ReleaseTask is a retryable infrastructure outcome. RPC deadlines, phase deadlines, lease TTL, Run hard deadline and soft SLO are distinct.

ReportMetrics sends cumulative count/sum/non-cumulative histogram bins every ~1s with a monotonically increasing sequence per session. Independent loop, no custom Trace-upload RPC. StageTiming carries relative stage intervals; Linux containers on the single host share the wall-clock anchor, supervision always uses monotonic time.

QueueAdmin provides pool/runners, DRAINING/OFFLINE policies, and version-CAS scheduling/autoscaling JSON policies. Policy patches are merged into effective config before persistence/application so restarts preserve previous settings. Durations in policy JSON are Go duration nanoseconds; read current policy before editing. Unknown fields and invalid configs are rejected.

## Drain and shutdown

ACTIVE -> DRAINING is durable before response; Claim immediately refuses new allocation. Runner retains heartbeats/renewals/results. Drain acknowledgment requires observed DRAINING and zero local active/pending work, plus server active leases must be zero. Autoscaler only stops/removes deployment-labeled containers. `pool pause/resume` controls new capacity; `pool shutdown` pauses and waits for all sessions to drain. The host down wrapper then stops only project-owned runners and preserves data volumes.

## Errors and retries

INVALID_ARGUMENT: malformed spec/binding/limits. NOT_FOUND: unknown version/resource. ALREADY_EXISTS: immutable content/idempotency/policy conflict. FAILED_PRECONDITION: stale session/lease or unsupported transition. UNAVAILABLE: retryable dependency failure; retry mutating operations with their existing identities. Large data and credentials never enter a user function's IPC envelope.
