CREATE TABLE IF NOT EXISTS ff_schema_version(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS function_versions (
 function_id text NOT NULL, version text NOT NULL, spec jsonb NOT NULL, digest text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(function_id,version)
);
CREATE TABLE IF NOT EXISTS workflow_versions (
 workflow_id text NOT NULL, version text NOT NULL, spec jsonb NOT NULL, digest text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(workflow_id,version)
);
CREATE TABLE IF NOT EXISTS runs (
 id uuid PRIMARY KEY, workflow_id text NOT NULL, workflow_version text NOT NULL,
 idempotency_key text NOT NULL UNIQUE, request_hash text NOT NULL,
 state text NOT NULL CHECK(state IN ('PENDING','RUNNING','CANCELLING','SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')),
 input jsonb NOT NULL, output jsonb, priority integer NOT NULL CHECK(priority BETWEEN 0 AND 100),
 slo_ms bigint NOT NULL CHECK(slo_ms>0), deadline timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 terminal_target text, error text NOT NULL DEFAULT '', trace_parent text NOT NULL DEFAULT '', trace_state text NOT NULL DEFAULT '',
 FOREIGN KEY(workflow_id,workflow_version) REFERENCES workflow_versions(workflow_id,version)
);
CREATE TABLE IF NOT EXISTS invocations (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES runs(id), step_name text NOT NULL,
 function_id text NOT NULL, function_version text NOT NULL,
 state text NOT NULL CHECK(state IN ('WAITING','READY','LEASED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')),
 remaining_deps integer NOT NULL CHECK(remaining_deps>=0), ready_generation bigint NOT NULL DEFAULT 1,
 attempt_no integer NOT NULL DEFAULT 0, current_attempt_id uuid,
 first_ready_at timestamptz, ready_at timestamptz, not_before timestamptz, started_at timestamptz, finished_at timestamptz,
 output jsonb, error text NOT NULL DEFAULT '',
 UNIQUE(run_id,step_name), UNIQUE(run_id,id),
 FOREIGN KEY(function_id,function_version) REFERENCES function_versions(function_id,version)
);
CREATE TABLE IF NOT EXISTS invocation_edges (
 run_id uuid NOT NULL REFERENCES runs(id), parent_id uuid NOT NULL, child_id uuid NOT NULL,
 satisfied_at timestamptz, PRIMARY KEY(run_id,parent_id,child_id),
 FOREIGN KEY(run_id,parent_id) REFERENCES invocations(run_id,id),
 FOREIGN KEY(run_id,child_id) REFERENCES invocations(run_id,id), CHECK(parent_id<>child_id)
);
CREATE TABLE IF NOT EXISTS runner_sessions (
 id uuid PRIMARY KEY, runner_id text NOT NULL, boot_id text NOT NULL,
 state text NOT NULL CHECK(state IN ('ACTIVE','RETIRED','OFFLINE')),
 policy text NOT NULL CHECK(policy IN ('ACTIVE','DRAINING','OFFLINE')),
 slots integer NOT NULL CHECK(slots>0), memory_bytes bigint NOT NULL CHECK(memory_bytes>0), cpu_millis bigint NOT NULL CHECK(cpu_millis>0),
 available_slots integer NOT NULL, used_memory_bytes bigint NOT NULL DEFAULT 0,
 cached_keys jsonb NOT NULL DEFAULT '[]', last_seen timestamptz NOT NULL DEFAULT now(),
 container_id text NOT NULL DEFAULT '', runtimes jsonb NOT NULL DEFAULT '["cpp20"]', architecture text NOT NULL DEFAULT 'amd64', runtime_digest text NOT NULL DEFAULT 'ubuntu22.04-cpp20-abi1',
 active_attempts integer NOT NULL DEFAULT 0, pending_completions integer NOT NULL DEFAULT 0, drain_acknowledged boolean NOT NULL DEFAULT false, UNIQUE(runner_id,boot_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_live_runner ON runner_sessions(runner_id) WHERE state='ACTIVE';
CREATE TABLE IF NOT EXISTS attempts (
 id uuid PRIMARY KEY, invocation_id uuid NOT NULL REFERENCES invocations(id), run_id uuid NOT NULL REFERENCES runs(id),
 attempt_no integer NOT NULL, session_id uuid NOT NULL REFERENCES runner_sessions(id), lease_token text NOT NULL,
 state text NOT NULL CHECK(state IN ('LEASED','RUNNING','COMPLETING','SUCCEEDED','USER_FAILED','TIMEOUT','OOM','CRASHED','LOST','CANCELLED','RELEASED','RESOURCE_ERROR','INFRA_ERROR')),
 lease_expires_at timestamptz NOT NULL, execution_deadline timestamptz, upload_deadline timestamptz, execution_finished_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
 duration_ms bigint NOT NULL DEFAULT 0, error text NOT NULL DEFAULT '', output jsonb,
 completion_hash text, UNIQUE(invocation_id,attempt_no)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt ON attempts(invocation_id) WHERE state IN ('LEASED','RUNNING','COMPLETING');
CREATE INDEX IF NOT EXISTS active_attempt_expiry ON attempts(lease_expires_at) WHERE state IN ('LEASED','RUNNING','COMPLETING');
CREATE INDEX IF NOT EXISTS session_attempts ON attempts(session_id,state);
CREATE TABLE IF NOT EXISTS acquire_requests (
 session_id uuid NOT NULL REFERENCES runner_sessions(id), request_id text NOT NULL,
 request_hash text NOT NULL, attempt_ids jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(session_id,request_id)
);
CREATE TABLE IF NOT EXISTS scheduler_outbox (
 id bigserial PRIMARY KEY, kind text NOT NULL, invocation_id uuid REFERENCES invocations(id), run_id uuid NOT NULL REFERENCES runs(id),
 generation bigint NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now(), delivered_at timestamptz,
 UNIQUE(kind,invocation_id,generation)
);
CREATE INDEX IF NOT EXISTS pending_outbox ON scheduler_outbox(id) WHERE delivered_at IS NULL;
CREATE INDEX IF NOT EXISTS ready_invocations ON invocations(not_before,ready_at,id) WHERE state IN ('READY','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS run_invocations ON invocations(run_id,state);
INSERT INTO ff_schema_version(version) VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS runtime_policies(kind text PRIMARY KEY,version bigint NOT NULL,json jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now());
