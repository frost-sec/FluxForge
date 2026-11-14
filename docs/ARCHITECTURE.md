# Architecture and consistency boundaries

The accepted specification is IMPLEMENTATION_PLAN.md; each module README documents actual support.

PostgreSQL is authoritative. Scheduler reservations are hints, never execution authorization. Claim persists an Attempt, opaque lease and replay batch before acknowledgment. Completion persists the outcome, consumes dependency edges, decrements fan-in and writes outbox in one transaction. Outbox projection and startup rebuild make memory disposable.

Lock order is Run -> Session -> sorted Invocation -> Attempt. Completion holds Run SHARE; cancellation/finalization separately hold Run UPDATE. No lock upgrade and no heap lock spans database/network I/O. Sessions fence process restarts; generation fences old queue entries; tokens fence old attempts. External business side effects still require invocation-level idempotency.

Claim-check freezes MinIO object versions. API object IDs are opaque; the backend resolves stored locations, sizes and digests. Assignment contains reference-based InputPlans; Runner resolves and validates inputs. Upload completes before a successful Completion is committed.

Runner owns RPC/lease/transfer state. A separate single-threaded Host creates clean Worker executables, then Workers load functions. Host owns process supervision and reaping. Docker resource limits constrain the full Runner; worker limits constrain wall time/address space/files/logs. This is a team-owned-code execution environment.

Run and Invocation latency include queuing/retries; execution histograms measure Worker calls only. Live backlog drives scaling; completed samples supply estimates. Soft SLO and hard deadline remain distinct. DRAINING is persisted before notification, and deletion requires both server lease accounting and Runner acknowledgment.
