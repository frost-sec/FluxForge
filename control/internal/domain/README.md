# Domain

Pure Go domain contracts for immutable functions/workflows, claim-check payloads,
Run/Invocation/Attempt identity, input plans and validation. This package does not
import gRPC, PostgreSQL, Docker or MinIO. `NormalizeWorkflow` validates and compiles
dependencies (including input references), rejects cycles and freezes the output
step. `ResolveInput` binds payload metadata only; it never downloads user data.

Tests: run the Docker test service documented in the repository README. There is
no independently running process or network API in this module.
