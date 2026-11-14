# Protocol module

Business gRPC uses fluxforge_v1.proto; local worker IPC uses worker.proto. Legacy field numbers are not repurposed. New clients use typed WorkflowSpec, PayloadRef and Runner Session identities.

Public services: Catalog, Object, Workflow, Observability. Internal services: RunnerQueue and QueueAdmin. Generate code using the Docker codegen service or scripts/ff.ps1 generate. Generated files are build artifacts, not edited source. See docs/IMPLEMENTATION_PLAN.md for timing, authorization and replay semantics.
