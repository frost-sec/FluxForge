# Object gRPC adapter

Maps ObjectService protobuf messages to the claim-check service and maps domain failures to gRPC statuses. A distinct adapter instance on each listener selects the public or Docker-network signing endpoint. Authentication is provided by the shared listener interceptor. Business logic and credentials remain in objects / MinIO adapter.

Compile/test using the root Docker Go test command. The API carries object metadata and grants only, never bulk payload bytes.
