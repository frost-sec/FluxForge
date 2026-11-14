# Docker delivery

Tool images contain compilers/code generators; runtime images contain executable binaries and their runtime libraries. Build in order: Go tools, C++ tools, Runner runtime, Control runtime, CLI. scripts/ff.ps1 performs this order without a host compiler.

compose.yml uses a separate fluxforge-v1 deployment/network/volumes and localhost ports 19090/18080/19000/19001. It does not reuse or erase legacy ff-postgres/ff-minio data. The control process alone mounts Docker socket and creates labeled, constrained Runner containers. Runners use a non-root user, read-only root and writable limited /tmp; workers receive no credentials. Normal down retains all persistent volumes.

The original docker-compose.dev.yml is the legacy environment, retained for compatibility. It is not the new deployment command. Pin release image digests when publishing; local :dev tags represent rebuildable development images, not production immutability.
