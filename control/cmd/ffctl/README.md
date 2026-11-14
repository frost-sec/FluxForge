# ffctl

Container-native gRPC client for publication, submission, wait/cancel, invocation/attempt inspection, statistics and runner drain. Business connection FF_ADDR (control-plane:9090), internal object/admin connection FF_INTERNAL_ADDR (control-plane:9091). Uses FF_PUBLIC_TOKEN and FF_INTERNAL_TOKEN. Large JSON input files are uploaded directly to MinIO; outputs can be downloaded with checksum verification. CLI only requires Docker on the host.

`demo` uploads the compiled echo/add/fan_in shared libraries, publishes A -> [B,C] -> D and verifies result value=10. Function compilation itself runs via the Docker SDK build helper, not through a local compiler. Help lists actual implemented commands; policy administration is exposed over gRPC.

`verify` exercises replay, large input/output, upload phases, crash retries, timeout, cancellation of a running function and seconds metrics. `benchmark --count 40 --verify-scale` records separate Run/Invocation/execution quantiles and validates real scale-out/idle-shrink. Pool pause/resume/shutdown supports graceful host lifecycle without replacing immutable catalog versions or deleting volumes.
