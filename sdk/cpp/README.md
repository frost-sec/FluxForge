# FluxForge C++ function SDK

The first runtime is a Linux/amd64 shared object exporting ff_abi_version and ff_invoke_v1. function.h is the C ABI; function.hpp provides FLUXFORGE_FUNCTION(lambda) for nlohmann JSON handlers. Build with the same Docker toolchain/runtime family as Runner, not a host compiler. Example sources are in examples/functions and built as .so targets by runner/CMakeLists.txt.

The ABI uses versioned structs, pointer/byte length and host-owned write callbacks. IDs are NUL-terminated UTF-8 and valid for this call only; input JSON bytes are length-delimited. Never free host memory, throw across C ABI, retain pointers, or launch subprocesses. The SDK catches normal C++ exceptions. Faults and limits terminate the Worker.

Library constructors and `ff_abi_version` run in the Function-Zygote. Initialization must be safe to fork from and leave a single thread with no live connections, thread pools or locks held across fork. Build read-only lookup data there. Each invocation inherits a fresh copy-on-write view of that state, and the Worker exits with `_exit` after replying. Finish per-call cleanup inside the handler. Persist business state externally and use `invocation_id` for idempotent side effects.

Each invocation must emit exactly one valid JSON document through the output writer. Nonzero return reports user failure; the bounded error writer carries diagnostics. Input/output schemas are validated outside the library. Only libraries available in the runtime image are supported; arbitrary dependency installation is not implemented.
