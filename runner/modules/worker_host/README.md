# worker_host

Sources: src/worker_host/main.cpp.

## Purpose and implementation

Independent single-threaded Linux reactor: fork/exec workers, preserve prepared ownership, reuse compatible workers, supervise timeouts/cancellation/logs and reap children.

## Interfaces, dependencies and ownership

Uses epoll/timerfd/signalfd, process groups, parent cleanup and bounded log queues. Worker processes receive a clean environment and RLIMIT_AS/FSIZE/NOFILE/CORE. This is resource containment for team-owned code, not a hostile multitenant sandbox.

## Configuration

Capacity equals RUNNER_SLOTS. Poll timer 50ms, idle eviction 60s, replacement after 1000 calls or 10 minutes.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
