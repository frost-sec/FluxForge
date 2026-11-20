# worker_host

Sources: src/worker_host/main.cpp.

## Purpose and implementation

The independent single-threaded reactor starts Base-Zygote once. It caches function seeds, tracks prepared and running Attempts, supervises child deadlines and forwards Worker results. Concurrent preparations for the same environment share one seed.

## Interfaces, dependencies and ownership

Agent sends `prepare`, `invoke`, `cancel`, `stats` and `shutdown`. Host owns epoll, timerfd, signalfd, a pidfd per child and one socketpair per Attempt. `SCM_RIGHTS` passes the Worker endpoint and log writer down the tree. The launch grant ensures Host can terminate a child before it enters library or function code. Worker crash, timeout and cancellation retire the leaf. A broken seed fails its pending Attempts and is removed from the cache. Base failure disconnects Host and shuts down the Agent so the deployment can replace it.

## Configuration

`RUNNER_SLOTS` caps active Attempts and independently caps cached templates. Idle templates expire after 60s and are evicted by least recent use when capacity is needed. Host checks deadlines at phase boundaries and sweeps expired processes every 50ms. `stats` advertises the loaded seed affinity keys. Its existing RSS field sums per-process RSS, so shared pages can be counted multiple times.

## Docker validation

Use the root Docker tools wrapper or run the C++ tools image: cmake -S /workspace/runner -B /workspace/runner/build-agent -DFF_BUILD_AGENT=ON; cmake --build /workspace/runner/build-agent -j 4; ctest --test-dir /workspace/runner/build-agent --output-on-failure. No host compiler is needed. Full distributed behavior is verified by the root integration tests, not the isolated CTest alone.
