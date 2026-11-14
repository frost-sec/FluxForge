#include <fluxforge/function.hpp>
#include <atomic>
FLUXFORGE_FUNCTION([](const fluxforge::json&, const ff_call_v1&) -> fluxforge::json {
    for (;;) std::atomic_signal_fence(std::memory_order_seq_cst);
})
