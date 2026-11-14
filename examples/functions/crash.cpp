#include <fluxforge/function.hpp>
#include <csignal>
FLUXFORGE_FUNCTION([](const fluxforge::json&, const ff_call_v1&) -> fluxforge::json {
    raise(SIGSEGV); return nullptr;
})
