#include <fluxforge/function.hpp>
#include <unistd.h>
FLUXFORGE_FUNCTION([](const fluxforge::json&, const ff_call_v1&) -> fluxforge::json {
    const std::string chunk(4096, 'x');
    for (;;) { auto n = ::write(STDOUT_FILENO, chunk.data(), chunk.size()); (void)n; }
})
