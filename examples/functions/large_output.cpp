#include <fluxforge/function.hpp>
FLUXFORGE_FUNCTION([](const fluxforge::json& input, const ff_call_v1&) {
    return fluxforge::json{{"data", std::string(input.value("bytes", 128 * 1024), 'x')}};
})
