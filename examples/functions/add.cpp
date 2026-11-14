#include <fluxforge/function.hpp>
FLUXFORGE_FUNCTION([](const fluxforge::json& input, const ff_call_v1&) {
    return fluxforge::json{{"value", input.at("a").get<double>() + input.at("b").get<double>()}};
})
