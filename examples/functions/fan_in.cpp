#include <fluxforge/function.hpp>
FLUXFORGE_FUNCTION([](const fluxforge::json& input, const ff_call_v1&) {
    double total = 0;
    for (const auto& item : input.items()) total += item.value().at("value").get<double>();
    return fluxforge::json{{"value", total}};
})
