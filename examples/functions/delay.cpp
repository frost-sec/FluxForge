#include <fluxforge/function.hpp>
#include <thread>
#include <chrono>
FLUXFORGE_FUNCTION([](const fluxforge::json& input, const ff_call_v1&) {
    const auto delay = input.value("milliseconds", 200);
    if (delay < 0 || delay > 1000) throw std::runtime_error("milliseconds must be 0..1000");
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    return fluxforge::json{{"value", delay}};
})
