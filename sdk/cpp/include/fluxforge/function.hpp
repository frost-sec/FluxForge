#pragma once
#include "function.h"
#include <exception>
#include <string>
#include <nlohmann/json.hpp>
namespace fluxforge {
using json = nlohmann::json;
inline int write(const ff_writer_v1* writer, const std::string& value) {
    return writer->write(writer->context, reinterpret_cast<const uint8_t*>(value.data()), value.size());
}
template<class Handler>
int invoke(const ff_call_v1* call, const ff_writer_v1* output,
           const ff_writer_v1* error, Handler handler) noexcept {
    try {
        if (!call || call->abi_version != FF_ABI_VERSION || call->struct_size < sizeof(ff_call_v1))
            throw std::runtime_error("unsupported FluxForge ABI");
        const auto input = json::parse(call->input, call->input + call->input_size);
        return write(output, handler(input, *call).dump()) == 0 ? 0 : 1;
    } catch (const std::exception& e) {
        write(error, json({{"message", e.what()}}).dump());
    } catch (...) {
        write(error, "{\"message\":\"unknown function exception\"}");
    }
    return 1;
}
}
#define FLUXFORGE_FUNCTION(...) \
    extern "C" uint32_t ff_abi_version(void) { return FF_ABI_VERSION; } \
    extern "C" int32_t ff_invoke_v1(const ff_call_v1* call, const ff_writer_v1* output, const ff_writer_v1* error) { \
        return ::fluxforge::invoke(call, output, error, __VA_ARGS__); \
    }
