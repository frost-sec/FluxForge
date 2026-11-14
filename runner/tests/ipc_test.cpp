#include "fluxforge/ipc.hpp"
#include <iostream>
int main() {
    try {
        auto bytes = ff::ipc::frame({{"op", "invoke"}, {"input", "hello"}});
        ff::ipc::Decoder decoder; ff::json value;
        for (size_t i = 0; i < bytes.size(); ++i) { decoder.append(&bytes[i], 1); if (decoder.next(value) != (i + 1 == bytes.size())) throw std::runtime_error("partial frame handled incorrectly"); }
        if (value.at("input") != "hello") throw std::runtime_error("frame content mismatch");
        auto second = ff::ipc::frame({{"op", "result"}}); auto multiple = bytes + second; decoder.append(multiple.data(), multiple.size());
        if (!decoder.next(value) || !decoder.next(value) || decoder.next(value)) throw std::runtime_error("coalesced frame handling");
        uint32_t invalid = htonl(ff::ipc::max_frame + 1); decoder.append(reinterpret_cast<char*>(&invalid), 4); bool rejected = false;
        try { decoder.next(value); } catch (const std::exception&) { rejected = true; } if (!rejected) throw std::runtime_error("oversized frame accepted");
        fluxforge::worker::v1::WorkerEnvelope forged;
        forged.set_protocol_version(1); forged.set_correlation_id("request-a");
        forged.set_json_body(R"({"protocol_version":1,"id":"request-b"})");
        rejected = false;
        try { ff::ipc::decode_body(forged.SerializeAsString()); } catch (const std::exception&) { rejected = true; }
        if (!rejected) throw std::runtime_error("mismatched envelope identity accepted");
        forged.set_protocol_version(99); rejected = false;
        try { ff::ipc::decode_body(forged.SerializeAsString()); } catch (const std::exception&) { rejected = true; }
        if (!rejected) throw std::runtime_error("unsupported envelope version accepted");
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
