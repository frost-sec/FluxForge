#include "fluxforge/worker_runtime.hpp"
#include <nlohmann/json-schema.hpp>

namespace ff::worker {
namespace {
struct Sink {
    std::ofstream file;
    uint64_t bytes = 0, limit;
    bool failed = false;
    explicit Sink(const std::string& path, uint64_t max) : file(path, std::ios::binary | std::ios::trunc), limit(max) { if (!file) throw std::runtime_error("cannot open output file"); }
    static int32_t write(void* context, const uint8_t* data, uint64_t size) noexcept {
        auto* self = static_cast<Sink*>(context);
        if (size > self->limit - self->bytes) { self->failed = true; return -1; }
        self->file.write(reinterpret_cast<const char*>(data), size);
        if (!self->file) { self->failed = true; return -1; }
        self->bytes += size; return 0;
    }
};
void validate(const ff::json& instance, const std::string& schema) {
    if (schema.empty()) return;
    nlohmann::json_schema::json_validator validator;
    // No loader: external references cannot trigger network access.
    validator.set_root_schema(ff::json::parse(schema));
    validator.validate(instance);
}
}

ff::json invoke(const ff::json& req, Function function) {
    const auto start = ff::now_ms();
    ff::json result = {{"op", "result"}, {"id", req.at("id")}, {"attempt_id", req.at("attempt_id")}, {"generation", req.at("generation")}, {"status", "USER_FAILED"}, {"worker_pid", getpid()}};
    try {
        auto input = ff::read_file(req.at("input_path").get<std::string>(), req.value("max_payload_bytes", 64ULL * 1024 * 1024));
        validate(ff::json::parse(input), req.value("input_schema", ""));
        auto run = req.value("run_id", ""), invocation = req.value("invocation_id", ""), attempt = req.at("attempt_id").get<std::string>();
        Sink output(req.at("output_path"), req.value("max_payload_bytes", 64ULL * 1024 * 1024));
        const auto error_path = req.at("output_path").get<std::string>() + ".error";
        Sink error(error_path, 16384);
        ff_writer_v1 out{sizeof(ff_writer_v1), &output, &Sink::write}, err{sizeof(ff_writer_v1), &error, &Sink::write};
        ff_call_v1 call{sizeof(ff_call_v1), FF_ABI_VERSION, reinterpret_cast<const uint8_t*>(input.data()), input.size(), run.c_str(), invocation.c_str(), attempt.c_str()};
        int code = function(&call, &out, &err);
        output.file.close(); error.file.close();
        if (output.failed) throw std::runtime_error("output exceeds limit or cannot be written");
        if (code != 0) throw std::runtime_error(ff::read_file(error_path, 16384));
        validate(ff::json::parse(ff::read_file(req.at("output_path").get<std::string>(), output.limit)), req.value("output_schema", ""));
        result["status"] = "SUCCESS"; result["output_size"] = output.bytes;
        std::filesystem::remove(error_path);
    } catch (const std::bad_alloc&) {
        result["status"] = "RESOURCE_EXHAUSTED"; result["error"] = "worker address space exhausted";
    } catch (const std::exception& e) {
        result["error"] = std::string(e.what()).substr(0, 16384);
    } catch (...) { result["error"] = "function threw an unknown exception across its ABI"; }
    result["duration_ms"] = ff::now_ms() - start;
    return result;
}
}
