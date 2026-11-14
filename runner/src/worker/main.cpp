#include "fluxforge/common.hpp"
#include "fluxforge/ipc.hpp"
#include <fluxforge/function.h>
#include <nlohmann/json-schema.hpp>
#include <dlfcn.h>
#include <sys/prctl.h>
#include <signal.h>
#include <unistd.h>
#include <iostream>
#include <memory>

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
ff::json invoke(const ff::json& req, int32_t (*function)(const ff_call_v1*, const ff_writer_v1*, const ff_writer_v1*)) {
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
int main(int argc, char** argv) {
    try {
        if (argc != 3) throw std::runtime_error("usage: fluxforge-worker IPC_FD LIBRARY_PATH");
        const int fd = std::stoi(argv[1]); const auto parent = getppid();
        if (prctl(PR_SET_PDEATHSIG, SIGKILL) < 0 || getppid() != parent || parent == 1) return 1;
        sigset_t empty; sigemptyset(&empty); pthread_sigmask(SIG_SETMASK, &empty, nullptr);
        void* lib = dlopen(argv[2], RTLD_NOW | RTLD_LOCAL);
        if (!lib) throw std::runtime_error(dlerror());
        auto version = reinterpret_cast<uint32_t (*)()>(dlsym(lib, "ff_abi_version"));
        auto function = reinterpret_cast<int32_t (*)(const ff_call_v1*, const ff_writer_v1*, const ff_writer_v1*)>(dlsym(lib, "ff_invoke_v1"));
        if (!version || !function || version() != FF_ABI_VERSION) throw std::runtime_error("incompatible function ABI");
        ff::ipc::send_blocking(fd, {{"op", "ready"}, {"worker_pid", getpid()}});
        for (;;) {
            auto req = ff::ipc::receive_blocking(fd);
            if (req.at("op") == "shutdown") break;
            if (req.at("op") != "invoke") throw std::runtime_error("unsupported worker request");
            ff::ipc::send_blocking(fd, invoke(req, function));
        }
        dlclose(lib); return 0;
    } catch (const std::exception& e) { std::cerr << "worker: " << e.what() << '\n'; return 1; }
}
