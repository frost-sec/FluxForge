#pragma once
#include "common.hpp"
#include <openssl/rand.h>
#include <iomanip>
#include <sstream>
#include <unistd.h>
namespace ff {
inline std::string uuid() {
    unsigned char bytes[16]; if (RAND_bytes(bytes, sizeof(bytes)) != 1) throw std::runtime_error("secure random generator failed"); bytes[6] = (bytes[6] & 0x0f) | 0x40; bytes[8] = (bytes[8] & 0x3f) | 0x80;
    std::ostringstream out; for (size_t i = 0; i < sizeof(bytes); ++i) { if (i == 4 || i == 6 || i == 8 || i == 10) out << '-'; out << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(bytes[i]); } return out.str();
}
struct Config {
    std::string address = env("QUEUE_ADDR", "control-plane:9091"), token = env("FF_INTERNAL_TOKEN"), runtime = env("FF_RUNTIME_DIGEST", "cpp20-linux-amd64-v1"), runner_id, boot_id = uuid(), container_id;
    int slots = env_int("RUNNER_SLOTS", 2);
    int64_t memory = env_int("RUNNER_MEMORY_BYTES", 1073741824), cpu = env_int("RUNNER_CPU_MILLIS", 2000), grace_ms = env_int("RUNNER_SHUTDOWN_GRACE_MS", 45000);
    uint64_t max_payload = env_int("FF_MAX_PAYLOAD_BYTES", 16 * 1024 * 1024), inline_limit = 64 * 1024;
    std::filesystem::path directory = env("FF_WORK_DIR", env("RUNNER_WORK_DIR", "/tmp/fluxforge")), host_binary, worker_binary;
    Config() {
        char hostname[256]{}; if (gethostname(hostname, sizeof(hostname) - 1) != 0) throw std::runtime_error("gethostname failed"); container_id = env("HOSTNAME", hostname); runner_id = env("RUNNER_ID", container_id);
        auto executable = std::filesystem::read_symlink("/proc/self/exe").parent_path(); host_binary = env("FF_WORKER_HOST", (executable / "fluxforge-worker-host").string()); worker_binary = env("FF_WORKER_BINARY", (executable / "fluxforge-worker").string());
        if (slots < 1 || slots > 128 || memory < 64 * 1024 * 1024 || cpu < 1 || max_payload < inline_limit || max_payload > 64 * 1024 * 1024 || grace_ms < 1000) throw std::runtime_error("invalid Runner resource configuration");
        std::filesystem::create_directories(directory / "attempts"); std::filesystem::create_directories(directory / "artifacts");
        std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
    }
};
}
