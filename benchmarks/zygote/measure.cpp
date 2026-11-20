#include "fluxforge/worker_client.hpp"
#include <iostream>
#include <sstream>
#include <sys/utsname.h>

namespace {
using ff::json;

constexpr auto host = "/build/fluxforge-worker-host";
constexpr auto worker = "/build/fluxforge-worker";
constexpr int pairs = 100;
constexpr int warmup_pairs = 5;

json request(const std::filesystem::path& directory) {
    return {{"attempt_id", ""}, {"affinity_key", ""}, {"library_path", "/build/libff_zygote_fixture.so"},
            {"input_path", (directory / "input.json").string()},
            {"output_path", (directory / "output.json").string()},
            {"memory_limit_bytes", 256 * 1024 * 1024}, {"timeout_ms", 2000}, {"require_prepared", true}};
}

void require_status(const json& response, const char* status) {
    if (response.at("status") != status) throw std::runtime_error(response.dump());
}

template<class Predicate>
void wait_until(Predicate predicate, const char* error) {
    const auto deadline = ff::Clock::now() + std::chrono::seconds(2);
    while (!predicate()) {
        if (ff::Clock::now() >= deadline) throw std::runtime_error(error);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

void wait_for_exit(int pid) {
    wait_until([=] { return kill(pid, 0) < 0 && errno == ESRCH; }, "Worker was not reaped");
}

json measure_latency(const std::filesystem::path& directory) {
    ff::WorkerClient client(host, worker, 2);
    client.start();
    auto req = request(directory);
    auto samples = json::array();
    for (int i = -warmup_pairs; i < pairs; ++i) {
        req["affinity_key"] = "pair-" + std::to_string(i);
        json sample{{"pair", i + 1}};
        int seed = 0;
        for (bool hit : {false, true}) {
            req["attempt_id"] = std::to_string(i) + (hit ? "-hit" : "-miss");
            const auto start = ff::Clock::now();
            auto prepared = client.prepare(req).get();
            const auto elapsed = std::chrono::duration<double, std::micro>(ff::Clock::now() - start).count();
            require_status(prepared, "PREPARED");
            const int prepared_seed = prepared.at("zygote_pid");
            if (prepared.at("warm").get<bool>() != hit || (hit && prepared_seed != seed))
                throw std::runtime_error("unexpected Zygote cache state: " + prepared.dump());
            seed = prepared_seed;
            sample[hit ? "hit_us" : "miss_us"] = elapsed;
            require_status(client.invoke(req).get(), "SUCCESS");
            wait_for_exit(prepared.at("worker_pid"));
        }
        if (i >= 0) samples.push_back(std::move(sample));
    }
    return samples;
}

json process_memory(int pid, const char* role) {
    std::ifstream proc("/proc/" + std::to_string(pid) + "/smaps_rollup");
    if (!proc) throw std::runtime_error("cannot open smaps_rollup for " + std::to_string(pid));
    json fields = json::object();
    std::string line;
    while (std::getline(proc, line)) {
        std::istringstream entry(line);
        std::string key;
        int64_t kib;
        if (entry >> key >> kib) fields[key] = kib;
    }
    return {{"role", role}, {"pid", pid}, {"rss_kib", fields.at("Rss:")}, {"pss_kib", fields.at("Pss:")},
            {"private_kib", fields.at("Private_Clean:").get<int64_t>() + fields.at("Private_Dirty:").get<int64_t>()}};
}

int parent_of(int pid) {
    std::ifstream proc("/proc/" + std::to_string(pid) + "/status");
    std::string line;
    while (std::getline(proc, line)) {
        if (line.starts_with("PPid:")) return std::stoi(line.substr(5));
    }
    throw std::runtime_error("cannot read worker parent PID");
}

json measure_memory(const std::filesystem::path& directory) {
    // Use a fresh Host so a second cached function cannot affect these samples.
    ff::WorkerClient client(host, worker, 2);
    client.start();
    auto req = request(directory);
    const int base = client.stats().get().at("base_pid");
    const int supervisor = parent_of(base);
    wait_until([=] {
        std::error_code error;
        return std::filesystem::read_symlink("/proc/" + std::to_string(base) + "/exe", error) == worker;
    }, "Base-Zygote did not exec the Worker binary");

    auto phases = json::array();
    int seed = 0;
    auto snapshot = [&](const char* phase, std::initializer_list<int> leaves) {
        auto processes = json::array({process_memory(supervisor, "Host"), process_memory(base, "Base-Zygote")});
        if (seed) processes.push_back(process_memory(seed, "Function-Zygote"));
        for (int leaf : leaves) processes.push_back(process_memory(leaf, "Worker"));
        phases.push_back({{"phase", phase}, {"processes", std::move(processes)}});
    };
    snapshot("base_only", {});

    req["affinity_key"] = "memory";
    req["attempt_id"] = "seed-filler";
    auto cold = client.prepare(req).get();
    require_status(cold, "PREPARED");
    seed = cold.at("zygote_pid");
    require_status(client.invoke(req).get(), "SUCCESS");
    wait_for_exit(cold.at("worker_pid"));
    snapshot("seed_idle", {});

    req["attempt_id"] = "parked-a";
    auto a = client.prepare(req).get();
    require_status(a, "PREPARED");
    const int worker_a = a.at("worker_pid");
    snapshot("one_worker", {worker_a});

    auto second = req;
    second["attempt_id"] = "parked-b";
    second["output_path"] = (directory / "second.json").string();
    auto b = client.prepare(second).get();
    require_status(b, "PREPARED");
    const int worker_b = b.at("worker_pid");
    if (worker_a == worker_b || a.at("zygote_pid") != seed || b.at("zygote_pid") != seed)
        throw std::runtime_error("memory snapshot did not use two Workers from one seed");
    snapshot("two_workers", {worker_a, worker_b});

    require_status(client.invoke(req).get(), "SUCCESS");
    require_status(client.invoke(second).get(), "SUCCESS");
    wait_for_exit(worker_a);
    wait_for_exit(worker_b);
    snapshot("after_exit", {});
    return phases;
}
}

int main(int argc, char** argv) {
    try {
        if (argc != 2) throw std::runtime_error("expected scratch directory");
        const std::filesystem::path directory = argv[1];
        ff::write_file(directory / "input.json", "{}");
        utsname system_info{};
        if (uname(&system_info) != 0) throw std::runtime_error("uname failed");
        json result{{"schema_version", 1}, {"architecture", system_info.machine}, {"kernel", system_info.release},
                    {"builder_image_id", ff::env("FF_BENCH_IMAGE_ID")}, {"uid", getuid()},
                    {"fixture_bytes", 8 * 1024 * 1024}, {"warmup_pairs", warmup_pairs},
                    {"latency_pairs", measure_latency(directory)}, {"memory_phases", measure_memory(directory)}};
        std::cout << result.dump(2) << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "zygote benchmark: " << error.what() << '\n';
        return 1;
    }
}
