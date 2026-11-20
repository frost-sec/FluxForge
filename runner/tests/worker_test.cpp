#include "fluxforge/worker_client.hpp"
#include <algorithm>
#include <iostream>
#include <vector>

namespace {
int parent_of(int pid) {
    std::ifstream proc("/proc/" + std::to_string(pid) + "/status");
    std::string key;
    while (proc >> key) {
        if (key == "PPid:") {
            int parent = 0;
            proc >> parent;
            return parent;
        }
        proc.ignore(8192, '\n');
    }
    return 0;
}

void wait_for_exit(int pid) {
    for (int i = 0; i < 100; ++i) {
        if (kill(pid, 0) < 0 && errno == ESRCH) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    throw std::runtime_error("process did not exit and reap: " + std::to_string(pid));
}

int64_t private_kib(int pid) {
    std::ifstream proc("/proc/" + std::to_string(pid) + "/smaps_rollup");
    if (!proc) return -1;
    int64_t total = 0;
    std::string key;
    while (proc >> key) {
        if (key == "Private_Dirty:" || key == "Private_Clean:") {
            int64_t kib = 0;
            proc >> kib;
            total += kib;
        }
        proc.ignore(8192, '\n');
    }
    return total == 0 ? -1 : total;
}

void check_zygote(ff::WorkerClient& client, ff::json req, const std::string& library) {
    req["library_path"] = library;
    req["affinity_key"] = "snapshot";
    req["attempt_id"] = "snapshot-a";
    req["require_prepared"] = true;
    auto first_future = client.prepare(req);
    auto second = req;
    second["attempt_id"] = "snapshot-b";
    second["output_path"] = req.at("output_path").get<std::string>() + ".second";
    auto second_future = client.prepare(second);
    auto a = first_future.get();
    auto b = second_future.get();
    if (a.at("status") != "PREPARED" || b.at("status") != "PREPARED" ||
        a.at("zygote_pid") != b.at("zygote_pid") || a.at("worker_pid") == b.at("worker_pid") || a.at("warm"))
        throw std::runtime_error("concurrent cold preparations did not share a seed: " + a.dump() + b.dump());

    const int seed = a.at("zygote_pid");
    const int base = client.stats().get().at("base_pid");
    if (parent_of(a.at("worker_pid")) != seed || parent_of(b.at("worker_pid")) != seed || parent_of(seed) != base)
        throw std::runtime_error("unexpected zygote process tree");
    const auto seed_private = private_kib(seed);
    const auto a_private = private_kib(a.at("worker_pid"));
    const auto b_private = private_kib(b.at("worker_pid"));
    if (seed_private >= 0 && a_private >= 0 && b_private >= 0)
        std::cout << "8 MiB initialized heap; private KiB while parked: seed=" << seed_private
                  << " worker_a=" << a_private << " worker_b=" << b_private << '\n';

    auto run_a = client.invoke(req);
    auto run_b = client.invoke(second);
    auto check = [&](const ff::json& result, const ff::json& request, const ff::json& prepared) {
        if (result.at("status") != "SUCCESS" || result.at("worker_pid") != prepared.at("worker_pid"))
            throw std::runtime_error("prepared worker was not used: " + result.dump());
        auto output = ff::json::parse(ff::read_file(request.at("output_path").get<std::string>(), 4096));
        if (output.at("parent_pid") != seed || output.at("initializer_pid") != seed ||
            output.at("worker_pid") != result.at("worker_pid") || output.at("invocations") != 1 || output.at("page_value") != 43)
            throw std::runtime_error("worker did not inherit pristine function state: " + output.dump());
    };
    check(run_a.get(), req, a);
    check(run_b.get(), second, b);
    wait_for_exit(a.at("worker_pid"));
    wait_for_exit(b.at("worker_pid"));

    req["attempt_id"] = "lost-seed";
    auto parked = client.prepare(req).get();
    if (parked.at("status") != "PREPARED" || parked.at("zygote_pid") != seed || kill(seed, SIGKILL) != 0)
        throw std::runtime_error("could not exercise seed failure");
    wait_for_exit(parked.at("worker_pid"));
    wait_for_exit(seed);
    if (client.invoke(req).get().at("status") != "CRASHED")
        throw std::runtime_error("lost prepared worker was silently replaced");

    std::vector<int64_t> cold, warm;
    for (int i = 0; i < 20; ++i) {
        // A new environment key forces a miss, then stays constant for its hit.
        req["affinity_key"] = "snapshot-" + std::to_string(i);
        for (bool hit : {false, true}) {
            req["attempt_id"] = std::to_string(i) + (hit ? "-hit" : "-miss");
            const auto start = ff::Clock::now();
            auto prepared = client.prepare(req).get();
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(ff::Clock::now() - start).count();
            if (prepared.at("status") != "PREPARED" || prepared.at("warm") != hit)
                throw std::runtime_error("unexpected seed cache state: " + prepared.dump());
            (hit ? warm : cold).push_back(us);
            auto result = client.invoke(req).get();
            if (result.at("status") != "SUCCESS") throw std::runtime_error(result.dump());
        }
    }
    auto report = [](const char* name, std::vector<int64_t>& samples) {
        std::sort(samples.begin(), samples.end());
        std::cout << name << " prepare us (20 samples): p50=" << samples[9]
                  << " p95=" << samples[18] << " p99=" << samples[19] << '\n';
    };
    report("zygote miss", cold);
    report("zygote hit", warm);
    if (client.stats().get().at("workers") != 0)
        throw std::runtime_error("completed one-shot worker retained a slot");
}
}

int main(int argc, char** argv) {
    try {
        if (argc != 9) throw std::runtime_error("expected host worker echo spin crash log_flood large_output zygote_fixture paths");
        auto directory = std::filesystem::temp_directory_path() / ("ff-worker-test-" + std::to_string(getpid())); std::filesystem::create_directories(directory);
        ff::write_file(directory / "input.json", "{\"message\":\"warm\"}");
        ff::WorkerClient client(argv[1], argv[2], 2); client.start();
        ff::json req{{"attempt_id", "test-1"}, {"affinity_key", "echo"}, {"library_path", argv[3]}, {"input_path", (directory / "input.json").string()}, {"output_path", (directory / "output.json").string()}, {"timeout_ms", 1000}, {"memory_limit_bytes", 256 * 1024 * 1024}};
        auto first = client.invoke(req).get(); if (first.at("status") != "SUCCESS") throw std::runtime_error(first.dump());
        req["attempt_id"] = "test-2"; auto second = client.invoke(req).get();
        if (second.at("status") != "SUCCESS" || first.at("worker_pid") == second.at("worker_pid") || first.at("zygote_pid") != second.at("zygote_pid") || !second.at("warm").get<bool>()) throw std::runtime_error("one-shot Worker did not reuse the zygote: " + second.dump());
        if (ff::json::parse(ff::read_file(directory / "output.json", 4096)).at("message") != "warm") throw std::runtime_error("wrong function output");
        req["attempt_id"] = "test-prepare";
        auto prepared = client.prepare(req).get(); if (prepared.at("status") != "PREPARED") throw std::runtime_error("explicit preparation failed");
        auto prepared_pid = prepared.at("worker_pid");
        auto after_prepare = client.invoke(req).get();
        if (after_prepare.at("status") != "SUCCESS" || after_prepare.at("worker_pid") != prepared_pid) throw std::runtime_error("prepared worker was not reserved for invocation");
        req["attempt_id"] = "test-schema"; req["input_schema"] = "{\"type\":\"object\",\"required\":[\"missing\"]}";
        auto invalid = client.invoke(req).get(); if (invalid.at("status") != "USER_FAILED") throw std::runtime_error("schema was not checked"); req.erase("input_schema");
        req["attempt_id"] = "test-spin"; req["affinity_key"] = "spin"; req["library_path"] = argv[4]; req["timeout_ms"] = 100;
        auto timeout = client.invoke(req).get(); if (timeout.at("status") != "TIMEOUT") throw std::runtime_error("infinite function not terminated: " + timeout.dump());
        req["attempt_id"] = "test-cancel"; auto cancelling = client.invoke(req); std::this_thread::sleep_for(std::chrono::milliseconds(50)); client.cancel("test-cancel", "test cancellation");
        auto cancelled = cancelling.get(); if (cancelled.at("status") != "CANCELLED") throw std::runtime_error("cancellation failed");
        if (timeout.at("zygote_pid") != cancelled.at("zygote_pid") || !cancelled.at("warm").get<bool>()) throw std::runtime_error("timeout destroyed the warm function zygote");
        req["attempt_id"] = "test-crash"; req["affinity_key"] = "crash"; req["library_path"] = argv[5]; req["timeout_ms"] = 1000;
        auto crashed = client.invoke(req).get(); if (crashed.at("status") != "CRASHED") throw std::runtime_error("crashed worker was not reaped: " + crashed.dump());
        req["attempt_id"] = "test-log-flood"; req["affinity_key"] = "log_flood"; req["library_path"] = argv[6]; req["timeout_ms"] = 200;
        auto flooded = client.invoke(req).get(); if (flooded.at("status") != "TIMEOUT") throw std::runtime_error("log flood blocked execution supervision");
        ff::write_file(directory / "input.json", "{\"bytes\":131072}");
        req["attempt_id"] = "test-large-output"; req["affinity_key"] = "large_output"; req["library_path"] = argv[7]; req["timeout_ms"] = 1000;
        auto large = client.invoke(req).get(); if (large.at("status") != "SUCCESS" || std::filesystem::file_size(directory / "output.json") < 65536) throw std::runtime_error("large local output failed: " + large.dump());
        req["attempt_id"] = "test-output-bound"; req["max_payload_bytes"] = 65536;
        auto oversized = client.invoke(req).get(); if (oversized.at("status") == "SUCCESS") throw std::runtime_error("output limit not enforced"); req.erase("max_payload_bytes");
        ff::write_file(directory / "input.json", "{\"message\":\"warm\"}");
        req["attempt_id"] = "test-recovery"; req["affinity_key"] = "echo"; req["library_path"] = argv[3]; req["timeout_ms"] = 1000;
        auto recovered = client.invoke(req).get(); if (recovered.at("status") != "SUCCESS") throw std::runtime_error("worker capacity did not recover: " + recovered.dump());
        check_zygote(client, req, argv[8]);
        std::filesystem::remove_all(directory); return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
