#include "fluxforge/worker_client.hpp"
#include <iostream>
int main(int argc, char** argv) {
    try {
        if (argc != 8) throw std::runtime_error("expected host worker echo spin crash log_flood large_output paths");
        auto directory = std::filesystem::temp_directory_path() / ("ff-worker-test-" + std::to_string(getpid())); std::filesystem::create_directories(directory);
        ff::write_file(directory / "input.json", "{\"message\":\"warm\"}");
        ff::WorkerClient client(argv[1], argv[2], 2); client.start();
        ff::json req{{"attempt_id", "test-1"}, {"affinity_key", "echo"}, {"library_path", argv[3]}, {"input_path", (directory / "input.json").string()}, {"output_path", (directory / "output.json").string()}, {"timeout_ms", 1000}, {"memory_limit_bytes", 256 * 1024 * 1024}};
        auto first = client.invoke(req).get(); if (first.at("status") != "SUCCESS") throw std::runtime_error(first.dump());
        req["attempt_id"] = "test-2"; auto second = client.invoke(req).get();
        if (second.at("status") != "SUCCESS" || first.at("worker_pid") != second.at("worker_pid") || !second.at("warm").get<bool>()) throw std::runtime_error("worker did not remain warm: " + second.dump());
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
        std::filesystem::remove_all(directory); return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
