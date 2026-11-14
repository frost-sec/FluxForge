#pragma once
#include "attempt_manager.hpp"
#include "control_client.hpp"
#include "input_plan.hpp"
#include "metrics.hpp"
#include "worker_client.hpp"
#include <sys/signalfd.h>
#include <iostream>
namespace ff {
inline void log_event(const std::string& event, const json& fields = json::object()) { auto message = fields; message["event"] = event; std::cerr << message.dump(-1, ' ', false, json::error_handler_t::replace) << '\n'; }
class Agent {
    Config config_; WorkerClient& workers_; ControlClient queue_; std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<ObjectIO> objects_; ArtifactCache cache_; AttemptManager attempts_; Metrics metrics_;
    std::string session_;
    std::atomic<pb::RunnerStatePolicy> policy_{pb::RUNNER_POLICY_ACTIVE};
    std::atomic<bool> stopping_{false}; int heartbeat_seconds_ = 3;
    std::thread heartbeat_, renewal_, metrics_thread_;
    void revoke(const std::shared_ptr<Attempt>& attempt, const std::string& reason) {
        if (!attempt->revoked.exchange(true)) { try { workers_.cancel(attempt->task.attempt_id(), reason); } catch (...) {} log_event("attempt_revoked", {{"attempt_id", attempt->task.attempt_id()}, {"reason", reason}}); }
    }
    bool sleep(int64_t milliseconds) { auto end = now_ms() + milliseconds; while (!stopping_ && now_ms() < end) std::this_thread::sleep_for(std::chrono::milliseconds(50)); return !stopping_; }
    template<class Operation> bool retry(const std::shared_ptr<Attempt>& attempt, Operation operation) {
        while (attempt->alive()) { auto status = operation(); if (status.ok()) return true; if (!retryable(status)) { revoke(attempt, status.error_message()); return false; } std::this_thread::sleep_for(std::chrono::milliseconds(200)); }
        revoke(attempt, "lease expired during RPC"); return false;
    }
    template<class Future> json await(const std::shared_ptr<Attempt>& attempt, Future& future, int64_t deadline) {
        for (;;) { if (future.wait_for(std::chrono::milliseconds(50)) == std::future_status::ready) return future.get(); if (!attempt->alive() || now_ms() >= deadline) { revoke(attempt, "attempt preparation or execution deadline exceeded"); throw std::runtime_error("attempt no longer executable"); } }
    }
    static pb::AttemptStatus status(const std::string& value) {
        if (value == "SUCCESS") return pb::ATTEMPT_STATUS_SUCCESS;
        if (value == "USER_FAILED") return pb::ATTEMPT_STATUS_USER_FAILED;
        if (value == "TIMEOUT") return pb::ATTEMPT_STATUS_TIMEOUT;
        if (value == "CANCELLED") return pb::ATTEMPT_STATUS_CANCELLED;
        if (value == "RESOURCE_EXHAUSTED") return pb::ATTEMPT_STATUS_RESOURCE_ERROR;
        return pb::ATTEMPT_STATUS_CRASHED;
    }
    void execute(const std::shared_ptr<Attempt>& attempt) noexcept {
        const auto& task = attempt->task; const auto& function = task.function_spec();
        auto directory = config_.directory / "attempts" / task.attempt_id();
        pb::ReportCompletionRequest completion; completion.set_runner_id(config_.runner_id); completion.set_session_id(session_); completion.set_attempt_id(task.attempt_id()); completion.set_lease_token(task.initial_lease().lease_token()); completion.set_final_status(pb::ATTEMPT_STATUS_INFRA_ERROR);
        const auto trace_base = now_ms();
        const auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        const auto trace_offset = task.created_unix_ms() > 0 ? std::max<int64_t>(0, wall_ms - task.created_unix_ms()) : 0;
        auto record_stage = [&](const char* name, int64_t start) { auto* stage=completion.add_stages(); stage->set_name(name); stage->set_offset_ms(trace_offset+std::max<int64_t>(0,start-trace_base)); stage->set_duration_ms(std::max<int64_t>(0,now_ms()-start)); };
        try {
            if (function.runtime() != pb::RUNTIME_CPP20 || function.abi_version() != 1 || function.runtime_digest() != config_.runtime) throw std::runtime_error("unsupported runtime, ABI or runtime image compatibility ID");
            if (task.attempt_id().find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != std::string::npos) throw std::runtime_error("invalid attempt ID");
            std::filesystem::create_directories(directory);
            const auto preparing_until = now_ms() + (task.prepare_timeout_ms() > 0 ? task.prepare_timeout_ms() : 30000);
            auto alive = [attempt, preparing_until] { return attempt->alive() && (attempt->phase != Phase::Preparing || now_ms() < preparing_until); };
            auto stage_start=now_ms(); auto artifact = cache_.get(function.artifact(), *objects_, task.attempt_id(), alive); record_stage("artifact.fetch",stage_start);
            stage_start=now_ms(); InputResolver inputs(*objects_, directory, task.attempt_id(), config_.max_payload, alive); auto input = inputs.resolve(task.input_plan()); record_stage("input.resolve",stage_start);
            json request{{"attempt_id", task.attempt_id()}, {"run_id", task.run_id()}, {"invocation_id", task.invocation_id()}, {"affinity_key", task.affinity_key()}, {"library_path", artifact.path.string()}, {"input_path", input.string()}, {"output_path", (directory / "output.json").string()}, {"input_schema", function.input_schema()}, {"output_schema", function.output_schema()}, {"memory_limit_bytes", function.memory_limit_bytes()}, {"timeout_ms", function.timeout_ms()}, {"prepare_timeout_ms", std::max<int64_t>(1, preparing_until - now_ms())}, {"max_payload_bytes", config_.max_payload}};
            stage_start=now_ms(); auto preparing = workers_.prepare(request); auto prepared = await(attempt, preparing, preparing_until); record_stage("worker.prepare",stage_start);
            if (prepared.value("status", "") != "PREPARED") throw std::runtime_error("worker preparation failed: " + prepared.value("error", "unknown") + " " + prepared.value("log_tail", ""));
            pb::ReportStartedRequest started; started.set_runner_id(config_.runner_id); started.set_session_id(session_); started.set_attempt_id(task.attempt_id()); started.set_lease_token(task.initial_lease().lease_token()); pb::ReportStartedResponse accepted;
            const auto started_at = now_ms(); if (!retry(attempt, [&] { return queue_.ReportStarted(started, accepted); }) || !accepted.valid()) throw std::runtime_error("execution start rejected");
            auto execution_budget = function.timeout_ms(); if (accepted.execution_deadline_unix_ms() && accepted.server_unix_ms()) execution_budget = std::min(execution_budget, accepted.execution_deadline_unix_ms() - accepted.server_unix_ms());
            execution_budget -= std::min<int64_t>(now_ms() - started_at, 3000); if (execution_budget <= 0) throw std::runtime_error("execution deadline already expired");
            request["timeout_ms"] = execution_budget; attempt->phase = Phase::Running;
            stage_start=now_ms(); auto executing = workers_.invoke(request); auto result = await(attempt, executing, now_ms() + execution_budget + 1000); record_stage("worker.execute",stage_start);
            completion.set_final_status(status(result.value("status", "CRASHED"))); completion.set_duration_ms(result.value("duration_ms", 0LL)); completion.set_error_message(result.value("error", "").substr(0, 4096));
            metrics_.record(function, completion.duration_ms(), completion.final_status() == pb::ATTEMPT_STATUS_SUCCESS, result.value("warm", false));
            if (completion.final_status() == pb::ATTEMPT_STATUS_SUCCESS) {
                const auto output = directory / "output.json"; const auto size = std::filesystem::file_size(output);
                if (size <= config_.inline_limit) completion.mutable_output()->set_inline_json(read_file(output, config_.max_payload));
                else {
                    pb::ReportExecutionFinishedRequest finished; finished.set_session_id(session_); finished.set_attempt_id(task.attempt_id()); finished.set_lease_token(task.initial_lease().lease_token()); finished.set_duration_ms(completion.duration_ms()); pb::ReportExecutionFinishedResponse upload;
                    if (!retry(attempt, [&] { return queue_.ReportExecutionFinished(finished, upload); }) || !upload.valid()) throw std::runtime_error("output upload phase rejected");
                    attempt->phase = Phase::Uploading;
                    const auto uploading_until = now_ms() + std::max<int64_t>(0, upload.upload_deadline_unix_ms() - upload.server_unix_ms() - 3000);
                    auto upload_alive = [attempt, uploading_until] { return attempt->alive() && now_ms() < uploading_until; };
                    stage_start=now_ms(); pb::ObjectRef object;
                    while (upload_alive()) {
                        try { object = objects_->upload(output, task.attempt_id(), upload_alive); break; }
                        catch (const std::exception&) { if (!upload_alive()) throw; std::this_thread::sleep_for(std::chrono::milliseconds(250)); }
                    }
                    if (object.object_id().empty()) throw std::runtime_error("upload deadline exceeded"); *completion.mutable_output()->mutable_object() = object; record_stage("output.upload",stage_start);
                }
            }
            log_event("attempt_executed", {{"attempt_id", task.attempt_id()}, {"run_id", task.run_id()}, {"invocation_id", task.invocation_id()}, {"worker_pid", result.value("worker_pid", 0)}, {"warm", result.value("warm", false)}, {"status", result.value("status", "CRASHED")}, {"duration_ms", completion.duration_ms()}, {"traceparent", task.trace_context().traceparent()}});
        } catch (const std::exception& e) { completion.set_final_status(pb::ATTEMPT_STATUS_INFRA_ERROR); completion.clear_output(); completion.set_error_message(std::string(e.what()).substr(0, 4096)); log_event("attempt_error", {{"attempt_id", task.attempt_id()}, {"error", e.what()}}); }
        catch (...) { completion.set_error_message("unknown runner error"); }
        try {
            if (attempt->alive()) { attempt->phase = Phase::Completing; pb::ReportCompletionResponse response; if (retry(attempt, [&] { return queue_.ReportCompletion(completion, response); }) && !response.acknowledged()) revoke(attempt, "completion not acknowledged"); }
            else revoke(attempt, "lease ended before completion acknowledgement");
            std::filesystem::remove_all(directory);
        } catch (const std::exception& e) { log_event("attempt_cleanup_error", {{"attempt_id", task.attempt_id()}, {"error", e.what()}}); }
    }
    void heartbeat_loop() {
        while (!stopping_) {
            pb::HeartbeatRequest req; req.set_runner_id(config_.runner_id); req.set_session_id(session_); req.set_observed_policy(policy_.load()); auto active = attempts_.snapshot(); req.set_available_slots(std::max<int>(0, config_.slots - active.size())); req.set_active_attempts(active.size());
            for (auto& attempt : active) if (attempt->phase == Phase::Completing) req.set_pending_completions(req.pending_completions() + 1);
            try { auto query = workers_.stats(); if (query.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready) { auto stats = query.get(); req.set_used_memory_bytes(stats.value("worker_rss_bytes", 0ULL)); for (const auto& key : stats.at("affinity_keys")) req.add_cached_affinity_keys(key.get<std::string>()); } } catch (const std::exception& e) { log_event("worker_host_failed", {{"error", e.what()}}); policy_ = pb::RUNNER_POLICY_OFFLINE; }
            pb::HeartbeatResponse resp; auto code = queue_.Heartbeat(req, resp);
            if (code.ok() && resp.acknowledged()) { policy_ = resp.policy(); for (const auto& id : resp.cancel_attempt_ids()) for (auto& attempt : active) if (attempt->task.attempt_id() == id) revoke(attempt, "control plane cancellation"); }
            else if (!code.ok() && !retryable(code)) { policy_ = pb::RUNNER_POLICY_OFFLINE; for (auto& attempt : active) revoke(attempt, "runner session rejected"); }
            if (!sleep(heartbeat_seconds_ * 1000)) break;
        }
    }
    void renew_loop() {
        while (!stopping_) {
            auto active = attempts_.snapshot(); pb::RenewLeasesRequest req; req.set_runner_id(config_.runner_id); req.set_session_id(session_);
            for (auto& attempt : active) { if (!attempt->alive()) { revoke(attempt, "local lease deadline exceeded"); continue; } auto* item = req.add_items(); item->set_attempt_id(attempt->task.attempt_id()); item->set_lease_token(attempt->task.initial_lease().lease_token()); }
            if (req.items_size()) { pb::RenewLeasesResponse resp; const auto sent = now_ms(); auto code = queue_.RenewLeases(req, resp); if (code.ok()) for (const auto& result : resp.results()) for (auto& attempt : active) if (attempt->task.attempt_id() == result.attempt_id()) { if (result.valid()) attempt->lease_deadline = sent + std::max<int64_t>(0, result.remaining_ms() - 500); else revoke(attempt, result.reason()); } }
            if (!sleep(1000)) break;
        }
    }
    void metrics_loop() {
        uint64_t sequence = 1; auto request = metrics_.snapshot(config_.runner_id, session_, sequence);
        while (!stopping_) { pb::ReportMetricsResponse response; auto code = queue_.ReportMetrics(request, response); if (code.ok()) request = metrics_.snapshot(config_.runner_id, session_, ++sequence); if (!sleep(1000)) break; }
    }
public:
    Agent(Config config, WorkerClient& workers, std::shared_ptr<grpc::Channel> channel) : config_(std::move(config)), workers_(workers), queue_(channel, config_.token), channel_(std::move(channel)), cache_(config_.directory / "artifacts") {}
    bool register_runner(int signal_fd) {
        pb::RegisterRunnerRequest request; request.set_runner_id(config_.runner_id); request.set_boot_id(config_.boot_id); request.set_container_id(config_.container_id); request.set_max_slots(config_.slots); request.set_memory_capacity_bytes(config_.memory); request.set_cpu_capacity_millis(config_.cpu); request.set_architecture("linux/amd64"); request.set_runtime_digest(config_.runtime); request.add_supported_runtimes(pb::RUNTIME_CPP20);
        int backoff = 250;
        for (;;) {
            signalfd_siginfo info; if (read(signal_fd, &info, sizeof(info)) == sizeof(info)) return false;
            pb::RegisterRunnerResponse response; auto code = queue_.RegisterRunner(request, response);
            if (code.ok() && response.success() && !response.session_id().empty()) { session_ = response.session_id(); heartbeat_seconds_ = std::clamp(response.heartbeat_interval_sec(), 1, 30); policy_ = response.policy(); objects_ = std::make_unique<ObjectIO>(channel_, session_, config_.token); log_event("runner_registered", {{"runner_id", config_.runner_id}, {"session_id", session_}, {"slots", config_.slots}}); return true; }
            if (!code.ok() && !retryable(code)) throw std::runtime_error("runner registration rejected: " + code.error_message());
            log_event("registration_retry", {{"error", code.error_message()}, {"backoff_ms", backoff}});
            for (int elapsed = 0; elapsed < backoff; elapsed += 50) { if (read(signal_fd, &info, sizeof(info)) == sizeof(info)) return false; std::this_thread::sleep_for(std::chrono::milliseconds(50)); } backoff = std::min(backoff * 2, 10000);
        }
    }
    void run(int signal_fd) {
        workers_.start(); heartbeat_ = std::thread([this] { heartbeat_loop(); }); renewal_ = std::thread([this] { renew_loop(); }); metrics_thread_ = std::thread([this] { metrics_loop(); });
        pb::AcquireTasksRequest pending_request; bool has_pending_request = false; bool draining = false; int64_t stop_at = 0;
        try {
            for (;;) {
                attempts_.reap(); signalfd_siginfo info;
                if (!draining && read(signal_fd, &info, sizeof(info)) == sizeof(info)) { draining = true; policy_ = pb::RUNNER_POLICY_DRAINING; stop_at = now_ms() + config_.grace_ms; log_event("runner_draining"); }
                if (policy_ == pb::RUNNER_POLICY_OFFLINE) { draining = true; stop_at = now_ms(); }
                auto active = attempts_.snapshot(); if (draining && (active.empty() || now_ms() >= stop_at)) { for (auto& attempt : active) revoke(attempt, "shutdown grace exceeded"); if (active.empty()) break; }
                if (draining || policy_ != pb::RUNNER_POLICY_ACTIVE || active.size() >= static_cast<size_t>(config_.slots)) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); continue; }
                if (!has_pending_request) {
                    pending_request.Clear(); pending_request.set_runner_id(config_.runner_id); pending_request.set_session_id(session_); pending_request.set_requested_slots(config_.slots - active.size()); pending_request.set_request_id(uuid()); pending_request.set_wait_timeout_ms(1000); has_pending_request = true;
                }
                pb::AcquireTasksResponse response; auto code = queue_.AcquireTasks(pending_request, response);
                if (code.ok()) { has_pending_request = false; for (const auto& task : response.tasks()) attempts_.start(task, [this](const std::shared_ptr<Attempt>& attempt) { execute(attempt); }); }
                else { if (!retryable(code)) { log_event("acquire_rejected", {{"error", code.error_message()}}); policy_ = pb::RUNNER_POLICY_OFFLINE; } std::this_thread::sleep_for(std::chrono::milliseconds(200)); }
            }
        } catch (...) { for (auto& attempt : attempts_.snapshot()) revoke(attempt, "runner fatal error"); stop_threads(); throw; }
        stop_threads(); attempts_.reap(); log_event("runner_stopped");
    }
    void stop_threads() { stopping_ = true; if (heartbeat_.joinable()) heartbeat_.join(); if (renewal_.joinable()) renewal_.join(); if (metrics_thread_.joinable()) metrics_thread_.join(); }
    ~Agent() { stop_threads(); }
};
}
