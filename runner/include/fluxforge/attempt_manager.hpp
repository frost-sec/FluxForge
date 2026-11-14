#pragma once
#include "common.hpp"
#include "fluxforge_v1.pb.h"
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
namespace ff {
enum class Phase { Preparing, Running, Uploading, Completing, Done };
struct Attempt {
    fluxforge::v1::TaskAssignment task;
    std::atomic<int64_t> lease_deadline{0};
    std::atomic<bool> revoked{false}, done{false};
    std::atomic<Phase> phase{Phase::Preparing};
    bool alive() const { return !revoked.load() && now_ms() < lease_deadline.load(); }
};
class AttemptManager {
    struct Job { std::shared_ptr<Attempt> attempt; std::thread thread; };
    std::mutex mutex_; std::unordered_map<std::string, Job> jobs_;
public:
    template<class Function> bool start(const fluxforge::v1::TaskAssignment& task, Function function) {
        std::lock_guard lock(mutex_); if (jobs_.count(task.attempt_id())) return false;
        auto attempt = std::make_shared<Attempt>(); attempt->task = task;
        const auto& lease = task.initial_lease(); auto remaining = lease.lease_duration_ms(); if (lease.expires_unix_ms() && lease.server_unix_ms()) remaining = std::min(remaining, lease.expires_unix_ms() - lease.server_unix_ms());
        // RPC timeout is three seconds: reserve that entire budget conservatively.
        attempt->lease_deadline = now_ms() + std::max<int64_t>(0, remaining - 3500);
        auto [it, inserted] = jobs_.emplace(task.attempt_id(), Job{attempt, {}});
        it->second.thread = std::thread([attempt, function] { function(attempt); attempt->phase = Phase::Done; attempt->done = true; }); return true;
    }
    std::vector<std::shared_ptr<Attempt>> snapshot() { std::lock_guard lock(mutex_); std::vector<std::shared_ptr<Attempt>> result; for (auto& [id, job] : jobs_) if (!job.attempt->done) result.push_back(job.attempt); return result; }
    size_t size() { return snapshot().size(); }
    void reap() { std::vector<std::thread> completed; { std::lock_guard lock(mutex_); for (auto it = jobs_.begin(); it != jobs_.end();) { if (it->second.attempt->done) { completed.push_back(std::move(it->second.thread)); it = jobs_.erase(it); } else ++it; } } for (auto& thread : completed) thread.join(); }
    ~AttemptManager() { for (auto& [id, job] : jobs_) if (job.thread.joinable()) job.thread.join(); }
};
}
