#pragma once
#include "ipc.hpp"
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <atomic>
#include <sys/wait.h>
#include <signal.h>
namespace ff {
class WorkerClient {
    int fd_ = -1; pid_t pid_ = -1;
    std::mutex send_mutex_, promises_mutex_;
    std::unordered_map<std::string, std::promise<json>> promises_;
    std::thread reader_;
    std::atomic<uint64_t> sequence_{1};
    std::atomic<bool> disconnected_{false};
public:
    // Must be called before creating gRPC objects or threads.
    WorkerClient(const std::string& host, const std::string& worker, int slots) {
        int sockets[2]; if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets)) throw std::runtime_error("host socketpair failed");
        const auto capacity = std::to_string(slots); pid_ = fork();
        if (pid_ == 0) {
            if (dup2(sockets[1], 3) < 0) _exit(126); fcntl(3, F_SETFD, 0);
            if (sockets[0] != 3) close(sockets[0]); if (sockets[1] != 3) close(sockets[1]);
            char* args[] = {const_cast<char*>(host.c_str()), const_cast<char*>("3"), const_cast<char*>(capacity.c_str()), const_cast<char*>(worker.c_str()), nullptr};
            char* environment[] = {const_cast<char*>("PATH=/usr/bin:/bin"), const_cast<char*>("LANG=C.UTF-8"), nullptr};
            execve(host.c_str(), args, environment); _exit(127);
        }
        close(sockets[1]); if (pid_ < 0) { close(sockets[0]); throw std::runtime_error("fork host failed"); } fd_ = sockets[0];
    }
    void start() {
        reader_ = std::thread([this] {
            try {
                for (;;) { auto value = ipc::receive_blocking(fd_); auto id = value.at("id").get<std::string>(); std::lock_guard lock(promises_mutex_); auto it = promises_.find(id); if (it != promises_.end()) { it->second.set_value(std::move(value)); promises_.erase(it); } }
            } catch (...) {
                std::lock_guard lock(promises_mutex_);
                disconnected_ = true;
                for (auto& [id, promise] : promises_) promise.set_exception(std::current_exception());
                promises_.clear();
            }
        });
    }
    std::future<json> request(json request) {
        auto id = std::to_string(sequence_++); request["id"] = id;
        std::promise<json> promise; auto future = promise.get_future();
        {
            std::lock_guard lock(promises_mutex_);
            if (disconnected_) {
                promise.set_exception(std::make_exception_ptr(std::runtime_error("worker host disconnected")));
                return future;
            }
            promises_.emplace(id, std::move(promise));
        }
        try { std::lock_guard lock(send_mutex_); ipc::send_blocking(fd_, request); }
        catch (...) { std::lock_guard lock(promises_mutex_); auto it = promises_.find(id); if (it != promises_.end()) { it->second.set_exception(std::current_exception()); promises_.erase(it); } }
        return future;
    }
    std::future<json> invoke(json value) { value["op"] = "invoke"; return request(std::move(value)); }
    std::future<json> prepare(json value) { value["op"] = "prepare"; return request(std::move(value)); }
    std::future<json> stats() { return request({{"op", "stats"}}); }
    bool disconnected() const { return disconnected_; }
    void cancel(const std::string& attempt_id, const std::string& reason) { std::lock_guard lock(send_mutex_); ipc::send_blocking(fd_, {{"op", "cancel"}, {"attempt_id", attempt_id}, {"reason", reason}}); }
    ~WorkerClient() {
        if (fd_ >= 0) { try { std::lock_guard lock(send_mutex_); ipc::send_blocking(fd_, {{"op", "shutdown"}}); } catch (...) {} shutdown(fd_, SHUT_RDWR); }
        if (reader_.joinable()) reader_.join();
        if (fd_ >= 0) close(fd_);
        if (pid_ > 0) { int status; while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {} }
    }
    WorkerClient(const WorkerClient&) = delete;
    WorkerClient& operator=(const WorkerClient&) = delete;
};
}
