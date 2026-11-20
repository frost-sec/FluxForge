#include "fluxforge/zygote_ipc.hpp"
#include <sys/epoll.h>
#include <sys/prctl.h>
#include <sys/signalfd.h>
#include <sys/timerfd.h>
#include <sys/wait.h>
#include <iostream>
#include <set>
#include <unordered_map>
#include <vector>

namespace {
using ff::json;
using ff::now_ms;
using namespace ff::zygote;

struct Peer {
    Fd socket, pidfd, logs;
    ff::ipc::Decoder decoder;
    std::string outgoing, log_tail;

    int fd() const { return socket.get(); }
};
struct Seed {
    Peer peer;
    std::string key, affinity_key;
    pid_t pid = 0;
    int64_t last_used = now_ms();
};
enum class Phase { WaitingForSeed, Forking, Prepared, Running };
struct Attempt {
    Peer peer;
    std::array<Fd, 2> launch_fds;
    int seed_fd = -1;
    bool warm = false;
    pid_t pid = 0;
    uint64_t generation = 0;
    Phase phase = Phase::WaitingForSeed;
    int64_t deadline = 0;
    int64_t started = 0;
    json request;
};

class Host {
    Peer agent_;
    Fd epoll_, timer_, signals_, base_control_;
    pid_t base_pid_ = -1;
    size_t capacity_;
    bool running_ = true;
    uint64_t next_generation_ = 1;
    std::unordered_map<int, Seed> seeds_;
    std::unordered_map<int, Attempt> attempts_;
    std::unordered_map<int, int> log_owners_;
    // epoll_wait may have already returned events for a retired descriptor.
    // Delay close until this batch ends so the descriptor cannot be reused.
    std::vector<Fd> retired_;

    void add(int fd, uint32_t flags) {
        epoll_event event{};
        event.events = flags;
        event.data.fd = fd;
        if (epoll_ctl(epoll_.get(), EPOLL_CTL_ADD, fd, &event) < 0)
            throw std::runtime_error("epoll add failed");
    }

    void register_peer(Peer& peer) {
        ff::ipc::nonblocking(peer.fd());
        add(peer.fd(), EPOLLIN | EPOLLRDHUP);
        if (peer.logs) {
            add(peer.logs.get(), EPOLLIN);
            log_owners_.emplace(peer.logs.get(), peer.fd());
        }
    }

    void retire(Fd& fd) {
        if (!fd) return;
        epoll_ctl(epoll_.get(), EPOLL_CTL_DEL, fd.get(), nullptr);
        retired_.push_back(std::move(fd));
    }

    void retire_peer(Peer& peer) {
        log_owners_.erase(peer.logs.get());
        retire(peer.logs);
        // Shutdown also revokes a child which has not received its launch grant.
        shutdown(peer.fd(), SHUT_RDWR);
        retire(peer.socket);
    }

    void flush(Peer& peer) {
        while (!peer.outgoing.empty()) {
            auto n = send(peer.fd(), peer.outgoing.data(), peer.outgoing.size(), MSG_NOSIGNAL);
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            if (n <= 0) {
                if (&peer == &agent_) { running_ = false; return; }
                throw std::runtime_error("worker IPC send failed");
            }
            peer.outgoing.erase(0, n);
        }
        epoll_event event{};
        event.events = EPOLLIN | EPOLLRDHUP;
        if (!peer.outgoing.empty()) event.events |= EPOLLOUT;
        event.data.fd = peer.fd();
        epoll_ctl(epoll_.get(), EPOLL_CTL_MOD, peer.fd(), &event);
    }

    void reply(Peer& peer, const json& msg) {
        auto frame = ff::ipc::frame(msg);
        if (peer.outgoing.size() + frame.size() > 2 * ff::ipc::max_frame)
            throw std::runtime_error("IPC send backlog exceeded");
        peer.outgoing += frame;
        flush(peer);
    }

    void read_log(Peer& peer) {
        if (!peer.logs) return;
        char buf[4096];
        size_t drained = 0;
        while (drained < 65536) {
            auto n = read(peer.logs.get(), buf, sizeof(buf));
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                if (n == 0) {
                    log_owners_.erase(peer.logs.get());
                    retire(peer.logs);
                }
                break;
            }
            drained += n;
            peer.log_tail.append(buf, n);
            if (peer.log_tail.size() > 8192) peer.log_tail.erase(0, peer.log_tail.size() - 8192);
        }
    }

    void finish_attempt(int fd, json result, bool kill) {
        auto& attempt = attempts_.at(fd);
        if (kill) kill_child(attempt.peer.pidfd);
        read_log(attempt.peer);
        auto seed = seeds_.find(attempt.seed_fd);
        if (seed != seeds_.end()) seed->second.last_used = now_ms();
        if (attempt.phase != Phase::Prepared) {
            result["id"] = attempt.request.at("id");
            result["attempt_id"] = attempt.request.at("attempt_id");
            result["worker_pid"] = attempt.pid;
            result["zygote_pid"] = seed == seeds_.end() ? 0 : seed->second.pid;
            result["warm"] = attempt.warm;
            result["generation"] = attempt.generation;
            if (!attempt.peer.log_tail.empty()) result["log_tail"] = attempt.peer.log_tail;
            reply(agent_, result);
        }
        retire_peer(attempt.peer);
        attempts_.erase(fd);
    }

    void fail_attempt(int fd, const std::string& status, const std::string& error) {
        auto start = attempts_.at(fd).started;
        finish_attempt(fd, {{"op", "result"}, {"status", status}, {"error", error},
                            {"duration_ms", start ? now_ms() - start : 0}}, true);
    }

    std::vector<int> attached(int seed_fd) const {
        std::vector<int> fds;
        for (const auto& [fd, attempt] : attempts_) {
            if (attempt.seed_fd == seed_fd) fds.push_back(fd);
        }
        return fds;
    }

    void remove_seed(int fd, const std::string& reason) {
        auto& seed = seeds_.at(fd);
        kill_child(seed.peer.pidfd);
        read_log(seed.peer);
        const std::string error = seed.peer.log_tail.empty() ? reason : reason + ": " + seed.peer.log_tail;
        for (int attempt : attached(fd)) fail_attempt(attempt, "CRASHED", error);
        retire_peer(seed.peer);
        seeds_.erase(fd);
    }

    int find_or_create_seed(const json& request) {
        json config{{"library_path", request.at("library_path")},
                    {"memory_limit_bytes", request.value("memory_limit_bytes", 256ULL * 1024 * 1024)},
                    {"max_payload_bytes", request.value("max_payload_bytes", 64ULL * 1024 * 1024)}};
        auto key = json::array({request.at("affinity_key"), config}).dump();
        int victim = -1;
        for (const auto& [fd, seed] : seeds_) {
            if (seed.key == key) return fd;
            if (attached(fd).empty() && (victim < 0 || seed.last_used < seeds_.at(victim).last_used)) victim = fd;
        }
        if (seeds_.size() >= capacity_) {
            if (victim < 0) throw std::runtime_error("function zygote capacity unavailable");
            remove_seed(victim, "zygote eviction");
        }

        auto sockets = socket_pair(SOCK_SEQPACKET);
        auto logs = log_pipe();
        const int fd = sockets[0].get();
        const std::array descriptors{sockets[1].get(), logs[1].get()};
        send_fds(base_control_.get(), config.dump(), descriptors);
        Seed seed;
        seed.key = std::move(key);
        seed.affinity_key = request.at("affinity_key");
        seed.peer.socket = std::move(sockets[0]);
        seed.peer.logs = std::move(logs[0]);
        register_peer(seed.peer);
        seeds_.emplace(fd, std::move(seed));
        return fd;
    }

    void fork_worker(Attempt& attempt) {
        const std::array descriptors{attempt.launch_fds[0].get(), attempt.launch_fds[1].get()};
        send_fds(attempt.seed_fd, "fork", descriptors);
        attempt.launch_fds = {};
        attempt.phase = Phase::Forking;
    }

    void start_execution(Attempt& attempt, json request) {
        attempt.phase = Phase::Running;
        attempt.started = now_ms();
        attempt.deadline = attempt.started + request.value("timeout_ms", 30000LL);
        request["generation"] = attempt.generation;
        attempt.request = std::move(request);
        reply(attempt.peer, attempt.request);
    }

    void request_worker(const json& request) {
        const auto accepted_at = now_ms();
        const auto& attempt_id = request.at("attempt_id");
        const bool prepare = request.at("op") == "prepare";
        for (auto& [fd, attempt] : attempts_) {
            if (attempt.request.at("attempt_id") != attempt_id) continue;
            if (!prepare && attempt.phase == Phase::Prepared) {
                if (accepted_at >= attempt.deadline) {
                    attempt.request = request;
                    attempt.phase = Phase::Forking;
                    fail_attempt(fd, "TIMEOUT", "prepared worker expired");
                    return;
                }
                try { start_execution(attempt, request); }
                catch (const std::exception& error) { fail_attempt(fd, "CRASHED", error.what()); }
                return;
            }
            reply(agent_, {{"op", "result"}, {"id", request.at("id")}, {"status", "RESOURCE_EXHAUSTED"},
                           {"error", "attempt already owns a worker"}});
            return;
        }

        if (!prepare && request.value("require_prepared", false)) {
            reply(agent_, {{"op", "result"}, {"id", request.at("id")}, {"status", "CRASHED"},
                           {"error", "prepared worker no longer exists"}});
            return;
        }
        if (attempts_.size() >= capacity_) {
            reply(agent_, {{"op", "result"}, {"id", request.at("id")}, {"status", "RESOURCE_EXHAUSTED"},
                           {"error", "worker capacity unavailable"}});
            return;
        }

        int fd = -1;
        try {
            int seed_fd = find_or_create_seed(request);
            auto sockets = socket_pair(SOCK_STREAM);
            auto logs = log_pipe();
            Attempt attempt;
            attempt.seed_fd = seed_fd;
            attempt.warm = seeds_.at(seed_fd).pid > 0;
            attempt.generation = next_generation_++;
            attempt.deadline = accepted_at + request.value("prepare_timeout_ms", 10000LL);
            attempt.request = request;
            attempt.peer.socket = std::move(sockets[0]);
            attempt.peer.logs = std::move(logs[0]);
            attempt.launch_fds = {std::move(sockets[1]), std::move(logs[1])};
            const int candidate = attempt.peer.fd();
            register_peer(attempt.peer);
            attempts_.emplace(candidate, std::move(attempt));
            fd = candidate;
            if (attempts_.at(fd).warm) fork_worker(attempts_.at(fd));
        } catch (const std::exception& error) {
            if (fd >= 0) fail_attempt(fd, "CRASHED", error.what());
            else reply(agent_, {{"op", "result"}, {"id", request.at("id")}, {"status", "RESOURCE_EXHAUSTED"}, {"error", error.what()}});
        }
    }

    void seed_message(int fd, const json& msg) {
        auto& seed = seeds_.at(fd);
        if (msg.at("op") != "ready" || seed.pid || msg.at("zygote_pid").get<pid_t>() <= 0)
            throw std::runtime_error("invalid function zygote response");
        seed.pid = msg.at("zygote_pid");
        for (int worker_fd : attached(fd)) {
            if (now_ms() >= attempts_.at(worker_fd).deadline) {
                fail_attempt(worker_fd, "TIMEOUT", "worker preparation timed out");
                continue;
            }
            try { fork_worker(attempts_.at(worker_fd)); }
            catch (const std::exception& error) { fail_attempt(worker_fd, "CRASHED", error.what()); }
        }
    }

    void worker_message(int fd, const json& msg) {
        auto& attempt = attempts_.at(fd);
        if (msg.at("op") == "ready" && attempt.phase == Phase::Forking) {
            attempt.pid = msg.at("worker_pid");
            if (now_ms() >= attempt.deadline) {
                fail_attempt(fd, "TIMEOUT", "worker preparation timed out");
                return;
            }
            if (attempt.request.at("op") != "prepare") {
                start_execution(attempt, attempt.request);
                return;
            }
            attempt.phase = Phase::Prepared;
            attempt.deadline = now_ms() + 10000;
            reply(agent_, {{"op", "prepared"}, {"id", attempt.request.at("id")}, {"status", "PREPARED"},
                           {"worker_pid", attempt.pid}, {"zygote_pid", seeds_.at(attempt.seed_fd).pid},
                           {"warm", attempt.warm}});
            return;
        }
        if (msg.at("op") != "result" || attempt.phase != Phase::Running ||
            msg.at("id") != attempt.request.at("id") || msg.at("attempt_id") != attempt.request.at("attempt_id") ||
            msg.value("generation", 0ULL) != attempt.generation)
            throw std::runtime_error("invalid one-shot worker response");
        if (now_ms() >= attempt.deadline) {
            fail_attempt(fd, "TIMEOUT", "function execution timed out");
            return;
        }
        finish_attempt(fd, msg, false);
    }

    void stats(const json& msg) {
        std::set<std::string> keys;
        uint64_t rss = 0;
        auto add_rss = [&rss](pid_t pid) {
            if (pid <= 0) return;
            uint64_t pages, resident;
            std::ifstream stat("/proc/" + std::to_string(pid) + "/statm");
            if (stat >> pages >> resident) rss += resident * sysconf(_SC_PAGESIZE);
        };
        for (const auto& [fd, seed] : seeds_) {
            if (seed.pid > 0) keys.insert(seed.affinity_key);
            add_rss(seed.pid);
        }
        for (const auto& [fd, attempt] : attempts_) add_rss(attempt.pid);
        reply(agent_, {{"op", "stats"}, {"id", msg.at("id")}, {"affinity_keys", keys},
                       {"worker_rss_bytes", rss}, {"workers", attempts_.size()}, {"zygotes", seeds_.size()},
                       {"base_pid", base_pid_}});
    }

    void agent_message(const json& msg) {
        auto op = msg.at("op").get<std::string>();
        if (op == "prepare" || op == "invoke") request_worker(msg);
        else if (op == "stats") stats(msg);
        else if (op == "shutdown") running_ = false;
        else if (op == "cancel") {
            for (const auto& [fd, attempt] : attempts_) {
                if (attempt.request.at("attempt_id") != msg.at("attempt_id")) continue;
                fail_attempt(fd, "CANCELLED", msg.value("reason", "cancelled"));
                break;
            }
            evict_idle_seeds();
        } else throw std::runtime_error("unknown host operation");
    }

    Peer* peer(int fd) {
        if (fd == agent_.fd()) return &agent_;
        if (auto it = attempts_.find(fd); it != attempts_.end()) return &it->second.peer;
        if (auto it = seeds_.find(fd); it != seeds_.end()) return &it->second.peer;
        return nullptr;
    }

    void read_peer(int fd) {
        auto* connection = peer(fd);
        if (fd != agent_.fd() && !connection->pidfd && !allow_launch(fd, connection->pidfd)) return;
        char buf[16384];
        size_t received = 0;
        while (received < 65536 && running_) {
            auto n = recv(fd, buf, sizeof(buf), 0);
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
            if (n <= 0) {
                if (fd == agent_.fd()) { running_ = false; return; }
                throw std::runtime_error("zygote or worker disconnected");
            }
            received += n;
            connection->decoder.append(buf, n);
            json msg;
            while (connection->decoder.next(msg)) {
                if (fd == agent_.fd()) agent_message(msg);
                else if (seeds_.contains(fd)) seed_message(fd, msg);
                else worker_message(fd, msg);
                if (!peer(fd) || !running_) return;
            }
        }
    }

    void reap() {
        pid_t pid;
        while ((pid = waitpid(-1, nullptr, WNOHANG)) > 0) {
            if (pid == base_pid_) {
                base_pid_ = -1;
                running_ = false;
            }
        }
    }

    void evict_idle_seeds() {
        const auto now = now_ms();
        std::vector<int> evict;
        for (const auto& [fd, seed] : seeds_) {
            if ((!seed.pid || now - seed.last_used >= 60000) && attached(fd).empty())
                evict.push_back(fd);
        }
        for (int fd : evict) remove_seed(fd, "idle zygote eviction");
    }

    void tick() {
        uint64_t ticks;
        if (read(timer_.get(), &ticks, sizeof(ticks)) != sizeof(ticks)) return;
        const auto now = now_ms();
        std::vector<int> expired;
        for (const auto& [fd, attempt] : attempts_) {
            if (now >= attempt.deadline) expired.push_back(fd);
        }
        for (int fd : expired) {
            const auto phase = attempts_.at(fd).phase;
            const auto* error = phase == Phase::Running ? "function execution timed out" : "worker preparation timed out";
            fail_attempt(fd, "TIMEOUT", error);
        }
        evict_idle_seeds();
        reap();
    }

    void start_base(const std::string& executable) {
        auto sockets = socket_pair(SOCK_SEQPACKET);
        auto parent = getpid();
        base_pid_ = fork();
        if (base_pid_ < 0) throw std::runtime_error("base zygote fork failed");
        if (base_pid_ == 0) {
            if (prctl(PR_SET_PDEATHSIG, SIGKILL) < 0 || getppid() != parent ||
                prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0 ||
                dup2(sockets[1].get(), 3) < 0 || fcntl(3, F_SETFD, 0) < 0) _exit(126);
            // Close every inherited descriptor, including the Agent channel.
            if (syscall(SYS_close_range, 4u, ~0u, 0u) < 0) _exit(126);
            char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>("3"),
                            const_cast<char*>("--base-zygote"), nullptr};
            char* env[] = {const_cast<char*>("PATH=/usr/bin:/bin"), const_cast<char*>("LANG=C.UTF-8"), nullptr};
            execve(executable.c_str(), args, env);
            _exit(127);
        }
        base_control_ = std::move(sockets[0]);
    }

public:
    Host(int agent, int capacity) : capacity_(capacity) {
        agent_.socket = Fd(agent);
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGCHLD);
        sigaddset(&mask, SIGTERM);
        sigaddset(&mask, SIGINT);
        if (capacity <= 0 || sigprocmask(SIG_BLOCK, &mask, nullptr) < 0 ||
            prctl(PR_SET_CHILD_SUBREAPER, 1) < 0) throw std::runtime_error("cannot initialize worker host");
        epoll_ = Fd(epoll_create1(EPOLL_CLOEXEC));
        signals_ = Fd(signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC));
        timer_ = Fd(timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC));
        if (!epoll_ || !signals_ || !timer_) throw std::runtime_error("cannot initialize Linux reactor");
        itimerspec timer{};
        timer.it_value.tv_nsec = 50000000;
        timer.it_interval = timer.it_value;
        if (timerfd_settime(timer_.get(), 0, &timer, nullptr) < 0) throw std::runtime_error("cannot arm host timer");
        register_peer(agent_);
        add(signals_.get(), EPOLLIN);
        add(timer_.get(), EPOLLIN);
    }

    ~Host() {
        for (const auto& [fd, attempt] : attempts_) kill_child(attempt.peer.pidfd);
        for (const auto& [fd, seed] : seeds_) kill_child(seed.peer.pidfd);
        if (base_pid_ > 0) kill(base_pid_, SIGKILL);
        while (waitpid(-1, nullptr, 0) > 0 || errno == EINTR) {}
    }

    void run(const std::string& executable) {
        start_base(executable);
        // Descriptor transfer must never block the supervisor's timers.
        ff::ipc::nonblocking(base_control_.get());
        epoll_event events[32];
        while (running_) {
            auto count = epoll_wait(epoll_.get(), events, 32, -1);
            if (count < 0 && errno == EINTR) continue;
            if (count < 0) throw std::runtime_error("epoll wait failed");
            for (int i = 0; i < count && running_; ++i) {
                const int fd = events[i].data.fd;
                if (fd == signals_.get()) {
                    signalfd_siginfo info;
                    while (read(fd, &info, sizeof(info)) == sizeof(info)) {
                        if (info.ssi_signo == SIGCHLD) reap();
                        else running_ = false;
                    }
                } else if (fd == timer_.get()) tick();
                else if (auto it = log_owners_.find(fd); it != log_owners_.end()) read_log(*peer(it->second));
                else if (auto* connection = peer(fd)) {
                    try {
                        if (events[i].events & EPOLLOUT) flush(*connection);
                        if (events[i].events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) read_peer(fd);
                    } catch (const std::exception& error) {
                        if (fd == agent_.fd()) throw;
                        if (seeds_.contains(fd)) remove_seed(fd, error.what());
                        else if (attempts_.contains(fd)) fail_attempt(fd, "CRASHED", error.what());
                    }
                }
            }
            retired_.clear();
        }
    }
};
}
int main(int argc, char** argv) {
    try {
        if (argc != 4) throw std::runtime_error("usage: fluxforge-worker-host IPC_FD CAPACITY WORKER_EXECUTABLE");
        Host host(std::stoi(argv[1]), std::stoi(argv[2]));
        host.run(argv[3]);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "worker-host: " << error.what() << '\n';
        return 1;
    }
}
