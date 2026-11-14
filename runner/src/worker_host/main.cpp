#include "fluxforge/ipc.hpp"
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <sys/signalfd.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <signal.h>
#include <unordered_map>
#include <vector>
#include <deque>
#include <iostream>

namespace {
struct Peer { ff::ipc::Decoder decoder; std::string pending; };
struct Worker {
    pid_t pid{}; int fd{-1}, log_fd{-1}; std::string key, log;
    bool ready = false, used = false; int calls = 0;
    uint64_t generation = 0;
    uint64_t memory_limit = 256ULL * 1024 * 1024;
    int64_t started_at = 0;
    std::string prepared_attempt;
    int64_t born = ff::now_ms(), last_used = born, deadline = born + 10000;
    ff::json request;
};
class Host {
    int agent_, epoll_, timer_, signals_, capacity_;
    std::string executable_;
    std::unordered_map<int, Peer> peers_;
    std::unordered_map<int, Worker> workers_;
    std::unordered_map<int, int> log_workers_;
    std::deque<ff::json> waiting_;
    bool running_ = true;
    uint64_t next_generation_ = 1;
    void watch(int fd, uint32_t events = EPOLLIN | EPOLLRDHUP) { epoll_event event{}; event.events = events; event.data.fd = fd; if (epoll_ctl(epoll_, EPOLL_CTL_ADD, fd, &event) < 0) throw std::runtime_error("epoll add failed"); }
    void flush(int fd) {
        auto it = peers_.find(fd); if (it == peers_.end()) return;
        auto& bytes = it->second.pending;
        while (!bytes.empty()) {
            auto n = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            if (n <= 0) { if (fd == agent_) running_ = false; return; }
            bytes.erase(0, n);
        }
        epoll_event event{}; event.data.fd = fd; event.events = EPOLLIN | EPOLLRDHUP | (bytes.empty() ? 0 : EPOLLOUT); epoll_ctl(epoll_, EPOLL_CTL_MOD, fd, &event);
    }
    void send(int fd, ff::json value) {
        auto& bytes = peers_.at(fd).pending;
        auto frame = ff::ipc::frame(std::move(value));
        if (bytes.size() + frame.size() > 2 * ff::ipc::max_frame) throw std::runtime_error("IPC send backlog exceeded");
        bytes += frame; flush(fd);
    }
    void finish(Worker& w, ff::json result) {
        if (w.request.is_null()) return;
        result["id"] = w.request.at("id"); result["attempt_id"] = w.request.at("attempt_id");
        result["warm"] = w.used; result["worker_pid"] = w.pid; result["generation"] = w.generation;
        if (!w.log.empty()) result["log_tail"] = w.log.substr(0, 8192);
        send(agent_, std::move(result));
        w.request = nullptr; w.prepared_attempt.clear(); w.used = true; ++w.calls; w.last_used = ff::now_ms(); w.deadline = 0; w.started_at = 0;
    }
    void terminate(int fd, const std::string& status, const std::string& error) {
        auto it = workers_.find(fd); if (it == workers_.end()) return;
        auto& w = it->second;
        const auto duration = w.started_at ? ff::now_ms() - w.started_at : 0;
        finish(w, {{"op", "result"}, {"status", status}, {"error", error}, {"duration_ms", duration}});
        kill(-w.pid, SIGKILL); w.ready = false; w.deadline = 0;
    }
    static void limit(int resource, rlim_t value) { rlimit lim{value, value}; if (setrlimit(resource, &lim) < 0) _exit(126); }
    int spawn(const ff::json& request) {
        int sockets[2], logs[2];
        if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) || pipe2(logs, O_CLOEXEC)) throw std::runtime_error("worker pipe creation failed");
        const auto library = request.at("library_path").get<std::string>();
        const auto memory = request.value("memory_limit_bytes", 256ULL * 1024 * 1024);
        auto pid = fork();
        if (pid == 0) {
            setpgid(0, 0); prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
            if (dup2(logs[1], STDOUT_FILENO) < 0 || dup2(logs[1], STDERR_FILENO) < 0 || dup2(sockets[1], 3) < 0) _exit(126);
            fcntl(3, F_SETFD, 0);
            limit(RLIMIT_AS, memory); limit(RLIMIT_NOFILE, 64); limit(RLIMIT_CORE, 0); limit(RLIMIT_FSIZE, request.value("max_payload_bytes", 64ULL * 1024 * 1024));
            // This process is single-threaded and never initializes gRPC or loads user libraries.
#ifdef SYS_close_range
            syscall(SYS_close_range, 4u, ~0u, 0u);
#else
            for (int fd = 4; fd < 4096; ++fd) close(fd);
#endif
            char* args[] = {const_cast<char*>(executable_.c_str()), const_cast<char*>("3"), const_cast<char*>(library.c_str()), nullptr};
            char* environment[] = {const_cast<char*>("PATH=/usr/bin:/bin"), const_cast<char*>("LANG=C.UTF-8"), nullptr};
            execve(executable_.c_str(), args, environment); _exit(127);
        }
        close(sockets[1]); close(logs[1]);
        if (pid < 0) { close(sockets[0]); close(logs[0]); throw std::runtime_error("fork failed"); }
        setpgid(pid, pid); ff::ipc::nonblocking(sockets[0]); ff::ipc::nonblocking(logs[0]);
        Worker w; w.pid = pid; w.fd = sockets[0]; w.log_fd = logs[0]; w.key = request.at("affinity_key"); w.request = request; w.generation = next_generation_++; w.memory_limit=memory;
        w.deadline = ff::now_ms() + request.value("prepare_timeout_ms", 10000LL);
        peers_.emplace(w.fd, Peer{}); log_workers_[w.log_fd] = w.fd;
        watch(w.fd); watch(w.log_fd, EPOLLIN | EPOLLHUP); workers_.emplace(w.fd, std::move(w));
        return sockets[0];
    }
    void invoke(ff::json request) {
        int selected = -1;
        for (const auto& [fd, w] : workers_) if (w.ready && w.request.is_null() && w.key == request.at("affinity_key") &&
            (w.prepared_attempt.empty() || w.prepared_attempt == request.at("attempt_id"))) { selected = fd; break; }
        if (selected < 0 && workers_.size() >= static_cast<size_t>(capacity_)) {
            bool evicting = false;
            for (const auto& [fd, w] : workers_) if (w.ready && w.request.is_null() && w.prepared_attempt.empty()) { terminate(fd, "CANCELLED", "cache eviction"); evicting = true; break; }
            if (evicting) { waiting_.push_back(std::move(request)); return; }
            reap();
        }
        if (selected < 0) {
            if (workers_.size() >= static_cast<size_t>(capacity_)) { send(agent_, {{"op", "result"}, {"id", request.at("id")}, {"attempt_id", request.at("attempt_id")}, {"status", "RESOURCE_EXHAUSTED"}, {"error", "worker capacity unavailable; retry preparation"}}); return; }
            spawn(request); return;
        }
        auto& w = workers_.at(selected); w.request = std::move(request); w.log.clear();
        if (w.request.value("op", "") == "prepare") {
            w.prepared_attempt = w.request.at("attempt_id");
            send(agent_, {{"id", w.request.at("id")}, {"op", "prepared"}, {"status", "PREPARED"}, {"worker_pid", w.pid}, {"warm", w.used}});
            w.request = nullptr; w.deadline = ff::now_ms() + 10000; return;
        }
        w.started_at = ff::now_ms(); w.prepared_attempt.clear(); w.request["generation"] = w.generation;
        w.deadline = w.started_at + w.request.value("timeout_ms", 30000LL);
        send(selected, w.request);
    }
    void message(int fd, const ff::json& msg) {
        if (fd == agent_) {
            auto op = msg.at("op").get<std::string>();
            if (op == "invoke" || op == "prepare") invoke(msg);
            else if (op == "cancel") {
                for (auto it = waiting_.begin(); it != waiting_.end();) { if (it->at("attempt_id") == msg.at("attempt_id")) { send(agent_, {{"id", it->at("id")}, {"op", "result"}, {"status", "CANCELLED"}, {"error", "preparation cancelled"}}); it = waiting_.erase(it); } else ++it; }
                for (auto& [worker_fd, w] : workers_) if ((!w.request.is_null() && w.request.at("attempt_id") == msg.at("attempt_id")) || w.prepared_attempt == msg.at("attempt_id")) { terminate(worker_fd, "CANCELLED", msg.value("reason", "cancelled")); w.prepared_attempt.clear(); }
            } else if (op == "shutdown") running_ = false;
            else if (op == "stats") {
                ff::json keys = ff::json::array(); uint64_t rss = 0;
                for (const auto& [worker_fd, w] : workers_) {
                    if (w.ready) keys.push_back(w.key);
                    std::ifstream stat("/proc/" + std::to_string(w.pid) + "/statm"); uint64_t pages, resident;
                    if (stat >> pages >> resident) rss += resident * static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
                }
                send(agent_, {{"id", msg.at("id")}, {"op", "stats"}, {"affinity_keys", keys}, {"worker_rss_bytes", rss}, {"workers", workers_.size()}});
            }
            else throw std::runtime_error("unknown host operation");
            return;
        }
        auto it = workers_.find(fd); if (it == workers_.end()) return;
        auto& w = it->second; auto op = msg.at("op").get<std::string>();
        if (op == "ready") {
            w.ready = true;
            if (!w.request.is_null()) {
                if (w.request.value("op", "") == "prepare") {
                    w.prepared_attempt = w.request.at("attempt_id");
                    send(agent_, {{"id", w.request.at("id")}, {"op", "prepared"}, {"status", "PREPARED"}, {"worker_pid", w.pid}, {"warm", w.used}});
                    w.request = nullptr; w.deadline = ff::now_ms() + 10000;
                } else { w.started_at = ff::now_ms(); w.request["generation"] = w.generation; w.deadline = w.started_at + w.request.value("timeout_ms", 30000LL); send(fd, w.request); }
            }
        }
        else if (op == "result" && !w.request.is_null() && msg.at("attempt_id") == w.request.at("attempt_id") && msg.value("generation", 0ULL) == w.generation) {
            auto status = msg.value("status", "CRASHED"); finish(w, msg);
            if (status != "SUCCESS" || w.calls >= 1000 || ff::now_ms() - w.born >= 600000) terminate(fd, "CANCELLED", "worker recycling");
        } else throw std::runtime_error("invalid worker response");
    }
    void read_socket(int fd) {
        char buf[16384];
        for (;;) {
            auto n = recv(fd, buf, sizeof(buf), 0);
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            if (n <= 0) { if (fd == agent_) running_ = false; else {auto it=workers_.find(fd);if(it!=workers_.end())read_log(it->second.log_fd);terminate(fd, "CRASHED", "worker disconnected");} break; }
            auto& decoder = peers_.at(fd).decoder; decoder.append(buf, n);
            ff::json msg; while (decoder.next(msg)) message(fd, msg);
        }
    }
    void read_log(int fd) {
        auto it = log_workers_.find(fd); if (it == log_workers_.end()) return;
        auto worker = workers_.find(it->second); if (worker == workers_.end()) return;
        char buf[4096];
        size_t drained = 0;
        while (drained < 65536) { auto n = read(fd, buf, sizeof(buf)); if (n < 0 && errno == EINTR) continue; if (n <= 0) break; drained += n; auto& log = worker->second.log; if (log.size() < 8192) log.append(buf, std::min<size_t>(n, 8192 - log.size())); }
    }
    void reap() {
        int status; pid_t pid;
        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            for (auto it = workers_.begin(); it != workers_.end(); ++it) if (it->second.pid == pid) {
                const int fd = it->first;
                try { read_socket(fd); read_log(it->second.log_fd); } catch (...) {}
                finish(it->second, {{"op", "result"}, {"status", "CRASHED"}, {"error", "worker exited (status " + std::to_string(status) + ")"}});
                close(it->second.log_fd); log_workers_.erase(it->second.log_fd); close(fd); peers_.erase(fd); workers_.erase(it); break;
            }
        }
        while (!waiting_.empty() && workers_.size() < static_cast<size_t>(capacity_)) { auto request = std::move(waiting_.front()); waiting_.pop_front(); spawn(request); }
    }
public:
    Host(int agent, int capacity, std::string executable) : agent_(agent), capacity_(capacity), executable_(std::move(executable)) {
        sigset_t mask; sigemptyset(&mask); sigaddset(&mask, SIGCHLD); sigaddset(&mask, SIGTERM); sigaddset(&mask, SIGINT); pthread_sigmask(SIG_BLOCK, &mask, nullptr);
        prctl(PR_SET_CHILD_SUBREAPER, 1);
        epoll_ = epoll_create1(EPOLL_CLOEXEC); signals_ = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC); timer_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (epoll_ < 0 || signals_ < 0 || timer_ < 0) throw std::runtime_error("cannot initialize Linux reactor");
        itimerspec timer{}; timer.it_value.tv_nsec = 50000000; timer.it_interval = timer.it_value; timerfd_settime(timer_, 0, &timer, nullptr);
        ff::ipc::nonblocking(agent_); peers_.emplace(agent_, Peer{}); watch(agent_); watch(signals_); watch(timer_);
    }
    ~Host() {
        for (auto& [fd, w] : workers_) kill(-w.pid, SIGKILL);
        while (waitpid(-1, nullptr, 0) > 0 || errno == EINTR) {}
        for (auto& [fd, w] : workers_) { close(fd); close(w.log_fd); }
        close(agent_); close(timer_); close(signals_); close(epoll_);
    }
    void run() {
        epoll_event events[32];
        while (running_) {
            auto count = epoll_wait(epoll_, events, 32, -1); if (count < 0 && errno == EINTR) continue; if (count < 0) throw std::runtime_error("epoll wait failed");
            for (int i = 0; i < count; ++i) {
                const int fd = events[i].data.fd;
                if (fd == signals_) { signalfd_siginfo info; while (read(fd, &info, sizeof(info)) == sizeof(info)) { if (info.ssi_signo == SIGCHLD) reap(); else running_ = false; } }
                else if (fd == timer_) {
                    uint64_t ticks=0; const auto received=read(fd,&ticks,sizeof(ticks)); if(received!=sizeof(ticks)) continue;
                    const auto now=ff::now_ms();
                    for(auto& [worker_fd,w]:workers_){
                        if(w.deadline && now>=w.deadline) terminate(worker_fd,"TIMEOUT",w.ready?"function execution timed out":"worker preparation timed out");
                        else if(w.ready && w.request.is_null() && w.prepared_attempt.empty()){
                            uint64_t pages=0,resident=0;std::ifstream stat("/proc/"+std::to_string(w.pid)+"/statm");stat>>pages>>resident;
                            const auto rss=resident*static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
                            if(rss>w.memory_limit*3/4)terminate(worker_fd,"CANCELLED","idle RSS recycling");
                            else if(now-w.last_used>60000)terminate(worker_fd,"CANCELLED","idle eviction");
                        }
                    }
                    reap();
                }
                else if (log_workers_.count(fd)) read_log(fd);
                else if (peers_.count(fd)) {
                    try { if (events[i].events & EPOLLOUT) flush(fd); if (events[i].events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) read_socket(fd); }
                    catch (const std::exception& e) { if (fd == agent_) throw; terminate(fd, "CRASHED", e.what()); }
                }
            }
        }
    }
};
}
int main(int argc, char** argv) {
    try { if (argc != 4) throw std::runtime_error("usage: fluxforge-worker-host IPC_FD CAPACITY WORKER_EXECUTABLE"); Host host(std::stoi(argv[1]), std::stoi(argv[2]), argv[3]); host.run(); return 0; }
    catch (const std::exception& e) { std::cerr << "worker-host: " << e.what() << '\n'; return 1; }
}
