#include "fluxforge/worker_runtime.hpp"
#include "fluxforge/zygote_ipc.hpp"
#include <dlfcn.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <iostream>

namespace {
using namespace ff::zygote;

void bind_parent(pid_t expected) {
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) < 0 || getppid() != expected) _exit(126);
}

void limit(int resource, rlim_t value) {
    rlimit limits{value, value};
    if (setrlimit(resource, &limits) < 0) throw std::runtime_error("cannot apply worker resource limit");
}

void redirect_logs(const Fd& logs) {
    if (dup2(logs.get(), STDOUT_FILENO) < 0 || dup2(logs.get(), STDERR_FILENO) < 0) _exit(126);
}

template<typename Child>
void fork_loop(int control, Child run_child) {
    // Linux auto-reaps our direct children. Host owns their pidfds and
    // observes their dedicated response channels rather than waitpid status.
    if (signal(SIGCHLD, SIG_IGN) == SIG_ERR) throw std::runtime_error("cannot configure child reaping");
    for (;;) {
        std::array<char, max_command> bytes;
        std::array<Fd, 2> endpoints;
        auto count = receive_fds(control, bytes, endpoints);
        if (count == 0) return;
        if (count < 0) throw std::runtime_error("zygote command receive failed");
        const auto parent = getpid();
        const auto pid = fork();
        if (pid < 0) {
            // Closing the passed descriptors makes this launch fail at Host.
            continue;
        }
        if (pid == 0) {
            bind_parent(parent);
            close(control);
            redirect_logs(endpoints[1]);
            endpoints[1] = Fd{};
            try {
                run_child(endpoints[0].get(), std::string_view(bytes.data(), count));
                _exit(0);
            } catch (const std::exception& error) {
                std::cerr << "worker: " << error.what() << '\n';
                _exit(1);
            }
        }
    }
}

void serve_function(int channel, std::string_view bytes) {
    await_launch(channel);
    const auto config = ff::json::parse(bytes);
    limit(RLIMIT_AS, config.at("memory_limit_bytes").get<uint64_t>());
    limit(RLIMIT_NOFILE, 64);
    limit(RLIMIT_CORE, 0);
    limit(RLIMIT_FSIZE, config.at("max_payload_bytes").get<uint64_t>());

    void* library = dlopen(config.at("library_path").get_ref<const std::string&>().c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!library) throw std::runtime_error(dlerror());
    auto version = reinterpret_cast<uint32_t (*)()>(dlsym(library, "ff_abi_version"));
    auto function = reinterpret_cast<ff::worker::Function>(dlsym(library, "ff_invoke_v1"));
    if (!version || !function || version() != FF_ABI_VERSION) throw std::runtime_error("incompatible function ABI");

    auto tasks = std::filesystem::directory_iterator("/proc/self/task");
    if (++tasks != std::filesystem::directory_iterator{}) throw std::runtime_error("function initialization must leave a single-threaded zygote");
    std::cout.flush();
    ff::ipc::send_blocking(channel, {{"op", "ready"}, {"zygote_pid", getpid()}});

    fork_loop(channel, [function](int worker, std::string_view command) {
        if (command != "fork") throw std::runtime_error("invalid worker launch");
        signal(SIGCHLD, SIG_DFL);
        await_launch(worker);
        ff::ipc::send_blocking(worker, {{"op", "ready"}, {"worker_pid", getpid()}});
        auto request = ff::ipc::receive_blocking(worker);
        if (request.at("op") != "invoke") throw std::runtime_error("invalid one-shot worker request");
        auto result = ff::worker::invoke(request, function);
        std::cout.flush();
        ff::ipc::send_blocking(worker, result);
    });
}
}

int main(int argc, char** argv) {
    try {
        if (argc != 3 || std::string_view(argv[2]) != "--base-zygote")
            throw std::runtime_error("usage: fluxforge-worker IPC_FD --base-zygote");
        sigset_t empty;
        sigemptyset(&empty);
        if (sigprocmask(SIG_SETMASK, &empty, nullptr) < 0) throw std::runtime_error("cannot reset signal mask");
        fork_loop(std::stoi(argv[1]), serve_function);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "base-zygote: " << error.what() << '\n';
        return 1;
    }
}
