#pragma once
#include "ipc.hpp"
#include <span>
#include <signal.h>
#include <string_view>
#include <sys/syscall.h>
#include <utility>

namespace ff::zygote {
class Fd {
    int fd_ = -1;
public:
    Fd() = default;
    explicit Fd(int fd) : fd_(fd) {}
    ~Fd() { if (fd_ >= 0) close(fd_); }
    Fd(Fd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    Fd& operator=(Fd&& other) noexcept { std::swap(fd_, other.fd_); return *this; }
    int get() const { return fd_; }
    explicit operator bool() const { return fd_ >= 0; }
};

inline std::array<Fd, 2> socket_pair(int type) {
    int fds[2];
    if (socketpair(AF_UNIX, type | SOCK_CLOEXEC, 0, fds) < 0) throw std::runtime_error("zygote socketpair failed");
    return {Fd(fds[0]), Fd(fds[1])};
}

inline std::array<Fd, 2> log_pipe() {
    int fds[2];
    if (pipe2(fds, O_CLOEXEC) < 0) throw std::runtime_error("zygote log pipe failed");
    std::array<Fd, 2> result{Fd(fds[0]), Fd(fds[1])};
    ipc::nonblocking(result[0].get());
    return result;
}

// Private launch messages fit in a single seqpacket. A one-byte stream
// handshake uses the same fd transfer to identify a child before it runs.
inline constexpr size_t max_command = 8192;
inline void send_fds(int fd, std::string_view bytes, std::span<const int> fds) {
    if (fds.empty() || fds.size() > 2 || bytes.size() > max_command) throw std::runtime_error("invalid zygote command size");
    alignas(cmsghdr) char control[CMSG_SPACE(2 * sizeof(int))]{};
    iovec payload{const_cast<char*>(bytes.data()), bytes.size()};
    msghdr msg{};
    msg.msg_iov = &payload;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = CMSG_SPACE(fds.size_bytes());
    auto* header = CMSG_FIRSTHDR(&msg);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(fds.size_bytes());
    std::memcpy(CMSG_DATA(header), fds.data(), fds.size_bytes());
    ssize_t sent;
    do { sent = sendmsg(fd, &msg, MSG_NOSIGNAL); } while (sent < 0 && errno == EINTR);
    if (sent != static_cast<ssize_t>(bytes.size())) throw std::runtime_error("zygote descriptor send failed");
}

template<size_t N>
ssize_t receive_fds(int fd, std::span<char> bytes, std::array<Fd, N>& fds) {
    alignas(cmsghdr) char control[CMSG_SPACE(2 * sizeof(int))]{};
    iovec payload{bytes.data(), bytes.size()};
    msghdr msg{};
    msg.msg_iov = &payload;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);
    ssize_t count;
    do { count = recvmsg(fd, &msg, MSG_CMSG_CLOEXEC); } while (count < 0 && errno == EINTR);
    if (count <= 0) return count;
    size_t received = 0;
    for (auto* header = CMSG_FIRSTHDR(&msg); header; header = CMSG_NXTHDR(&msg, header)) {
        if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS) continue;
        const size_t size = (header->cmsg_len - CMSG_LEN(0)) / sizeof(int);
        for (size_t i = 0; i < size; ++i) {
            int descriptor;
            std::memcpy(&descriptor, CMSG_DATA(header) + i * sizeof(int), sizeof(int));
            Fd owned(descriptor);
            if (received < N) fds[received] = std::move(owned);
            ++received;
        }
    }
    if (received != N || (msg.msg_flags & (MSG_TRUNC | MSG_CTRUNC))) throw std::runtime_error("invalid zygote descriptors");
    return count;
}

inline void await_launch(int channel) {
    Fd pidfd(static_cast<int>(syscall(SYS_pidfd_open, getpid(), 0)));
    if (!pidfd) throw std::runtime_error("pidfd_open failed");
    const int descriptor = pidfd.get();
    send_fds(channel, "P", std::span(&descriptor, 1));
    char grant;
    ssize_t count;
    do { count = recv(channel, &grant, 1, 0); } while (count < 0 && errno == EINTR);
    if (count != 1 || grant != 'G') throw std::runtime_error("zygote launch revoked");
}

inline bool allow_launch(int channel, Fd& pidfd) {
    std::array<Fd, 1> fds;
    char marker;
    auto count = receive_fds(channel, std::span(&marker, 1), fds);
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return false;
    if (count != 1 || marker != 'P') throw std::runtime_error("zygote launch disconnected");
    pidfd = std::move(fds[0]);
    ssize_t sent;
    do { sent = send(channel, "G", 1, MSG_NOSIGNAL); } while (sent < 0 && errno == EINTR);
    if (sent != 1) throw std::runtime_error("cannot authorize zygote launch");
    return true;
}

inline void kill_child(const Fd& pidfd) {
    if (pidfd) syscall(SYS_pidfd_send_signal, pidfd.get(), SIGKILL, nullptr, 0);
}
}
