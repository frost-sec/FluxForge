#pragma once
#include "common.hpp"
#include "worker.pb.h"
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
namespace ff::ipc {
constexpr uint32_t max_frame = 1024 * 1024;
inline void nonblocking(int fd) { if (fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) < 0) throw std::runtime_error("fcntl nonblocking failed"); }
inline std::string frame(json message) {
    message["protocol_version"] = 1;
    fluxforge::worker::v1::WorkerEnvelope envelope;
    envelope.set_protocol_version(1);
    envelope.set_correlation_id(message.value("id", ""));
    envelope.set_attempt_id(message.value("attempt_id", ""));
    envelope.set_worker_generation(message.value("generation", 0ULL));
    envelope.set_json_body(message.dump(-1, ' ', false, json::error_handler_t::replace));
    auto body = envelope.SerializeAsString();
    if (body.size() > max_frame) throw std::runtime_error("IPC frame too large");
    uint32_t length = htonl(static_cast<uint32_t>(body.size()));
    return std::string(reinterpret_cast<const char*>(&length), sizeof(length)) + body;
}
inline json decode_body(const std::string& bytes) {
    fluxforge::worker::v1::WorkerEnvelope envelope;
    if (!envelope.ParseFromString(bytes) || envelope.protocol_version() != 1)
        throw std::runtime_error("invalid IPC envelope or version");
    auto message = json::parse(envelope.json_body());
    if (message.value("protocol_version", 0) != 1 ||
        message.value("id", "") != envelope.correlation_id() ||
        message.value("attempt_id", "") != envelope.attempt_id() ||
        message.value("generation", 0ULL) != envelope.worker_generation())
        throw std::runtime_error("IPC envelope identity mismatch");
    return message;
}
class Decoder {
    std::string buffer_;
public:
    void append(const char* data, size_t size) { if (buffer_.size() + size > 2 * max_frame + 8) throw std::runtime_error("IPC receive buffer full"); buffer_.append(data, size); }
    bool next(json& message) {
        if (buffer_.size() < 4) return false;
        uint32_t length; std::memcpy(&length, buffer_.data(), 4); length = ntohl(length);
        if (length == 0 || length > max_frame) throw std::runtime_error("invalid IPC frame length");
        if (buffer_.size() < length + 4) return false;
        message = decode_body(buffer_.substr(4, length));
        buffer_.erase(0, length + 4);
        if (message.value("protocol_version", 0) != 1) throw std::runtime_error("unsupported IPC version");
        return true;
    }
};
inline void send_blocking(int fd, const json& message) {
    auto bytes = frame(message); size_t sent = 0;
    while (sent < bytes.size()) { auto count = send(fd, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL); if (count < 0 && errno == EINTR) continue; if (count <= 0) throw std::runtime_error("IPC send failed"); sent += count; }
}
inline json receive_blocking(int fd) {
    auto read_exact = [fd](char* buf, size_t size) { size_t received = 0; while (received < size) { auto count = recv(fd, buf + received, size - received, 0); if (count < 0 && errno == EINTR) continue; if (count <= 0) throw std::runtime_error("IPC peer closed"); received += count; } };
    uint32_t length; read_exact(reinterpret_cast<char*>(&length), 4); length = ntohl(length);
    if (!length || length > max_frame) throw std::runtime_error("invalid IPC frame length");
    std::string body(length, '\0'); read_exact(body.data(), length); auto message = decode_body(body);
    if (message.value("protocol_version", 0) != 1) throw std::runtime_error("unsupported IPC version");
    return message;
}
}
