#pragma once
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <nlohmann/json.hpp>
namespace ff {
using json = nlohmann::json;
using Clock = std::chrono::steady_clock;
inline int64_t now_ms() { return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count(); }
inline std::string env(const char* key, const std::string& fallback = {}) { const char* value = std::getenv(key); return value ? value : fallback; }
inline int64_t env_int(const char* key, int64_t fallback) { auto value = env(key); return value.empty() ? fallback : std::stoll(value); }
inline std::string read_file(const std::filesystem::path& path, uint64_t max_size) {
    std::ifstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("cannot open file: " + path.string());
    std::string value;
    char buf[16384];
    while (file) { file.read(buf, sizeof(buf)); auto count = file.gcount(); if (value.size() + count > max_size) throw std::runtime_error("input exceeds size limit"); value.append(buf, count); }
    return value;
}
inline void write_file(const std::filesystem::path& path, const std::string& value) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file || !file.write(value.data(), value.size())) throw std::runtime_error("cannot write file: " + path.string());
}
}
