#pragma once
#include "common.hpp"
#include "fluxforge_v1.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <curl/curl.h>
#include <openssl/evp.h>
#include <iomanip>
#include <sstream>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <atomic>

namespace ff {
namespace pb = fluxforge::v1;
inline std::string sha256(const std::filesystem::path& path) {
    auto* context = EVP_MD_CTX_new(); if (!context) throw std::runtime_error("cannot allocate digest context");
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> guard(context, EVP_MD_CTX_free);
    EVP_DigestInit_ex(context, EVP_sha256(), nullptr); std::ifstream input(path, std::ios::binary); if (!input) throw std::runtime_error("cannot hash file");
    char buffer[16384]; while (input) { input.read(buffer, sizeof(buffer)); if (auto n = input.gcount(); n > 0) EVP_DigestUpdate(context, buffer, n); }
    unsigned char digest[EVP_MAX_MD_SIZE]; unsigned int length; EVP_DigestFinal_ex(context, digest, &length);
    std::ostringstream text; for (unsigned int i = 0; i < length; ++i) text << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]); return text.str();
}
class ObjectIO {
    std::unique_ptr<pb::ObjectService::Stub> stub_;
    std::string session_, token_;
    struct Download { std::ofstream file; uint64_t size = 0, max; bool failed = false; };
    static size_t write_download(char* data, size_t size, size_t count, void* pointer) {
        auto& out = *static_cast<Download*>(pointer); auto bytes = size * count;
        if (bytes > out.max - out.size) { out.failed = true; return 0; }
        out.file.write(data, bytes); if (!out.file) { out.failed = true; return 0; } out.size += bytes; return bytes;
    }
    static int progress(void* context, curl_off_t, curl_off_t, curl_off_t, curl_off_t) { return (*static_cast<std::function<bool()>*>(context))() ? 0 : 1; }
    void context(grpc::ClientContext& ctx) {
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        if (!token_.empty()) ctx.AddMetadata("authorization", "Bearer " + token_);
    }
    static void check(const grpc::Status& status) { if (!status.ok()) throw std::runtime_error("ObjectService: " + status.error_message()); }
public:
    ObjectIO(std::shared_ptr<grpc::Channel> channel, std::string session, std::string token) : stub_(pb::ObjectService::NewStub(channel)), session_(std::move(session)), token_(std::move(token)) {}
    void download(const pb::ObjectRef& object, const std::filesystem::path& destination,
                  const std::string& attempt, uint64_t max, std::function<bool()> alive) {
        if (object.size_bytes() < 0 || static_cast<uint64_t>(object.size_bytes()) > max) throw std::runtime_error("object exceeds configured size limit");
        pb::GetDownloadURLRequest req; req.set_object_id(object.object_id()); req.set_session_id(session_); req.set_attempt_id(attempt);
        pb::GetDownloadURLResponse resp; grpc::ClientContext ctx; context(ctx); check(stub_->GetDownloadURL(&ctx, req, &resp));
        Download out{std::ofstream(destination, std::ios::binary | std::ios::trunc), 0, max}; if (!out.file) throw std::runtime_error("cannot create download destination");
        auto* curl = curl_easy_init(); if (!curl) throw std::runtime_error("curl allocation failed"); std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> guard(curl, curl_easy_cleanup);
        curl_easy_setopt(curl, CURLOPT_URL, resp.url().c_str()); curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L); curl_easy_setopt(curl, CURLOPT_TIMEOUT, 120L); curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &write_download); curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, &progress); curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &alive); curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        auto code = curl_easy_perform(curl); long http = 0; curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http); out.file.close();
        if (code != CURLE_OK || http != 200 || out.failed) throw std::runtime_error("object download failed: " + std::string(curl_easy_strerror(code)) + " HTTP " + std::to_string(http));
        if (out.size != static_cast<uint64_t>(object.size_bytes()) || sha256(destination) != object.sha256()) throw std::runtime_error("object size or SHA256 mismatch");
    }
    pb::ObjectRef upload(const std::filesystem::path& file, const std::string& attempt, std::function<bool()> alive) {
        pb::BeginUploadRequest req; req.set_purpose("ATTEMPT_OUTPUT"); req.set_size_bytes(std::filesystem::file_size(file)); req.set_sha256(sha256(file)); req.set_content_type("application/json"); req.set_session_id(session_); req.set_attempt_id(attempt);
        pb::BeginUploadResponse resp; grpc::ClientContext ctx; context(ctx); check(stub_->BeginUpload(&ctx, req, &resp));
        auto* curl = curl_easy_init(); if (!curl) throw std::runtime_error("curl allocation failed"); std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> guard(curl, curl_easy_cleanup);
        auto* input = fopen(file.c_str(), "rb"); if (!input) throw std::runtime_error("cannot read output file"); std::unique_ptr<FILE, decltype(&fclose)> file_guard(input, fclose);
        curl_slist* headers = nullptr; for (const auto& header : resp.headers()) headers = curl_slist_append(headers, (header.first + ": " + header.second).c_str());
        headers = curl_slist_append(headers, "Expect:"); std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> header_guard(headers, curl_slist_free_all);
        curl_easy_setopt(curl, CURLOPT_URL, resp.url().c_str()); curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS); curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L); curl_easy_setopt(curl, CURLOPT_READDATA, input); curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(req.size_bytes())); curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L); curl_easy_setopt(curl, CURLOPT_TIMEOUT, 120L); curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +[](char*, size_t size, size_t count, void*) -> size_t { return size * count; });
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, &progress); curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &alive); curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        auto code = curl_easy_perform(curl); long http = 0; curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http);
        if (code != CURLE_OK || http < 200 || http >= 300) throw std::runtime_error("object upload failed: " + std::string(curl_easy_strerror(code)) + " HTTP " + std::to_string(http));
        pb::CommitUploadRequest commit; commit.set_upload_id(resp.upload_id()); pb::CommitUploadResponse committed; grpc::ClientContext commit_context; context(commit_context); check(stub_->CommitUpload(&commit_context, commit, &committed)); return committed.object();
    }
};
class ArtifactCache {
    struct Entry { std::filesystem::path path; std::mutex mutex; std::condition_variable ready; std::atomic<bool> done{false}; std::exception_ptr error; int64_t used = now_ms(); std::atomic<uint64_t> size{0}; };
    std::filesystem::path directory_; uint64_t max_bytes_;
    std::mutex mutex_; std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
public:
    struct Handle { std::filesystem::path path; std::shared_ptr<void> pin; };
    explicit ArtifactCache(std::filesystem::path directory, uint64_t max_bytes = 128ULL * 1024 * 1024) : directory_(std::move(directory)), max_bytes_(max_bytes) { std::filesystem::create_directories(directory_); }
    Handle get(const pb::ObjectRef& object, ObjectIO& objects, const std::string& attempt, const std::function<bool()>& alive) {
        if (object.sha256().size() != 64 || object.sha256().find_first_not_of("0123456789abcdef") != std::string::npos) throw std::runtime_error("invalid artifact SHA256");
        std::shared_ptr<Entry> entry; bool owner = false;
        { std::lock_guard lock(mutex_); auto it = entries_.find(object.sha256()); if (it != entries_.end()) entry = it->second; else { entry = std::make_shared<Entry>(); entry->path = directory_ / (object.sha256() + ".so"); entries_[object.sha256()] = entry; owner = true; } entry->used = now_ms(); }
        if (owner) {
            try {
                if (!std::filesystem::exists(entry->path) || std::filesystem::file_size(entry->path) != static_cast<uint64_t>(object.size_bytes()) || sha256(entry->path) != object.sha256()) {
                    auto temporary = entry->path.string() + ".download"; try { objects.download(object, temporary, attempt, 64ULL * 1024 * 1024, alive); std::filesystem::rename(temporary, entry->path); } catch (...) { std::filesystem::remove(temporary); throw; }
                }
                entry->size = std::filesystem::file_size(entry->path);
            } catch (...) { entry->error = std::current_exception(); }
            { std::lock_guard lock(entry->mutex); entry->done = true; } entry->ready.notify_all();
        } else {
            std::unique_lock lock(entry->mutex); while (!entry->done) { if (!alive()) throw std::runtime_error("artifact preparation revoked"); entry->ready.wait_for(lock, std::chrono::milliseconds(100)); }
        }
        if (entry->error) { std::lock_guard lock(mutex_); entries_.erase(object.sha256()); std::rethrow_exception(entry->error); }
        { std::lock_guard lock(mutex_); uint64_t total = 0; for (const auto& [key, value] : entries_) total += value->size;
            while (total > max_bytes_) { auto oldest = entries_.end(); for (auto it = entries_.begin(); it != entries_.end(); ++it) if (it->second.use_count() == 1 && it->second->done && (oldest == entries_.end() || it->second->used < oldest->second->used)) oldest = it; if (oldest == entries_.end()) break; total -= oldest->second->size; std::filesystem::remove(oldest->second->path); entries_.erase(oldest); }
        }
        return {entry->path, entry};
    }
};
}
