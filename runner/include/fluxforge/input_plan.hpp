#pragma once
#include "object_io.hpp"
namespace ff {
class InputResolver {
    ObjectIO& objects_; std::filesystem::path directory_; std::string attempt_; uint64_t max_;
    std::function<bool()> alive_; std::unordered_map<std::string, json> downloaded_;
    uint64_t source_bytes_ = 0;
    void account(uint64_t size) {
        if (size > max_ - source_bytes_) throw std::runtime_error("total bound source bytes exceed payload budget");
        source_bytes_ += size;
    }
    json bound(const pb::BoundInput& input) {
        const auto& payload = input.payload(); json value;
        if (payload.value_case() == pb::PayloadRef::kInlineJson) {
            account(payload.inline_json().size()); value = json::parse(payload.inline_json());
        } else if (payload.value_case() == pb::PayloadRef::kObject) {
            const auto& object = payload.object(); auto it = downloaded_.find(object.object_id());
            if (it == downloaded_.end()) { if (object.size_bytes() < 0) throw std::runtime_error("negative source size"); account(static_cast<uint64_t>(object.size_bytes())); auto path = directory_ / ("bound-" + std::to_string(downloaded_.size()) + ".json"); objects_.download(object, path, attempt_, max_, alive_); it = downloaded_.emplace(object.object_id(), json::parse(read_file(path, max_))).first; }
            value = it->second;
        } else throw std::runtime_error("input binding has no payload");
        if (!input.json_pointer().empty()) value = value.at(json::json_pointer(input.json_pointer())); return value;
    }
public:
    InputResolver(ObjectIO& objects, std::filesystem::path directory, std::string attempt, uint64_t max, std::function<bool()> alive) : objects_(objects), directory_(std::move(directory)), attempt_(std::move(attempt)), max_(max), alive_(std::move(alive)) {}
    std::filesystem::path resolve(const pb::InputPlan& plan) {
        json input;
        if (plan.binding_case() == pb::InputPlan::kSource) input = bound(plan.source());
        else if (plan.binding_case() == pb::InputPlan::kObjectFields) { if (plan.object_fields().fields_size() > 256) throw std::runtime_error("too many input bindings"); input = json::object(); for (const auto& field : plan.object_fields().fields()) input[field.first] = bound(field.second); }
        else throw std::runtime_error("input plan binding is missing");
        auto encoded = input.dump(); if (encoded.size() > max_) throw std::runtime_error("assembled input exceeds payload limit"); auto destination = directory_ / "input.json"; write_file(destination, encoded); return destination;
    }
};
}
