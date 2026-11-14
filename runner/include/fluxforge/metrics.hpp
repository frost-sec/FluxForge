#pragma once
#include "common.hpp"
#include "fluxforge_v1.pb.h"
#include <array>
#include <mutex>
#include <map>
namespace ff {
class Metrics {
    struct Value { std::array<uint64_t, 15> bins{}; uint64_t count = 0, errors = 0, cold = 0, warm = 0; double sum = 0; };
    const std::array<double, 14> bounds_{0.001,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5,10,30,60};
    using Key = std::tuple<std::string,std::string,std::string>;
    std::mutex mutex_; std::map<Key, Value> values_;
public:
    void record(const fluxforge::v1::FunctionSpec& function, int64_t duration_ms, bool success, bool warm) {
        std::lock_guard lock(mutex_); Key key{function.function_id(), function.version(), function.runtime_digest()}; if (!values_.count(key) && values_.size() >= 1024) return; auto& value = values_[key];
        auto seconds = static_cast<double>(duration_ms) / 1000; size_t bin = 0; while (bin < bounds_.size() && seconds > bounds_[bin]) ++bin;
        ++value.bins[bin]; ++value.count; value.sum += seconds; value.errors += !success; value.warm += warm; value.cold += !warm;
    }
    fluxforge::v1::ReportMetricsRequest snapshot(const std::string& runner, const std::string& session, uint64_t sequence) {
        std::lock_guard lock(mutex_); fluxforge::v1::ReportMetricsRequest request;
        request.set_runner_id(runner); request.set_session_id(session); request.set_sequence(sequence); request.set_observed_unix_ms(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
        for (const auto& [key, value] : values_) { auto* function = request.add_functions(); function->set_function_id(std::get<0>(key)); function->set_version(std::get<1>(key)); function->set_runtime_digest(std::get<2>(key)); function->set_errors(value.errors); function->set_cold_starts(value.cold); function->set_warm_starts(value.warm); auto* histogram = function->mutable_execution(); for (auto bound : bounds_) histogram->add_bounds(bound); for (auto bin : value.bins) histogram->add_counts(bin); histogram->set_count(value.count); histogram->set_sum(value.sum); }
        return request;
    }
};
}
