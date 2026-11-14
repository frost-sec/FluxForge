#pragma once
#include "config.hpp"
#include "fluxforge_v1.grpc.pb.h"
#include <grpcpp/grpcpp.h>
namespace ff {
namespace pb = fluxforge::v1;
class ControlClient {
    std::unique_ptr<pb::RunnerQueueService::Stub> stub_;
    std::string token_;
public:
    ControlClient(std::shared_ptr<grpc::Channel> channel, const std::string& token) : stub_(pb::RunnerQueueService::NewStub(channel)), token_(token) {}
#define FF_CONTROL_RPC(NAME, REQUEST, RESPONSE, TIMEOUT) \
    grpc::Status NAME(const pb::REQUEST& req, pb::RESPONSE& resp) { \
        grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(TIMEOUT)); \
        if (!token_.empty()) context.AddMetadata("authorization", "Bearer " + token_); \
        return stub_->NAME(&context, req, &resp); \
    }
    FF_CONTROL_RPC(RegisterRunner, RegisterRunnerRequest, RegisterRunnerResponse, 5000)
    FF_CONTROL_RPC(Heartbeat, HeartbeatRequest, HeartbeatResponse, 2000)
    FF_CONTROL_RPC(AcquireTasks, AcquireTasksRequest, AcquireTasksResponse, 3000)
    FF_CONTROL_RPC(ReportStarted, ReportStartedRequest, ReportStartedResponse, 3000)
    FF_CONTROL_RPC(ReportExecutionFinished, ReportExecutionFinishedRequest, ReportExecutionFinishedResponse, 3000)
    FF_CONTROL_RPC(RenewLeases, RenewLeasesRequest, RenewLeasesResponse, 2000)
    FF_CONTROL_RPC(ReportCompletion, ReportCompletionRequest, ReportCompletionResponse, 5000)
    FF_CONTROL_RPC(ReleaseTask, ReleaseTaskRequest, ReleaseTaskResponse, 2000)
    FF_CONTROL_RPC(ReportMetrics, ReportMetricsRequest, ReportMetricsResponse, 2000)
#undef FF_CONTROL_RPC
};
inline bool retryable(const grpc::Status& status) {
    return status.error_code() == grpc::StatusCode::UNAVAILABLE || status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED || status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED || status.error_code() == grpc::StatusCode::ABORTED || status.error_code() == grpc::StatusCode::INTERNAL;
}
}
