#include "fluxforge/agent.hpp"
#include <sys/signalfd.h>
#include <signal.h>
#include <curl/curl.h>
#include <iostream>
int main() {
    try {
        sigset_t mask; sigemptyset(&mask); sigaddset(&mask, SIGINT); sigaddset(&mask, SIGTERM);
        if (pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0) throw std::runtime_error("cannot block shutdown signals");
        int signals = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC); if (signals < 0) throw std::runtime_error("cannot create signal descriptor");
        ff::Config config;
        // Worker Host is forked before gRPC and libcurl can create internal threads.
        ff::WorkerClient workers(config.host_binary.string(), config.worker_binary.string(), config.slots);
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) throw std::runtime_error("cannot initialize libcurl");
        auto channel = grpc::CreateChannel(config.address, grpc::InsecureChannelCredentials());
        { ff::Agent agent(config, workers, channel); if (agent.register_runner(signals)) agent.run(signals); }
        close(signals); curl_global_cleanup(); return 0;
    } catch (const std::exception& e) { std::cerr << "runner: " << e.what() << '\n'; return 1; }
}
