#include <fluxforge/function.hpp>
#include <unistd.h>
#include <vector>

namespace {
const auto initializer_pid = getpid();
std::vector<unsigned char> pages(8 * 1024 * 1024, 42);
int invocations = 0;
}

FLUXFORGE_FUNCTION([](const nlohmann::json&, const ff_call_v1&) {
    return nlohmann::json{{"initializer_pid", initializer_pid}, {"parent_pid", getppid()},
                          {"worker_pid", getpid()}, {"invocations", ++invocations},
                          {"page_value", ++pages[0]}};
})
