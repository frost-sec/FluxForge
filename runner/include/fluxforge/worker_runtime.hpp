#pragma once
#include "common.hpp"
#include <fluxforge/function.h>

namespace ff::worker {
using Function = int32_t (*)(const ff_call_v1*, const ff_writer_v1*, const ff_writer_v1*);
json invoke(const json& request, Function function);
}
