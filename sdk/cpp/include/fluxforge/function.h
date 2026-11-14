#ifndef FLUXFORGE_FUNCTION_H
#define FLUXFORGE_FUNCTION_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
#define FF_ABI_VERSION 1u
typedef struct ff_call_v1 {
    uint32_t struct_size;
    uint32_t abi_version;
    const uint8_t *input;
    uint64_t input_size;
    const char *run_id;
    const char *invocation_id;
    const char *attempt_id;
} ff_call_v1;
typedef struct ff_writer_v1 {
    uint32_t struct_size;
    void *context;
    int32_t (*write)(void *context, const uint8_t *data, uint64_t size);
} ff_writer_v1;
/* Return 0 for success, 1 for a user error. All buffers belong to the host.
 * Functions must not throw across this boundary, fork, or retain call pointers. */
uint32_t ff_abi_version(void);
int32_t ff_invoke_v1(const ff_call_v1 *call, const ff_writer_v1 *output,
                     const ff_writer_v1 *error);
#ifdef __cplusplus
}
#endif
#endif
