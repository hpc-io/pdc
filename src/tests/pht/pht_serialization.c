#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc_client_server_common.h"

#define CHECK(condition)                                                                                     \
    do {                                                                                                     \
        if (!(condition)) {                                                                                  \
            fprintf(stderr, "%s:%d: %s failed\n", __FILE__, __LINE__, #condition);                           \
            return 1;                                                                                        \
        }                                                                                                    \
    } while (0)

int
main(void)
{
    unsigned char  wire[8192], bytes[4096];
    const uint32_t sizes[]  = {0, 1, 5, 8, sizeof(bytes)};
    hg_class_t *   hg_class = HG_Init("sm", HG_FALSE);
    hg_proc_t      proc;
    CHECK(hg_class);
    for (size_t i = 0; i < sizeof(bytes); i++)
        bytes[i] = (unsigned char)i;
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        metadata_key_add_in_t input  = {.prefix = "#01", .key = "binary", .size = sizes[i], .value = bytes};
        metadata_key_add_in_t output = {0};
        CHECK(hg_proc_create_set(hg_class, wire, sizeof(wire), HG_ENCODE, HG_NOHASH, &proc) == HG_SUCCESS);
        CHECK(hg_proc_metadata_key_add_in_t(proc, &input) == HG_SUCCESS);
        hg_size_t used = hg_proc_get_size_used(proc);
        CHECK(hg_proc_reset(proc, wire, used, HG_DECODE) == HG_SUCCESS);
        CHECK(hg_proc_metadata_key_add_in_t(proc, &output) == HG_SUCCESS);
        CHECK(strcmp(output.key, input.key) == 0 && strcmp(output.prefix, input.prefix) == 0);
        CHECK(output.size == input.size);
        CHECK(output.size ? memcmp(output.value, bytes, output.size) == 0 : output.value == NULL);
        CHECK(hg_proc_reset(proc, NULL, 0, HG_FREE) == HG_SUCCESS);
        CHECK(hg_proc_metadata_key_add_in_t(proc, &output) == HG_SUCCESS);
        CHECK(output.value == NULL);
        CHECK(hg_proc_free(proc) == HG_SUCCESS);
    }
    CHECK(HG_Finalize(hg_class) == HG_SUCCESS);
    puts("PHT binary RPC serialization passed");
    return 0;
}
