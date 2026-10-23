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
    const char *   transport = getenv("HG_TRANSPORT");
    hg_class_t *   hg_class  = HG_Init(transport ? transport : "ofi+tcp", HG_FALSE);
    const uint32_t counts[]  = {0, 4, 8192};
    CHECK(hg_class);
    for (size_t mode = 0; mode < 2; mode++) {
        for (size_t test = 0; test < sizeof(counts) / sizeof(counts[0]); test++) {
            uint32_t                      count  = counts[test];
            hg_size_t                     bytes  = count * sizeof(uint64_t);
            uint64_t *                    ids    = count ? malloc(bytes) : NULL;
            kvtag_query_out_t             input  = {.ret                       = count,
                                       .server_time_elapsed       = 17,
                                       .server_memory_consumption = 29,
                                       .bulk_handle               = HG_BULK_NULL};
            metadata_query_transfer_out_t output = {0};
            unsigned char                 initial[128];
            hg_proc_t                     proc;
            CHECK(!count || ids);
            for (uint32_t i = 0; i < count; i++)
                ids[i] = UINT64_C(0x1234567800000000) + i;
            if (count)
                CHECK(HG_Bulk_create(hg_class, 1, (void **)&ids, &bytes, HG_BULK_READ_ONLY,
                                     &input.bulk_handle) == HG_SUCCESS);
            CHECK(hg_proc_create_set(hg_class, initial, sizeof(initial), HG_ENCODE, HG_NOHASH, &proc) ==
                  HG_SUCCESS);
            uint8_t flags = mode ? HG_PROC_SM : 0;
            hg_proc_set_flags(proc, flags);
            CHECK(hg_proc_kvtag_query_out_t(proc, &input) == HG_SUCCESS);
            CHECK(hg_proc_get_flags(proc) == flags);
            CHECK(hg_proc_flush(proc) == HG_SUCCESS);
            hg_size_t used    = hg_proc_get_size_used(proc);
            void *    encoded = malloc(used);
            CHECK(encoded);
            memcpy(encoded, hg_proc_get_extra_buf(proc) ? hg_proc_get_extra_buf(proc) : initial, used);
            CHECK(hg_proc_free(proc) == HG_SUCCESS);

            // The old decoder must work after the server's handle and storage are gone.
            if (count) {
                CHECK(HG_Bulk_free(input.bulk_handle) == HG_SUCCESS);
                memset(ids, 0xa5, bytes);
                free(ids);
            }
            CHECK(hg_proc_create_set(hg_class, encoded, used, HG_DECODE, HG_NOHASH, &proc) == HG_SUCCESS);
            CHECK(hg_proc_metadata_query_transfer_out_t(proc, &output) == HG_SUCCESS);
            CHECK(output.ret == (int32_t)count && output.server_time_elapsed == 17 &&
                  output.server_memory_consumption == 29);
            if (count) {
                uint64_t *data = NULL;
                hg_size_t size;
                uint32_t  actual;
                CHECK(HG_Bulk_access(output.bulk_handle, 0, bytes, HG_BULK_READ_ONLY, 1, (void **)&data,
                                     &size, &actual) == HG_SUCCESS);
                CHECK(actual == 1 && size == bytes);
                for (uint32_t i = 0; i < count; i++)
                    CHECK(data[i] == UINT64_C(0x1234567800000000) + i);
            }
            else
                CHECK(output.bulk_handle == HG_BULK_NULL);
            CHECK(hg_proc_reset(proc, NULL, 0, HG_FREE) == HG_SUCCESS);
            CHECK(hg_proc_metadata_query_transfer_out_t(proc, &output) == HG_SUCCESS);
            CHECK(hg_proc_free(proc) == HG_SUCCESS);
            free(encoded);
        }
    }
    CHECK(HG_Finalize(hg_class) == HG_SUCCESS);
    puts("SOMETA response ownership and legacy decoding passed");
    return 0;
}
