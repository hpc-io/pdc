/* Exercise full static-object reads and a partial-read fallback with reusable user buffers. */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc.h"

#define CHECK(expr)                                                                                          \
    do {                                                                                                     \
        if (!(expr)) {                                                                                       \
            fprintf(stderr, "rank %d: %s failed at line %d\n", rank, #expr, __LINE__);                       \
            exit(1);                                                                                         \
        }                                                                                                    \
    } while (0)

int
main(int argc, char **argv)
{
    int rank          = 0;
    int read_existing = argc > 1 && strcmp(argv[1], "read") == 0;
    int write_only    = argc > 1 && strcmp(argv[1], "write") == 0;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif
    pdcid_t pdc = PDCinit("pdc");
    CHECK(pdc);
    pdcid_t cp = PDCprop_create(PDC_CONT_CREATE, pdc);
    char    name[128];
    snprintf(name, sizeof(name), "contiguous_read_c_%d", rank);
    pdcid_t cont = read_existing ? PDCcont_open(name, pdc) : PDCcont_create(name, cp);
    CHECK(cp && cont);
    for (int ndim = 2; ndim <= 3; ndim++) {
        uint64_t dims[3] = {5, 9, 11}, zero[3] = {0, 0, 0};
        size_t   count = 1;
        for (int d = 0; d < ndim; d++)
            count *= dims[d];
        int *input  = malloc(count * sizeof(int));
        int *output = malloc(count * sizeof(int));
        CHECK(input && output);
        for (size_t i = 0; i < count; i++)
            input[i] = (int)i + 1000 * rank;
        pdcid_t prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
        CHECK(prop);
        CHECK(PDCprop_set_obj_type(prop, PDC_INT) >= 0);
        CHECK(PDCprop_set_obj_dims(prop, ndim, dims) >= 0);
        CHECK(PDCprop_set_obj_transfer_region_type(prop, PDC_OBJ_STATIC) >= 0);
        snprintf(name, sizeof(name), "contiguous_read_%dd_%d", ndim, rank);
        pdcid_t obj    = read_existing ? PDCobj_open(name, pdc) : PDCobj_create(cont, name, prop);
        pdcid_t local  = PDCregion_create(ndim, zero, dims);
        pdcid_t remote = PDCregion_create(ndim, zero, dims);
        CHECK(obj && local && remote);
        pdcid_t tr;
        if (!read_existing) {
            tr = PDCregion_transfer_create(input, PDC_WRITE, obj, local, remote);
            CHECK(tr && PDCregion_transfer_start(tr) >= 0 && PDCregion_transfer_wait(tr) >= 0);
            CHECK(PDCregion_transfer_close(tr) >= 0);
        }
        int *other_input = malloc(count * sizeof(int));
        CHECK(other_input);
        for (size_t i = 0; i < count; i++)
            other_input[i] = input[i] + 1000000;
        snprintf(name, sizeof(name), "contiguous_read_other_%dd_%d", ndim, rank);
        pdcid_t other_obj = read_existing ? PDCobj_open(name, pdc) : PDCobj_create(cont, name, prop);
        CHECK(other_obj);
        if (!read_existing) {
            tr = PDCregion_transfer_create(other_input, PDC_WRITE, other_obj, local, remote);
            CHECK(tr && PDCregion_transfer_start(tr) >= 0 && PDCregion_transfer_wait(tr) >= 0);
            CHECK(PDCregion_transfer_close(tr) >= 0);
        }
        if (write_only) {
            CHECK(PDCregion_close(local) >= 0 && PDCregion_close(remote) >= 0);
            CHECK(PDCobj_close(obj) >= 0 && PDCobj_close(other_obj) >= 0 && PDCprop_close(prop) >= 0);
            free(input);
            free(other_input);
            free(output);
            continue;
        }
        /* Complete single and aggregated starts through wait, wait_all and status. */
        for (int repeat = 0; repeat < 6; repeat++) {
            memset(output, 0, count * sizeof(int));
            tr = PDCregion_transfer_create(output, PDC_READ, obj, local, remote);
            CHECK(tr);
            if (repeat % 3 == 1 || repeat >= 3)
                CHECK(PDCregion_transfer_start_all(&tr, 1) >= 0);
            else
                CHECK(PDCregion_transfer_start(tr) >= 0);
            if (repeat % 3 == 1)
                CHECK(PDCregion_transfer_wait_all(&tr, 1) >= 0);
            else if (repeat % 3 == 2) {
                pdc_transfer_status_t status = PDC_TRANSFER_STATUS_PENDING;
                for (int poll = 0; status != PDC_TRANSFER_STATUS_COMPLETE && poll < 10000; poll++)
                    CHECK(PDCregion_transfer_status(tr, &status) >= 0);
                CHECK(status == PDC_TRANSFER_STATUS_COMPLETE);
                CHECK(PDCregion_transfer_wait(tr) >= 0);
            }
            else
                CHECK(PDCregion_transfer_wait(tr) >= 0);
            CHECK(memcmp(input, output, count * sizeof(int)) == 0);
            CHECK(PDCregion_transfer_close(tr) >= 0);
            /* The completed request must retain caller ownership of output. */
            output[count - 1] = -1;
        }
        /* Complete two full reads into distinct caller-owned buffers. This
         * also exercises a response crossing a registered segment boundary. */
        int *second_output = malloc(count * sizeof(int));
        CHECK(second_output);
        memset(output, -1, count * sizeof(int));
        memset(second_output, -1, count * sizeof(int));
        pdcid_t full_batch[2];
        full_batch[0] = PDCregion_transfer_create(output, PDC_READ, obj, local, remote);
        full_batch[1] = PDCregion_transfer_create(second_output, PDC_READ, other_obj, local, remote);
        CHECK(full_batch[0] && full_batch[1]);
        CHECK(PDCregion_transfer_start_all(full_batch, 2) >= 0);
        CHECK(PDCregion_transfer_wait_all(full_batch, 2) >= 0);
        CHECK(memcmp(input, output, count * sizeof(int)) == 0);
        CHECK(memcmp(other_input, second_output, count * sizeof(int)) == 0);
        CHECK(PDCregion_transfer_close(full_batch[0]) >= 0 && PDCregion_transfer_close(full_batch[1]) >= 0);
        /* Closing a completed single request must preserve progress for other
         * started requests. New requests can also join that active worker. */
        int *third_output = malloc(count * sizeof(int));
        CHECK(third_output);
        for (int repeat = 0; repeat < 4; repeat++) {
            memset(output, -1, count * sizeof(int));
            memset(second_output, -1, count * sizeof(int));
            memset(third_output, -1, count * sizeof(int));
            pdcid_t first  = PDCregion_transfer_create(output, PDC_READ, obj, local, remote);
            pdcid_t second = PDCregion_transfer_create(second_output, PDC_READ, other_obj, local, remote);
            CHECK(first && second);
            CHECK(PDCregion_transfer_start(first) >= 0 && PDCregion_transfer_start(second) >= 0);
            CHECK(PDCregion_transfer_wait(first) >= 0 && PDCregion_transfer_close(first) >= 0);
            pdcid_t third = PDCregion_transfer_create(third_output, PDC_READ, obj, local, remote);
            CHECK(third && PDCregion_transfer_start(third) >= 0);
            CHECK(PDCregion_transfer_wait(second) >= 0 && PDCregion_transfer_close(second) >= 0);
            CHECK(PDCregion_transfer_wait(third) >= 0 && PDCregion_transfer_close(third) >= 0);
            CHECK(memcmp(input, output, count * sizeof(int)) == 0);
            CHECK(memcmp(other_input, second_output, count * sizeof(int)) == 0);
            CHECK(memcmp(input, third_output, count * sizeof(int)) == 0);
        }
        third_output[count - 1] = -1;
        free(third_output);
        second_output[count - 1] = -1;
        free(second_output);
        pdcid_t  full_local = local, full_remote = remote;
        uint64_t subset[3] = {3, 4, 6}, offset[3] = {1, 2, 3};
        local  = PDCregion_create(ndim, zero, subset);
        remote = PDCregion_create(ndim, offset, subset);
        memset(output, 0, count * sizeof(int));
        int *full_output = malloc(count * sizeof(int));
        CHECK(full_output);
        pdcid_t full_tr  = PDCregion_transfer_create(full_output, PDC_READ, obj, full_local, full_remote);
        tr               = PDCregion_transfer_create(output, PDC_READ, obj, local, remote);
        pdcid_t batch[2] = {full_tr, tr};
        CHECK(full_tr && tr && PDCregion_transfer_start_all(batch, 2) >= 0);
        CHECK(PDCregion_transfer_wait_all(batch, 2) >= 0);
        CHECK(memcmp(input, full_output, count * sizeof(int)) == 0);
        size_t pos = 0;
        for (uint64_t i = 0; i < subset[0]; i++) {
            for (uint64_t j = 0; j < subset[1]; j++) {
                if (ndim == 2)
                    CHECK(output[pos++] == input[(i + offset[0]) * dims[1] + j + offset[1]]);
                else {
                    for (uint64_t k = 0; k < subset[2]; k++)
                        CHECK(output[pos++] ==
                              input[((i + offset[0]) * dims[1] + j + offset[1]) * dims[2] + k + offset[2]]);
                }
            }
        }
        CHECK(PDCregion_transfer_close(full_tr) >= 0 && PDCregion_transfer_close(tr) >= 0);
        CHECK(PDCregion_close(full_local) >= 0 && PDCregion_close(full_remote) >= 0);
        CHECK(PDCregion_close(local) >= 0 && PDCregion_close(remote) >= 0);
        full_output[count - 1] = -1;
        free(full_output);
        CHECK(PDCobj_close(obj) >= 0 && PDCobj_close(other_obj) >= 0 && PDCprop_close(prop) >= 0);
        free(input);
        free(other_input);
        free(output);
    }
    CHECK(PDCcont_close(cont) >= 0 && PDCprop_close(cp) >= 0 && PDCclose(pdc) >= 0);
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    printf("rank %d: %s\n", rank, write_only ? "write stage complete" : "full and partial reads verified");
    return 0;
}
