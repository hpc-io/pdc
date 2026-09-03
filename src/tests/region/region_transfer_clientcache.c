/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/time.h>
#include "pdc.h"
#include "pdc_private.h"
#include "test_helper.h"

#define BUF_LEN 128

/*
 * Integration test for the client-side region cache.
 *
 * It writes a region of known values to an object, then reads the same region
 * back twice. The first read populates the client cache (when built with
 * PDC_CLIENT_CACHE=ON); the second read should be served from the cache. Both
 * reads are verified byte-for-byte against the written data.
 */

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop, reg, reg_global;
    pdcid_t obj1;
    char    cont_name[128], obj_name1[128];
    pdcid_t transfer_request;

    int rank = 0, size = 1, i;
    int ret_value = TSUCCEED;

    uint64_t offset[1], offset_length[1], local_offset[1];
    uint64_t dims[1];

    local_offset[0]  = 0;
    offset[0]        = 0;
    offset_length[0] = BUF_LEN;

    int *data      = (int *)malloc(sizeof(int) * BUF_LEN);
    int *data_read = (int *)malloc(sizeof(int) * BUF_LEN);
    dims[0]        = PDC_SIZE_UNLIMITED;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
    sprintf(cont_name, "c%d", rank);
    TASSERT((cont = PDCcont_create(cont_name, cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");
    // create an object property
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    TASSERT(PDCprop_set_obj_type(obj_prop, PDC_INT) >= 0, "Call to PDCprop_set_obj_type succeeded",
            "Call to PDCprop_set_obj_type failed");
    TASSERT(PDCprop_set_obj_dims(obj_prop, 1, dims) >= 0, "Call to PDCprop_set_obj_dims succeeded",
            "Call to PDCprop_set_obj_dims failed");
    TASSERT(PDCprop_set_obj_user_id(obj_prop, getuid()) >= 0, "Call to PDCprop_set_obj_user_id succeeded",
            "Call to PDCprop_set_obj_user_id failed");
    TASSERT(PDCprop_set_obj_time_step(obj_prop, 0) >= 0, "Call to PDCprop_set_obj_time_step succeeded",
            "Call to PDCprop_set_obj_time_step failed");
    TASSERT(PDCprop_set_obj_app_name(obj_prop, "ClientCacheTest") >= 0,
            "Call to PDCprop_set_obj_app_name succeeded", "Call to PDCprop_set_obj_app_name failed");
    TASSERT(PDCprop_set_obj_tags(obj_prop, "tag0=1") >= 0, "Call to PDCprop_set_obj_tags succeeded",
            "Call to PDCprop_set_obj_tags failed");

    // create the object
    sprintf(obj_name1, "o1_%d", rank);
    TASSERT((obj1 = PDCobj_create(cont, obj_name1, obj_prop)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");

    // regions for the write
    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    TASSERT((reg = PDCregion_create(1, offset, offset_length)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    // known pattern
    for (i = 0; i < BUF_LEN; ++i)
        data[i] = i;

    // ---- WRITE ----
    TASSERT((transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    // fresh regions for the read (PDC requires re-creating regions for the read)
    TASSERT((reg = PDCregion_create(1, local_offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    // ---- READ #1 (populates the client cache when enabled) ----
    memset(data_read, 0, sizeof(int) * BUF_LEN);
    TASSERT((transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj1, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    // verify read #1
    for (i = 0; i < BUF_LEN; ++i) {
        if (data_read[i] != i)
            PGOTO_ERROR(FAIL, "read #1 wrong value at %d: %d != %d", i, data_read[i], i);
    }

    // fresh regions again for the second read
    TASSERT((reg = PDCregion_create(1, local_offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    // ---- READ #2 (should hit the client cache) ----
    memset(data_read, 0, sizeof(int) * BUF_LEN);
    TASSERT((transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj1, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    // verify read #2
    for (i = 0; i < BUF_LEN; ++i) {
        if (data_read[i] != i)
            PGOTO_ERROR(FAIL, "read #2 (cache hit) wrong value at %d: %d != %d", i, data_read[i], i);
    }

    if (rank == 0)
        printf("[PASS] client cache read-back verified for %d elements over 2 reads\n", BUF_LEN);

    // ---- cleanup ----
    TASSERT(PDCregion_close(reg) >= 0, "Call to PDCregion_close succeeded", "Call to PDCregion_close failed");
    TASSERT(PDCregion_close(reg_global) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");
    TASSERT(PDCobj_close(obj1) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    TASSERT(PDCprop_close(obj_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
    free(data);
    free(data_read);
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
