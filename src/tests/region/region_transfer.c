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

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop, reg, reg_global;
    perr_t  ret;
    pdcid_t obj1, obj2;
    char    cont_name[128], obj_name1[128], obj_name2[128];
    pdcid_t transfer_request;

    int rank = 0, size = 1, i;
    int ret_value = TSUCCEED;

    uint64_t offset[3], offset_length[3], local_offset[1];
    uint64_t dims[1];
    local_offset[0]  = 0;
    offset[0]        = 0;
    offset[1]        = 2;
    offset[2]        = 5;
    offset_length[0] = BUF_LEN;
    offset_length[1] = 3;
    offset_length[2] = 5;

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
    TASSERT(PDCprop_set_obj_app_name(obj_prop, "DataServerTest") >= 0,
            "Call to PDCprop_set_obj_user_id succeeded", "Call to PDCprop_set_obj_user_id failed");
    TASSERT(PDCprop_set_obj_tags(obj_prop, "tag0=1") >= 0, "Call to PDCprop_set_obj_tags succeeded",
            "Call to PDCprop_set_obj_tags failed");
    TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop, PDC_REGION_LOCAL) >= 0,
            "Call to PDCprop_set_obj_transfer_region_type succeeded",
            "Call to PDCprop_set_obj_transfer_region_type failed");

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    TASSERT((obj1 = PDCobj_create(cont, obj_name1, obj_prop)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");
    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    TASSERT((obj2 = PDCobj_create(cont, obj_name2, obj_prop)) != 0, "Call to PDCobj_create succeeded",
            "Call to PDCobj_create failed");

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    TASSERT((reg = PDCregion_create(1, offset, offset_length)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    for (i = 0; i < BUF_LEN; ++i)
        data[i] = i;

    // write transfer request
    TASSERT((transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");

    TASSERT((reg = PDCregion_create(1, local_offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    memset(data_read, 0, sizeof(int) * BUF_LEN);

    // read transfer request
    TASSERT((transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj1, reg, reg_global)) != 0,
            "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    TASSERT(PDCregion_transfer_start(transfer_request) >= 0, "Call to PDCregion_transfer_start succeeded",
            "Call to PDCregion_transfer_start failed");
    TASSERT(PDCregion_transfer_wait(transfer_request) >= 0, "Call to PDCregion_transfer_wait succeeded",
            "Call to PDCregion_transfer_wait failed");
    TASSERT(PDCregion_transfer_close(transfer_request) >= 0, "Call to PDCregion_transfer_close succeeded",
            "Call to PDCregion_transfer_close failed");
    // Check if data written previously has been correctly read.
    for (i = 0; i < BUF_LEN; ++i) {
        if (data_read[i] != i)
            PGOTO_ERROR(FAIL, "Wrong value %d!=%d", data_read[i], i);
    }

    TASSERT(PDCregion_close(reg) >= 0, "Call to PDCregion_close succeeded", "Call to PDCregion_close failed");
    TASSERT(PDCregion_close(reg_global) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");
    // close object
    TASSERT(PDCobj_close(obj1) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    TASSERT(PDCobj_close(obj2) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");
    // close a container
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    // close a object property
    TASSERT(PDCprop_close(obj_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close a container property
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

    free(data);
    free(data_read);

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
