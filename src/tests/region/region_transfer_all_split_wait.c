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
#include "test_helper.h"

#define BUF_LEN 4096
#define OBJ_NUM 20

int
main(int argc, char **argv)
{
    pdcid_t  pdc, cont_prop, cont, obj_prop, reg, reg_global;
    perr_t   ret;
    pdcid_t *obj;
    char     cont_name[128], obj_name[128];
    pdcid_t *transfer_request;

    int   rank = 0, size = 1, i, j;
    int   ret_value = 0;
    int **data, **data_read;

    uint64_t offset[1], offset_length[1];
    uint64_t dims[1];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    data         = (int **)malloc(sizeof(int *) * OBJ_NUM);
    data_read    = (int **)malloc(sizeof(int *) * OBJ_NUM);
    data[0]      = (int *)malloc(sizeof(int) * BUF_LEN * OBJ_NUM);
    data_read[0] = (int *)malloc(sizeof(int) * BUF_LEN * OBJ_NUM);

    for (i = 1; i < OBJ_NUM; ++i) {
        data[i]      = data[i - 1] + BUF_LEN;
        data_read[i] = data_read[i - 1] + BUF_LEN;
    }

    dims[0] = BUF_LEN;

    // create a container property
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

    // create many objects
    obj = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
    for (i = 0; i < OBJ_NUM; ++i) {
        switch (i % 4) {
            case 0: {
                TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop, PDC_REGION_STATIC) >= 0,
                        "Call to PDCprop_set_obj_transfer_region_type succeeded",
                        "Call to PDCprop_set_obj_transfer_region_type failed");
                break;
            }
            case 1: {
                TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop, PDC_OBJ_STATIC) >= 0,
                        "Call to PDCprop_set_obj_transfer_region_type succeeded",
                        "Call to PDCprop_set_obj_transfer_region_type failed");
                break;
            }
            case 2: {
                TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop, PDC_REGION_LOCAL) >= 0,
                        "Call to PDCprop_set_obj_transfer_region_type succeeded",
                        "Call to PDCprop_set_obj_transfer_region_type failed");
                break;
            }
            default: {
                break;
            }
        }
        sprintf(obj_name, "o%d_%d", i, rank);
        TASSERT((obj[i] = PDCobj_create(cont, obj_name, obj_prop)) != 0, "Call to PDCobj_create succeeded",
                "Call to PDCobj_create failed");
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    TASSERT((reg = PDCregion_create(1, offset, offset_length)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i) {
            data[j][i] = i;
        }
    }
    transfer_request = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);

    // Place a transfer request for every objects
    for (i = 0; i < OBJ_NUM; ++i) {
        TASSERT((transfer_request[i] =
                     PDCregion_transfer_create(data[i], PDC_WRITE, obj[i], reg, reg_global)) != 0,
                "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    }
    TASSERT(PDCregion_transfer_start_all(transfer_request, OBJ_NUM / 2) >= 0,
            "Call to PDCregion_transfer_start_all succeeded", "Call to PDCregion_transfer_start_all failed");
    TASSERT(PDCregion_transfer_start_all(transfer_request + OBJ_NUM / 2, OBJ_NUM / 2) >= 0,
            "Call to PDCregion_transfer_start_all succeeded", "Call to PDCregion_transfer_start_all failed");
    TASSERT(PDCregion_transfer_wait_all(transfer_request + OBJ_NUM / 4, OBJ_NUM / 2) >= 0,
            "Call to PDCregion_transfer_wait_all succeeded", "Call to PDCregion_transfer_wait_all failed");
    TASSERT(PDCregion_transfer_wait_all(transfer_request, OBJ_NUM / 4) >= 0,
            "Call to PDCregion_transfer_wait_all succeeded", "Call to PDCregion_transfer_wait_all failed");
    TASSERT(PDCregion_transfer_wait_all(transfer_request + OBJ_NUM * 3 / 4, OBJ_NUM / 4) >= 0,
            "Call to PDCregion_transfer_wait_all succeeded", "Call to PDCregion_transfer_wait_all failed");

    for (i = 0; i < OBJ_NUM; ++i) {
        TASSERT(PDCregion_transfer_close(transfer_request[i]) >= 0,
                "Call to PDCregion_transfer_close succeeded", "Call to PDCregion_transfer_close failed");
    }

    TASSERT(PDCregion_close(reg) >= 0, "Call to PDCregion_close succeeded", "Call to PDCregion_close failed");
    TASSERT(PDCregion_close(reg_global) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    TASSERT((reg = PDCregion_create(1, offset, offset_length)) != 0, "Call to PDCregion_create succeeded",
            "Call to PDCregion_create failed");
    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    TASSERT((reg_global = PDCregion_create(1, offset, offset_length)) != 0,
            "Call to PDCregion_create succeeded", "Call to PDCregion_create failed");

    for (i = 0; i < OBJ_NUM; ++i) {
        memset(data_read[i], 0, sizeof(int) * BUF_LEN);
        TASSERT((transfer_request[i] =
                     PDCregion_transfer_create(data_read[i], PDC_READ, obj[i], reg, reg_global)) != 0,
                "Call to PDCregion_transfer_create succeeded", "Call to PDCregion_transfer_create failed");
    }

    TASSERT(PDCregion_transfer_start_all(transfer_request, OBJ_NUM / 2) >= 0,
            "Call to PDCregion_transfer_start_all succeeded", "Call to PDCregion_transfer_start_all failed");
    TASSERT(PDCregion_transfer_start_all(transfer_request + OBJ_NUM / 2, OBJ_NUM / 2) >= 0,
            "Call to PDCregion_transfer_start_all succeeded", "Call to PDCregion_transfer_start_all failed");
    TASSERT(PDCregion_transfer_wait_all(transfer_request + OBJ_NUM / 4, OBJ_NUM / 2) >= 0,
            "Call to PDCregion_transfer_wait_all succeeded", "Call to PDCregion_transfer_wait_all failed");
    TASSERT(PDCregion_transfer_wait_all(transfer_request, OBJ_NUM / 4) >= 0,
            "Call to PDCregion_transfer_wait_all succeeded", "Call to PDCregion_transfer_wait_all failed");
    TASSERT(PDCregion_transfer_wait_all(transfer_request + OBJ_NUM * 3 / 4, OBJ_NUM / 4) >= 0,
            "Call to PDCregion_transfer_wait_all succeeded", "Call to PDCregion_transfer_wait_all failed");

    for (i = 0; i < OBJ_NUM; ++i) {
        TASSERT(PDCregion_transfer_close(transfer_request[i]) >= 0, "Call to PDCregion_close succeeded",
                "Call to PDCregion_close failed");
    }

    // close object
    for (i = 0; i < OBJ_NUM; ++i)
        TASSERT(PDCobj_close(obj[i]) >= 0, "Call to PDCobj_close succeeded", "Call to PDCobj_close failed");

    // Check if data written previously has been correctly read.
    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i) {
            if (data_read[j][i] != i)
                TGOTO_ERROR(TFAIL, "Wrong value %d!=%d\n", data_read[j][i], i);
        }
    }

    TASSERT(PDCregion_close(reg) >= 0, "Call to PDCregion_close succeeded", "Call to PDCregion_close failed");
    TASSERT(PDCregion_close(reg_global) >= 0, "Call to PDCregion_close succeeded",
            "Call to PDCregion_close failed");

    // close container
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    // close object property
    TASSERT(PDCprop_close(obj_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close container property
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

    free(data[0]);
    free(data_read[0]);
    free(data);
    free(data_read);
    free(obj);
    free(transfer_request);

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
