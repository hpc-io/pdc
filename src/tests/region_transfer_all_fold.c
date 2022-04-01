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
#define BUF_LEN          64
#define OBJ_NUM          2
#define SUB_BUF_LEN      8
#define SUB_BUF_INTERVAL 1

int
main(int argc, char **argv)
{
    pdcid_t  pdc, cont_prop, cont, obj_prop, reg, reg_global;
    perr_t   ret;
    pdcid_t *obj;
    char     cont_name[128], obj_name[128];
    pdcid_t *transfer_request;

    int rank = 0, size = 1, i, j, index;
    int ret_value = 0;

    uint64_t offset[1], offset_length[1];
    uint64_t dims[1];

    int **data, **data_read;
    int   start_method = 1;
    int   wait_method  = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (argc >= 2) {
        start_method = atoi(argv[0]);
    }
    if (argc >= 3) {
        wait_method = atoi(argv[1]);
    }

    data         = (int **)malloc(sizeof(int *) * OBJ_NUM);
    data_read    = (int **)malloc(sizeof(int *) * OBJ_NUM);
    data[0]      = (int *)malloc(sizeof(int) * BUF_LEN * OBJ_NUM);
    data_read[0] = (int *)malloc(sizeof(int) * BUF_LEN * OBJ_NUM);

    for (i = 1; i < OBJ_NUM; ++i) {
        data[i]      = data[i - 1] + BUF_LEN;
        data_read[i] = data_read[i - 1] + BUF_LEN;
    }
    if (!rank) {
        printf("testing region_transfer_all_append, start_method = %d, wait_method = %d\n", start_method,
               wait_method);
    }
    dims[0] = BUF_LEN;

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);

    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);

    PDCprop_set_obj_type(obj_prop, PDC_INT);
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // create many objects
    obj = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
    for (i = 0; i < OBJ_NUM; ++i) {
        PDCprop_set_obj_type(obj_prop, PDC_REGION_STATIC);

        sprintf(obj_name, "o%d_%d", i, rank);
        obj[i] = PDCobj_create(cont, obj_name, obj_prop);
    }

    offset[0]        = 0;
    offset_length[0] = SUB_BUF_LEN;
    reg              = PDCregion_create(1, offset, offset_length);

    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i) {
            data[j][i] = i;
        }
    }
    transfer_request = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM * REQ_SIZE);

    // Place a transfer request for every objects
    index = 0;
    for (i = 0; i < OBJ_NUM; ++i) {
        for (j = 0; j + SUB_BUF_LEN < BUF_LEN; j += SUB_BUF_INTERVAL) {
            offset[0]               = (uint64_t)j;
            offset_length[0]        = SUB_BUF_LEN;
            reg_global              = PDCregion_create(1, offset, offset_length);
            transfer_request[index] = PDCregion_transfer_create(data[i], PDC_WRITE, obj[i], reg, reg_global);
            index++;
            PDCregion_close(reg_global);
        }
    }

    PDCregion_close(reg);

    for (i = 0; i < index; ++i) {
        PDCregion_transfer_start(transfer_request[i]);
    }

    for (i = 0; i < index; ++i) {
        PDCregion_transfer_wait(transfer_request[i]);
    }

    for (i = 0; i < index; ++i) {
        PDCregion_transfer_close(transfer_request[i]);
    }

    for (i = 0; i < OBJ_NUM; ++i) {
        PDCobj_close(obj[i]);
    }

    for (i = 0; i < OBJ_NUM; ++i) {
        sprintf(obj_name, "o%d_%d", i, rank);
        obj[i] = PDCobj_open(obj_name, pdc);
        if (obj[i] > 0) {
            printf("Create an object o1\n");
        }
        else {
            printf("Fail to create object @ line  %d!\n", __LINE__);
            ret_value = 1;
        }
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg              = PDCregion_create(1, offset, offset_length);
    reg_global       = PDCregion_create(1, offset, offset_length);

    transfer_request[0] = PDCregion_transfer_create(data_read[0], PDC_READ, obj[0], reg, reg_global);

    PDCregion_close(reg);
    PDCregion_close(reg_global);

    PDCregion_transfer_start(transfer_request[0]);

    PDCregion_transfer_wait(transfer_request[0]);

    PDCregion_transfer_close(transfer_request[0]);

    // close object
    for (i = 0; i < OBJ_NUM; ++i) {
        PDCobj_close(obj[i]);
    }

    // Check if data written previously has been correctly read.
    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i) {
            if (data_read[j][i] != i) {
                printf("wrong value %d!=%d @ line %d\n", data_read[j][i], i, __LINE__);
                ret_value = 1;
                break;
            }
        }
    }

    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container c1 @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close container c1 @ line %d\n", __LINE__);
    }
    // close a object property
    if (PDCprop_close(obj_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close object property @ line %d\n", __LINE__);
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close container property @ line %d\n", __LINE__);
    }
    free(data);
    free(data_read);
    free(obj);
    free(transfer_request);
    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("fail to close PDC @ line %d\n", __LINE__);
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
