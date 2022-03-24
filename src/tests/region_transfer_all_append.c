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
#define BUF_LEN  64
#define OBJ_NUM  2
#define REQ_SIZE 8

int
main(int argc, char **argv)
{
    pdcid_t  pdc, cont_prop, cont, obj_prop, reg, reg_global;
    perr_t   ret;
    pdcid_t *obj;
    char     cont_name[128], obj_name[128];
    pdcid_t *transfer_request;

    int rank = 0, size = 1, i, j;
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
    printf("testing region_transfer_all_append, start_method = %d, wait_method = %d\n", start_method,
           wait_method);

    dims[0] = BUF_LEN;

    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop > 0) {
        printf("Create a container property\n");
    }
    else {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);
    if (cont > 0) {
        printf("Create a container c1\n");
    }
    else {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop > 0) {
        printf("Create an object property\n");
    }
    else {
        printf("Fail to create object property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    ret = PDCprop_set_obj_type(obj_prop, PDC_INT);
    if (ret != SUCCEED) {
        printf("Fail to set obj type @ line %d\n", __LINE__);
        ret_value = 1;
    }
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // create many objects
    obj = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
    for (i = 0; i < OBJ_NUM; ++i) {
        if (i % 2) {
            ret = PDCprop_set_obj_type(obj_prop, PDC_REGION_STATIC);
        }
        else {
            ret = PDCprop_set_obj_type(obj_prop, PDC_OBJ_STATIC);
        }
        sprintf(obj_name, "o%d_%d", i, rank);
        obj[i] = PDCobj_create(cont, obj_name, obj_prop);
        if (obj[i] > 0) {
            printf("Create an object o1\n");
        }
        else {
            printf("Fail to create object @ line  %d!\n", __LINE__);
            ret_value = 1;
        }
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN / REQ_SIZE;
    reg              = PDCregion_create(1, offset, offset_length);
    if (reg > 0) {
        printf("Create local region\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i) {
            data[j][i] = i;
        }
    }
    transfer_request = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM * REQ_SIZE);

    // Place a transfer request for every objects
    for (i = 0; i < OBJ_NUM; ++i) {
        for (j = 0; j < REQ_SIZE; ++j) {
            offset[0]        = j * (BUF_LEN / REQ_SIZE);
            offset_length[0] = BUF_LEN / REQ_SIZE;
            reg_global       = PDCregion_create(1, offset, offset_length);
            if (reg_global > 0) {
                printf("Create global region\n");
            }
            else {
                printf("Fail to create region @ line  %d!\n", __LINE__);
                ret_value = 1;
            }
            transfer_request[i * REQ_SIZE + j] = PDCregion_transfer_create(
                data[i] + j * BUF_LEN / REQ_SIZE, PDC_WRITE, obj[i], reg, reg_global);
            if (PDCregion_close(reg_global) < 0) {
                printf("fail to close global region @ line %d\n", __LINE__);
                ret_value = 1;
            }
            else {
                printf("successfully closed global region @ line %d\n", __LINE__);
            }
        }
    }

    if (start_method) {
        ret = PDCregion_transfer_start_all(transfer_request, OBJ_NUM * REQ_SIZE);
        if (ret != SUCCEED) {
            printf("Fail to region transfer start @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else {
        for (i = 0; i < OBJ_NUM * REQ_SIZE; ++i) {
            ret = PDCregion_transfer_start(transfer_request[i]);
            if (ret != SUCCEED) {
                printf("Fail to region transfer start @ line %d\n", __LINE__);
                ret_value = 1;
            }
        }
    }
    if (wait_method == 1) {
        ret = PDCregion_transfer_wait_all(transfer_request, OBJ_NUM * REQ_SIZE);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else if (wait_method == 0) {
        pdcid_t *transfer_request_all = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM * REQ_SIZE);
        int      request_size         = 0;
        for (i = 0; i < OBJ_NUM * REQ_SIZE; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        request_size = 0;
        for (i = 1; i < OBJ_NUM * REQ_SIZE; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        free(transfer_request_all);
    }
    for (i = 0; i < OBJ_NUM; ++i) {
        for (j = 0; j < REQ_SIZE; ++j) {
            ret = PDCregion_transfer_close(transfer_request[i * REQ_SIZE + j]);
            if (ret != SUCCEED) {
                printf("Fail to region transfer close @ line %d\n", __LINE__);
                ret_value = 1;
            }
        }
    }

    for (i = 0; i < OBJ_NUM; ++i) {
        if (PDCobj_close(obj[i]) < 0) {
            printf("fail to close object o1 @ line %d\n", __LINE__);
            ret_value = 1;
        }
        else {
            printf("successfully close object o1 @ line %d\n", __LINE__);
        }
    }

    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed local region @ line %d\n", __LINE__);
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
    offset_length[0] = BUF_LEN / REQ_SIZE;
    reg              = PDCregion_create(1, offset, offset_length);

    for (i = 0; i < OBJ_NUM; ++i) {
        memset(data_read[i], 0, sizeof(int) * BUF_LEN);
        for (j = 0; j < REQ_SIZE; ++j) {
            offset[0]        = j * (BUF_LEN / REQ_SIZE);
            offset_length[0] = BUF_LEN / REQ_SIZE;
            reg_global       = PDCregion_create(1, offset, offset_length);
            if (reg_global > 0) {
                printf("Create global region\n");
            }
            else {
                printf("Fail to create region @ line  %d!\n", __LINE__);
                ret_value = 1;
            }
            transfer_request[i * REQ_SIZE + j] = PDCregion_transfer_create(
                data_read[i] + j * BUF_LEN / REQ_SIZE, PDC_READ, obj[i], reg, reg_global);
            if (PDCregion_close(reg_global) < 0) {
                printf("fail to close global region @ line %d\n", __LINE__);
                ret_value = 1;
            }
            else {
                printf("successfully closed global region @ line %d\n", __LINE__);
            }
        }
    }
    if (start_method) {
        ret = PDCregion_transfer_start_all(transfer_request, OBJ_NUM * REQ_SIZE);
        if (ret != SUCCEED) {
            printf("Fail to region transfer start @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else {
        for (i = 0; i < OBJ_NUM * REQ_SIZE; ++i) {
            ret = PDCregion_transfer_start(transfer_request[i]);
            if (ret != SUCCEED) {
                printf("Fail to region transfer start @ line %d\n", __LINE__);
                ret_value = 1;
            }
        }
    }
    if (wait_method == 1) {
        ret = PDCregion_transfer_wait_all(transfer_request, OBJ_NUM * REQ_SIZE);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else if (wait_method == 0) {
        pdcid_t *transfer_request_all = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM * REQ_SIZE);
        int      request_size         = 0;
        for (i = 0; i < OBJ_NUM * REQ_SIZE; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        request_size = 0;
        for (i = 1; i < OBJ_NUM * REQ_SIZE; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        free(transfer_request_all);
    }
    for (i = 0; i < OBJ_NUM; ++i) {
        for (j = 0; j < REQ_SIZE; ++j) {
            ret = PDCregion_transfer_close(transfer_request[i * REQ_SIZE + j]);
            if (ret != SUCCEED) {
                printf("Fail to region transfer close @ line %d\n", __LINE__);
                ret_value = 1;
            }
        }
    }

    // close object
    for (i = 0; i < OBJ_NUM; ++i) {
        if (PDCobj_close(obj[i]) < 0) {
            printf("fail to close object o1 @ line %d\n", __LINE__);
            ret_value = 1;
        }
        else {
            printf("successfully close object o1 @ line %d\n", __LINE__);
        }
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
    if (reg > 0) {
        printf("Create local region\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg_global       = PDCregion_create(1, offset, offset_length);
    if (reg_global > 0) {
        printf("Create global region\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    for (i = 0; i < OBJ_NUM; ++i) {
        memset(data_read[i], 0, sizeof(int) * BUF_LEN);
        transfer_request[i] = PDCregion_transfer_create(data_read[i], PDC_READ, obj[i], reg, reg_global);
    }
    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed global region @ line %d\n", __LINE__);
    }

    if (start_method) {
        ret = PDCregion_transfer_start_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED) {
            printf("Fail to region transfer start @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else {
        for (i = 0; i < OBJ_NUM; ++i) {
            ret = PDCregion_transfer_start(transfer_request[i]);
            if (ret != SUCCEED) {
                printf("Fail to region transfer start @ line %d\n", __LINE__);
                ret_value = 1;
            }
        }
    }
    if (wait_method == 1) {
        ret = PDCregion_transfer_wait_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else if (wait_method == 0) {
        pdcid_t *transfer_request_all = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
        int      request_size         = 0;
        for (i = 0; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        request_size = 0;
        for (i = 1; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        free(transfer_request_all);
    }
    for (i = 0; i < OBJ_NUM; ++i) {
        ret = PDCregion_transfer_close(transfer_request[i]);
        if (ret != SUCCEED) {
            printf("Fail to region transfer close @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }

    // close object
    for (i = 0; i < OBJ_NUM; ++i) {
        if (PDCobj_close(obj[i]) < 0) {
            printf("fail to close object o1 @ line %d\n", __LINE__);
            ret_value = 1;
        }
        else {
            printf("successfully close object o1 @ line %d\n", __LINE__);
        }
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

    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully local region @ line %d\n", __LINE__);
    }

    // Now we rewrite the whole object and check its values.
    // open object
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

    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i) {
            data[j][i] = i + 84441111 * j + 3;
        }
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg              = PDCregion_create(1, offset, offset_length);
    if (reg > 0) {
        printf("Create local region\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    reg_global = PDCregion_create(1, offset, offset_length);
    if (reg_global > 0) {
        printf("Create global region\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    for (i = 0; i < OBJ_NUM; ++i) {
        transfer_request[i] = PDCregion_transfer_create(data[i], PDC_WRITE, obj[i], reg, reg_global);
    }

    if (start_method) {
        ret = PDCregion_transfer_start_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED) {
            printf("Fail to region transfer start @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else {
        for (i = 0; i < OBJ_NUM; ++i) {
            ret = PDCregion_transfer_start(transfer_request[i]);
            if (ret != SUCCEED) {
                printf("Fail to region transfer start @ line %d\n", __LINE__);
                ret_value = 1;
            }
        }
    }
    if (wait_method == 1) {
        ret = PDCregion_transfer_wait_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else if (wait_method == 0) {
        pdcid_t *transfer_request_all = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
        int      request_size         = 0;
        for (i = 0; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        request_size = 0;
        for (i = 1; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        free(transfer_request_all);
    }
    for (i = 0; i < OBJ_NUM; ++i) {
        ret = PDCregion_transfer_close(transfer_request[i]);
        if (ret != SUCCEED) {
            printf("Fail to region transfer close @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }

    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully local region @ line %d\n", __LINE__);
    }

    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully global region @ line %d\n", __LINE__);
    }

    // close object
    for (i = 0; i < OBJ_NUM; ++i) {
        if (PDCobj_close(obj[i]) < 0) {
            printf("fail to close object o1 @ line %d\n", __LINE__);
            ret_value = 1;
        }
        else {
            printf("successfully close object o1 @ line %d\n", __LINE__);
        }
    }

    // open object
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
    if (reg > 0) {
        printf("Create local region\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    reg_global = PDCregion_create(1, offset, offset_length);
    if (reg_global > 0) {
        printf("Create global region\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    for (i = 0; i < OBJ_NUM; ++i) {
        transfer_request[i] = PDCregion_transfer_create(data_read[i], PDC_READ, obj[i], reg, reg_global);
    }

    if (start_method) {
        ret = PDCregion_transfer_start_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED) {
            printf("Fail to region transfer start @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else {
        for (i = 0; i < OBJ_NUM; ++i) {
            ret = PDCregion_transfer_start(transfer_request[i]);
            if (ret != SUCCEED) {
                printf("Fail to region transfer start @ line %d\n", __LINE__);
                ret_value = 1;
            }
        }
    }
    if (wait_method == 1) {
        ret = PDCregion_transfer_wait_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }
    else if (wait_method == 0) {
        pdcid_t *transfer_request_all = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
        int      request_size         = 0;
        for (i = 0; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        request_size = 0;
        for (i = 1; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED) {
            printf("Fail to region transfer wait @ line %d\n", __LINE__);
            ret_value = 1;
        }
        free(transfer_request_all);
    }
    for (i = 0; i < OBJ_NUM; ++i) {
        ret = PDCregion_transfer_close(transfer_request[i]);
        if (ret != SUCCEED) {
            printf("Fail to region transfer close @ line %d\n", __LINE__);
            ret_value = 1;
        }
    }

    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully local region @ line %d\n", __LINE__);
    }

    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully global region @ line %d\n", __LINE__);
    }

    // close object
    for (i = 0; i < OBJ_NUM; ++i) {
        if (PDCobj_close(obj[i]) < 0) {
            printf("fail to close object o1 @ line %d\n", __LINE__);
            ret_value = 1;
        }
        else {
            printf("successfully close object o1 @ line %d\n", __LINE__);
        }
    }

    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i) {
            if (data_read[j][i] != i + 84441111 * j + 3) {
                printf("wrong value %d!=%d @ line %d\n", data_read[j][i], 84441111 * j + 3, __LINE__);
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
