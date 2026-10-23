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

#define BUF_LEN 256
#define OBJ_NUM 10

#define BATCH_REQUESTS      1
#define INDIVIDUAL_REQUESTS 0

#define TRANSFORM_GRAPH_PATH TF_GRAPHS_DIR "zfp.json"

int
main(int argc, char **argv)
{
    pdcid_t  pdc, cont_prop, cont, obj_prop, reg, reg_global;
    perr_t   ret;
    pdcid_t *obj;
    pdcid_t *dg_ids;
    char     cont_name[128], obj_name[128];
    pdcid_t *transfer_request;

    int   rank = 0, size = 1, i, j;
    int   ret_value = 0;
    int **data, **data_read;
    int   start_method = BATCH_REQUESTS;
    int   wait_method  = BATCH_REQUESTS;

    uint64_t offset[1], offset_length[1];
    uint64_t dims[1];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (argc >= 2)
        start_method = atoi(argv[1]);
    if (argc >= 3)
        wait_method = atoi(argv[2]);
    if (!rank) {
        LOG_INFO("Start method: %s\n", (start_method) ? "batch" : "individual");
        LOG_INFO("Wait method: %s\n", (start_method) ? "batch" : "individual");
    }

    data         = (int **)malloc(sizeof(int *) * OBJ_NUM);
    data_read    = (int **)malloc(sizeof(int *) * OBJ_NUM);
    data[0]      = (int *)malloc(sizeof(int) * BUF_LEN * OBJ_NUM);
    data_read[0] = (int *)malloc(sizeof(int) * BUF_LEN * OBJ_NUM);

    for (i = 1; i < OBJ_NUM; ++i) {
        data[i]      = data[i - 1] + BUF_LEN;
        data_read[i] = data_read[i - 1] + BUF_LEN;
    }

    dims[0] = BUF_LEN;

    // create a pdc
    pdc = PDCinit("pdc");
    LOG_INFO("Created a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop > 0)
        LOG_INFO("Created container property\n");
    else
        TGOTO_ERROR(FAIL, "Failed to create container property");

    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);
    if (cont > 0)
        LOG_INFO("Created container c1\n");
    else
        TGOTO_ERROR(TFAIL, "Failed to create container");
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop > 0)
        LOG_INFO("Created an object property\n");
    else {
        LOG_ERROR("Failed to create object property\n");
        ret_value = 1;
    }

    ret = PDCprop_set_obj_type(obj_prop, PDC_INT);
    if (ret != SUCCEED)
        TGOTO_ERROR(TFAIL, "Failed to set obj type\n");
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // create many objects
    obj    = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
    dg_ids = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
    for (i = 0; i < OBJ_NUM; ++i) {
        TASSERT(PDCprop_set_obj_transfer_region_type(obj_prop, PDC_OBJ_STATIC) >= 0,
                "Call to PDCprop_set_obj_transfer_region_type succeeded",
                "Call to PDCprop_set_obj_transfer_region_type failed");
        if (ret != SUCCEED)
            TGOTO_ERROR(TFAIL, "Failed to set obj type");

        sprintf(obj_name, "o%d_%d", i, rank);
        obj[i] = PDCobj_create(cont, obj_name, obj_prop);
        if (obj[i] > 0)
            LOG_INFO("Created an object o1\n");
        else
            TGOTO_ERROR(TFAIL, "Failed to create object");
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg              = PDCregion_create(1, offset, offset_length);
    if (reg > 0)
        LOG_INFO("Create local region\n");
    else
        TGOTO_ERROR(TFAIL, "Failed to create region\n");

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg_global       = PDCregion_create(1, offset, offset_length);
    if (reg_global > 0)
        LOG_INFO("Created a global region\n");
    else
        TGOTO_ERROR(TFAIL, "Failed to create region");

    for (j = 0; j < OBJ_NUM; ++j) {
        for (i = 0; i < BUF_LEN; ++i)
            data[j][i] = i;
    }
    transfer_request = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);

    // Place a transfer request for every objects
    for (i = 0; i < OBJ_NUM; ++i) {
        dg_ids[i] = PDCtf_dg_json_create(TRANSFORM_GRAPH_PATH);
        PDCtf_attach_to_region(dg_ids[i], obj[i], reg_global, "decompressed", "compressed");
        transfer_request[i] = PDCregion_transfer_create(data[i], PDC_WRITE, obj[i], reg, reg_global);
    }
    if (start_method) {
        ret = PDCregion_transfer_start_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED)
            TGOTO_ERROR(TFAIL, "Failed to region transfer start");
    }
    else {
        for (i = 0; i < OBJ_NUM; ++i) {
            ret = PDCregion_transfer_start(transfer_request[i]);
            if (ret != SUCCEED)
                TGOTO_ERROR(TFAIL, "Failed to region transfer start");
        }
    }
    if (wait_method == 1) {
        ret = PDCregion_transfer_wait_all(transfer_request, OBJ_NUM);
        if (ret != SUCCEED)
            TGOTO_ERROR(TFAIL, "Failed to region transfer wait");
    }
    else if (wait_method == 0) {
        pdcid_t *transfer_request_all = (pdcid_t *)malloc(sizeof(pdcid_t) * OBJ_NUM);
        int      request_size         = 0;
        for (i = 0; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED)
            TGOTO_ERROR(TFAIL, "Failed to region transfer wait");
        request_size = 0;
        for (i = 1; i < OBJ_NUM; i += 2) {
            transfer_request_all[request_size] = transfer_request[i];
            request_size++;
        }
        ret = PDCregion_transfer_wait_all(transfer_request_all, request_size);
        if (ret != SUCCEED)
            TGOTO_ERROR(TFAIL, "Failed to region transfer wait");
        free(transfer_request_all);
    }
    for (i = 0; i < OBJ_NUM; ++i) {
        PDCtf_close_dg(dg_ids[i]);
        ret = PDCregion_transfer_close(transfer_request[i]);
        if (ret != SUCCEED)
            TGOTO_ERROR(TFAIL, "Failed to region transfer close");
    }
    if (PDCregion_close(reg) < 0)
        TGOTO_ERROR(TFAIL, "Failed to close local region");
    else
        LOG_INFO("Successfully closed local region\n");

    if (PDCregion_close(reg_global) < 0)
        TGOTO_ERROR(TFAIL, "Failed to close global region\n");
    else
        LOG_INFO("Successfully closed global region\n");

done:
    free(data[0]);
    free(data_read[0]);
    free(data);
    free(data_read);
    free(obj);
    free(transfer_request);
    // close pdc
    if (PDCclose(pdc) < 0)
        TGOTO_ERROR(TFAIL, "Failed to close PDC");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
