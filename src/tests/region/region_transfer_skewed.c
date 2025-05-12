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
    int ret_value = 0;

    uint64_t offset[1], offset_length[1];
    uint64_t dims[1];

    int *data      = (int *)malloc(sizeof(int) * BUF_LEN);
    int *data_read = (int *)malloc(sizeof(int) * BUF_LEN);
    dims[0]        = BUF_LEN;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    pdc = PDCinit("pdc");
    LOG_INFO("create a new pdc\n");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop > 0) {
        LOG_INFO("Create a container property");
    }
    else {
        LOG_ERROR("Failed to create container property");
        ret_value = 1;
    }
    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);
    if (cont > 0) {
        LOG_INFO("Create a container c1");
    }
    else {
        LOG_ERROR("Failed to create container");
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop > 0) {
        LOG_INFO("Create an object property");
    }
    else {
        LOG_ERROR("Failed to create object property");
        ret_value = 1;
    }

    ret = PDCprop_set_obj_type(obj_prop, PDC_INT);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to set obj type");
        ret_value = 1;
    }
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    obj1 = PDCobj_create(cont, obj_name1, obj_prop);
    if (obj1 > 0) {
        LOG_INFO("Create an object o1");
    }
    else {
        LOG_ERROR("Failed to create object");
        ret_value = 1;
    }
    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_create(cont, obj_name2, obj_prop);
    if (obj2 > 0) {
        LOG_INFO("Create an object o2");
    }
    else {
        LOG_ERROR("Failed to create object");
        ret_value = 1;
    }

    offset[0]        = 1;
    offset_length[0] = BUF_LEN / 2;
    reg              = PDCregion_create(1, offset, offset_length);
    offset[0]        = BUF_LEN / 4;
    offset_length[0] = BUF_LEN / 2;
    reg_global       = PDCregion_create(1, offset, offset_length);

    for (i = 0; i < BUF_LEN; ++i) {
        data[i] = i + 77;
    }
    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global);

    PDCregion_transfer_start(transfer_request);
    PDCregion_transfer_wait(transfer_request);

    PDCregion_transfer_close(transfer_request);

    if (PDCregion_close(reg) < 0) {
        LOG_ERROR("Failed to close local region");
        ret_value = 1;
    }
    else {
        LOG_INFO("successfully closed local region");
    }

    if (PDCregion_close(reg_global) < 0) {
        LOG_ERROR("Failed to close global region");
        ret_value = 1;
    }
    else {
        LOG_INFO("successfully closed global region");
    }

    offset[0]        = BUF_LEN / 2;
    offset_length[0] = BUF_LEN / 2;
    reg              = PDCregion_create(1, offset, offset_length);
    offset[0]        = BUF_LEN / 4;
    offset_length[0] = BUF_LEN / 2;
    reg_global       = PDCregion_create(1, offset, offset_length);

    transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj1, reg, reg_global);

    PDCregion_transfer_start(transfer_request);
    PDCregion_transfer_wait(transfer_request);

    PDCregion_transfer_close(transfer_request);

    // Check if data written previously has been correctly read.
    for (i = 0; i < BUF_LEN / 2; ++i) {
        if (data_read[i + BUF_LEN / 2] != i + 77 + 1) {
            LOG_ERROR("wrong value %d!=%d\n", data_read[i + BUF_LEN / 2], i + 77 + 1);
            ret_value = 1;
            break;
        }
    }
    if (PDCregion_close(reg) < 0) {
        LOG_ERROR("Failed to close local region");
        ret_value = 1;
    }
    else {
        LOG_INFO("successfully local region");
    }

    if (PDCregion_close(reg_global) < 0) {
        LOG_ERROR("Failed to close global region");
        ret_value = 1;
    }
    else {
        LOG_INFO("successfully closed global region");
    }

    // close object
    if (PDCobj_close(obj1) < 0) {
        LOG_ERROR("Failed to close object o1");
        ret_value = 1;
    }
    else {
        LOG_INFO("Successfully closed object o1");
    }
    if (PDCobj_close(obj2) < 0) {
        LOG_ERROR("Failed to close object o2");
        ret_value = 1;
    }
    else {
        LOG_INFO("Successfully closed object o2");
    }
    // close a container
    if (PDCcont_close(cont) < 0) {
        LOG_ERROR("Failed to close container c1");
        ret_value = 1;
    }
    else {
        LOG_INFO("Successfully closed container c1");
    }
    // close a object property
    if (PDCprop_close(obj_prop) < 0) {
        LOG_ERROR("Failed to close property");
        ret_value = 1;
    }
    else {
        LOG_INFO("Successfully closed object property");
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        LOG_ERROR("Failed to close property");
        ret_value = 1;
    }
    else {
        LOG_INFO("Successfully closed container property");
    }
    free(data);
    free(data_read);
    // close pdc
    if (PDCclose(pdc) < 0) {
        LOG_ERROR("Failed to close PDC");
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
