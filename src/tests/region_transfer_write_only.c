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
    pdcid_t obj1, obj2, obj3;
    char    cont_name[128], obj_name1[128], obj_name2[128], obj_name3[128];
    pdcid_t transfer_request;

    int rank = 0, size = 1, i;
    int ret_value = 0;

    uint64_t offset[3], offset_length[3];
    uint64_t dims[3];

    int *data = (int *)malloc(sizeof(int) * BUF_LEN);
    dims[0]   = BUF_LEN;

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
        LOG_INFO("Create a container property\n");
    }
    else {
        LOG_ERROR("Failed to create container property");
        ret_value = 1;
    }
    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);
    if (cont > 0) {
        LOG_INFO("Create a container c1\n");
    }
    else {
        LOG_ERROR("Failed to create container");
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop > 0) {
        LOG_INFO("Create an object property\n");
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
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // create first object
    dims[0] = BUF_LEN;
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    sprintf(obj_name1, "o1_%d", rank);
    obj1 = PDCobj_create(cont, obj_name1, obj_prop);
    if (obj1 > 0) {
        LOG_INFO("Create an object o1\n");
    }
    else {
        LOG_ERROR("Failed to create object");
        ret_value = 1;
    }
    // create second object
    dims[0] = BUF_LEN / 4;
    dims[1] = 4;
    PDCprop_set_obj_dims(obj_prop, 2, dims);
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_create(cont, obj_name2, obj_prop);
    if (obj2 > 0) {
        LOG_INFO("Create an object o2\n");
    }
    else {
        LOG_ERROR("Failed to create object");
        ret_value = 1;
    }
    // create third object
    dims[0] = BUF_LEN / 4;
    dims[1] = 2;
    dims[2] = 2;
    PDCprop_set_obj_dims(obj_prop, 3, dims);
    sprintf(obj_name3, "o3_%d", rank);
    obj3 = PDCobj_create(cont, obj_name3, obj_prop);
    if (obj3 > 0) {
        LOG_INFO("Create an object o3\n");
    }
    else {
        LOG_ERROR("Failed to create object");
        ret_value = 1;
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg              = PDCregion_create(1, offset, offset_length);
    if (reg > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    reg_global = PDCregion_create(1, offset, offset_length);
    if (reg_global > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    for (i = 0; i < BUF_LEN; ++i) {
        data[i] = i;
    }

    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer start");
        ret_value = 1;
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer wait");
        ret_value = 1;
    }
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer close");
        ret_value = 1;
    }
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

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg              = PDCregion_create(1, offset, offset_length);
    if (reg > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    reg_global = PDCregion_create(1, offset, offset_length);
    if (reg_global > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    for (i = 0; i < BUF_LEN; ++i) {
        data[i] = i;
    }

    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer start");
        ret_value = 1;
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer wait");
        ret_value = 1;
    }
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer close");
        ret_value = 1;
    }
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
    // Write the second object
    offset[0]        = 0;
    offset_length[0] = BUF_LEN / 4;
    offset[1]        = 0;
    offset_length[1] = 4;

    reg = PDCregion_create(2, offset, offset_length);
    if (reg > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    reg_global = PDCregion_create(2, offset, offset_length);
    if (reg_global > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    for (i = 0; i < BUF_LEN; ++i) {
        data[i] = i;
    }

    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj2, reg, reg_global);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer start");
        ret_value = 1;
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer wait");
        ret_value = 1;
    }
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer close");
        ret_value = 1;
    }
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

    // Write the third object
    offset[0]        = 0;
    offset_length[0] = BUF_LEN / 4;
    offset[1]        = 0;
    offset_length[1] = 2;
    offset[2]        = 0;
    offset_length[2] = 2;
    reg              = PDCregion_create(3, offset, offset_length);
    if (reg > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    reg_global = PDCregion_create(3, offset, offset_length);
    if (reg_global > 0) {
        LOG_INFO("Create an region o1\n");
    }
    else {
        LOG_ERROR("Failed to create region");
        ret_value = 1;
    }
    for (i = 0; i < BUF_LEN; ++i) {
        data[i] = i;
    }

    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj3, reg, reg_global);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer start");
        ret_value = 1;
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer wait");
        ret_value = 1;
    }
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        LOG_ERROR("Failed to region transfer close");
        ret_value = 1;
    }
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
    if (PDCobj_close(obj3) < 0) {
        LOG_ERROR("Failed to close object o3");
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
