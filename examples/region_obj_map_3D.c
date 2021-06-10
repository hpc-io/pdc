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

    int rank = 0, size = 1, i;
    int ret_value = 0;

    uint64_t offset[3], offset_length[3];
    uint64_t dims[3];
    offset[0]        = 0;
    offset[1]        = 0;
    offset[2]        = 0;
    offset_length[0] = BUF_LEN / 4;
    offset_length[1] = 2;
    offset_length[2] = 2;

    int *data      = (int *)malloc(sizeof(int) * BUF_LEN);
    int *data_read = (int *)malloc(sizeof(int) * BUF_LEN);
    int *obj_data  = (int *)calloc(BUF_LEN, sizeof(int));
    dims[0]        = BUF_LEN / 4;
    dims[1]        = 2;
    dims[2]        = 2;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
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
    PDCprop_set_obj_buf(obj_prop, obj_data);
    PDCprop_set_obj_dims(obj_prop, 3, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    obj1 = PDCobj_create(cont, obj_name1, obj_prop);
    if (obj1 > 0) {
        printf("Create an object o1\n");
    }
    else {
        printf("Fail to create object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_create(cont, obj_name2, obj_prop);
    if (obj2 > 0) {
        printf("Create an object o2\n");
    }
    else {
        printf("Fail to create object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    // reg = PDCregion_create(1, offset, offset_length);
    // reg_global = PDCregion_create(1, offset, offset_length);
    reg        = PDCregion_create(3, offset, offset_length);
    reg_global = PDCregion_create(3, offset, offset_length);

    for (i = 0; i < BUF_LEN; ++i) {
        data[i] = i;
    }
    ret = PDCbuf_obj_map(data, PDC_INT, reg, obj1, reg_global);
    if (ret != SUCCEED) {
        printf("PDCbuf_obj_map failed\n");
        ret_value = 1;
    }

    ret = PDCreg_obtain_lock(obj1, reg_global, PDC_WRITE, PDC_BLOCK);
    if (ret != SUCCEED) {
        printf("PDCreg_obtain_lock failed\n");
        exit(-1);
    }

    ret = PDCreg_release_lock(obj1, reg_global, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("PDCreg_release_lock failed\n");
        ret_value = 1;
    }

    ret = PDCbuf_obj_unmap(obj1, reg_global);
    if (ret != SUCCEED) {
        printf("PDCbuf_obj_unmap failed\n");
        ret_value = 1;
    }

    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region\n");
        ret_value = 1;
    }
    else {
        printf("successfully local region\n");
    }

    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region\n");
        ret_value = 1;
    }
    else {
        printf("successfully global region\n");
    }

    reg        = PDCregion_create(3, offset, offset_length);
    reg_global = PDCregion_create(3, offset, offset_length);

    ret = PDCbuf_obj_map(data_read, PDC_INT, reg, obj1, reg_global);
    if (ret != SUCCEED) {
        printf("PDCbuf_obj_map failed\n");
        ret_value = 1;
    }

    ret = PDCreg_obtain_lock(obj1, reg_global, PDC_READ, PDC_BLOCK);
    if (ret != SUCCEED) {
        printf("PDCreg_obtain_lock failed\n");
        ret_value = 1;
    }

    ret = PDCreg_release_lock(obj1, reg_global, PDC_READ);
    if (ret != SUCCEED) {
        printf("PDCreg_release_lock failed\n");
        ret_value = 1;
    }

    ret = PDCbuf_obj_unmap(obj1, reg_global);
    if (ret != SUCCEED) {
        printf("PDCbuf_obj_unmap failed\n");
        ret_value = 1;
    }

    for (i = 0; i < BUF_LEN; ++i) {
        if (data_read[i] != i) {
            printf("wrong value %d!=%d\n", data_read[i], i);
            ret_value = 1;
            break;
        }
    }

    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region\n");
        ret_value = 1;
    }
    else {
        printf("successfully local region\n");
    }

    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region\n");
        ret_value = 1;
    }
    else {
        printf("successfully global region\n");
    }

    // close object
    if (PDCobj_close(obj1) < 0) {
        printf("fail to close object o1\n");
        ret_value = 1;
    }
    else {
        printf("successfully close object o1\n");
    }
    if (PDCobj_close(obj2) < 0) {
        printf("fail to close object o2\n");
        ret_value = 1;
    }
    else {
        printf("successfully close object o2\n");
    }
    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container c1\n");
        ret_value = 1;
    }
    else {
        printf("successfully close container c1\n");
    }
    // close a object property
    if (PDCprop_close(obj_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close object property\n");
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close container property\n");
    }
    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("fail to close PDC\n");
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
