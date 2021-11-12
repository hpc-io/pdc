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
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"
#define BUF_LEN 128

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont, reg, reg_global;
    perr_t  ret;
    pdcid_t obj1, obj2, obj3;
    char    cont_name[128], obj_name1[128], obj_name2[128], obj_name3[128];
    pdcid_t transfer_request;

    int rank = 0, size = 1, i;
    int ret_value = 0;

    uint64_t offset[3], offset_length[3];

    int *data_read = (int *)malloc(sizeof(int) * BUF_LEN);

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_open(cont_name, pdc);
    if (cont > 0) {
        printf("Create a container c1\n");
    }
    else {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    obj1 = PDCobj_open(obj_name1, pdc);
    if (obj1 > 0) {
        printf("Create an object o1\n");
    }
    else {
        printf("Fail to create object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create second object
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_open(obj_name2, pdc);
    if (obj2 > 0) {
        printf("Create an object o2\n");
    }
    else {
        printf("Fail to create object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create third object
    sprintf(obj_name3, "o3_%d", rank);
    obj3 = PDCobj_open(obj_name3, pdc);
    if (obj3 > 0) {
        printf("Create an object o3\n");
    }
    else {
        printf("Fail to create object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }


    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg        = PDCregion_create(1, offset, offset_length);
    reg_global = PDCregion_create(1, offset, offset_length);

    transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj1, reg, reg_global);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer start @ line %d\n", __LINE__);
        ret_value = 1;
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer wait @ line %d\n", __LINE__);
        ret_value = 1;
    }
    // Check if data written previously has been correctly read.
    for (i = 0; i < BUF_LEN; ++i) {
        if (data_read[i] != i) {
            printf("wrong value %d!=%d @ line %d\n", data_read[i], i, __LINE__);
            ret_value = 1;
            break;
        }
    }
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer close @ line %d\n", __LINE__);
        ret_value = 1;
    }
    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed local region @ line %d\n", __LINE__);
    }

    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed global region @ line %d\n", __LINE__);
    }

    // Read the second object
    offset[0]        = 0;
    offset_length[0] = BUF_LEN / 4;
    offset[1]        = 0;
    offset_length[1] = 4;
    reg              = PDCregion_create(2, offset, offset_length);
    if (reg > 0) {
        printf("Create an region o1\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    reg_global = PDCregion_create(2, offset, offset_length);
    if (reg_global > 0) {
        printf("Create an region o1\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    memset(data_read, 0, BUF_LEN);
    transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj2, reg, reg_global);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer start @ line %d\n", __LINE__);
        ret_value = 1;
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer wait @ line %d\n", __LINE__);
        ret_value = 1;
    }
    // Check if data written previously has been correctly read.
    for (i = 0; i < BUF_LEN; ++i) {
        if (data_read[i] != i) {
            printf("wrong value %d!=%d @ line %d\n", data_read[i], i, __LINE__);
            ret_value = 1;
            break;
        }
    }
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer close @ line %d\n", __LINE__);
        ret_value = 1;
    }
    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed local region @ line %d\n", __LINE__);
    }

    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed global region @ line %d\n", __LINE__);
    }

    // Read the third object
    offset[0]        = 0;
    offset_length[0] = BUF_LEN / 4;
    offset[1]        = 0;
    offset_length[1] = 2;
    offset[2]        = 0;
    offset_length[2] = 2;
    reg              = PDCregion_create(3, offset, offset_length);
    if (reg > 0) {
        printf("Create an region o1\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    reg_global = PDCregion_create(3, offset, offset_length);
    if (reg_global > 0) {
        printf("Create an region o1\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    memset(data_read, 0, BUF_LEN);
    transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj3, reg, reg_global);

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer start @ line %d\n", __LINE__);
        ret_value = 1;
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer wait @ line %d\n", __LINE__);
        ret_value = 1;
    }
    // Check if data written previously has been correctly read.
    for (i = 0; i < BUF_LEN; ++i) {
        if (data_read[i] != i) {
            printf("wrong value %d!=%d @ line %d\n", data_read[i], i, __LINE__);
            ret_value = 1;
            break;
        }
    }
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer close @ line %d\n", __LINE__);
        ret_value = 1;
    }
    if (PDCregion_close(reg) < 0) {
        printf("fail to close local region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed local region @ line %d\n", __LINE__);
    }

    if (PDCregion_close(reg_global) < 0) {
        printf("fail to close global region @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully closed global region @ line %d\n", __LINE__);
    }
    // close object
    if (PDCobj_close(obj1) < 0) {
        printf("fail to close object o1 @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close object o1 @ line %d\n", __LINE__);
    }
    if (PDCobj_close(obj2) < 0) {
        printf("fail to close object o2 @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close object o2 @ line %d\n", __LINE__);
    }
    if (PDCobj_close(obj3) < 0) {
        printf("fail to close object o3 @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close object o3 @ line %d\n", __LINE__);
    }
    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container c1 @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close container c1 @ line %d\n", __LINE__);
    }
    free(data_read);
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
