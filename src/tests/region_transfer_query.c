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
    pdcid_t obj_id;
    char    cont_name[128], obj_name1[128], obj_name2[128];
    pdcid_t transfer_request;

    int rank = 0, size = 1, i;
    int ret_value = 0;

    uint64_t offset[3], offset_length[3], local_offset[1];
    uint64_t dims[1];
    local_offset[0]  = 0;
    offset[0]        = 0;
    offset[1]        = 2;
    offset[2]        = 5;
    offset_length[0] = BUF_LEN;
    offset_length[1] = 3;
    offset_length[2] = 5;

    double *data      = (double *)malloc(sizeof(double) * BUF_LEN);
    double *query_data      = (double *)malloc(sizeof(double) * BUF_LEN);
    dims[0]        = BUF_LEN;

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

    ret = PDCprop_set_obj_type(obj_prop, PDC_DOUBLE);
    if (ret != SUCCEED) {
        printf("Fail to set obj type @ line %d\n", __LINE__);
        ret_value = 1;
    }
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // create first object
    sprintf(obj_name1, "o1_%d", rank);
    obj_id = PDCobj_create(cont, obj_name1, obj_prop);
    if (obj_id > 0) {
        printf("Create an object o1\n");
    }
    else {
        printf("Fail to create object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    offset[0]        = 0;
    offset_length[0] = BUF_LEN;
    reg              = PDCregion_create(1, offset, offset_length);
    if (reg > 0) {
        printf("Create an region o1\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    reg_global = PDCregion_create(1, offset, offset_length);
    if (reg_global > 0) {
        printf("Create an region o1\n");
    }
    else {
        printf("Fail to create region @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    for (i = 0; i < BUF_LEN; ++i) {
        data[i] = i;
    }

    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj_id, reg, reg_global);

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
    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        printf("Fail to region transfer close @ line %d\n", __LINE__);
        ret_value = 1;
    }

    // Query
    pdc_query_t *q, *q0, *q1;
    double lo=1.0, hi=8.0;
    printf("constraint: %f < value < %f\n", lo, hi);
    pdc_selection_t sel;

    q0 = PDCquery_create(obj_id, PDC_GT, PDC_DOUBLE, &lo);
    q1 = PDCquery_create(obj_id, PDC_LT, PDC_DOUBLE, &hi);
    q  = PDCquery_and(q0, q1);

    PDCquery_get_selection(q, &sel);
    printf("Query result:\n");
    PDCselection_print(&sel);

    /* PDCquery_get_data(obj_id, &sel, query_data); */
    /* printf("Data values:\n"); */
    /* for (i = 0; i < sel.nhits; i++) */
    /*     printf("%f \n", query_data[i]); */

    PDCquery_free_all(q);
    PDCselection_free(&sel);

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
    if (PDCobj_close(obj_id) < 0) {
        printf("fail to close object o1 @ line %d\n", __LINE__);
        ret_value = 1;
    }
    else {
        printf("successfully close object o1 @ line %d\n", __LINE__);
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
