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
#include "pdc.h"

int
main(int argc, char **argv)
{
    pdcid_t pdc, cont_prop, cont, obj_prop;
    perr_t  ret;
    pdcid_t obj1, obj2;
    int     ret_value = 0;

    int rank = 0, size = 1;

    size_t   ndim = 3;
    uint64_t dims[3];
    dims[0] = 64;
    dims[1] = 3;
    dims[2] = 4;
    char    tag_value[128], tag_value2[128], *tag_value_ret;
    char    cont_name[128], obj_name1[128], obj_name2[128];
    psize_t value_size;

    strcpy(tag_value, "some tag value");
    strcpy(tag_value2, "some tag value 2 is longer");

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

    ret = PDCprop_set_obj_dims(obj_prop, ndim, dims);
    if (ret != SUCCEED) {
        printf("Fail to set obj dim @ line %d\n", __LINE__);
        ret_value = 1;
    }
    ret = PDCprop_set_obj_type(obj_prop, PDC_DOUBLE);
    if (ret != SUCCEED) {
        printf("Fail to set obj type @ line %d\n", __LINE__);
        ret_value = 1;
    }

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

    ret = PDCobj_put_tag(obj1, "some tag", tag_value, strlen(tag_value) + 1);
    if (ret != SUCCEED) {
        printf("Put tag failed at object 1\n");
        ret_value = 1;
    }
    ret = PDCobj_put_tag(obj1, "some tag 2", tag_value2, strlen(tag_value2) + 1);
    if (ret != SUCCEED) {
        printf("Put tag failed at object 1\n");
        ret_value = 1;
    }

    ret = PDCobj_put_tag(obj2, "some tag", tag_value, strlen(tag_value) + 1);
    if (ret != SUCCEED) {
        printf("Put tag failed at object 2\n");
        ret_value = 1;
    }

    ret = PDCobj_put_tag(obj2, "some tag 2", tag_value2, strlen(tag_value2) + 1);
    if (ret != SUCCEED) {
        printf("Put tag failed at object 2\n");
        ret_value = 1;
    }

    ret = PDCobj_get_tag(obj1, "some tag", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at object 1\n");
        ret_value = 1;
    }

    if (strcmp(tag_value, tag_value_ret) != 0) {
        printf("Wrong tag value at object 1, expected = %s, get %s\n", tag_value, tag_value_ret);
        ret_value = 1;
    }

    ret = PDCobj_get_tag(obj1, "some tag 2", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at object 1\n");
        ret_value = 1;
    }

    if (strcmp(tag_value2, tag_value_ret) != 0) {
        printf("Wrong tag value 2 at object 1, expected = %s, get %s\n", tag_value2, tag_value_ret);
        ret_value = 1;
    }

    ret = PDCobj_get_tag(obj2, "some tag", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at object 2\n");
        ret_value = 1;
    }

    if (strcmp(tag_value, tag_value_ret) != 0) {
        printf("Wrong tag value at object 2, expected = %s, get %s\n", tag_value, tag_value_ret);
        ret_value = 1;
    }

    ret = PDCobj_get_tag(obj2, "some tag 2", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at object 2\n");
        ret_value = 1;
    }

    if (strcmp(tag_value2, tag_value_ret) != 0) {
        printf("Wrong tag value 2 at object 2, expected = %s, get %s\n", tag_value2, tag_value_ret);
        ret_value = 1;
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
