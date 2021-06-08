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
    pdcid_t pdc, cont_prop, cont, cont2;
    perr_t  ret;
    int     ret_value = 0;

    int rank = 0, size = 1;

    char    tag_value[128], tag_value2[128], *tag_value_ret;
    psize_t value_size;
    strcpy(tag_value, "some tag value");
    strcpy(tag_value2, "some tag value 2 is longer than tag 1");

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
    cont = PDCcont_create("c1", cont_prop);
    if (cont > 0) {
        printf("Create a container c1\n");
    }
    else {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    cont2 = PDCcont_create("c2", cont_prop);
    if (cont > 0) {
        printf("Create a container c2\n");
    }
    else {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    ret = PDCcont_put_tag(cont, "some tag", tag_value, strlen(tag_value) + 1);

    if (ret != SUCCEED) {
        printf("Put tag failed at container 1\n");
        ret_value = 1;
    }
    ret = PDCcont_put_tag(cont, "some tag 2", tag_value2, strlen(tag_value2) + 1);
    if (ret != SUCCEED) {
        printf("Put tag failed at container 1\n");
        ret_value = 1;
    }

    ret = PDCcont_put_tag(cont2, "some tag", tag_value, strlen(tag_value) + 1);
    if (ret != SUCCEED) {
        printf("Put tag failed at container 2\n");
        ret_value = 1;
    }

    ret = PDCcont_put_tag(cont2, "some tag 2", tag_value2, strlen(tag_value2) + 1);
    if (ret != SUCCEED) {
        printf("Put tag failed at container 2\n");
        ret_value = 1;
    }

    ret = PDCcont_get_tag(cont, "some tag", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at container 1\n");
        ret_value = 1;
    }
    if (strcmp(tag_value, tag_value_ret) != 0) {
        printf("Wrong tag value at container 1, expected = [%s], get [%s]\n", tag_value, tag_value_ret);
        ret_value = 1;
    }

    ret = PDCcont_get_tag(cont, "some tag 2", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at container 1\n");
        ret_value = 1;
    }

    if (strcmp(tag_value2, tag_value_ret) != 0) {
        printf("Wrong tag value at container 1, expected = [%s], get [%s]\n", tag_value2, tag_value_ret);
        ret_value = 1;
    }

    ret = PDCcont_get_tag(cont2, "some tag", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at container 2\n");
        ret_value = 1;
    }

    if (strcmp(tag_value, tag_value_ret) != 0) {
        printf("Wrong tag value at container 2, expected = [%s], get [%s]\n", tag_value, tag_value_ret);
        ret_value = 1;
    }

    ret = PDCcont_get_tag(cont2, "some tag 2", (void **)&tag_value_ret, &value_size);
    if (ret != SUCCEED) {
        printf("Get tag failed at container 2\n");
        ret_value = 1;
    }

    if (strcmp(tag_value2, tag_value_ret) != 0) {
        printf("Wrong tag value at container 2, expected = [%s], get [%s]\n", tag_value2, tag_value_ret);
        ret_value = 1;
    }

    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container c1\n");
        ret_value = 1;
    }
    else {
        printf("successfully close container c1\n");
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
