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
    pdcid_t pdc, cont_prop, cont;
    pdcid_t obj1, obj2;
    perr_t  error_code;
    char    cont_name[128], obj_name1[128], obj_name2[128];

    int      rank = 0, size = 1;
    unsigned i;
    int      ret_value = 0;

    char *data = (char *)malloc(sizeof(double) * 128);

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
        printf("Rank %d Create a container %s\n", rank, cont_name);
    }
    else {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    memset(data, 1, 128 * sizeof(double));
    sprintf(obj_name1, "o1_%d", rank);
    obj1 = PDCobj_put_data(obj_name1, (void *)data, 16 * sizeof(double), cont);
    if (obj1 > 0) {
        printf("Rank %d Put data to %s\n", rank, obj_name1);
    }
    else {
        printf("Fail to put data into object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    memset(data, 2, 128 * sizeof(double));
    sprintf(obj_name2, "o2_%d", rank);
    obj2 = PDCobj_put_data(obj_name2, (void *)data, 128 * sizeof(double), cont);
    if (obj2 > 0) {
        printf("Rank %d Put data to %s\n", rank, obj_name2);
    }
    else {
        printf("Fail to put data into object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    memset(data, 0, 128 * sizeof(double));
    error_code = PDCobj_get_data(obj1, (void *)(data), 16 * sizeof(double));
    if (error_code != SUCCEED) {
        printf("Fail to get obj 1 data\n");
        ret_value = 1;
    }
    for (i = 0; i < 16 * sizeof(double); ++i) {
        if (data[i] != 1) {
            printf("wrong value at obj 1\n");
            ret_value = 1;
            break;
        }
    }
    memset(data, 0, 128 * sizeof(double));
    error_code = PDCobj_get_data(obj2, (void *)(data), 128 * sizeof(double));
    if (error_code != SUCCEED) {
        printf("Fail to get obj 1 data\n");
        ret_value = 1;
    }
    for (i = 0; i < 128 * sizeof(double); ++i) {
        if (data[i] != 2) {
            printf("wrong value at obj 2\n");
            ret_value = 1;
            break;
        }
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
