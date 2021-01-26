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


int main(int argc, char **argv) {
    pdcid_t pdc, cont_prop, cont, obj_prop;
    perr_t ret;
    pdcid_t obj1, obj2;

    int rank = 0, size = 1;
    int ret_value = 0;

    size_t ndim = 3;
    uint64_t dims[3];
    dims[0] = 64;
    dims[1] = 3;
    dims[2] = 4;
    double *data = (double*)malloc(sizeof(double)*128);
    memset(data, 1, 128 * sizeof(double));

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
    if(cont_prop > 0) {
        printf("Create a container property\n");
    } else {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if(cont > 0) {
        printf("Create a container c1\n");
    } else {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    obj1 = PDCobj_put_data("o1", (void*)data, 16*sizeof(double), cont);
    if(obj1 > 0) {
        printf("Put data to o1\n");
    } else {
        printf("Fail to put data into object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    obj2 = PDCobj_put_data("o2", (void*)data, 128*sizeof(double), cont);
    if(obj2 > 0) {
        printf("Put data to o2\n");
    } else {
        printf("Fail to put data into object @ line  %d!\n", __LINE__);
        ret_value = 1;
    }

    // close object
    if(PDCobj_close(obj1) < 0) {
        printf("fail to close object o1\n");
        ret_value = 1;
    } else {
        printf("successfully close object o1\n");
    }
    if(PDCobj_close(obj2) < 0) {
        printf("fail to close object o2\n");
        ret_value = 1;
    } else {
        printf("successfully close object o2\n");
    }

    // close a container
    if(PDCcont_close(cont) < 0) {
        printf("fail to close container c1\n");
        ret_value = 1;
    } else {
        printf("successfully close container c1\n");
    }
    // close a container property
    if(PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    } else {
        printf("successfully close container property\n");
    }
    // close pdc
    if(PDCclose(pdc) < 0) {
        printf("fail to close PDC\n");
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
