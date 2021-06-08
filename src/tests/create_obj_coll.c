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
    pdcid_t obj1, obj2, open11, open12, open21;
    int     rank = 0, size = 1;
    int     ret_value = 0;
    char    cont_name[128], obj_name1[128], obj_name2[128];

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
        printf("Rank %d Create a container property\n", rank);
    }
    else {
        printf("Rank %d Fail to create container property @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    // create a container
    sprintf(cont_name, "c%d", rank);
    cont = PDCcont_create(cont_name, cont_prop);
    if (cont > 0) {
        printf("Rank %d Create a container c1\n", rank);
    }
    else {
        printf("Rank %d Fail to create container @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop > 0) {
        printf("Rank %d Create an object property\n", rank);
    }
    else {
        printf("Rank %d Fail to create object property @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    // create first object
    sprintf(obj_name1, "o1");
    obj1 = PDCobj_create_mpi(cont, obj_name1, obj_prop, 0, MPI_COMM_WORLD);
    if (obj1 > 0) {
        printf("Rank %d Create an object o1\n", rank);
    }
    else {
        printf("Rank %d Fail to create object @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    printf("checkpoint 1 rank %d\n", rank);
    // create second object
    sprintf(obj_name2, "o2");
    obj2 = PDCobj_create_mpi(cont, obj_name2, obj_prop, 0, MPI_COMM_WORLD);
    if (obj2 > 0) {
        printf("Rank %d Create an object o2\n", rank);
    }
    else {
        printf("Rank %d Fail to create object @ line  %d!\n", rank, __LINE__);
        ret_value = 1;
    }
    printf("checkpoint 2 rank %d\n", rank);
    // open first object twice
    open11 = PDCobj_open(obj_name1, pdc);
    if (open11 == 0) {
        printf("Rank %d Fail to open object o1\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d Open object o1\n", rank);
    }
    open12 = PDCobj_open(obj_name1, pdc);
    if (open12 == 0) {
        printf("Rank %d Fail to open object o1\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d Open object o1\n", rank);
    }
    // open second object once
    open21 = PDCobj_open(obj_name2, pdc);
    if (open21 == 0) {
        printf("Rank %d Fail to open object o2\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d Open object o2\n", rank);
    }
    // close object
    if (PDCobj_close(obj1) < 0) {
        printf("Rank %d fail to close object o1\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object o1\n", rank);
    }
    if (PDCobj_close(open11) < 0) {
        printf("Rank %d fail to close object open11\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object open11\n", rank);
    }
    if (PDCobj_close(open12) < 0) {
        printf("Rank %d fail to close object open12\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object open12\n", rank);
    }
    if (PDCobj_close(obj2) < 0) {
        printf("Rank %d fail to close object o2\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object o2\n", rank);
    }
    if (PDCobj_close(open21) < 0) {
        printf("Rank %d fail to close object open21\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object open21\n", rank);
    }
    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("Rank %d fail to close container c1\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close container c1\n", rank);
    }
    // close a object property
    if (PDCprop_close(obj_prop) < 0) {
        printf("Rank %d Fail to close property @ line %d\n", rank, __LINE__);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object property\n", rank);
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Rank %d Fail to close property @ line %d\n", rank, __LINE__);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close container property\n", rank);
    }
    // close pdc
    if (PDCclose(pdc) < 0) {
        printf("Rank %d fail to close PDC\n", rank);
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
