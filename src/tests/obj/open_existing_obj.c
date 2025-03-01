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
    pdcid_t pdc;
    pdcid_t open11, open12, open21;
    int     rank = 0, size = 1;
    int     ret_value = 0;
    char    obj_name1[128], obj_name2[128];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    // create a pdc
    pdc = PDCinit("pdc");
    printf("create a new pdc\n");

    sprintf(obj_name1, "o1_%d", rank);
    sprintf(obj_name2, "o2_%d", rank);
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
    if (PDCobj_close(open21) < 0) {
        printf("Rank %d fail to close object open21\n", rank);
        ret_value = 1;
    }
    else {
        printf("Rank %d successfully close object open21\n", rank);
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
