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
#include "multidataset_plugin.h"

#define DATA_SIZE 128
#define ARRAY_SIZE 10

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1;
    int     ret_value = 0;
    int     *data;
    int     i;
    char    data_name[1024];

    // create a pdc
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

#ifdef H5_TIMING_ENABLE
    init_timers();
#endif

    init_multidataset();

    data = (int*) malloc(sizeof(int) * DATA_SIZE);
    for ( i = 0; i < DATA_SIZE; ++i ) {
        data[i] = i;
    }
    sprintf(data_name, "data0_%d", rank);

    printf("rank %d C++ example for writing %d lists of arrays with size %d\n", rank, ARRAY_SIZE, DATA_SIZE);
    for ( i = 0; i < ARRAY_SIZE; ++i ) {
        register_multidataset_request_append(data_name, 0, data, sizeof(int) * DATA_SIZE, H5T_NATIVE_CHAR);
    }

    sprintf(data_name, "data1_%d", rank);

    printf("rank %d C++ example for writing %d lists of arrays with size %d\n", rank, ARRAY_SIZE, DATA_SIZE);
    for ( i = 0; i < ARRAY_SIZE; ++i ) {
        register_multidataset_request_append(data_name, 0, data, sizeof(int) * DATA_SIZE, H5T_NATIVE_CHAR);
    }
    flush_multidatasets();

    free(data);

    finalize_multidataset();

#ifdef ENABLE_MPI
    finalize_timers();
#endif

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return ret_value;
}
