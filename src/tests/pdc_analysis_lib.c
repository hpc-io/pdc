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
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>

#include "pdc.h"

/* Sum the elements of a row to produce a single output */

int
demo_sum(pdcid_t iterIn, pdcid_t iterOut)
{
    int  i, k, total = 0;
    int *dataIn  = NULL;
    int *dataOut = NULL;
    int  blockLengthOut, blockLengthIn;
    printf("Entered: %s\n----------------\n", __func__);

    if ((blockLengthOut = PDCobj_data_getNextBlock(iterOut, (void **)&dataOut, NULL)) > 0) {
        while ((blockLengthIn = PDCobj_data_getNextBlock(iterIn, (void **)&dataIn, NULL)) > 0) {
            printf("Summing %d elements\n", blockLengthIn);
            for (i = 0, k = 0; i < blockLengthIn; i++) {
                printf("\t%d\n", dataIn[i]);
                total += dataIn[i];
            }
            printf("\nSum = %d\n", total);
            dataOut[k++] = total;
            total        = 0;
        }
    }
    printf("Leaving: %s\n----------------\n", __func__);
    return 0;
}

/* int check_mpi_access(pdcid_t iterIn , pdcid_t iterOut) */
int
check_mpi_access()
{
    int initialized = 0;
    MPI_Initialized(&initialized);
    if (initialized) {
        int rank;
        int size;

#ifdef ENABLE_MPI
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);
        printf("MPI rank is %d of %d\n", rank, size);
#endif
    }

    return 0;
}
