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

#define ENABLE_MPI 1

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"


/* Sum the elements of a row to produce a single output */

int demo_sum(pdcid_t iterIn, pdcid_t iterOut)
{
    int i,k,total = 0;
    int *dataIn = NULL;
    int *dataOut = NULL;
    int blockLengthOut, blockLengthIn;
    printf("Entered: %s\n----------------\n", __func__);
    
    if ((blockLengthOut = PDCobj_data_getNextBlock(iterOut, (void **)&dataOut, NULL)) > 0) {
      while((blockLengthIn = PDCobj_data_getNextBlock(iterIn, (void **)&dataIn, NULL)) > 0) {
	printf("Summing %d elements\n", blockLengthIn);
	for(i=0,k=0; i < blockLengthIn; i++) {
	    printf("\t%d\n", dataIn[i]);
	    total += dataIn[i];
	}
	printf("\nSum = %d\n", total);
	dataOut[k++] = total;
	total = 0;
      }
    }
    printf("Leaving: %s\n----------------\n", __func__);
    return 0;
}

/* int simple_null_iterator_demo(pdcid_t iterIn , pdcid_t iterOut)
{
    printf("Hello from a simple server based analysis demo\n");
    return 666;
} */

/* int check_mpi_access(pdcid_t iterIn , pdcid_t iterOut) */
int check_mpi_access()
{
    int initialized = 0;
    MPI_Initialized(&initialized);
    if (initialized) {
        int rank;
        int size;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);
        printf("MPI rank is %d of %d\n", rank, size);
    }

    return 0;
}


/*
 * ArrayUDF:: neon/ running average application
 * For each point, create the average of the current point value 
 * and the 3 previous values.  The input data is a 3D array,
 * but to ease the computing in PDC, we treat each 2D (X,Y) plane
 * as a vector of (X * Y) points.
 * Because we need to access the 3 previous values in each column,
 * we define the stencil as having 4 values where:
 *   1.  The current point at index (3,0,0).
 *   2.  The average is thus ((3,0,0) + (2,0,0, +(1,0,0, + (0,0,0))/4.
 * -------------------------------------------------
 inline float myfunc1(const SDSArrayCell<float> &c){
	  return (c(0,0,0)+c(-1,0,0)+c(-2,0, 0)+c(-3,0,0))/4;
}
*/

size_t cortad_avg_func(short *stencil[4], float *out, size_t elements)
{
    int k;
    for(k = 0; k < elements; k++) {
      out[k] = (float)((stencil[0][k] + stencil[1][k] + stencil[2][k] + stencil[3][k]) / 4.0);
    }
    return 0;
}

/* PDC analysis entrypoint:
 * This calls the cortad running average function on
 * each PDC array slice as returned from the PDCobj_data_getNextBlock();
 * We treat the 2D array as a contiguous vector of points
 * which is then presented as a series of 4 historical datapoints.
 * [ the grid of 2D geographic points is "flattened" into 1D and the
 *   3rd dimension (historial data) is the collection of NextBlocks... ]
 * 
 */
size_t neon_stencil(pdcid_t iterIn, pdcid_t iterOut, iterator_cbs_t *callbacks)
{
    short *dataIn = NULL;
    short *stencil[4] = {NULL, NULL, NULL, NULL};

    float *dataOut = NULL;
    size_t dimsIn[3] = {0,};
    size_t dimsOut[3] = {0,};
    size_t k, blockLengthOut, blockLengthIn, number_of_slices;
    size_t result = 0;
    double start, end, total;

    start = MPI_Wtime();
    printf("Entered: %s\n----------------\n", __func__);
    number_of_slices = (*callbacks->getSliceCount)(iterOut);
    if ((blockLengthIn = (*callbacks->getNextBlock)(iterIn, (void **)&dataIn, dimsIn)) == 0)
      printf("neon_stencil: Empty Input!\n");
    else {
        printf("Total # Slices = %ld, Size of each slice = %ld elements = (%ld x %ld)\n",number_of_slices, blockLengthIn, dimsIn[2], dimsIn[1]);
        stencil[0] = stencil[1] = stencil[2] = stencil[3] = dataIn;
        for (k=0; k< number_of_slices; k++) {
          if ((blockLengthOut = (*callbacks->getNextBlock)(iterOut, (void **)&dataOut, dimsOut)) > 0) {
             if (cortad_avg_func(stencil,dataOut,blockLengthIn) == 0) {
                stencil[0] = stencil[1];
                stencil[1] = stencil[2];
                stencil[2] = stencil[3];
             }
             blockLengthIn = (*callbacks->getNextBlock)(iterIn, (void **)&dataIn, NULL);
             stencil[3] = dataIn;
          }
        }
    }
    end = MPI_Wtime();
    total = end-start;

    printf("Leaving: %s execution time = %lf seconds\n----------------\n", __func__, total);
    return result;
}

