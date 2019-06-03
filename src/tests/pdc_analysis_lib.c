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

    // start = MPI_Wtime();
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
    // end = MPI_Wtime();
    // total = end-start;

    // printf("Leaving: %s execution time = %lf seconds\n----------------\n", __func__, total);
    return result;
}


/// ArrayUDF::
/// udf-openmsi
///           
///                  +-------+-------+-------+
///                  |-1...  |-1,-1,0|-1...  |
///                  +-------+-------+-------+
///                  |-1...  |-1,0,0 |-1...  |     
///                  +-------+-------+-------+
///           +-------+-------+-------+      |
///           |0 ...  | 0,-1,0|0,-1,1 |------+
///           +-------+-------+-------+
///           |0,0,-1 | 0,0,0 |0, 0,1 |   X = (0,0,0)
///           +-------+-------+-------+
///     +-------+-------+-------+     |
///     |1,-1,-1|1,-1,0 |0,-1,1 |-----+
///     +-------+-------+-------+
///     |1, 0,-1|1, 0,0 |1, 0,1 |   
///     +-------+-------+-------+
///     |1, 1,-1|1, 1,0 |1, 1,1 |
///     +-------+-------+-------+
///
///
/// New X is 7 time current X - 6 adjacent points in 3D.
/// (CURRENT_X * 7) minus (same plane: right, left, above, below), - (next plane X) - (prev plane X) 
///
//  return (c(0,0,0)*7.0 - c(0,0,1) - c(0,0,-1) - c(0,1,0) - c(0,-1,0) - c(1,0,0) - c(-1,0,0));

/// UDF Three: five point stencil 
///     +-----+-----+-----+
///     |-1,-1|-1,0 |-1,1 |
///     +-----+-----+-----+
///     | 0,-1|  X  | 0,1 |
///     +-----+-----+-----+
///     | 1,-1| 1,0 | 1,1 |
///     +-----+-----+-----+
///
//   return (c(0,0)+c(0,-1)+c(0,1)+c(1,0)+c(-1,0)) * 5.0;

size_t myfunc3(float *stencil[3], float *out, size_t dims[2])
{
    size_t k, elements = dims[1];
    size_t last = elements-1;
    for(k = 0; k < elements; k++) {
      /* Edge cases first */
      if (k == 0)   /* self       +    above      +   below       +    left           + right */
          out[k] = (stencil[1][0] + stencil[0][0] + stencil[2][0] + stencil[1][0] + stencil[1][1]) * 5;
      else if (k == last) 
          out[k] = (stencil[1][last] + stencil[0][last] + stencil[2][last] + stencil[1][last-1] + stencil[1][last]) *5;
      /* The normal case is last */
      else
          out[k] = (stencil[1][k] + stencil[0][k] + stencil[2][k] + stencil[1][k-1] + stencil[1][k+1]) * 5;
    }
    // printf("out[0-9] = %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", out[0],out[1],out[2],out[3],out[4],out[5],out[6],out[7],out[8],out[9]);
    return 0;
}



/// ArrayUDF::
/// New X is 7 time current X - 6 adjacent points in 3D.
/// (CURRENT_X * 7) minus (same plane: above, below, right, left), - (next plane X) - (prev plane X) 
///
//  return (c(0,0,0)*7.0 - c(0,0,1) - c(0,0,-1) - c(0,1,0) - c(0,-1,0) - c(1,0,0) - c(-1,0,0));

/*
 *  Edge conditions are at encountered on the 1st "row" and last "row"
 *  We can check these conditions by checking whether the (current index)
 *  MINUS the row_length is < 0; or if the (current index) PLUS the row length
 *  is greater than the number of elements...
 */
size_t msi_analysis_func(float *stencil[3], float *out, size_t elements, size_t row_length)
{
    float my_value;
    size_t k;
    size_t last_row = elements - row_length;
    size_t last_elem = row_length -1;

    for(k = 0; k < elements; k++) {
      /* First ROW */
      if (k < row_length) {
	if (k == 0) {
	  my_value = stencil[1][k];
	  out[k] = (my_value * 7.0)        /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - my_value   		   /* left */
	    - stencil[1][k+1]		   /* right */
	    - my_value          	   /* prev */
	    - stencil[1][k+row_length];	   /* next */
	}
	else if (k == last_elem) {
	  my_value = stencil[1][k];
	  out[k] = (my_value * 7.0)        /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - stencil[1][k-1]  		   /* left */
	    - my_value   		   /* right */
	    - my_value          	   /* prev */
	    - stencil[1][k+row_length];	   /* next */
	}
	else {
	  my_value = stencil[1][k];
	  out[k] = (my_value * 7.0)        /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - stencil[1][k-1]  		   /* left */
	    - stencil[1][k+1]  		   /* right */
	    - my_value          	   /* prev */
	    - stencil[1][k+row_length];	   /* next */
	}
      }
      /* Last ROW (almost the same as the First row */
      /* Diff:: uses my_value for next vs. prev */
      else if (k >= last_row) {
	if (k == 0) {
	  my_value = stencil[1][k];
	  out[k] = (my_value * 7.0)        /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - my_value   		   /* left */
	    - stencil[1][k+1]		   /* right */
	    - stencil[1][k-row_length];	   /* prev */
	    - my_value ;         	   /* next */
	}
	else if (k == last_elem) {
	  my_value = stencil[1][k];
	  out[k] = (my_value * 7.0)        /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - stencil[1][k-1]  		   /* left */
	    - my_value   		   /* right */
	    - stencil[1][k-row_length];	   /* prev */
	    - my_value ;         	   /* next */
	}
	else {
	  my_value = stencil[1][k];
	  out[k] = (my_value * 7.0)        /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - stencil[1][k-1]  		   /* left */
	    - stencil[1][k+1]  		   /* right */
	    - stencil[1][k-row_length];	   /* prev */
	    - my_value ;         	   /* next */
	}
      }
      /* All other rows::
       * Note that we still need to worry about the first element
       * and last elments in each row..
       */
      else {
	size_t index = k % row_length;
	if (index == 0) {
	  out[k] = (stencil[1][k] * 7.0)   /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - stencil[1][k]		   /* left == self */
	    - stencil[1][k+1]		   /* right */
	    - stencil[1][k-row_length]	   /* prev */
	    - stencil[1][k+row_length];	   /* next */
	}
	else if (index == last_elem) {
	  out[k] = (stencil[1][k] * 7.0)   /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - stencil[1][k-1]		   /* left */
	    - stencil[1][k]		   /* right == self */
	    - stencil[1][k-row_length]	   /* prev */
	    - stencil[1][k+row_length];	   /* next */
	}
	else {
	  out[k] = (stencil[1][k] * 7.0)   /* Self */
	    - stencil[0][k]		   /* up */
	    - stencil[2][k]		   /* down */
	    - stencil[1][k-1]		   /* left */
	    - stencil[1][k+1]		   /* right */
	    - stencil[1][k-row_length]	   /* prev */
	    - stencil[1][k+row_length];	   /* next */
	}
      }
    }

    // printf("out[0-9] = %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", out[0],out[1],out[2],out[3],out[4],out[5],out[6],out[7],out[8],out[9]);
    return 0;
}
  

/* PDC analysis entrypoint:
 * OpenMSI example
 * This is basically the 5 point stencil in 2D, plus 
 * an 2 extra data points in the 3rd Dimension.  The key to working
 * in the 3rd dimension is to understand:
 *    a) that while we present a 2D plane returned from the iterator
 *       as a 1D slice to the stencil computation, we are helped
 *       by having contiguous data.
 *    b) Within the contiguous data, we can reference other rows
 *       (plus or minus) by knowing the row length.  
 *    c) next row (same column) == current point reference + row length.
 *    d) prev row (same column) == current point reference - row length.
 *
 * In this example, we treat the 2D array as a contiguous vector of points
 * which is then presented as a series of flattened 2D planes.
 */

size_t openmsi_stencil(pdcid_t iterIn, pdcid_t iterOut, iterator_cbs_t *callbacks)
{
    float *dataIn = NULL;
    float *stencil[3] = {NULL, NULL, NULL};

    float *dataOut = NULL;
    size_t dimsIn[3] = {0,};
    size_t dimsOut[3] = {0,};
    size_t k, blockLengthOut, blockLengthIn, number_of_slices;
    size_t result = 0;
    double start, end, total;

    start = MPI_Wtime();
    number_of_slices = (*callbacks->getSliceCount)(iterOut);
    if ((blockLengthIn = (*callbacks->getNextBlock)(iterIn, (void **)&dataIn, dimsIn)) == 0)
      printf("openmsi_stencil: Empty Input!\n");

    stencil[0] = stencil[1] = dataIn;
    // printf("Total # Slices = %ld, Size of each slice = %ld elements = (%ld x %ld)\n",number_of_slices, blockLengthIn, dimsIn[2], dimsIn[1]);
    for (k=0; k< number_of_slices; k++) {
        if ((blockLengthOut = (*callbacks->getNextBlock)(iterOut, (void **)&dataOut, dimsOut)) > 0) {
	  if ((*callbacks->getNextBlock)(iterIn, (void **)&dataIn, dimsIn) > 0) {
                stencil[2] = dataIn;
                if (msi_analysis_func(stencil,dataOut,blockLengthIn,dimsIn[2]) == 0) {
                   stencil[0] = stencil[1];
                   stencil[1] = stencil[2];
		}
            }
            else result = msi_analysis_func(stencil,dataOut,blockLengthIn,dimsIn[2]);
        }
    }
    end = MPI_Wtime();
    total = end-start;
    printf("Leaving: %s execution time = %lf seconds\n----------------\n", __func__, total);
    return result;
}


// ArrayUDF:: S3D analysis
//      (next-row - prev-row)
// 	return (c(0,1,0)-c(0,-1,0));

size_t s3d_analysis_func(float *stencil[1], float *out, size_t elements, size_t row_length)
{
    size_t k;
    size_t last_row = elements - row_length;
    for(k = 0; k < elements; k++) {
       if (k < row_length) {
	  out[k] = stencil[0][k+row_length] - stencil[0][k];
       }
       else if (k >= last_row) {
	  out[k] = stencil[0][k] - stencil[0][k-row_length];
       }
       else {
	  out[k] = stencil[0][k+row_length] - stencil[0][k-row_length];	 
       }
    }
    // printf("out[0-9] = %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", out[0],out[1],out[2],out[3],out[4],out[5],out[6],out[7],out[8],out[9]);
    return 0;
}


size_t s3d_stencil(pdcid_t iterIn, pdcid_t iterOut, iterator_cbs_t *callbacks)
{
    float *dataIn = NULL;
    float *stencil[1] = {NULL};

    float *dataOut = NULL;
    size_t dimsIn[3] = {0,};
    size_t dimsOut[3] = {0,};
    size_t k, blockLengthOut, blockLengthIn, number_of_slices;
    size_t result = 0;
    double start, end, total;

    start = MPI_Wtime();
    number_of_slices = (*callbacks->getSliceCount)(iterOut);
    // printf("Total # Slices = %ld, Size of each slice = %ld elements = (%ld x %ld)\n",number_of_slices, blockLengthIn, dimsIn[2], dimsIn[1]);
    for (k=0; k< number_of_slices; k++) {
        if ((blockLengthOut = (*callbacks->getNextBlock)(iterOut, (void **)&dataOut, dimsOut)) > 0) {
	    if ((blockLengthIn = (*callbacks->getNextBlock)(iterIn, (void **)&dataIn, dimsIn)) == 0)
                printf("s3d_stencil: Empty Input!\n");
	    stencil[0] = dataIn;
            if (s3d_analysis_func(stencil,dataOut,blockLengthIn,dimsIn[2]) < 0) {
	      goto done;
	    }
        }
    }
 done:
    end = MPI_Wtime();
    total = end-start;
    printf("Leaving: %s execution time = %lf seconds\n----------------\n", __func__, total);
    return result;
}

