#include "pdc.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define ELEMENTS 100
#define NDIM     2
// #define SHOW_PROGRESS 1

extern int pdc_convert_datatype(void *dataIn, PDC_var_type_t srcType,
				int ndim, uint64_t *dims,
				void *dataOut, PDC_var_type_t destType);


int
main(int argc, char **argv)
{
  double *d_array = NULL;
  int    *i_array = NULL;
  int    seed = 13;
  uint64_t dims[NDIM] = {1,ELEMENTS};
  size_t  n, elements = ELEMENTS;
  double start, end, total;

  if (argc > 1) {
      long newcount = atol(argv[1]);
      if (newcount > 0) {
	  elements = newcount;
	  dims[1] = newcount;
      }
  }

  MPI_Init(&argc, &argv);
  if ((d_array = (double *)malloc(elements * sizeof(double))) == NULL) {
    puts("Error!  Unable to allocate d_array");
    return -1;
  }

  if ((i_array = (int *)malloc(elements * sizeof(int))) == NULL) {
    puts("Error!  Unable to allocate i_array");
    return -1;
  }

  srand(seed);
  for(n=0; n < elements; n++) {
    d_array[n] = (double)(rand() / (double)(RAND_MAX/INT_MAX)) + 0.5;
  }

#ifdef SHOW_PROGRESS
  for(n=0; n < elements; ) {
      int k;
      printf("%4ld  ",n);
      for(k = 0; k < 10; k++, n++)
          printf(" %10.2f", d_array[n]);
      puts("");
  }  
  puts("--------------------------------");
#endif

  start = MPI_Wtime();
  if (pdc_convert_datatype(d_array, PDC_DOUBLE, NDIM, dims, i_array, PDC_INT) < 0) {
    puts("Error!  convert_datatype failed!");
    return -1;
  }
  end = MPI_Wtime();

#ifdef SHOW_PROGRESS
  for(n=0; n < elements; ) {
      int k;
      printf("%4ld  ",n);
      for(k = 0; k < 10; k++, n++)
          printf(" %10.2ld", i_array[n]);
      puts("");
  }  
  puts("--------------------------------");
#endif
  total = end-start;
  printf("pdc_convert_datatype (elements= %ld) completed in %lf seconds\n", elements, total);

  MPI_Finalize();

  return 0;
}
