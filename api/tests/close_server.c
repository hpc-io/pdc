#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    int i;

    PDC_prop_t p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);
    
    pdcid_t test_obj = -1;

    test_obj = PDCobj_create(pdc, NULL, "Closing server request", NULL);

done:
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
