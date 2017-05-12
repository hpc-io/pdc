#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif
    int i;

    struct PDC_prop p;
    pdcid_t pdc = PDC_init(p);

    PDC_Client_close_all_server();

    if(PDC_close(pdc) < 0)
        printf("fail to close PDC\n");
#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
