#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include "pdc.h"

void
print_usage()
{
    LOG_JUST_PRINT("Usage: srun -n ./data_server_read obj_name size_MB\n");
}

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1;
    pdcid_t pdc, cont_prop, cont;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        LOG_ERROR("Failed to create container property");

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        LOG_ERROR("Failed to create container");

    LOG_INFO("PROC[%d] FINISHED!\n", rank);

    // close a container
    if (PDCcont_close(cont) < 0)
        LOG_ERROR("Failed to close container\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        LOG_ERROR("Failed to close property");

    if (PDCclose(pdc) < 0)
        LOG_ERROR("Failed to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
