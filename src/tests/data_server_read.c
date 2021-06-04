#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/time.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

void
print_usage()
{
    printf("Usage: srun -n ./data_server_read obj_name readsize\n");
}

int
main(int argc, char **argv)
{
    int                    rank = 0, size = 1;
    uint64_t               readsize;
    pdcid_t                pdc, cont_prop, cont;
    struct pdc_region_info region;
    pdc_metadata_t *       metadata;
    uint64_t               my_readsize;
    int                    ndim = 1;
    void *                 buf;

    struct timeval ht_total_start;
    struct timeval ht_total_end;
    long long      ht_total_elapsed;
    double         ht_total_sec;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (argc < 3) {
        print_usage();
        return 0;
    }

    readsize = atoi(argv[2]);
    readsize *= 1048567;
    my_readsize = readsize / size;

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    PDC_Client_query_metadata_name_timestep(argv[1], 0, &metadata);

    region.ndim      = ndim;
    region.offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region.size      = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region.offset[0] = rank * (my_readsize);
    region.size[0]   = my_readsize;

    buf = (void *)malloc(region.size[0] + 1);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    PDC_Client_read_wait_notify(metadata, &region, buf);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;

    if (rank == 0) {
        printf("Time to read data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
