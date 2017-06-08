#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

void print_usage() {
    printf("Usage: srun -n ./data_server_read \n");
}

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1;
    int i;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    struct PDC_prop p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    pdcid_t cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    pdcid_t test_obj = -1;
    char obj_name[128];

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    struct PDC_region_info region;

    pdc_metadata_t *metadata;
    PDC_Client_query_metadata_name_timestep( "DataServerTestBin", 0, &metadata);
    // Debug print
    /* if (rank == 1) { */
    /*     PDC_print_metadata(metadata); */
    /* } */

    int ndim = 2;
    region.ndim = ndim;
    region.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region.offset[0] = 0;
    region.offset[1] = rank*(metadata->dims[1]/size);
    region.size[0] = metadata->dims[0];
    region.size[1] = metadata->dims[1]/size;

    void *buf = (void*)malloc(region.size[0]*region.size[1] + 1);
    /* printf("%d: reading start (%llu, %llu) count (%llu, %llu)\n", rank, region.offset[0], region.offset[1], */
    /*         region.size[0], region.size[1]); */

    /* PDC_Client_data_server_read(0, size, metadata, &region); */
    /* PDC_Client_data_server_read_check(0, rank, metadata, &region, &io_status, buf); */

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    PDC_Client_read(metadata, &region, buf);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    if (rank == 0) { 
        printf("Time to read data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

    // Data verification
    /* printf("%d buf:\n%s\n", rank, (char*)buf); */
    /* printf("%d buf:\n%.10s\n", rank, (char*)buf); */
    ((char*)buf)[region.size[0]*region.size[1]] = 0;
    if ( ((char*)buf)[0] != 'A' + rank) {
        printf("Proc%d: Data correctness verification FAILED!!!\n", rank);
    }
    else {
        if (rank == 0) {
            printf("Data read successfully!\n");
        }
    }

done:
    /* free(buf); */

    // close a container
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);

    // close a container property
    if(PDCprop_close(cont_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
