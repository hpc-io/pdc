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

    // create an object property
    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    pdcid_t test_obj = -1;
    char obj_name[128];

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    sprintf(obj_name, "/global/cscratch1/sd/houhun/test.h5");

    int dims[3]={100,200,300};
    PDCprop_set_obj_dims(obj_prop, 2, dims, pdc);
    PDCprop_set_obj_user_id( obj_prop, getuid(),    pdc);
    PDCprop_set_obj_time_step( obj_prop, 0,    pdc);
    PDCprop_set_obj_app_name(obj_prop, "DataServerRW",  pdc);
    PDCprop_set_obj_tags(    obj_prop, "tag0=1",    pdc);
    PDCprop_set_obj_data_loc(obj_prop, obj_name, pdc);

    struct PDC_region_info region;
    void *buf = (void*)malloc(1024);;

    // Create a object
    test_obj = PDCobj_create(pdc, cont, "DataServerTestBin", obj_prop);
    if (test_obj < 0) {
        printf("Error getting an object id of %s from server, exit...\n", obj_name);
        exit(-1);
    }

    pdc_metadata_t *metadata;
    PDC_Client_query_metadata_name_timestep( "DataServerTestBin", 0, &metadata);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    gettimeofday(&ht_total_start, 0);

    int ndim = 2;
    region.ndim = ndim;
    region.offset = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region.size = (uint64_t*)malloc(sizeof(uint64_t) * ndim);
    region.offset[0] = rank*10;
    region.offset[1] = 0;
    region.size[0] = 10;
    region.size[1] = 10;

    PDC_Client_data_server_read(0, size, metadata, &region, buf);


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

done:
    free(buf);

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
