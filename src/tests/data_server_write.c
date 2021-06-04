#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/time.h>
#include <inttypes.h>
#include <unistd.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

void
print_usage()
{
    printf("Usage: srun -n ./data_server_read obj_name size_MB\n");
}

int
main(int argc, char **argv)
{
    int                    rank = 0, size = 1;
    uint64_t               size_MB;
    char *                 obj_name;
    pdcid_t                test_obj = -1;
    pdcid_t                pdc, cont_prop, cont, obj_prop;
    struct pdc_region_info region;
    int                    my_data_size;
    uint64_t               dims[1];
    int                    ndim = 1;
    pdc_metadata_t *       metadata;
    char *                 mydata;

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

    obj_name = argv[1];
    size_MB  = atoi(argv[2]);

    if (rank == 0) {
        printf("Writing a %" PRIu64 " MB object [%s] with %d clients.\n", size_MB, obj_name, size);
    }
    size_MB *= 1048576;

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

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    my_data_size = size_MB / size;
    dims[0]      = my_data_size;
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    // Create a object with only rank 0
    if (rank == 0) {
        test_obj = PDCobj_create(cont, obj_name, obj_prop);
        if (test_obj <= 0) {
            printf("Error getting an object id of %s from server, exit...\n", "DataServerTestBin");
            exit(-1);
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL || metadata->obj_id == 0) {
        printf("Error with metadata!\n");
    }

    region.ndim      = ndim;
    region.offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region.size      = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region.offset[0] = rank * my_data_size;
    region.size[0]   = my_data_size;

    mydata = (char *)malloc(my_data_size);
    memset(mydata, 'A' + rank % 26, my_data_size);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    PDC_Client_write(metadata, &region, mydata);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;

    if (rank == 0) {
        printf("Time to write data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

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
