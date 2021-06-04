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
    printf("Usage: srun -n ./query_data obj_name size_MB\n");
}

int
main(int argc, char **argv)
{
    int                    rank = 0, size = 1;
    uint64_t               size_MB;
    pdcid_t                obj_id = -1;
    struct pdc_region_info region;
    uint64_t               i, dims[1];
    pdc_selection_t        sel;
    char *                 obj_name;
    int                    my_data_count;
    pdc_metadata_t *       metadata;
    pdcid_t                pdc, cont_prop, cont, obj_prop;
    int                    ndim = 1;
    int *                  mydata;
    int                    lo0 = 1000;
    int                    lo1 = 2000, hi1 = 3000;
    int                    lo2 = 5000, hi2 = 7000;
    pdc_query_t *          q0, *q1l, *q1h, *q1, *q2l, *q2h, *q2, *q, *q12;
    int                    ret_value = 0;

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
#ifdef ENABLE_MPI
        MPI_Finalize();
#endif
        return 1;
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
    if (cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0) {
        printf("Fail to create object property @ line  %d!\n", __LINE__);
        ret_value = 1;
    }
    my_data_count = size_MB / size;
    dims[0]       = my_data_count;
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");
    PDCprop_set_obj_type(obj_prop, PDC_INT);

    // Create a object with only rank 0
    if (rank == 0) {
        printf("Creating an object with name [%s]\n", obj_name);
        fflush(stdout);
        obj_id = PDCobj_create(cont, obj_name, obj_prop);
        if (obj_id <= 0) {
            printf("Error getting an object id of %s from server, exit...\n", "DataServerTestBin");
            ret_value = 1;
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL || metadata->obj_id == 0) {
        printf("Error with metadata!\n");
        ret_value = 1;
    }

    region.ndim      = ndim;
    region.offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region.size      = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region.offset[0] = rank * my_data_count;
    region.size[0]   = my_data_count;

    mydata = (int *)malloc(my_data_count);
    for (i = 0; i < my_data_count / sizeof(int); i++)
        mydata[i] = i + rank * 1000;

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

    // Construct query constraints
    q0 = PDCquery_create(obj_id, PDC_LT, PDC_INT, &lo0);

    PDCquery_sel_region(q0, &region);

    q1l = PDCquery_create(obj_id, PDC_GTE, PDC_INT, &lo1);
    q1h = PDCquery_create(obj_id, PDC_LT, PDC_INT, &hi1);
    q1  = PDCquery_and(q1l, q1h);

    q2l = PDCquery_create(obj_id, PDC_GTE, PDC_INT, &lo2);
    q2h = PDCquery_create(obj_id, PDC_LT, PDC_INT, &hi2);
    q2  = PDCquery_and(q2l, q2h);

    q12 = PDCquery_or(q1, q2);
    q   = PDCquery_or(q0, q12);

    PDCquery_get_selection(q, &sel);

    PDCselection_print(&sel);

    PDCquery_free_all(q);
    PDCregion_free(&region);
    PDCselection_free(&sel);
    // close a container
    if (PDCcont_close(cont) < 0) {
        printf("fail to close container c1\n");
        ret_value = 1;
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        ret_value = 1;
    }
    if (PDCclose(pdc) < 0) {
        printf("fail to close PDC\n");
        ret_value = 1;
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return ret_value;
}
