#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

void
print_usage()
{
    printf("Usage: srun -n ./data_server_read obj_name size_MB\n");
}

int
main(int argc, const char *argv[])
{
    int      rank = 0, size = 1;
    uint64_t size_MB  = 0;
    char *   obj_name = "test";

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdcid_t pdc = PDC_init("pdc");
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    pdcid_t cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    pdcid_t  test_obj     = -1;
    uint64_t my_data_size = size_MB / size;

    uint64_t dims[1] = {size_MB};
    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");

    struct PDC_region_info region;

    // Create a object with only rank 0
    if (rank == 0) {
        /* printf("Creating an object with name [%s]", obj_name); */
        /* fflush(stdout); */
        test_obj = PDCobj_create(cont, obj_name, obj_prop);
        if (test_obj <= 0) {
            printf("Error getting an object id of %s from server, exit...\n", "DataServerTestBin");
            exit(-1);
        }
    }

    /* printf("%d: object created.\n", rank); */
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query the created object
    /* printf("%d: Start to query object just created.\n", rank); */
    pdc_metadata_t *metadata;
    PDC_Client_query_metadata_name_timestep_agg(obj_name, 0, &metadata);
    if (metadata == NULL || metadata->obj_id == 0) {
        printf("Proc %d: Error with metadata!\n", rank);
        exit(-1);
    }

    if (rank % 4 == 0) {
        PDC_print_metadata(metadata);
    }

done:
    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container %lu\n", cont);

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
