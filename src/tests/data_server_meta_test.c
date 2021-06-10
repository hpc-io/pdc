#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/time.h>
#include <inttypes.h>
#include <unistd.h>
#include "pdc.h"
#include "pdc_client_connect.h"

#define NOBJ 10

void
print_usage()
{
    printf("Should run with more than 1 processes!\n");
}

int
main(int argc, char **argv)
{
    int                    rank = 0, size = 1;
    int                    i;
    pdcid_t                obj_ids[NOBJ];
    pdcid_t                pdc, cont_prop, cont, obj_prop;
    struct pdc_region_info write_region;
    struct pdc_region_info read_region;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    pdc_metadata_t *metadata[NOBJ];
    char *          obj_names[NOBJ];
    const int       my_data_size = 1048576;
    int             ndim         = 1;
    uint64_t        dims[1]      = {my_data_size};
    char *          write_data   = (char *)malloc(my_data_size);

    int       my_read_obj       = NOBJ / size;
    int       my_read_obj_start = my_read_obj * rank;
    uint64_t *out_buf_sizes     = (uint64_t *)calloc(sizeof(uint64_t), my_read_obj);
    void **   out_buf;

    write_region.ndim      = ndim;
    write_region.offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    write_region.size      = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    write_region.offset[0] = rank * my_data_size;
    write_region.size[0]   = my_data_size;

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

    PDCprop_set_obj_dims(obj_prop, 1, dims);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
    PDCprop_set_obj_tags(obj_prop, "tag0=1");
    PDCprop_set_obj_time_step(obj_prop, 0);

    for (i = 0; i < NOBJ; i++) {
        obj_names[i] = (char *)calloc(128, 1);
        sprintf(obj_names[i], "TestObj%d", i);
    }

    // Create a object with only rank 0
    if (rank == 0) {
        for (i = 0; i < NOBJ; i++) {
            obj_ids[i] = PDCobj_create(cont, obj_names[i], obj_prop);
            if (obj_ids[i] <= 0) {
                printf("Error getting an object id of %s from server, exit...\n", "DataServerTestBin");
                goto done;
            }
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query and write the created object
    for (i = 0; i < NOBJ; i++) {
        PDC_Client_query_metadata_name_timestep(obj_names[i], 0, &metadata[i]);
        if (metadata[i]->obj_id == 0) {
            printf("Error with metadata!\n");
            goto done;
        }

        memset(write_data, 'A' + i, my_data_size);

        PDC_Client_write(metadata[i], &write_region, write_data);
    }

    printf("%d - Finished writing %d regions. \n", rank, NOBJ);
    fflush(stdout);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Each rank tries to read its next rank's written region
    read_region.ndim      = ndim;
    read_region.offset    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    read_region.size      = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    read_region.offset[0] = (rank + 1) * my_data_size;
    if (rank == size - 1)
        read_region.offset[0] = 0;
    read_region.size[0] = my_data_size;

    // Read two regions
    for (i = 0; i < my_read_obj; i++) {
        sprintf(obj_names[i], "TestObj%d", i + my_read_obj_start);
    }

    PDC_Client_query_name_read_entire_obj(my_read_obj, obj_names, &out_buf, out_buf_sizes);
    printf("Received %d data objects:\n", NOBJ);
    for (i = 0; i < my_read_obj; i++) {
        printf("Proc %d - [%s]: [%c] ... [%c], size %" PRId64 "\n", rank, obj_names[i],
               ((char **)out_buf)[i][0], ((char **)out_buf)[i][out_buf_sizes[i] - 1], out_buf_sizes[i]);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (rank == 0) {
        PDC_Client_all_server_checkpoint();
    }

done:
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
