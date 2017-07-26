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

int rank = 0, size = 1;
pdcid_t pdc;
pdcid_t cont;
pdcid_t cont_prop;

void print_usage() {
    printf("Usage: srun -n ./data_rw_test\n");
}

pdcid_t create_obj(char *obj_name, uint32_t ndim, uint64_t *dims)
{
    pdcid_t obj_id = 0;

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    PDCprop_set_obj_dims     (obj_prop, ndim, dims);
    PDCprop_set_obj_user_id  (obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);
    PDCprop_set_obj_app_name (obj_prop, "DataServerTest");
    PDCprop_set_obj_tags     (obj_prop, "tag0=1");

    // Create a object with only rank 0
    if (rank == 0) {
        obj_id = PDCobj_create(cont, obj_name, obj_prop);
        if (obj_id <= 0) {
            printf("Error getting an object id of %s from server, exit...\n", "DataServerTestBin");
            exit(-1);
        }
    }

    return obj_id;
}

int test1d(char *obj_name)
{
    uint32_t ndim = 1;

    uint64_t dims[3];
    uint64_t i = 0;
    pdcid_t obj_id;
    pdc_metadata_t *metadata;
    struct PDC_region_info region0;
    struct PDC_region_info region1;
    struct PDC_region_info region2;
    struct PDC_region_info region3;
    struct PDC_region_info read_region;
    uint64_t offset0, offset1, offset2, offset3, read_offset;
    uint64_t size0, size1, size2, size3, read_size;

    dims[0] = 120;
    dims[1] =   0;
    dims[2] =   0;
    obj_id = create_obj(obj_name, ndim, dims);

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL) {
        printf("Error getting metadata from server!\n");
        exit(-1);
    }

    offset0 = 10;
    offset1 = 25;
    offset2 = 45;
    offset3 = 70;

    size0   = offset1 - offset0; // 15
    size1   = offset2 - offset1; // 20
    size2   = offset3 - offset2; // 25
    size3   = dims[0] - offset3; // 30

    region0.ndim = ndim;
    region1.ndim = ndim;
    region2.ndim = ndim;
    region3.ndim = ndim;

    region0.offset = &offset0;
    region1.offset = &offset1;
    region2.offset = &offset2;
    region3.offset = &offset3;

    region0.size = &size0;
    region1.size = &size1;
    region2.size = &size2;
    region3.size = &size3;

    char *data0 = (char*)malloc(size0 * sizeof(char));
    char *data1 = (char*)malloc(size1 * sizeof(char));
    char *data2 = (char*)malloc(size2 * sizeof(char));
    char *data3 = (char*)malloc(size3 * sizeof(char));
    for (i = 0; i < dims[0]; i++) {
        if (i < size0) 
            data0[i] = i % 6 + 'A';
        if (i < size1) 
            data1[i] = i % 6 + 'a' + 6;
        if (i < size2) 
            data2[i] = i % 6 + 'A' + 12;
        if (i < size3) 
            data3[i] = i % 6 + 'a' + 18;
    }

    PDC_Client_write_wait_notify(metadata, &region0, data0);
    PDC_Client_write_wait_notify(metadata, &region1, data1);
    PDC_Client_write_wait_notify(metadata, &region2, data2);
    PDC_Client_write_wait_notify(metadata, &region3, data3);

    read_offset = 15;
    read_size   = 80;
    read_region.ndim   = ndim;
    read_region.offset = &read_offset;
    read_region.size   = &read_size;

    char *read_data = (char*)malloc(sizeof(char) * read_size + 1);

    PDC_Client_read_wait_notify(metadata, &read_region, read_data);

    read_data[read_size] = 0;

    printf("read data:\n%s\n", read_data);

done:
    free(data0);
    free(data1);
    free(data2);
    free(data3);

    return 1;
}

int main(int argc, const char *argv[])
{

    uint64_t dims_1d, dims_2d[2], dims_3d[3];
    uint64_t size_MB;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc = PDC_init("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    test1d("test_obj_1d");



done:
    // close a container property
    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    // close a container
    if(PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
