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
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            exit(-1);
        }
    }

    return obj_id;
}

int data_verify(int ndim, int *start, int *count, char *data, int *truth_start, char *truth_data)
{
    int i, j, k, m;

    int tmp_truth_start[3]={0,0,0}, tmp_start[3]={0,0,0}, tmp_count[3]={1,1,1};
    for (i = 0; i < ndim; i++) {
        tmp_start[i] = start[i];
        tmp_count[i] = count[i];
        tmp_truth_start[i] = truth_start[i];
    }

    for (i = 0; i < tmp_count[2]; i++) {
        for (j = 0; j < tmp_count[1]; j++) {
            for (k = 0; k < tmp_count[0]; k++) {
                if (      data[i*tmp_count[2]*tmp_count[1] + j*tmp_count[1] + k + 
                               tmp_start[2]*tmp_count[1]*tmp_count[0] + 
                               tmp_start[1]*tmp_count[0] + tmp_start[0]
                              ] != 
                    truth_data[i*tmp_count[2]*tmp_count[1] + j*tmp_count[1] + k +
                               tmp_truth_start[2]*tmp_count[1]*tmp_count[0] + 
                               tmp_truth_start[1]*tmp_count[0] + tmp_truth_start[0]
                              ] ) {
                    printf("Truth data: %s\n", truth_data+tmp_truth_start[2]*tmp_count[1]*tmp_count[0] 
                                               + tmp_truth_start[1]*tmp_count[0] + tmp_truth_start[0]);
                    return -1;
                }

            }
        }
    }

    return 1;
}

int test1d(char *obj_name)
{

    uint64_t dims[3];
    uint64_t i = 0;
    pdcid_t obj_id;
    pdc_metadata_t *metadata;
    struct PDC_region_info region_a;
    struct PDC_region_info region_b;
    struct PDC_region_info region_c;
    struct PDC_region_info region_d;
    struct PDC_region_info read_region;
    uint64_t storage_offset_a, storage_offset_b, storage_offset_c, storage_offset_d, read_offset;
    uint64_t storage_size_a, storage_size_b, storage_size_c, storage_size_d, read_size;

    uint32_t ndim = 1;
    dims[0] = 120;
    dims[1] =   0;
    dims[2] =   0;
    obj_id = create_obj(obj_name, ndim, dims);
    if (obj_id < 0) {
        printf("[%d]: Error creating an object [%s]\n", rank, obj_name);
        exit(-1);
    }

#ifdef ENABLE_MPI
     MPI_Barrier(MPI_COMM_WORLD);
#endif

    /* sleep(rank*3); */

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL) {
        printf("Error getting metadata from server!\n");
        exit(-1);
    }

    storage_offset_a = 10 + rank*dims[0];
    storage_offset_b = 25 + rank*dims[0];
    storage_offset_c = 45 + rank*dims[0];
    storage_offset_d = 70 + rank*dims[0];

    storage_size_a = storage_offset_b - storage_offset_a; // 15
    storage_size_b = storage_offset_c - storage_offset_b; // 20
    storage_size_c = storage_offset_d - storage_offset_c; // 25
    storage_size_d = (rank+1)*dims[0] - storage_offset_d; // 30

    region_a.ndim = ndim;
    region_b.ndim = ndim;
    region_c.ndim = ndim;
    region_d.ndim = ndim;

    region_a.offset = &storage_offset_a;
    region_b.offset = &storage_offset_b;
    region_c.offset = &storage_offset_c;
    region_d.offset = &storage_offset_d;

    region_a.size = &storage_size_a;
    region_b.size = &storage_size_b;
    region_c.size = &storage_size_c;
    region_d.size = &storage_size_d;

    int total_size = storage_size_a + storage_size_b + storage_size_c + storage_size_d;
    char *data  = (char*)malloc(total_size* sizeof(char));

    char *data0 = data;
    char *data1 = data + storage_size_a;
    char *data2 = data + storage_size_b + storage_size_a;
    char *data3 = data + storage_size_b + storage_size_a + storage_size_c;

    /* char *data0 = (char*)malloc(storage_size_a * sizeof(char)); */
    /* char *data1 = (char*)malloc(storage_size_b * sizeof(char)); */
    /* char *data2 = (char*)malloc(storage_size_c * sizeof(char)); */
    /* char *data3 = (char*)malloc(storage_size_d * sizeof(char)); */
    for (i = 0; i < dims[0]; i++) {
        if (i < storage_size_a) 
            data[i] = i % 13 + 'A';
        else if (i < storage_size_a + storage_size_b) 
            data[i] = i % 13 + 'a';
        else if (i < storage_size_a + storage_size_b + storage_size_c) 
            data[i] = i % 13 + 'A' + 13;
        else 
            data[i] = i % 13 + 'a' + 13;
    }

    PDC_Client_write_wait_notify(metadata, &region_a, data0);
    PDC_Client_write_wait_notify(metadata, &region_b, data1);
    PDC_Client_write_wait_notify(metadata, &region_c, data2);
    PDC_Client_write_wait_notify(metadata, &region_d, data3);

    read_offset = 15 + rank*dims[0];
    read_size   = 80;
    read_region.ndim   = ndim;
    read_region.offset = &read_offset;
    read_region.size   = &read_size;


    /* printf("proc%d: write data [%s]\n", rank, data); */

    /* printf("proc%d: read data region: %llu size: %llu\n", rank, read_offset, read_size); */


    char *read_data = (char*)malloc(sizeof(char) * read_size + 1);

    PDC_Client_read_wait_notify(metadata, &read_region, read_data);

    read_data[read_size] = 0;

    /* printf("===\nproc%d: read data:\n%s\n===\n", rank, read_data); */
    /* fflush(stdout); */

    int truth_start = read_offset - region_a.offset[0];
    int data_start = 0;

    printf("proc%d: Data verfication ...", rank, read_data);
    if (data_verify(ndim, &data_start, read_region.size, read_data, &truth_start, data) != 1) 
        printf("FAILED!\n");
    else
        printf("SUCCEED!\n");
    

done:
    free(data);
    /* free(data0); */
    /* free(data1); */
    /* free(data2); */
    /* free(data3); */

    return 1;
}


int test2d(char *obj_name)
{

    pdcid_t obj_id;
    pdc_metadata_t *metadata;
    struct PDC_region_info region_a;
    struct PDC_region_info region_b;
    struct PDC_region_info region_c;
    struct PDC_region_info region_d;
    struct PDC_region_info read_region;
    uint64_t storage_offset_a[3], storage_offset_b[3], storage_offset_c[3], storage_offset_d[3], read_offset[3];
    uint64_t storage_size_a[3], storage_size_b[3], storage_size_c[3], storage_size_d[3], read_size[3];
    uint64_t prob_domain[3], storage_domain[3], sel_offset[3], sel_size[3];
    uint64_t i,j;

    uint32_t ndim = 2;
    prob_domain[0] = 25 * size;
    prob_domain[1] = 25 * size;
    prob_domain[2] =  0;

    obj_id = create_obj(obj_name, ndim, prob_domain);
    if (obj_id < 0) {
        printf("[%d]: Error creating an object [%s]\n", rank, obj_name);
        exit(-1);
    }

#ifdef ENABLE_MPI
     MPI_Barrier(MPI_COMM_WORLD);
#endif

    /* sleep(rank*3); */

    // Query the created object
    PDC_Client_query_metadata_name_timestep(obj_name, 0, &metadata);
    if (metadata == NULL) {
        printf("Error getting metadata from server!\n");
        exit(-1);
    }

    // Storage Regions  start     size
    //            a      5,10     4, 6
    //            b      9,10    11, 6
    //            c      5,16     4, 9
    //            d      9,16    11, 9
    //            
    //         5       9               25                 8             18
    //    |-----------------------------|        |-----------------------------|
    //    |                             |        |                             |
    //    |                             |        |                             |
    // 10 |    |-------|----------------|        |                             |
    //    |    |   a   |       b        |     14 |        |--------------|     |
    // 16 |    |-------|----------------|        |        |  selection   |     |
    //    |    |       |                |     18 |        |--------------|     |
    //    |    |   c   |       d        |        |                             |
    //    |    |       |                |        |                             |
    // 25 |----|-------|----------------|        |------------ ----------------|

    storage_offset_a[0] =  5 + rank*prob_domain[0];
    storage_offset_b[0] =  9 + rank*prob_domain[0];
    storage_offset_c[0] =  5 + rank*prob_domain[0];
    storage_offset_d[0] =  9 + rank*prob_domain[0];

    storage_offset_a[1] = 10 + rank*prob_domain[1];
    storage_offset_b[1] = 10 + rank*prob_domain[1];
    storage_offset_c[1] = 16 + rank*prob_domain[1];
    storage_offset_d[1] = 16 + rank*prob_domain[1];

    storage_size_a[0]   =  4;
    storage_size_b[0]   = 16;
    storage_size_c[0]   =  4;
    storage_size_d[0]   = 16;

    storage_size_a[1]   =  6;
    storage_size_b[1]   =  6;
    storage_size_c[1]   =  9;
    storage_size_d[1]   =  9;

    storage_domain[0]   = (storage_size_a[0] + storage_size_b[0]) * size; // 20
    storage_domain[1]   = (storage_size_a[1] + storage_size_c[1]) * size; // 15
    storage_domain[2]   =  1;

    region_a.ndim       = ndim;
    region_b.ndim       = ndim;
    region_c.ndim       = ndim;
    region_d.ndim       = ndim;

    region_a.offset     = storage_offset_a;
    region_b.offset     = storage_offset_b;
    region_c.offset     = storage_offset_c;
    region_d.offset     = storage_offset_d;

    region_a.size       = storage_size_a;
    region_b.size       = storage_size_b;
    region_c.size       = storage_size_c;
    region_d.size       = storage_size_d;

    // Now data selection
    sel_offset[0] =  8 + rank*prob_domain[0];
    sel_offset[1] = 14 + rank*prob_domain[1];
    sel_size[0]   = 10;
    sel_size[0]   =  4;
    
    int data_total_size = storage_domain[0] * storage_domain[1];
    char *data          = ( char*)calloc(sizeof(char) , data_total_size);
    char **data_2d      = (char**)calloc(sizeof(char*), storage_domain[1]);
    for (i = 0; i < storage_domain[1]; i++) 
        data_2d[i] = data + i * storage_domain[0];
   
    char **data_a_2d = (char**)malloc(sizeof(char*) * storage_size_a[1]); 
    char **data_b_2d = (char**)malloc(sizeof(char*) * storage_size_b[1]); 
    char **data_c_2d = (char**)malloc(sizeof(char*) * storage_size_c[1]); 
    char **data_d_2d = (char**)malloc(sizeof(char*) * storage_size_d[1]); 

    for (i = 0; i < storage_size_a[1]; i++) {
        data_a_2d[i] = data_2d[i];
        for (j = 0; j < storage_size_a[0]; j++) 
            data_a_2d[i][j] = (i+j) % 4 + 'A';
    }

    for (i = 0; i < storage_size_b[1]; i++) {
        data_b_2d[i] = data_2d[i] + storage_size_a[0];
        for (j = 0; j < storage_size_b[0]; j++) 
            data_b_2d[i][j] = (i+j) % 12 + 'a' + 6;
    }
 
    for (i = 0; i < storage_size_c[1]; i++) {
        data_c_2d[i] = data_2d[i+storage_size_a[1]];
        for (j = 0; j < storage_size_c[0]; j++) 
            data_c_2d[i][j] = (i+j) % 4 + 'a' + 14;
    }
 
    for (i = 0; i < storage_size_d[1]; i++) {
        data_d_2d[i] = data_2d[i+storage_size_a[1]] + storage_size_a[0];
        for (j = 0; j < storage_size_d[0]; j++) 
            data_d_2d[i][j] = (i+j) % 5 + 'A' + 20;
    }

    // Debug print
    /* printf("Linearized Data:\n"); */
    /* for (i = 0; i < storage_domain[1]; i++) */ 
    /*     printf("%.2llu (%.2llu): %.*s\n", i, storage_domain[0], storage_domain[0], data_2d[i]); */

    /* printf("\n\n2D Data by region:\na:\n"); */
    /* for (i = 0; i < storage_size_a[1]; i++) */ 
    /*     printf("%.*s\n", storage_size_a[0], data_a_2d[i]); */
    /* printf("\nb:\n"); */
    /* for (i = 0; i < storage_size_b[1]; i++) */ 
    /*     printf("%.*s\n", storage_size_b[0], data_b_2d[i]); */
    /* printf("\nc:\n"); */
    /* for (i = 0; i < storage_size_c[1]; i++) */ 
    /*     printf("%.*s\n", storage_size_c[0], data_c_2d[i]); */
    /* printf("\nd:\n"); */
    /* for (i = 0; i < storage_size_d[1]; i++) */ 
    /*     printf("%.*s\n", storage_size_d[0], data_d_2d[i]); */
    
    PDC_Client_write_wait_notify(metadata, &region_a, data_a_2d);
    PDC_Client_write_wait_notify(metadata, &region_b, data_b_2d);
    PDC_Client_write_wait_notify(metadata, &region_c, data_c_2d);
    PDC_Client_write_wait_notify(metadata, &region_d, data_d_2d);

    read_offset[0] =  8 + rank*prob_domain[0];
    read_offset[1] = 14 + rank*prob_domain[1];
    read_size[0]   = 10;
    read_size[1]   = 4;
    read_region.ndim   = ndim;
    read_region.offset = read_offset;
    read_region.size   = read_size;


/*     /1* printf("proc%d: write data [%s]\n", rank, data); *1/ */

/*     /1* printf("proc%d: read data region: %llu size: %llu\n", rank, read_offset, read_size); *1/ */


    char *read_data     = (char*)malloc(sizeof(char) * read_size[0]*read_size[1] + 1);
    char **read_data_2d = (char**)calloc(sizeof(char*), storage_domain[1]);
    for (i = 0; i < read_size[1]; i++) 
        read_data_2d[i] = read_data + i * read_size[0];

    PDC_Client_read_wait_notify(metadata, &read_region, read_data_2d);

    read_data[read_size[0]*read_size[1]] = 0;

    
    printf("===\nproc%d: read data:\n", rank);
    for (i = 0; i < read_size[1]; i++) 
        printf("[%.*s]\n", read_size[0], read_data_2d[i]);
    fflush(stdout);

    int data_start[3] = {0,0,0};

    printf("proc%d: Data verfication ...", rank, read_data);
    if (data_verify(ndim, read_offset, read_region.size, data, data_start, read_data) != 1) 
        printf("FAILED!\n");
    else
        printf("SUCCEED!\n");
    

done:
    free(data);
    free(data_2d);

    free(read_data);
    free(read_data_2d);

    return 1;
}

int main(int argc, const char *argv[])
{

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

    test2d("test_obj_2d");

    /* test3d("test_obj_3d"); */

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
