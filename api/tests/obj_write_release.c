#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/time.h>

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1;
    perr_t ret;
    struct PDC_prop p;
    pdcid_t pdc_id, cont_prop, cont_id, obj_prop1, obj_prop2, obj_prop3;
    pdcid_t obj1, obj2, obj3, r1, r2, r3, meta_id;
    uint64_t dims[2] = {4,4};
    uint64_t offset[2] = {1, 2};
    uint64_t rdims[2] = {3, 2};
    char obj_name1[512];
    char obj_name2[512];
    char obj_name3[512];
    char obj_name[64];
    
    int myArray1[4][4] = {{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}, {13, 14, 15, 16}};
    int myArray2[4][4];
    int myArray3[4][4];
    
    struct timeval  start_time;
    struct timeval  end;
    long long elapsed;
    double total_lock_overhead;
    
//    pbool_t lock_status;
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc_id = PDC_init(p);

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create(pdc_id, "c1", cont_prop);
    if(cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    obj_prop3 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

//	char srank[10];
//	sprintf(srank, "%d", rank);
//	sprintf(obj_name1, "%s%s", rand_string(tmp_str, 16), srank);
//	sprintf(obj_name2, "%s%s", rand_string(tmp_str, 16), srank);
//	sprintf(obj_name3, "%s%s", rand_string(tmp_str, 16), srank);

    PDCprop_set_obj_dims(obj_prop1, 2, dims, pdc_id);
    PDCprop_set_obj_dims(obj_prop2, 2, dims, pdc_id);
    PDCprop_set_obj_dims(obj_prop3, 2, dims, pdc_id);

    PDCprop_set_obj_type(obj_prop1, PDC_INT, pdc_id);
    PDCprop_set_obj_type(obj_prop2, PDC_INT, pdc_id);
    PDCprop_set_obj_type(obj_prop3, PDC_INT, pdc_id);

    PDCprop_set_obj_buf(obj_prop1, &myArray1[0], pdc_id);
    PDCprop_set_obj_time_step(obj_prop1, 0, pdc_id);
    PDCprop_set_obj_user_id( obj_prop1, getuid(),    pdc_id);
    PDCprop_set_obj_app_name(obj_prop1, "test_app",  pdc_id);
    PDCprop_set_obj_tags(    obj_prop1, "tag0=1",    pdc_id);

	PDCprop_set_obj_buf(obj_prop2, &myArray2[0], pdc_id);
    PDCprop_set_obj_time_step(obj_prop2, 0, pdc_id);
    PDCprop_set_obj_user_id( obj_prop2, getuid(),    pdc_id);
    PDCprop_set_obj_app_name(obj_prop2, "test_app",  pdc_id);
    PDCprop_set_obj_tags(    obj_prop2, "tag0=1",    pdc_id);

	PDCprop_set_obj_buf(obj_prop3, &myArray3[0], pdc_id);
    PDCprop_set_obj_time_step(obj_prop3, 0, pdc_id);
    PDCprop_set_obj_user_id( obj_prop3, getuid(),    pdc_id);
    PDCprop_set_obj_app_name(obj_prop3, "test_app",  pdc_id);
    PDCprop_set_obj_tags(    obj_prop3, "tag0=1",    pdc_id);

    // Only rank 0 create a object
    if (rank == 0) {
        sprintf(obj_name1, "test_obj1");
        sprintf(obj_name2, "test_obj2");
        sprintf(obj_name3, "test_obj3");
        obj1 = PDCobj_create(pdc_id, cont_id, obj_name1, obj_prop1);
        obj2 = PDCobj_create(pdc_id, cont_id, obj_name2, obj_prop2);
        obj3 = PDCobj_create(pdc_id, cont_id, obj_name3, obj_prop3);
    }
    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif

    // create a region
    r1 = PDCregion_create(2, offset, rdims, pdc_id);
//    printf("first region id: %lld\n", r1);
    r2 = PDCregion_create(2, offset, rdims, pdc_id);
//    printf("second region id: %lld\n", r2);
    r3 = PDCregion_create(2, offset, rdims, pdc_id);
//    printf("second region id: %lld\n", r3);

	PDCobj_map(obj1, r1, obj2, r2, pdc_id);
//	PDCobj_map(obj2, r2, obj3, r3, pdc_id);
//    PDCreg_unmap(obj1, r1, pdc_id);
//	PDCobj_map(obj2, r2, obj3, r3, pdc_id);

//    region = PDCregion_get_info(r1, obj1, pdc_id);

//    info = PDCobj_get_info(obj1, pdc_id);
//    meta_id = info->meta_id;
//    printf("meta id is %lld\n", info->meta_id);

/*
    // Query and get meta id
    pdc_metadata_t *metadata = NULL;
    pdcid_t meta_id;
    PDC_Client_query_metadata_name_only("test_obj1", &metadata);
    if (metadata != NULL) {
        meta_id = metadata->obj_id;
    }
    else {
        printf("Previously created object test_obj1 by rank 0 not found!\n");
        goto done;
    }
*/
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);

    ret = PDCreg_obtain_lock(pdc_id, cont_id, obj1, r1, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region\n");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&end, 0);

    elapsed = (end.tv_sec-start_time.tv_sec)*1000000LL + end.tv_usec-start_time.tv_usec;
    total_lock_overhead = elapsed / 1000000.0;

    if (rank == 0) {
        printf("Total lock obtain overhead:  %.6f\n", total_lock_overhead);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);

    ret = PDCreg_release_lock(pdc_id, cont_id, obj1, r1, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region\n");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&end, 0);

    elapsed = (end.tv_sec-start_time.tv_sec)*1000000LL + end.tv_usec-start_time.tv_usec;
    total_lock_overhead = elapsed / 1000000.0;

    if (rank == 0) {
        printf("Total lock release overhead: %.6f\n", total_lock_overhead);
    }
    
    PDCreg_unmap(obj1, r1, pdc_id);

    // close a container
    if(PDCcont_close(cont_id, pdc_id) < 0)
        printf("fail to close container %lld\n", cont_id);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("successfully close container # %lld\n", cont); */

    // close a container property
    if(PDCprop_close(cont_prop, pdc_id) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("successfully close container property # %lld\n", cont_prop); */

    if(PDC_close(pdc_id) < 0)
       printf("fail to close PDC\n");
    /* else */
    /*    printf("PDC is closed\n"); */

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}
