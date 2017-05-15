#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pdc.h"
#include  "pdc_client_connect.h"
#include  "pdc_client_server_common.h"

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

int main(int argc, const char *argv[])
{
    int rank = 0, size = 1, i;
    struct PDC_prop p;
    pdcid_t pdc, cont_prop, cont, obj_prop, obj1;
    uint64_t d[3] = {10, 20, 30};
    char obj_name[64];
    pbool_t lock_status;
    struct PDC_obj_prop *op;
    
    struct PDC_region_info *region;
    struct PDC_region_info region_info;
    uint64_t start[3] = {10,10,10};
    uint64_t count[3] = {10,10,10};
    
    struct PDC_region_info region_info1;
    uint64_t start1[3] = {11,11,11};
    uint64_t count1[3] = {5,5,5};
    
    struct PDC_region_info region_info_no_overlap;
    uint64_t start_no_overlap[3] = {1,1,1};
    uint64_t count_no_overlap[3] = {1,1,1};
    
    struct timeval  start_time;
    struct timeval  end;
    long long elapsed;
    double total_lock_overhead;
    
    pdc_metadata_t *metadata = NULL;
    pdcid_t meta_id;
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc = PDC_init(p);
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);
    
    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);
    
    // set object dimension
    PDCprop_set_obj_dims(obj_prop, 3, d, pdc);
    PDCprop_set_obj_time_step(obj_prop, 0, pdc); 
    op = PDCobj_prop_get_info(obj_prop, pdc);
    /* printf("# of dim = %d\n", op->ndim); */
    /* int i; */
    /* for(i=0; i<op->ndim; i++) { */
    /*     printf("dim%d: %lld\n", i, (op->dims)[i]); */
    /* } */
 
    // Only rank 0 create a object
    if (rank == 0) {
        sprintf(obj_name, "test_obj");
        obj1 = PDCobj_create(pdc, cont, obj_name, obj_prop);
        if(obj1 <= 0)
            printf("Fail to create object @ line  %d!\n", __LINE__);
        /* else */
        /*     printf("Created object!\n"); */
    }
 
    // Lock Test
    region_info.ndim = 3;
    region_info.offset = &start[0];
    region_info.size   = &count[0];

    region_info1.ndim = 3;
    region_info1.offset = &start1[0];
    region_info1.size   = &count1[0];

    for (i = 0; i < 3; i++) {
        start_no_overlap[i] *= rank;
    }
    region_info_no_overlap.ndim = 3;
    region_info_no_overlap.offset = &start_no_overlap[0];
    region_info_no_overlap.size   = &count_no_overlap[0];

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query and get meta id
    PDC_Client_query_metadata_name_timestep("test_obj", 0, &metadata);
    if (metadata != NULL) {
        meta_id = metadata->obj_id;
    }
    else {
        printf("Previously created object by rank 0 not found!\n");
        goto done;
    }

    /* // Obtain a read lock for region 0 */
    /* PDC_Client_obtain_region_lock(pdc, cont, meta_id, &region_info, READ, BLOCK, &lock_status); */
    /* if (lock_status == TRUE) */ 
    /*     printf("[%d] Succeed to lock for region (10,10,10) - (20,20,20) ... expected\n", rank); */
    /* else */
    /*     printf("[%d] Failed to obtain a lock for region (10,10,10) - (20,20,20) ... error\n", rank); */
    /* fflush(stdout); */

/* #ifdef ENABLE_MPI */
/*     // let other proc wait before rank 0 aquires the write lock */
/*     if (rank != 0) */ 
/*         sleep(7); */
/* #endif */

    // Obtain a write lock for region 0
    /* PDC_Client_obtain_region_lock(pdc, cont, meta_id, &region_info, WRITE, BLOCK, &lock_status); */
    /* if (lock_status == TRUE) */ 
    /*     printf("[%d] Succeed to lock for region (10,10,10) - (20,20,20) ... expected\n", rank); */
    /* else */
    /*     printf("[%d] Failed to obtain a lock for region (10,10,10) - (20,20,20) ... error\n", rank); */
    /* fflush(stdout); */

    /* // Obtain a write lock for region 1, this should fail */
    /* PDC_Client_obtain_region_lock(pdc, cont, meta_id, &region_info1, WRITE, NOBLOCK, &lock_status); */
    /* if (lock_status == TRUE) */ 
    /*     printf("[%d] Succeed to obtain lock for region (11,11,11) - (15,15,15) ... error\n", rank); */
    /* else */
    /*     printf("[%d] Failed to obtain lock for region (11,11,11) - (15,15,15) ... expected\n", rank); */
    /* fflush(stdout); */

    /* if (rank == 0) { */
    /*     printf("[%d] sleep for a moment before releasing the lock\n", rank); */
    /*     fflush(stdout); */
    /*     sleep(15); */
    /* } */

    /* // Release the lock for region 0 */
    /* PDC_Client_release_region_lock(pdc,cont, meta_id, &region_info, &lock_status); */
    /* if (lock_status == TRUE) */ 
    /*     printf("[%d] Succeed to release lock for region (10,10,10) - (20,20,20) ... expected\n", rank); */
    /* else */
    /*     printf("[%d] Failed to release a lock for region (10,10,10) - (20,20,20) ... error\n", rank); */
    /* fflush(stdout); */

    /* // Obtain a write lock for region 1 */
    /* PDC_Client_obtain_region_lock(pdc, cont, meta_id, &region_info1, WRITE, BLOCK, &lock_status); */
    /* if (lock_status == TRUE) */ 
    /*     printf("[%d] Succeed to obtain lock for region (11,11,11) - (15,15,15) ... expected\n", rank); */
    /* else */
    /*     printf("[%d] Failed to obtain lock for region (11,11,11) - (15,15,15) ... error\n", rank); */
    /* fflush(stdout); */

    /* // Release the lock for region 1 */
    /* PDC_Client_release_region_lock(pdc,cont, meta_id, &region_info1, &lock_status); */
    /* if (lock_status == TRUE) */ 
    /*     printf("[%d] Succeed to release lock for region (10,10,10) - (20,20,20) ... expected\n", rank); */
    /* else */
    /*     printf("[%d] Failed to release a lock for region (10,10,10) - (20,20,20) ... error\n", rank); */
    /* fflush(stdout); */

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);
      
    region = &region_info_no_overlap;
    region->mapping = 0;
    PDC_Client_obtain_region_lock(pdc, cont, meta_id, region, WRITE, NOBLOCK, &lock_status);
    if (lock_status != TRUE) 
        printf("[%d] Failed to obtain lock for region (%lld,%lld,%lld) (%lld,%lld,%lld) ... error\n", rank,
                region->offset[0], region->offset[1], region->offset[2], region->size[0], region->size[1], region->size[2]);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&end, 0);

    elapsed = (end.tv_sec-start_time.tv_sec)*1000000LL + end.tv_usec-start_time.tv_usec;
    total_lock_overhead = elapsed / 1000000.0;

    if (rank == 0) {
        printf("Total lock overhead        : %.6f\n", total_lock_overhead);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);
      
    region = &region_info_no_overlap;
    PDC_Client_release_region_lock(pdc, cont, meta_id, region, WRITE, &lock_status);
    if (lock_status != TRUE) 
        printf("[%d] Failed to release lock for region (%d,%d,%d) (%d,%d,%d) ... error\n", rank, 
                region->offset[0], region->offset[1], region->offset[2], region->size[0], region->size[1], region->size[2]);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&end, 0);

    elapsed = (end.tv_sec-start_time.tv_sec)*1000000LL + end.tv_usec-start_time.tv_usec;
    total_lock_overhead = elapsed / 1000000.0;

    if (rank == 0) {
        printf("Total lock release overhead: %.6f\n", total_lock_overhead);
    }
      
    // close object
    if (rank == 0) {
        if(PDCobj_close(obj1, pdc) < 0)
            printf("fail to close object %lld\n", obj1);
    }
    
    // close object property
    if(PDCprop_close(obj_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
       
    // close a container
    if(PDCcont_close(cont, pdc) < 0)
        printf("fail to close container %lld\n", cont);
    

    // close a container property
    if(PDCprop_close(cont_prop, pdc) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    // close pdc
    if(PDC_close(pdc) < 0)
       printf("fail to close PDC\n");

done:
#ifdef ENABLE_MPI
     MPI_Finalize();
#endif
    return 0;
}
