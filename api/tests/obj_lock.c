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
    int rank = 0, size = 1;
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    PDC_prop_t p;
    // create a pdc
    pdcid_t pdc = PDC_init(p);
    printf("create a new pdc, pdc id is: %lld\n", pdc);

    // create a container property
    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop > 0)
        printf("Create a container property, id is %lld\n", cont_prop);
    else
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    pdcid_t cont = PDCcont_create(pdc, "c1", cont_prop);
    if(cont > 0)
        printf("Create a container, id is %lld\n", cont);
    else
        printf("Fail to create container @ line  %d!\n", __LINE__);
    
    // create an object property
    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop > 0)
        printf("Create an object property, id is %lld\n", obj_prop);
    else
        printf("Fail to create object property @ line  %d!\n", __LINE__);
    
    // set object dimension
    uint64_t d[3] = {10, 20, 30};
    PDCprop_set_obj_dims(obj_prop, 3, d, pdc);
    PDC_obj_prop_t *op = PDCobj_prop_get_info(obj_prop, pdc);
    /* printf("# of dim = %d\n", op->ndim); */
    /* int i; */
    /* for(i=0; i<op->ndim; i++) { */
    /*     printf("dim%d: %lld\n", i, (op->dims)[i]); */
    /* } */
 
    // Only rank 0 create a object
    char obj_name[64];
    pdcid_t obj1;
    if (rank == 0) {
        sprintf(obj_name, "test_obj");
        obj1 = PDCobj_create(pdc, cont, obj_name, obj_prop);
        if(obj1 > 0)
            printf("Create an object, id is %lld\n", obj1);
        else
            printf("Fail to create object @ line  %d!\n", __LINE__);
    }
 
    // Lock Test
    PDC_region_info_t region_info;
    uint64_t start[3] = {10,10,10};
    uint64_t count[3] = {10,10,10};
    region_info.ndim = 3;
    region_info.offset = &start[0];
    region_info.size   = &count[0];

    PDC_region_info_t region_info1;
    uint64_t start1[3] = {11,11,11};
    uint64_t count1[3] = {5,5,5};
    region_info1.ndim = 3;
    region_info1.offset = &start1[0];
    region_info1.size   = &count1[0];

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // Query and get meta id
    pdc_metadata_t *metadata = NULL;
    pdcid_t meta_id;
    PDC_Client_query_metadata_name_timestep("test_obj", 0, &metadata);
    if (metadata != NULL) {
        meta_id = metadata->obj_id;
    }
    else {
        printf("Previously created object by rank 0 not found!\n");
        goto done;
    }

    pbool_t lock_status;
    /* // Obtain a read lock for region 0 */
    /* PDC_Client_obtain_region_lock(pdc, cont, meta_id, &region_info, READ, BLOCK, &lock_status); */
    /* if (lock_status == TRUE) */ 
    /*     printf("[%d] Succeed to lock for region (10,10,10) - (20,20,20) ... expected\n", rank); */
    /* else */
    /*     printf("[%d] Failed to obtain a lock for region (10,10,10) - (20,20,20) ... error\n", rank); */

#ifdef ENABLE_MPI
    // let other proc wait before rank 0 aquires the write lock
    if (rank != 0) 
        sleep(7);
#endif

    fflush(stdout);

    // Obtain a write lock for region 0
    PDC_Client_obtain_region_lock(pdc, cont, meta_id, &region_info, WRITE, BLOCK, &lock_status);
    if (lock_status == TRUE) 
        printf("[%d] Succeed to lock for region (10,10,10) - (20,20,20) ... expected\n", rank);
    else
        printf("[%d] Failed to obtain a lock for region (10,10,10) - (20,20,20) ... error\n", rank);
    fflush(stdout);

    // Obtain a write lock for region 1, this should fail
    PDC_Client_obtain_region_lock(pdc, cont, meta_id, &region_info1, WRITE, NOBLOCK, &lock_status);
    if (lock_status == TRUE) 
        printf("[%d] Succeed to obtain lock for region (11,11,11) - (15,15,15) ... error\n", rank);
    else
        printf("[%d] Failed to obtain lock for region (11,11,11) - (15,15,15) ... expected\n", rank);
    fflush(stdout);

    if (rank == 0) {
        printf("[%d] sleep for a moment before releasing the lock\n", rank);
        fflush(stdout);
        sleep(15);
    }

    // Release the lock for region 0
    PDC_Client_release_region_lock(pdc,cont, meta_id, &region_info, &lock_status);
    if (lock_status == TRUE) 
        printf("[%d] Succeed to release lock for region (10,10,10) - (20,20,20) ... expected\n", rank);
    else
        printf("[%d] Failed to release a lock for region (10,10,10) - (20,20,20) ... error\n", rank);
    fflush(stdout);

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
    else
       printf("PDC is closed\n");

done:
#ifdef ENABLE_MPI
     MPI_Finalize();
#endif
    return 0;
}
