/*
 * Copyright Notice for 
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***
 
 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.
 
 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.
 
 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

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

int main(int argc, char **argv)
{
    int rank = 0, size = 1;
    perr_t ret;
    pdcid_t pdc_id, cont_prop, cont_id, obj_prop1, obj_prop2, obj_prop3;
    pdcid_t obj1, obj2, obj3, r1, r2, r3;
    uint64_t dims[2] = {4,4};
    uint64_t offset[2] = {1, 2};
    uint64_t offset1[2] = {0, 0};
    uint64_t rdims[2] = {3, 2};
    char obj_name1[512];
    char obj_name2[512];
    char obj_name3[512];
    
//    int myArray1[4][4] = {{101, 102, 103, 104}, {105,106, 107, 108}, {109, 110, 111, 112}, {113, 114, 115, 116}};
    int myArray1[3][2] = {{107, 108}, {111, 112}, {115, 116}};
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
    pdc_id = PDC_init("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("c1", cont_prop);
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

//    PDCprop_set_obj_dims(obj_prop1, 2, dims);
    PDCprop_set_obj_dims(obj_prop2, 2, dims);
//    PDCprop_set_obj_dims(obj_prop3, 2, dims);

    PDCprop_set_obj_type(obj_prop2, PDC_INT);
//    PDCprop_set_obj_type(obj_prop3, PDC_INT);

	PDCprop_set_obj_buf(obj_prop2, &myArray2[0]     );
    PDCprop_set_obj_time_step(obj_prop2, 0          );
    PDCprop_set_obj_user_id( obj_prop2, getuid()    );
    PDCprop_set_obj_app_name(obj_prop2, "test_app"  );
    PDCprop_set_obj_tags(    obj_prop2, "tag0=1"    );

/*
	PDCprop_set_obj_buf(obj_prop3, &myArray3[0]     );
    PDCprop_set_obj_time_step(obj_prop3, 0          );
    PDCprop_set_obj_user_id( obj_prop3, getuid()    );
    PDCprop_set_obj_app_name(obj_prop3, "test_app"  );
    PDCprop_set_obj_tags(    obj_prop3, "tag0=1"    );
*/
    // Only rank 0 create a object
    if (rank == 0) {
//        sprintf(obj_name1, "test_obj1");
        sprintf(obj_name2, "test_obj2");
//       sprintf(obj_name3, "test_obj3");
//        obj1 = PDCobj_create(cont_id, obj_name1, obj_prop1);
        obj2 = PDCobj_create(cont_id, obj_name2, obj_prop2);
//        obj3 = PDCobj_create(cont_id, obj_name3, obj_prop3);
    }
    #ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    #endif

    // create a region
    r1 = PDCregion_create(2, offset1, rdims);
//    printf("first region id: %lld\n", r1);
    r2 = PDCregion_create(2, offset, rdims);
//    printf("second region id: %lld\n", r2);
//    r3 = PDCregion_create(2, offset, rdims);
//    printf("second region id: %lld\n", r3);
    
//	PDCobj_map(obj1, r1, obj2, r2);
//	PDCobj_map(obj1, r1, obj3, r3);
    obj1 = PDCobj_buf_map(cont_id, &myArray1[0], PDC_INT, r1, obj2, r2);
//    PDCreg_unmap(obj1, r1);
//	PDCobj_map(obj2, r2, obj3, r3);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);

    ret = PDCreg_obtain_lock(obj1, r1, WRITE, NOBLOCK);
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
    
    ret = PDCreg_obtain_lock(obj2, r2, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region\n");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);

    ret = PDCreg_release_lock(obj1, r1, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region\n");
    printf("r1 is released\n");
    fflush(stdout);
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&end, 0);

    elapsed = (end.tv_sec-start_time.tv_sec)*1000000LL + end.tv_usec-start_time.tv_usec;
    total_lock_overhead = elapsed / 1000000.0;

    if (rank == 0) {
        printf("Total lock release overhead: %.6f\n", total_lock_overhead);
    }

    ret = PDCreg_release_lock(obj2, r2, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region\n");

    printf("mapped region2 is updated to: \n");
    printf("%d, %d\n", myArray2[1][2], myArray2[1][3]);
    printf("%d, %d\n", myArray2[2][2], myArray2[2][3]);
    printf("%d, %d\n", myArray2[3][2], myArray2[3][3]);

/*
    printf("mapped region3 is updated to: \n");
    printf("%d, %d\n", myArray3[1][2], myArray3[1][3]);
    printf("%d, %d\n", myArray3[2][2], myArray3[2][3]);
    printf("%d, %d\n", myArray3[3][2], myArray3[3][3]);
*/
    
    PDCreg_unmap(obj1, r1);

    // close a container
    if(PDCcont_close(cont_id) < 0)
        printf("fail to close container %lld\n", cont_id);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("successfully close container # %lld\n", cont); */

    // close a container property
    if(PDCprop_close(cont_prop) < 0)
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
