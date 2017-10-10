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
#include <time.h>
#include <sys/time.h>
#include <math.h>

/* #define ENABLE_MPI 1 */

#ifdef ENABLE_MPI
  #include "mpi.h"
#endif

#include "pdc.h"

double uniform_random_number()
{
    return (((double)rand())/((double)(RAND_MAX)));
}

int main(int argc, char **argv)
{
    int rank = 0, size = 1;
    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t obj_prop1, obj_prop2;
    pdcid_t obj1, obj2;
    pdcid_t meta1, meta2;
    pdcid_t r1, r2;
    perr_t ret;
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    float *x, *xx;
    int x_dim = 64;
    int y_dim = 64;
    int z_dim = 64;
    long numparticles = 4;
    const int my_data_size = 4;
    uint64_t dims[1] = {my_data_size};  // {8388608};
    int ndim = 1;
    uint64_t *offset;
    uint64_t *mysize;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    x = (float *)malloc(numparticles*sizeof(float));
    xx = (float *)malloc(numparticles*sizeof(float));

    // create a pdc
    pdc_id = PDC_init("pdc");
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("Create a container property, id is %lld\n", cont_prop); */

    // create a container
    cont_id = PDCcont_create("c1", cont_prop);
    if(cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);
    /* else */
    /*     if (rank == 0) */ 
    /*         printf("Create a container, id is %lld\n", cont_id); */

    // create an object property
    obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop1, 1, dims);
    PDCprop_set_obj_dims(obj_prop2, 1, dims);

    PDCprop_set_obj_type(obj_prop1, PDC_FLOAT);
    PDCprop_set_obj_type(obj_prop2, PDC_FLOAT);

    PDCprop_set_obj_buf(obj_prop1, &x[0]  );
    PDCprop_set_obj_time_step(obj_prop1, 0       );
    PDCprop_set_obj_user_id( obj_prop1, getuid()    );
    PDCprop_set_obj_app_name(obj_prop1, "VPICIO"  );
    PDCprop_set_obj_tags(    obj_prop1, "tag0=1"    );

	PDCprop_set_obj_buf(obj_prop2, &xx[0]  );
    PDCprop_set_obj_time_step(obj_prop2, 0       );
    PDCprop_set_obj_user_id( obj_prop2, getuid()    );
    PDCprop_set_obj_app_name(obj_prop2, "VPICIO"  );
    PDCprop_set_obj_tags(    obj_prop2, "tag0=1"    );

    obj1 = PDCobj_create_(cont_id, "obj-var-x", obj_prop1, PDC_GLOBAL);
    if (obj1 < 0) { 
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-x");
        exit(-1);
    }

    obj2 = PDCobj_create_(cont_id, "obj-var-xx", obj_prop2, PDC_GLOBAL);
    if (obj2 < 0) {    
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
        exit(-1);
    }

    PDCobj_encode(obj1, &meta1);
    PDCobj_encode(obj2, &meta2);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    MPI_Bcast(&meta1, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    MPI_Bcast(&meta2, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    PDCobj_decode(obj1, meta1);
    PDCobj_decode(obj2, meta2);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0] = rank * my_data_size/size;
    mysize[0] = my_data_size/size;

    // create a region
    r1 = PDCregion_create(1, offset, mysize);
//    printf("first region id: %lld\n", r1);
    r2 = PDCregion_create(1, offset, mysize);
//    printf("second region id: %lld\n", r2);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

	PDCobj_map(obj1, r1, obj2, r2);

    ret = PDCreg_obtain_lock(obj1, r1, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for r1\n");

    for (int i=0; i<numparticles; i++) {
        x[i]   = uniform_random_number() * x_dim;
        xx[i]  = 0;
printf("x = %f\n", x[i]);
    }

    ret = PDCreg_obtain_lock(obj2, r2, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for r2\n");

    ret = PDCreg_release_lock(obj1, r1, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_x\n");
    ret = PDCreg_release_lock(obj2, r2, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_y\n");

for (int i=0; i<numparticles; i++) {
printf("xx = %f\n", xx[i]);
    }

    ret = PDCreg_unmap(obj1, r1);
//    ret = PDCreg_unmap(obj1);
    if (ret != SUCCEED)
        printf("region unmap failed\n");

    if(PDCobj_close(obj1) < 0)
        printf("fail to close obj1 %lld\n", obj1);

    if(PDCobj_close(obj2) < 0)
        printf("fail to close obj2 %lld\n", obj2);

    // close a container
    if(PDCcont_close(cont_id) < 0)
        printf("fail to close container %lld\n", cont_id);


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

