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
    pdcid_t obj_prop1;
    pdcid_t obj_prop_xx, obj_prop_yy, obj_prop_zz, obj_prop_pxx, obj_prop_pyy, obj_prop_pzz;
    pdcid_t obj_x, obj_y, obj_z, obj_px, obj_py, obj_pz;
    pdcid_t obj_xx, obj_yy, obj_zz, obj_pxx, obj_pyy, obj_pzz;
    pdcid_t region_x, region_y, region_z, region_px, region_py, region_pz; 
    pdcid_t region_xx, region_yy, region_zz, region_pxx, region_pyy, region_pzz; 
    perr_t ret;
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    float *x, *y, *z, *xx, *yy, *zz;
    float *px, *py, *pz, *pxx, *pyy, *pzz;
    int *id1, *id2;
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
    y = (float *)malloc(numparticles*sizeof(float));
    z = (float *)malloc(numparticles*sizeof(float));

    px = (float *)malloc(numparticles*sizeof(float));
    py = (float *)malloc(numparticles*sizeof(float));
    pz = (float *)malloc(numparticles*sizeof(float));

    id1 = (int *)malloc(numparticles*sizeof(int));
    id2 = (int *)malloc(numparticles*sizeof(int));

    xx = (float *)malloc(numparticles*sizeof(float));
    yy = (float *)malloc(numparticles*sizeof(float));
    zz = (float *)malloc(numparticles*sizeof(float));

    pxx = (float *)malloc(numparticles*sizeof(float));
    pyy = (float *)malloc(numparticles*sizeof(float));
    pzz = (float *)malloc(numparticles*sizeof(float));

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
    obj_prop_xx = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop_xx, 1, dims);
    PDCprop_set_obj_type(obj_prop_xx, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_xx, &xx[0]  );
    PDCprop_set_obj_time_step(obj_prop_xx, 0       );
    PDCprop_set_obj_user_id( obj_prop_xx, getuid()    );
    PDCprop_set_obj_app_name(obj_prop_xx, "VPICIO"  );
    PDCprop_set_obj_tags(    obj_prop_xx, "tag0=1"    );

    obj_prop_yy = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_yy, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_yy, yy);

    obj_prop_zz = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_zz, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_zz, zz);

    obj_prop_pxx = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_pxx, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_pxx, pxx);

    obj_prop_pyy = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_pyy, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_pyy, pyy);

    obj_prop_pzz = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_pzz, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_pzz, pzz);

    obj_xx = PDCobj_create_mpi(cont_id, "obj-var-xx", obj_prop_xx, PDC_OBJ_GLOBAL);
    if (obj_xx < 0) {    
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
        exit(-1);
    }
   
    obj_yy = PDCobj_create_mpi(cont_id, "obj-var-yy", obj_prop_yy, PDC_OBJ_GLOBAL);
    if (obj_yy < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-yy");
        exit(-1);
    }
    obj_zz = PDCobj_create_mpi(cont_id, "obj-var-zz", obj_prop_zz, PDC_OBJ_GLOBAL);
    if (obj_zz < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-zz");
        exit(-1);
    }
    obj_pxx = PDCobj_create_mpi(cont_id, "obj-var-pxx", obj_prop_pxx, PDC_OBJ_GLOBAL);
    if (obj_pxx < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pxx");
        exit(-1);
    }
    obj_pyy = PDCobj_create_mpi(cont_id, "obj-var-pyy", obj_prop_pyy, PDC_OBJ_GLOBAL);
    if (obj_pyy < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pyy");
        exit(-1);
    }
    obj_pzz = PDCobj_create_mpi(cont_id, "obj-var-pzz", obj_prop_pzz, PDC_OBJ_GLOBAL);
    if (obj_pzz < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pzz");
        exit(-1);
    }

//    pdc_metadata_t *res = NULL;
//    PDC_Client_query_metadata_name_only("obj-var-xx", &res);
//    printf("rank %d: meta id is %lld\n", rank, res->obj_id);

    offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0] = rank * my_data_size/size;
    mysize[0] = my_data_size/size;

    // create a region
    region_x = PDCregion_create(1, offset, mysize);
    region_y = PDCregion_create(ndim, offset, mysize);
    region_z = PDCregion_create(ndim, offset, mysize);
    region_px = PDCregion_create(ndim, offset, mysize);
    region_py = PDCregion_create(ndim, offset, mysize);
    region_pz = PDCregion_create(ndim, offset, mysize);

    region_xx = PDCregion_create(1, offset, mysize);
    region_yy = PDCregion_create(ndim, offset, mysize);
    region_zz = PDCregion_create(ndim, offset, mysize);
    region_pxx = PDCregion_create(ndim, offset, mysize);
    region_pyy = PDCregion_create(ndim, offset, mysize);
    region_pzz = PDCregion_create(ndim, offset, mysize);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    obj_x = PDCobj_buf_map(cont_id, "obj-var-x", &x[0], PDC_FLOAT, region_x, obj_xx, region_xx);
    obj_y = PDCobj_buf_map(cont_id, "obj-var-y", &y[0], PDC_FLOAT, region_y, obj_yy, region_yy);
    obj_z = PDCobj_buf_map(cont_id, "obj-var-z", &z[0], PDC_FLOAT, region_z, obj_zz, region_zz);
    obj_px = PDCobj_buf_map(cont_id, "obj-var-px", &px[0], PDC_FLOAT, region_px, obj_pxx, region_pxx);
    obj_py = PDCobj_buf_map(cont_id, "obj-var-py", &py[0], PDC_FLOAT, region_py, obj_pyy, region_pyy);
    obj_pz = PDCobj_buf_map(cont_id, "obj-var-pz", &pz[0], PDC_FLOAT, region_pz, obj_pzz, region_pzz);    
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    ret = PDCreg_obtain_lock(obj_xx, region_xx, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_xx\n");
    ret = PDCreg_obtain_lock(obj_yy, region_yy, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_yy\n");
    ret = PDCreg_obtain_lock(obj_zz, region_zz, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_zz\n");
    ret = PDCreg_obtain_lock(obj_pxx, region_pxx, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_pxx\n");
    ret = PDCreg_obtain_lock(obj_pyy, region_pyy, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_pyy\n");
    ret = PDCreg_obtain_lock(obj_pzz, region_pzz, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_pzz\n");

    for (int i=0; i<numparticles; i++) {
        x[i]   = uniform_random_number() * x_dim;
        y[i]   = uniform_random_number() * y_dim;
        z[i]   = ((float)id1[i]/numparticles) * z_dim;
        px[i]  = uniform_random_number() * x_dim;
        py[i]  = uniform_random_number() * y_dim;
        pz[i]  = ((float)id2[i]/numparticles) * z_dim;
        xx[i]  = 0;
        yy[i]  = 0;
        zz[i]  = 0;
        pxx[i] = 0;
        pyy[i] = 0;
        pzz[i] = 0;       
printf("x = %f\n", x[i]);
    }

    ret = PDCreg_release_lock(obj_xx, region_xx, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_y\n");
    ret = PDCreg_release_lock(obj_yy, region_yy, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_yy\n");
    ret = PDCreg_release_lock(obj_zz, region_zz, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_zz\n");
    ret = PDCreg_release_lock(obj_pxx, region_pxx, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_pxx\n");
    ret = PDCreg_release_lock(obj_pyy, region_pyy, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_pyy\n");
    ret = PDCreg_release_lock(obj_pzz, region_pzz, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_pzz\n");

for (int i=0; i<numparticles; i++) {
printf("xx = %f\n", xx[i]);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    ret = PDCreg_unmap(obj_x, region_x);
    PDCreg_unmap(obj_y, region_y);
    PDCreg_unmap(obj_z, region_z);
    PDCreg_unmap(obj_px, region_px);
    PDCreg_unmap(obj_py, region_py);
    PDCreg_unmap(obj_pz, region_pz);
    if (ret != SUCCEED)
        printf("region unmap failed\n");

    if(PDCobj_close(obj_x) < 0)
        printf("fail to close obj_x %lld\n", obj_x);

    if(PDCobj_close(obj_y) < 0)
        printf("fail to close object %lld\n", obj_y);
  
    if(PDCobj_close(obj_z) < 0)
        printf("fail to close object %lld\n", obj_z);

    if(PDCobj_close(obj_px) < 0)
        printf("fail to close object %lld\n", obj_px);

    if(PDCobj_close(obj_py) < 0)
        printf("fail to close object %lld\n", obj_py);

    if(PDCobj_close(obj_pz) < 0)
        printf("fail to close object %lld\n", obj_pz);

    if(PDCobj_close(obj_xx) < 0)
        printf("fail to close obj_xx %lld\n", obj_xx);

    if(PDCobj_close(obj_yy) < 0)
            printf("fail to close object %lld\n", obj_yy);

    if(PDCobj_close(obj_zz) < 0)
        printf("fail to close object %lld\n", obj_zz);

    if(PDCobj_close(obj_pxx) < 0)
        printf("fail to close object %lld\n", obj_pxx);

    if(PDCobj_close(obj_pyy) < 0)
        printf("fail to close object %lld\n", obj_pyy);
    
    if(PDCobj_close(obj_pzz) < 0)
        printf("fail to close object %lld\n", obj_pzz);
    
    if(PDCprop_close(obj_prop_xx) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_xx);

    if(PDCprop_close(obj_prop_yy) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_yy);

    if(PDCprop_close(obj_prop_zz) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_zz);

    if(PDCprop_close(obj_prop_pxx) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_pxx);

    if(PDCprop_close(obj_prop_pyy) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_pyy);

    if(PDCprop_close(obj_prop_pzz) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_pzz);
    
    if(PDCregion_close(region_x) < 0)
        printf("fail to close region %lld\n", region_x);

    if(PDCregion_close(region_y) < 0)
        printf("fail to close region %lld\n", region_y);

    if(PDCregion_close(region_z) < 0)
        printf("fail to close region %lld\n", region_z);

    if(PDCregion_close(region_px) < 0)
        printf("fail to close region %lld\n", region_px);

    if(PDCregion_close(region_py) < 0)
        printf("fail to close region %lld\n", region_py);
    
    if(PDCobj_close(region_pz) < 0)
        printf("fail to close region %lld\n", region_pz);

    if(PDCregion_close(region_xx) < 0)
        printf("fail to close region %lld\n", region_xx);
        
    if(PDCregion_close(region_yy) < 0)
        printf("fail to close region %lld\n", region_yy);
        
    if(PDCregion_close(region_zz) < 0)
        printf("fail to close region %lld\n", region_zz);
        
    if(PDCregion_close(region_pxx) < 0)
        printf("fail to close region %lld\n", region_pxx);
    
    if(PDCregion_close(region_pyy) < 0)
        printf("fail to close region %lld\n", region_pyy);
    
    if(PDCregion_close(region_pzz) < 0)
        printf("fail to close region %lld\n", region_pzz);

    // close a container
    if(PDCcont_close(cont_id) < 0)
        printf("fail to close container %lld\n", cont_id);

    // close a container property
    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc_id) < 0)
       printf("fail to close PDC\n");

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}

