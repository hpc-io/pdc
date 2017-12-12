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
    pdcid_t obj_prop_xx, obj_prop_yy, obj_prop_zz, obj_prop_pxx, obj_prop_pyy, obj_prop_pzz, obj_prop_id11, obj_prop_id22;
    pdcid_t obj_x, obj_y, obj_z, obj_px, obj_py, obj_pz, obj_id1, obj_id2;
    pdcid_t obj_xx, obj_yy, obj_zz, obj_pxx, obj_pyy, obj_pzz, obj_id11, obj_id22;
    pdcid_t region_x, region_y, region_z, region_px, region_py, region_pz, region_id1, region_id2; 
    pdcid_t region_xx, region_yy, region_zz, region_pxx, region_pyy, region_pzz, region_id11, region_id22; 
    perr_t ret;
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    float *x, *y, *z, *xx, *yy, *zz;
    float *px, *py, *pz, *pxx, *pyy, *pzz;
    int *id1, *id2, *id11, *id22;
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

    id11 = (int *)malloc(numparticles*sizeof(int));
    id22 = (int *)malloc(numparticles*sizeof(int));

    // create a pdc
    pdc_id = PDC_init("pdc");
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("c1", cont_prop);
    if(cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
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

    obj_prop_id11 = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_id11, PDC_INT);
    PDCprop_set_obj_buf(obj_prop_id11, id11);

    obj_prop_id22 = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_id22, PDC_INT);
    PDCprop_set_obj_buf(obj_prop_id22, id22);

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

    obj_id11 = PDCobj_create_mpi(cont_id, "id11", obj_prop_id11, PDC_OBJ_GLOBAL);
    if (obj_id11 < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj_id11");
        exit(-1);
    }
    obj_id22 = PDCobj_create_mpi(cont_id, "id22", obj_prop_id22, PDC_OBJ_GLOBAL);
    if (obj_id22 < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj_id22");
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
    region_x = PDCregion_create(ndim, offset, mysize);
    region_y = PDCregion_create(ndim, offset, mysize);
    region_z = PDCregion_create(ndim, offset, mysize);
    region_px = PDCregion_create(ndim, offset, mysize);
    region_py = PDCregion_create(ndim, offset, mysize);
    region_pz = PDCregion_create(ndim, offset, mysize);
    region_id1 = PDCregion_create(ndim, offset, mysize);
    region_id2 = PDCregion_create(ndim, offset, mysize);

    region_xx = PDCregion_create(ndim, offset, mysize);
    region_yy = PDCregion_create(ndim, offset, mysize);
    region_zz = PDCregion_create(ndim, offset, mysize);
    region_pxx = PDCregion_create(ndim, offset, mysize);
    region_pyy = PDCregion_create(ndim, offset, mysize);
    region_pzz = PDCregion_create(ndim, offset, mysize);
    region_id11 = PDCregion_create(ndim, offset, mysize);
    region_id22 = PDCregion_create(ndim, offset, mysize);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    obj_x = PDCobj_buf_map(cont_id, "obj-var-x", &x[0], PDC_FLOAT, region_x, obj_xx, region_xx);
    obj_y = PDCobj_buf_map(cont_id, "obj-var-y", &y[0], PDC_FLOAT, region_y, obj_yy, region_yy);
    obj_z = PDCobj_buf_map(cont_id, "obj-var-z", &z[0], PDC_FLOAT, region_z, obj_zz, region_zz);
    obj_px = PDCobj_buf_map(cont_id, "obj-var-px", &px[0], PDC_FLOAT, region_px, obj_pxx, region_pxx);
    obj_py = PDCobj_buf_map(cont_id, "obj-var-py", &py[0], PDC_FLOAT, region_py, obj_pyy, region_pyy);
    obj_pz = PDCobj_buf_map(cont_id, "obj-var-pz", &pz[0], PDC_FLOAT, region_pz, obj_pzz, region_pzz);    
    obj_id1 = PDCobj_buf_map(cont_id, "id1", &id1[0], PDC_INT, region_id1, obj_id11, region_id11);
    obj_id2 = PDCobj_buf_map(cont_id, "id2", &id2[0], PDC_INT, region_id2, obj_id22, region_id22);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to map with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);
    
    ret = PDCreg_obtain_lock(obj_x, region_x, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_x\n");
    ret = PDCreg_obtain_lock(obj_y, region_y, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_y\n");
    ret = PDCreg_obtain_lock(obj_z, region_z, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_z\n");
    ret = PDCreg_obtain_lock(obj_px, region_px, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_px\n");
    ret = PDCreg_obtain_lock(obj_py, region_py, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_py\n");
    ret = PDCreg_obtain_lock(obj_pz, region_pz, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_pz\n");
    ret = PDCreg_obtain_lock(obj_id1, region_id1, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_id1\n");
    ret = PDCreg_obtain_lock(obj_id2, region_id2, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_id2\n");

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
    ret = PDCreg_obtain_lock(obj_id11, region_id11, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_id11\n");
    ret = PDCreg_obtain_lock(obj_id22, region_id22, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_id22\n");
    
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to lock with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

     for (int i=0; i<numparticles; i++) {
        id1[i] = i;
        id2[i] = i*2;
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
        id11[i] = 0;
        id22[i] = 0;
//printf("px = %f\n", px[i]);
//printf("id1 = %d\n", id1[i]);
//printf("id2 = %d\n", id2[i]);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);
    
    ret = PDCreg_release_lock(obj_x, region_x, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_x\n");
    ret = PDCreg_release_lock(obj_y, region_y, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_y\n");
    ret = PDCreg_release_lock(obj_z, region_z, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_z\n");
    ret = PDCreg_release_lock(obj_px, region_px, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_px\n");
    ret = PDCreg_release_lock(obj_py, region_py, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_py\n");
    ret = PDCreg_release_lock(obj_pz, region_pz, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_pz\n");
    ret = PDCreg_release_lock(obj_id1, region_id1, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_id1\n");
    ret = PDCreg_release_lock(obj_id2, region_id2, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_id2\n");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to write data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);
    
    ret = PDCreg_release_lock(obj_xx, region_xx, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_xx\n");
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
    ret = PDCreg_release_lock(obj_id11, region_id11, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_id11\n");
    ret = PDCreg_release_lock(obj_id22, region_id22, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_id22\n");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to update data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }
    
//for (int i=0; i<numparticles; i++) {
//printf("id11 = %d\n", id11[i]);
//printf("id22 = %d\n", id22[i]);
//    }


    for (int i=0; i<my_data_size/size; i++) {
        if(xx[rank * my_data_size/size+i] != x[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: x data does not match\n", rank);
        if(yy[rank * my_data_size/size+i] != y[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: y data does not match\n", rank);
        if(zz[rank * my_data_size/size+i] != z[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: z data does not match\n", rank);
        if(pxx[rank * my_data_size/size+i] != px[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: px data does not match\n", rank);
        if(pyy[rank * my_data_size/size+i] != py[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: py data does not match\n", rank);
        if(pzz[rank * my_data_size/size+i] != pz[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: pz data does not match\n", rank);
        if(id11[rank * my_data_size/size+i] != id1[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: id1 data does not match\n", rank);
        if(id22[rank * my_data_size/size+i] != id2[rank * my_data_size/size+i])
            printf("== ERROR == rank %d: id2 data does not match\n", rank);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    PDCreg_unmap(obj_x, region_x);
    PDCreg_unmap(obj_y, region_y);
    PDCreg_unmap(obj_z, region_z);
    PDCreg_unmap(obj_px, region_px);
    PDCreg_unmap(obj_py, region_py);
    PDCreg_unmap(obj_pz, region_pz);
    PDCreg_unmap(obj_id1, region_id1);
    PDCreg_unmap(obj_id2, region_id2);

    if (ret != SUCCEED)
        printf("region unmap failed\n");

    if(PDCobj_close(obj_x) < 0)
        printf("fail to close obj_x\n");

    if(PDCobj_close(obj_y) < 0)
        printf("fail to close object obj_y\n");

    if(PDCobj_close(obj_z) < 0)
        printf("fail to close object obj_z\n");

    if(PDCobj_close(obj_px) < 0)
        printf("fail to close object obj_px\n");

    if(PDCobj_close(obj_py) < 0)
        printf("fail to close object obj_py\n");

    if(PDCobj_close(obj_pz) < 0)
        printf("fail to close object obj_pz\n");

    if(PDCobj_close(obj_id1) < 0)
        printf("fail to close object obj_id1\n");

    if(PDCobj_close(obj_id2) < 0)
        printf("fail to close object obj_id2\n");

    if(PDCobj_close(obj_xx) < 0)
        printf("fail to close obj_xx\n");

    if(PDCobj_close(obj_yy) < 0)
        printf("fail to close object obj_yy\n");

    if(PDCobj_close(obj_zz) < 0)
        printf("fail to close object obj_zz\n");

    if(PDCobj_close(obj_pxx) < 0)
        printf("fail to close object obj_pxx\n");

    if(PDCobj_close(obj_pyy) < 0)
        printf("fail to close object obj_pyy\n");

    if(PDCobj_close(obj_pzz) < 0)
        printf("fail to close object obj_pzz\n");

    if(PDCobj_close(obj_id11) < 0)
        printf("fail to close object obj_id11\n");

    if(PDCobj_close(obj_id22) < 0)
        printf("fail to close object obj_id22\n");

    if(PDCprop_close(obj_prop_xx) < 0)
        printf("Fail to close obj property obj_prop_xx\n");

    if(PDCprop_close(obj_prop_yy) < 0)
        printf("Fail to close obj property obj_prop_yy\n");

    if(PDCprop_close(obj_prop_zz) < 0)
        printf("Fail to close obj property obj_prop_zz\n");

    if(PDCprop_close(obj_prop_pxx) < 0)
        printf("Fail to close obj property obj_prop_pxx\n");

    if(PDCprop_close(obj_prop_pyy) < 0)
        printf("Fail to close obj property obj_prop_pyy\n");

    if(PDCprop_close(obj_prop_pzz) < 0)
        printf("Fail to close obj property obj_prop_pzz\n");

    if(PDCprop_close(obj_prop_id11) < 0)
        printf("Fail to close obj property obj_prop_id11\n");

    if(PDCprop_close(obj_prop_id22) < 0)
        printf("Fail to close obj property obj_prop_id22\n");

    if(PDCregion_close(region_x) < 0)
        printf("fail to close region region_x\n");

    if(PDCregion_close(region_y) < 0)
        printf("fail to close region region_y\n");

    if(PDCregion_close(region_z) < 0)
        printf("fail to close region region_z\n");

    if(PDCregion_close(region_px) < 0)
        printf("fail to close region region_px\n");

    if(PDCregion_close(region_py) < 0)
        printf("fail to close region region_py\n");

    if(PDCobj_close(region_pz) < 0)
        printf("fail to close region region_pz\n");

    if(PDCobj_close(region_id1) < 0)
        printf("fail to close region region_id1\n");

    if(PDCobj_close(region_id2) < 0)
        printf("fail to close region region_id2\n");

    if(PDCregion_close(region_xx) < 0)
        printf("fail to close region region_xx\n");

    if(PDCregion_close(region_yy) < 0)
        printf("fail to close region region_yy\n");

    if(PDCregion_close(region_zz) < 0)
        printf("fail to close region region_zz\n");

    if(PDCregion_close(region_pxx) < 0)
        printf("fail to close region region_pxx\n");

    if(PDCregion_close(region_pyy) < 0)
        printf("fail to close region region_pyy\n");

    if(PDCregion_close(region_pzz) < 0)
        printf("fail to close region region_pzz\n");

    if(PDCobj_close(region_id11) < 0)
        printf("fail to close region region_id11\n");

    if(PDCobj_close(region_id22) < 0)
        printf("fail to close region region_id22\n");

    // close a container
    if(PDCcont_close(cont_id) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if(PDC_close(pdc_id) < 0)
       printf("fail to close PDC\n");

    free(x);
    free(y);
    free(z);
    free(px);
    free(py);
    free(pz);
    free(id1);
    free(id2);
    free(xx);
    free(yy);
    free(zz);
    free(pxx);
    free(pyy);
    free(pzz);
    free(id11);
    free(id22);

#ifdef ENABLE_MPI
     MPI_Finalize();
#endif

     return 0;
}

