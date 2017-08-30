#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

/*
inline double uniform_random_number()
{
        return (((double)rand())/((double)(RAND_MAX)));
}
*/
int main(int argc, char **argv)
{
    int rank = 0, size = 1;
    char *obj_name;
    uint64_t size_MB;

// Not taking size_MB into account yet. Fixing the number of particles to 8M
//    long numparticles = 8388608;
//    const int my_data_size = 8388608;
    long numparticles = 1024;
    const int my_data_size = 1024;
    uint64_t dims[1] = {my_data_size};  // {8388608};

    float *x, *y, *z, *xx, *yy, *zz;
    float *px, *py, *pz, *pxx, *pyy, *pzz;
    int *id1, *id2;
    int x_dim = 64;
    int y_dim = 64;
    int z_dim = 64;
 
    perr_t ret;
    int ndim = 1;
    uint64_t *offset;
    uint64_t *mysize;
    pdcid_t pdc, cont_prop, cont, test_obj;
    pdcid_t obj_prop_x, obj_prop_y, obj_prop_z, obj_prop_px, obj_prop_py, obj_prop_pz;
    pdcid_t obj_prop_xx, obj_prop_yy, obj_prop_zz, obj_prop_pxx, obj_prop_pyy, obj_prop_pzz;
    pdcid_t obj_x, obj_y, obj_z, obj_px, obj_py, obj_pz;
    pdcid_t obj_xx, obj_yy, obj_zz, obj_pxx, obj_pyy, obj_pzz;
    pdcid_t region_x, region_y, region_z, region_px, region_py, region_pz; 
    pdcid_t region_xx, region_yy, region_zz, region_pxx, region_pyy, region_pzz; 

    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    inline double uniform_random_number()
    {
        return (((double)rand())/((double)(RAND_MAX)));
    }

#ifdef ENABLE_MPI	
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    obj_name = argv[1];
    size_MB = atoi(argv[2]);
    size_MB *= 1048576;
    
    pdc = PDC_init("pdc");

    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if(cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    cont = PDCcont_create("c1", cont_prop);
    if(cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

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

    obj_prop_x = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if(obj_prop_x <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);
    PDCprop_set_obj_dims(obj_prop_x, 1, dims);
    PDCprop_set_obj_user_id( obj_prop_x, getuid());
    PDCprop_set_obj_time_step( obj_prop_x, 0);
    PDCprop_set_obj_app_name(obj_prop_x, "VPICIO");
    PDCprop_set_obj_tags(obj_prop_x, "tag0=1");
    PDCprop_set_obj_type(obj_prop_x, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_x, x);

    obj_prop_xx = PDCprop_obj_dup(obj_prop_x);
    PDCprop_set_obj_type(obj_prop_xx, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_xx, xx);

    obj_prop_y = PDCprop_obj_dup(obj_prop_x);
    PDCprop_set_obj_type(obj_prop_y, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_y, y);
  
    obj_prop_yy = PDCprop_obj_dup(obj_prop_y);
    PDCprop_set_obj_type(obj_prop_yy, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_yy, yy);

    obj_prop_z = PDCprop_obj_dup(obj_prop_x);
    PDCprop_set_obj_type(obj_prop_z, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_z, z);

    obj_prop_zz = PDCprop_obj_dup(obj_prop_z);
    PDCprop_set_obj_type(obj_prop_zz, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_zz, zz);

    obj_prop_px = PDCprop_obj_dup(obj_prop_x);
    PDCprop_set_obj_type(obj_prop_px, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_px, px);

    obj_prop_pxx = PDCprop_obj_dup(obj_prop_px);
    PDCprop_set_obj_type(obj_prop_pxx, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_pxx, pxx);

    obj_prop_py = PDCprop_obj_dup(obj_prop_x);
    PDCprop_set_obj_type(obj_prop_py, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_py, py);

    obj_prop_pyy = PDCprop_obj_dup(obj_prop_py);
    PDCprop_set_obj_type(obj_prop_pyy, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_pyy, pyy);

    obj_prop_pz = PDCprop_obj_dup(obj_prop_x);
    PDCprop_set_obj_type(obj_prop_pz, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_pz, pz);

    obj_prop_pzz = PDCprop_obj_dup(obj_prop_x);
    PDCprop_set_obj_type(obj_prop_pzz, PDC_FLOAT);
    PDCprop_set_obj_buf(obj_prop_pzz, pzz);

#ifdef ENABLE_MPI 
    MPI_Barrier(MPI_COMM_WORLD);
#endif
/*
	for(int i=0; i<=7; i++){
        sprintf(obj_name, "obj-var-%d", i );

        test_obj = PDCobj_create(cont, obj_name, obj_prop);
        if (test_obj < 0) {
            printf("Error getting an object id of %s from server, exit...\n", obj_name);
            exit(-1);
		}        
        obj[i] = test_obj;
	}
*/   
    obj_x = PDCobj_create(cont, "obj-var-x", obj_prop_x); 
    if (obj_x < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-x");
        exit(-1);
    }
    obj_xx = PDCobj_create(cont, "obj-var-xx", obj_prop_xx);
    if (obj_xx < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
        exit(-1);
    }
    obj_y = PDCobj_create(cont, "obj-var-y", obj_prop_y);
    if (obj_y < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-y");
        exit(-1);
    }
    obj_yy = PDCobj_create(cont, "obj-var-yy", obj_prop_yy);
    if (obj_yy < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-yy");
        exit(-1);
    }
    obj_z = PDCobj_create(cont, "obj-var-z", obj_prop_z);
    if (obj_z < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-z");
        exit(-1);
    }
    obj_zz = PDCobj_create(cont, "obj-var-zz", obj_prop_zz);
    if (obj_zz < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-zz");
        exit(-1);
    }
    obj_px = PDCobj_create(cont, "obj-var-px", obj_prop_px);
    if (obj_px < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-px");
        exit(-1);
    }
    obj_pxx = PDCobj_create(cont, "obj-var-pxx", obj_prop_pxx);
    if (obj_pxx < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pxx");
        exit(-1);
    }
    obj_py = PDCobj_create(cont, "obj-var-py", obj_prop_py);
    if (obj_py < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-py");
        exit(-1);
    }
    obj_pyy = PDCobj_create(cont, "obj-var-pyy", obj_prop_pyy);
    if (obj_pyy < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pyy");
        exit(-1);
    }
    obj_pz = PDCobj_create(cont, "obj-var-pz", obj_prop_pz);
    if (obj_pz < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pz");
        exit(-1);
    }
    obj_pzz = PDCobj_create(cont, "obj-var-pzz", obj_prop_pzz);
    if (obj_pzz < 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pzz");
        exit(-1);
    }
/*
// Query the created object
    PDC_Client_query_metadata_name_timestep("obj-var-1", 0, &metadata1);
    PDC_Client_query_metadata_name_timestep("obj-var-2", 0, &metadata2);
    PDC_Client_query_metadata_name_timestep("obj-var-3", 0, &metadata3);
    PDC_Client_query_metadata_name_timestep("obj-var-4", 0, &metadata4);
    PDC_Client_query_metadata_name_timestep("obj-var-5", 0, &metadata5);
    PDC_Client_query_metadata_name_timestep("obj-var-6", 0, &metadata6);
    PDC_Client_query_metadata_name_timestep("obj-var-7", 0, &metadata7);
    PDC_Client_query_metadata_name_timestep("obj-var-8", 0, &metadata8);
*/

    offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0] = rank * my_data_size;
    mysize[0] = my_data_size;

    region_x = PDCregion_create(ndim, offset, mysize);
    region_y = PDCregion_create(ndim, offset, mysize);
    region_z = PDCregion_create(ndim, offset, mysize);
    region_px = PDCregion_create(ndim, offset, mysize);
    region_py = PDCregion_create(ndim, offset, mysize);
    region_pz = PDCregion_create(ndim, offset, mysize);

    region_xx = PDCregion_create(ndim, offset, mysize);
    region_yy = PDCregion_create(ndim, offset, mysize);
    region_zz = PDCregion_create(ndim, offset, mysize);
    region_pxx = PDCregion_create(ndim, offset, mysize);
    region_pyy = PDCregion_create(ndim, offset, mysize);
    region_pzz = PDCregion_create(ndim, offset, mysize);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);
   
    PDCobj_map(obj_x, region_x, obj_xx, region_xx);
    PDCobj_map(obj_y, region_y, obj_yy, region_yy);
    PDCobj_map(obj_z, region_z, obj_zz, region_zz);
    PDCobj_map(obj_px, region_px, obj_pxx, region_pxx);
    PDCobj_map(obj_py, region_py, obj_pyy, region_pyy);
    PDCobj_map(obj_pz, region_pz, obj_pzz, region_pzz);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

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
    
    for (int i=0; i<numparticles; i++)
    {
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
    }

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

    PDCobj_unmap(obj_x);
    PDCobj_unmap(obj_y);
    PDCobj_unmap(obj_z);
    PDCobj_unmap(obj_px);
    PDCobj_unmap(obj_py);
    PDCobj_unmap(obj_pz);

    if(rank == 0)
    {
        if(PDCobj_close(obj_x) < 0)
            printf("fail to close object %lld\n", obj_x);

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
            printf("fail to close object %lld\n", obj_xx);

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
    }

    if(PDCprop_close(obj_prop_x) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_x);

    if(PDCprop_close(obj_prop_y) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_y);

    if(PDCprop_close(obj_prop_z) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_z);

    if(PDCprop_close(obj_prop_px) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_px);

    if(PDCprop_close(obj_prop_py) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_py);

    if(PDCprop_close(obj_prop_pz) < 0)
        printf("Fail to close obj property %lld\n", obj_prop_pz);

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

    if(PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    if(PDCprop_close(cont_prop) < 0)
        printf("Fail to close container property %lld\n", cont_prop);

    if(PDC_close(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
