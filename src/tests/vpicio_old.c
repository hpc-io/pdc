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
#include <inttypes.h>
#include "pdc.h"

#define NPARTICLES 8388608

double
uniform_random_number()
{
    return (((double)rand()) / ((double)(RAND_MAX)));
}

void
print_usage()
{
    printf("Usage: srun -n ./vpicio #particles\n");
}

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1;
    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t obj_prop_xx, obj_prop_yy, obj_prop_zz, obj_prop_pxx, obj_prop_pyy, obj_prop_pzz, obj_prop_id11,
        obj_prop_id22;
    pdcid_t obj_xx, obj_yy, obj_zz, obj_pxx, obj_pyy, obj_pzz, obj_id11, obj_id22;
    pdcid_t region_x, region_y, region_z, region_px, region_py, region_pz, region_id1, region_id2;
    pdcid_t region_xx, region_yy, region_zz, region_pxx, region_pyy, region_pzz, region_id11, region_id22;
    perr_t  ret;
#ifdef ENABLE_MPI
    MPI_Comm comm;
#else
    int comm = 1;
#endif
    struct timeval ht_total_start;
    struct timeval ht_total_end;
    long long      ht_total_elapsed;
    double         ht_total_sec;
    float *        x, *y, *z;
    float *        px, *py, *pz;
    int *          id1, *id2;
    int            x_dim = 64;
    int            y_dim = 64;
    int            z_dim = 64;
    uint64_t       numparticles, i;
    uint64_t       dims[1];
    int            ndim = 1;
    uint64_t *     offset;
    uint64_t *     offset_remote;
    uint64_t *     mysize;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
#endif

    numparticles = NPARTICLES;
    if (argc == 2) {
        numparticles = atoll(argv[1]);
        if (rank == 0)
            printf("Writing %" PRIu64 " number of particles with %d clients.\n", numparticles, size);
    }

    dims[0] = numparticles;

    x = (float *)malloc(numparticles * sizeof(float));
    y = (float *)malloc(numparticles * sizeof(float));
    z = (float *)malloc(numparticles * sizeof(float));

    px = (float *)malloc(numparticles * sizeof(float));
    py = (float *)malloc(numparticles * sizeof(float));
    pz = (float *)malloc(numparticles * sizeof(float));

    id1 = (int *)malloc(numparticles * sizeof(int));
    id2 = (int *)malloc(numparticles * sizeof(int));

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0) {
        printf("Fail to create container property @ line  %d!\n", __LINE__);
        return 1;
    }
    // create a container
    cont_id = PDCcont_create_col("c1", cont_prop);
    if (cont_id <= 0) {
        printf("Fail to create container @ line  %d!\n", __LINE__);
        return 1;
    }
    // create an object property
    obj_prop_xx = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop_xx, 1, dims);
    PDCprop_set_obj_type(obj_prop_xx, PDC_FLOAT);
    PDCprop_set_obj_time_step(obj_prop_xx, 0);
    PDCprop_set_obj_user_id(obj_prop_xx, getuid());
    PDCprop_set_obj_app_name(obj_prop_xx, "VPICIO");
    PDCprop_set_obj_tags(obj_prop_xx, "tag0=1");

    obj_prop_yy = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_yy, PDC_FLOAT);

    obj_prop_zz = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_zz, PDC_FLOAT);

    obj_prop_pxx = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_pxx, PDC_FLOAT);

    obj_prop_pyy = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_pyy, PDC_FLOAT);

    obj_prop_pzz = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_pzz, PDC_FLOAT);

    obj_prop_id11 = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_id11, PDC_INT);

    obj_prop_id22 = PDCprop_obj_dup(obj_prop_xx);
    PDCprop_set_obj_type(obj_prop_id22, PDC_INT);

    obj_xx = PDCobj_create_mpi(cont_id, "obj-var-xx", obj_prop_xx, 0, comm);
    if (obj_xx == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
        exit(-1);
    }

    obj_yy = PDCobj_create_mpi(cont_id, "obj-var-yy", obj_prop_yy, 0, comm);
    if (obj_yy == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-yy");
        exit(-1);
    }
    obj_zz = PDCobj_create_mpi(cont_id, "obj-var-zz", obj_prop_zz, 0, comm);
    if (obj_zz == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-zz");
        exit(-1);
    }
    obj_pxx = PDCobj_create_mpi(cont_id, "obj-var-pxx", obj_prop_pxx, 0, comm);
    if (obj_pxx == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pxx");
        exit(-1);
    }
    obj_pyy = PDCobj_create_mpi(cont_id, "obj-var-pyy", obj_prop_pyy, 0, comm);
    if (obj_pyy == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pyy");
        exit(-1);
    }
    obj_pzz = PDCobj_create_mpi(cont_id, "obj-var-pzz", obj_prop_pzz, 0, comm);
    if (obj_pzz == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-pzz");
        exit(-1);
    }

    obj_id11 = PDCobj_create_mpi(cont_id, "id11", obj_prop_id11, 0, comm);
    if (obj_id11 == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj_id11");
        exit(-1);
    }
    obj_id22 = PDCobj_create_mpi(cont_id, "id22", obj_prop_id22, 0, comm);
    if (obj_id22 == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj_id22");
        exit(-1);
    }

    offset           = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset_remote    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize           = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0]        = 0;
    offset_remote[0] = rank * numparticles;
    mysize[0]        = numparticles;

    // create a region
    region_x   = PDCregion_create(ndim, offset, mysize);
    region_y   = PDCregion_create(ndim, offset, mysize);
    region_z   = PDCregion_create(ndim, offset, mysize);
    region_px  = PDCregion_create(ndim, offset, mysize);
    region_py  = PDCregion_create(ndim, offset, mysize);
    region_pz  = PDCregion_create(ndim, offset, mysize);
    region_id1 = PDCregion_create(ndim, offset, mysize);
    region_id2 = PDCregion_create(ndim, offset, mysize);

    region_xx   = PDCregion_create(ndim, offset_remote, mysize);
    region_yy   = PDCregion_create(ndim, offset_remote, mysize);
    region_zz   = PDCregion_create(ndim, offset_remote, mysize);
    region_pxx  = PDCregion_create(ndim, offset_remote, mysize);
    region_pyy  = PDCregion_create(ndim, offset_remote, mysize);
    region_pzz  = PDCregion_create(ndim, offset_remote, mysize);
    region_id11 = PDCregion_create(ndim, offset_remote, mysize);
    region_id22 = PDCregion_create(ndim, offset_remote, mysize);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    ret = PDCbuf_obj_map(&x[0], PDC_FLOAT, region_x, obj_xx, region_xx);
    if (ret < 0) {
        printf("Array x PDCbuf_obj_map failed\n");
        return 1;
    }
    ret = PDCbuf_obj_map(&y[0], PDC_FLOAT, region_y, obj_yy, region_yy);
    if (ret < 0) {
        printf("Array y PDCbuf_obj_map failed\n");
        return 1;
    }
    ret = PDCbuf_obj_map(&z[0], PDC_FLOAT, region_z, obj_zz, region_zz);
    if (ret < 0) {
        printf("Array z PDCbuf_obj_map failed\n");
        return 1;
    }
    ret = PDCbuf_obj_map(&px[0], PDC_FLOAT, region_px, obj_pxx, region_pxx);
    if (ret < 0) {
        printf("Array px PDCbuf_obj_map failed\n");
        return 1;
    }
    ret = PDCbuf_obj_map(&py[0], PDC_FLOAT, region_py, obj_pyy, region_pyy);
    if (ret < 0) {
        printf("Array py PDCbuf_obj_map failed\n");
        return 1;
    }
    ret = PDCbuf_obj_map(&pz[0], PDC_FLOAT, region_pz, obj_pzz, region_pzz);
    if (ret < 0) {
        printf("Array pz PDCbuf_obj_map failed\n");
        return 1;
    }
    ret = PDCbuf_obj_map(&id1[0], PDC_INT, region_id1, obj_id11, region_id11);
    if (ret < 0) {
        printf("Array id1 PDCbuf_obj_map failed\n");
        return 1;
    }
    ret = PDCbuf_obj_map(&id2[0], PDC_INT, region_id2, obj_id22, region_id22);
    if (ret < 0) {
        printf("Array id2 PDCbuf_obj_map failed\n");
        return 1;
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to map with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    ret = PDCreg_obtain_lock(obj_xx, region_xx, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_xx\n");
        return 1;
    }
    ret = PDCreg_obtain_lock(obj_yy, region_yy, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_yy\n");
        return 1;
    }
    ret = PDCreg_obtain_lock(obj_zz, region_zz, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_zz\n");
        return 1;
    }
    ret = PDCreg_obtain_lock(obj_pxx, region_pxx, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_pxx\n");
        return 1;
    }
    ret = PDCreg_obtain_lock(obj_pyy, region_pyy, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_pyy\n");
        return 1;
    }
    ret = PDCreg_obtain_lock(obj_pzz, region_pzz, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_pzz\n");
        return 1;
    }
    ret = PDCreg_obtain_lock(obj_id11, region_id11, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_id11\n");
        return 1;
    }
    ret = PDCreg_obtain_lock(obj_id22, region_id22, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED) {
        printf("Failed to obtain lock for region_id22\n");
        return 1;
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to lock with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

    for (i = 0; i < numparticles; i++) {
        id1[i] = i;
        id2[i] = i * 2;
        x[i]   = uniform_random_number() * x_dim;
        y[i]   = uniform_random_number() * y_dim;
        z[i]   = ((float)id1[i] / numparticles) * z_dim;
        px[i]  = uniform_random_number() * x_dim;
        py[i]  = uniform_random_number() * y_dim;
        pz[i]  = ((float)id2[i] / numparticles) * z_dim;
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    ret = PDCreg_release_lock(obj_xx, region_xx, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_xx\n");
        return 1;
    }
    ret = PDCreg_release_lock(obj_yy, region_yy, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_yy\n");
        return 1;
    }
    ret = PDCreg_release_lock(obj_zz, region_zz, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_zz\n");
        return 1;
    }
    ret = PDCreg_release_lock(obj_pxx, region_pxx, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_pxx\n");
        return 1;
    }
    ret = PDCreg_release_lock(obj_pyy, region_pyy, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_pyy\n");
        return 1;
    }
    ret = PDCreg_release_lock(obj_pzz, region_pzz, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_pzz\n");
        return 1;
    }
    ret = PDCreg_release_lock(obj_id11, region_id11, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_id11\n");
        return 1;
    }
    ret = PDCreg_release_lock(obj_id22, region_id22, PDC_WRITE);
    if (ret != SUCCEED) {
        printf("Failed to release lock for region_id22\n");
        return 1;
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to update data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    ret = PDCbuf_obj_unmap(obj_xx, region_xx);
    if (ret != SUCCEED) {
        printf("region xx unmap failed\n");
        return 1;
    }
    ret = PDCbuf_obj_unmap(obj_yy, region_yy);
    if (ret != SUCCEED) {
        printf("region yy unmap failed\n");
        return 1;
    }
    ret = PDCbuf_obj_unmap(obj_zz, region_zz);
    if (ret != SUCCEED) {
        printf("region zz unmap failed\n");
        return 1;
    }
    ret = PDCbuf_obj_unmap(obj_pxx, region_pxx);
    if (ret != SUCCEED) {
        printf("region pxx unmap failed\n");
        return 1;
    }
    ret = PDCbuf_obj_unmap(obj_pyy, region_pyy);
    if (ret != SUCCEED) {
        printf("region pyy unmap failed\n");
        return 1;
    }
    ret = PDCbuf_obj_unmap(obj_pzz, region_pzz);
    if (ret != SUCCEED) {
        printf("region pzz unmap failed\n");
        return 1;
    }
    ret = PDCbuf_obj_unmap(obj_id11, region_id11);
    if (ret != SUCCEED) {
        printf("region id11 unmap failed\n");
        return 1;
    }
    ret = PDCbuf_obj_unmap(obj_id22, region_id22);
    if (ret != SUCCEED) {
        printf("region id22 unmap failed\n");
        return 1;
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (PDCobj_close(obj_xx) < 0) {
        printf("fail to close obj_xx\n");
        return 1;
    }

    if (PDCobj_close(obj_yy) < 0) {
        printf("fail to close object obj_yy\n");
        return 1;
    }
    if (PDCobj_close(obj_zz) < 0) {
        printf("fail to close object obj_zz\n");
        return 1;
    }
    if (PDCobj_close(obj_pxx) < 0) {
        printf("fail to close object obj_pxx\n");
        return 1;
    }
    if (PDCobj_close(obj_pyy) < 0) {
        printf("fail to close object obj_pyy\n");
        return 1;
    }
    if (PDCobj_close(obj_pzz) < 0) {
        printf("fail to close object obj_pzz\n");
        return 1;
    }
    if (PDCobj_close(obj_id11) < 0) {
        printf("fail to close object obj_id11\n");
        return 1;
    }
    if (PDCobj_close(obj_id22) < 0) {
        printf("fail to close object obj_id22\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_xx) < 0) {
        printf("Fail to close obj property obj_prop_xx\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_yy) < 0) {
        printf("Fail to close obj property obj_prop_yy\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_zz) < 0) {
        printf("Fail to close obj property obj_prop_zz\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_pxx) < 0) {
        printf("Fail to close obj property obj_prop_pxx\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_pyy) < 0) {
        printf("Fail to close obj property obj_prop_pyy\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_pzz) < 0) {
        printf("Fail to close obj property obj_prop_pzz\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_id11) < 0) {
        printf("Fail to close obj property obj_prop_id11\n");
        return 1;
    }
    if (PDCprop_close(obj_prop_id22) < 0) {
        printf("Fail to close obj property obj_prop_id22\n");
        return 1;
    }
    if (PDCregion_close(region_x) < 0) {
        printf("fail to close region region_x\n");
        return 1;
    }
    if (PDCregion_close(region_y) < 0) {
        printf("fail to close region region_y\n");
        return 1;
    }
    if (PDCregion_close(region_z) < 0) {
        printf("fail to close region region_z\n");
        return 1;
    }
    if (PDCregion_close(region_px) < 0) {
        printf("fail to close region region_px\n");
        return 1;
    }
    if (PDCregion_close(region_py) < 0) {
        printf("fail to close region region_py\n");
        return 1;
    }
    if (PDCobj_close(region_pz) < 0) {
        printf("fail to close region region_pz\n");
        return 1;
    }
    if (PDCobj_close(region_id1) < 0) {
        printf("fail to close region region_id1\n");
        return 1;
    }
    if (PDCobj_close(region_id2) < 0) {
        printf("fail to close region region_id2\n");
        return 1;
    }
    if (PDCregion_close(region_xx) < 0) {
        printf("fail to close region region_xx\n");
        return 1;
    }
    if (PDCregion_close(region_yy) < 0) {
        printf("fail to close region region_yy\n");
        return 1;
    }
    if (PDCregion_close(region_zz) < 0) {
        printf("fail to close region region_zz\n");
        return 1;
    }
    if (PDCregion_close(region_pxx) < 0) {
        printf("fail to close region region_pxx\n");
        return 1;
    }
    if (PDCregion_close(region_pyy) < 0) {
        printf("fail to close region region_pyy\n");
        return 1;
    }
    if (PDCregion_close(region_pzz) < 0) {
        printf("fail to close region region_pzz\n");
        return 1;
    }
    if (PDCobj_close(region_id11) < 0) {
        printf("fail to close region region_id11\n");
        return 1;
    }
    if (PDCobj_close(region_id22) < 0) {
        printf("fail to close region region_id22\n");
        return 1;
    }
    // close a container
    if (PDCcont_close(cont_id) < 0) {
        printf("fail to close container c1\n");
        return 1;
    }
    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        printf("Fail to close property @ line %d\n", __LINE__);
        return 1;
    }
    if (PDCclose(pdc_id) < 0) {
        printf("fail to close PDC\n");
        return 1;
    }
    free(offset);
    free(offset_remote);
    free(mysize);
    free(x);
    free(y);
    free(z);
    free(px);
    free(py);
    free(pz);
    free(id1);
    free(id2);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
