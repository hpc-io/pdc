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
    pdcid_t pdc_id, cont_id;
    pdcid_t obj_xx, obj_yy, obj_zz, obj_pxx, obj_pyy, obj_pzz, obj_id11, obj_id22;
    pdcid_t region_x, region_y, region_z, region_px, region_py, region_pz, region_id1, region_id2;
    pdcid_t region_xx, region_yy, region_zz, region_pxx, region_pyy, region_pzz, region_id11, region_id22;
    perr_t  ret;
    struct timeval ht_total_start;
    struct timeval ht_total_end;
    long long      ht_total_elapsed;
    double         ht_total_sec;
    float *        x, *y, *z;
    float *        px, *py, *pz;
    int *          id1, *id2;
    uint64_t       numparticles;
    int            ndim = 1;
    uint64_t *     offset;
    uint64_t *     offset_remote;
    uint64_t *     mysize;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    numparticles = NPARTICLES;

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

    // open a container
    cont_id = PDCcont_open("c1", pdc_id);
    if (cont_id == 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // open objects
    obj_xx = PDCobj_open("obj-var-xx", pdc_id);
    if (obj_xx == 0) {
        printf("Error when open object %s\n", "obj-var-xx");
        exit(-1);
    }
    obj_yy = PDCobj_open("obj-var-yy", pdc_id);
    if (obj_yy == 0) {
        printf("Error when open object %s\n", "obj-var-yy");
        exit(-1);
    }
    obj_zz = PDCobj_open("obj-var-zz", pdc_id);
    if (obj_zz == 0) {
        printf("Error when open object %s\n", "obj-var-zz");
        exit(-1);
    }
    obj_pxx = PDCobj_open("obj-var-pxx", pdc_id);
    if (obj_pxx == 0) {
        printf("Error when open object %s\n", "obj-var-pxx");
        exit(-1);
    }
    obj_pyy = PDCobj_open("obj-var-pyy", pdc_id);
    if (obj_pyy == 0) {
        printf("Error when open object %s\n", "obj-var-pyy");
        exit(-1);
    }
    obj_pzz = PDCobj_open("obj-var-pzz", pdc_id);
    if (obj_pzz == 0) {
        printf("Error when open object %s\n", "obj-var-pzz");
        exit(-1);
    }
    obj_id11 = PDCobj_open("id11", pdc_id);
    if (obj_id11 == 0) {
        printf("Error when open object %s\n", "id11");
        exit(-1);
    }
    obj_id22 = PDCobj_open("id22", pdc_id);
    if (obj_id22 == 0) {
        printf("Error when open object %s\n", "id22");
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
    if (ret < 0)
        printf("Array x PDCbuf_obj_map failed\n");
    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &x[0], region_x, obj_xx, region_xx, 1,
                                        DECR_STATE, DATA_IN);

    ret = PDCbuf_obj_map(&y[0], PDC_FLOAT, region_y, obj_yy, region_yy);
    if (ret < 0)
        printf("Array y PDCbuf_obj_map failed\n");

    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &y[0], region_y, obj_yy, region_yy, 1,
                                        DECR_STATE, DATA_IN);

    ret = PDCbuf_obj_map(&z[0], PDC_FLOAT, region_z, obj_zz, region_zz);
    if (ret < 0)
        printf("Array z PDCbuf_obj_map failed\n");
    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &z[0], region_z, obj_zz, region_zz, 1,
                                        DECR_STATE, DATA_IN);

    ret = PDCbuf_obj_map(&px[0], PDC_FLOAT, region_px, obj_pxx, region_pxx);
    if (ret < 0)
        printf("Array px PDCbuf_obj_map failed\n");
    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &px[0], region_px, obj_pxx, region_pxx, 1,
                                        DECR_STATE, DATA_IN);

    ret = PDCbuf_obj_map(&py[0], PDC_FLOAT, region_py, obj_pyy, region_pyy);
    if (ret < 0)
        printf("Array py PDCbuf_obj_map failed\n");
    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &py[0], region_py, obj_pyy, region_pyy, 1,
                                        DECR_STATE, DATA_IN);

    ret = PDCbuf_obj_map(&pz[0], PDC_FLOAT, region_pz, obj_pzz, region_pzz);
    if (ret < 0)
        printf("Array pz PDCbuf_obj_map failed\n");
    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &pz[0], region_pz, obj_pzz, region_pzz, 1,
                                        DECR_STATE, DATA_IN);

    ret = PDCbuf_obj_map(&id1[0], PDC_INT, region_id1, obj_id11, region_id11);
    if (ret < 0)
        printf("Array id1 PDCbuf_obj_map failed\n");
    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &id1[0], region_id1, obj_id11,
                                        region_id11, 1, DECR_STATE, DATA_IN);

    ret = PDCbuf_obj_map(&id2[0], PDC_INT, region_id2, obj_id22, region_id22);
    if (ret < 0)
        printf("Array id2 PDCbuf_obj_map failed\n");
    ret = PDCbuf_map_transform_register("pdc_transform_decompress", &id2[0], region_id2, obj_id22,
                                        region_id22, 1, DECR_STATE, DATA_IN);

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

    ret = PDCreg_obtain_lock(obj_xx, region_xx, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_xx\n");

    ret = PDCreg_obtain_lock(obj_yy, region_yy, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_yy\n");

    ret = PDCreg_obtain_lock(obj_zz, region_zz, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_zz\n");

    ret = PDCreg_obtain_lock(obj_pxx, region_pxx, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_pxx\n");

    ret = PDCreg_obtain_lock(obj_pyy, region_pyy, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_pyy\n");

    ret = PDCreg_obtain_lock(obj_pzz, region_pzz, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_pzz\n");

    ret = PDCreg_obtain_lock(obj_id11, region_id11, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_id11\n");

    ret = PDCreg_obtain_lock(obj_id22, region_id22, PDC_READ, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_id22\n");

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

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    ret = PDCreg_release_lock(obj_xx, region_xx, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_xx\n");

    ret = PDCreg_release_lock(obj_yy, region_yy, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_yy\n");

    ret = PDCreg_release_lock(obj_zz, region_zz, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_zz\n");

    ret = PDCreg_release_lock(obj_pxx, region_pxx, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_pxx\n");

    ret = PDCreg_release_lock(obj_pyy, region_pyy, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_pyy\n");

    ret = PDCreg_release_lock(obj_pzz, region_pzz, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_pzz\n");

    ret = PDCreg_release_lock(obj_id11, region_id11, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_id11\n");

    ret = PDCreg_release_lock(obj_id22, region_id22, PDC_READ);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_id22\n");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to relese lock with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    ret = PDCbuf_obj_unmap(obj_xx, region_xx);
    if (ret != SUCCEED)
        printf("region xx unmap failed\n");

    ret = PDCbuf_obj_unmap(obj_yy, region_yy);
    if (ret != SUCCEED)
        printf("region yy unmap failed\n");

    ret = PDCbuf_obj_unmap(obj_zz, region_zz);
    if (ret != SUCCEED)
        printf("region zz unmap failed\n");

    ret = PDCbuf_obj_unmap(obj_pxx, region_pxx);
    if (ret != SUCCEED)
        printf("region pxx unmap failed\n");

    ret = PDCbuf_obj_unmap(obj_pyy, region_pyy);
    if (ret != SUCCEED)
        printf("region pyy unmap failed\n");

    ret = PDCbuf_obj_unmap(obj_pzz, region_pzz);
    if (ret != SUCCEED)
        printf("region pzz unmap failed\n");

    ret = PDCbuf_obj_unmap(obj_id11, region_id11);
    if (ret != SUCCEED)
        printf("region id11 unmap failed\n");

    ret = PDCbuf_obj_unmap(obj_id22, region_id22);
    if (ret != SUCCEED)
        printf("region id22 unmap failed\n");

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec - ht_total_start.tv_sec) * 1000000LL + ht_total_end.tv_usec -
                       ht_total_start.tv_usec;
    ht_total_sec = ht_total_elapsed / 1000000.0;
    if (rank == 0) {
        printf("Time to read data with %d ranks: %.6f\n", size, ht_total_sec);
        fflush(stdout);
    }

    if (PDCobj_close(obj_xx) < 0)
        printf("fail to close obj_xx\n");

    if (PDCobj_close(obj_yy) < 0)
        printf("fail to close object obj_yy\n");

    if (PDCobj_close(obj_zz) < 0)
        printf("fail to close object obj_zz\n");

    if (PDCobj_close(obj_pxx) < 0)
        printf("fail to close object obj_pxx\n");

    if (PDCobj_close(obj_pyy) < 0)
        printf("fail to close object obj_pyy\n");

    if (PDCobj_close(obj_pzz) < 0)
        printf("fail to close object obj_pzz\n");

    if (PDCobj_close(obj_id11) < 0)
        printf("fail to close object obj_id11\n");

    if (PDCobj_close(obj_id22) < 0)
        printf("fail to close object obj_id22\n");

    if (PDCregion_close(region_x) < 0)
        printf("fail to close region region_x\n");

    if (PDCregion_close(region_y) < 0)
        printf("fail to close region region_y\n");

    if (PDCregion_close(region_z) < 0)
        printf("fail to close region region_z\n");

    if (PDCregion_close(region_px) < 0)
        printf("fail to close region region_px\n");

    if (PDCregion_close(region_py) < 0)
        printf("fail to close region region_py\n");

    if (PDCobj_close(region_pz) < 0)
        printf("fail to close region region_pz\n");

    if (PDCobj_close(region_id1) < 0)
        printf("fail to close region region_id1\n");

    if (PDCobj_close(region_id2) < 0)
        printf("fail to close region region_id2\n");

    if (PDCregion_close(region_xx) < 0)
        printf("fail to close region region_xx\n");

    if (PDCregion_close(region_yy) < 0)
        printf("fail to close region region_yy\n");

    if (PDCregion_close(region_zz) < 0)
        printf("fail to close region region_zz\n");

    if (PDCregion_close(region_pxx) < 0)
        printf("fail to close region region_pxx\n");

    if (PDCregion_close(region_pyy) < 0)
        printf("fail to close region region_pyy\n");

    if (PDCregion_close(region_pzz) < 0)
        printf("fail to close region region_pzz\n");

    if (PDCobj_close(region_id11) < 0)
        printf("fail to close region region_id11\n");

    if (PDCobj_close(region_id22) < 0)
        printf("fail to close region region_id22\n");

    // close a container
    if (PDCcont_close(cont_id) < 0)
        printf("fail to close container c1\n");

    if (PDCclose(pdc_id) < 0)
        printf("fail to close PDC\n");

    free(x);
    free(y);
    free(z);
    free(px);
    free(py);
    free(pz);
    free(id1);
    free(id2);
    free(offset);
    free(offset_remote);
    free(mysize);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
