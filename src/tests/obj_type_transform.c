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
#include <limits.h>
#include <inttypes.h>

#define ENABLE_MPI 1
// #define SHOW_PROGRESS 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"

// #define NPARTICLES      8388608
#define NPARTICLES 100

double
uniform_random_number()
{
    double fraction = (((double)rand()) / ((double)(RAND_MAX)));
    return (double)(rand() / (double)(RAND_MAX / INT_MAX)) + fraction;
}

void
print_usage()
{
    printf("Usage: srun -n ./vpicio #particles\n");
}

int
main(int argc, char **argv)
{
    int            rank = 0, size = 1;
    pdcid_t        pdc_id, cont_prop, cont_id;
    pdcid_t        obj_prop_dx, obj_prop_ix;
    pdcid_t        obj_dx, obj_ix;
    pdcid_t        region_dx, region_ix;
    perr_t         ret;
    struct timeval ht_total_start;
    struct timeval ht_total_end;
    long long      ht_total_elapsed;
    double         ht_total_sec;
    double *       dx;
    int *          ix;
    uint64_t       numparticles, i;
    uint64_t       dims[1];
    int            ndim = 1;
    uint64_t *     offset;
    uint64_t *     offset_remote;
    uint64_t *     mysize;
    char           obj_name[64];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    numparticles = NPARTICLES;

    if (argc > 1)
        numparticles = atoll(argv[1]) * 1024 * 1024;

    if (rank == 0) {
        printf("Writing %" PRIu64 " number of particles with %d clients.\n", numparticles, size);
    }

    dims[0] = numparticles;
    dx      = (double *)malloc(numparticles * sizeof(double));
    ix      = (int *)malloc(numparticles * sizeof(int));

    // create a pdc
    pdc_id = PDCinit("pdc");
    if (pdc_id <= 0)
        printf("PDC_init failed @ line  %d!\n", __LINE__);

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("c1", cont_prop);
    if (cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop_dx = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop_dx, 1, dims);
    PDCprop_set_obj_type(obj_prop_dx, PDC_DOUBLE);
    PDCprop_set_obj_time_step(obj_prop_dx, 0);
    PDCprop_set_obj_user_id(obj_prop_dx, getuid());
    PDCprop_set_obj_app_name(obj_prop_dx, "VPICIO");
    PDCprop_set_obj_tags(obj_prop_dx, "tag0=1");

    // Dup and then set the obj datatype to PDC_INT
    obj_prop_ix = PDCprop_obj_dup(obj_prop_dx);
    PDCprop_set_obj_type(obj_prop_ix, PDC_INT);

    // Set the buf property of each to the actual data buffers
    PDCprop_set_obj_buf(obj_prop_dx, dx);
    PDCprop_set_obj_buf(obj_prop_ix, ix);

    sprintf(obj_name, "obj-var-dx-%d", rank);
    obj_dx = PDCobj_create(cont_id, obj_name, obj_prop_dx);
    if (obj_dx == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-dx");
        exit(-1);
    }
    sprintf(obj_name, "obj-var-ix-%d", rank);
    obj_ix = PDCobj_create(cont_id, obj_name, obj_prop_ix);
    if (obj_ix == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-ix");
        exit(-1);
    }

    offset           = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset_remote    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize           = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0]        = 0;
    offset_remote[0] = rank * numparticles;
    mysize[0]        = numparticles;

    // create regions
    region_dx = PDCregion_create(ndim, offset, mysize);
    region_ix = PDCregion_create(ndim, offset, mysize);

    for (i = 0; i < numparticles; i++) {
        dx[i] = uniform_random_number();
        ix[i] = i;
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&ht_total_start, 0);

    // ret = PDCobj_map(obj_dx, region_dx, obj_ix, region_ix);
    ret = PDCbuf_obj_map(&dx[0], PDC_DOUBLE, region_dx, obj_ix, region_ix);
    if (ret < 0)
        printf("PDCbuf_obj_map failed\n");

    // register a transform for when the mapping of 'obj_dx' takes place
    ret = PDCobj_transform_register("pdc_convert_datatype", obj_dx, 0, INCR_STATE, PDC_DATA_MAP, DATA_OUT);
    if (ret < 0)
        printf("PDCobj_transform_register failed\n");

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

    ret = PDCreg_obtain_lock(obj_ix, region_ix, PDC_WRITE, PDC_NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for region_xx\n");

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

#ifdef SHOW_PROGRESS
    if (rank == 0) {
        printf("dx = ");
        for (i = 0; i < 10; i++)
            printf(" %10.2lf", dx[i]);
        puts("\n----------------------");
    }
#endif

    gettimeofday(&ht_total_start, 0);

    ret = PDCreg_release_lock(obj_ix, region_ix, PDC_WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for region_xx\n");

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

    ret = PDCbuf_obj_unmap(obj_ix, region_ix);
    if (ret != SUCCEED)
        printf("region xx unmap failed\n");

    if (PDCobj_close(obj_dx) < 0)
        printf("fail to close obj_xx\n");

    if (PDCobj_close(obj_ix) < 0)
        printf("fail to close object obj_ix\n");

    if (PDCprop_close(obj_prop_dx) < 0)
        printf("Fail to close obj property obj_prop_xx\n");

    if (PDCprop_close(obj_prop_ix) < 0)
        printf("Fail to close obj property obj_prop_yy\n");

    if (PDCregion_close(region_dx) < 0)
        printf("fail to close region region_x\n");

    if (PDCregion_close(region_ix) < 0)
        printf("fail to close region region_y\n");

    // close a container
    if (PDCcont_close(cont_id) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc_id) < 0)
        printf("fail to close PDC\n");

    free(dx);
    free(ix);

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
