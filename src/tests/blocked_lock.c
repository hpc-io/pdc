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
#include "pdc.h"

double
uniform_random_number()
{
    return (((double)rand()) / ((double)(RAND_MAX)));
}

int
main(int argc, char **argv)
{
    int     rank = 0, size = 1;
    pdcid_t pdc_id, cont_prop, cont_id;
    pdcid_t obj_prop2;
    pdcid_t obj2;
    pdcid_t r1, r2;
    perr_t  ret;
    float * x;
    int     x_dim        = 64;
    long    numparticles = 8388608;
    //    const int my_data_size = 992;
    uint64_t  dims[1] = {numparticles}; // {8388608};
    int       ndim    = 1;
    uint64_t *offset;
    uint64_t *offset_remote;
    uint64_t *mysize;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    x = (float *)malloc(numparticles * sizeof(float));

    // create a pdc
    pdc_id = PDC_init("pdc");
    /* printf("create a new pdc, pdc id is: %lld\n", pdc); */

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont_id = PDCcont_create("c1", cont_prop);
    if (cont_id <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    PDCprop_set_obj_dims(obj_prop2, 1, dims);
    PDCprop_set_obj_type(obj_prop2, PDC_FLOAT);
    //	PDCprop_set_obj_buf(obj_prop2, &xx[0]  );
    PDCprop_set_obj_time_step(obj_prop2, 0);
    PDCprop_set_obj_user_id(obj_prop2, getuid());
    PDCprop_set_obj_app_name(obj_prop2, "VPICIO");
    PDCprop_set_obj_tags(obj_prop2, "tag0=1");

    obj2 = PDCobj_create_mpi(cont_id, "obj-var-xx", obj_prop2, 0);
    if (obj2 == 0) {
        printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
        exit(-1);
    }

    offset           = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset_remote    = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize           = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0]        = 0;
    offset_remote[0] = rank * numparticles;
    mysize[0]        = numparticles;

    // create a region
    r1 = PDCregion_create(1, offset, mysize);
    //    printf("first region id: %lld\n", r1);
    r2 = PDCregion_create(1, offset_remote, mysize);
    //    printf("second region id: %lld\n", r2);

    ret = PDCbuf_obj_map(&x[0], PDC_FLOAT, r1, obj2, r2);
    if (ret < 0) {
        printf("PDCbuf_obj_map failed\n");
        exit(-1);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    ret = PDCreg_obtain_lock(obj2, r2, WRITE, NOBLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for r2\n");

    MPI_Barrier(MPI_COMM_WORLD);

    for (int i = 0; i < numparticles; i++) {
        x[i] = uniform_random_number() * x_dim;
        // printf("x = %f\n", x[i]);
        // fflush(stdout);
    }

    ret = PDCreg_release_lock(obj2, r2, WRITE);
    if (ret != SUCCEED)
        printf("Failed to release lock for r2\n");

    MPI_Barrier(MPI_COMM_WORLD);
    printf("done with region release\n");
    fflush(stdout);

    if (rank == 0) {
        printf("request another lock\n");
        fflush(stdout);
    }

    ret = PDCreg_obtain_lock(obj2, r2, WRITE, BLOCK);
    if (ret != SUCCEED)
        printf("Failed to obtain lock for r2\n");
    if (rank == 0) {
        printf("lock is granted\n");
        fflush(stdout);
    }

    ret = PDCbuf_obj_unmap(obj2, r2);
    if (ret != SUCCEED)
        printf("region unmap failed\n");

    if (PDCobj_close(obj2) < 0)
        printf("fail to close obj2\n");

    // close a container
    if (PDCcont_close(cont_id) < 0)
        printf("fail to close container\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDC_close(pdc_id) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
