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
    LOG_JUST_PRINT("Usage: srun -n ./bdcats #particles #steps sleep_time(s)\n");
}

int
main(int argc, char **argv)
{
    int         rank = 0, size = 1;
    pdcid_t     pdc_id, cont_id, region_local, region_remote;
    pdcid_t     obj_ids[8];
    float *     dx, *dy, *dz, *ux, *uy, *uz, *q;
    int *       id;
    int         x_dim = 64, y_dim = 64, z_dim = 64, ndim = 1, steps = 1, sleeptime = 0;
    uint64_t    numparticles, dims[1], offset_local[1], offset_remote[1], mysize[1];
    double      t0, t1;
    const char *obj_names[] = {"dX", "dY", "dZ", "Ux", "Uy", "Uz", "q", "i"};
    char        obj_name[64];

    pdcid_t transfer_requests[8];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    numparticles = NPARTICLES;
    if (argc >= 2)
        numparticles = atoll(argv[1]);
    if (argc >= 3)
        steps = atoi(argv[2]);
    if (argc >= 4)
        sleeptime = atoi(argv[3]);

    if (rank == 0)
        LOG_INFO("Reading %" PRIu64 " particles per rank for %d steps with %d sec sleep time.\n",
                 numparticles, steps, sleeptime);

    dims[0] = numparticles * size;

    dx = (float *)malloc(numparticles * sizeof(float));
    dy = (float *)malloc(numparticles * sizeof(float));
    dz = (float *)malloc(numparticles * sizeof(float));
    ux = (float *)malloc(numparticles * sizeof(float));
    uy = (float *)malloc(numparticles * sizeof(float));
    uz = (float *)malloc(numparticles * sizeof(float));
    q  = (float *)malloc(numparticles * sizeof(float));

    id = (int *)malloc(numparticles * sizeof(int));

    void *data_ptrs[] = {&dx[0], &dy[0], &dz[0], &ux[0], &uy[0], &uz[0], &q[0], &id[0]};

    // create a pdc
    pdc_id = PDCinit("pdc");

    // create a container
    cont_id = PDCcont_open("c1", pdc_id);
    if (cont_id <= 0) {
        LOG_ERROR("Failed to create container");
        return FAIL;
    }

    offset_local[0]  = 0;
    offset_remote[0] = rank * numparticles;
    mysize[0]        = numparticles;

    // create local and remote region
    region_local  = PDCregion_create(ndim, offset_local, mysize);
    region_remote = PDCregion_create(ndim, offset_remote, mysize);

    for (int iter = 0; iter < steps; iter++) {

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        if (rank == 0)
            LOG_INFO("\n#Step  %d\n", iter);
        t0 = MPI_Wtime();
#endif
        for (int i = 0; i < 8; i++) {
            sprintf(obj_name, "%s-%d", obj_names[i], iter);
            obj_ids[i] = PDCobj_open(obj_name, pdc_id);
            if (obj_ids[i] == 0) {
                LOG_ERROR("Error getting an object id of %s from server\n", obj_name);
                return FAIL;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            LOG_INFO("Obj open time: %.5e\n", t1 - t0);
#endif

        for (int i = 0; i < 8; i++) {
            transfer_requests[i] =
                PDCregion_transfer_create(data_ptrs[i], PDC_READ, obj_ids[i], region_local, region_remote);
            if (transfer_requests[i] == 0) {
                LOG_ERROR("%s transfer request creation failed\n", obj_names[i]);
                return FAIL;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
        if (rank == 0)
            LOG_INFO("Transfer create time: %.5e\n", t0 - t1);
#endif

#ifdef ENABLE_MPI
        if (PDCregion_transfer_start_all_mpi(transfer_requests, 8, MPI_COMM_WORLD) != SUCCEED) {
#else
        if (PDCregion_transfer_start_all(transfer_requests, 8) != SUCCEED) {
#endif
            LOG_ERROR("Failed to start transfer requests\n");
            return FAIL;
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            LOG_INFO("Transfer start time: %.5e\n", t1 - t0);
#endif
        // Emulate compute with sleep
        if (iter != steps - 1) {
            if (rank == 0)
                LOG_INFO("Sleep start: %llu.00\n", sleeptime);
            sleep(sleeptime);
            if (rank == 0)
                LOG_INFO("Sleep end: %llu.00\n", sleeptime);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
#endif

        if (PDCregion_transfer_wait_all(transfer_requests, 8) != SUCCEED) {
            LOG_ERROR("Failed to transfer wait all\n");
            return FAIL;
        }

        // Verify data of id and q
        if (id[0] != rank + iter || q[0] != rank + iter * 2 || id[numparticles - 1] != rank - iter ||
            q[numparticles - 1] != rank - iter * 2) {
            LOG_ERROR("Data verification failed on rank %d for step %d! id[0]=%d/%d, "
                      "q[0]=%d/%d, id[end]=%d/%d, q[end]=%d/%d\n",
                      rank, iter, id[0], rank + iter, q[0], rank + iter * 2, id[numparticles - 1],
                      rank - iter, q[numparticles - 1], rank - iter * 2);
            return FAIL;
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            LOG_INFO("Transfer wait time: %.5e\n", t1 - t0);
#endif

        for (int j = 0; j < 8; j++) {
            if (PDCregion_transfer_close(transfer_requests[j]) != SUCCEED) {
                LOG_ERROR("region transfer close failed\n");
                return FAIL;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
        if (rank == 0)
            LOG_INFO("Transfer close time: %.5e\n", t0 - t1);
#endif

        for (int i = 0; i < 8; i++) {
            if (PDCobj_close(obj_ids[i]) != SUCCEED) {
                LOG_ERROR("Failed to close object #%d\n", i);
                return FAIL;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        if (rank == 0)
            LOG_INFO("Obj close time: %.5e\n", t1 - t0);
#endif
    } // End for steps

    if (PDCregion_close(region_local) != SUCCEED) {
        LOG_ERROR("Failed to close local region \n");
        return FAIL;
    }
    if (PDCobj_close(region_remote) != SUCCEED) {
        LOG_ERROR("Failed to close remote region\n");
        return FAIL;
    }
    if (PDCcont_close(cont_id) != SUCCEED) {
        LOG_ERROR("Failed to close container\n");
        return FAIL;
    }
    if (PDCclose(pdc_id) != SUCCEED) {
        LOG_ERROR("Failed to close PDC\n");
        return FAIL;
    }
    free(dx);
    free(dy);
    free(dz);
    free(ux);
    free(uy);
    free(uz);
    free(q);
    free(id);
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
