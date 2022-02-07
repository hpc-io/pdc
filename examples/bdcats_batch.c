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
#include "pdc_timing.h"

#define NPARTICLES 8388608
#define N_OBJS     8

double
uniform_random_number()
{
    return (((double)rand()) / ((double)(RAND_MAX)));
}

void
print_usage()
{
    printf("Usage: srun -n ./vpicio sleep_time timestamps #particles\n");
}

int
main(int argc, char **argv)
{
    int      rank = 0, size = 1;
    pdcid_t  pdc_id;
    pdcid_t *obj_xx, *obj_yy, *obj_zz, *obj_pxx, *obj_pyy, *obj_pzz, *obj_id11, *obj_id22;
    pdcid_t  region_x, region_y, region_z, region_px, region_py, region_pz, region_id1, region_id2;
    pdcid_t  region_xx, region_yy, region_zz, region_pxx, region_pyy, region_pzz, region_id11, region_id22;
    perr_t   ret;
#ifdef ENABLE_MPI
    MPI_Comm comm;
#else
    int comm = 1;
#endif
    char      obj_name[128];
    float *   x, *y, *z;
    float *   px, *py, *pz;
    int *     id1, *id2;
    int       x_dim = 64;
    int       y_dim = 64;
    int       z_dim = 64;
    uint64_t  numparticles, i, j;
    int       ndim = 1;
    uint64_t *offset;
    uint64_t *offset_remote;
    uint64_t *mysize;
    unsigned  sleep_time = 0;

    pdcid_t *transfer_request_x, *transfer_request_y, *transfer_request_z, *transfer_request_px,
        *transfer_request_py, *transfer_request_pz, *transfer_request_id1, *transfer_request_id2, *ptr,
        *temp_requests;

    uint64_t timestamps = 10;

    double start, end, transfer_start = .0, transfer_wait = .0, transfer_create = .0, transfer_close = .0,
                       max_time, min_time, avg_time;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
#endif

    numparticles = NPARTICLES;

    if (argc >= 2) {
        sleep_time = (unsigned)atoi(argv[1]);
    }
    if (argc >= 3) {
        timestamps = atoll(argv[2]);
    }
    if (argc >= 4) {
        numparticles = atoll(argv[3]);
        if (rank == 0)
            printf("Writing %" PRIu64 " number of particles with %d clients.\n", numparticles, size);
    }
    if (!rank) {
        printf("sleep time = %u, timestamps = %" PRIu64 ", numparticles = %" PRIu64 "\n", sleep_time,
               timestamps, numparticles);
    }

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

    obj_xx   = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);
    obj_yy   = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);
    obj_zz   = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);
    obj_pxx  = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);
    obj_pyy  = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);
    obj_pzz  = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);
    obj_id11 = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);
    obj_id22 = (pdcid_t *)malloc(sizeof(pdcid_t) * timestamps);

    offset        = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset_remote = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    mysize        = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    offset[0]     = 0;
    mysize[0]     = numparticles;

    // create a region
    region_x   = PDCregion_create(ndim, offset, mysize);
    region_y   = PDCregion_create(ndim, offset, mysize);
    region_z   = PDCregion_create(ndim, offset, mysize);
    region_px  = PDCregion_create(ndim, offset, mysize);
    region_py  = PDCregion_create(ndim, offset, mysize);
    region_pz  = PDCregion_create(ndim, offset, mysize);
    region_id1 = PDCregion_create(ndim, offset, mysize);
    region_id2 = PDCregion_create(ndim, offset, mysize);

    transfer_request_x = (pdcid_t *)malloc(sizeof(pdcid_t) * (timestamps + 1) * N_OBJS);
    ptr                = transfer_request_x + timestamps;
    transfer_request_y = ptr;
    ptr += timestamps;
    transfer_request_z = ptr;
    ptr += timestamps;
    transfer_request_px = ptr;
    ptr += timestamps;
    transfer_request_py = ptr;
    ptr += timestamps;
    transfer_request_pz = ptr;
    ptr += timestamps;
    transfer_request_id1 = ptr;
    ptr += timestamps;
    transfer_request_id2 = ptr;
    ptr += timestamps;
    temp_requests = ptr;

    for (i = 0; i < timestamps; ++i) {
        sprintf(obj_name, "obj-var-xx %" PRIu64 "", i);
        obj_xx[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_xx[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj-var-xx");
            exit(-1);
        }
        sprintf(obj_name, "obj-var-yy %" PRIu64 "", i);
        obj_yy[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_yy[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj-var-yy");
            exit(-1);
        }
        sprintf(obj_name, "obj-var-zz %" PRIu64 "", i);
        obj_zz[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_zz[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj-var-zz");
            exit(-1);
        }
        sprintf(obj_name, "obj-var-pxx %" PRIu64 "", i);
        obj_pxx[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_pxx[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj-var-pxx");
            exit(-1);
        }
        sprintf(obj_name, "obj-var-pyy %" PRIu64 "", i);
        obj_pyy[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_pyy[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj-var-pyy");
            exit(-1);
        }
        sprintf(obj_name, "obj-var-pzz %" PRIu64 "", i);
        obj_pzz[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_pzz[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj-var-pzz");
            exit(-1);
        }
        sprintf(obj_name, "id11 %" PRIu64 "", i);
        obj_id11[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_id11[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj_id11");
            exit(-1);
        }
        sprintf(obj_name, "id22 %" PRIu64 "", i);
        obj_id22[i] = PDCobj_open(obj_name, pdc_id);
        if (obj_id22[i] == 0) {
            printf("Error getting an object id of %s from server, exit...\n", "obj_id22");
            exit(-1);
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    for (i = 0; i < timestamps; ++i) {

        offset_remote[0] = rank * numparticles;
        region_xx        = PDCregion_create(ndim, offset_remote, mysize);
        region_yy        = PDCregion_create(ndim, offset_remote, mysize);
        region_zz        = PDCregion_create(ndim, offset_remote, mysize);
        region_pxx       = PDCregion_create(ndim, offset_remote, mysize);
        region_pyy       = PDCregion_create(ndim, offset_remote, mysize);
        region_pzz       = PDCregion_create(ndim, offset_remote, mysize);
        region_id11      = PDCregion_create(ndim, offset_remote, mysize);
        region_id22      = PDCregion_create(ndim, offset_remote, mysize);

#ifdef ENABLE_MPI
        start = MPI_Wtime();
#endif
        transfer_request_x[i] = PDCregion_transfer_create(&x[0], PDC_WRITE, obj_xx[i], region_x, region_xx);
        if (transfer_request_x[i] == 0) {
            printf("Array x transfer request creation failed\n");
            return 1;
        }
        transfer_request_y[i] = PDCregion_transfer_create(&y[0], PDC_WRITE, obj_yy[i], region_y, region_yy);
        if (transfer_request_y[i] == 0) {
            printf("Array y transfer request creation failed\n");
            return 1;
        }
        transfer_request_z[i] = PDCregion_transfer_create(&z[0], PDC_WRITE, obj_zz[i], region_z, region_zz);
        if (transfer_request_z[i] == 0) {
            printf("Array z transfer request creation failed\n");
            return 1;
        }
        transfer_request_px[i] =
            PDCregion_transfer_create(&px[0], PDC_READ, obj_pxx[i], region_px, region_pxx);
        if (transfer_request_px[i] == 0) {
            printf("Array px transfer request creation failed\n");
            return 1;
        }
        transfer_request_py[i] =
            PDCregion_transfer_create(&py[0], PDC_READ, obj_pyy[i], region_py, region_pyy);
        if (transfer_request_py[i] == 0) {
            printf("Array py transfer request creation failed\n");
            return 1;
        }
        transfer_request_pz[i] =
            PDCregion_transfer_create(&pz[0], PDC_READ, obj_pzz[i], region_pz, region_pzz);
        if (transfer_request_pz[i] == 0) {
            printf("Array pz transfer request creation failed\n");
            return 1;
        }
        transfer_request_id1[i] =
            PDCregion_transfer_create(&id1[0], PDC_READ, obj_id11[i], region_id1, region_id11);
        if (transfer_request_id1[i] == 0) {
            printf("Array id1 transfer request creation failed\n");
            return 1;
        }
        transfer_request_id2[i] =
            PDCregion_transfer_create(&id2[0], PDC_READ, obj_id22[i], region_id2, region_id22);
        if (transfer_request_id2[i] == 0) {
            printf("Array id2 transfer request creation failed\n");
            return 1;
        }
#ifdef ENABLE_MPI
        transfer_create += MPI_Wtime() - start;
#endif

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

        temp_requests[0] = transfer_request_x[i];
        temp_requests[1] = transfer_request_y[i];
        temp_requests[2] = transfer_request_z[i];
        temp_requests[3] = transfer_request_px[i];
        temp_requests[4] = transfer_request_py[i];
        temp_requests[5] = transfer_request_pz[i];
        temp_requests[6] = transfer_request_id1[i];
        temp_requests[7] = transfer_request_id2[i];

        for (j = 0; j < numparticles; j++) {
            id1[j] = j;
            id2[j] = j * 2;
            x[j]   = uniform_random_number() * x_dim;
            y[j]   = uniform_random_number() * y_dim;
            z[j]   = ((float)id1[j] / numparticles) * z_dim;
            px[j]  = uniform_random_number() * x_dim;
            py[j]  = uniform_random_number() * y_dim;
            pz[j]  = ((float)id2[j] / numparticles) * z_dim;
        }

#ifdef ENABLE_MPI
        start = MPI_Wtime();
#endif
        ret = PDCregion_transfer_start_all(temp_requests, 8);
        if (ret != SUCCEED) {
            printf("Failed to start transfer for all regions\n");
            return 1;
        }
#ifdef ENABLE_MPI
        transfer_start += MPI_Wtime() - start;
#endif
        if (sleep_time) {
            sleep(sleep_time);
        }
#ifdef ENABLE_MPI
        start = MPI_Wtime();
#endif
        ret = PDCregion_transfer_wait_all(temp_requests, 8);
        if (ret != SUCCEED) {
            printf("Failed to wait transfer for all regions\n");
            return 1;
        }
#ifdef ENABLE_MPI
        end = MPI_Wtime();
        transfer_wait += end - start;
        start = end;
#endif

        ret = PDCregion_transfer_close(transfer_request_x[i]);
        if (ret != SUCCEED) {
            printf("region xx transfer close failed\n");
            return 1;
        }
        ret = PDCregion_transfer_close(transfer_request_y[i]);
        if (ret != SUCCEED) {
            printf("region yy transfer close failed\n");
            return 1;
        }
        ret = PDCregion_transfer_close(transfer_request_z[i]);
        if (ret != SUCCEED) {
            printf("region zz transfer close failed\n");
            return 1;
        }
        ret = PDCregion_transfer_close(transfer_request_px[i]);
        if (ret != SUCCEED) {
            printf("region pxx transfer close failed\n");
            return 1;
        }
        ret = PDCregion_transfer_close(transfer_request_py[i]);
        if (ret != SUCCEED) {
            printf("region pyy transfer close failed\n");
            return 1;
        }
        ret = PDCregion_transfer_close(transfer_request_pz[i]);
        if (ret != SUCCEED) {
            printf("region pzz transfer close failed\n");
            return 1;
        }
        ret = PDCregion_transfer_close(transfer_request_id1[i]);
        if (ret != SUCCEED) {
            printf("region id11 transfer close failed\n");
            return 1;
        }
        ret = PDCregion_transfer_close(transfer_request_id2[i]);
        if (ret != SUCCEED) {
            printf("region id22 transfer close failed\n");
            return 1;
        }
#ifdef ENABLE_MPI
        transfer_close += MPI_Wtime() - start;
#endif
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    for (i = 0; i < timestamps; ++i) {
        if (PDCobj_close(obj_xx[i]) < 0) {
            printf("fail to close obj_xx\n");
            return 1;
        }
        if (PDCobj_close(obj_yy[i]) < 0) {
            printf("fail to close object obj_yy\n");
            return 1;
        }
        if (PDCobj_close(obj_zz[i]) < 0) {
            printf("fail to close object obj_zz\n");
            return 1;
        }
        if (PDCobj_close(obj_pxx[i]) < 0) {
            printf("fail to close object obj_pxx\n");
            return 1;
        }
        if (PDCobj_close(obj_pyy[i]) < 0) {
            printf("fail to close object obj_pyy\n");
            return 1;
        }
        if (PDCobj_close(obj_pzz[i]) < 0) {
            printf("fail to close object obj_pzz\n");
            return 1;
        }
        if (PDCobj_close(obj_id11[i]) < 0) {
            printf("fail to close object obj_id11\n");
            return 1;
        }
        if (PDCobj_close(obj_id22[i]) < 0) {
            printf("fail to close object obj_id22\n");
            return 1;
        }
    }

    free(transfer_request_x);
#ifdef PDC_TIMING
    PDC_timing_report("read");
#endif

#ifdef ENABLE_MPI
    MPI_Reduce(&transfer_create, &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_create, &avg_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_create, &min_time, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    if (!rank) {
        printf("transfer create: %lf - %lf - %lf\n", min_time, avg_time / size, max_time);
    }
    MPI_Reduce(&transfer_start, &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_start, &avg_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_start, &min_time, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    if (!rank) {
        printf("transfer start: %lf - %lf - %lf\n", min_time, avg_time / size, max_time);
    }
    MPI_Reduce(&transfer_wait, &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_wait, &avg_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_wait, &min_time, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    if (!rank) {
        printf("transfer wait: %lf - %lf - %lf\n", min_time, avg_time / size, max_time);
    }
    MPI_Reduce(&transfer_close, &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_close, &avg_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&transfer_close, &min_time, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    if (!rank) {
        printf("transfer close: %lf - %lf - %lf\n", min_time, avg_time / size, max_time);
    }
#endif

    free(obj_xx);
    free(obj_yy);
    free(obj_zz);
    free(obj_pxx);
    free(obj_pyy);
    free(obj_pzz);
    free(obj_id11);
    free(obj_id22);

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
