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
#include <sys/time.h>
#include <inttypes.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

int
main(int argc, char **argv)
{
    int      rank = 0, size = 1, i;
    pdcid_t  pdc, cont_prop, cont, obj_prop, obj1 = 0;
    uint64_t d[3] = {10, 20, 30};
    char     obj_name[64];

    struct pdc_region_info *region = NULL;
    struct pdc_region_info  region_info_no_overlap;
    uint64_t                start_no_overlap[3] = {1, 1, 1};
    uint64_t                count_no_overlap[3] = {1, 1, 1};

    struct timeval start_time;
    struct timeval end;
    long long      elapsed;
    double         total_lock_overhead;

    pdcid_t reg;
    perr_t  ret;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    // create a pdc
    pdc = PDCinit("pdc");

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    // create a container
    cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    // create an object property
    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    // set object dimension
    PDCprop_set_obj_dims(obj_prop, 3, d);
    PDCprop_set_obj_time_step(obj_prop, 0);

    // Only rank 0 create a object
    if (rank == 0) {
        sprintf(obj_name, "test_obj");
        obj1 = PDCobj_create(cont, obj_name, obj_prop);
        if (obj1 <= 0)
            printf("Fail to create object @ line  %d!\n", __LINE__);
    }

    for (i = 0; i < 3; i++) {
        start_no_overlap[i] *= rank;
    }
    region_info_no_overlap.ndim   = 3;
    region_info_no_overlap.offset = &start_no_overlap[0];
    region_info_no_overlap.size   = &count_no_overlap[0];

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);

    reg = PDCregion_create(region_info_no_overlap.ndim, start_no_overlap, count_no_overlap);
    ret = PDCreg_obtain_lock(obj1, reg, PDC_WRITE, PDC_NOBLOCK);

    if (ret != SUCCEED)
        printf("[%d] Failed to obtain lock for region (%" PRIu64 ",%" PRIu64 ",%" PRIu64 ") (%" PRIu64
               ",%" PRIu64 ",%" PRIu64 ") ... error\n",
               rank, region->offset[0], region->offset[1], region->offset[2], region->size[0],
               region->size[1], region->size[2]);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&end, 0);

    elapsed             = (end.tv_sec - start_time.tv_sec) * 1000000LL + end.tv_usec - start_time.tv_usec;
    total_lock_overhead = elapsed / 1000000.0;

    if (rank == 0) {
        printf("Total lock overhead        : %.6f\n", total_lock_overhead);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&start_time, 0);

    ret = PDCreg_release_lock(obj1, reg, PDC_WRITE);
    if (ret != SUCCEED)
        printf("[%d] Failed to release lock for region (%" PRIu64 ",%" PRIu64 ",%" PRIu64 ") (%" PRIu64
               ",%" PRIu64 ",%" PRIu64 ") ... error\n",
               rank, region->offset[0], region->offset[1], region->offset[2], region->size[0],
               region->size[1], region->size[2]);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    gettimeofday(&end, 0);

    elapsed             = (end.tv_sec - start_time.tv_sec) * 1000000LL + end.tv_usec - start_time.tv_usec;
    total_lock_overhead = elapsed / 1000000.0;

    if (rank == 0) {
        printf("Total lock release overhead: %.6f\n", total_lock_overhead);
    }

    // close object
    if (rank == 0) {
        if (PDCobj_close(obj1) < 0)
            printf("fail to close object o1\n");
    }

    // close object property
    if (PDCprop_close(obj_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    // close pdc
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
