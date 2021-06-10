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
#include "pdc.h"
#include "mercury.h"
#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"

#define MAX_THREADS (8)

typedef struct _thread_args_t {
    int     ThreadRank;
    pdcid_t pdcId;
} thread_args_t;

void *
TestThread(void *ThreadArgs)
{
    thread_args_t *args         = (thread_args_t *)ThreadArgs;
    int            tests_passed = 0;
    int            tests_failed = 0;

    // create a container property
    pdcid_t create_prop = PDCprop_create(PDC_CONT_CREATE, args->pdcId);
    if (create_prop > 0)
        printf("[%d] Create a container property, id is %llx\n", args->ThreadRank, create_prop);
    else
        printf("[%d] Fail to create container property @ line  %d!\n", args->ThreadRank, __LINE__);

    // create a container
    pdcid_t cont1 = PDCcont_create("c1", create_prop);
    if (cont1 > 0)
        tests_passed += 1;
    else
        tests_failed += 1;

    // create second container
    pdcid_t cont2 = PDCcont_create("c2", create_prop);
    if (cont2 > 0)
        tests_passed += 1;
    else
        tests_failed += 1;

    // create third container
    pdcid_t cont3 = PDCcont_create("c3", create_prop);
    if (cont3 > 0)
        tests_passed += 1;
    else
        tests_failed += 1;

    cont_handle *ch = PDCcont_iter_start(args->pdcId);
    while (!PDCcont_iter_null(ch)) {
        struct PDC_cont_info *info = PDCcont_iter_get_info(ch);
        printf("[%d] container name is: %s\n", args->ThreadRank, info->name);
        printf("[%d] container is in pdc %lld\n", args->ThreadRank, info->cont_pt->pdc->local_id);
        printf("[%d] container property id is %llx\n", args->ThreadRank, info->cont_pt->cont_prop_id);

        ch = PDCcont_iter_next(ch);
    }

    // close cont1
    if (PDCcont_close(cont1) < 0)
        printf("fail to close container %lld\n", cont1);
    else
        printf("[%d] successfully close container # %llx\n", args->ThreadRank, cont1);

    // close cont2
    if (PDCcont_close(cont2) < 0)
        printf("fail to close container %lld\n", cont2);
    else
        printf("[%d] successfully close container # %llx\n", args->ThreadRank, cont2);

    // close cont3
    if (PDCcont_close(cont3) < 0)
        printf("fail to close container %lld\n", cont3);
    else
        printf("[%d] successfully close container # %llx\n", args->ThreadRank, cont3);

    // close a container property
    if (PDCprop_close(create_prop) < 0)
        printf("[%d] Fail to close property @ line %d\n", args->ThreadRank, __LINE__);
    else
        printf("[%d] Successfully closed container property # %lld\n", args->ThreadRank, create_prop);

    return NULL;
}

int
main(int argc, char **argv)
{

    int            i, nThreads = 1;
    hg_thread_t    tId[MAX_THREADS];
    int            status = 0;
    thread_args_t *args;

    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    if (argc > 1)
        nThreads = (atoi(argv[1]));
    args = (thread_args_t *)malloc(nThreads * sizeof(thread_args_t));

    // create a pdc
    pdcid_t create_prop;
    pdcid_t pdc = PDC_init("pdc");
    printf("[MAIN] created a new pdc, pdc id is: %lld\n", pdc);

    /*Create nThreads threads in each process*/
    for (i = 0; i < nThreads; i++) {
        /* initialize thread argmuments */
        args[i].ThreadRank = i;
        args[i].pdcId      = pdc;
        hg_thread_create(&tId[i], TestThread, (void *)&args[i]);
    }

    /* Wait for all threads to finish their work*/
    for (i = 0; i < nThreads; i++) {
        status += hg_thread_join(tId[i]);
    }

    // close pdc
    if (PDC_close(pdc) < 0)
        printf("fail to close PDC\n");
    else
        printf("PDC is closed\n");

    free(args);
    if (status > 0) {
        printf("%d threads exited with an error status\n", status);
    }
    else {
        printf("No errors reported\n");
    }
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
