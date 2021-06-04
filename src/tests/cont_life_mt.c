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

    // print default container lifetime (persistent)
    struct PDC_cont_prop *prop = PDCcont_prop_get_info(create_prop);
    if (prop->cont_life == PDC_PERSIST)
        printf("[%d] container property (id: %lld) default lifetime is persistent\n", args->ThreadRank,
               create_prop);
    else
        printf("[%d] container property (id: %lld) default lifetime is transient\n", args->ThreadRank,
               create_prop);

    // create a container
    pdcid_t cont = PDCcont_create("c1", create_prop);
    if (cont > 0)
        printf("[%d] Create a container, id is %lld\n", args->ThreadRank, cont);
    else
        printf("[%d] Failed to create container @ line  %d!\n", args->ThreadRank, __LINE__);

    // set container lifetime to transient
    PDCprop_set_cont_lifetime(create_prop, PDC_TRANSIENT);
    prop = PDCcont_prop_get_info(create_prop);
    if (prop->cont_life == PDC_PERSIST)
        printf("[%d] container property (id: %lld) lifetime is persistent\n", args->ThreadRank, create_prop);
    else
        printf("[%d] container property (id: %lld) lifetime is transient\n", args->ThreadRank, create_prop);

    // set container lifetime to persistent
    PDCcont_persist(cont);
    prop = PDCcont_prop_get_info(create_prop);
    if (prop->cont_life == PDC_PERSIST)
        printf("[%d] container property (id: %lld) lifetime is persistent\n", args->ThreadRank, create_prop);
    else
        printf("[%d] container property (id: %lld) lifetime is transient\n", args->ThreadRank, create_prop);

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("[%d] failed to close container %lld\n", args->ThreadRank, cont);
    else
        printf("[%d] successfully closed container # %lld\n", args->ThreadRank, cont);

    // close a container property
    if (PDCprop_close(create_prop) < 0)
        printf("[%d] Fail to close property @ line %d\n", args->ThreadRank, __LINE__);
    else
        printf("[%d] successfully closed container property # %lld\n", args->ThreadRank, create_prop);

    return NULL;
}

int
main(int argc, char **argv)
{

    int            i, nThreads = 1;
    hg_thread_t    tId[MAX_THREADS];
    int            status = 0;
    thread_args_t *args;
    int            rank = 0, size = 1;

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
