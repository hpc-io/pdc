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
#include <getopt.h>
#include <time.h>
#include "pdc.h"
#include "pdc_client_connect.h"

int
assign_work_to_rank(int rank, int size, int nwork, int *my_count, int *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        printf("assign_work_to_rank(): Error with input!\n");
        return -1;
    }
    if (nwork < size) {
        if (rank < nwork)
            *my_count = 1;
        else
            *my_count = 0;
        (*my_start) = rank * (*my_count);
    }
    else {
        (*my_count) = nwork / size;
        (*my_start) = rank * (*my_count);

        // Last few ranks may have extra work
        if (rank >= size - nwork % size) {
            (*my_count)++;
            (*my_start) += (rank - (size - nwork % size));
        }
    }

    return 1;
}

void
print_usage(char *name)
{
    printf("%s n_obj n_query\n", name);
}

int
main(int argc, char *argv[])
{
    pdcid_t     pdc, cont_prop, cont, obj_prop;
    pdcid_t *   obj_ids;
    int         n_obj, n_add_tag, my_obj, my_obj_s, my_add_tag, my_add_tag_s;
    int         proc_num, my_rank, i, v, iter;
    char        obj_name[128];
    double      stime, total_time;
    pdc_kvtag_t kvtag;
    uint64_t *  pdc_ids;
    int         nres, ntotal;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif

    if (argc != 2) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj     = atoi(argv[1]);
    n_add_tag = n_obj / 100;
    int round = 7;

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

    // Create a number of objects, add at least one tag to that object
    assign_work_to_rank(my_rank, proc_num, n_obj, &my_obj, &my_obj_s);
    if (my_rank == 0)
        printf("I will create %d obj\n", my_obj);
    obj_ids = (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));
    for (i = 0; i < my_obj; i++) {
        sprintf(obj_name, "obj%d", my_obj_s + i);
        obj_ids[i] = PDCobj_create(cont, obj_name, obj_prop);
        if (obj_ids[i] <= 0)
            printf("Fail to create object @ line  %d!\n", __LINE__);
    }

    if (my_rank == 0)
        printf("Created %d objects\n", n_obj);
    fflush(stdout);

    // Add tags
    kvtag.name  = "Group";
    kvtag.value = (void *)&v;
    kvtag.size  = sizeof(int);

    for (iter = 0; iter < round; iter++) {
        assign_work_to_rank(my_rank, proc_num, n_add_tag, &my_add_tag, &my_add_tag_s);

        v = iter;
        for (i = 0; i < my_add_tag; i++) {
            if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.size) < 0)
                printf("fail to add a kvtag to o%d\n", i + my_obj_s);
        }

        if (my_rank == 0)
            printf("Rank %d: Added a kvtag to %d objects\n", my_rank, my_add_tag);
        fflush(stdout);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif

        n_add_tag *= 2;
    }

    kvtag.name  = "Group";
    kvtag.value = (void *)&v;
    kvtag.size  = sizeof(int);

    for (iter = 0; iter < round; iter++) {
        v = iter;

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        stime = MPI_Wtime();
#endif

        if (PDC_Client_query_kvtag_col(&kvtag, &nres, &pdc_ids) < 0) {
            printf("fail to query kvtag [%s] with rank %d\n", kvtag.name, my_rank);
            break;
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Reduce(&nres, &ntotal, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        total_time = MPI_Wtime() - stime;
#endif

        if (my_rank == 0)
            printf("Total time to query %d objects with tag: %.4f\n", ntotal, total_time);
        fflush(stdout);
    }

    // close a container
    if (PDCcont_close(cont) < 0)
        printf("fail to close container c1\n");
    else
        printf("successfully close container c1\n");

    // close an object property
    if (PDCprop_close(obj_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close object property\n");

    // close a container property
    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);
    else
        printf("successfully close container property\n");

    // close pdc
    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");
done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
