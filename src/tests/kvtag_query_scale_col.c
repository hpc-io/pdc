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
    printf("%s n_obj n_round n_selectivity is_using_dart\n", name);
    printf("Summary: This test will create n_obj objects, and add n_selectivity tags to each object. Then it "
           "will "
           "perform n_round point-to-point queries against the tags, each query from each client should get "
           "a whole result set.\n");
    printf("Parameters:\n");
    printf("  n_obj: number of objects\n");
    printf("  n_round: number of rounds, it can be the total number of tags too, as each round will perform "
           "one query against one tag\n");
    printf("  n_selectivity: selectivity, on a 100 scale. \n");
    printf("  is_using_dart: 1 for using dart, 0 for not using dart\n");
}

char **
gen_random_strings(int count, int maxlen, int alphabet_size)
{
    int    c      = 0;
    int    i      = 0;
    char **result = (char **)calloc(count, sizeof(char *));
    for (c = 0; c < count; c++) {
        int   len = (rand() % (maxlen - 1)) + 1; // Ensure at least 1 character
        char *str = (char *)calloc(len + 1, sizeof(char));
        for (i = 0; i < len; i++) {
            char chr = (char)((rand() % alphabet_size) + 65); // ASCII printable characters
            str[i]   = chr;
        }
        str[len]  = '\0'; // Null-terminate the string
        result[c] = str;
    }
    return result;
}

int
main(int argc, char *argv[])
{
    pdcid_t     pdc, cont_prop, cont, obj_prop;
    pdcid_t *   obj_ids;
    int         n_obj, n_add_tag, my_obj, my_obj_s, my_add_tag, my_add_tag_s;
    int         proc_num, my_rank, i, v, iter, round, selectivity, is_using_dart;
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

    if (argc < 4) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj         = atoi(argv[1]);
    round         = atoi(argv[2]);
    selectivity   = atoi(argv[3]);
    is_using_dart = atoi(argv[4]);
    n_add_tag     = n_obj * selectivity / 100;

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

    char *attr_name_per_rank = gen_random_strings(1, 8, 26)[0];
    // Add tags
    kvtag.name  = attr_name_per_rank;
    kvtag.value = (void *)&v;
    kvtag.type  = PDC_INT;
    kvtag.size  = sizeof(int);

    char key[32];
    char value[32];
    char exact_query[48];

    dart_object_ref_type_t ref_type  = REF_PRIMARY_ID;
    dart_hash_algo_t       hash_algo = DART_HASH;

    // tag-obj map:
    // 0: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    // 1: 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    // 2: 20, 21, 22, 23, 24, 25, 26, 27, 28, 29
    // 3: 30, 31, 32, 33, 34, 35, 36, 37, 38, 39

    for (iter = 0; iter < round; iter++) {
        assign_work_to_rank(my_rank, proc_num, n_add_tag, &my_add_tag, &my_add_tag_s);
        v = iter;
        sprintf(value, "%d", v);
        if (is_using_dart) {
            for (i = 0; i < my_add_tag; i++) {
                if (PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, value, ref_type,
                                                        (uint64_t)obj_ids[i]) < 0) {
                    printf("fail to add a kvtag to o%d\n", i + my_obj_s);
                }
            }
        }
        else {
            for (i = 0; i < my_add_tag; i++) {
                if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0)
                    printf("fail to add a kvtag to o%d\n", i + my_obj_s);
            }
        }

        if (my_rank == 0)
            printf("Rank %d: Added a kvtag to %d objects\n", my_rank, my_add_tag);
        fflush(stdout);

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
#endif
    }

    kvtag.name  = attr_name_per_rank;
    kvtag.value = (void *)&v;
    kvtag.type  = PDC_INT;
    kvtag.size  = sizeof(int);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
#endif

    for (iter = 0; iter < round; iter++) {
        v = iter;

        if (is_using_dart) {
            sprintf(value, "%ld", v);
            sprintf(exact_query, "%s=%s", kvtag.name, value);
            PDC_Client_search_obj_ref_through_dart(hash_algo, exact_query, ref_type, &nres, &pdc_ids);
        }
        else {
            if (PDC_Client_query_kvtag_mpi(&kvtag, &nres, &pdc_ids, MPI_COMM_WORLD) < 0) {
                printf("fail to query kvtag [%s] with rank %d\n", kvtag.name, my_rank);
                break;
            }
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&nres, &ntotal, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;

    if (my_rank == 0)
        println("Total time to query %d objects with tag: %.5f", ntotal, total_time);
#endif
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
