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
#include "dart_core.h"
#include "string_utils.h"
#include "test_helper.h"

int
assign_work_to_rank(int rank, int size, int nwork, int *my_count, int *my_start)
{
    if (rank > size || my_count == NULL || my_start == NULL) {
        LOG_ERROR("assign_work_to_rank(): Error with input\n");
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
    LOG_JUST_PRINT("%s n_obj n_round n_selectivity is_using_dart\n", name);
    LOG_JUST_PRINT(
        "Summary: This test will create n_obj objects, and add n_selectivity tags to each object. Then it "
        "will "
        "perform n_round collective queries against the tags, each query from each client should get "
        "a whole result set.\n");
    LOG_JUST_PRINT("Parameters:\n");
    LOG_JUST_PRINT("  n_obj: number of objects\n");
    LOG_JUST_PRINT(
        "  n_round: number of rounds, it can be the total number of tags too, as each round will perform "
        "one query against one tag\n");
    LOG_JUST_PRINT("  n_selectivity: selectivity, on a 100 scale. \n");
    LOG_JUST_PRINT("  is_using_dart: 1 for using dart, 0 for not using dart\n");
}

int
main(int argc, char *argv[])
{
    pdcid_t     pdc, cont_prop, cont, obj_prop;
    pdcid_t *   obj_ids;
    int         n_obj, n_add_tag, my_obj, my_obj_s, my_add_tag, my_add_tag_s;
    int         proc_num = 1, rank = 0, i, v, iter, round, selectivity, is_using_dart;
    char        obj_name[128];
    double      stime, total_time;
    pdc_kvtag_t kvtag;
    uint64_t *  pdc_ids;
    int         nres, ntotal;
    int         ret_value = SUCCEED;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#endif

    if (argc < 5) {
        if (rank == 0)
            print_usage(argv[0]);
        TGOTO_DONE(ret_value);
    }
    n_obj         = atoi(argv[1]);
    round         = atoi(argv[2]);
    selectivity   = atoi(argv[3]);
    is_using_dart = atoi(argv[4]);
    n_add_tag     = n_obj * selectivity / 100;

    // create a pdc
    TASSERT((pdc = PDCinit("pdc")) != 0, "Call to PDCinit succeeded", "Call to PDCinit failed");
    // create a container property
    TASSERT((cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // create a container
#ifdef ENABLE_MPI
    TASSERT((cont = PDCcont_create_coll("c1", cont_prop, MPI_COMM_WORLD)) != 0,
            "Call to PDCcont_create_coll succeeded", "Call to PDCcont_create_coll failed");
#else
    TASSERT((cont = PDCcont_create("c1", cont_prop)) != 0, "Call to PDCcont_create succeeded",
            "Call to PDCcont_create failed");
#endif
    // create an object property
    TASSERT((obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc)) != 0, "Call to PDCprop_create succeeded",
            "Call to PDCprop_create failed");
    // Create a number of objects, add at least one tag to that object
    assign_work_to_rank(rank, proc_num, n_obj, &my_obj, &my_obj_s);

    obj_ids = (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));
    for (i = 0; i < my_obj; i++) {
        sprintf(obj_name, "obj%d", my_obj_s + i);
        TASSERT((obj_ids[i] = PDCobj_create(cont, obj_name, obj_prop)) != 0,
                "Call to PDCobj_create succeeded", "Call to PDCobj_create failed");
    }

    if (rank == 0)
        LOG_INFO("Created %d objects\n", n_obj);

    char *attr_name_per_rank = gen_random_strings(1, 6, 8, 26)[0];
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

    assign_work_to_rank(rank, proc_num, n_add_tag, &my_add_tag, &my_add_tag_s);

    // This is for adding #rounds tags to the objects.
    for (i = 0; i < my_add_tag; i++) {
        for (iter = 0; iter < round; iter++) {
            v = iter;
            sprintf(value, "%d", v);
            if (is_using_dart &&
                PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, value, strlen(value), PDC_STRING,
                                                    ref_type, (uint64_t)obj_ids[i]) < 0)
                TGOTO_ERROR(FAIL, "Failed to add a kvtag to o%d\n", i + my_obj_s);
            else if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0)
                TGOTO_ERROR(FAIL, "Failed to add a kvtag to o%d\n", i + my_obj_s);
        }
        if (rank == 0)
            LOG_JUST_PRINT("Rank %d: Added %d kvtag to the %d th object\n", rank, round, i);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

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
            sprintf(value, "%d", v);
            sprintf(exact_query, "%s=%s", kvtag.name, value);
#ifdef ENABLE_MPI
            if (PDC_Client_search_obj_ref_through_dart_mpi(hash_algo, exact_query, ref_type, &nres, &pdc_ids,
                                                           MPI_COMM_WORLD) < 0) {
#else
            if (PDC_Client_search_obj_ref_through_dart(hash_algo, exact_query, ref_type, &nres, &pdc_ids) <
                0) {
#endif
                TGOTO_ERROR(TFAIL, "Failed to PDC_Client_search_obj_ref_through_dart_mpi\n", kvtag.name,
                            rank);
            }
        }
        else {
#ifdef ENABLE_MPI
            if (PDC_Client_query_kvtag_mpi(&kvtag, &nres, &pdc_ids, MPI_COMM_WORLD) < 0) {
#else
            if (PDC_Client_query_kvtag(&kvtag, &nres, &pdc_ids) < 0) {
#endif
                TGOTO_ERROR(TFAIL, "Failed to query kvtag [%s] with rank %d\n", kvtag.name, rank);
            }
        }
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&nres, &ntotal, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;

    if (rank == 0)
        LOG_INFO("Total time to query %d objects with tag: %.5f\n", ntotal, total_time);
#else
    LOG_INFO("Query found %d objects\n", nres);
#endif
    // close a container
    TASSERT(PDCcont_close(cont) >= 0, "Call to PDCcont_close succeeded", "Call to PDCcont_close failed");
    // close an object property
    TASSERT(PDCprop_close(obj_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");
    // close a container property
    TASSERT(PDCprop_close(cont_prop) >= 0, "Call to PDCprop_close succeeded", "Call to PDCprop_close failed");

    // close pdc
    TASSERT(PDCclose(pdc) >= 0, "Call to PDCclose succeeded", "Call to PDCclose failed");

done:
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return ret_value;
}
