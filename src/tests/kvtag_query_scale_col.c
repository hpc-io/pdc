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
#include <inttypes.h>
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
    printf("%s n_obj n_round n_selectivity is_using_dart query_type comm_type\n", name);
    printf("Summary: This test will create n_obj objects, and add n_selectivity tags to each object. Then it "
           "will "
           "perform n_round collective queries against the tags, each query from each client should get "
           "a whole result set.\n");
    printf("Parameters:\n");
    printf("  n_obj: number of objects\n");
    printf("  n_round: number of rounds, it can be the total number of tags too, as each round will perform "
           "one query against one tag\n");
    printf("  n_selectivity: selectivity, on a 100 scale. \n");
    printf("  is_using_dart: 1 for using dart, 0 for not using dart\n");
    printf("  query_type: 0 for exact, 1 for prefix, 2 for suffix, 3 for infix\n");
    printf("  comm_type: 0 for point-to-point, 1 for collective\n");
}

perr_t
prepare_container(pdcid_t *pdc, pdcid_t *cont_prop, pdcid_t *cont, pdcid_t *obj_prop, int my_rank)
{
    perr_t ret_value = FAIL;
    // create a pdc
    *pdc = PDCinit("pdc");

    // create a container property
    *cont_prop = PDCprop_create(PDC_CONT_CREATE, *pdc);
    if (*cont_prop <= 0) {
        printf("[Client %d] Fail to create container property @ line  %d!\n", my_rank, __LINE__);
        goto done;
    }
    // create a container
    *cont = PDCcont_create("c1", *cont_prop);
    if (*cont <= 0) {
        printf("[Client %d] Fail to create container @ line  %d!\n", my_rank, __LINE__);
        goto done;
    }

    // create an object property
    *obj_prop = PDCprop_create(PDC_OBJ_CREATE, *pdc);
    if (*obj_prop <= 0) {
        printf("[Client %d] Fail to create object property @ line  %d!\n", my_rank, __LINE__);
        goto done;
    }

    ret_value = SUCCEED;
done:
    return ret_value;
}

perr_t
creating_objects(pdcid_t **obj_ids, int my_obj, int my_obj_s, pdcid_t cont, pdcid_t obj_prop, int my_rank)
{
    perr_t  ret_value = FAIL;
    char    obj_name[128];
    int64_t timestamp = get_timestamp_ms();
    *obj_ids          = (pdcid_t *)calloc(my_obj, sizeof(pdcid_t));
    for (int i = 0; i < my_obj; i++) {
        sprintf(obj_name, "obj%" PRId64 "%d", timestamp, my_obj_s + i);
        (*obj_ids)[i] = PDCobj_create(cont, obj_name, obj_prop);
        if ((*obj_ids)[i] <= 0) {
            printf("[Client %d] Fail to create object @ line  %d!\n", my_rank, __LINE__);
            goto done;
        }
    }
    ret_value = SUCCEED;
done:
    return ret_value;
}

int
main(int argc, char *argv[])
{
    pdcid_t     pdc, cont_prop, cont, obj_prop;
    pdcid_t *   obj_ids;
    int         n_obj, my_obj, my_obj_s;
    int         proc_num, my_rank, i, v, iter, round, selectivity, is_using_dart, query_type, comm_type;
    double      stime, total_time;
    pdc_kvtag_t kvtag;
    uint64_t *  pdc_ids;
    int         nres, ntotal;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif

    if (argc < 7) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj         = atoi(argv[1]);
    round         = atoi(argv[2]);
    selectivity   = atoi(argv[3]);
    is_using_dart = atoi(argv[4]); // 0 for no index, 1 for using dart.
    query_type    = atoi(argv[5]); // 0 for exact, 1 for prefix, 2 for suffix, 3 for infix,
                                   // 4 for num_exact, 5 for num_range
    comm_type = atoi(argv[6]);     // 0 for point-to-point, 1 for collective

    // prepare container
    if (prepare_container(&pdc, &cont_prop, &cont, &obj_prop, my_rank) < 0) {
        println("fail to prepare container @ line %d", __LINE__);
        goto done;
    }
    // Create a number of objects, add at least one tag to that object
    assign_work_to_rank(my_rank, proc_num, n_obj, &my_obj, &my_obj_s);

    if (my_rank == 0) {
        println("Each client will create about %d obj", my_obj);
    }

    // creating objects
    creating_objects(&obj_ids, my_obj, my_obj_s, cont, obj_prop, my_rank);

    if (my_rank == 0)
        println("All clients created %d objects", n_obj);

    dart_object_ref_type_t ref_type  = REF_PRIMARY_ID;
    dart_hash_algo_t       hash_algo = DART_HASH;

    // This is for adding #rounds tags to the objects.
    // Each rank will add #rounds tags to #my_obj objects.
    // With the selectivity, we should be able to control how many objects will be attached with the #round
    // tags. So that, each of these #round tags can roughly the same selectivity.
    int my_obj_after_selectivity = my_obj * selectivity / 100;
    for (i = 0; i < my_obj_after_selectivity; i++) {
        for (iter = 0; iter < round; iter++) {
            char attr_name[64];
            char tag_value[64];
            snprintf(attr_name, 63, "%d%dattr_name%d%d", iter, my_rank, my_rank, iter);
            snprintf(tag_value, 63, "%d%dtag_value%d%d", iter, my_rank, my_rank, iter);
            kvtag.name  = strdup(attr_name);
            kvtag.value = (void *)strdup(tag_value);
            kvtag.type  = PDC_STRING;
            kvtag.size  = (strlen(tag_value) + 1) * sizeof(char);
            if (is_using_dart) {
                if (PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, kvtag.value, ref_type,
                                                        (uint64_t)obj_ids[i]) < 0) {
                    printf("fail to add a kvtag to o%d\n", i + my_obj_s);
                }
            }
            else {
                if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0) {
                    printf("fail to add a kvtag to o%d\n", i + my_obj_s);
                }
            }
            free(kvtag.name);
            free(kvtag.value);
        }
        if (my_rank == 0) {
            println("Rank %d: Added %d kvtag to the %d / %d th object, I'm applying selectivity %d to %d "
                    "objects.\n",
                    my_rank, round, i + 1, my_obj_after_selectivity, selectivity, my_obj);
        }
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    // For the queries, we issue #round queries.
    // The selectivity of each exact query should be #selectivity / 100 * #n_obj.
    // Namely, if you have 1M objects, selectivity is 10, then each query should return 100K objects.
    for (comm_type = 0; comm_type >= 0; comm_type--) {
        for (query_type = 0; query_type < 4; query_type++) {
            perr_t ret_value;
            if (comm_type == 0 && is_using_dart == 0)
                round = 2;
            int round_total = 0;
            for (iter = -1; iter < round; iter++) { // -1 is for warm up
#ifdef ENABLE_MPI
                if (iter == 0) {
                    MPI_Barrier(MPI_COMM_WORLD);
                    stime = MPI_Wtime();
                }
#endif
                char attr_name[64];
                char tag_value[64];
                snprintf(attr_name, 63, "%d%dattr_name%d%d", iter, my_rank, my_rank, iter);
                snprintf(tag_value, 63, "%d%dtag_value%d%d", iter, my_rank, my_rank, iter);

                kvtag.name  = strdup(attr_name);
                kvtag.value = (void *)strdup(tag_value);
                kvtag.type  = PDC_STRING;
                kvtag.size  = (strlen(tag_value) + 1) * sizeof(char);

                query_gen_input_t  input;
                query_gen_output_t output;
                input.base_tag         = &kvtag;
                input.key_query_type   = query_type;
                input.value_query_type = query_type;
                input.affix_len        = 4;

                gen_query_key_value(&input, &output);

                if (is_using_dart) {
                    char *query_string = gen_query_str(&output);
                    ret_value          = (comm_type == 0)
                                    ? PDC_Client_search_obj_ref_through_dart(hash_algo, query_string,
                                                                             ref_type, &nres, &pdc_ids)
                                    : PDC_Client_search_obj_ref_through_dart_mpi(
                                          hash_algo, query_string, ref_type, &nres, &pdc_ids, MPI_COMM_WORLD);
                }
                else {
                    kvtag.name  = output.key_query;
                    kvtag.value = output.value_query;
                    ret_value   = (comm_type == 0)
                                    ? PDC_Client_query_kvtag(&kvtag, &nres, &pdc_ids)
                                    : PDC_Client_query_kvtag_mpi(&kvtag, &nres, &pdc_ids, MPI_COMM_WORLD);
                }
                if (ret_value < 0) {
                    printf("fail to query kvtag [%s] with rank %d\n", kvtag.name, my_rank);
                    break;
                }
                round_total += nres;
                free(kvtag.name);
                free(kvtag.value);
            }

#ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
            // MPI_Reduce(&round_total, &ntotal, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            total_time = MPI_Wtime() - stime;

            if (my_rank == 0) {
                char *query_type_str = "EXACT";
                if (query_type == 1)
                    query_type_str = "PREFIX";
                else if (query_type == 2)
                    query_type_str = "SUFFIX";
                else if (query_type == 3)
                    query_type_str = "INFIX";
                println("[%s Client %s Query with%sINDEX] %d rounds with %d results, time: %.5f ms",
                        comm_type == 0 ? "Single" : "Multi", query_type_str,
                        is_using_dart == 0 ? " NO " : " DART ", round, round_total, total_time * 1000.0);
            }
#endif
        }
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
