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
#include <assert.h>
#include "pdc.h"
#include "pdc_client_connect.h"
#include "string_utils.h"

dart_object_ref_type_t ref_type  = REF_PRIMARY_ID;
dart_hash_algo_t       hash_algo = DART_HASH;

int
compare_uint64(const void *a, const void *b)
{
    return (*(uint64_t *)a - *(uint64_t *)b);
}

void
print_usage(char *name)
{
    printf("%s \n", name);
    printf("Summary: This test will create a number of objects which are attached with a number of tags. "
           "Then, it will"
           "perform different types queries supported by IDIOMS to retrieve the object IDs, and perform the "
           "necessary validation.\n");
}

perr_t
prepare_container(pdcid_t *pdc, pdcid_t *cont_prop, pdcid_t *cont, pdcid_t *obj_prop, int world_rank)
{
    perr_t ret_value = FAIL;
    // create a pdc
    *pdc = PDCinit("pdc");

    // create a container property
    *cont_prop = PDCprop_create(PDC_CONT_CREATE, *pdc);
    if (*cont_prop <= 0) {
        printf("[Client %d] Fail to create container property @ line  %d!\n", world_rank, __LINE__);
        goto done;
    }
    // create a container
    *cont = PDCcont_create("c1", *cont_prop);
    if (*cont <= 0) {
        printf("[Client %d] Fail to create container @ line  %d!\n", world_rank, __LINE__);
        goto done;
    }

    // create an object property
    *obj_prop = PDCprop_create(PDC_OBJ_CREATE, *pdc);
    if (*obj_prop <= 0) {
        printf("[Client %d] Fail to create object property @ line  %d!\n", world_rank, __LINE__);
        goto done;
    }

    ret_value = SUCCEED;
done:
    return ret_value;
}

perr_t
insert_index_records(int world_rank, int world_size)
{
    int    i;
    perr_t ret_value = SUCCEED;
    for (i = 0; i < 1000; i++) {
        if (i % world_size != world_rank) {
            continue;
        }
        char *key   = (char *)calloc(64, sizeof(char));
        void *value = (char *)calloc(64, sizeof(char));
        snprintf(key, 63, "str%03dstr", i);
        snprintf(value, 63, "str%03dstr", i);
        if (PDC_Client_insert_obj_ref_into_dart(hash_algo, key, value, strlen(value) + 1, PDC_STRING,
                                                ref_type, i) < 0) {
            printf("CLIENT %d failed to create index record %s %s %" PRIu64 "\n", world_rank, key, value, i);
            ret_value |= FAIL;
        }
        free(key);
        free(value);

        key = (char *)calloc(16, sizeof(char));
        sprintf(key, "int%s", "key");
        value               = (int64_t *)calloc(1, sizeof(int64_t));
        *((int64_t *)value) = i;
        if (PDC_Client_insert_obj_ref_into_dart(hash_algo, key, value, sizeof(int64_t), PDC_INT64, ref_type,
                                                i) < 0) {
            printf("CLIENT %d failed to create index record %s %" PRId64 " %d\n", world_rank, key, value, i);
            ret_value |= FAIL;
        }
        free(key);
        free(value);
    }
    return ret_value;
}

perr_t
delete_index_records(int world_rank, int world_size)
{
    int    i;
    perr_t ret_value = SUCCEED;
    for (i = 0; i < 1000; i++) {
        if (i % world_size != world_rank) {
            continue;
        }
        char *key   = (char *)calloc(64, sizeof(char));
        void *value = (char *)calloc(64, sizeof(char));
        snprintf(key, 63, "str%03dstr", i);
        snprintf(value, 63, "str%03dstr", i);
        if (PDC_Client_delete_obj_ref_from_dart(hash_algo, key, value, strlen(value) + 1, PDC_STRING,
                                                ref_type, i) < 0) {
            printf("CLIENT %d failed to create index record %s %s %" PRIu64 "\n", world_rank, key, value, i);
            ret_value |= FAIL;
        }
        free(key);
        free(value);

        key = (char *)calloc(16, sizeof(char));
        sprintf(key, "int%s", "key");
        value               = (int64_t *)calloc(1, sizeof(int64_t));
        *((int64_t *)value) = i;
        if (PDC_Client_delete_obj_ref_from_dart(hash_algo, key, value, sizeof(int64_t), PDC_INT64, ref_type,
                                                i) < 0) {
            printf("CLIENT %d failed to create index record %s %" PRId64 " %d\n", world_rank, key, value, i);
            ret_value |= FAIL;
        }
        free(key);
        free(value);
    }
    return ret_value;
}

int
validate_empty_result(int world_rank, int nres, uint64_t *pdc_ids)
{
    int query_series = world_rank % 6;
    if (nres > 0) {
        printf("fail to query kvtag [%s] with rank %d\n", "str109str=str109str", world_rank);
        return query_series;
    }
    return -1;
}

int
validate_query_result(int world_rank, int nres, uint64_t *pdc_ids)
{
    int i;
    int query_series = world_rank % 6;
    int step_failed  = -1;
    switch (query_series) {
        case 0:
            if (nres != 1) {
                printf("fail to query kvtag [%s] with rank %d\n", "str109str=str109str", world_rank);
                step_failed = 0;
            }
            if (pdc_ids[0] != 109) {
                printf("fail to query kvtag [%s] with rank %d\n", "str109str=str109str", world_rank);
                step_failed = 0;
            }
            break;
        case 1:
            if (nres != 10) {
                printf("fail to query kvtag [%s] with rank %d\n", "str09*=str09*", world_rank);
                step_failed = 1;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != i + 90) {
                    printf("fail to query kvtag [%s] with rank %d\n", "str09*=str09*", world_rank);
                    step_failed = 1;
                    break;
                }
            }
            break;
        case 2:
            if (nres != 10) {
                printf("fail to query kvtag [%s] with rank %d\n", "*09str=*09str", world_rank);
                step_failed = 2;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != i * 100 + 9) {
                    printf("fail to query kvtag [%s] with rank %d\n", "*09str=*09str", world_rank);
                    step_failed = 2;
                    break;
                }
            }
            break;
        case 3:
            if (nres != 20) {
                printf("fail to query kvtag [%s] with rank %d, nres = %d, expected 20\n", "*09*=*09*",
                       world_rank, nres);
                step_failed = 3;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            uint64_t expected[20] = {9,  90,  91,  92,  93,  94,  95,  96,  97,  98,
                                     99, 109, 209, 309, 409, 509, 609, 709, 809, 909};
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != expected[i]) {
                    printf("fail to query kvtag [%s] with rank %d, pdc_ids[%d]=%" PRIu64 ", expected %" PRIu64
                           "\n",
                           "*09*=*09*", world_rank, i, pdc_ids[i], expected[i]);
                    step_failed = 3;
                    break;
                }
            }
            break;
        case 4:
            if (nres != 1) {
                printf("fail to query kvtag [%s] with rank %d\n", "intkey=109", world_rank);
                step_failed = 4;
            }
            if (pdc_ids[0] != 109) {
                printf("fail to query kvtag [%s] with rank %d\n", "intkey=109", world_rank);
                step_failed = 4;
            }
            break;
        case 5:
            if (nres != 10) {
                printf("fail to query kvtag [%s] with rank %d\n", "intkey=90|~|99", world_rank);
                step_failed = 5;
            }
            // the result is not in order, so we need to sort the result first
            qsort(pdc_ids, nres, sizeof(uint64_t), compare_uint64);
            for (i = 0; i < nres; i++) {
                if (pdc_ids[i] != i + 90) {
                    printf("fail to query kvtag [%s] with rank %d\n", "intkey=90|~|99", world_rank);
                    step_failed = 5;
                    break;
                }
            }
            break;
        default:
            break;
    }
    return step_failed;
}

int
search_through_index(int world_rank, int world_size, int (*validator)(int r, int n, uint64_t *ids))
{
    int       nres;
    uint64_t *pdc_ids;
    int       query_seires = world_rank % 6;
    int       step_failed  = -1;
    switch (query_seires) {
        case 0:
            // exact string query
            if (PDC_Client_search_obj_ref_through_dart(hash_algo, "str109str=\"str109str\"", ref_type, &nres,
                                                       &pdc_ids) < 0) {
                printf("fail to query kvtag [%s] with rank %d\n", "str109str=str109str", world_rank);
                step_failed = 0;
            };
            break;
        case 1:
            // prefix string query
            if (PDC_Client_search_obj_ref_through_dart(hash_algo, "str09*=\"str09*\"", ref_type, &nres,
                                                       &pdc_ids) < 0) {
                printf("fail to query kvtag [%s] with rank %d\n", "str09*=str09*", world_rank);
                step_failed = 1;
            }
            break;
        case 2:
            // suffix string query
            if (PDC_Client_search_obj_ref_through_dart(hash_algo, "*09str=\"*09str\"", ref_type, &nres,
                                                       &pdc_ids) < 0) {
                printf("fail to query kvtag [%s] with rank %d\n", "*09str=*09str", world_rank);
                step_failed = 2;
            }
            break;
        // case 3:
        //     // infix string query
        //     if (PDC_Client_search_obj_ref_through_dart(hash_algo, "*09*=\"*09*\"", ref_type, &nres,
        //                                                &pdc_ids) < 0) {
        //         printf("fail to query kvtag [%s] with rank %d\n", "*09*=*09*", world_rank);
        //         step_failed = 3;
        //     }
        //     break;
        case 4:
            // exact integer query
            if (PDC_Client_search_obj_ref_through_dart(hash_algo, "intkey=109", ref_type, &nres, &pdc_ids) <
                0) {
                printf("fail to query kvtag [%s] with rank %d\n", "intkey=109", world_rank);
                step_failed = 4;
            }
            break;
        case 5:
            // range integer query
            if (PDC_Client_search_obj_ref_through_dart(hash_algo, "intkey=90|~|99", ref_type, &nres,
                                                       &pdc_ids) < 0) {
                printf("fail to query kvtag [%s] with rank %d\n", "intkey=90|~|99", world_rank);
                step_failed = 5;
            }
            break;
        default:
            break;
    }
    if (step_failed < 0) {
        step_failed = validator(world_rank, nres, pdc_ids);
    }
    return step_failed;
}

int
main(int argc, char *argv[])
{
    pdcid_t pdc, cont_prop, cont, obj_prop;
    int     world_size, world_rank, i;
    double  stime, total_time;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
#endif

    // prepare container
    if (prepare_container(&pdc, &cont_prop, &cont, &obj_prop, world_rank) < 0) {
        println("fail to prepare container @ line %d", __LINE__);
        goto done;
    }

    if (world_rank == 0) {
        println("Initialization Done!");
    }

    // No need to create any object for testing only the index.

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    stime = MPI_Wtime();
#endif

    // insert into index
    // insert 1000 string type index records
    // format of key and value: "str%03dstr".
    // exact: "str109str=str109str", result should be 1 number, 109.
    // prefix: "str09*=str09*", result should be 10 different numbers, from 090 to 099.
    // suffix: "*09str=*09str", result should be 10 different numbers, from 009 to 909.
    // infix: "*09*=*09*", result should be 20 different numbers, including 090, 091, 092, 093, 094, 095,
    // 096, 097, 098, 099 and 009, 109, 209, 309, 409, 509, 609, 709, 809, 909.

    // insert 1000 integer index records
    // format of the key: "intkey".
    // exact number query "intkey=109", result should be 1 number, 109.
    // range query "intkey=90|~|99", result should be 10 different numbers, from 090 to 099.

    // delete the index records
    // perform the same query, there should be no result.

    // we are performing 1000 insertion operations for string value and 1000 times for numerical values.
    perr_t ret_value = insert_index_records(world_rank, world_size);
    if (ret_value == FAIL) {
        printf("CLIENT %d failed to insert index records\n", world_rank);
    }
    assert(ret_value == SUCCEED);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
    stime      = MPI_Wtime();
#endif
    // print performance and timing information
    if (world_rank == 0) {
        printf("Index Insertion 2000 times, time: %.5f ms, TPS = %.5f\n", total_time * 1000.0,
               2000.0 / total_time);
    }

    // perform 1000 times query test, each test will be performed by a different client,
    // and depending on the rank of the client, each client is performing a different type of query.
    // So totally, there will be 1000 * N queries issued, where N is the number of clients.
    for (i = 0; i < 1000; i++) {
        int  step_failed        = search_through_index(world_rank, world_size, validate_query_result);
        char query_types[6][20] = {"EXACT STRING", "PREFIX", "SUFFIX", "INFIX", "EXACT NUMBER", "RANGE"};
        if (step_failed >= 0) {
            printf("CLIENT %d failed to search index for query type %s\n", world_rank,
                   query_types[step_failed]);
        }
        assert(step_failed < 0);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
    stime      = MPI_Wtime();
#endif
    // print performance and timing information
    if (world_rank == 0) {
        printf("Index Query %d times, time: %.5f ms, TPS = %.5f\n", 1000 * world_size, total_time * 1000.0,
               1000.0 * world_size / total_time);
    }

    // delete the index records.
    ret_value = delete_index_records(world_rank, world_size);
    if (ret_value == FAIL) {
        printf("CLIENT %d failed to delete index records\n", world_rank);
    }
    assert(ret_value == SUCCEED);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
    stime      = MPI_Wtime();
#endif
    // print performance and timing information
    if (world_rank == 0) {
        printf("Index Deletion 2000 times, time: %.5f ms, TPS = %.5f\n", total_time * 1000.0,
               2000.0 / total_time);
    }

    for (i = 0; i < 1000; i++) {
        int  step_failed        = search_through_index(world_rank, world_size, validate_empty_result);
        char query_types[6][20] = {"EXACT STRING", "PREFIX", "SUFFIX", "INFIX", "EXACT NUMBER", "RANGE"};
        if (step_failed >= 0) {
            printf("CLIENT %d failed to search index for query type %s\n", world_rank,
                   query_types[step_failed]);
        }
        assert(step_failed < 0);
    }

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    total_time = MPI_Wtime() - stime;
#endif
    // print performance and timing information
    if (world_rank == 0) {
        printf("EMPTY Query %d times, time: %.5f ms, TPS = %.5f\n", 1000 * world_size, total_time * 1000.0,
               1000.0 * world_size / total_time);
    }

done:
    // close a container
    if (PDCcont_close(cont) < 0) {
        if (world_rank == 0) {
            printf("fail to close container c1\n");
        }
    }
    else {
        if (world_rank == 0)
            printf("successfully close container c1\n");
    }

    // close an object property
    if (PDCprop_close(obj_prop) < 0) {
        if (world_rank == 0)
            printf("Fail to close property @ line %d\n", __LINE__);
    }
    else {
        if (world_rank == 0)
            printf("successfully close object property\n");
    }

    // close a container property
    if (PDCprop_close(cont_prop) < 0) {
        if (world_rank == 0)
            printf("Fail to close property @ line %d\n", __LINE__);
    }
    else {
        if (world_rank == 0)
            printf("successfully close container property\n");
    }

    // close pdc
    if (PDCclose(pdc) < 0) {
        if (world_rank == 0)
            printf("fail to close PDC\n");
    }

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}
