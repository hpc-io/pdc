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

typedef struct query_gen_input {
    pdc_kvtag_t *base_tag;
    int          key_query_type;
    int          value_query_type;
    int          range_lo;
    int          range_hi;
} query_gen_input_t;

typedef struct query_gen_output {
    char * key_query;
    size_t key_query_len;
    char * value_query;
    size_t value_query_len;
} query_gen_output_t;

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

void
gen_query_key_value(query_gen_input_t *input, query_gen_output_t *output)
{
    char * key_ptr       = NULL;
    size_t key_ptr_len   = 0;
    char * value_ptr     = NULL;
    size_t value_ptr_len = 0;
    size_t affix_len     = 4;

    if (input->key_query_type == 0) { // exact
        key_ptr_len = strlen(input->base_tag->name);
        key_ptr     = (char *)calloc(key_ptr_len + 1, sizeof(char));
        strcpy(key_ptr, input->base_tag->name);
    }
    else if (input->key_query_type == 1) { // prefix
        key_ptr_len = affix_len + 1;
        key_ptr     = (char *)calloc(key_ptr_len + 1, sizeof(char));
        strncpy(key_ptr, input->base_tag->name, affix_len);
        key_ptr[affix_len - 1] = '*';
    }
    else if (input->key_query_type == 2) { // suffix
        key_ptr_len = affix_len + 1;
        key_ptr     = (char *)calloc(key_ptr_len + 1, sizeof(char));
        key_ptr[0]  = '*';
        key_ptr     = key_ptr + 1;
        strncpy(key_ptr, input->base_tag->name, affix_len);
    }
    else if (input->key_query_type == 3) { // infix
        key_ptr_len            = affix_len + 2;
        key_ptr                = (char *)calloc(key_ptr_len + 1, sizeof(char));
        key_ptr[0]             = '*';
        key_ptr[affix_len - 1] = '*';
        key_ptr                = key_ptr + 1;
        strncpy(key_ptr, input->base_tag->name, affix_len);
    }
    else {
        printf("Invalid key query type!\n");
        return;
    }

    if (input->base_tag->type == PDC_STRING) {
        if (input->value_query_type == 0) {
            value_ptr_len = strlen((char *)input->base_tag->value);
            value_ptr     = (char *)calloc(value_ptr_len + 1, sizeof(char));
            strcpy(value_ptr, (char *)input->base_tag->value);
        }
        else if (input->value_query_type == 1) {
            value_ptr_len = affix_len + 1;
            value_ptr     = (char *)calloc(value_ptr_len + 1, sizeof(char));
            strncpy(value_ptr, (char *)input->base_tag->value, affix_len);
            value_ptr[affix_len - 1] = '*';
        }
        else if (input->value_query_type == 2) {
            value_ptr_len = affix_len + 1;
            value_ptr     = (char *)calloc(value_ptr_len + 1, sizeof(char));
            value_ptr[0]  = '*';
            value_ptr     = value_ptr + 1;
            strncpy(value_ptr, (char *)input->base_tag->value, affix_len);
        }
        else if (input->value_query_type == 3) {
            value_ptr_len            = affix_len + 2;
            value_ptr                = (char *)calloc(value_ptr_len + 1, sizeof(char));
            value_ptr[0]             = '*';
            value_ptr[affix_len - 1] = '*';
            value_ptr                = value_ptr + 1;
            strncpy(value_ptr, (char *)input->base_tag->value, affix_len);
        }
        else {
            printf("Invalid value query type for string tag!\n");
            return;
        }
    }
    else if (input->base_tag->type == PDC_INT) {
        if (input->value_query_type == 4) {
            value_ptr_len = snprintf(NULL, 0, "%d", ((int *)input->base_tag->value)[0]);
            value_ptr     = (char *)calloc(value_ptr_len + 1, sizeof(char));
            snprintf(value_ptr, value_ptr_len + 1, "%d", ((int *)input->base_tag->value)[0]);
        }
        else if (input->value_query_type == 5) {
            size_t lo_len = snprintf(NULL, 0, "%d", input->range_lo);
            size_t hi_len = snprintf(NULL, 0, "%d", input->range_hi);
            value_ptr_len = lo_len + hi_len + 1;
            value_ptr     = (char *)calloc(value_ptr_len + 1, sizeof(char));
            snprintf(value_ptr, value_ptr_len + 1, "%d~%d", input->range_lo, input->range_hi);
        }
        else {
            printf("Invalid value query type for integer!\n");
            return;
        }
    }
    else {
        printf("Invalid tag type!\n");
        return;
    }

    output->key_query       = key_ptr;
    output->key_query_len   = key_ptr_len;
    output->value_query     = value_ptr;
    output->value_query_len = value_ptr_len;
}

char **
gen_random_strings(int count, int maxlen, int alphabet_size)
{
    int    c      = 0;
    int    i      = 0;
    char **result = (char **)calloc(count, sizeof(char *));
    for (c = 0; c < count; c++) {
        int len   = rand() % maxlen;
        len       = len < 6 ? 6 : len; // Ensure at least 6 character
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
    int         proc_num, my_rank, i, v, iter, round, selectivity, is_using_dart, query_type, comm_type;
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

    if (argc < 7) {
        if (my_rank == 0)
            print_usage(argv[0]);
        goto done;
    }
    n_obj         = atoi(argv[1]);
    round         = atoi(argv[2]);
    selectivity   = atoi(argv[3]);
    is_using_dart = atoi(argv[4]);
    query_type    = atoi(argv[5]); // 0 for exact, 1 for prefix, 2 for suffix, 3 for infix,
                                   // 4 for num_exact, 5 for num_range
    comm_type = atoi(argv[6]);     // 0 for point-to-point, 1 for collective
    n_add_tag = n_obj * selectivity / 100;

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
    char query_string[48];

    dart_object_ref_type_t ref_type  = REF_PRIMARY_ID;
    dart_hash_algo_t       hash_algo = DART_HASH;

    assign_work_to_rank(my_rank, proc_num, n_add_tag, &my_add_tag, &my_add_tag_s);

    // This is for adding #rounds tags to the objects.
    for (i = 0; i < my_add_tag; i++) {
        for (iter = 0; iter < round; iter++) {
            v = iter;
            sprintf(value, "%d", v);
            if (is_using_dart) {
                if (PDC_Client_insert_obj_ref_into_dart(hash_algo, kvtag.name, value, ref_type,
                                                        (uint64_t)obj_ids[i]) < 0) {
                    printf("fail to add a kvtag to o%d\n", i + my_obj_s);
                }
            }
            else {
                if (PDCobj_put_tag(obj_ids[i], kvtag.name, kvtag.value, kvtag.type, kvtag.size) < 0) {
                    printf("fail to add a kvtag to o%d\n", i + my_obj_s);
                }
            }
        }
        if (my_rank == 0)
            println("Rank %d: Added %d kvtag to the %d th object\n", my_rank, round, i);
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
            sprintf(value, "%ld", v);
            sprintf(query_string, "%s=%s", kvtag.name, value);
            PDC_Client_search_obj_ref_through_dart_mpi(hash_algo, query_string, ref_type, &nres, &pdc_ids,
                                                       MPI_COMM_WORLD);
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
