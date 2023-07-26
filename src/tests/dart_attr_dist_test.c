#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include "string_utils.h"
#include "timer_utils.h"
#include "dart_core.h"

// #define ENABLE_MPI 1

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc.h"
#include "pdc_client_connect.h"
#include "julia_helper_loader.h"

#define JULIA_HELPER_NAME "JuliaHelper"
// only define the following once, in an executable (not in a shared library) if you want fast
// code.
JULIA_DEFINE_FAST_TLS

void
generate_incremental_associations(int64_t num_attr, int64_t num_obj, int64_t num_groups, int64_t **arr,
                                  size_t *len)
{
    /* run generate_incremental_associations  with parameters */
    jl_fn_args_t *args = (jl_fn_args_t *)calloc(1, sizeof(jl_fn_args_t));
    args->nargs        = 3;
    args->args         = (jl_value_t **)calloc(args->nargs, sizeof(jl_value_t *));
    args->args[0]      = jl_box_int64(num_attr);
    args->args[1]      = jl_box_int64(num_obj);
    args->args[2]      = jl_box_int64(num_groups);

    run_jl_get_int64_array(JULIA_HELPER_NAME, "generate_incremental_associations", args, arr, len);
}

void
generate_attribute_occurrences(int64_t num_attr, int64_t num_obj, const char *dist, int64_t **arr,
                               size_t *len)
{
    /* run generate_incremental_associations  with parameters */
    jl_fn_args_t *args = (jl_fn_args_t *)calloc(1, sizeof(jl_fn_args_t));
    args->nargs        = 3;
    args->args         = (jl_value_t **)calloc(args->nargs, sizeof(jl_value_t *));
    args->args[0]      = jl_box_int64(num_attr);
    args->args[1]      = jl_box_int64(num_obj);
    args->args[2]      = jl_cstr_to_string(dist);

    /* run generate_attribute_occurrences  with parameters */
    run_jl_get_int64_array(JULIA_HELPER_NAME, "generate_attribute_occurrences", args, arr, len);
}

void
generate_attr_obj_association(int64_t num_attr, int64_t num_obj, int64_t **arr, size_t *len)
{
    *arr = (int64_t *)calloc(num_attr, sizeof(int64_t));
    *len = num_attr;
    for (int64_t i = 0; i < num_attr; i++) {
        (*arr)[i] = (num_obj / num_attr) * (i + 1);
    }
}

int
main(int argc, char *argv[])
{
    int rank = 0, size = 1;

#ifdef ENABLE_MPI
    fflush(stdout);
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    int64_t *attr_2_obj_array = NULL;
    size_t   arr_len          = 0;
    size_t   total_num_obj    = 1000000000;
    size_t   total_num_attr   = 10;

    char key[32];
    char value[32];
    char exact_query[48];
    char prefix_query[48];
    char suffix_query[48];
    char infix_query[48];

    // jl_module_list_t modules = {.julia_modules = (char *[]){JULIA_HELPER_NAME}, .num_modules = 1};
    // init_julia(&modules);

    if (rank == 0) {
        // calling julia helper to get the array.

        generate_attr_obj_association(total_num_attr, total_num_obj, &attr_2_obj_array, &arr_len);

        // generate_incremental_associations(total_num_attr, total_num_obj, total_num_attr, &attr_2_obj_array,
        //                                   &arr_len);

        // generate_attribute_occurrences(total_num_attr, total_num_obj, "uniform", &attr_2_obj_array,
        // &arr_len);

        // broadcast the size from rank 0 to all other processes
        MPI_Bcast(&arr_len, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
    }
    else {
        // receive the size on all other ranks
        MPI_Bcast(&arr_len, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);

        // allocate memory for the array
        attr_2_obj_array = (int64_t *)malloc(arr_len * sizeof(int64_t));
    }
    // broadcast the array itself
    MPI_Bcast(attr_2_obj_array, arr_len, MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD);

    // print array.
    for (size_t i = 0; i < arr_len; ++i) {
        printf("rank %d: %ld\n", rank, attr_2_obj_array[i]);
    }

    pdcid_t pdc = PDCinit("pdc");

    pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    if (cont_prop <= 0)
        printf("Fail to create container property @ line  %d!\n", __LINE__);

    pdcid_t cont = PDCcont_create("c1", cont_prop);
    if (cont <= 0)
        printf("Fail to create container @ line  %d!\n", __LINE__);

    pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    if (obj_prop <= 0)
        printf("Fail to create object property @ line  %d!\n", __LINE__);

    dart_object_ref_type_t ref_type  = REF_PRIMARY_ID;
    dart_hash_algo_t       hash_algo = DART_HASH;
    int                    i, j, pct = 0;

    stopwatch_t timer;
    timer_start(&timer);
    for (i = 0; i < arr_len; i++) {
        sprintf(key, "k%ld", i);
        sprintf(value, "v%ld", i);
        for (j = 0; j < attr_2_obj_array[i]; j++) {
            if (j % size == rank) {
                PDC_Client_insert_obj_ref_into_dart(hash_algo, key, value, ref_type, j);
            }
            size_t num_object_per_attr = attr_2_obj_array[i] / 100;
            if (rank == 0 && j % num_object_per_attr == 0) {
                pct += 1;
                timer_pause(&timer);
                printf("[Client_Side_Insert] %d\%: Insert '%s=%s' for ref %llu within  %.4f ms\n", pct, key,
                       value, j, (double)timer_delta_ms(&timer));
                timer_start(&timer);
            }
        }
    }
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    for (i = 0; i < arr_len; i++) {
        if (i % size == rank) {
            timer_start(&timer);
            sprintf(key, "k%ld", i);
            sprintf(value, "v%ld", i);
            sprintf(exact_query, "%s=%s", key, value);
            uint64_t *out1;
            int       rest_count1 = 0;
            PDC_Client_search_obj_ref_through_dart(hash_algo, exact_query, ref_type, &rest_count1, &out1);

            timer_pause(&timer);

            println("[Client_Side_Exact] Search '%s' and get %d results : %llu within %.4f ms\n", key,
                    rest_count1, out1[rest_count1 - 1], (double)timer_delta_ms(&timer));
        }
    }

    if (PDCcont_close(cont) < 0)
        printf("fail to close container %lld\n", cont);

    if (PDCprop_close(cont_prop) < 0)
        printf("Fail to close property @ line %d\n", __LINE__);

    if (PDCclose(pdc) < 0)
        printf("fail to close PDC\n");

    // free attr_2_obj_array
    free(attr_2_obj_array);

    // close_julia();

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif

    return 0;
}