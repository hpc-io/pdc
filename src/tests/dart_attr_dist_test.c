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
    printf("called generate_incremental_associations\n");
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
    run_jl_get_int64_array(JULIA_HELPER_NAME, "generate_attribute_occurrences", args, &arr, &len);
    printf("called generate_attribute_occurrences\n");
}

int
main(int argc, char *argv[])
{
    jl_module_list_t modules = {.julia_modules = (char *[]){JULIA_HELPER_NAME}, .num_modules = 1};
    init_julia(&modules);

    int64_t *arr = NULL;
    size_t   len = 0;

    generate_incremental_associations(10, 1000, 0, &arr, &len);
    // print array.
    for (size_t i = 0; i < len; ++i) {
        printf("%ld\n", arr[i]);
    }

    generate_attribute_occurrences(10, 100, "uniform", &arr, &len);
    // print array.
    for (size_t i = 0; i < len; ++i) {
        printf("%ld\n", arr[i]);
    }

    close_julia();
    return 0;
}