#include "julia_helper_loader.h"

// only define the following once, in an executable (not in a shared library) if you want fast
// code.
JULIA_DEFINE_FAST_TLS

int
main(int argc, char *argv[])
{
    jl_init();

    char *jl_module_name = "JuliaHelper";
    /* load JuliaHelper module */
    jl_load_module(jl_module_name);

    /* run generate_attribute_occurrences with parameters */
    jl_fn_args_t *args = (jl_fn_args_t *)calloc(1, sizeof(jl_fn_args_t));
    args->nargs        = 3;
    args->args         = (jl_value_t **)calloc(args->nargs, sizeof(jl_value_t *));
    args->args[0]      = jl_box_int64(10);
    args->args[1]      = jl_box_int64(1000);
    args->args[2]      = jl_cstr_to_string("exponential");

    int64_t *arr = NULL;
    size_t   len = 0;
    run_jl_get_int64_array(jl_module_name, "generate_attribute_occurrences", args, &arr, &len);

    // get array length
    for (size_t i = 0; i < len; ++i) {
        printf("%ld\n", arr[i]);
    }

    jl_atexit_hook(0);

    return 0;
}