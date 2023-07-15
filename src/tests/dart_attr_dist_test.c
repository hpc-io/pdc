#include <julia.h>

JULIA_DEFINE_FAST_TLS

int
main(int argc, char *argv[])
{

    /* get julia helper directory */
    const char *julia_helper_dir = getenv("PDC_JULIA_HELPER_DIR");
    /* get julia helper path */
    char *julia_helper_path = malloc(strlen(julia_helper_dir) + strlen("julia_helper.jl") + 2);
    strcpy(julia_helper_path, julia_helper_dir);
    strcat(julia_helper_path, "/");
    strcat(julia_helper_path, "julia_helper.jl");
    printf("Julia helper path: %s\n", julia_helper_path);
    /* get include command */
    char include_cmd[strlen(julia_helper_path) + 20];
    sprintf(include_cmd, "include(\"%s\")", julia_helper_path);

    /* required: setup the Julia context */
    jl_init();

    /* run Julia commands */
    jl_eval_string(include_cmd);

    int64_t *(*incr_jl)(int64_t) =
        jl_unbox_voidpointer(jl_eval_string("@cfunction(my_julia_func, Vector{Int64}, (Int64,))"));

    int64_t *result = incr_jl(4);
    printf("Result: %ld\n", result[0]);

    // Call Julia function
    // jl_value_t *ret = jl_call0(func);

    // jl_value_t *result = jl_eval_string("generate_attribute_occurrences(10, 1000, \"exponential\", 1.0)");

    // JL_GC_PUSH1(&result);
    // jl_array_t *result_array = (jl_array_t *)result;
    // int64_t *   data         = (int64_t *)jl_array_data(result_array);
    // size_t      length       = jl_array_len(result_array);

    // for (size_t i = 0; i < length; ++i) {
    //     printf("%ld\n", data[i]);
    // }
    // JL_GC_POP();

    /* shutdown the Julia context */
    jl_atexit_hook(0);

    return 0;
}