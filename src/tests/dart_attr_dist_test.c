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

    jl_function_t *my_julia_func = jl_get_function(jl_main_module, "my_julia_func");
    jl_array_t *   y             = (jl_array_t *)jl_call1(my_julia_func, jl_box_int64(4));

    int64_t *data = (int64_t *)jl_array_data(y);
    // get array length
    size_t length = jl_array_len(y);
    for (size_t i = 0; i < length; ++i) {
        printf("%ld\n", data[i]);
    }

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