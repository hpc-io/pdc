#include <julia.h>

JULIA_DEFINE_FAST_TLS()

int
main(int argc, char *argv[])
{
    /* required: setup the Julia context */
    jl_init();

    /* run Julia commands */
    jl_eval_string("include(\"julia_helper.jl\")");
    jl_value_t *result = jl_eval_string("generate_attribute_occurrences(10, 1000, \"exponential\", 1.0)");

    JL_GC_PUSH1(&result);
    jl_array_t *result_array = (jl_array_t *)result;
    int64_t *   data         = (int64_t *)jl_array_data(result_array);
    size_t      length       = jl_array_len(result_array);

    for (size_t i = 0; i < length; ++i) {
        printf("%ld\n", data[i]);
    }
    JL_GC_POP();

    /* shutdown the Julia context */
    jl_atexit_hook(0);

    return 0;
}