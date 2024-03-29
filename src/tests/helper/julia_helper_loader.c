#include "julia_helper_loader.h"

void
jl_load_module(const char *mod_name)
{
    /* get julia helper directory */
    const char *julia_module_dir = getenv("PDC_JULIA_MODULE_DIR");
    if (julia_module_dir == NULL || strlen(julia_module_dir) == 0) {
        // try to get it from PWD
        printf("[PDC_JL_HELPER] Warning: PDC_JULIA_MODULE_DIR is not set, fallback to PWD!\n");
        julia_module_dir = getenv("PWD");
    }

    if (julia_module_dir == NULL || strlen(julia_module_dir) == 0) {
        // No way to find julia module directory
        printf("[PDC_JL_HELPER] Error: Not able to find Julia module directory!\n");
        exit(-1);
    }
    printf("[PDC_JL_HELPER] Julia module directory: %s\n", julia_module_dir);

    /* get julia helper path */
    char *julia_module_path = malloc(strlen(julia_module_dir) + strlen(mod_name) + 5);
    strcpy(julia_module_path, julia_module_dir);
    strcat(julia_module_path, "/");
    strcat(julia_module_path, mod_name);
    strcat(julia_module_path, ".jl");
    printf("[PDC_JL_HELPER] Julia module path: %s\n", julia_module_path);
    /* get include command */
    char include_cmd[strlen(julia_module_path) + 30];
    sprintf(include_cmd, "Base.include(Main, \"%s\")", julia_module_path);
    /* get using command */
    char using_cmd[strlen(mod_name) + 15];
    sprintf(using_cmd, "using Main.%s", mod_name);

    jl_eval_string(include_cmd);
    jl_eval_string(using_cmd);

    printf("[PDC_JL_HELPER] Loaded module %s\n", mod_name);
}

void
init_julia(jl_module_list_t *modules)
{
    /* required: setup the Julia context */
    jl_init();

    /* load JuliaHelper module */
    for (int i = 0; i < modules->num_modules; ++i) {
        jl_load_module(modules->julia_modules[i]);
    }
}

jl_value_t *
run_jl_function(const char *mod_name, const char *fun_name, jl_fn_args_t *args)
{
    char module_eval_cmd[strlen(mod_name) + 15];
    sprintf(module_eval_cmd, "Main.%s", mod_name);
    jl_module_t *  JuliaModule = (jl_module_t *)jl_eval_string(module_eval_cmd);
    jl_function_t *jl_func     = jl_get_function(JuliaModule, fun_name);
    jl_value_t *   result      = jl_call(jl_func, args->args, args->nargs);
    printf("[PDC_JL_HELPER] Function called: %s.%s\n", mod_name, fun_name);
    return result;
}

void
run_jl_get_int64_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, int64_t **arr_ptr,
                       size_t *arr_len)
{
    jl_value_t *ret = run_jl_function(mod_name, fun_name, args);
    JL_GC_PUSH1(&ret);
    jl_array_t *ret_array = (jl_array_t *)ret;
    int64_t *   data      = (int64_t *)jl_array_data(ret_array);
    *arr_ptr              = data;
    *arr_len              = jl_array_len(ret_array);
    JL_GC_POP();
}

void
run_jl_get_float64_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, double **arr_ptr,
                         size_t *arr_len)
{
    jl_value_t *ret = run_jl_function(mod_name, fun_name, args);
    JL_GC_PUSH1(&ret);
    jl_array_t *ret_array = (jl_array_t *)ret;
    double *    data      = (double *)jl_array_data(ret_array);
    *arr_ptr              = data;
    *arr_len              = jl_array_len(ret_array);
    JL_GC_POP();
}

void
run_jl_get_string_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, char ***arr_ptr,
                        size_t *arr_len)
{
    jl_value_t *ret = run_jl_function(mod_name, fun_name, args);
    JL_GC_PUSH1(&ret);
    jl_array_t *ret_array = (jl_array_t *)ret;
    size_t      length    = jl_array_len(ret);
    char **     strings   = malloc(length * sizeof(char *));

    for (size_t i = 0; i < length; ++i) {
        jl_value_t *julia_str    = jl_arrayref(ret_array, i);
        const char *c_str        = jl_string_ptr(julia_str);
        size_t      c_str_length = jl_string_len(julia_str);
        strings[i]               = malloc((c_str_length + 1) * sizeof(char));
        strncpy(strings[i], c_str, c_str_length);
        strings[i][c_str_length] = '\0';
    }
    *arr_ptr = strings;
    *arr_len = length;
    JL_GC_POP();
}

void
close_julia()
{
    jl_atexit_hook(0);
}