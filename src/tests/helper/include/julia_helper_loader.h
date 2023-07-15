#ifndef JULIA_HELPER_LOADER_H
#define JULIA_HELPER_LOADER_H

#include <julia.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    jl_value_t **args;
    int32_t nargs;
} jl_fn_args_t;

/**
 * @brief load a julia module into the current session. 
 *        You must have a module named mod_name.jl in the specified julia module directory.
 *        The module directory is specified in by environment variable PDC_JULIA_MODULE_DIR, or PWD.
 *        You may load all the modules you have first and then call the functions in them when needed.
 * @param mod_name the name of the module to load
 * 
 */
void jl_load_module(const char *mod_name);

/**
 * @brief run a julia function with the specified arguments
 * 
 * @param mod_name the name of the module to load
 * @param fun_name the name of the function to run
 * @param args the arguments to pass to the function
 * @return jl_value_t* the return value of the function
 */
jl_value_t *run_jl_function(const char *mod_name, const char *fun_name, jl_fn_args_t *args);

/**
 * @brief run a julia function with the specified arguments and get an int64_t array and its length
 * 
 * @param mod_name the name of the module to load
 * @param fun_name the name of the function to run
 * @param args the arguments to pass to the function
 * @param arr_ptr the pointer to the int64_t array to be returned
 * @param arr_len the length of the int64_t array to be returned
 */
void run_jl_get_int64_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, int64_t **arr_ptr, size_t *arr_len);

/**
 * @brief run a julia function with the specified arguments and get a float64_t array and its length
 * 
 * @param mod_name the name of the module to load
 * @param fun_name the name of the function to run
 * @param args the arguments to pass to the function
 * @param arr_ptr the pointer to the float64_t array to be returned
 * @param arr_len the length of the float64_t array to be returned
*/
void run_jl_get_float64_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, double **arr_ptr, size_t *arr_len);


/**
 * @brief run a julia function with the specified arguments and get a string array and its length
 * 
 * @param mod_name the name of the module to load
 * @param fun_name the name of the function to run
 * @param args the arguments to pass to the function
 * @param arr_ptr the pointer to the string array to be returned
 * @param arr_len the length of the string array to be returned
*/
void run_jl_get_string_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, char ***arr_ptr, size_t *arr_len);

#endif  // JULIA_HELPER_LOADER_H