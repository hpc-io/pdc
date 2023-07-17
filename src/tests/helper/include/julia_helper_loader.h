#ifndef JULIA_HELPER_LOADER_H
#define JULIA_HELPER_LOADER_H

/**
 * This is a helper library to load Julia modules and run Julia functions from C.
 * It is now used in the PDC C tests. We may consider moving it to PDC core in the future.
 * If you need to add any new functions, please add them to julia_helper.jl and then add the corresponding
 * C function here as the Julia function caller.
 * You may refer to the following link for more information:
 * https://docs.julialang.org/en/v1/manual/embedding/index.html
 *
 * For thread safety, please make sure you call init_julia(), julia function caller and close_julia() in the
 * same C thread.
 *
 * If you need to call share variables between multiple threads, the best practice is to start your threads in
 * Julia and then call the Julia functions from Julia threads. Or you can also call the C functions from Julia
 * threads using ccall(). For calling C from Julia, please refer to the following link:
 * https://docs.julialang.org/en/v1/manual/calling-c-and-fortran-code/index.html
 *
 * An example of calling the Julia C API from a thread started by Julia itself:
 *
 * #include <julia/julia.h>
 * JULIA_DEFINE_FAST_TLS
 *
 * double c_func(int i)
 * {
 *     printf("[C %08x] i = %d\n", pthread_self(), i);
 *
 *     // Call the Julia sqrt() function to compute the square root of i, and return it
 *     jl_function_t *sqrt = jl_get_function(jl_base_module, "sqrt");
 *     jl_value_t* arg = jl_box_int32(i);
 *     double ret = jl_unbox_float64(jl_call1(sqrt, arg));
 *
 *     return ret;
 * }
 *
 * int main()
 * {
 *     jl_init();
 *
 *     // Define a Julia function func() that calls our c_func() defined in C above
 *     jl_eval_string("func(i) = ccall(:c_func, Float64, (Int32,), i)");
 *
 *     // Call func() multiple times, using multiple threads to do so
 *     jl_eval_string("println(Threads.threadpoolsize())");
 *     jl_eval_string("use(i) = println(\"[J $(Threads.threadid())] i = $(i) -> $(func(i))\")");
 *     jl_eval_string("Threads.@threads for i in 1:5 use(i) end");
 *
 *     jl_atexit_hook(0);
 * }
 *
 * @file julia_helper_loader.h
 * @author Wei Zhang
 */

#include <julia.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char **julia_modules;
    int    num_modules;
} jl_module_list_t;
typedef struct {
    jl_value_t **args;
    int32_t      nargs;
} jl_fn_args_t;

/**
 * @brief initialize the julia runtime with a list of julia modules
 *
 * @param modules the list of modules to load
 */
void init_julia(jl_module_list_t *modules);

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
void run_jl_get_int64_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, int64_t **arr_ptr,
                            size_t *arr_len);

/**
 * @brief run a julia function with the specified arguments and get a float64_t array and its length
 *
 * @param mod_name the name of the module to load
 * @param fun_name the name of the function to run
 * @param args the arguments to pass to the function
 * @param arr_ptr the pointer to the float64_t array to be returned
 * @param arr_len the length of the float64_t array to be returned
 */
void run_jl_get_float64_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args,
                              double **arr_ptr, size_t *arr_len);

/**
 * @brief run a julia function with the specified arguments and get a string array and its length
 *
 * @param mod_name the name of the module to load
 * @param fun_name the name of the function to run
 * @param args the arguments to pass to the function
 * @param arr_ptr the pointer to the string array to be returned
 * @param arr_len the length of the string array to be returned
 */
void run_jl_get_string_array(const char *mod_name, const char *fun_name, jl_fn_args_t *args, char ***arr_ptr,
                             size_t *arr_len);

/**
 * @brief exit the julia runtime
 */
void close_julia();

#endif // JULIA_HELPER_LOADER_H