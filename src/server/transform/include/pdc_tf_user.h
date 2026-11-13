/**
 * NOTICE: This file is included in a custom transformation libaries.
 * It should not include any headers that are not available to the custom transformation library.
 * It should also not include any headers that define symbols that
 * may conflict with symbols in the custom transformation library.
 */
#ifndef PDC_TF_USER_H
#define PDC_TF_USER_H

#define NUM_TF_FUNC_TIMES 5

#include "pdc_vector.h"
#include "pdc_dg.h"

// Specifies what device the function can run on
typedef enum pdc_tf_dev_t { PDC_TF_CPU_DEVICE, PDC_TF_GPU_DEVICE, PDC_TF_NUM_DEVICES } pdc_tf_dev_t;

typedef struct pdc_tf_state_t {
    char *      name;
    PDC_VECTOR *pdc_tf_dg_params_vector;
} pdc_tf_state_t;

// Specifies whether a function is internal or external.
typedef enum pdc_tf_location_t { PDC_TF_BUILTIN, PDC_TF_EXTERNAL, PDC_TF_NUM_LOCATIONS } pdc_tf_location_t;
extern char *pdc_tf_location_strs[];

#define DIM_MAX 4

typedef struct pdc_tf_region_t {
    size_t   ndim;
    uint8_t  pdc_var_type;
    uint64_t size[DIM_MAX];
} pdc_tf_region_t;

typedef struct pdc_tf_internal_param {
    pdc_dg_t *dg;
    uint64_t  flat_conceptual_offset;
    double    host_to_dev_time;
    double    dev_to_host_time;
} pdc_tf_internal_param;

/**
 * Prototype for region transformation functions
 *
 * Before the function is invoked, `output_state.tf_region` is set to `input_state.tf_region`, so if the
 * transformation does not change the region size, the user does not need to
 * modify `output_state.tf_region`.
 *
 * `region_data` is a double pointer to the input region's data buffer.
 * The function may either mutate the existing buffer in place or allocate a new
 * buffer and update `*region_data` to point to it.
 *
 * If a new data buffer is assigned to `*region_data`, it must be heap-allocated
 * so that PDC can free it. The original pointer should NOT be freed.
 */
typedef bool (*c_func_t)(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                         pdc_tf_region_t input_region, pdc_tf_region_t *output_region);

typedef struct pdc_tf_builtin_func_t {
    char *            name;
    pdc_tf_dev_t      dev;
    pdc_tf_location_t location;
    c_func_t          c_func;
    char *            params_str;
    PDC_VECTOR *      pdc_tf_dg_params_vector;
    uint32_t          cur_host_to_dev_avg_time_index;
    uint32_t          cur_dev_to_host_avg_time_index;
    uint32_t          cur_exec_avg_time_index;
    double            host_to_dev_avg_time[NUM_TF_FUNC_TIMES];
    double            dev_to_host_avg_time[NUM_TF_FUNC_TIMES];
    double            exec_avg_time[NUM_TF_FUNC_TIMES];
} pdc_tf_builtin_func_t;

typedef pdc_tf_builtin_func_t pdc_tf_func_t;
/**
 * Used to store parameters for states and edges
 * within the directed graph.
 *
 * The conceptual ID (which is the flat offset)
 * is used to identify the parameters
 * for a specific region.
 */
typedef struct pdc_tf_dg_params_t {
    uint64_t flat_conceptual_offset;
    void *   params;
    uint64_t params_size;
} pdc_tf_dg_params_t;

/**
 * Note: true is success and false is failure for the functions in this file.
 * This is because the custom transformation library.
 */
bool PDCtf_set_state_param(pdc_dg_t *dg, char *state_name, uint64_t flat_conceptual_offset, void *params,
                           uint64_t params_size);
bool PDCtf_get_state_param(pdc_dg_t *dg, char *state_name, uint64_t flat_conceptual_offset, void **params,
                           uint64_t *params_size);
bool PDCtf_set_func_param(pdc_dg_t *dg, char *func_name, pdc_tf_dev_t dev, uint64_t flat_conceptual_offset,
                          void *params, uint64_t params_size);
bool PDCtf_get_func_param(pdc_dg_t *dg, char *func_name, pdc_tf_dev_t dev, uint64_t flat_conceptual_offset,
                          void **params, uint64_t *params_size);
/**
 * @brief Set state parameters.
 *
 * This macro wraps the call to PDCtf_set_state_param to set the value
 * of a named state parameter.
 *
 * @param name          [in]  The name of the state parameter to set.
 * @param params        [in]  Pointer to the data to set for the parameter.
 * @param params_size   [in]  Size in bytes of the data pointed to by params.
 */
#define SET_STATE_PARAMS(name, params, params_size)                                                          \
    do {                                                                                                     \
        PDCtf_set_state_param(internal_param.dg, name, internal_param.flat_conceptual_offset, params,        \
                              params_size);                                                                  \
    } while (0)

/**
 * @brief Get state parameters.
 *
 * @param name          The name of the state parameter to retrieve.
 * @param params        [in,out] Pointer to a buffer pointer that will be set by the function.
 * @param params_size   [in,out] Pointer to a size_t that will be set to the size of the returned buffer.
 */
#define GET_STATE_PARAMS(name, params, params_size)                                                          \
    do {                                                                                                     \
        PDCtf_get_state_param(internal_param.dg, name, internal_param.flat_conceptual_offset, params,        \
                              params_size);                                                                  \
    } while (0)

/**
 * @brief Set func parameters.
 *
 * This macro wraps the call to PDCtf_set_func_param to set the value
 * of a named func parameter.
 *
 * @param name          [in]  The name of the func parameter to set.
 * @param params        [in]  Pointer to the data to set for the parameter.
 * @param params_size   [in]  Size in bytes of the data pointed to by params.
 */
#define SET_FUNC_PARAMS(name, dev, params, params_size)                                                      \
    do {                                                                                                     \
        PDCtf_set_func_param(internal_param->dg, name, dev, internal_param->flat_conceptual_offset, params,  \
                             params_size);                                                                   \
    } while (0)

/**
 * @brief Get func parameters.
 *
 * @param name          The name of the func parameter to retrieve.
 * @param params        [in,out] Pointer to a buffer pointer that will be set by the function.
 * @param params_size   [in,out] Pointer to a size_t that will be set to the size of the returned buffer.
 */
#define GET_FUNC_PARAMS(name, dev, params, params_size)                                                      \
    do {                                                                                                     \
        PDCtf_get_func_param(internal_param->dg, name, dev, internal_param->flat_conceptual_offset, params,  \
                             params_size);                                                                   \
    } while (0)

/**
 * @brief Start timing a host-to-device transfer.
 *
 * This macro records the start timestamp for a host-to-device transfer
 * into a stack-allocated timespec. Pair with END_HOST_TO_DEV_TIME to
 * accumulate the elapsed duration into internal_param.host_to_dev_time.
 */
#define START_HOST_TO_DEV_TIME()                                                                             \
    struct timespec __host_to_dev_start_time__;                                                              \
    do {                                                                                                     \
        clock_gettime(CLOCK_MONOTONIC, &__host_to_dev_start_time__);                                         \
    } while (0)

/**
 * @brief End timing a host-to-device transfer.
 *
 * This macro records the end timestamp and accumulates the elapsed time
 * since the matching START_HOST_TO_DEV_TIME() call into
 * internal_param.host_to_dev_time.
 *
 * @note May be called multiple times if there are multiple transfer
 *       segments; each call adds to the running total.
 */
#define END_HOST_TO_DEV_TIME()                                                                               \
    do {                                                                                                     \
        struct timespec __host_to_dev_end_time__;                                                            \
        clock_gettime(CLOCK_MONOTONIC, &__host_to_dev_end_time__);                                           \
        internal_param->host_to_dev_time +=                                                                  \
            (__host_to_dev_end_time__.tv_sec - __host_to_dev_start_time__.tv_sec) +                          \
            (__host_to_dev_end_time__.tv_nsec - __host_to_dev_start_time__.tv_nsec) / 1e9;                   \
    } while (0)

/**
 * @brief Start timing a device-to-host transfer.
 *
 * This macro records the start timestamp for a device-to-host transfer
 * into a stack-allocated timespec. Pair with END_DEV_TO_HOST_TIME to
 * accumulate the elapsed duration into internal_param.dev_to_host_time.
 */
#define START_DEV_TO_HOST_TIME()                                                                             \
    struct timespec __dev_to_host_start_time__;                                                              \
    do {                                                                                                     \
        clock_gettime(CLOCK_MONOTONIC, &__dev_to_host_start_time__);                                         \
    } while (0)

/**
 * @brief End timing a device-to-host transfer.
 *
 * This macro records the end timestamp and accumulates the elapsed time
 * since the matching START_DEV_TO_HOST_TIME() call into
 * internal_param.dev_to_host_time.
 *
 * @note May be called multiple times if there are multiple transfer
 *       segments; each call adds to the running total.
 */
#define END_DEV_TO_HOST_TIME()                                                                               \
    do {                                                                                                     \
        struct timespec __dev_to_host_end_time__;                                                            \
        clock_gettime(CLOCK_MONOTONIC, &__dev_to_host_end_time__);                                           \
        internal_param->dev_to_host_time +=                                                                  \
            (__dev_to_host_end_time__.tv_sec - __dev_to_host_start_time__.tv_sec) +                          \
            (__dev_to_host_end_time__.tv_nsec - __dev_to_host_start_time__.tv_nsec) / 1e9;                   \
    } while (0)

#endif