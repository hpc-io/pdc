#include <stdlib.h>
#include <assert.h>
#include "pdc_public.h"
#include "pdc_private.h"


typedef perr_t (*pdc_conv_t) (void *src_data, void *des_data, size_t nelemt, size_t stride);

/**
 * To find type conversion function
 *
 * \param src_id [IN]           Id of source variable type
 * \param des_id [IN]           Id of target variable type
 * \param nelemt [IN]           Number of elements to convert
 * \param stride [IN]           Stride between each element to convert
 *
 * \return convert function on success/NULL on failure
 */
pdc_conv_t pdc_find_conv_func(PDC_var_type_t src_id, PDC_var_type_t des_id, size_t nelemt, size_t stride);

/**
 * Type conversion function
 *
 * \param src_id [IN]           Id of source variable type
 * \param des_id [IN]           Id of target variable type
 * \param src_data [IN]         Pointer to source variable storage
 * \param des_data [IN]         Pointer to target variable storage
 * \param nelemt [IN]           Number of elements to convert
 * \param stride [IN]           Stride between each element to convert
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc_type_conv(PDC_var_type_t src_id, PDC_var_type_t des_id, void *src_data, void *des_data, size_t nelemt, size_t stride);

/**
 * Convert from float to int
 *
 * \param src_data [IN]         Pointer to source data
 * \param des_data [IN]         Pointer to target data
 * \param nelemt [IN]           Number of elements to convert
 * \param stride [IN]           Stride between each element to convert
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc__conv_f_i(float *src_data, int *des_data, size_t nelemt, size_t stride);

/**
 * Type conversion function from double to int
 *
 * \param src_data [IN]         A pointer to source data
 * \param des_data [IN]         A pointer to target data
 * \param nelemt [IN]           Number of elements to convert
 * \param stride [IN]           Stride between element
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc__conv_db_i(double *src_data, int *des_data, size_t nelemt, size_t stride);
