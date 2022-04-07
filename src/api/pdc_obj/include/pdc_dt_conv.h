/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include <stdlib.h>
#include <assert.h>
#include "pdc_public.h"
#include "pdc_private.h"

typedef perr_t (*pdc_conv_t)(void *src_data, void *des_data, size_t nelemt, size_t stride);

/**
 * To find type conversion function
 *
 * \param src_id [IN]           ID of source variable type
 * \param des_id [IN]           ID of target variable type
 * \param nelemt [IN]           Number of elements to convert
 * \param stride [IN]           Stride between each element to convert
 *
 * \return convert function on success/NULL on failure
 */
pdc_conv_t pdc_find_conv_func(PDC_var_type_t src_id, PDC_var_type_t des_id, size_t nelemt, size_t stride);

/**
 * Type conversion function
 *
 * \param src_id [IN]           ID of source variable type
 * \param des_id [IN]           ID of target variable type
 * \param src_data [IN]         Pointer to source variable storage
 * \param des_data [IN]         Pointer to target variable storage
 * \param nelemt [IN]           Number of elements to convert
 * \param stride [IN]           Stride between each element to convert
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc_type_conv(PDC_var_type_t src_id, PDC_var_type_t des_id, void *src_data, void *des_data,
                     size_t nelemt, size_t stride);

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
