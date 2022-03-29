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
#include "pdc_dt_conv.h"
#include "pdc_private.h"
#include "pdc_error.h"
#include <limits.h>

/*
PDC_UNKNOWN      = -1,
PDC_INT          = 0,
PDC_FLOAT        = 1,
PDC_DOUBLE       = 2,
PDC_STRING       = 3,
PDC_COMPOUND     = 4,
PDC_ENUM         = 5,
PDC_ARRAY        = 6,
*/

/* Called if overflow is possible */
#define PDC_CONV_NOEX_CORE(S, D, ST, DT, D_MIN, D_MAX)                                                       \
    if (*(S) > (DT)(D_MAX))                                                                                  \
        *(D) = (D_MAX);                                                                                      \
    else                                                                                                     \
        *(D) = (DT)(*(S));

pdc_conv_t
pdc_find_conv_func(PDC_var_type_t src_id, PDC_var_type_t des_id, size_t nelemt, size_t stride)
{
    pdc_conv_t ret_value; /* Return value */

    FUNC_ENTER(NULL);

    if (src_id == PDC_FLOAT && des_id == PDC_INT)
        ret_value = pdc__conv_f_i;
    if (src_id == PDC_DOUBLE && des_id == PDC_INT)
        ret_value = pdc__conv_db_i;
    else
        PGOTO_ERROR(NULL, "no matching type convert function");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
pdc_type_conv(PDC_var_type_t src_id, PDC_var_type_t des_id, void *src_data, void *des_data, size_t nelemt,
              size_t stride)
{
    perr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER(NULL);

    pdc_conv_t func = pdc_find_conv_func(src_id, des_id, nelemt, stride);
    (*func)(src_data, des_data, nelemt, stride);

    FUNC_LEAVE(ret_value);
}

perr_t
pdc__conv_f_i(float *src_data, int *des_data, size_t nelemt, size_t stride)
{
    perr_t ret_value = SUCCEED; /* Return value */
    size_t i;

    FUNC_ENTER(NULL);

    for (i = 0; i < nelemt; i++) {
        PDC_CONV_NOEX_CORE(src_data, des_data, float, int, INT_MIN, INT_MAX);
        src_data += stride;
        des_data++;
    }

    FUNC_LEAVE(ret_value);
}

perr_t
pdc__conv_db_i(double *src_data, int *des_data, size_t nelemt, size_t stride)
{
    perr_t ret_value = SUCCEED; /* Return value */
    int    i;

    FUNC_ENTER(NULL);

    for (i = 0; i < nelemt; i++) {
        PDC_CONV_NOEX_CORE(src_data, des_data, double, int, INT_MIN, INT_MAX);
        src_data += stride;
        des_data++;
    }

    FUNC_LEAVE(ret_value);
}
