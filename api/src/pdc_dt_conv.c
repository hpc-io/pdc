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
#define PDC_CONV_NOEX_CORE(S, D, ST, DT, D_MIN, D_MAX)                   \
if (*(S) > (DT)(D_MAX))                                                   \
*(D) = (D_MAX);                                                       \
else                                                                      \
*(D) = (DT)(*(S));

pdc_conv_t
pdc_find_conv_func(PDC_var_type_t src_id, PDC_var_type_t des_id) {
    pdc_conv_t ret_value;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    if(src_id == PDC_FLOAT && des_id == PDC_INT)
        ret_value = pdc__conv_f_i;
    else
        PGOTO_ERROR(FAIL, "no matching type convert function");
done:
    FUNC_LEAVE(ret_value);
}

perr_t
pdc_type_conv(PDC_var_type_t src_id, PDC_var_type_t des_id, void *src_data, void *des_data, size_t nelemt, size_t stride) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    pdc_conv_t func = pdc_find_conv_func(src_id, des_id);
    (*func)(src_data, des_data, nelemt, stride);
}

perr_t
pdc__conv_f_i(float *src_data, int *des_data, size_t nelemt, size_t stride) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);
    
    size_t i;
    uint64_t addr;
    for(i = 0; i<nelemt; i++) {
        PDC_CONV_NOEX_CORE(src_data, des_data, float, int, INT_MIN, INT_MAX);
        src_data+=i*stride;
        des_data++;
    }
}
