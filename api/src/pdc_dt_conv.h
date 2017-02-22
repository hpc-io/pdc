#include <stdlib.h>
#include <assert.h>
#include "pdc_public.h"
#include "pdc_private.h"


typedef perr_t (*pdc_conv_t) (void *src_data, void *des_data, size_t nelemt, size_t stride);

pdc_conv_t
pdc_find_conv_func(PDC_var_type_t src_id, PDC_var_type_t des_id);

perr_t
pdc_type_conv(PDC_var_type_t src_id, PDC_var_type_t des_id, void *src_data, void *des_data, size_t nelemt, size_t stride);

perr_t
pdc__conv_f_i(float *src_data, int *des_data, size_t nelemt, size_t stride);
