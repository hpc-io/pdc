#include <math.h>
#include <stddef.h>

#include "pdc_public.h"
#include "pdc_an_builtin_common.h"
#include "pdc_malloc.h"
#include "pdc_logger.h"

static bool
get_elem_as_double(void *buf, uint8_t pdc_var_type, size_t idx, double *out)
{
    switch (pdc_var_type) {
        case PDC_FLOAT:
            *out = (double)((float *)buf)[idx];
            return true;
        case PDC_DOUBLE:
            *out = ((double *)buf)[idx];
            return true;
        default:
            return false;
    }
}

bool
pdc_an_builtin_vector_magnitude(pdc_tf_internal_param *internal_param, char *params_str, void **input_bufs,
                                pdc_tf_region_t *input_regions, int num_inputs, void **output_bufs,
                                pdc_tf_region_t *output_regions, int num_outputs)
{
    (void)internal_param;
    (void)params_str;

    if (num_inputs < 1 || num_outputs != 1) {
        LOG_ERROR("vector_magnitude requires at least 1 input and exactly 1 output\n");
        return false;
    }

    size_t num_elements = 1;
    for (size_t d = 0; d < input_regions[0].ndim; d++)
        num_elements *= input_regions[0].size[d];

    for (int i = 1; i < num_inputs; i++) {
        if (input_regions[i].ndim != input_regions[0].ndim ||
            input_regions[i].pdc_var_type != input_regions[0].pdc_var_type) {
            LOG_ERROR("vector_magnitude: input %d shape/type does not match input 0\n", i);
            return false;
        }
        for (size_t d = 0; d < input_regions[0].ndim; d++) {
            if (input_regions[i].size[d] != input_regions[0].size[d]) {
                LOG_ERROR("vector_magnitude: input %d shape does not match input 0\n", i);
                return false;
            }
        }
    }

    double *out = (double *)PDC_malloc(num_elements * sizeof(double));
    if (out == NULL) {
        LOG_ERROR("vector_magnitude: failed to allocate output buffer\n");
        return false;
    }

    for (size_t e = 0; e < num_elements; e++) {
        double sumsq = 0.0;
        for (int i = 0; i < num_inputs; i++) {
            double v;
            if (!get_elem_as_double(input_bufs[i], input_regions[i].pdc_var_type, e, &v)) {
                LOG_ERROR("vector_magnitude: unsupported input type\n");
                PDC_free(out);
                return false;
            }
            sumsq += v * v;
        }
        out[e] = sqrt(sumsq);
    }

    output_bufs[0]                 = out;
    output_regions[0]              = input_regions[0];
    output_regions[0].pdc_var_type = PDC_DOUBLE;

    return true;
}
