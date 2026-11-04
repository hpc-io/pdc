#include <stddef.h>

#include "pdc_public.h"
#include "pdc_an_builtin_common.h"
#include "pdc_malloc.h"
#include "pdc_logger.h"

#include "curl_math.h"

bool
pdc_an_builtin_curl(pdc_tf_internal_param *internal_param, char *params_str, void **input_bufs,
                    pdc_tf_region_t *input_regions, int num_inputs, void **output_bufs,
                    pdc_tf_region_t *output_regions, int num_outputs)
{
    (void)internal_param;
    (void)params_str;

    if (num_inputs != 3 || num_outputs != 3) {
        LOG_ERROR("curl requires exactly 3 inputs (u, v, w) and 3 outputs (curl_x, curl_y, curl_z)\n");
        return false;
    }

    pdc_tf_region_t *r0 = &input_regions[0];
    if (r0->ndim != 3) {
        LOG_ERROR("curl requires 3D input regions (local nx,ny,nz block per rank)\n");
        return false;
    }
    for (int i = 0; i < 3; i++) {
        if (input_regions[i].ndim != r0->ndim || input_regions[i].pdc_var_type != PDC_FLOAT ||
            input_regions[i].size[0] != r0->size[0] || input_regions[i].size[1] != r0->size[1] ||
            input_regions[i].size[2] != r0->size[2]) {
            LOG_ERROR("curl: input %d shape/type does not match input 0 (all of u,v,w must be float32 "
                      "with identical shape)\n",
                      i);
            return false;
        }
    }

    size_t nx           = (size_t)r0->size[0];
    size_t ny           = (size_t)r0->size[1];
    size_t nz           = (size_t)r0->size[2];
    size_t num_elements = nx * ny * nz;

    double *curl_x = (double *)PDC_malloc(num_elements * sizeof(double));
    double *curl_y = (double *)PDC_malloc(num_elements * sizeof(double));
    double *curl_z = (double *)PDC_malloc(num_elements * sizeof(double));
    if (curl_x == NULL || curl_y == NULL || curl_z == NULL) {
        LOG_ERROR("curl: failed to allocate output buffers\n");
        PDC_free(curl_x);
        PDC_free(curl_y);
        PDC_free(curl_z);
        return false;
    }

    curl_math_compute((const float *)input_bufs[0], (const float *)input_bufs[1],
                      (const float *)input_bufs[2], nx, ny, nz, curl_x, curl_y, curl_z);

    output_bufs[0] = curl_x;
    output_bufs[1] = curl_y;
    output_bufs[2] = curl_z;
    for (int i = 0; i < 3; i++) {
        output_regions[i]              = *r0;
        output_regions[i].pdc_var_type = PDC_DOUBLE;
    }

    return true;
}
