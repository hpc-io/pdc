/**
 * GPU device-variant of vector_magnitude, registered as a second candidate
 * (alongside pdc_an_builtin_magnitude_cpu.c's pdc_an_builtin_vector_magnitude)
 * so PDCan_exec_graph's dynamic scheduler (see an_select_variant, pdc_an_server.c)
 * has something real to choose between for a "vector_magnitude" transformation
 * whose JSON entry omits "device".
 *
 * Deliberately simple -- one thread per element, no shared memory, no
 * tuning -- since the point of this first cut is proving the scheduling
 * mechanism end-to-end, not kernel performance. PDC_DOUBLE inputs only;
 * curl's GPU kernel is out of scope for this pass (see the implementation
 * plan).
 */
#include <cmath>
#include <cuda_runtime.h>

/* pdc_malloc.h/pdc_logger.h are plain C headers with no extern "C" guards
 * of their own (nothing in this codebase included them from C++ before) --
 * without this, nvcc (compiling this file as C++) would give PDC_malloc/
 * PDC_free/log_message C++ linkage here, which would not match their
 * actual C-linkage definitions in pdc_malloc.c/pdc_logger.c at link time. */
extern "C" {
#include "pdc_public.h"
#include "pdc_an_builtin_common.h"
#include "pdc_malloc.h"
#include "pdc_logger.h"
}

__global__ void
an_magnitude_kernel(const double *const *inputs, int num_inputs, double *out, size_t n)
{
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n)
        return;

    double sumsq = 0.0;
    for (int k = 0; k < num_inputs; k++) {
        double v = inputs[k][i];
        sumsq += v * v;
    }
    out[i] = sqrt(sumsq);
}

extern "C" bool
pdc_an_builtin_vector_magnitude_gpu(pdc_tf_internal_param *internal_param, char *params_str,
                                    void **input_bufs, pdc_tf_region_t *input_regions, int num_inputs,
                                    void **output_bufs, pdc_tf_region_t *output_regions, int num_outputs)
{
    (void)internal_param;
    (void)params_str;

    if (num_inputs < 1 || num_outputs != 1) {
        LOG_ERROR("vector_magnitude_gpu requires at least 1 input and exactly 1 output\n");
        return false;
    }

    for (int i = 0; i < num_inputs; i++) {
        if (input_regions[i].pdc_var_type != PDC_DOUBLE) {
            LOG_ERROR("vector_magnitude_gpu: input %d is not PDC_DOUBLE; this first-cut GPU kernel only "
                      "supports double-precision input\n",
                      i);
            return false;
        }
        if (input_regions[i].ndim != input_regions[0].ndim) {
            LOG_ERROR("vector_magnitude_gpu: input %d shape does not match input 0\n", i);
            return false;
        }
        for (size_t d = 0; d < input_regions[0].ndim; d++) {
            if (input_regions[i].size[d] != input_regions[0].size[d]) {
                LOG_ERROR("vector_magnitude_gpu: input %d shape does not match input 0\n", i);
                return false;
            }
        }
    }

    size_t num_elements = 1;
    for (size_t d = 0; d < input_regions[0].ndim; d++)
        num_elements *= input_regions[0].size[d];

    bool     ret_value     = false;
    double * d_out         = NULL;
    double **d_inputs_arr  = NULL; /* device array of device pointers */
    double **h_inputs_arr  = (double **)PDC_malloc((size_t)num_inputs * sizeof(double *));
    double * out           = NULL;

    for (int i = 0; i < num_inputs; i++)
        h_inputs_arr[i] = NULL;

    if (cudaMalloc(&d_out, num_elements * sizeof(double)) != cudaSuccess) {
        LOG_ERROR("vector_magnitude_gpu: cudaMalloc failed for output\n");
        goto done;
    }
    if (cudaMalloc(&d_inputs_arr, (size_t)num_inputs * sizeof(double *)) != cudaSuccess) {
        LOG_ERROR("vector_magnitude_gpu: cudaMalloc failed for input pointer array\n");
        goto done;
    }

    for (int i = 0; i < num_inputs; i++) {
        double *d_in = NULL;
        if (cudaMalloc(&d_in, num_elements * sizeof(double)) != cudaSuccess) {
            LOG_ERROR("vector_magnitude_gpu: cudaMalloc failed for input %d\n", i);
            goto done;
        }
        h_inputs_arr[i] = d_in;
        if (cudaMemcpy(d_in, input_bufs[i], num_elements * sizeof(double), cudaMemcpyHostToDevice) !=
            cudaSuccess) {
            LOG_ERROR("vector_magnitude_gpu: cudaMemcpy H2D failed for input %d\n", i);
            goto done;
        }
    }

    if (cudaMemcpy(d_inputs_arr, h_inputs_arr, (size_t)num_inputs * sizeof(double *),
                   cudaMemcpyHostToDevice) != cudaSuccess) {
        LOG_ERROR("vector_magnitude_gpu: cudaMemcpy H2D failed for input pointer array\n");
        goto done;
    }

    {
        int threads = 256;
        int blocks  = (int)((num_elements + (size_t)threads - 1) / (size_t)threads);
        an_magnitude_kernel<<<blocks, threads>>>(d_inputs_arr, num_inputs, d_out, num_elements);
    }

    if (cudaGetLastError() != cudaSuccess) {
        LOG_ERROR("vector_magnitude_gpu: kernel launch failed\n");
        goto done;
    }
    if (cudaDeviceSynchronize() != cudaSuccess) {
        LOG_ERROR("vector_magnitude_gpu: cudaDeviceSynchronize failed\n");
        goto done;
    }

    out = (double *)PDC_malloc(num_elements * sizeof(double));
    if (out == NULL) {
        LOG_ERROR("vector_magnitude_gpu: failed to allocate host output buffer\n");
        goto done;
    }
    if (cudaMemcpy(out, d_out, num_elements * sizeof(double), cudaMemcpyDeviceToHost) != cudaSuccess) {
        LOG_ERROR("vector_magnitude_gpu: cudaMemcpy D2H failed\n");
        PDC_free(out);
        out = NULL;
        goto done;
    }

    output_bufs[0]                 = out;
    output_regions[0]              = input_regions[0];
    output_regions[0].pdc_var_type = PDC_DOUBLE;
    ret_value                      = true;

done:
    if (d_out != NULL)
        cudaFree(d_out);
    if (d_inputs_arr != NULL)
        cudaFree(d_inputs_arr);
    for (int i = 0; i < num_inputs; i++)
        if (h_inputs_arr[i] != NULL)
            cudaFree(h_inputs_arr[i]);
    PDC_free(h_inputs_arr);

    return ret_value;
}
