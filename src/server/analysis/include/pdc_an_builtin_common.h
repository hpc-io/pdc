#ifndef PDC_AN_BUILTIN_COMMON_H
#define PDC_AN_BUILTIN_COMMON_H

#include "pdc_an_user.h"

#ifdef __cplusplus
/* Included from both plain C translation units and pdc_an_builtin_magnitude_gpu.cu
 * (compiled as C++ by nvcc) -- without this, the GPU builtin's extern "C"
 * definition there would mismatch the C++ linkage this header would
 * otherwise give its declaration below. */
extern "C" {
#endif

/**
 * @brief Elementwise vector magnitude: sqrt(sum of squares) across
 * num_inputs same-shaped input buffers (e.g. vx, vy, vz components of a
 * vector field), producing one double-precision output buffer of the same
 * shape. Every input must share the same ndim/size/pdc_var_type, and must
 * be PDC_FLOAT or PDC_DOUBLE.
 */
bool pdc_an_builtin_vector_magnitude(pdc_tf_internal_param *internal_param, char *params_str,
                                     void **input_bufs, pdc_tf_region_t *input_regions, int num_inputs,
                                     void **output_bufs, pdc_tf_region_t *output_regions, int num_outputs);

/**
 * @brief Curl of a 3D vector field: exactly 3 float32 inputs (u, v, w --
 * a local (nx,ny,nz) block, x fastest-varying) producing exactly 3
 * float64 outputs (curl_x, curl_y, curl_z), via the shared kernel in
 * src/tests/analysis/curl_math.h. Central differences in the block
 * interior, one-sided at the block's own edges (no cross-rank halo
 * exchange) and unit grid spacing -- a benchmark stand-in for curl's cost
 * and data shape, not a scientifically exact discretization.
 */
bool pdc_an_builtin_curl(pdc_tf_internal_param *internal_param, char *params_str, void **input_bufs,
                         pdc_tf_region_t *input_regions, int num_inputs, void **output_bufs,
                         pdc_tf_region_t *output_regions, int num_outputs);

/**
 * @brief GPU implementation of pdc_an_builtin_vector_magnitude, defined in
 * pdc_an_builtin_magnitude_gpu.cu (only compiled/linked when CUDA_ENABLED).
 * First cut: PDC_DOUBLE inputs only (fails, rather than silently narrowing,
 * on PDC_FLOAT input -- unlike the CPU version, which tolerates either).
 * One CUDA thread per element; not tuned for throughput, since this exists
 * to prove out the dynamic CPU/GPU scheduler in PDCan_exec_graph, not to be
 * a fast kernel.
 */
bool pdc_an_builtin_vector_magnitude_gpu(pdc_tf_internal_param *internal_param, char *params_str,
                                         void **input_bufs, pdc_tf_region_t *input_regions, int num_inputs,
                                         void **output_bufs, pdc_tf_region_t *output_regions,
                                         int num_outputs);

#ifdef __cplusplus
}
#endif

#endif /* PDC_AN_BUILTIN_COMMON_H */
