#ifndef PDC_TF_BUILTIN_COMMON_H
#define PDC_TF_BUILTIN_COMMON_H

#include <stdbool.h>
#include <stdint.h>

// Each of these tracks whether the root CMakeLists.txt actually found that
// transformation's third-party dependency (ZFP_ENABLED/LIBSODIUM_ENABLED/
// SZ3_ENABLED/TURBO_ENABLED, set only when the corresponding *_ROOT_DIR
// env var was set and the library was actually found there) -- every
// transformation dependency is optional, so a missing one compiles that
// one transformation out instead of failing the whole build.
#ifdef ZFP_ENABLED
#define ENABLE_TF_ZFP_COMPRESSION
#endif
#ifdef LIBSODIUM_ENABLED
#define ENABLE_TF_SECRET_BOX_ENCRYPTION
#endif
#ifdef SZ3_ENABLED
#define ENABLE_TF_SZ_COMPRESSION
#endif
// pdc_tf_builtin_sz_gpu.c needs cusz.h/cuda_runtime.h and real cuSZ/CUDA
// linkage to compile at all, so this additionally requires CUDA_ENABLED
// (matching the ENABLE_TF_ZFP_COMPRESSION + CUDA_ENABLED pattern below).
#if defined(ENABLE_TF_SZ_COMPRESSION) && defined(CUDA_ENABLED)
#define ENABLE_TF_SZ_GPU_COMPRESSSION
#endif
#ifdef TURBO_ENABLED
#define ENABLE_TF_TURBO_COMPRESSION
#endif

#include "pdc_tf_common.h"

#ifdef ENABLE_TF_SZ_GPU_COMPRESSSION
bool pdc_tf_builtin_sz_compress_cuda(pdc_tf_internal_param *internal_param, char *params_str,
                                     void **region_data, pdc_tf_region_t input_region,
                                     pdc_tf_region_t *output_region);
bool pdc_tf_builtin_sz_decompress_cuda(pdc_tf_internal_param *internal_param, char *params_str,
                                       void **region_data, pdc_tf_region_t input_region,
                                       pdc_tf_region_t *output_region);
#endif
#ifdef ENABLE_TF_SZ_COMPRESSION
bool pdc_tf_builtin_sz_compress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                                pdc_tf_region_t input_region, pdc_tf_region_t *output_region);
bool pdc_tf_builtin_sz_decompress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                                  pdc_tf_region_t input_region, pdc_tf_region_t *output_region);
#endif
#ifdef ENABLE_TF_ZFP_COMPRESSION
bool pdc_tf_builtin_zfp_compress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                                 pdc_tf_region_t input_region, pdc_tf_region_t *output_region);
bool pdc_tf_builtin_zfp_decompress(pdc_tf_internal_param *internal_param, char *params_str,
                                   void **region_data, pdc_tf_region_t input_region,
                                   pdc_tf_region_t *output_region);
#endif
#if defined(ENABLE_TF_ZFP_COMPRESSION) && defined(CUDA_ENABLED)
bool pdc_tf_builtin_zfp_compress_cuda(pdc_tf_internal_param *internal_param, char *params_str,
                                      void **region_data, pdc_tf_region_t input_region,
                                      pdc_tf_region_t *output_region);
bool pdc_tf_builtin_zfp_decompress_cuda(pdc_tf_internal_param *internal_param, char *params_str,
                                        void **region_data, pdc_tf_region_t input_region,
                                        pdc_tf_region_t *output_region);
#endif

#ifdef ENABLE_TF_SECRET_BOX_ENCRYPTION
bool pdc_tf_builtin_encrypt(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                            pdc_tf_region_t input_region, pdc_tf_region_t *output_region);
bool pdc_tf_builtin_decrypt(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                            pdc_tf_region_t input_region, pdc_tf_region_t *output_region);
#endif

#ifdef ENABLE_TF_TURBO_COMPRESSION
bool pdc_tf_builtin_turbo_compress(pdc_tf_internal_param *internal_param, char *params_str,
                                   void **region_data, pdc_tf_region_t input_region,
                                   pdc_tf_region_t *output_region);
bool pdc_tf_builtin_turbo_decompress(pdc_tf_internal_param *internal_param, char *params_str,
                                     void **region_data, pdc_tf_region_t input_region,
                                     pdc_tf_region_t *output_region);
#endif

#endif
