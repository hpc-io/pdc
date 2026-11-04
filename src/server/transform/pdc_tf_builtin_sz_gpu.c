#include <stdbool.h>
#include <stdint.h>

#include "pdc_tf_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_tf_common.h"
#include "pdc_tf_user.h"
#include "pdc_logger.h"

#ifdef ENABLE_TF_SZ_GPU_COMPRESSSION
#include "cusz.h"
#include <cuda_runtime.h>

typedef struct sz_compress_params_t {
    pdc_tf_region_t decompressed_region;
} sz_compress_params_t;

#define CUDA_CHECK(call)                                                                                     \
    do {                                                                                                     \
        cudaError_t err = call;                                                                              \
        if (err != cudaSuccess) {                                                                            \
            fprintf(stderr, "[CUDA ERROR] %s:%d: %s (code %d)\n", __FILE__, __LINE__,                        \
                    cudaGetErrorString(err), err);                                                           \
            exit(EXIT_FAILURE);                                                                              \
        }                                                                                                    \
    } while (0)

bool
pdc_tf_builtin_sz_compress_cuda(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                                pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_sz_compress_cuda called\n");
    PDCtf_log_pdc_region_t(input_region);

    psz_dtype dtype;
    switch (input_region.pdc_var_type) {
        case PDC_FLOAT:
            dtype = F4;
            break;
        case PDC_DOUBLE:
            dtype = F8;
            break;
        case PDC_INT32:
            dtype = F4;
            break;
        case PDC_INT64:
            dtype = F8;
            break;
        default:
            LOG_ERROR("Unsupported element type for SZ: %d\n", input_region.pdc_var_type);
            return false;
    }

    uint64_t decompressed_bytes = PDC_get_region_desc_size_bytes(
        input_region.size, PDC_get_var_type_size(input_region.pdc_var_type), input_region.ndim);

    float *d_data = NULL;
    cudaMalloc((void **)&d_data, decompressed_bytes);
    cudaMemcpy(d_data, *region_data, decompressed_bytes, cudaMemcpyHostToDevice);

    psz_len3 len = {1, 1, 1};
    if (input_region.ndim >= 1)
        len.x = input_region.size[0];
    if (input_region.ndim >= 2)
        len.y = input_region.size[1];
    if (input_region.ndim >= 3)
        len.z = input_region.size[2];
    if (input_region.ndim > 3) {
        LOG_ERROR("Invalid input region ndim: %d\n", input_region.ndim);
        return false;
    }

    psz_compressor *comp = psz_create_default(dtype, len);

    uint8_t *  d_compressed    = NULL;
    size_t     compressed_size = 0;
    psz_header header;
    void *     record = psz_make_timerecord();
    pszerror   err =
        psz_compress(comp, d_data, len, 0.01, Abs, &d_compressed, &compressed_size, &header, record, 0);
    if (err != PSZ_SUCCESS) {
        printf("Compression failed!\n");
        return false;
    }

    *region_data = (uint8_t *)malloc(compressed_size);
    cudaMemcpy(*region_data, d_compressed, compressed_size, cudaMemcpyDeviceToHost);
    cudaFree(d_data);
    cudaFree(d_compressed);
    psz_release(comp);

    output_region->ndim         = 1;
    output_region->pdc_var_type = PDC_CHAR;
    output_region->size[0]      = compressed_size;

    sz_compress_params_t *out_params = (sz_compress_params_t *)PDC_malloc(sizeof(sz_compress_params_t));
    PDCtf_copy_tf_region_t(&input_region, &out_params->decompressed_region);
    SET_FUNC_PARAMS("zfp_compress", PDC_TF_CPU_DEVICE, out_params, sizeof(sz_compress_params_t));

    printf("Compressed %lu bytes\n", compressed_size);
    return true;
}

bool
pdc_tf_builtin_sz_decompress_cuda(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                                  pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_sz_decompress_cuda called\n");

    psz_header      header;
    psz_compressor *comp = psz_create_from_header(&header);
    if (!comp) {
        LOG_ERROR("Failed to create cuSZ decompressor\n");
        return false;
    }

    uint8_t *d_compressed = NULL;
    CUDA_CHECK(cudaMalloc((void **)&d_compressed, input_region.size[0]));
    CUDA_CHECK(cudaMemcpy(d_compressed, *region_data, input_region.size[0], cudaMemcpyHostToDevice));

    psz_len3 len            = {output_region->size[0], output_region->ndim > 1 ? output_region->size[1] : 1,
                    output_region->ndim > 2 ? output_region->size[2] : 1};
    void *   d_decompressed = NULL;
    size_t   nbytes         = len.x * len.y * len.z * sizeof(float);
    CUDA_CHECK(cudaMalloc(&d_decompressed, nbytes));

    void *   record = capi_psz_make_timerecord();
    pszerror p_err = psz_decompress(comp, d_compressed, input_region.size[0], d_decompressed, len, record, 0);
    if (p_err != PSZ_SUCCESS) {
        LOG_ERROR("cuSZ decompression failed\n");
        cudaFree(d_compressed);
        cudaFree(d_decompressed);
        psz_release(comp);
        return false;
    }
    CUDA_CHECK(cudaDeviceSynchronize());

    void *h_decompressed = malloc(nbytes);
    CUDA_CHECK(cudaMemcpy(h_decompressed, d_decompressed, nbytes, cudaMemcpyDeviceToHost));
    *region_data = h_decompressed;

    cudaFree(d_compressed);
    cudaFree(d_decompressed);

    p_err = psz_clear_buffer(comp);
    if (p_err != PSZ_SUCCESS) {
        LOG_ERROR("cuSZ psz_clear_buffer failed\n");
        return false;
    }
    p_err = psz_release(comp);
    if (p_err != PSZ_SUCCESS) {
        LOG_ERROR("cuSZ psz_release failed\n");
        return false;
    }

    LOG_INFO("cuSZ decompression succeeded\n");
    return true;
}

#endif // ENABLE_TF_SZ_GPU_COMPRESSSION