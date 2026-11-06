#include <stdint.h>
#include <assert.h>

#include "pdc_tf_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_tf_common.h"
#include "pdc_tf_user.h"
#include "pdc_logger.h"

#ifdef ENABLE_TF_ZFP_COMPRESSION
#ifdef CUDA_ENABLED

#include <zfp.h>
#include <cuda_runtime.h>

typedef struct zfp_compress_params_t {
    pdc_tf_region_t decompressed_region;
} zfp_compress_params_t;

#define FIXED_CR_RATIO 1

#define CUDA_CHECK(call)                                                                                     \
    do {                                                                                                     \
        cudaError_t err = call;                                                                              \
        if (err != cudaSuccess) {                                                                            \
            fprintf(stderr, "[CUDA ERROR] %s:%d: %s (code %d)\n", __FILE__, __LINE__,                        \
                    cudaGetErrorString(err), err);                                                           \
            exit(EXIT_FAILURE);                                                                              \
        }                                                                                                    \
        cudaDeviceSynchronize();                                                                             \
    } while (0)

static bool
pdc_tf_builtin_zfp_compress_cuda_helper(pdc_tf_internal_param *internal_param, char *params_str,
                                        void **region_data, pdc_tf_region_t input_region,
                                        pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_zfp_compress_cuda was called\n");
    PDCtf_log_pdc_region_t(input_region);

    zfp_type z_type;
    switch (input_region.pdc_var_type) {
        case PDC_FLOAT:
            z_type = zfp_type_float;
            break;
        case PDC_DOUBLE:
            z_type = zfp_type_double;
            break;
        case PDC_INT:
        case PDC_INT32:
            z_type = zfp_type_int32;
            break;
        case PDC_INT64:
            z_type = zfp_type_int64;
            break;
        default:
            LOG_ERROR("Invalid element type\n");
            return false;
    }
    size_t bits_per_value = (8 * PDC_get_var_type_size(input_region.pdc_var_type)) / FIXED_CR_RATIO;
    size_t num_bytes      = PDCtf_get_pdc_region_t_bytes(input_region);

    void *dev_in  = NULL;
    void *dev_out = NULL;
    CUDA_CHECK(cudaMalloc(&dev_in, num_bytes));

    START_HOST_TO_DEV_TIME();
    CUDA_CHECK(cudaMemcpy(dev_in, *region_data, num_bytes, cudaMemcpyHostToDevice));
    END_HOST_TO_DEV_TIME();

    zfp_field *field = NULL;
    switch (input_region.ndim) {
        case 1:
            field = zfp_field_1d(dev_in, z_type, input_region.size[0]);
            break;
        case 2:
            field = zfp_field_2d(dev_in, z_type, input_region.size[0], input_region.size[1]);
            break;
        case 3:
            field = zfp_field_3d(dev_in, z_type, input_region.size[0], input_region.size[1],
                                 input_region.size[2]);
            break;
        case 4:
            field = zfp_field_4d(dev_in, z_type, input_region.size[0], input_region.size[1],
                                 input_region.size[2], input_region.size[3]);
            break;
        default:
            LOG_ERROR("Unsupported ndim: %d\n", input_region.ndim);
            cudaFree(dev_in);
            return false;
    }

    zfp_stream *zfp     = zfp_stream_open(NULL);
    size_t      bufsize = zfp_stream_maximum_size(zfp, field);
    cudaMalloc(&dev_out, bufsize);

    bitstream *stream = stream_open(dev_out, bufsize);
    zfp_stream_set_bit_stream(zfp, stream);
    zfp_stream_set_rate(zfp, bits_per_value, z_type, input_region.ndim, 1);
    zfp_stream_set_execution(zfp, zfp_exec_cuda);
    zfp_stream_rewind(zfp);

    size_t compressed_size = zfp_compress(zfp, field);
    CUDA_CHECK(cudaDeviceSynchronize());
    assert(compressed_size > 0);

    void *host_compressed = malloc(compressed_size);

    START_DEV_TO_HOST_TIME();
    CUDA_CHECK(cudaMemcpy(host_compressed, dev_out, compressed_size, cudaMemcpyDeviceToHost));
    END_DEV_TO_HOST_TIME();

    CUDA_CHECK(cudaFree(dev_in));
    CUDA_CHECK(cudaFree(dev_out));

    *region_data                = host_compressed;
    output_region->ndim         = 1;
    output_region->pdc_var_type = PDC_CHAR;
    output_region->size[0]      = compressed_size;

    zfp_field_free(field);
    zfp_stream_close(zfp);
    stream_close(stream);

    zfp_compress_params_t *out_params = (zfp_compress_params_t *)malloc(sizeof(zfp_compress_params_t));
    PDCtf_copy_tf_region_t(&input_region, &out_params->decompressed_region);
    SET_FUNC_PARAMS("zfp_compress", PDC_TF_GPU_DEVICE, out_params, sizeof(zfp_compress_params_t));

    LOG_DEBUG("CUDA ZFP compression succeeded, %zu bytes\n", compressed_size);
    return true;
}

static bool
pdc_tf_builtin_zfp_decompress_cuda_helper(pdc_tf_internal_param *internal_param, char *params_str,
                                          void **region_data, pdc_tf_region_t input_region,
                                          pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_zfp_decompress_cuda_safe called\n");
    PDCtf_log_pdc_region_t(input_region);

    zfp_compress_params_t *in_params      = NULL;
    uint64_t               in_params_size = 0;

    GET_FUNC_PARAMS("zfp_compress", PDC_TF_GPU_DEVICE, (void **)&in_params, &in_params_size);
    if (!in_params)
        GET_FUNC_PARAMS("zfp_compress", PDC_TF_CPU_DEVICE, (void **)&in_params, &in_params_size);
    if (!in_params) {
        LOG_ERROR("Failed to get ZFP compression parameters.\n");
        return false;
    }

    zfp_type z_type;
    size_t   type_size = 0;
    switch (in_params->decompressed_region.pdc_var_type) {
        case PDC_FLOAT:
            z_type    = zfp_type_float;
            type_size = sizeof(float);
            break;
        case PDC_DOUBLE:
            z_type    = zfp_type_double;
            type_size = sizeof(double);
            break;
        case PDC_INT:
        case PDC_INT32:
            z_type    = zfp_type_int32;
            type_size = sizeof(int32_t);
            break;
        case PDC_INT64:
            z_type    = zfp_type_int64;
            type_size = sizeof(int64_t);
            break;
        default:
            LOG_ERROR("Unsupported element type %d\n", in_params->decompressed_region.pdc_var_type);
            return false;
    }
    size_t bits_per_value =
        (8 * PDC_get_var_type_size(in_params->decompressed_region.pdc_var_type)) / FIXED_CR_RATIO;

    size_t total_elements = 1;
    for (int i = 0; i < in_params->decompressed_region.ndim; i++)
        total_elements *= in_params->decompressed_region.size[i];
    size_t uncompressed_size = total_elements * type_size;
    size_t compressed_size   = input_region.size[0];

    void *dev_compressed   = NULL;
    void *dev_uncompressed = NULL;
    CUDA_CHECK(cudaMalloc(&dev_compressed, compressed_size + 64));
    CUDA_CHECK(cudaMalloc(&dev_uncompressed, uncompressed_size));

    START_HOST_TO_DEV_TIME();
    CUDA_CHECK(cudaMemcpy(dev_compressed, *region_data, compressed_size, cudaMemcpyHostToDevice));
    END_HOST_TO_DEV_TIME();

    zfp_field *field = NULL;
    switch (in_params->decompressed_region.ndim) {
        case 1:
            field = zfp_field_1d(dev_uncompressed, z_type, in_params->decompressed_region.size[0]);
            break;
        case 2:
            field = zfp_field_2d(dev_uncompressed, z_type, in_params->decompressed_region.size[0],
                                 in_params->decompressed_region.size[1]);
            break;
        case 3:
            field =
                zfp_field_3d(dev_uncompressed, z_type, in_params->decompressed_region.size[0],
                             in_params->decompressed_region.size[1], in_params->decompressed_region.size[2]);
            break;
        case 4:
            field =
                zfp_field_4d(dev_uncompressed, z_type, in_params->decompressed_region.size[0],
                             in_params->decompressed_region.size[1], in_params->decompressed_region.size[2],
                             in_params->decompressed_region.size[3]);
            break;
        default:
            LOG_ERROR("Unsupported ndim: %d\n", in_params->decompressed_region.ndim);
            CUDA_CHECK(cudaFree(dev_compressed));
            CUDA_CHECK(cudaFree(dev_uncompressed));
            return false;
    }

    zfp_stream *zfp = zfp_stream_open(NULL);
    if (!zfp) {
        LOG_ERROR("Failed to open zfp stream\n");
        CUDA_CHECK(cudaFree(dev_compressed));
        CUDA_CHECK(cudaFree(dev_uncompressed));
        zfp_field_free(field);
        return false;
    }

    bitstream *stream = stream_open(dev_compressed, compressed_size);
    if (!stream) {
        LOG_ERROR("Failed to open bitstream\n");
        zfp_stream_close(zfp);
        zfp_field_free(field);
        CUDA_CHECK(cudaFree(dev_compressed));
        CUDA_CHECK(cudaFree(dev_uncompressed));
        return false;
    }

    zfp_stream_set_bit_stream(zfp, stream);
    zfp_stream_set_rate(zfp, bits_per_value, z_type, in_params->decompressed_region.ndim, 1);
    zfp_stream_set_execution(zfp, zfp_exec_cuda);
    zfp_stream_rewind(zfp);

    size_t decompressed_size = zfp_decompress(zfp, field);
    CUDA_CHECK(cudaDeviceSynchronize());
    if (decompressed_size == 0) {
        LOG_ERROR("ZFP decompression failed\n");
        stream_close(stream);
        zfp_stream_close(zfp);
        zfp_field_free(field);
        CUDA_CHECK(cudaFree(dev_compressed));
        CUDA_CHECK(cudaFree(dev_uncompressed));
        return false;
    }

    void *host_buf = malloc(uncompressed_size);
    if (!host_buf) {
        LOG_ERROR("Failed to allocate host buffer\n");
        stream_close(stream);
        zfp_stream_close(zfp);
        zfp_field_free(field);
        CUDA_CHECK(cudaFree(dev_compressed));
        CUDA_CHECK(cudaFree(dev_uncompressed));
        return false;
    }

    START_DEV_TO_HOST_TIME();
    CUDA_CHECK(cudaMemcpy(host_buf, dev_uncompressed, uncompressed_size, cudaMemcpyDeviceToHost));
    END_DEV_TO_HOST_TIME();

    CUDA_CHECK(cudaFree(dev_compressed));
    CUDA_CHECK(cudaFree(dev_uncompressed));

    *region_data = host_buf;
    PDCtf_copy_tf_region_t(&in_params->decompressed_region, output_region);

    zfp_field_free(field);
    zfp_stream_close(zfp);
    stream_close(stream);

    LOG_DEBUG("CUDA ZFP decompression succeeded: %zu bytes\n", decompressed_size);
    return true;
}

bool
pdc_tf_builtin_zfp_compress_cuda(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                                 pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    return pdc_tf_builtin_zfp_compress_cuda_helper(internal_param, params_str, region_data, input_region,
                                                   output_region);
}

bool
pdc_tf_builtin_zfp_decompress_cuda(pdc_tf_internal_param *internal_param, char *params_str,
                                   void **region_data, pdc_tf_region_t input_region,
                                   pdc_tf_region_t *output_region)
{
    return pdc_tf_builtin_zfp_decompress_cuda_helper(internal_param, params_str, region_data, input_region,
                                                     output_region);
}

#endif // CUDA_ENABLED
#endif // ENABLE_TF_ZFP_COMPRESSION