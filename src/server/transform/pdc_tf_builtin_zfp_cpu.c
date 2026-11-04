#include <stdint.h>
#include <assert.h>

#include "pdc_tf_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_tf_common.h"
#include "pdc_tf_user.h"
#include "pdc_logger.h"

#ifdef ENABLE_TF_ZFP_COMPRESSION
#include <zfp.h>

typedef struct zfp_compress_params_t {
    pdc_tf_region_t decompressed_region;
} zfp_compress_params_t;

#define FIXED_CR_RATIO 1

static void
print_ztype(zfp_type z_type)
{
    switch (z_type) {
        case zfp_type_int32:
            LOG_DEBUG("ZFP type: int32\n");
            break;
        case zfp_type_int64:
            LOG_DEBUG("ZFP type: int64\n");
            break;
        case zfp_type_float:
            LOG_DEBUG("ZFP type: float\n");
            break;
        case zfp_type_double:
            LOG_DEBUG("ZFP type: double\n");
            break;
        case zfp_type_none:
        default:
            LOG_ERROR("ZFP type: none/unknown (%d)\n", (int)z_type);
            break;
    }
}

static bool
pdc_tf_builtin_zfp_compress_helper(pdc_tf_internal_param *internal_param, char *params_str,
                                   void **region_data, pdc_tf_region_t input_region,
                                   pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_zfp_compress was called\n");
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
    print_ztype(z_type);
    size_t bits_per_value = (8 * PDC_get_var_type_size(input_region.pdc_var_type)) / FIXED_CR_RATIO;
    LOG_INFO("BITS PER VALUE %d\n", bits_per_value);

    zfp_field *field = NULL;
    switch (input_region.ndim) {
        case 1:
            field = zfp_field_1d(*region_data, z_type, input_region.size[0]);
            break;
        case 2:
            field = zfp_field_2d(*region_data, z_type, input_region.size[0], input_region.size[1]);
            break;
        case 3:
            field = zfp_field_3d(*region_data, z_type, input_region.size[0], input_region.size[1],
                                 input_region.size[2]);
            break;
        case 4:
            field = zfp_field_4d(*region_data, z_type, input_region.size[0], input_region.size[1],
                                 input_region.size[2], input_region.size[3]);
            break;
        case 0:
            LOG_ERROR("ZFP compression not supported for 0 dimensions\n");
            return false;
        default:
            LOG_ERROR("ZFP compression not supported for > 4 dimensions\n");
            return false;
    }
    if (!field) {
        LOG_ERROR("field was NULL\n");
        return false;
    }

    zfp_stream *zfp               = zfp_stream_open(NULL);
    size_t      bufsize           = zfp_stream_maximum_size(zfp, field);
    void *      compressed_buffer = PDC_malloc(bufsize);

    bitstream *stream = stream_open(compressed_buffer, bufsize);
    zfp_stream_set_rate(zfp, bits_per_value, z_type, input_region.ndim, 1);
    zfp_stream_set_bit_stream(zfp, stream);
    zfp_stream_rewind(zfp);

    size_t compressed_size = zfp_compress(zfp, field);

    *region_data                = compressed_buffer;
    output_region->ndim         = 1;
    output_region->pdc_var_type = PDC_CHAR;
    output_region->size[0]      = compressed_size;

    zfp_field_free(field);
    zfp_stream_close(zfp);
    stream_close(stream);

    zfp_compress_params_t *out_params = (zfp_compress_params_t *)PDC_malloc(sizeof(zfp_compress_params_t));
    PDCtf_copy_tf_region_t(&input_region, &out_params->decompressed_region);
    SET_FUNC_PARAMS("zfp_compress", PDC_TF_CPU_DEVICE, out_params, sizeof(zfp_compress_params_t));

    LOG_INFO("ZFP compression succeeded, compressed size: %zu bytes\n", compressed_size);
    return true;
}

static bool
pdc_tf_builtin_zfp_decompress_helper(pdc_tf_internal_param *internal_param, char *params_str,
                                     void **region_data, pdc_tf_region_t input_region,
                                     pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_zfp_decompress was called\n");
    PDCtf_log_pdc_region_t(input_region);

    zfp_compress_params_t *in_params = NULL;
    uint64_t               in_params_size;

    GET_FUNC_PARAMS("zfp_compress", PDC_TF_GPU_DEVICE, (void **)&in_params, &in_params_size);
    if (!in_params)
        GET_FUNC_PARAMS("zfp_compress", PDC_TF_CPU_DEVICE, (void **)&in_params, &in_params_size);
    if (!in_params) {
        LOG_ERROR("Failed to get ZFP compression parameters.\n");
        return false;
    }

    PDCtf_log_pdc_region_t(in_params->decompressed_region);

    zfp_type z_type;
    switch (in_params->decompressed_region.pdc_var_type) {
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
            LOG_ERROR("Invalid element type %d\n", in_params->decompressed_region.pdc_var_type);
            return false;
    }
    print_ztype(z_type);

    size_t bits_per_value =
        (8 * PDC_get_var_type_size(in_params->decompressed_region.pdc_var_type)) / FIXED_CR_RATIO;
    size_t compressed_size = input_region.size[0];

    bitstream * stream = stream_open(*region_data, compressed_size);
    zfp_stream *zfp    = zfp_stream_open(NULL);
    zfp_stream_set_rate(zfp, bits_per_value, z_type, in_params->decompressed_region.ndim, 1);
    zfp_stream_set_bit_stream(zfp, stream);
    zfp_stream_rewind(zfp);

    size_t total_bytes = PDCtf_get_pdc_region_t_bytes(in_params->decompressed_region);
    void * buf         = malloc(total_bytes);

    zfp_field *field = NULL;
    switch (in_params->decompressed_region.ndim) {
        case 1:
            field = zfp_field_1d(buf, z_type, in_params->decompressed_region.size[0]);
            break;
        case 2:
            field = zfp_field_2d(buf, z_type, in_params->decompressed_region.size[0],
                                 in_params->decompressed_region.size[1]);
            break;
        case 3:
            field =
                zfp_field_3d(buf, z_type, in_params->decompressed_region.size[0],
                             in_params->decompressed_region.size[1], in_params->decompressed_region.size[2]);
            break;
        case 4:
            field = zfp_field_4d(
                buf, z_type, in_params->decompressed_region.size[0], in_params->decompressed_region.size[1],
                in_params->decompressed_region.size[2], in_params->decompressed_region.size[3]);
            break;
        default:
            LOG_ERROR("Unsupported ndim: %d\n", in_params->decompressed_region.ndim);
            return false;
    }

    size_t decompressed_size = zfp_decompress(zfp, field);
    PDCtf_copy_tf_region_t(&in_params->decompressed_region, output_region);
    *region_data = buf;

    size_t sizes[4];
    size_t num_elements = zfp_field_size(field, sizes);
    size_t elem_size    = 0;
    switch (zfp_field_type(field)) {
        case zfp_type_int32:
            elem_size = 4;
            break;
        case zfp_type_int64:
            elem_size = 8;
            break;
        case zfp_type_float:
            elem_size = 4;
            break;
        case zfp_type_double:
            elem_size = 8;
            break;
        default:
            elem_size = 0;
            break;
    }
    LOG_DEBUG("Actual decompressed size: %zu bytes\n", num_elements * elem_size);

    zfp_field_free(field);
    zfp_stream_close(zfp);
    stream_close(stream);

    return true;
}

bool
pdc_tf_builtin_zfp_compress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                            pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    return pdc_tf_builtin_zfp_compress_helper(internal_param, params_str, region_data, input_region,
                                              output_region);
}

bool
pdc_tf_builtin_zfp_decompress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                              pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    return pdc_tf_builtin_zfp_decompress_helper(internal_param, params_str, region_data, input_region,
                                                output_region);
}

#endif // ENABLE_TF_ZFP_COMPRESSION