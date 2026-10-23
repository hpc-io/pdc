#include <stdint.h>

#include "pdc_tf_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_tf_common.h"
#include "pdc_tf_user.h"
#include "pdc_logger.h"

#ifdef ENABLE_TF_SZ_COMPRESSION
#include "sz3c.h"

typedef struct sz_compress_params_t {
    pdc_tf_region_t decompressed_region;
} sz_compress_params_t;

bool
pdc_tf_builtin_sz_compress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                           pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_sz_compress was called\n");
    PDCtf_log_pdc_region_t(input_region);

    int sz_type;
    switch (input_region.pdc_var_type) {
        case PDC_FLOAT:
            sz_type = SZ_FLOAT;
            break;
        case PDC_DOUBLE:
            sz_type = SZ_DOUBLE;
            break;
        case PDC_INT:
        case PDC_INT32:
            sz_type = SZ_FLOAT;
            break;
        case PDC_INT64:
            sz_type = SZ_DOUBLE;
            break;
        default:
            LOG_ERROR("Unsupported element type for SZ3: %d\n", input_region.pdc_var_type);
            return false;
    }

    int    err_bound_mode = ABS;
    double absErrBound    = 0.01;
    double relBoundRatio  = 0.0;
    double pwrBoundRatio  = 0.0;

    size_t r1 = input_region.size[0];
    size_t r2 = (input_region.ndim > 1) ? input_region.size[1] : 1;
    size_t r3 = (input_region.ndim > 2) ? input_region.size[2] : 1;
    size_t r4 = (input_region.ndim > 3) ? input_region.size[3] : 1;
    size_t r5 = (input_region.ndim > 4) ? input_region.size[4] : 1;

    size_t         compressed_size = 0;
    unsigned char *compressed =
        SZ_compress_args(sz_type, *region_data, &compressed_size, err_bound_mode, absErrBound, relBoundRatio,
                         pwrBoundRatio, r5, r4, r3, r2, r1);
    if (!compressed) {
        LOG_ERROR("SZ3 compression failed\n");
        return false;
    }

    *region_data                = compressed;
    output_region->ndim         = 1;
    output_region->pdc_var_type = PDC_CHAR;
    output_region->size[0]      = compressed_size;

    sz_compress_params_t *out_params = (sz_compress_params_t *)PDC_malloc(sizeof(sz_compress_params_t));
    PDCtf_copy_tf_region_t(&input_region, &out_params->decompressed_region);
    SET_FUNC_PARAMS("sz_compress", PDC_TF_CPU_DEVICE, out_params, sizeof(sz_compress_params_t));

    LOG_INFO("SZ3 compression succeeded, compressed size: %zu bytes\n", compressed_size);
    return true;
}

bool
pdc_tf_builtin_sz_decompress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                             pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_sz_decompress was called\n");
    PDCtf_log_pdc_region_t(input_region);

    sz_compress_params_t *in_params = NULL;
    uint64_t              in_params_size;
    GET_FUNC_PARAMS("sz_compress", PDC_TF_CPU_DEVICE, (void **)&in_params, &in_params_size);
    PDCtf_log_pdc_region_t(in_params->decompressed_region);

    int sz_type;
    switch (in_params->decompressed_region.pdc_var_type) {
        case PDC_FLOAT:
            sz_type = SZ_FLOAT;
            break;
        case PDC_DOUBLE:
            sz_type = SZ_DOUBLE;
            break;
        case PDC_INT:
        case PDC_INT32:
            sz_type = SZ_FLOAT;
            break;
        case PDC_INT64:
            sz_type = SZ_DOUBLE;
            break;
        default:
            LOG_ERROR("Unsupported element type for SZ3: %d\n", in_params->decompressed_region.pdc_var_type);
            return false;
    }

    size_t r1 = in_params->decompressed_region.size[0];
    size_t r2 = (in_params->decompressed_region.ndim > 1) ? in_params->decompressed_region.size[1] : 1;
    size_t r3 = (in_params->decompressed_region.ndim > 2) ? in_params->decompressed_region.size[2] : 1;
    size_t r4 = (in_params->decompressed_region.ndim > 3) ? in_params->decompressed_region.size[3] : 1;
    size_t r5 = (in_params->decompressed_region.ndim > 4) ? in_params->decompressed_region.size[4] : 1;

    void *decompressed = SZ_decompress(sz_type, *region_data, input_region.size[0], r5, r4, r3, r2, r1);
    if (!decompressed) {
        LOG_ERROR("SZ3 decompression failed\n");
        return false;
    }

    *region_data = decompressed;
    PDCtf_copy_tf_region_t(&in_params->decompressed_region, output_region);

    LOG_INFO("SZ3 decompression succeeded\n");
    return true;
}

#endif // ENABLE_TF_SZ_COMPRESSION