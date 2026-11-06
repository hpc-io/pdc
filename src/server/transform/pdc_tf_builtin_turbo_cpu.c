#include <stdint.h>

#include "pdc_tf_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_tf_common.h"
#include "pdc_tf_user.h"
#include "pdc_logger.h"

#ifdef ENABLE_TF_TURBO_COMPRESSION
#include <ic.h>

typedef struct turbo_compress_params_t {
    pdc_tf_region_t decompressed_region;
} turbo_compress_params_t;

bool
pdc_tf_builtin_turbo_compress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                              pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_turbo_compress called\n");
    PDCtf_log_pdc_region_t(input_region);

    size_t size       = PDCtf_get_pdc_region_t_bytes(input_region);
    void * compressed = malloc(size);
    size_t ret;

    switch (input_region.pdc_var_type) {
        case PDC_INT:
        case PDC_INT32:
            ret = p4nenc32((uint32_t *)*region_data, PDCtf_get_pdc_region_t_elements(input_region),
                           (unsigned char *)compressed);
            break;
        case PDC_INT64:
            ret = p4nenc64((uint64_t *)*region_data, PDCtf_get_pdc_region_t_elements(input_region),
                           (unsigned char *)compressed);
            break;
        default:
            LOG_ERROR("Invalid element type\n");
            return false;
    }

    output_region->ndim         = 1;
    output_region->pdc_var_type = PDC_CHAR;
    output_region->size[0]      = ret;
    *region_data                = compressed;

    turbo_compress_params_t *out_params =
        (turbo_compress_params_t *)PDC_malloc(sizeof(turbo_compress_params_t));
    PDCtf_copy_tf_region_t(&input_region, &out_params->decompressed_region);
    SET_FUNC_PARAMS("turbo_compress", PDC_TF_CPU_DEVICE, out_params, sizeof(turbo_compress_params_t));

    LOG_DEBUG("Turbo compression succeeded: %zu bytes\n", ret);
    return true;
}

bool
pdc_tf_builtin_turbo_decompress(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                                pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_turbo_decompress called\n");
    PDCtf_log_pdc_region_t(input_region);

    turbo_compress_params_t *in_params      = NULL;
    uint64_t                 in_params_size = 0;
    GET_FUNC_PARAMS("turbo_compress", PDC_TF_CPU_DEVICE, (void **)&in_params, &in_params_size);

    size_t size         = PDCtf_get_pdc_region_t_bytes(in_params->decompressed_region);
    void * decompressed = malloc(size);
    size_t ret;

    switch (in_params->decompressed_region.pdc_var_type) {
        case PDC_INT:
        case PDC_INT32:
            ret = p4ndec32((unsigned char *)*region_data,
                           PDCtf_get_pdc_region_t_elements(in_params->decompressed_region),
                           (uint32_t *)decompressed);
            break;
        case PDC_INT64:
            ret = p4ndec64((unsigned char *)*region_data,
                           PDCtf_get_pdc_region_t_elements(in_params->decompressed_region),
                           (uint64_t *)decompressed);
            break;
        default:
            LOG_ERROR("Invalid element type\n");
            return false;
    }

    *region_data = decompressed;
    PDCtf_copy_tf_region_t(&in_params->decompressed_region, output_region);

    LOG_DEBUG("Turbo decompression succeeded: %zu bytes\n", ret);
    return true;
}

#endif // ENABLE_TF_TURBO_COMPRESSION