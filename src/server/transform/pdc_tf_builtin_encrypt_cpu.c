#include <stdint.h>

#include "pdc_tf_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_tf_common.h"
#include "pdc_tf_user.h"
#include "pdc_logger.h"

#ifdef ENABLE_TF_SECRET_BOX_ENCRYPTION
#include <sodium.h>

unsigned char key[crypto_secretbox_KEYBYTES]     = {0};
unsigned char nonce[crypto_secretbox_NONCEBYTES] = {0};

typedef struct encrypt_params_t {
    pdc_tf_region_t unencrypted_region;
} encrypt_params_t;

bool
pdc_tf_builtin_encrypt(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                       pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_encrypt called\n");
    PDCtf_log_pdc_region_t(input_region);

    size_t         plaintext_len  = PDCtf_get_pdc_region_t_bytes(input_region);
    size_t         ciphertext_len = plaintext_len + crypto_secretbox_MACBYTES;
    unsigned char *ciphertext     = malloc(ciphertext_len);
    if (!ciphertext) {
        LOG_ERROR("Failed to allocate ciphertext buffer\n");
        return false;
    }

    if (crypto_secretbox_easy(ciphertext, (unsigned char *)*region_data, plaintext_len, nonce, key) != 0) {
        LOG_ERROR("Encryption failed\n");
        free(ciphertext);
        return false;
    }

    output_region->ndim         = 1;
    output_region->pdc_var_type = PDC_CHAR;
    output_region->size[0]      = ciphertext_len;

    encrypt_params_t *out_params = (encrypt_params_t *)malloc(sizeof(encrypt_params_t));
    PDCtf_copy_tf_region_t(&input_region, &out_params->unencrypted_region);
    SET_FUNC_PARAMS("secret_box_encrypt", PDC_TF_CPU_DEVICE, out_params, sizeof(encrypt_params_t));

    *region_data = ciphertext;
    LOG_DEBUG("Encryption succeeded, ciphertext length: %zu bytes\n", ciphertext_len);
    return true;
}

bool
pdc_tf_builtin_decrypt(pdc_tf_internal_param *internal_param, char *params_str, void **region_data,
                       pdc_tf_region_t input_region, pdc_tf_region_t *output_region)
{
    LOG_DEBUG("pdc_tf_builtin_decrypt called\n");
    PDCtf_log_pdc_region_t(input_region);

    size_t ciphertext_len = PDCtf_get_pdc_region_t_bytes(input_region);

    encrypt_params_t *in_params;
    uint64_t          in_params_size;
    GET_FUNC_PARAMS("secret_box_encrypt", PDC_TF_CPU_DEVICE, (void **)&in_params, &in_params_size);

    if (ciphertext_len < crypto_secretbox_MACBYTES) {
        LOG_ERROR("Ciphertext too short\n");
        return false;
    }

    size_t plaintext_len = PDC_get_region_desc_size_bytes(
        in_params->unencrypted_region.size, PDC_get_var_type_size(in_params->unencrypted_region.pdc_var_type),
        in_params->unencrypted_region.ndim);

    unsigned char *plaintext = malloc(plaintext_len);
    if (!plaintext) {
        LOG_ERROR("Failed to allocate plaintext buffer\n");
        return false;
    }

    if (crypto_secretbox_open_easy(plaintext, (unsigned char *)*region_data, ciphertext_len, nonce, key) !=
        0) {
        LOG_ERROR("Decryption failed or ciphertext tampered\n");
        free(plaintext);
        return false;
    }

    PDCtf_copy_tf_region_t(&in_params->unencrypted_region, output_region);
    *region_data = plaintext;

    LOG_DEBUG("Decryption succeeded, plaintext length: %zu bytes\n", plaintext_len);
    return true;
}

#endif // ENABLE_TF_SECRET_BOX_ENCRYPTION