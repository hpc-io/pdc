#include "pdc_generic.h"

size_t
get_number_from_string(char *str, pdc_c_var_type_t type, void **val_ptr)
{
    if (val_ptr == NULL) {
        return 0;
    }

    void * k       = NULL;
    size_t key_len = get_size_by_dtype(type);

    k = malloc(key_len);

    switch (type) {
        case PDC_SHORT:
            *((short *)k) = (short)strtol(str, NULL, 10);
            break;
        case PDC_INT:
        case PDC_INT32:
            *((int *)k) = (int)strtol(str, NULL, 10);
            break;
        case PDC_UINT:
        case PDC_UINT32:
            *((unsigned int *)k) = (unsigned int)strtoul(str, NULL, 10);
            break;
        case PDC_LONG:
            *((long *)k) = strtol(str, NULL, 10);
            break;
        case PDC_INT8:
            *((int8_t *)k) = (int8_t)strtol(str, NULL, 10);
            break;
        case PDC_UINT8:
            *((uint8_t *)k) = (uint8_t)strtoul(str, NULL, 10);
            break;
        case PDC_INT16:
            *((int16_t *)k) = (int16_t)strtol(str, NULL, 10);
            break;
        case PDC_UINT16:
            *((uint16_t *)k) = (uint16_t)strtoul(str, NULL, 10);
            break;
        case PDC_INT64:
            *((int64_t *)k) = strtoll(str, NULL, 10);
            break;
        case PDC_UINT64:
            *((uint64_t *)k) = strtoull(str, NULL, 10);
            break;
        case PDC_FLOAT:
            *((float *)k) = strtof(str, NULL);
            break;
        case PDC_DOUBLE:
            *((double *)k) = strtod(str, NULL);
            break;
        default:
            free(k);
            return 0;
    }

    *val_ptr = k;
    return key_len;
}