#include "bulki.h"

BULKI *
pdc_serde_init(int initial_field_count)
{
    BULKI *data             = malloc(sizeof(BULKI));
    data->numKeys           = initial_field_count;
    data->header            = malloc(sizeof(BULKI_Header));
    data->header->keys      = malloc(sizeof(BULKI_Key) * initial_field_count);
    data->header->totalSize = 0;
    data->data              = malloc(sizeof(BULKI_Data));
    data->data->values      = malloc(sizeof(BULKI_Value) * initial_field_count);
    data->data->totalSize   = 0;
    return data;
}

void
pdc_serde_append_key_value(BULKI *data, BULKI_Key *key, BULKI_Value *value)
{
    data->header->keys[data->numKeys] = *key;
    // append bytes for type, size, and key
    data->header->totalSize += (sizeof(uint8_t) + sizeof(uint64_t) + key->size);

    data->data->values[data->numValues] = *value;
    // append bytes for class, type, size, and data
    data->data->totalSize += (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint64_t) + value->size);

    data->numKeys++;
    data->totalSize = data->header->totalSize + data->data->totalSize + sizeof(uint64_t) * 6;
}

BULKI_Key *
BULKI_KEY(void *key, pdc_c_var_type_t pdc_type, uint64_t size)
{
    BULKI_Key *pdc_key  = (BULKI_Key *)malloc(sizeof(BULKI_Key));
    uint64_t   key_size = (uint64_t)get_size_by_class_n_type(key, size, PDC_CLS_SCALAR, pdc_type);
    pdc_key->key        = malloc(key_size);
    memcpy(pdc_key->key, key, key_size);
    pdc_key->pdc_type = pdc_type;
    pdc_key->size     = key_size;
    return pdc_key;
}

BULKI_Value *
BULKI_VALUE(void *data, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class, uint64_t size)
{
    BULKI_Value *pdc_value  = (BULKI_Value *)malloc(sizeof(BULKI_Value));
    size_t       value_size = size;
    if (pdc_class == PDC_CLS_STRUCT) {
        // we are postponing the serialization of the embedded SERDE_SerializedData, so no need to check the
        // type here.
        BULKI *struct_data = (BULKI *)data;
        value_size         = struct_data->totalSize;
        pdc_value->data    = data;
    }
    else if (pdc_class <= PDC_CLS_ARRAY) {
        value_size      = (size_t)get_size_by_class_n_type(data, size, pdc_class, pdc_type);
        pdc_value->data = malloc(value_size);
        memcpy(pdc_value->data, data, value_size);
    }
    else {
        printf("Error: unsupported class %d\n", pdc_class);
        return NULL;
    }
    pdc_value->pdc_class = pdc_class;
    pdc_value->pdc_type  = pdc_type;
    pdc_value->size      = value_size;
    return pdc_value;
}

void
pdc_serde_free(BULKI *data)
{
    for (size_t i = 0; i < data->header->numKeys; i++) {
        free(data->header->keys[i].key);
    }
    free(data->header->keys);
    for (size_t i = 0; i < data->data->numValues; i++) {
        free(data->data->values[i].data);
    }
    free(data->data->values);
    free(data->header);
    free(data->data);
    free(data);
}

void
pdc_serde_print(BULKI *data)
{
    printf("Header:\n");
    printf("  numKeys: %zu\n", data->header->numKeys);
    printf("  totalSize: %zu\n", data->header->totalSize);
    for (size_t i = 0; i < data->header->numKeys; i++) {
        printf("  key %ld:\n", i);
        printf("    type: %d\n", data->header->keys[i].pdc_type);
        printf("    size: %zu\n", data->header->keys[i].size);
        printf("    key: %s\n", (char *)data->header->keys[i].key);
    }
    printf("Data:\n");
    printf("  numValues: %zu\n", data->data->numValues);
    printf("  totalSize: %zu\n", data->data->totalSize);
    for (size_t i = 0; i < data->data->numValues; i++) {
        printf("  value %ld:\n", i);
        printf("    class: %d\n", data->data->values[i].pdc_class);
        printf("    type: %d\n", data->data->values[i].pdc_type);
        printf("    size: %zu\n", data->data->values[i].size);
        printf("    data: ");
        if (data->data->values[i].pdc_type == PDC_STRING) {
            printf("%s\n", (char *)data->data->values[i].data);
        }
        else {
            printf("\n");
        }
    }
}