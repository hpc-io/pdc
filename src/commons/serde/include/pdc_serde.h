#ifndef PDC_SERDE_H
#define PDC_SERDE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pdc_generic.h"

#define MAX_KEYS        10
#define MAX_BUFFER_SIZE 1000

typedef struct {
    PDC_CType type; /**< Data type of the key */
    size_t    size; /**< Size of the key */
    void *    key;  /**< Pointer to the key data */
} PDC_SERDE_Key;

typedef struct {
    PDC_CTypeClass class; /**< Class of the value */
    PDC_CType type;       /**< Data type of the value */
    size_t    size;       // size of the data. If a string, it is strlen(data) + 1;
                          // if an array, it is the number of elements;
                          // if a struct, it is the totalSize of the data chunk of the struct, etc.
    void *data;           /**< Pointer to the value data */
} PDC_SERDE_Value;

typedef struct {
    PDC_SERDE_Key *keys;      /**< Array of keys */
    size_t         numKeys;   /**< Number of keys */
    size_t         totalSize; /**< Total size of the header */
} PDC_SERDE_Header;

typedef struct {
    size_t           numValues; /**< Number of values */
    PDC_SERDE_Value *values;    /**< Array of values */
    size_t           totalSize; /**< Total size of the data */
} PDC_SERDE_Data;

typedef struct {
    PDC_SERDE_Header *header;    /**< Pointer to the header */
    PDC_SERDE_Data *  data;      /**< Pointer to the data */
    size_t            totalSize; /**< Total size of the serialized data */
} PDC_SERDE_SerializedData;

/**
 * @brief Create a PDC_SERDE_Key structure
 *
 * @param key Pointer to the key data
 * @param type Data type of the key
 * @param size Size of the key data
 *
 * @return Pointer to the created PDC_SERDE_Key structure
 */
inline PDC_SERDE_Key *
PDC_SERDE_KEY(void *key, PDC_CType type, size_t size)
{
    PDC_SERDE_Key *pdc_key = malloc(sizeof(PDC_SERDE_Key));
    if (type == PDC_CT_STRING) {
        size = (strlen((char *)key) + 1) * sizeof(char);
    }
    pdc_key->key = malloc(size);
    memcpy(pdc_key->key, key, size);
    pdc_key->type = type;
    pdc_key->size = size;
    return pdc_key;
}

/**
 * @brief Create a PDC_SERDE_Value structure
 *
 * @param data Pointer to the value data
 * @param type Data type of the value
 * @param class Class of the value
 * @param size Size of the value data
 *
 * @return Pointer to the created PDC_SERDE_Value structure
 */
inline PDC_SERDE_Value *
PDC_SERDE_VALUE(void *data, PDC_CType type, PDC_CTypeClass class, size_t size)
{
    PDC_SERDE_Value *pdc_value = malloc(sizeof(PDC_SERDE_Value));
    if (class == PDC_CT_STRING) {
        size = (strlen((char *)data) + 1) * sizeof(char);
    }
    if (class == PDC_CT_CLS_ARRAY) {
        size = size * get_size_by_dtype(type);
    }
    if (class == PDC_CT_CLS_STRUCT) {
        PDC_SERDE_SerializedData *struct_data = (PDC_SERDE_SerializedData *)data;
        size                                  = struct_data->totalSize;
    }
    pdc_value->data = malloc(size);
    memcpy(pdc_value->data, data, size);
    pdc_value->class = class;
    pdc_value->type  = type;
    pdc_value->size  = size;
    return pdc_value;
}

/**
 * @brief Initialize a serialized data structure
 *
 * @param initial_field_count Number of initial fields to allocate space for
 *
 * @return Pointer to the initialized PDC_SERDE_SerializedData structure
 */
PDC_SERDE_SerializedData *pdc_serde_init(int initial_field_count);

/**
 * @brief Append a key-value pair to the serialized data structure
 *
 * @param data Pointer to the PDC_SERDE_SerializedData structure
 * @param key Pointer to the PDC_SERDE_Key structure representing the key
 * @param value Pointer to the PDC_SERDE_Value structure representing the value
 */
void pdc_serde_append_key_value(PDC_SERDE_SerializedData *data, PDC_SERDE_Key *key, PDC_SERDE_Value *value);

/**
 * @brief Serialize the data in the serialized data structure and return the buffer
 *
 * @param data Pointer to the PDC_SERDE_SerializedData structure
 *
 * @return Pointer to the buffer containing the serialized data
 */
void *pdc_serde_serialize(PDC_SERDE_SerializedData *data);

/**
 * @brief Deserialize the buffer and return the deserialized data structure
 *
 * @param buffer Pointer to the buffer containing the serialized data
 *
 * @return Pointer to the deserialized PDC_SERDE_SerializedData structure
 */
PDC_SERDE_SerializedData *pdc_serde_deserialize(void *buffer);

/**
 * @brief Free the memory allocated for the serialized data structure
 *
 * @param data Pointer to the PDC_SERDE_SerializedData structure to be freed
 */
void pdc_serde_free(PDC_SERDE_SerializedData *data);

/**
 * @brief Print the contents of the serialized data structure
 *
 * @param data Pointer to the PDC_SERDE_SerializedData structure to be printed
 */
void pdc_serde_print(PDC_SERDE_SerializedData *data);

#endif /* PDC_SERDE_H */