#ifndef BULKI_H
#define BULKI_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include "pdc_generic.h"

typedef struct {
    pdc_c_var_type_t pdc_type; /**< Data type of the key */
    uint64_t         size;     /**< Size of the key */
    void *           key;      /**< Pointer to the key data */
} BULKI_Key;

typedef struct {
    pdc_c_var_class_t pdc_class; /**< Class of the value */
    pdc_c_var_type_t  pdc_type;  /**< Data type of the value */
    uint64_t          size;      // size of the data. If a string, it is strlen(data) + 1;
                                 // if an array, it is the number of elements;
                                 // if a struct, it is the totalSize of the data chunk of the struct, etc.
    void *data;                  /**< Pointer to the value data */
} BULKI_Value;

typedef struct {
    BULKI_Key *keys;      /**< Array of keys */
    size_t     totalSize; /**< Total size of the header */
} BULKI_Header;

typedef struct {
    BULKI_Value *values;    /**< Array of values */
    uint64_t     totalSize; /**< Total size of the data */
} BULKI_Data;

typedef struct {
    BULKI_Header *header;    /**< Pointer to the header */
    BULKI_Data *  data;      /**< Pointer to the data */
    uint64_t      totalSize; /**< Total size of the serialized data */
    uint64_t      numKeys;   /**< Number of keys */
} BULKI;

/**
 * @brief Initialize a serialized data structure
 *
 * @param initial_field_count Number of initial fields to allocate space for
 *
 * @return Pointer to the initialized BULKI structure
 */
BULKI *BULKI_serde_init(int initial_field_count);

/**
 * @brief Append a key-value pair to the serialized data structure
 *
 * @param data Pointer to the BULKI structure
 * @param key Pointer to the BULKI_Key structure representing the key
 * @param value Pointer to the BULKI_Value structure representing the value
 */
void BULKI_serde_append_key_value(BULKI *data, BULKI_Key *key, BULKI_Value *value);

/**
 * @brief Free the memory allocated for the serialized data structure
 *
 * @param data Pointer to the BULKI structure to be freed
 */
void BULKI_serde_free(BULKI *data);

/**
 * @brief Print the contents of the serialized data structure
 *
 * @param data Pointer to the BULKI structure to be printed
 */
void BULKI_serde_print(BULKI *data);

/**
 * @brief Create a BULKI_Key structure
 *
 * @param key Pointer to the key data
 * @param pdc_type Data type of the key. For BULKI_Key, we only support PDC_CLS_SCALAR class.
 * @param size Size of the key data
 *
 * @return Pointer to the created BULKI_Key structure
 */
BULKI_Key *BULKI_KEY(void *key, pdc_c_var_type_t pdc_type, uint64_t size);

/**
 * @brief Create a BULKI_Value structure
 *
 * @param data Pointer to the value data
 * @param pdc_type Data type of the value
 * @param pdc_class Class of the value
 * @param size Size of the value data.
 *        For scalar value, it is the result of sizeof(type) function;
 *        for array, it is the number of elements;
 *        for struct, it is the totalSize of the data chunk of the struct, etc.
 *
 * @return Pointer to the created BULKI_Value structure
 */
BULKI_Value *BULKI_VALUE(void *data, pdc_c_var_type_t pdc_type, pdc_c_var_class_t pdc_class, uint64_t size);

#endif /* BULKI_H */