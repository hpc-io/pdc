#ifndef BULKI_H
#define BULKI_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include "pdc_generic.h"

/**
 * @brief BULKI_Entity structure
 *
 * pdc_class can be either SCALAR or ARRAY, or STRUCT.
 * For SCALAR, the data is a single value of the type specified by pdc_type.
 * For ARRAY, the data is an array of values of the type specified by pdc_type.
 * For STRUCT, the data is a pointer to a BULKI structure, which is a collection of key-value pairs.
 */
typedef struct {
    pdc_c_var_class_t pdc_class; /**< Class of the value */
    pdc_c_var_type_t  pdc_type;  /**< Data type of the value */
    size_t            count;     /**< Number of elements in the array */
    size_t            size;      // size in byte of the data.
    void *            data;      /**< Pointer to the value data */
} BULKI_Entity;

typedef struct {
    BULKI_Entity *keys;       /**< Array of keys */
    size_t        headerSize; /**< Total bytes of the header region */
} BULKI_Header;

typedef struct {
    BULKI_Entity *values;   /**< Array of values */
    uint64_t      dataSize; /**< Total bytes of the data region */
} BULKI_Data;

typedef struct {
    BULKI_Header *header;    /**< Pointer to the header */
    BULKI_Data *  data;      /**< Pointer to the data */
    uint64_t      totalSize; /**< Total size of the serialized data */
    uint64_t      numKeys;   /**< Actual Number of keys in the header*/
    uint64_t capacity; /**< The predefined number of keys in Bulki. If numKeys >= capacity, array expansion is
                          needed  */
} BULKI;

/**
 * @brief Append a BULKI structure to the BULKI_Entity structure
 * @param bulki_entity Pointer to the BULKI_Entity structure
 * @param bulki Pointer to the BULKI structure
 * @return Pointer to the BULKI_Entity structure
 */
BULKI_Entity *BULKI_ENTITY_append_BULKI(BULKI_Entity *dest, BULKI *src);

/**
 * @brief Append a BULKI_Entity structure to the BULKI_Entity structure
 * @param bulki_entity Pointer to the BULKI_Entity structure
 * @param ent Pointer to the BULKI_Entity structure
 * @return Pointer to the BULKI_Entity structure
 */
BULKI_Entity *BULKI_ENTITY_append_BULKI_Entity(BULKI_Entity *dest, BULKI_Entity *src);

/**
 * @brief Create a BULKI_Entity structure with data of base type.
 *
 * @param data Pointer to the Entity data
 * @param count Number of elements in the array
 * @param pdc_type Data type of each element in the array
 * @param pdc_class Class of the each element in the array
 *
 * @return Pointer to the created BULKI_Entity structure
 */
BULKI_Entity *BULKI_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type,
                           pdc_c_var_class_t pdc_class);

/**
 * @brief Initialize a Bulki data structure
 *
 * @param initial_field_count Number of initial fields to allocate space for
 *
 * @return Pointer to the initialized BULKI structure
 */
BULKI *BULKI_init(int initial_field_count);

/**
 * @brief Compare two BULKI_Entity structures for equality
 * @param be1 Pointer to the first BULKI_Entity structure
 * @param be2 Pointer to the second BULKI_Entity structure
 * @return 1 if the two structures are equal, 0 otherwise
 */
int BULKI_Entity_equal(BULKI_Entity *be1, BULKI_Entity *be2);

/**
 * @brief Append a key-value pair to the serialized data structure
 *
 * @param data Pointer to the BULKI structure
 * @param key Pointer to the BULKI_Entity structure representing the key
 * @param value Pointer to the BULKI_Entity structure representing the value
 */
void BULKI_add(BULKI *bulki, BULKI_Entity *key, BULKI_Entity *value);

/**
 * @brief Delete a key-value pair from the serialized data structure
 *
 * @param data Pointer to the BULKI structure
 * @param key Pointer to the BULKI_Entity structure representing the key
 * @return the deleted BULKI_Entity value. If the key is not found, return NULL.
 */
BULKI_Entity *BULKI_delete(BULKI *bulki, BULKI_Entity *key);

/**
 * @brief Print the contents of BULKI or BULKI_Entity structure.
 * @param data Pointer to the BULKI structure
 */
void BULKI_print(BULKI *bulki);

/**
 * @brief Print the contents of BULKI or BULKI_Entity structure.
 * @param data Pointer to the BULKI structure
 */
void BULKI_Entity_print(BULKI_Entity *bulk_entity);

/**
 * @brief get the total size of BULKI or BULKI_Entity structure.
 *
 * @param data Pointer to the BULKI structure
 * @return size_t
 */
size_t get_BULKI_size(BULKI *bulki);

/**
 * @brief get the total size of BULKI_Entity structure.
 *
 * @param data Pointer to the BULKI_Entity structure
 * @return size_t
 */
size_t get_BULKI_Entity_size(BULKI_Entity *bulk_entity);

/**
 * @brief free the memory allocated for the BULKI_Entity structure
 *
 * @param bulk_entity Pointer to the BULKI_Entity structure to be freed
 * @param free_struct If 1, free the BULKI_Entity structure itself
 * @return void
 */
void BULKI_Entity_free(BULKI_Entity *bulk_entity, int free_struct);

/**
 * @brief free the memory allocated for the BULKI structure
 *
 * @param bulki Pointer to the BULKI structure to be freed
 * @param free_struct If 1, free the BULKI structure itself
 * @return void
 */
void BULKI_free(BULKI *bulki, int free_struct);

#endif /* BULKI_H */