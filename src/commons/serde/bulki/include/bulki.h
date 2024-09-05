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
    uint64_t          count;     /**< Number of elements in the array */
    uint64_t          size;      // size in byte of the data.
    void *            data;      /**< Pointer to the value data */
} BULKI_Entity;

typedef struct {
    BULKI_Entity *keys;       /**< Array of keys */
    uint64_t      headerSize; /**< Total bytes of the header region */
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

typedef struct {
    BULKI_Entity *entity_array;   // Points to the array being iterated
    uint64_t      current_idx;    // Current index in the array
    uint64_t      total_size;     // Total number of elements in the array
    void *        bent_filter;    // Optional filter to apply during iteration, it can be NULL/an insteance of
                                  // BULKI/an instance of BULKI_Entity
    pdc_c_var_type_t filter_type; // The type of the filter
} BULKI_Entity_Iterator;

typedef struct {
    BULKI_Entity key;
    BULKI_Entity value;
} BULKI_KV_Pair;

typedef struct {
    BULKI *  bulki;
    uint64_t current_idx;
    uint64_t total_size;
} BULKI_KV_Pair_Iterator;

/* ========================= BULKI_Entity starts here ============================= */

/* ================--------- BULKI_Entity creation APIs -----------================ */

/**
 * @brief Create a BULKI_Entity structure with data of base type.
 * It is okay to change the content of `data` after this call if `data` is a BULKI or BULKI_Entity structure.
 * But to make sure you won't change teh content of `data` or the content of the result BULKI_Entity structure
 * after any `BULKI_put` call or `BULKI_ENTITY_append_*` call.
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
 * @brief create an empty BULKI_Entity structure, usually used as a wrapper for BULKI_Entity structure. Since
 * this will be used as a wrapper, we consider this to be an array of BULKI_Entity.
 * @return Pointer to the created BULKI_Entity structure
 */
BULKI_Entity *empty_Bent_Array_Entity();

/**
 * @brief create an empty BULKI structure, usually used as a wrapper for a BULKI
 * @return Pointer to the created BULKI_Entity structure
 */
BULKI_Entity *empty_BULKI_Array_Entity();

/**
 * @brief Create a BULKI_Entity structure with data of base type, which is a wrapper of BULKI_ENTITY((void
 * *)single_data, 1, pdc_type, PDC_CLS_ITEM)
 * @param data Pointer to the Entity data
 * @param pdc_type Data type of the data
 * @return Pointer to the created BULKI_Entity structure
 */
BULKI_Entity *BULKI_singleton_ENTITY(void *data, pdc_c_var_type_t pdc_type);

/**
 * @brief Create a BULKI_Entity structure with data of base type, and the count of elements in the array,
 * which is a wrapper of BULKI_ENTITY((void*)array_data, array_len, pdc_type, PDC_CLS_ARRAY)
 * @param data Pointer to the Entity data
 * @param count Number of elements in the array
 * @param pdc_type Data type of each element in the array
 * @return Pointer to the created BULKI_Entity structure
 */
BULKI_Entity *BULKI_array_ENTITY(void *data, uint64_t count, pdc_c_var_type_t pdc_type);

/* ================--------- BULKI_Entity Data Manipulation APIs -----------================ */

/**
 * @brief Append a BULKI_Entity structure to the BULKI_Entity structure
 * You need to make sure the content the src structure is the final version before calling this function.
 * Any change to the content of src after calling this function will not be reflected in the serialized data
 * structure.
 * If you need to change the content of src after calling this
 * function, you need to iterate through all the BULKI_Entity structure in the dest structure and update them.
 * We do not recommend this because we do not assume the BULKI_Entity elements in the internal array would be
 * unique to each other.
 * @param bulki_entity Pointer to the BULKI_Entity structure
 * @param ent Pointer to the BULKI_Entity structure
 * @return Pointer to the BULKI_Entity structure
 */
BULKI_Entity *BULKI_ENTITY_append_BULKI_Entity(BULKI_Entity *dest, BULKI_Entity *src);

/**
 * @brief Append a BULKI structure to the BULKI_Entity structure that are returned by
 * `empty_BULKI_Array_Entity` call. You need to make sure the content the src structure is the final version
 * before calling this function. Any change to the content of src after calling this function will not be
 * reflected in the serialized data structure. If you need to change the content of src after calling this
 * function, you need to iterate through all the BULKI structure in the dest structure and update them. We do
 * not recommend this because we do not assume the BULKI elements in the internal array would be unique to
 * each other.
 * @param bulki_entity Pointer to the BULKI_Entity structure
 * @param bulki Pointer to the BULKI structure
 * @return Pointer to the BULKI_Entity structure
 */
BULKI_Entity *BULKI_ENTITY_append_BULKI(BULKI_Entity *dest, BULKI *src);

/* ================--------- BULKI_Entity Data Retrieval APIs -----------================ */

/**
 * @brief Get the BULKI_Entity structure from the BULKI_Entity array by the given index.
 * @param bulk_entity Pointer to the BULKI_Entity structure
 * @param idx Index of the BULKI_Entity structure to get
 *
 * @return Pointer to the BULKI_Entity structure
 */
BULKI_Entity *BULKI_ENTITY_get_BULKI_Entity(BULKI_Entity *bulk_entity, size_t idx);

/**
 * @brief Get the BULKI structure from the BULKI_Entity array by the given index.
 * @param bulki_entity Pointer to the BULKI_Entity structure
 * @param idx Index of the BULKI structure to get
 *
 * @return Pointer to the BULKI structure
 */
BULKI *BULKI_ENTITY_get_BULKI(BULKI_Entity *bulki_entity, size_t idx);

/* ================--------- BULKI_Entity Iterator APIs -----------================ */

/**
 * @brief Initialize the iterator with an optional BULKI_Entity as a filter
 * @param entity_array Pointer to the BULKI_Entity array
 * @param filter This can be NULL/an instance of BULKI/an instance of BULKI_Entity
 * @param filter_type The type of the filter, must be PDC_BULKI, PDC_BULKI_ENTITY
 * @return Pointer to the Bent_Iterator structure
 */
BULKI_Entity_Iterator *Bent_iterator_init(BULKI_Entity *array, void *filter, pdc_c_var_type_t filter_type);

/**
 * @brief Test if the iterator has more BULKI structures to return
 * @param iter Pointer to the Bent_Iterator structure
 * @return 1 if there are more BULKI structures to return, 0 otherwise
 */
int Bent_iterator_has_next_BULKI(BULKI_Entity_Iterator *it);

/**
 * @brief Get the next BULKI from the iterator
 * @param iter Pointer to the Bent_Iterator structure
 * @return Pointer to the next BULKI structure
 */
BULKI *Bent_iterator_next_BULKI(BULKI_Entity_Iterator *it);

/**
 * @brief Test if the iterator has more BULKI_Entity structures to return
 * @param iter Pointer to the Bent_Iterator structure
 * @return 1 if there are more BULKI_Entity structures to return, 0 otherwise
 */
int Bent_iterator_has_next_Bent(BULKI_Entity_Iterator *it);

/**
 * @brief Get the next BULKI_Entity from the iterator
 * @param iter Pointer to the Bent_Iterator structure
 * @return Pointer to the next BULKI_Entity structure
 */
BULKI_Entity *Bent_iterator_next_Bent(BULKI_Entity_Iterator *it);

/* ================--------- BULKI_Entity Utility APIs -----------================ */

/**
 * @brief Compare two BULKI_Entity structures for equality
 * @param be1 Pointer to the first BULKI_Entity structure
 * @param be2 Pointer to the second BULKI_Entity structure
 * @return 1 if the two structures are equal, 0 otherwise
 */
int BULKI_Entity_equal(BULKI_Entity *be1, BULKI_Entity *be2);

/**
 * @brief Print the contents of BULKI or BULKI_Entity structure.
 * @param data Pointer to the BULKI structure
 */
void BULKI_Entity_print(BULKI_Entity *bulk_entity);

/**
 * @brief get the total size of BULKI_Entity structure.
 *
 * @param data Pointer to the BULKI_Entity structure
 * @return size_t
 */
size_t get_BULKI_Entity_size(BULKI_Entity *bulk_entity);

/* ================--------- BULKI_Entity Destruction APIs -----------================ */

/**
 * @brief free the memory allocated for the BULKI_Entity structure
 *
 * @param bulk_entity Pointer to the BULKI_Entity structure to be freed
 * @param free_struct If 1, free the BULKI_Entity structure itself
 * @return void
 */
void BULKI_Entity_free(BULKI_Entity *bulk_entity, int free_struct);

/* ========================= BULKI starts here ============================= */

/* ================--------- BULKI Creation APIs -----------================ */

/**
 * @brief Initialize a Bulki data structure
 *
 * @param initial_field_count Number of initial fields to allocate space for
 *
 * @return Pointer to the initialized BULKI structure
 */
BULKI *BULKI_init(int initial_field_count);

/* ================--------- BULKI Data Manipulation APIs -----------================ */

/**
 * @brief Put a key-value pair to the serialized data structure. If the key already exists, update the value.
 * You need to make sure the content in both key and value are the final version before calling this function.
 * Any change to the key or value after calling this function will not be reflected in the serialized data
 * structure.
 * If you really have to change the content of key or value after calling this function, you need to call
 * BULKI_put again to update the serialized data structure.
 * @param data Pointer to the BULKI structure
 * @param key Pointer to the BULKI_Entity structure representing the key
 * @param value Pointer to the BULKI_Entity structure representing the value
 */
void BULKI_put(BULKI *bulki, BULKI_Entity *key, BULKI_Entity *value);

/**
 * @brief Delete a key-value pair from the serialized data structure
 *
 * @param data Pointer to the BULKI structure
 * @param key Pointer to the BULKI_Entity structure representing the key
 * @return the deleted BULKI_Entity value. If the key is not found, return NULL.
 */
BULKI_Entity *BULKI_delete(BULKI *bulki, BULKI_Entity *key);

/* ================--------- BULKI Data Retrieval APIs -----------================ */

/**
 * @brief Get the value of a key from the serialized data structure
 * @param data Pointer to the BULKI structure
 * @param key Pointer to the BULKI_Entity structure representing the key
 * @return Pointer to the BULKI_Entity structure representing the value
 */
BULKI_Entity *BULKI_get(BULKI *bulki, BULKI_Entity *key);

/* ================--------- BULKI Iterator APIs -----------================ */

/**
 * @brief Initialize the iterator with a BULKI structure
 * @param bulki Pointer to the BULKI structure
 * @return Pointer to the BULKI_KV_Pair_Iterator structure
 */
BULKI_KV_Pair_Iterator *BULKI_KV_Pair_iterator_init(BULKI *bulki);

/**
 * @brief Test if the iterator has more BULKI_KV_Pair structures to return
 * @param iter Pointer to the BULKI_KV_Pair_Iterator structure
 * @return 1 if there are more BULKI_KV_Pair structures to return, 0 otherwise
 */
int BULKI_KV_Pair_iterator_has_next(BULKI_KV_Pair_Iterator *it);

/**
 * @brief Get the next BULKI_KV_Pair from the iterator
 * @param iter Pointer to the BULKI_KV_Pair_Iterator structure
 * @return Pointer to the next BULKI_KV_Pair structure
 */
BULKI_KV_Pair *BULKI_KV_Pair_iterator_next(BULKI_KV_Pair_Iterator *it);

/* ================--------- BULKI Utility APIs -----------================ */

/**
 * @brief Compare two BULKI structures for equality
 * @param bulki1 Pointer to the first BULKI structure
 * @param bulki2 Pointer to the second BULKI structure
 * @return 1 if the two structures are equal, 0 otherwise
 */
int BULKI_equal(BULKI *bulki1, BULKI *bulki2);

/**
 * @brief Print the contents of BULKI or BULKI_Entity structure.
 * @param data Pointer to the BULKI structure
 */
void BULKI_print(BULKI *bulki);

/**
 * @brief get the total size of BULKI or BULKI_Entity structure.
 *
 * @param data Pointer to the BULKI structure
 * @return size_t
 */
size_t get_BULKI_size(BULKI *bulki);

/* ================--------- BULKI Destruction APIs -----------================ */

/**
 * @brief free the memory allocated for the BULKI structure
 *
 * @param bulki Pointer to the BULKI structure to be freed
 * @param free_struct If 1, free the BULKI structure itself
 * @return void
 */
void BULKI_free(BULKI *bulki, int free_struct);

#endif /* BULKI_H */