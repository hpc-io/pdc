#ifndef IDIOMS_LOCAL_INDEX_H
#define IDIOMS_LOCAL_INDEX_H

#include "pdc_public.h"
#include "pdc_config.h"
#include "query_utils.h"
#include "timer_utils.h"
#include "pdc_generic.h"
#include "art.h"
#include "pdc_set.h"
#include "pdc_hash.h"
#include "pdc_compare.h"
#include "dart_core.h"
// #include "pdc_hash_table.h"
#include "bulki_serde.h"
#include "rbtree.h"

/**
 * 2024-03-07: TODO items
 * 1. Debugging the Index persistence mechanism
 * 2. Make sure every IDIOMS API returns a struct that contains the number of index items.
 */

typedef struct {
    art_tree *art_key_prefix_tree_g;
    art_tree *art_key_suffix_tree_g;
    DART *    dart_info_g;
    uint32_t  server_id_g;
    uint32_t  num_servers_g;
    int64_t   index_record_count_g;
    int64_t   search_request_count_g;
    int64_t   insert_request_count_g;
    int64_t   delete_request_count_g;
    double    time_to_create_index_g;
    double    time_to_search_index_g;
    double    time_to_delete_index_g;
} IDIOMS_t;

typedef struct {
    char *           key;
    int8_t           is_key_suffix;
    uint64_t         virtual_node_id;
    pdc_c_var_type_t type;
    // int              simple_value_type; // 0: uint64_t, 1: int64_t, 2: double, 3: char*
    void *    value;
    size_t    value_len;
    uint64_t *obj_ids;
    size_t    num_obj_ids;
    size_t    key_offset;
    size_t    value_offset;
    uint32_t  src_client_id;
} IDIOMS_md_idx_record_t;

typedef struct {
    uint64_t virtural_node_id;
    size_t   indexed_item_count;
    // pdc_c_var_type_t type;
    // int simple_value_type; // 0: uint64_t, 1: int64_t, 2: double, 3: char*
    // Also, for key lookup ART, we also maintain the pointer to the value tree
    art_tree *primary_trie;
    art_tree *secondary_trie;
    rbt_t *   primary_rbt;
    rbt_t *   secondary_rbt;
    uint8_t   val_idx_dtype; // 0: uint64_t, 1: int64_t, 2: double
} key_index_leaf_content_t;

typedef struct {
    Set *  obj_id_set;
    size_t indexed_item_count;
} value_index_leaf_content_t;

typedef struct {
    void *    value;
    size_t    value_len;
    uint64_t *obj_ids;
    size_t    num_obj_ids;
} value_index_record_t;

typedef struct {
    value_index_record_t *value_idx_record;
    uint64_t              num_value_idx_record;
    char *                key;
    uint64_t              virtual_node_id;
} key_index_record_t;

typedef struct {
    void * buffer;
    size_t buffer_size;
    size_t buffer_capacity;
    size_t num_keys;
} index_buffer_t;

static void
_init_dart_space_via_idioms(DART *dart, int num_server)
{
    dart_space_init(dart, num_server);
}

static void
_encodeTypeToBitmap(uint8_t *bitmap, enum pdc_c_var_type_t type)
{
    if (bitmap == NULL) {
        return;
    }
    if (type >= PDC_STRING) {                      // Non-numerical types
        *bitmap |= ((type - PDC_STRING + 1) << 4); // Shift by 4 to set in the higher 4 bits
    }
    else {                        // Numerical types
        *bitmap |= (type & 0x0F); // Ensure only lower 4 bits are used for numerical types
    }
}

// Function to get numerical type from the bitmap
static enum pdc_c_var_type_t
_getNumericalTypeFromBitmap(uint8_t bitmap)
{
    return (enum pdc_c_var_type_t)(bitmap & 0x0F); // Extract lower 4 bits
}

// Function to get string (non-numerical) type from the bitmap
static enum pdc_c_var_type_t
_getCompoundTypeFromBitmap(uint8_t bitmap)
{
    return (enum pdc_c_var_type_t)(((bitmap >> 4) & 0x0F) + PDC_STRING -
                                   1); // Extract higher 4 bits and adjust index
}

/**
 * @brief Initialize the ART root index
 */
IDIOMS_t *IDIOMS_init(uint32_t server_id, uint32_t num_servers);

/**
 * @brief Create local index with the information in idx_record.
 * @param idx_record  Only the 'key', 'type' and 'value' and 'value_size' fields are used.
 * @return perr_t SUCCESS on success, FAIL on failure
 */
perr_t idioms_local_index_create(IDIOMS_t *idioms, IDIOMS_md_idx_record_t *idx_record);

/**
 * @brief Delete the local index with the information in idx_record.
 * @param idx_record  Only the 'key', 'type' and 'value' and 'value_size' fields are used.
 * @return perr_t SUCCESS on success, FAIL on failure
 */
perr_t idioms_local_index_delete(IDIOMS_t *idioms, IDIOMS_md_idx_record_t *idx_record);

/**
 * @brief Search the ART index for the given key.
 * @param idx_record  'key', 'type' and 'value' and 'value_size' fields are used for input.
 * @return the number of object IDs found.  The object IDs are stored in the 'obj_id' field of the idx_record.
 */
uint64_t idioms_local_index_search(IDIOMS_t *idioms, IDIOMS_md_idx_record_t *idx_record);

#endif // IDIOMS_LOCAL_INDEX_H
