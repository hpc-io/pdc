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

#define IDIOMS_DART_ALPHABET_SIZE        27
#define IDIOMS_MAX_SERVER_COUNT_TO_ADAPT 1024

typedef struct {
    int64_t   index_record_count_g;
    int64_t   search_request_count_g;
    int64_t   insert_request_count_g;
    int64_t   delete_request_count_g;
    double    time_to_create_index_g;
    double    time_to_search_index_g;
    double    time_to_delete_index_g;
    art_tree *art_key_prefix_tree_g;
    art_tree *art_key_suffix_tree_g;
    uint32_t  server_id_g;
    uint32_t  num_servers_g;
} IDIOMS_t;

typedef struct {
    char *           key;
    uint64_t         virtual_node_id;
    pdc_c_var_type_t type;
    int              simple_value_type; // 0: uint64_t, 1: int64_t, 2: double, 3: char*
    void *           value;
    size_t           value_len;
    uint64_t *       obj_ids;
    size_t           num_obj_ids;
} IDIOMS_md_idx_record_t;

typedef struct {
    uint64_t         virtural_node_id;
    pdc_c_var_type_t type;
    int              simple_value_type; // 0: uint64_t, 1: int64_t, 2: double, 3: char*
    // Also, for key lookup ART, we also maintain the pointer to the value tree
    art_tree *primary_trie;
    art_tree *secondary_trie;
    rbt_t *   primary_rbt;
    rbt_t *   secondary_rbt;
} key_index_leaf_content_t;

typedef struct {
    Set *obj_id_set;
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
} index_buffer_t;

/**
 * @brief Initialize the ART root index
 */
void IDIOMS_init(uint32_t server_id, uint32_t num_servers);

IDIOMS_t *get_idioms_g();

perr_t idioms_local_index_create(IDIOMS_md_idx_record_t *idx_record);

perr_t idioms_local_index_delete(IDIOMS_md_idx_record_t *idx_record);

/**
 * @brief Search the ART index for the given key
 * @param idioms
 * @param idx_record  Only the 'key', 'type' and 'value' and 'value_size' fields are used.
 *
 * @return the number of object IDs found.  The object IDs are stored in the 'obj_id' field of the idx_record.
 */
uint64_t idioms_local_index_search(IDIOMS_md_idx_record_t *idx_record);

perr_t idioms_metadata_index_dump(char *dir_path, uint32_t serverID);

perr_t idioms_metadata_index_recover(char *dir_path, int num_client, int num_server, uint32_t serverID);

void init_dart_space_via_idioms(DART *dart, int num_client, int num_server, int max_server_num_to_adapt);

#endif // IDIOMS_LOCAL_INDEX_H
