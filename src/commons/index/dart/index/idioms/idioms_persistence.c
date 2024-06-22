#include "idioms_persistence.h"
#include "comparators.h"
#include "rbtree.h"
#include "pdc_set.h"
#include "pdc_hash.h"
#include "bin_file_ops.h"
#include "pdc_hash_table.h"
#include "dart_core.h"
#include "string_utils.h"
#include "query_utils.h"
#include "pdc_logger.h"
#include <unistd.h>
#include <inttypes.h>
#include <stdint.h>
#include "bulki_serde.h"

/****************************/
/* Index Dump               */
/****************************/

// ********************* Index Dump and Load *********************

/**
 * This is a object ID set
 * |number of object IDs = n|object ID 1|...|object ID n|
 *
 * validated.
 */
uint64_t
append_obj_id_set(Set *obj_id_set, BULKI_Entity *id_set_entity)
{
    uint64_t    num_obj_id = set_num_entries(obj_id_set);
    SetIterator iter;
    set_iterate(obj_id_set, &iter);
    while (set_iter_has_more(&iter)) {
        uint64_t *    item      = (uint64_t *)set_iter_next(&iter);
        BULKI_Entity *id_entity = BULKI_ENTITY(item, 1, PDC_UINT64, PDC_CLS_ITEM);
        BULKI_ENTITY_append_BULKI_Entity(id_set_entity, id_entity);
    }
    return num_obj_id + 1;
}

int
append_value_tree_node(void *v_id_bulki, void *key, uint32_t key_size, pdc_c_var_type_t key_type, void *value)
{
    BULKI *bulki = (BULKI *)v_id_bulki;
    // entity for the key
    BULKI_Entity *key_entity =
        BULKI_ENTITY(key, key_type == PDC_STRING ? 1 : key_size, key_type, PDC_CLS_ITEM);

    BULKI_Entity *id_set_entity = BULKI_get(bulki, key_entity);

    if (id_set_entity == NULL) {
        id_set_entity = empty_Bent_Array_Entity();
    }

    value_index_leaf_content_t *value_index_leaf = (value_index_leaf_content_t *)(value);
    if (value_index_leaf != NULL) {
        Set *obj_id_set = (Set *)value_index_leaf->obj_id_set;
        append_obj_id_set(obj_id_set, id_set_entity);
    }

    BULKI_put(bulki, key_entity, id_set_entity);

    return 0; // return 0 for art iteration to continue;
}

/**
 * This is a string value node
 * |str_val|file_obj_pair_list|
 */
int
append_string_value_node(void *v_id_bulki, const unsigned char *key, uint32_t key_len, void *value)
{
    return append_value_tree_node(v_id_bulki, (void *)key, key_len, PDC_STRING, value);
}

rbt_walk_return_code_t
append_numeric_value_node(rbt_t *rbt, void *key, size_t klen, void *value, void *v_id_bulki)
{
    append_value_tree_node(v_id_bulki, key, klen, rbt_get_dtype(rbt), value);
    return RBT_WALK_CONTINUE;
}

/**
 * This is the string value region
 * |type = 3|number of values = n|value_node_1|...|value_node_n|
 *
 * return number of strings in the string value tree
 */
uint64_t
append_string_value_tree(art_tree *art, BULKI *v_id_bulki)
{
    uint64_t rst = art_iter(art, append_string_value_node, v_id_bulki);
    return rst;
}

/**
 * This is the numeric value region
 * |type = 0/1/2|number of values = n|value_node_1|...|value_node_n|
 *
 * return number of numeric values in the numeric value tree
 */
uint64_t
append_numeric_value_tree(rbt_t *rbt, BULKI *v_id_bulki)
{
    uint64_t rst = rbt_walk(rbt, append_numeric_value_node, v_id_bulki);
    return rst;
}

/**
 * return number of attribute values
 * This is an attribute node
 * |attr_name|attr_value_region|
 */
int
append_attr_name_node(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
    int rst = 0;

    key_index_leaf_content_t *leafcnt      = (key_index_leaf_content_t *)value;
    HashTable *               vnode_buf_ht = (HashTable *)data; // data is the parameter passed in
    // the hash table is used to store the buffer struct related to each vnode id.
    BULKI *kv_bulki = hash_table_lookup(vnode_buf_ht, &(leafcnt->virtural_node_id));
    // index_buffer_t *buffer = hash_table_lookup(vnode_buf_ht, &(leafcnt->virtural_node_id));
    if (kv_bulki == NULL) {
        kv_bulki = BULKI_init(1); // one key-value pair
        hash_table_insert(vnode_buf_ht, &(leafcnt->virtural_node_id), kv_bulki);
    }
    // printf("[PERSISTENCE]key = %s\n", key);
    BULKI_Entity *key_entity = BULKI_ENTITY((void *)key, 1, PDC_STRING, PDC_CLS_ITEM);

    BULKI_Entity *data_entity = BULKI_get(kv_bulki, key_entity);
    if (data_entity == NULL) {
        // initilize data entity
        data_entity = BULKI_ENTITY(BULKI_init(4), 1, PDC_BULKI, PDC_CLS_ITEM);
    }

    BULKI *tree_bulki = ((BULKI *)data_entity->data);
    // append the value type
    if (_getCompoundTypeFromBitmap(leafcnt->val_idx_dtype) == PDC_STRING) {
        if (leafcnt->primary_trie != NULL) {
            BULKI *v_id_bulki = BULKI_init(1);
            rst |= append_string_value_tree(leafcnt->primary_trie, v_id_bulki);
            BULKI_put(tree_bulki, BULKI_ENTITY("primary_trie", 1, PDC_STRING, PDC_CLS_ITEM),
                      BULKI_ENTITY(v_id_bulki, 1, PDC_BULKI, PDC_CLS_ITEM));
        }
        if (leafcnt->secondary_trie != NULL) {
            BULKI *v_id_bulki = BULKI_init(1);
            rst |= append_string_value_tree(leafcnt->secondary_trie, v_id_bulki);
            BULKI_put(tree_bulki, BULKI_ENTITY("secondary_trie", 1, PDC_STRING, PDC_CLS_ITEM),
                      BULKI_ENTITY(v_id_bulki, 1, PDC_BULKI, PDC_CLS_ITEM));
        }
    }

    if (_getNumericalTypeFromBitmap(leafcnt->val_idx_dtype) != PDC_UNKNOWN) {
        if (leafcnt->primary_rbt != NULL) {
            BULKI *v_id_bulki = BULKI_init(1);
            rst |= append_numeric_value_tree(leafcnt->primary_rbt, v_id_bulki);
            BULKI_put(tree_bulki, BULKI_ENTITY("primary_rbt", 1, PDC_STRING, PDC_CLS_ITEM),
                      BULKI_ENTITY(v_id_bulki, 1, PDC_BULKI, PDC_CLS_ITEM));
        }
        if (leafcnt->secondary_rbt != NULL) {
            BULKI *v_id_bulki = BULKI_init(1);
            rst |= append_numeric_value_tree(leafcnt->secondary_rbt, v_id_bulki);
            BULKI_put(tree_bulki, BULKI_ENTITY("secondary_rbt", 1, PDC_STRING, PDC_CLS_ITEM),
                      BULKI_ENTITY(v_id_bulki, 1, PDC_BULKI, PDC_CLS_ITEM));
        }
    }
    // add the kv pair to the bulki structure
    BULKI_put(kv_bulki, key_entity, data_entity);
    return 0; // return 0 for art iteration to continue;
}

/**
 * Append the IDIOMS root to the file
 *
 */
int
dump_attr_root_tree(art_tree *art, char *dir_path, char *base_name, uint32_t serverID)
{
    HashTable *vid_buf_hash =
        hash_table_new(pdc_default_uint64_hash_func_ptr, pdc_default_uint64_equal_func_ptr);
    int rst = art_iter(art, append_attr_name_node, vid_buf_hash);

    int n_entry = hash_table_num_entries(vid_buf_hash);
    // iterate the hashtable and store the buffers into the file corresponds to the vnode id
    HashTableIterator iter;
    hash_table_iterate(vid_buf_hash, &iter);
    while (n_entry != 0 && hash_table_iter_has_more(&iter)) {
        HashTablePair pair = hash_table_iter_next(&iter);
        // vnode ID.  On different server, there can be the same vnode ID at this line below
        uint64_t *vid   = pair.key;
        BULKI *   bulki = pair.value;
        char      file_name[1024];
        // and this is why do we need to differentiate the file name by the server ID.
        sprintf(file_name, "%s/%s_%" PRIu32 "_%" PRIu64 ".bin", dir_path, base_name, serverID, *vid);
        LOG_INFO("Writing index to file_name: %s\n", file_name);
        FILE *stream = fopen(file_name, "wb");
        BULKI_serialize_to_file(bulki, stream);
    }
    return rst;
}

void
dump_dart_info(DART *dart, char *dir_path, uint32_t serverID)
{
    if (serverID == 0) {
        char file_name[1024];
        sprintf(file_name, "%s/%s.bin", dir_path, "dart_info");
        LOG_INFO("Writing DART info to file_name: %s\n", file_name);
        FILE *        stream   = fopen(file_name, "wb");
        BULKI_Entity *dart_ent = empty_Bent_Array_Entity();
        BULKI_ENTITY_append_BULKI_Entity(dart_ent,
                                         BULKI_ENTITY(&(dart->alphabet_size), 1, PDC_INT, PDC_CLS_ITEM));
        BULKI_ENTITY_append_BULKI_Entity(dart_ent,
                                         BULKI_ENTITY(&(dart->dart_tree_height), 1, PDC_INT, PDC_CLS_ITEM));
        BULKI_ENTITY_append_BULKI_Entity(dart_ent,
                                         BULKI_ENTITY(&(dart->replication_factor), 1, PDC_INT, PDC_CLS_ITEM));
        BULKI_ENTITY_append_BULKI_Entity(
            dart_ent, BULKI_ENTITY(&(dart->client_request_count), 1, PDC_INT, PDC_CLS_ITEM));
        BULKI_ENTITY_append_BULKI_Entity(dart_ent,
                                         BULKI_ENTITY(&(dart->num_server), 1, PDC_UINT32, PDC_CLS_ITEM));
        BULKI_ENTITY_append_BULKI_Entity(dart_ent,
                                         BULKI_ENTITY(&(dart->num_vnode), 1, PDC_UINT64, PDC_CLS_ITEM));

        BULKI_Entity_serialize_to_file(dart_ent, stream);
    }
}

perr_t
idioms_metadata_index_dump(IDIOMS_t *idioms, char *dir_path, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;

    stopwatch_t timer;
    timer_start(&timer);

    // dump DART info
    dump_dart_info(idioms->dart_info_g, dir_path, serverID);

    dump_attr_root_tree(idioms->art_key_prefix_tree_g, dir_path, "idioms_prefix", serverID);
    dump_attr_root_tree(idioms->art_key_suffix_tree_g, dir_path, "idioms_suffix", serverID);

    timer_pause(&timer);
    println("[IDIOMS_Index_Dump_%d] Timer to dump index = %.4f microseconds\n", serverID,
            timer_delta_us(&timer));
    return ret_value;
}

// *********************** Index Loading ***********************************

int
fill_set_from_BULKI_Entity(value_index_leaf_content_t *value_index_leaf, BULKI_Entity *data_entity,
                           int64_t *index_record_count)
{
    BULKI_Entity_Iterator *it = Bent_iterator_init(data_entity, NULL, PDC_UNKNOWN);
    while (Bent_iterator_has_next_Bent(it)) {
        BULKI_Entity *id_entity = Bent_iterator_next_Bent(it);
        uint64_t *    obj_id    = calloc(1, sizeof(uint64_t));
        memcpy(obj_id, id_entity->data, sizeof(uint64_t));
        set_insert(value_index_leaf->obj_id_set, obj_id);
        value_index_leaf->indexed_item_count++;
        (*index_record_count)++;
    }
    return 0;
}

int
read_attr_value_node(key_index_leaf_content_t *leaf_cnt, int value_tree_idx, BULKI *v_id_bulki,
                     int64_t *index_record_count)
{
    int                     rst = 0;
    BULKI_KV_Pair_Iterator *it  = BULKI_KV_Pair_iterator_init(v_id_bulki);
    while (BULKI_KV_Pair_iterator_has_next(it)) {
        BULKI_KV_Pair *kv_pair     = BULKI_KV_Pair_iterator_next(it);
        BULKI_Entity * key_entity  = &(kv_pair->key);
        BULKI_Entity * data_entity = &(kv_pair->value);

        value_index_leaf_content_t *value_index_leaf =
            (value_index_leaf_content_t *)calloc(1, sizeof(value_index_leaf_content_t));
        value_index_leaf->obj_id_set = set_new(ui64_hash, ui64_equal);
        set_register_free_function(value_index_leaf->obj_id_set, free);

        fill_set_from_BULKI_Entity(value_index_leaf, data_entity, index_record_count);

        if (value_tree_idx == 0 && key_entity->pdc_type == PDC_STRING) {
            art_tree *value_index_art = (art_tree *)leaf_cnt->primary_trie;
            art_insert(value_index_art, (const unsigned char *)key_entity->data, strlen(key_entity->data),
                       value_index_leaf);
            leaf_cnt->indexed_item_count++;
        }
        if (value_tree_idx == 1 && key_entity->pdc_type == PDC_STRING) {
            art_tree *value_index_art = (art_tree *)leaf_cnt->secondary_trie;
            art_insert(value_index_art, (const unsigned char *)key_entity->data, strlen(key_entity->data),
                       value_index_leaf);
        }
        if (value_tree_idx == 2 && key_entity->pdc_type != PDC_STRING) {
            rbt_t *value_index_rbt = (rbt_t *)leaf_cnt->primary_rbt;
            rbt_add(value_index_rbt, key_entity->data, key_entity->size, value_index_leaf);
            leaf_cnt->indexed_item_count++;
        }
        if (value_tree_idx == 3 && key_entity->pdc_type != PDC_STRING) {
            rbt_t *value_index_rbt = (rbt_t *)leaf_cnt->secondary_rbt;
            rbt_add(value_index_rbt, key_entity->data, key_entity->size, value_index_leaf);
        }
    }
    return rst;
}

int
read_value_tree(key_index_leaf_content_t *leaf_cnt, int value_tree_idx, BULKI *v_id_bulki,
                int64_t *index_record_count)
{
    int rst = 0;

    switch (value_tree_idx) {
        case 0:
            leaf_cnt->primary_trie = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(leaf_cnt->primary_trie);
            _encodeTypeToBitmap(&(leaf_cnt->val_idx_dtype), PDC_STRING);
            rst = read_attr_value_node(leaf_cnt, 0, v_id_bulki, index_record_count);
            break;
        case 1:
            leaf_cnt->secondary_trie = (art_tree *)calloc(1, sizeof(art_tree));
            art_tree_init(leaf_cnt->secondary_trie);
            _encodeTypeToBitmap(&(leaf_cnt->val_idx_dtype), PDC_STRING);
            rst = read_attr_value_node(leaf_cnt, 1, v_id_bulki, index_record_count);
            break;
        case 2:
            if (v_id_bulki->numKeys > 0) {
                leaf_cnt->primary_rbt =
                    (rbt_t *)rbt_create_by_dtype(v_id_bulki->header->keys[0].pdc_type, PDC_free_void);
                _encodeTypeToBitmap(&(leaf_cnt->val_idx_dtype), rbt_get_dtype(leaf_cnt->primary_rbt));
                rst = read_attr_value_node(leaf_cnt, 2, v_id_bulki, index_record_count);
            }
            break;
        case 3:
            if (v_id_bulki->numKeys > 0) {
                leaf_cnt->secondary_rbt =
                    (rbt_t *)rbt_create_by_dtype(v_id_bulki->header->keys[0].pdc_type, PDC_free_void);
                _encodeTypeToBitmap(&(leaf_cnt->val_idx_dtype), rbt_get_dtype(leaf_cnt->secondary_rbt));
                rst = read_attr_value_node(leaf_cnt, 3, v_id_bulki, index_record_count);
            }
            break;
        default:
            break;
    }
    return rst;
}

int
read_attr_name_node(IDIOMS_t *idioms, char *dir_path, char *base_name, uint32_t serverID, uint64_t vnode_id)
{
    int  rst = 0;
    char file_name[1024];
    sprintf(file_name, "%s/%s_%" PRIu32 "_%" PRIu64 ".bin", dir_path, base_name, serverID, vnode_id);

    // check file existence
    if (access(file_name, F_OK) == -1) {
        return FAIL;
    }
    FILE *stream = fopen(file_name, "rb");
    if (stream == NULL) {
        return FAIL;
    }
    BULKI *bulki = BULKI_deserialize_from_file(stream);

    art_tree *art_key_index = NULL;
    if (strcmp(base_name, "idioms_prefix") == 0) {
        art_key_index = idioms->art_key_prefix_tree_g;
    }
    else if (strcmp(base_name, "idioms_suffix") == 0) {
        art_key_index = idioms->art_key_suffix_tree_g;
    }
    else {
        LOG_ERROR("Unknown base_name: %s\n", base_name);
        return FAIL;
    }

    LOG_INFO("Loaded Index from file_name: %s\n", file_name);

    // iterate the bulki structure and insert the key-value pair into the art tree
    BULKI_KV_Pair_Iterator *it = BULKI_KV_Pair_iterator_init(bulki);
    while (BULKI_KV_Pair_iterator_has_next(it)) {
        BULKI_KV_Pair *kv_pair     = BULKI_KV_Pair_iterator_next(it);
        BULKI_Entity * key_entity  = &(kv_pair->key);
        BULKI_Entity * data_entity = &(kv_pair->value);

        key_index_leaf_content_t *leafcnt =
            (key_index_leaf_content_t *)calloc(1, sizeof(key_index_leaf_content_t));
        art_insert(art_key_index, (const unsigned char *)key_entity->data, strlen(key_entity->data), leafcnt);

        BULKI *tree_bulki = (BULKI *)data_entity->data;
        // restore primary trie
        BULKI_Entity *primary_trie_ent =
            BULKI_get(tree_bulki, BULKI_ENTITY("primary_trie", 1, PDC_STRING, PDC_CLS_ITEM));
        if (primary_trie_ent != NULL) {
            BULKI *v_id_bulki = (BULKI *)primary_trie_ent->data;
            read_value_tree(leafcnt, 0, v_id_bulki, &(idioms->index_record_count_g));
        }
        // restore secondary trie
        BULKI_Entity *secondary_trie_ent =
            BULKI_get(tree_bulki, BULKI_ENTITY("secondary_trie", 1, PDC_STRING, PDC_CLS_ITEM));
        if (secondary_trie_ent != NULL) {
            BULKI *v_id_bulki = (BULKI *)secondary_trie_ent->data;
            read_value_tree(leafcnt, 1, v_id_bulki, &(idioms->index_record_count_g));
        }
        // restore primary rbt
        BULKI_Entity *primary_rbt_ent =
            BULKI_get(tree_bulki, BULKI_ENTITY("primary_rbt", 1, PDC_STRING, PDC_CLS_ITEM));
        if (primary_rbt_ent != NULL) {
            BULKI *v_id_bulki = (BULKI *)primary_rbt_ent->data;
            read_value_tree(leafcnt, 2, v_id_bulki, &(idioms->index_record_count_g));
        }
        // restore secondary rbt
        BULKI_Entity *secondary_rbt_ent =
            BULKI_get(tree_bulki, BULKI_ENTITY("secondary_rbt", 1, PDC_STRING, PDC_CLS_ITEM));
        if (secondary_rbt_ent != NULL) {
            BULKI *v_id_bulki = (BULKI *)secondary_rbt_ent->data;
            read_value_tree(leafcnt, 3, v_id_bulki, &(idioms->index_record_count_g));
        }
    }
    return rst;
}

void
load_dart_info(DART *dart, char *dir_path, uint32_t serverID)
{
    if (serverID == 0) {
        char file_name[1024];
        sprintf(file_name, "%s/%s.bin", dir_path, "dart_info");
        FILE *stream = fopen(file_name, "rb");
        if (stream == NULL) {
            return;
        }
        BULKI_Entity *         dart_ent = BULKI_Entity_deserialize_from_file(stream);
        BULKI_Entity_Iterator *it       = Bent_iterator_init(dart_ent, NULL, PDC_UNKNOWN);
        int                    i        = 0;
        while (Bent_iterator_has_next_Bent(it)) {
            BULKI_Entity *entry = Bent_iterator_next_Bent(it);
            switch (i) {
                case 0:
                    if (entry->pdc_type == PDC_INT) {
                        dart->alphabet_size = *(int *)entry->data;
                    }
                    break;
                case 1:
                    if (entry->pdc_type == PDC_INT) {
                        dart->dart_tree_height = *(int *)entry->data;
                    }
                    break;
                case 2:
                    if (entry->pdc_type == PDC_INT) {
                        dart->replication_factor = *(int *)entry->data;
                    }
                    break;
                case 3:
                    if (entry->pdc_type == PDC_INT) {
                        dart->client_request_count = *(int *)entry->data;
                    }
                    break;
                case 4:

                    if (entry->pdc_type == PDC_UINT32) {
                        dart->num_server = *(uint32_t *)entry->data;
                    }
                    break;

                case 5:

                    if (entry->pdc_type == PDC_UINT64) {
                        dart->num_vnode = *(uint64_t *)entry->data;
                    }
                    break;
            }
            i++;
        }
    }
}

perr_t
idioms_metadata_index_recover(IDIOMS_t *idioms, char *dir_path, int num_server, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;

    stopwatch_t timer;
    timer_start(&timer);

    load_dart_info(idioms->dart_info_g, dir_path, serverID);

    uint64_t *vid_array = NULL;
    size_t    num_vids  = get_vnode_ids_by_serverID(idioms->dart_info_g, serverID, &vid_array);

    // load the attribute region for each vnode
    for (size_t vid = 0; vid < num_vids; vid++) {
        for (size_t sid = 0; sid < num_server; sid++) {
            read_attr_name_node(idioms, dir_path, "idioms_prefix", sid, vid_array[vid]);
            read_attr_name_node(idioms, dir_path, "idioms_suffix", sid, vid_array[vid]);
        }
    }
    timer_pause(&timer);
    println("[IDIOMS_Index_Recover_%d] Timer to recover index = %.4f microseconds\n", serverID,
            timer_delta_us(&timer));
    return ret_value;
}
