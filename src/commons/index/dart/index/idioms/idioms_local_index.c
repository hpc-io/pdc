#include "comparators.h"
#include "rbtree.h"
#include "pdc_set.h"
#include "pdc_hash.h"
#include "idioms_local_index.h"
#include "bin_file_ops.h"
#include "pdc_hash_table.h"
#include "string_utils.h"
#include "query_utils.h"
#include "pdc_logger.h"
#include <unistd.h>
#include <inttypes.h>
#include <stdint.h>

#define DART_SERVER_DEBUG 0

#define KV_DELIM '='

IDIOMS_t *
IDIOMS_init(uint32_t server_id, uint32_t num_servers)
{
    IDIOMS_t *idioms              = (IDIOMS_t *)calloc(1, sizeof(IDIOMS_t));
    idioms->art_key_prefix_tree_g = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(idioms->art_key_prefix_tree_g);

    idioms->art_key_suffix_tree_g = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(idioms->art_key_suffix_tree_g);

    idioms->server_id_g   = server_id;
    idioms->num_servers_g = num_servers;

    idioms->dart_info_g = (DART *)calloc(1, sizeof(DART));
    _init_dart_space_via_idioms(idioms->dart_info_g, idioms->num_servers_g);

    return idioms;
}

/****************************/
/* Index Create             */
/****************************/

perr_t
insert_obj_ids_into_value_leaf(void *index, void *attr_val, int is_trie, size_t value_len, uint64_t *obj_ids,
                               size_t num_obj_ids)
{
    perr_t ret = SUCCEED;
    // printf("index is %p, obj_id: %llu\n", index, obj_ids[0]);
    if (index == NULL) {
        return FAIL;
    }

    // if (strcmp((char *)attr_val, "str109str") == 0) {
    //     printf("attr_val: %s, value_len: %zu, obj_ids: %llu\n", (char *)attr_val, value_len, obj_ids[0]);
    // }
    // if (strcmp((char *)attr_val, "str096str") == 0) {
    //     printf("attr_val: %s, value_len: %zu, obj_ids: %llu\n", (char *)attr_val, value_len, obj_ids[0]);
    // }
    // if (strcmp((char *)attr_val, "str099str") == 0) {
    //     printf("attr_val: %s, value_len: %zu, obj_ids: %llu\n", (char *)attr_val, value_len, obj_ids[0]);
    // }

    void *entry     = NULL;
    int   idx_found = -1; // -1 not found, 0 found.
    if (is_trie) {
        entry     = art_search((art_tree *)index, (unsigned char *)attr_val, value_len);
        idx_found = (entry != NULL) ? 0 : -1;
    }
    else {
        idx_found = rbt_find((rbt_t *)index, attr_val, value_len, &entry);
    }

    if (entry == NULL) { // not found
        entry = (value_index_leaf_content_t *)PDC_calloc(1, sizeof(value_index_leaf_content_t));
        // create new set for obj_ids
        Set *obj_id_set = set_new(ui64_hash, ui64_equal);
        set_register_free_function(obj_id_set, free);

        ((value_index_leaf_content_t *)entry)->obj_id_set = obj_id_set;
        if (is_trie) {
            art_insert((art_tree *)index, (unsigned char *)attr_val, value_len, entry);
        }
        else {
            rbt_add((rbt_t *)index, attr_val, value_len, entry);
        }
    }

    for (int j = 0; j < num_obj_ids; j++) {
        // the set is directly taking the pointer to every entry, which means we have to allocate memory for
        // every number stored in the set.
        uint64_t *obj_id = (uint64_t *)PDC_calloc(1, sizeof(uint64_t));
        *obj_id          = obj_ids[j];
        set_insert(((value_index_leaf_content_t *)entry)->obj_id_set, (SetValue)obj_id);
        size_t num_entires = set_num_entries(((value_index_leaf_content_t *)entry)->obj_id_set);
    }
    return ret;
}

perr_t
insert_value_into_second_level_index(key_index_leaf_content_t *leaf_content,
                                     IDIOMS_md_idx_record_t *  idx_record)
{
    perr_t ret = SUCCEED;
    if (leaf_content == NULL) {
        return FAIL;
    }
    char *value_type_str = get_enum_name_by_dtype(idx_record->type);

    if (_getCompoundTypeFromBitmap(leaf_content->val_idx_dtype) == PDC_STRING &&
        is_PDC_STRING(idx_record->type)) {
        void * attr_val      = stripQuotes(idx_record->value);
        size_t value_str_len = strlen(attr_val);
        ret                  = insert_obj_ids_into_value_leaf(leaf_content->primary_trie, attr_val,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
        if (ret == FAIL) {
            return ret;
        }
#ifndef PDC_DART_SFX_TREE
        void *reverted_val = reverse_str((char *)attr_val);
        // LOG_DEBUG("reverted_val: %s\n", (char *)reverted_val);
        // insert the value into the trie for suffix search.
        ret = insert_obj_ids_into_value_leaf(leaf_content->secondary_trie, reverted_val,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
#else
        int sub_loop_count = value_str_len;
        for (int j = 0; j < sub_loop_count; j++) {
            char *suffix = substring(attr_val, j, value_str_len);
            // LOG_DEBUG("suffix: %s\n", suffix);
            ret = insert_obj_ids_into_value_leaf(leaf_content->secondary_trie, suffix,
                                                 idx_record->type == PDC_STRING, value_str_len - j,
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
#endif
    }
    if (_getNumericalTypeFromBitmap(leaf_content->val_idx_dtype) != PDC_UNKNOWN &&
        is_PDC_NUMERIC(idx_record->type)) {
        ret = insert_obj_ids_into_value_leaf((rbt_t *)leaf_content->primary_rbt, idx_record->value,
                                             idx_record->type == PDC_STRING, idx_record->value_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
    }
    return ret;
}

perr_t
insert_into_key_trie(art_tree *key_trie, char *key, int len, IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    if (key_trie == NULL) {
        return FAIL;
    }

    // look up for leaf_content
    key_index_leaf_content_t *key_leaf_content =
        (key_index_leaf_content_t *)art_search(key_trie, (unsigned char *)key, len);

    // create leaf_content node if not exist.
    if (key_leaf_content == NULL) {
        // create key_leaf_content node for the key.
        key_leaf_content = (key_index_leaf_content_t *)PDC_calloc(1, sizeof(key_index_leaf_content_t));
        // insert the key into the the key trie along with the key_leaf_content.
        art_insert(key_trie, (unsigned char *)key, len, (void *)key_leaf_content);
        // LOG_DEBUG("Inserted key %s into the key trie\n", key);
    }

    // fill the content of the leaf_content node, if necessary.
    if (key_leaf_content->primary_trie == NULL && key_leaf_content->secondary_trie == NULL &&
        is_PDC_STRING(idx_record->type)) {
        // the following gurarantees that both prefix index and suffix index are initialized.
        key_leaf_content->primary_trie = (art_tree *)PDC_calloc(1, sizeof(art_tree));
        art_tree_init((art_tree *)key_leaf_content->primary_trie);

        key_leaf_content->secondary_trie = (art_tree *)PDC_calloc(1, sizeof(art_tree));
        art_tree_init((art_tree *)key_leaf_content->secondary_trie);

        _encodeTypeToBitmap(&(key_leaf_content->val_idx_dtype), idx_record->type);
    }
    if (key_leaf_content->primary_rbt == NULL) {
        if (is_PDC_UINT(idx_record->type)) {
            // TODO: This is a simplified implementation, but we need to have all the CMP_CB functions
            // defined for all numerical types in libhl/comparators.h
            key_leaf_content->primary_rbt = rbt_create_by_dtype(PDC_UINT64, PDC_free_void);
            _encodeTypeToBitmap(&(key_leaf_content->val_idx_dtype), PDC_UINT64);
        }
        if (is_PDC_INT(idx_record->type)) {
            key_leaf_content->primary_rbt = rbt_create_by_dtype(PDC_INT64, PDC_free_void);
            _encodeTypeToBitmap(&(key_leaf_content->val_idx_dtype), PDC_INT64);
        }
        if (is_PDC_FLOAT(idx_record->type)) {
            key_leaf_content->primary_rbt = rbt_create_by_dtype(PDC_DOUBLE, PDC_free_void);
            _encodeTypeToBitmap(&(key_leaf_content->val_idx_dtype), PDC_DOUBLE);
        }
    }

    key_leaf_content->virtural_node_id = idx_record->virtual_node_id;

    // insert the value part into second level index.
    ret = insert_value_into_second_level_index(key_leaf_content, idx_record);
    return ret;
}

perr_t
idioms_local_index_create(IDIOMS_t *idioms, IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    // get the key and create key_index_leaf_content node for it.
    char *key = idx_record->key;
    int   len = strlen(key);

    stopwatch_t index_timer;
    timer_start(&index_timer);
    art_tree *key_trie =
        (idx_record->is_key_suffix == 1) ? idioms->art_key_suffix_tree_g : idioms->art_key_prefix_tree_g;
    // if (strcmp(key, "str109str") == 0) {
    //     printf("key: %s, len: %d\n", key, len);
    // }
    // if (strcmp(key, "str096str") == 0) {
    //     printf("key: %s, len: %d\n", key, len);
    // }
    // if (strcmp(key, "str099str") == 0) {
    //     printf("key: %s, len: %d\n", key, len);
    // }
    insert_into_key_trie(key_trie, key, len, idx_record);
    /**
     * Note: in IDIOMS, the client-runtime is responsible for iterating all suffixes of the key.
     * Therefore, there is no need to insert the suffixes of the key into the key trie locally.
     * Different suffixes of the key should be inserted into the key trie on different servers,
     * distributed by DART.
     *
     * Therefore, the following logic is commented off.
     */
    // #ifndef PDC_DART_SFX_TREE
    //     if (ret == FAIL) {
    //         return ret;
    //     }
    //     ret = insert_into_key_trie(idioms_g->art_key_suffix_tree, reverse_str(key), len, 0,
    //     idx_record);
    // #else
    //     // insert every suffix of the key into the trie;
    //     int sub_loop_count = len;
    //     for (int j = 1; j < sub_loop_count; j++) {
    //         char *suffix = substring(key, j, len);
    //         // TODO: change delete and search functions for suffix/infix query on the suffix trie.
    //         ret = insert_into_key_trie(idioms_g->art_key_suffix_tree_g, suffix, strlen(suffix), 1,
    //         idx_record); if (ret == FAIL) {
    //             return ret;
    //         }
    //     }
    // #endif
    timer_pause(&index_timer);

    if (DART_SERVER_DEBUG) {
        printf("[Server_Side_Insert_%d] Timer to insert a keyword %s : %s into index = %.4f microseconds\n",
               idioms->server_id_g, key, idx_record->value, timer_delta_us(&index_timer));
        char value_str[64];
        if (idx_record->type == PDC_STRING) {
            snprintf(value_str, 64, "%s", idx_record->value);
        }
        else if (is_PDC_UINT(idx_record->type)) {
            snprintf(value_str, 64, "%" PRIu64, *((uint64_t *)idx_record->value));
        }
        else if (is_PDC_INT(idx_record->type)) {
            snprintf(value_str, 64, "%" PRId64, *((int64_t *)idx_record->value));
        }
        else if (is_PDC_FLOAT(idx_record->type)) {
            snprintf(value_str, 64, "%f", *((double *)idx_record->value));
        }
        else {
            snprintf(value_str, 64, "[UnknownValue]");
        }
        printf("[idioms_local_index_create] Client %" PRIu32 " inserted a kvtag \"%s\" : \"%s\" -> %" PRIu64
               " into Server %" PRIu32 " in %.4f microseconds, insert_request_count_g = %" PRId64
               ", index_record_count_g = %" PRId64 "\n",
               idx_record->src_client_id, key, value_str, idx_record->obj_ids[0], idioms->server_id_g,
               timer_delta_us(&index_timer), idioms->insert_request_count_g, idioms->index_record_count_g);
    }
    idioms->time_to_create_index_g += timer_delta_us(&index_timer);
    idioms->index_record_count_g++;
    idioms->insert_request_count_g++;

    return ret;
}

/****************************/
/* Index Delete             */
/****************************/
perr_t
delete_obj_ids_from_value_leaf(void *index, void *attr_val, int is_trie, size_t value_len, uint64_t *obj_ids,
                               size_t num_obj_ids)
{
    perr_t ret = SUCCEED;
    if (index == NULL) {
        return ret;
    }

    void *entry     = NULL;
    int   idx_found = -1; // -1 not found, 0 found.
    if (is_trie) {
        entry     = art_search((art_tree *)index, (unsigned char *)attr_val, value_len);
        idx_found = (entry != NULL) ? 0 : -1;
    }
    else {
        idx_found = rbt_find((rbt_t *)index, attr_val, value_len, &entry);
    }

    if (idx_found != 0) { // not found
        return SUCCEED;
    }

    uint64_t *obj_id = (uint64_t *)PDC_calloc(1, sizeof(uint64_t));
    for (int j = 0; j < num_obj_ids; j++) {
        if (ret == FAIL) {
            return ret;
        }
        // obj_id here is just for comparison purpose, and no need to allocate memory for it every time.
        *obj_id = obj_ids[j];
        set_remove(((value_index_leaf_content_t *)entry)->obj_id_set, (SetValue)obj_id);
    }

    PDC_free(obj_id);

    if (set_num_entries(((value_index_leaf_content_t *)entry)->obj_id_set) == 0) {
        set_free(((value_index_leaf_content_t *)entry)->obj_id_set);
        if (is_trie) {
            art_delete((art_tree *)index, (unsigned char *)attr_val, value_len);
        }
        else {
            rbt_remove((rbt_t *)index, attr_val, value_len, &entry);
        }
        // PDC_free(entry);
    }
    return ret;
}

perr_t
delete_value_from_second_level_index(key_index_leaf_content_t *leaf_content,
                                     IDIOMS_md_idx_record_t *  idx_record)
{
    perr_t ret = SUCCEED;
    if (leaf_content == NULL) {
        return FAIL;
    }
    char *value_type_str = get_enum_name_by_dtype(idx_record->type);
    if (_getCompoundTypeFromBitmap(leaf_content->val_idx_dtype) == PDC_STRING &&
        is_PDC_STRING(idx_record->type)) {
        // delete the value from the prefix tree.
        void *attr_val = stripQuotes(idx_record->value);

        size_t value_str_len = strlen(attr_val);
        ret                  = delete_obj_ids_from_value_leaf(leaf_content->primary_trie, attr_val,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
        if (ret == FAIL) {
            return ret;
        }
#ifndef PDC_DART_SFX_TREE
        void *reverted_val = reverse_str((char *)attr_val);
        LOG_DEBUG("DEL reverted_val: %s\n", (char *)reverted_val);
        // when suffix tree mode is OFF, the secondary trie is used for indexing reversed value strings.
        ret = delete_obj_ids_from_value_leaf(leaf_content->secondary_trie, reverted_val,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
#else
        // when suffix tree mode is ON, the secondary trie is used for indexing suffixes of the value
        // string.
        int sub_loop_count = value_str_len;
        for (int j = 0; j < sub_loop_count; j++) {
            char *suffix = substring(attr_val, j, value_str_len);
            ret          = delete_obj_ids_from_value_leaf(leaf_content->secondary_trie, suffix,
                                                 idx_record->type == PDC_STRING, value_str_len - j,
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
#endif
    }
    if (_getNumericalTypeFromBitmap(leaf_content->val_idx_dtype) != PDC_UNKNOWN &&
        is_PDC_NUMERIC(idx_record->type)) {
        // delete the value from the primary rbtree index. here, value_len is the size of the value in
        // bytes.
        ret = delete_obj_ids_from_value_leaf(leaf_content->primary_rbt, idx_record->value,
                                             idx_record->type == PDC_STRING, idx_record->value_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
    }
    return ret;
}

int
is_key_leaf_cnt_empty(key_index_leaf_content_t *leaf_content)
{
    if (leaf_content->primary_trie == NULL && leaf_content->secondary_trie == NULL &&
        leaf_content->primary_rbt == NULL && leaf_content->secondary_rbt == NULL) {
        return 1;
    }
    return 0;
}

/**
 *  @validated
 */
perr_t
delete_from_key_trie(art_tree *key_trie, char *key, int len, IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    if (key_trie == NULL) {
        return FAIL;
    }
    // look up for leaf_content
    key_index_leaf_content_t *key_leaf_content =
        (key_index_leaf_content_t *)art_search(key_trie, (unsigned char *)key, len);
    // if no corresponding leaf_content, that means the key has been deleted already.
    if (key_leaf_content == NULL) {
        return SUCCEED;
    }

    // delete the value part from second level index.
    ret = delete_value_from_second_level_index(key_leaf_content, idx_record);
    if (ret == FAIL) {
        return ret;
    }

    char *value_type_str       = get_enum_name_by_dtype(idx_record->type);
    int   count_in_value_index = 0;

    if (is_PDC_NUMERIC(idx_record->type)) {
        if (rbt_size(key_leaf_content->primary_rbt) == 0) {
            rbt_destroy(key_leaf_content->primary_rbt);
            key_leaf_content->primary_rbt = NULL;
        }
        if (rbt_size(key_leaf_content->secondary_rbt) == 0) {
            rbt_destroy(key_leaf_content->secondary_rbt);
            key_leaf_content->secondary_rbt = NULL;
        }
    }
    if (is_PDC_STRING(idx_record->type)) {
        if (art_size(key_leaf_content->primary_trie) == 0) {
            art_tree_destroy(key_leaf_content->primary_trie);
            key_leaf_content->primary_trie = NULL;
        }
        if (art_size(key_leaf_content->secondary_trie) == 0) {
            art_tree_destroy(key_leaf_content->secondary_trie);
            key_leaf_content->secondary_trie = NULL;
        }
    }

    if (is_key_leaf_cnt_empty(key_leaf_content)) {
        // delete the key from the the key trie along with the key_leaf_content.
        free(key_leaf_content);
        // LOG_DEBUG("Deleted key %s from the key trie\n", key);
        art_delete(key_trie, (unsigned char *)key, len);
        return SUCCEED;
    }

    return ret;
}

/**
 *  @validated
 */
perr_t
idioms_local_index_delete(IDIOMS_t *idioms, IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    // get the key and create key_index_leaf_content node for it.
    char *key = idx_record->key; // in a delete function for trie, there is no need to duplicate the string.
    int   len = strlen(key);

    stopwatch_t index_timer;
    timer_start(&index_timer);
    art_tree *key_trie =
        (idx_record->is_key_suffix == 1) ? idioms->art_key_suffix_tree_g : idioms->art_key_prefix_tree_g;
    delete_from_key_trie(key_trie, key, len, idx_record);
    /**
     * Note: in IDIOMS, the client-runtime is responsible for iterating all suffixes of the key.
     * Therefore, there is no need to insert the suffixes of the key into the key trie locally.
     * Different suffixes of the key should be inserted into the key trie on different servers,
     * distributed by DART.
     *
     * Therefore, the following logic is commented off.
     */
    // #ifndef PDC_DART_SFX_TREE
    //     if (ret == FAIL) {
    //         return ret;
    //     }
    //     ret = delete_from_key_trie(idioms_g->art_key_suffix_tree, reverse_str(key), len, idx_record);
    // #else
    //     // insert every suffix of the key into the trie;
    //     int sub_loop_count = len;
    //     for (int j = 1; j < sub_loop_count; j++) {
    //         if (ret == FAIL) {
    //             return ret;
    //         }
    //         char *suffix = substring(key, j, len);
    //         ret = delete_from_key_trie(idioms_g->art_key_prefix_tree_g, suffix, strlen(suffix),
    //         idx_record);
    //     }
    // #endif
    timer_pause(&index_timer);
    if (DART_SERVER_DEBUG) {
        printf("[Server_Side_Delete_%d] Timer to delete a keyword %s : %s from index = %.4f microseconds\n",
               idioms->server_id_g, key, idx_record->value, timer_delta_us(&index_timer));
    }
    idioms->time_to_delete_index_g += timer_delta_us(&index_timer);
    idioms->index_record_count_g--;
    idioms->delete_request_count_g++;
    return ret;
}

/****************************/
/* Index Search             */
/****************************/

int
collect_obj_ids(value_index_leaf_content_t *value_index_leaf, IDIOMS_md_idx_record_t *idx_record)
{
    Set *obj_id_set = (Set *)value_index_leaf->obj_id_set;

    // get number of object IDs in the set
    int num_obj_ids = set_num_entries(obj_id_set);
    // printf("[SEARCH] obj_id_set: %p, num_obj: %d\n", obj_id_set, num_obj_ids);

    // realloc the obj_ids array in idx_record
    idx_record->obj_ids =
        (uint64_t *)realloc(idx_record->obj_ids, sizeof(uint64_t) * (idx_record->num_obj_ids + num_obj_ids));
    size_t      offset = idx_record->num_obj_ids;
    SetIterator value_set_iter;
    set_iterate(obj_id_set, &value_set_iter);
    while (set_iter_has_more(&value_set_iter)) {
        uint64_t *item = (uint64_t *)set_iter_next(&value_set_iter);
        memcpy(idx_record->obj_ids + offset, item, sizeof(uint64_t));
        offset++;
    }
    if (offset - idx_record->num_obj_ids == num_obj_ids) {
        idx_record->num_obj_ids += num_obj_ids;
    }
    else {
        printf("ERROR: offset %zu != num_obj_ids %d\n", offset, num_obj_ids);
    }
    return 0;
}

int
value_trie_callback(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
    value_index_leaf_content_t *value_index_leaf = (value_index_leaf_content_t *)(value);
    IDIOMS_md_idx_record_t *    idx_record       = (IDIOMS_md_idx_record_t *)(data);

    // printf("value_trie_callback: key: %s, value: %s, value_index_leaf: %p\n", key, (char
    // *)idx_record->value,
    //        value_index_leaf);
    char *         v_query          = (char *)idx_record->value;
    pattern_type_t value_query_type = determine_pattern_type(v_query);
    if (value_query_type == PATTERN_MIDDLE) {
        char *infix = substring(v_query, 1, strlen(v_query) - 1);
        if (contains((char *)key, infix) == 0) {
            return 0;
        }
    }

    if (value_index_leaf != NULL) {
        collect_obj_ids(value_index_leaf, idx_record);
    }
    return 0;
}

rbt_walk_return_code_t
value_rbt_callback(rbt_t *rbt, void *key, size_t klen, void *value, void *priv)
{
    value_index_leaf_content_t *value_index_leaf = (value_index_leaf_content_t *)(value);
    IDIOMS_md_idx_record_t *    idx_record       = (IDIOMS_md_idx_record_t *)(priv);

    // printf("value_rbt_callback: key: %s, value: %s, value_index_leaf: %p\n", (char *)key,
    //        (char *)idx_record->value, value_index_leaf);

    if (value_index_leaf != NULL) {
        collect_obj_ids(value_index_leaf, idx_record);
    }
    return RBT_WALK_CONTINUE;
}

/**
 *  The following queries are what we need to support
 * 1. exact query -> key=|value| (key == value)
 * 5. range query -> key=value~  (key > value)
 * 6. range query -> key=~value (key < value)
 * 7. range query -> key=value|~ (key >= value)
 * 8. range query -> key=~|value (key <= value)
 * 9. range query -> key=value1|~value2 (value1 <= key < value2)
 * 10. range query -> key=value1~|value2 (value1 < key <= value2)
 * 11. range query -> key=value1~value2 (value1 < key < value2)
 * 11. range query -> key=value1|~|value2 (value1 <= key <= value2)
 *
 * When return 0, it is successful.
 */
int
value_number_query(char *secondary_query, key_index_leaf_content_t *leafcnt,
                   IDIOMS_md_idx_record_t *idx_record)
{
    if (leafcnt->primary_rbt == NULL) {
        return 0;
    }

    // allocate memory according to the val_idx_dtype for value 1 and value 2.
    void *val1;
    void *val2;
    if (startsWith(secondary_query, "|") && startsWith(secondary_query, "|")) {
        // exact number search
        char * num_str = substring(secondary_query, 1, strlen(secondary_query) - 1);
        size_t klen1 =
            get_number_from_string(num_str, _getNumericalTypeFromBitmap(leafcnt->val_idx_dtype), &val1);
        value_index_leaf_content_t *value_index_leaf = NULL;
        rbt_find(leafcnt->primary_rbt, val1, klen1, (void **)&value_index_leaf);
        if (value_index_leaf != NULL) {
            collect_obj_ids(value_index_leaf, idx_record);
        }
    }
    else if (startsWith(secondary_query, "~")) {
        int endInclusive = secondary_query[1] == '|';
        // find all numbers that are smaller than the given number
        int    beginPos = endInclusive ? 2 : 1;
        char * numstr   = substring(secondary_query, beginPos, strlen(secondary_query));
        size_t klen1 =
            get_number_from_string(numstr, _getNumericalTypeFromBitmap(leafcnt->val_idx_dtype), &val1);
        rbt_range_lt(leafcnt->primary_rbt, val1, klen1, value_rbt_callback, idx_record, endInclusive);
    }
    else if (endsWith(secondary_query, "~")) {
        int beginInclusive = secondary_query[strlen(secondary_query) - 2] == '|';
        int endPos         = beginInclusive ? strlen(secondary_query) - 2 : strlen(secondary_query) - 1;
        // find all numbers that are greater than the given number
        char * numstr = substring(secondary_query, 0, endPos);
        size_t klen1 =
            get_number_from_string(numstr, _getNumericalTypeFromBitmap(leafcnt->val_idx_dtype), &val1);
        rbt_range_gt(leafcnt->primary_rbt, val1, klen1, value_rbt_callback, idx_record, beginInclusive);
    }
    else if (contains(secondary_query, "~")) {
        int    num_tokens = 0;
        char **tokens     = NULL;
        // the string is not ended or started with '~', and if it contains '~', it is a in-between query.
        split_string(secondary_query, "~", &tokens, &num_tokens);
        if (num_tokens != 2) {
            printf("ERROR: invalid range query: %s\n", secondary_query);
            return -1;
        }
        char *lo_tok = tokens[0];
        char *hi_tok = tokens[1];
        // lo_tok might be ended with '|', and hi_tok might be started with '|', to indicate inclusivity.
        int    beginInclusive = endsWith(lo_tok, "|");
        int    endInclusive   = startsWith(hi_tok, "|");
        char * lo_num_str     = beginInclusive ? substring(lo_tok, 0, strlen(lo_tok) - 1) : lo_tok;
        char * hi_num_str     = endInclusive ? substring(hi_tok, 1, strlen(hi_tok)) : hi_tok;
        size_t klen1 =
            get_number_from_string(lo_num_str, _getNumericalTypeFromBitmap(leafcnt->val_idx_dtype), &val1);
        size_t klen2 =
            get_number_from_string(hi_num_str, _getNumericalTypeFromBitmap(leafcnt->val_idx_dtype), &val2);

        int num_visited_node = rbt_range_walk(leafcnt->primary_rbt, val1, klen1, val2, klen2,
                                              value_rbt_callback, idx_record, beginInclusive, endInclusive);
        // println("[value_number_query] num_visited_node: %d\n", num_visited_node);
    }
    else {
        // exact query by default
        // exact number search
        char * num_str = strdup(secondary_query);
        size_t klen1 =
            get_number_from_string(num_str, _getNumericalTypeFromBitmap(leafcnt->val_idx_dtype), &val1);
        value_index_leaf_content_t *value_index_leaf = NULL;
        rbt_find(leafcnt->primary_rbt, val1, klen1, (void **)&value_index_leaf);
        if (value_index_leaf != NULL) {
            collect_obj_ids(value_index_leaf, idx_record);
        }
        // free(num_str);
    }
    return 0;
}

int
value_string_query(char *secondary_query, key_index_leaf_content_t *leafcnt,
                   IDIOMS_md_idx_record_t *idx_record)
{
    pattern_type_t level_two_ptn_type = determine_pattern_type(secondary_query);
    char *         tok                = NULL;
    switch (level_two_ptn_type) {
        case PATTERN_EXACT:
            tok = secondary_query;
            if (leafcnt->primary_trie != NULL) {
                value_index_leaf_content_t *value_index_leaf = (value_index_leaf_content_t *)art_search(
                    leafcnt->primary_trie, (unsigned char *)tok, strlen(tok));
                if (value_index_leaf != NULL) {
                    value_trie_callback((void *)idx_record, (unsigned char *)tok, strlen(tok),
                                        (void *)value_index_leaf);
                }
            }
            break;
        case PATTERN_PREFIX:
            tok = subrstr(secondary_query, strlen(secondary_query) - 1);
            if (leafcnt->primary_trie != NULL) {
                art_iter_prefix((art_tree *)leafcnt->primary_trie, (unsigned char *)tok, strlen(tok),
                                value_trie_callback, idx_record);
            }
            break;
        case PATTERN_SUFFIX:
            tok = substr(secondary_query, 1);
#ifndef PDC_DART_SFX_TREE
            tok = reverse_str(tok);
            // LOG_DEBUG("reverted_val_tok: %s\n", tok);
            if (leafcnt->secondary_trie != NULL) {
                art_iter_prefix(leafcnt->secondary_trie, (unsigned char *)tok, strlen(tok),
                                value_trie_callback, idx_record);
            }
#else
            if (leafcnt->secondary_trie != NULL) {
                value_index_leaf_content_t *value_index_leaf = (value_index_leaf_content_t *)art_search(
                    leafcnt->secondary_trie, (unsigned char *)tok, strlen(tok));
                if (value_index_leaf != NULL) {
                    value_trie_callback((void *)idx_record, (unsigned char *)tok, strlen(tok),
                                        (void *)value_index_leaf);
                }
            }
#endif
            break;
        case PATTERN_MIDDLE:
            tok = substring(secondary_query, 1, strlen(secondary_query) - 1);
            if (leafcnt->primary_trie != NULL) {
#ifndef PDC_DART_SFX_TREE
                art_iter(leafcnt->primary_trie, value_trie_callback, idx_record);
#else
                art_iter_prefix(leafcnt->secondary_trie, (unsigned char *)tok, strlen(tok),
                                value_trie_callback, idx_record);
#endif
            }
            break;
        default:
            break;
    }
    return 0;
}

int
key_index_search_callback(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
    key_index_leaf_content_t *leafcnt    = (key_index_leaf_content_t *)value;
    IDIOMS_md_idx_record_t *  idx_record = (IDIOMS_md_idx_record_t *)(data);

    char *k_query = idx_record->key;
    char *v_query = idx_record->value;

    pattern_type_t key_query_type = determine_pattern_type(k_query);
    if (key_query_type == PATTERN_MIDDLE) {
        char *infix = substring(k_query, 1, strlen(k_query) - 1);
        if (contains((char *)key, infix) == 0) {
            return 0;
        }
    }

    pdc_c_var_type_t attr_type = is_string_query(v_query) ? PDC_STRING : idx_record->type;

    int query_rst = 0;
    if (is_string_query(v_query)) { // this is a test based on only the v_query string.
        // perform string search
        char *bare_v_query = stripQuotes(v_query);
        // the value_string_query function should check if the leafcnt matches with the query type or not.
        query_rst |= value_string_query(bare_v_query, leafcnt, idx_record);
    }

    if (is_number_query(v_query)) {
        // perform number search
        query_rst |= value_number_query(v_query, leafcnt, idx_record);
    }
    return query_rst;
}

/**
 * the query initially is passed via 'key' field.
 * the format can be:
 * 1. exact query -> key="value"
 * 2. prefix query -> key="value*"
 * 3. suffix query -> key="*value"
 * 4. infix query -> key="*value*"
 *
 * TODO: The following queries are what we need to support
 * 1. exact query -> key=|value| (key == value)
 * 5. range query -> key=value~  (key > value)
 * 6. range query -> key=~value (key < value)
 * 7. range query -> key=value|~ (key >= value)
 * 8. range query -> key=~|value (key <= value)
 * 9. range query -> key=value1|~value2 (value1 <= key < value2)
 * 10. range query -> key=value1~|value2 (value1 < key <= value2)
 * 11. range query -> key=value1~value2 (value1 < key < value2)
 * 11. range query -> key=value1|~|value2 (value1 <= key <= value2)
 */
uint64_t
idioms_local_index_search(IDIOMS_t *idioms, IDIOMS_md_idx_record_t *idx_record)
{
    uint64_t    result_count = 0;
    stopwatch_t index_timer;
    if (idioms == NULL) {
        println("[Server_Side_Query_%d] idioms is NULL.", idioms->server_id_g);
        return result_count;
    }
    if (idx_record == NULL) {
        return result_count;
    }

    char *query      = idx_record->key;
    char *kdelim_ptr = strchr(query, (int)KV_DELIM);

    if (NULL == kdelim_ptr) {
        if (DART_SERVER_DEBUG) {
            println("[Server_Side_Query_%d]query string '%s' is not valid.", idioms->server_id_g, query);
        }
        return result_count;
    }

    char *k_query = get_key(query, KV_DELIM);
    char *v_query = get_value(query, KV_DELIM);

    if (DART_SERVER_DEBUG) {
        println("[Server_Side_Query_%d] k_query = '%s' | v_query = '%s' ", idioms->server_id_g, k_query,
                v_query);
    }

    idx_record->key   = k_query;
    idx_record->value = v_query;

    timer_start(&index_timer);

    char *                    qType_string = "Exact";
    char *                    tok;
    pattern_type_t            level_one_ptn_type = determine_pattern_type(k_query);
    key_index_leaf_content_t *leafcnt            = NULL;
    // if (index_type == DHT_FULL_HASH || index_type == DHT_INITIAL_HASH) {
    //     search_through_hash_table(k_query, index_type, level_one_ptn_type, param);
    // }
    // else {
    switch (level_one_ptn_type) {
        case PATTERN_EXACT:
            qType_string = "Exact";
            tok          = k_query;
            leafcnt      = (key_index_leaf_content_t *)art_search(idioms->art_key_prefix_tree_g,
                                                             (unsigned char *)tok, strlen(tok));
            if (leafcnt != NULL) {
                key_index_search_callback((void *)idx_record, (unsigned char *)tok, strlen(tok),
                                          (void *)leafcnt);
            }
            break;
        case PATTERN_PREFIX:
            qType_string = "Prefix";
            tok          = substring(k_query, 0, strlen(k_query) - 1);
            art_iter_prefix(idioms->art_key_prefix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_search_callback, (void *)idx_record);
            break;
        case PATTERN_SUFFIX:
            qType_string = "Suffix";
            tok          = substring(k_query, 1, strlen(k_query));
#ifndef PDC_DART_SFX_TREE
            tok = reverse_str(tok);
            // LOG_DEBUG("reversed tok: %s\n", tok);
            art_iter_prefix(idioms->art_key_suffix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_search_callback, (void *)idx_record);
#else
            leafcnt = (key_index_leaf_content_t *)art_search(idioms->art_key_suffix_tree_g,
                                                             (unsigned char *)tok, strlen(tok));
            if (leafcnt != NULL) {
                key_index_search_callback((void *)idx_record, (unsigned char *)tok, strlen(tok),
                                          (void *)leafcnt);
            }
#endif
            break;
        case PATTERN_MIDDLE:
            qType_string = "Infix";
            tok          = substring(k_query, 1, strlen(k_query) - 1);
#ifndef PDC_DART_SFX_TREE
            art_iter(idioms->art_key_prefix_tree_g, key_index_search_callback, (void *)idx_record);
#else
            art_iter_prefix(idioms->art_key_suffix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_search_callback, (void *)idx_record);
#endif
            break;
        default:
            break;
    }
    // }

    timer_pause(&index_timer);
    if (DART_SERVER_DEBUG) {
        printf("[Server_Side_%s_%d] Time to address query '%s' and get %d results  = %.4f microseconds\n",
               qType_string, idioms->server_id_g, query, idx_record->num_obj_ids,
               timer_delta_us(&index_timer));
    }
    idioms->time_to_search_index_g += timer_delta_us(&index_timer);
    idioms->search_request_count_g += 1;
    return result_count;
}
