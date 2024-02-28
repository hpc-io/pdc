#include "comparators.h"
#include "rbtree.h"
#include "pdc_set.h"
#include "pdc_hash.h"
#include "idioms_local_index.h"
#include "bin_file_ops.h"
#include "pdc_hash_table.h"
#include "dart_core.h"

#define DART_SERVER_DEBUG 0

#define KV_DELIM '='

IDIOMS_t *idioms_g = NULL;

void
IDIOMS_init(uint32_t server_id, uint32_t num_servers)
{
    idioms_g                        = (IDIOMS_t *)calloc(1, sizeof(IDIOMS_t));
    idioms_g->art_key_prefix_tree_g = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(idioms_g->art_key_prefix_tree_g);

    idioms_g->art_key_suffix_tree_g = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(idioms_g->art_key_suffix_tree_g);

    idioms_g->server_id_g   = server_id;
    idioms_g->num_servers_g = num_servers;
}

IDIOMS_t *
get_idioms_g()
{
    return idioms_g;
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

    if (is_PDC_STRING(idx_record->type)) {
        void * attr_val      = idx_record->value;
        size_t value_str_len = strlen(idx_record->value);
        ret                  = insert_obj_ids_into_value_leaf(leaf_content->primary_trie, attr_val,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
        if (ret == FAIL) {
            return ret;
        }
#ifndef PDC_DART_SFX_TREE
        void *reverse_str = reverse_str((char *)attr_val);
        // insert the value into the trie for suffix search.
        ret = insert_obj_ids_into_value_leaf(leaf_content->secondary_trie, reverse_str,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
#else
        int sub_loop_count = value_str_len;
        for (int j = 1; j < sub_loop_count; j++) {
            char *suffix = substring(attr_val, j, value_str_len);
            ret          = insert_obj_ids_into_value_leaf(leaf_content->secondary_trie, suffix,
                                                 idx_record->type == PDC_STRING, strlen(suffix),
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
#endif
    }
    if (is_PDC_NUMERIC(idx_record->type)) {
        // idx_record->value_len should be the size of the value in bytes here.
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
        key_leaf_content = (key_index_leaf_content_t *)PDC_calloc(1, sizeof(key_index_leaf_content_t));
        // fill the content of the leaf_content node.
        if (is_PDC_STRING(idx_record->type)) {
            // the following gurarantees that both prefix index and suffix index are initialized.
            key_leaf_content->primary_trie = (art_tree *)PDC_calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)key_leaf_content->primary_trie);

            key_leaf_content->secondary_trie = (art_tree *)PDC_calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)key_leaf_content->secondary_trie);

            // idx_record->simple_value_type = 3; // string.
        }
        if (is_PDC_UINT(idx_record->type)) {
            key_leaf_content->primary_rbt = rbt_create(LIBHL_CMP_CB(PDC_UINT64), free);
        }
        if (is_PDC_INT(idx_record->type)) {
            key_leaf_content->primary_rbt = rbt_create(LIBHL_CMP_CB(PDC_INT64), free);
        }
        if (is_PDC_FLOAT(idx_record->type)) {
            key_leaf_content->primary_rbt = rbt_create(LIBHL_CMP_CB(PDC_DOUBLE), free);
        }
    }
    // insert the key into the the key trie along with the key_leaf_content.
    art_insert(key_trie, (unsigned char *)key, len, (void *)key_leaf_content);

    key_leaf_content->virtural_node_id = idx_record->virtual_node_id;

    // insert the value part into second level index.
    ret = insert_value_into_second_level_index(key_leaf_content, idx_record);
    return ret;
}

perr_t
idioms_local_index_create(IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    // get the key and create key_index_leaf_content node for it.
    char *key = idx_record->key;
    int   len = strlen(key);

    stopwatch_t index_timer;
    timer_start(&index_timer);
    art_tree *key_trie =
        (idx_record->is_key_suffix == 1) ? idioms_g->art_key_suffix_tree_g : idioms_g->art_key_prefix_tree_g;
    insert_into_key_trie(key_trie, key, len, idx_record);
    /**
     * Note: in IDIOMS, the client-runtime is responsible for iterating all suffixes of the key.
     * Therefore, there is no need to insert the suffixes of the key into the key trie locally.
     * Different suffixes of the key should be inserted into the key trie on different servers, distributed by
     * DART.
     *
     * Therefore, the following logic is commented off.
     */
    // #ifndef PDC_DART_SFX_TREE
    //     if (ret == FAIL) {
    //         return ret;
    //     }
    //     ret = insert_into_key_trie(idioms_g->art_key_suffix_tree, reverse_str(key), len, 0, idx_record);
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
               idioms_g->server_id_g, key, idx_record->value, timer_delta_us(&index_timer));
    }
    idioms_g->time_to_create_index_g += timer_delta_us(&index_timer);
    idioms_g->index_record_count_g++;
    idioms_g->insert_request_count_g++;
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

    free(obj_id);

    if (set_num_entries(((value_index_leaf_content_t *)entry)->obj_id_set) == 0) {
        set_free(((value_index_leaf_content_t *)entry)->obj_id_set);
        if (is_trie) {
            art_delete((art_tree *)index, (unsigned char *)attr_val, value_len);
        }
        else {
            rbt_remove((rbt_t *)index, attr_val, value_len, &entry);
        }
        PDC_free(entry);
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
    if (idx_record->type == PDC_STRING) {
        // delete the value from the prefix tree.
        void * attr_val      = idx_record->value;
        size_t value_str_len = strlen(idx_record->value);
        ret                  = delete_obj_ids_from_value_leaf(leaf_content->primary_trie, attr_val,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
        if (ret == FAIL) {
            return ret;
        }
#ifndef PDC_DART_SFX_TREE
        void *reverse_str = reverse_str((char *)attr_val);
        // when suffix tree mode is OFF, the secondary trie is used for indexing reversed value strings.
        ret = delete_obj_ids_from_value_leaf(leaf_content->secondary_trie, reverse_str,
                                             idx_record->type == PDC_STRING, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
#else
        // when suffix tree mode is ON, the secondary trie is used for indexing suffixes of the value string.
        int sub_loop_count = value_str_len;
        for (int j = 1; j < sub_loop_count; j++) {
            char *suffix = substring(attr_val, j, value_str_len);
            ret          = delete_obj_ids_from_value_leaf(leaf_content->secondary_trie, suffix,
                                                 idx_record->type == PDC_STRING, strlen(suffix),
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
#endif
    }
    else {
        // delete the value from the primary rbtree index. here, value_len is the size of the value in bytes.
        ret = delete_obj_ids_from_value_leaf(leaf_content->primary_rbt, idx_record->value,
                                             idx_record->type == PDC_STRING, idx_record->value_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);
    }
    return ret;
}

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
        art_delete(key_trie, (unsigned char *)key, len);
        return SUCCEED;
    }

    return ret;
}

/**
 *  @validated
 */
perr_t
idioms_local_index_delete(IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    // get the key and create key_index_leaf_content node for it.
    char *key = idx_record->key; // in a delete function for trie, there is no need to duplicate the string.
    int   len = strlen(key);

    stopwatch_t index_timer;
    timer_start(&index_timer);
    art_tree *key_trie =
        (idx_record->is_key_suffix == 1) ? idioms_g->art_key_suffix_tree_g : idioms_g->art_key_prefix_tree_g;
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
               idioms_g->server_id_g, key, idx_record->value, timer_delta_us(&index_timer));
    }
    idioms_g->time_to_delete_index_g += timer_delta_us(&index_timer);
    idioms_g->index_record_count_g--;
    idioms_g->delete_request_count_g++;
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

    printf("value_rbt_callback: key: %s, value: %s, value_index_leaf: %p\n", (char *)key,
           (char *)idx_record->value, value_index_leaf);

    if (value_index_leaf != NULL) {
        collect_obj_ids(value_index_leaf, idx_record);
    }
    return RBT_WALK_CONTINUE;
}

size_t
get_number_from_string(char *str, int simple_value_type, void **val_ptr)
{
    void * key     = NULL;
    size_t key_len = 0;
    if (simple_value_type == 0) { // UINT64
        uint64_t k = strtoull(str, NULL, 10);
        key_len    = sizeof(uint64_t);
        key        = malloc(key_len);
        memcpy(key, &k, key_len);
    }
    else if (simple_value_type == 1) { // INT64
        int64_t k = strtoll(str, NULL, 10);
        key_len   = sizeof(int64_t);
        key       = malloc(key_len);
        memcpy(key, &k, key_len);
    }
    else if (simple_value_type == 2) { // DOUBLE
        double k = strtod(str, NULL);
        key_len  = sizeof(double);
        key      = malloc(key_len);
        memcpy(key, &k, key_len);
    }
    *val_ptr = key;
    return key_len;
}

int
value_number_query(char *secondary_query, key_index_leaf_content_t *leafcnt,
                   IDIOMS_md_idx_record_t *idx_record)
{
    if (leafcnt->primary_rbt != NULL) {

        if (contains(secondary_query, "~")) {
            // find the first number before '~'
            int   tilde_pos = indexOf(secondary_query, '~');
            char *tok1      = substring(secondary_query, 0, tilde_pos);
            // find the second number after '~'
            char *tok2 = substring(secondary_query, tilde_pos + 1, strlen(secondary_query));

            void * val1;
            void * val2;
            size_t klen1 = get_number_from_string(tok1, idx_record->simple_value_type, &val1);
            size_t klen2 = get_number_from_string(tok2, idx_record->simple_value_type, &val2);

            rbt_range_walk(leafcnt->primary_rbt, val1, klen1, val2, klen2, value_rbt_callback, idx_record);

            // char *tok = subrstr(secondary_query, strlen(secondary_query) - 1);
            // rbt_walk_ge(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
            // rbt_walk_le(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
        }
        // else if (contains(secondary_query, "+")) {
        //     char *tok = subrstr(secondary_query, strlen(secondary_query) - 1);
        //     rbt_walk_gt(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
        //     rbt_walk_lt(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
        // }
        // else if (startsWith(secondary_query, ">=")) {
        //     char *tok = substring(secondary_query, 2, strlen(secondary_query));
        //     rbt_walk_ge(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
        // }
        // else if (startsWith(secondary_query, "<=")) {
        //     char *tok = substring(secondary_query, 2, strlen(secondary_query));
        //     rbt_walk_le(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
        // }
        // else if (startsWith(secondary_query, ">")) {
        //     char *tok = substring(secondary_query, 1, strlen(secondary_query));
        //     rbt_walk_gt(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
        // }
        // else if (startsWith(secondary_query, "<")) {
        //     char *tok = substring(secondary_query, 1, strlen(secondary_query));
        //     rbt_walk_lt(leafcnt->primary_rbt, tok, strlen(tok), value_rbt_callback, idx_record);
        // }
        else {
            value_index_leaf_content_t *value_index_leaf = NULL;

            void * val  = NULL;
            size_t klen = get_number_from_string(secondary_query, idx_record->simple_value_type, &val);
            rbt_find(leafcnt->primary_rbt, val, klen, (void **)&value_index_leaf);
            if (value_index_leaf != NULL) {
                collect_obj_ids(value_index_leaf, idx_record);
            }
        }
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
            if (leafcnt->secondary_trie != NULL) {
                art_iter_prefix(leafcnt->secondary_trie, (unsigned char *)tok, strlen(tok),
                                value_trie_callback, idx_record);
            }
#else
            if (leafcnt->primary_trie != NULL) {
                value_index_leaf_content_t *value_index_leaf = (value_index_leaf_content_t *)art_search(
                    leafcnt->primary_trie, (unsigned char *)tok, strlen(tok));
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
                art_iter_prefix(leafcnt->primary_trie, (unsigned char *)tok, strlen(tok), value_trie_callback,
                                idx_record);
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

    pdc_c_var_type_t attr_type = leafcnt->type;

    int query_rst = 0;
    if (idx_record->type == PDC_STRING) {
        // perform string search
        query_rst |= value_string_query(v_query, leafcnt, idx_record);
    }

    if (contains(value_type_str, "STRING")) {
        return value_string_query(v_query, leafcnt, idx_record);
    }
    else {
        char *value_type_str = get_enum_name_by_dtype(attr_type);

        int simple_value_type = -1;
        if (startsWith(value_type_str, "PDC_UINT")) {
            simple_value_type = 0; // UINT64
        }
        else if (startsWith(value_type_str, "PDC_INT") || startsWith(value_type_str, "PDC_LONG")) {
            simple_value_type = 1; // INT64
        }
        else if (startsWith(value_type_str, "PDC_FLOAT") || startsWith(value_type_str, "PDC_DOUBLE")) {
            simple_value_type = 2; // DOUBLE
        }
        else {
            printf("ERROR: unsupported data type %s\n", value_type_str);
            return 0;
        }
        idx_record->simple_value_type = simple_value_type;

        return value_number_query(v_query, leafcnt, idx_record);
    }
    return 0;
}

/**
 * the query initially is passed via 'key' field.
 * the format can be:
 * 1. exact query -> key=value
 * 2. prefix query -> key=value*
 * 3. suffix query -> key=*value
 * 4. infix query -> key=*value*
 * 5. range query -> key>value
 * 6. range query -> key<value
 * 7. range query -> key>=value
 * 8. range query -> key<=value
 * 9. range query -> key=value1|~value2
 * 10. range query -> key=value1~|value2
 * 11. range query -> key=value1|~|value2
 */
uint64_t
idioms_local_index_search(IDIOMS_md_idx_record_t *idx_record)
{
    uint64_t    result_count = 0;
    stopwatch_t index_timer;
    if (idioms_g == NULL) {
        println("[Server_Side_Query_%d] idioms_g is NULL.", idioms_g->server_id_g);
        return result_count;
    }
    if (idx_record == NULL) {
        return result_count;
    }

    char *query      = idx_record->key;
    char *kdelim_ptr = strchr(query, (int)KV_DELIM);

    if (NULL == kdelim_ptr) {
        if (DART_SERVER_DEBUG) {
            println("[Server_Side_Query_%d]query string '%s' is not valid.", idioms_g->server_id_g, query);
        }
        return result_count;
    }

    char *k_query = get_key(query, KV_DELIM);
    char *v_query = get_value(query, KV_DELIM);

    if (DART_SERVER_DEBUG) {
        println("[Server_Side_Query_%d] k_query = '%s' | v_query = '%s' ", idioms_g->server_id_g, k_query,
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
            leafcnt      = (key_index_leaf_content_t *)art_search(idioms_g->art_key_prefix_tree_g,
                                                             (unsigned char *)tok, strlen(tok));
            if (leafcnt != NULL) {
                key_index_search_callback((void *)idx_record, (unsigned char *)tok, strlen(tok),
                                          (void *)leafcnt);
            }
            break;
        case PATTERN_PREFIX:
            qType_string = "Prefix";
            tok          = substring(k_query, 0, strlen(k_query) - 1);
            art_iter_prefix(idioms_g->art_key_prefix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_search_callback, (void *)idx_record);
            break;
        case PATTERN_SUFFIX:
            qType_string = "Suffix";
            tok          = substring(k_query, 1, strlen(k_query));
#ifndef PDC_DART_SFX_TREE
            tok = reverse_str(tok);
            art_iter_prefix(idioms_g->art_key_suffix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_search_callback, (void *)idx_record);
#else
            leafcnt = (key_index_leaf_content_t *)art_search(idioms_g->art_key_suffix_tree_g,
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
            art_iter(idioms_g->art_key_prefix_tree_g, key_index_search_callback, (void *)idx_record);
#else
            art_iter_prefix(idioms_g->art_key_suffix_tree_g, (unsigned char *)tok, strlen(tok),
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
               qType_string, idioms_g->server_id_g, query, idx_record->num_obj_ids,
               timer_delta_us(&index_timer));
    }
    idioms_g->time_to_search_index_g += timer_delta_us(&index_timer);
    idioms_g->search_request_count_g += 1;
    return result_count;
}

/****************************/
/* Index Dump               */
/****************************/

// ********************* Index Dump and Load *********************

void *
append_buffer(index_buffer_t *buffer, void *new_buf, uint64_t new_buf_size)
{
    if (buffer->buffer_size + new_buf_size + sizeof(uint64_t) > buffer->buffer_capacity) {
        buffer->buffer_capacity = (buffer->buffer_size + new_buf_size + sizeof(uint64_t)) * 2;
        buffer->buffer          = realloc(buffer->buffer, buffer->buffer_capacity);
    }
    memcpy(buffer->buffer + buffer->buffer_size, &new_buf_size, sizeof(uint64_t));
    buffer->buffer_size += sizeof(uint64_t);
    memcpy(buffer->buffer + buffer->buffer_size, new_buf, new_buf_size);
    buffer->buffer_size += new_buf_size;
    return buffer->buffer;
}

/**
 * This is a object ID set
 * |number of object IDs = n|object ID 1|...|object ID n|
 */
uint64_t
append_obj_id_set(Set *obj_id_set, index_buffer_t *buffer)
{

    uint64_t  num_obj_id    = set_num_entries(obj_id_set);
    uint64_t *id_set_buffer = calloc(num_obj_id + 1, sizeof(uint64_t));
    id_set_buffer[0]        = num_obj_id;

    int         offset = 1;
    SetIterator iter;
    set_iterate(obj_id_set, &iter);
    while (set_iter_has_more(&iter)) {
        uint64_t *item        = (uint64_t *)set_iter_next(&iter);
        id_set_buffer[offset] = *item;
        offset++;
    }

    append_buffer(buffer, id_set_buffer, (num_obj_id + 1) * sizeof(uint64_t));
    free(id_set_buffer);
    return num_obj_id + 1;
}

int
append_value_tree_node(void *buf, void *key, uint32_t key_size, void *value)
{
    index_buffer_t *buffer = (index_buffer_t *)buf;
    append_buffer(buffer, key, key_size);

    value_index_leaf_content_t *value_index_leaf = (value_index_leaf_content_t *)(value);
    if (value_index_leaf != NULL) {
        Set *obj_id_set = (Set *)value_index_leaf->obj_id_set;
        append_obj_id_set(obj_id_set, buffer);
    }
    return 0; // return 0 for art iteration to continue;
}

/**
 * This is a string value node
 * |str_val|file_obj_pair_list|
 */
int
append_string_value_node(void *buffer, const unsigned char *key, uint32_t key_len, void *value)
{
    return append_value_tree_node(buffer, (void *)key, key_len, value);
}

rbt_walk_return_code_t
append_numeric_value_node(rbt_t *rbt, void *key, size_t klen, void *value, void *priv)
{
    append_value_tree_node(priv, key, klen, value);
    return RBT_WALK_CONTINUE;
}

/**
 * This is the string value region
 * |type = 3|number of values = n|value_node_1|...|value_node_n|
 *
 * return number of strings in the string value tree
 */
uint64_t
append_string_value_tree(art_tree *art, index_buffer_t *buffer)
{
    // 1. number of values
    uint64_t *num_str_value = malloc(sizeof(uint64_t));
    *num_str_value          = art_size(art);
    append_buffer(buffer, num_str_value, sizeof(uint64_t));

    // 2. value nodes
    uint64_t rst = art_iter(art, append_string_value_node, buffer);
    rst          = (rst == 0) ? *num_str_value : 0;
    free(num_str_value);
    return rst;
}

/**
 * This is the numeric value region
 * |type = 0/1/2|number of values = n|value_node_1|...|value_node_n|
 *
 * return number of numeric values in the numeric value tree
 */
uint64_t
append_numeric_value_tree(rbt_t *rbt, index_buffer_t *buffer)
{
    // 1. number of values
    uint64_t *num_str_value = malloc(sizeof(uint64_t));
    *num_str_value          = rbt_size(rbt);
    append_buffer(buffer, num_str_value, sizeof(uint64_t));

    // 2. value nodes
    uint64_t rst = rbt_walk(rbt, append_numeric_value_node, buffer);
    free(num_str_value);
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
    key_index_leaf_content_t *leafcnt      = (key_index_leaf_content_t *)value;
    HashTable *               vnode_buf_ht = (HashTable *)data;
    index_buffer_t *          buffer       = hash_table_lookup(vnode_buf_ht, &(leafcnt->virtural_node_id));
    if (buffer == NULL) {
        buffer                  = (index_buffer_t *)calloc(1, sizeof(index_buffer_t));
        buffer->buffer_capacity = 1024;
        buffer->buffer          = calloc(1, buffer->buffer_capacity);
        hash_table_insert(vnode_buf_ht, &(leafcnt->virtural_node_id), buffer);
    }
    int rst = 0;
    // 1. attr name
    append_buffer(buffer, (void *)key, key_len);

    // 2. attr value type
    int8_t *type = (int8_t *)calloc(1, sizeof(int8_t));
    type[0]      = leafcnt->type;
    append_buffer(buffer, type, sizeof(int8_t));

    // 3. attr value simple type.
    int8_t *simple_type = (int8_t *)calloc(1, sizeof(int8_t));
    simple_type[0]      = leafcnt->simple_value_type;
    append_buffer(buffer, simple_type, sizeof(int8_t));

    if (leafcnt->simple_value_type == 3) {
        rst = append_string_value_tree(leafcnt->primary_trie, buffer);
#ifndef PDC_DART_SFX_TREE
        rst = append_string_value_tree(leafcnt->secondary_trie, buffer);
#endif
    }
    else {
        rst = append_numeric_value_tree(leafcnt->primary_rbt, buffer);
    }
    // printf("number of attribute values = %d\n", rst);
    return 0; // return 0 for art iteration to continue;
}

/**
 * This is the "attribute region"
 * |number of attributes = n|attr_node 1|...|attr_node n|
 */
int
append_attr_root_tree(art_tree *art, char *dir_path, char *base_name)
{
    HashTable *vid_buf_hash =
        hash_table_new(pdc_default_uint64_hash_func_ptr, pdc_default_uint64_equal_func_ptr);
    int rst = art_iter(art, append_attr_name_node, vid_buf_hash);

    int n_entry = hash_table_num_entries(vid_buf_hash);
    // iterate the hashtable and store the buffers into the file corresponds to the vnode id
    HashTableIterator iter;
    hash_table_iterate(vid_buf_hash, &iter);
    while (n_entry != 0 && hash_table_iter_has_more(&iter)) {
        HashTablePair   pair   = hash_table_iter_next(&iter);
        uint64_t *      vid    = pair.key;
        index_buffer_t *buffer = pair.value;
        char            file_name[1024];
        sprintf(file_name, "%s/%s_%d.bin", dir_path, base_name, *vid);
        FILE *stream = fopen(file_name, "wb");
        fwrite(&(buffer->buffer_size), sizeof(uint64_t), 1, stream);
        fwrite(buffer->buffer, 1, buffer->buffer_size, stream);
        fclose(stream);
    }
    return rst;
}

perr_t
idioms_metadata_index_dump(char *dir_path, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;

    stopwatch_t timer;
    timer_start(&timer);

    // 2. append attribute region
    append_attr_root_tree(idioms_g->art_key_prefix_tree_g, dir_path, "idioms_prefix");
#ifndef PDC_DART_SFX_TREE
    append_attr_root_tree(idioms_g->art_key_suffix_tree_g, dir_path, "idioms_suffix");
#endif

    timer_pause(&timer);
    println("[IDIOMS_Index_Dump_%d] Timer to dump index = %.4f microseconds\n", serverID,
            timer_delta_us(&timer));
    return ret_value;
}

// *********************** Index Loading ***********************************

size_t
read_buffer(index_buffer_t *buffer, void **dst, size_t *size)
{
    if (buffer->buffer_size < *size) {
        return 0;
    }
    uint64_t dsize;
    memcpy(&dsize, buffer->buffer, sizeof(uint64_t));
    buffer->buffer += sizeof(uint64_t);
    buffer->buffer_size -= sizeof(uint64_t);
    *size = dsize;

    *dst = calloc(1, *size);
    memcpy(*dst, buffer->buffer, *size);
    buffer->buffer += *size;
    buffer->buffer_size -= *size;
    return *size;
}

int
read_attr_value_node(void *value_index, int simple_value_type, index_buffer_t *buffer)
{
    // 1. read value
    size_t len;
    void * val = NULL;
    read_buffer(buffer, (void **)&val, &len);

    // 2. read object ID set
    Set *     obj_id_set = set_new(ui64_hash, ui64_equal);
    uint64_t *num_obj_id;
    read_buffer(buffer, (void **)&num_obj_id, &len);
    uint64_t i = 0;
    for (i = 0; i < *num_obj_id; i++) {
        uint64_t *obj_id;
        read_buffer(buffer, (void **)&obj_id, &len);
        set_insert(obj_id_set, obj_id);
    }

    value_index_leaf_content_t *value_index_leaf =
        (value_index_leaf_content_t *)calloc(1, sizeof(value_index_leaf_content_t));
    value_index_leaf->obj_id_set = obj_id_set;

    // 3. insert value into art tree
    if (simple_value_type == 3) {
        art_insert((art_tree *)value_index, (const unsigned char *)val, strlen(val),
                   (void *)value_index_leaf);
    }
    else {
        rbt_add((rbt_t *)value_index, val, strlen(val), (void *)value_index_leaf);
    }
    int rst = (set_num_entries(obj_id_set) == *num_obj_id) ? 0 : 1;
    return rst;
}

int
read_value_tree(void *value_index, int simple_value_type, index_buffer_t *buffer)
{
    int rst = 0;
    if (simple_value_type == 3) {
        art_tree_init((art_tree *)value_index);
    }

    size_t    len;
    uint64_t *num_values;
    read_buffer(buffer, (void **)&num_values, &len);

    uint64_t i = 0;
    for (i = 0; i < *num_values; i++) {
        rst = rst | read_attr_value_node(value_index, simple_value_type, buffer);
    }
    return rst;
}

int
read_attr_name_node(art_tree *art_key_index, char *dir_path, char *base_name, uint64_t vnode_id)
{

    char file_name[1024];
    sprintf(file_name, "%s/%s_%d.bin", dir_path, base_name, vnode_id);
    FILE *          stream = fopen(file_name, "rb");
    index_buffer_t *buffer = (index_buffer_t *)calloc(1, sizeof(index_buffer_t));
    fread(&(buffer->buffer_size), sizeof(uint64_t), 1, stream);
    buffer->buffer_capacity = buffer->buffer_size;
    buffer->buffer          = calloc(1, buffer->buffer_capacity);
    fread(buffer->buffer, 1, buffer->buffer_size, stream);
    fclose(stream);
    int rst = 0;
    while (buffer->buffer_size > 0) {
        key_index_leaf_content_t *leafcnt =
            (key_index_leaf_content_t *)calloc(1, sizeof(key_index_leaf_content_t));

        // 1. read attr name
        size_t key_len   = 0;
        char * attr_name = NULL;
        read_buffer(buffer, (void **)&attr_name, &key_len);

        // 2. read attr value type
        int8_t *type;
        size_t  len;
        read_buffer(buffer, (void **)&type, &len);
        leafcnt->type = type[0];

        // 3. attr value simple type.
        int8_t *simple_type;
        read_buffer(buffer, (void **)&simple_type, &len);
        leafcnt->simple_value_type = simple_type[0];

        // 4. read attr values
        void *value_index = NULL;
        if (leafcnt->simple_value_type == 3) {
            leafcnt->primary_trie = (art_tree *)calloc(1, sizeof(art_tree));
            value_index           = leafcnt->primary_trie;
#ifndef PDC_DART_SFX_TREE
            leafcnt->secondary_trie = (art_tree *)calloc(1, sizeof(art_tree));
            value_index             = leafcnt->secondary_trie;
#endif
        }
        else {
            libhl_cmp_callback_t compare_func = NULL;
            if (leafcnt->simple_value_type == 0) { // UINT64
                compare_func = LIBHL_CMP_CB(PDC_UINT64);
            }
            else if (leafcnt->simple_value_type == 1) { // INT64
                compare_func = LIBHL_CMP_CB(PDC_INT64);
            }
            else if (leafcnt->simple_value_type == 2) { // DOUBLE
                compare_func = LIBHL_CMP_CB(PDC_DOUBLE);
            }
            else {
                printf("ERROR: unsupported data type %d\n", leafcnt->simple_value_type);
                return FAIL;
            }
            leafcnt->primary_rbt = (rbt_t *)rbt_create(compare_func, free);
            value_index          = leafcnt->primary_rbt;
        }
        read_value_tree(value_index, leafcnt->simple_value_type, buffer);

        // 5. insert the key into the key trie along with the key_leaf_content.
        art_insert(art_key_index, (const unsigned char *)attr_name, key_len, leafcnt);
    }
    return rst;
}

perr_t
idioms_metadata_index_recover(char *dir_path, int num_client, int num_server, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;

    stopwatch_t timer;
    timer_start(&timer);

    DART *dart_info = (DART *)calloc(1, sizeof(DART));
    init_dart_space_via_idioms(dart_info, num_client, num_server, IDIOMS_MAX_SERVER_COUNT_TO_ADAPT);

    uint64_t *vid_array = NULL;
    size_t    num_vids  = get_vnode_ids_by_serverID(dart_info, serverID, &vid_array);

    IDIOMS_init(serverID, num_server);

    // load the attribute region for each vnode
    for (size_t vid = 0; vid < num_vids; vid++) {
        read_attr_name_node(idioms_g->art_key_prefix_tree_g, dir_path, "idioms_prefix", vid);
#ifndef PDC_DART_SFX_TREE
        read_attr_name_node(idioms_g->art_key_suffix_tree_g, dir_path, "idioms_suffix", vid);
#endif
    }

    timer_pause(&timer);
    println("[IDIOMS_Index_Recover_%d] Timer to recover index = %.4f microseconds\n", serverID,
            timer_delta_us(&timer));
    return ret_value;
}

void
init_dart_space_via_idioms(DART *dart, int num_client, int num_server, int max_server_num_to_adapt)
{
    int extra_tree_height  = 0;
    int replication_factor = 3;
    replication_factor     = replication_factor > 0 ? replication_factor : 2;
    dart_space_init(dart, num_client, num_server, IDIOMS_DART_ALPHABET_SIZE, extra_tree_height,
                    replication_factor, max_server_num_to_adapt);
}