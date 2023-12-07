#include "comparators.h"
#include "rbtree.h"
#include "pdc_set.h"
#include "pdc_hash.h"
#include "idioms_local_index.h"

#define DART_SERVER_DEBUG 0

#define KV_DELIM ':'

IDIOMS_t *idioms_g = NULL;

void
IDIOMS_init(uint32_t server_id, uint32_t num_servers)
{
    idioms_g                        = (IDIOMS_t *)calloc(1, sizeof(IDIOMS_t));
    idioms_g->art_key_prefix_tree_g = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(idioms_g->art_key_prefix_tree_g);
#ifndef PDC_DART_SFX_TREE
    idioms_g->art_key_suffix_tree_g = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(idioms_g->art_key_suffix_tree_g);
#endif
    idioms_g->server_id_g   = server_id;
    idioms_g->num_servers_g = num_servers;
}

/****************************/
/* Index Create             */
/****************************/

perr_t
insert_obj_ids_into_value_leaf(void *index, void *attr_val, int is_trie, size_t value_len, uint64_t *obj_ids,
                               size_t num_obj_ids)
{
    perr_t ret = SUCCEED;
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
    if (idx_found != 0) { // not found
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
        if (ret == FAIL) {
            return ret;
        }
        uint64_t *obj_id = (uint64_t *)PDC_calloc(1, sizeof(uint64_t));
        *obj_id          = obj_ids[j];
        set_insert(((value_index_leaf_content_t *)entry)->obj_id_set, (SetValue)obj_id);
    }
    return ret;
}

perr_t
insert_into_value_index(void *value_index, int use_trie, IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    if (value_index == NULL) {
        return FAIL;
    }

    if (use_trie) { // logic for string values
        void * attr_val      = strdup(idx_record->value);
        size_t value_str_len = strlen(idx_record->value);
        ret = insert_obj_ids_into_value_leaf((art_tree *)value_index, attr_val, use_trie, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);

// insert every suffix if suffix-tree mode is on.
#ifdef PDC_DART_SFX_TREE
        int sub_loop_count = value_str_len;
        for (int j = 1; j < sub_loop_count; j++) {
            if (ret == FAIL) {
                return ret;
            }
            char *suffix = substring(attr_val, j, value_str_len);
            ret = insert_obj_ids_into_value_leaf((art_tree *)value_index, suffix, use_trie, strlen(suffix),
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
#endif
    }
    else { // logic for numeric values
        int    i          = 0;
        size_t mem_offset = 0;
        for (i = 0; i < idx_record->value_len; i++) {
            if (ret == FAIL) {
                return ret;
            }
            size_t value_size_by_type = get_size_by_dtype(idx_record->type);
            void * attr_val           = PDC_calloc(1, value_size_by_type);
            memcpy(attr_val, idx_record->value + mem_offset, value_size_by_type);
            mem_offset += value_size_by_type;

            ret = insert_obj_ids_into_value_leaf((rbt_t *)value_index, attr_val, use_trie, value_size_by_type,
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
    }
    return ret;
}

perr_t
insert_value_into_second_level_index(key_index_leaf_content_t *leaf_content, int use_trie,
                                     IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    if (leaf_content == NULL) {
        return FAIL;
    }
    char *value_type_str = get_enum_name_by_dtype(idx_record->type);
    if (use_trie) {
        // insert the value into the prefix tree.
        ret = insert_into_value_index((void *)leaf_content->primary_trie, use_trie, idx_record);
#ifndef PDC_DART_SFX_TREE
        if (ret == FAIL) {
            return ret;
        }
        // insert the value into the trie for suffix search.
        ret = insert_into_value_index((void *)leaf_content->secondary_trie, use_trie, idx_record);
#endif
    }
    else {
        // insert the value into the primary index.
        ret = insert_into_value_index((void *)leaf_content->primary_rbt, use_trie, idx_record);
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
    char *value_type_str = get_enum_name_by_dtype(idx_record->type);
    int   use_trie       = 0;
    // look up for leaf_content
    key_index_leaf_content_t *key_leaf_content =
        (key_index_leaf_content_t *)art_search(key_trie, (unsigned char *)key, len);
    // create leaf_content node if not exist.
    if (key_leaf_content == NULL) {
        key_leaf_content = (key_index_leaf_content_t *)PDC_calloc(1, sizeof(key_index_leaf_content_t));
        // fill the content of the leaf_content node.
        key_leaf_content->type = idx_record->type;

        if (contains(value_type_str, "STRING")) {
            use_trie = 1;
            // the following gurarantees that both prefix index and suffix index are initialized.
            key_leaf_content->primary_trie = (art_tree *)PDC_calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)key_leaf_content->primary_trie);

#ifndef PDC_DART_SFX_TREE
            // we only enable suffix index when suffix tree mode is off.
            key_leaf_content->secondary_trie = (art_tree *)PDC_calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)key_leaf_content->secondary_trie);
#endif
        }
        else {
            int                  simple_value_type = -1;
            libhl_cmp_callback_t compare_func      = NULL;
            if (startsWith(value_type_str, "PDC_UINT")) {
                simple_value_type = 0; // UINT64
                compare_func      = LIBHL_CMP_CB(PDC_UINT64);
            }
            else if (startsWith(value_type_str, "PDC_INT") || startsWith(value_type_str, "PDC_LONG")) {
                simple_value_type = 1; // INT64
                compare_func      = LIBHL_CMP_CB(PDC_INT64);
            }
            else if (startsWith(value_type_str, "PDC_FLOAT") || startsWith(value_type_str, "PDC_DOUBLE")) {
                simple_value_type = 2; // DOUBLE
                compare_func      = LIBHL_CMP_CB(PDC_DOUBLE);
            }
            else {
                printf("ERROR: unsupported data type %s\n", value_type_str);
                return FAIL;
            }
            key_leaf_content->primary_rbt = rbt_create(compare_func, free);
        }
        // insert the key into the the key trie along with the key_leaf_content.
        art_insert(key_trie, (unsigned char *)key, len, (void *)key_leaf_content);
    }
    // insert the value part into second level index.
    ret = insert_value_into_second_level_index(key_leaf_content, use_trie, idx_record);
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

    ret = insert_into_key_trie(idioms_g->art_key_prefix_tree_g, key, len, idx_record);
#ifndef PDC_DART_SFX_TREE
    if (ret == FAIL) {
        return ret;
    }
    ret = insert_into_key_trie(idioms_g->art_key_suffix_tree, reverse_str(key), len, idx_record);
#else
    // insert every suffix of the key into the trie;
    int sub_loop_count = len;
    for (int j = 1; j < sub_loop_count; j++) {
        char *suffix = substring(key, j, len);
        ret = insert_into_key_trie(idioms_g->art_key_prefix_tree_g, suffix, strlen(suffix), idx_record);
        if (ret == FAIL) {
            return ret;
        }
    }
#endif
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

    if (idx_found != 0) { // not found
        return SUCCEED;
    }

    for (int j = 0; j < num_obj_ids; j++) {
        if (ret == FAIL) {
            return ret;
        }
        uint64_t *obj_id = (uint64_t *)PDC_calloc(1, sizeof(uint64_t));
        *obj_id          = obj_ids[j];
        set_remove(((value_index_leaf_content_t *)entry)->obj_id_set, (SetValue)obj_id);
    }
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
delete_from_value_index(void *value_index, int use_trie, IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    if (value_index == NULL) {
        return FAIL;
    }

    if (use_trie) { // logic for string values
        void * attr_val      = strdup(idx_record->value);
        size_t value_str_len = strlen(idx_record->value);
        ret = delete_obj_ids_from_value_leaf((art_tree *)value_index, attr_val, use_trie, value_str_len,
                                             idx_record->obj_ids, idx_record->num_obj_ids);

// delete every suffix if suffix-tree mode is on.
#ifdef PDC_DART_SFX_TREE
        int sub_loop_count = value_str_len;
        for (int j = 1; j < sub_loop_count; j++) {
            if (ret == FAIL) {
                return ret;
            }
            char *suffix = substring(attr_val, j, value_str_len);
            ret = delete_obj_ids_from_value_leaf((art_tree *)value_index, suffix, use_trie, strlen(suffix),
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
#endif
    }
    else { // logic for numeric values
        int    i          = 0;
        size_t mem_offset = 0;
        for (i = 0; i < idx_record->value_len; i++) {
            if (ret == FAIL) {
                return ret;
            }
            size_t value_size_by_type = get_size_by_dtype(idx_record->type);
            void * attr_val           = PDC_calloc(1, value_size_by_type);
            memcpy(attr_val, idx_record->value + mem_offset, value_size_by_type);
            mem_offset += value_size_by_type;

            ret = delete_obj_ids_from_value_leaf((rbt_t *)value_index, attr_val, use_trie, value_size_by_type,
                                                 idx_record->obj_ids, idx_record->num_obj_ids);
        }
    }
    return ret;
}

perr_t
delete_value_from_second_level_index(key_index_leaf_content_t *leaf_content, int use_trie,
                                     IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    if (leaf_content == NULL) {
        return FAIL;
    }
    char *value_type_str = get_enum_name_by_dtype(idx_record->type);
    if (use_trie) {
        // delete the value from the prefix tree.
        ret = delete_from_value_index((void *)leaf_content->primary_trie, use_trie, idx_record);
#ifndef PDC_DART_SFX_TREE
        if (ret == FAIL) {
            return ret;
        }
        // delete the value from the trie for suffix search.
        ret = delete_from_value_index((void *)leaf_content->secondary_trie, use_trie, idx_record);
#endif
    }
    else {
        // delete the value from the primary index.
        ret = delete_from_value_index((void *)leaf_content->primary_rbt, use_trie, idx_record);
    }
    return ret;
}

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
    char *value_type_str       = get_enum_name_by_dtype(key_leaf_content->type);
    int   use_trie             = 0;
    int   count_in_value_index = 0;

    if (contains(value_type_str, "INT") || contains(value_type_str, "LONG") ||
        contains(value_type_str, "FLOAT") || contains(value_type_str, "DOUBLE")) {
        count_in_value_index = rbt_size(key_leaf_content->primary_rbt);
    }
    else if (contains(value_type_str, "STRING")) {
        use_trie             = 1;
        count_in_value_index = art_size(key_leaf_content->primary_trie);
    }
    if (count_in_value_index == 0) {
        // delete the key from the the key trie along with the key_leaf_content.
        art_delete(key_trie, (unsigned char *)key, len);
        return SUCCEED;
    }
    // delete the value part from second level index.
    ret = delete_value_from_second_level_index(key_leaf_content, use_trie, idx_record);
    return ret;
}

perr_t
idioms_local_index_delete(IDIOMS_md_idx_record_t *idx_record)
{
    perr_t ret = SUCCEED;
    // get the key and create key_index_leaf_content node for it.
    char *key = idx_record->key;
    int   len = strlen(key);

    stopwatch_t index_timer;

    timer_start(&index_timer);

    ret = delete_from_key_trie(idioms_g->art_key_prefix_tree_g, key, len, idx_record);
#ifndef PDC_DART_SFX_TREE
    if (ret == FAIL) {
        return ret;
    }
    ret = delete_from_key_trie(idioms_g->art_key_suffix_tree, reverse_str(key), len, idx_record);
#else
    // insert every suffix of the key into the trie;
    int sub_loop_count = len;
    for (int j = 1; j < sub_loop_count; j++) {
        if (ret == FAIL) {
            return ret;
        }
        char *suffix = substring(key, j, len);
        ret = delete_from_key_trie(idioms_g->art_key_prefix_tree_g, suffix, strlen(suffix), idx_record);
    }
#endif
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
    if (offset == num_obj_ids) {
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
key_index_callback(void *data, const unsigned char *key, uint32_t key_len, void *value)
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

    pdc_c_var_type_t attr_type      = leafcnt->type;
    char *           value_type_str = get_enum_name_by_dtype(attr_type);

    if (contains(value_type_str, "STRING")) {
        return value_string_query(v_query, leafcnt, idx_record);
    }
    else {
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
 * 1. exact query -> key:value
 * 2. prefix query -> key:value*
 * 3. suffix query -> key:*value
 * 4. infix query -> key:*value*
 * 5. range query -> key:>value
 * 6. range query -> key:<value
 * 7. range query -> key:>=value
 * 8. range query -> key:<=value
 * 9. range query -> key:value1|~value2
 * 10. range query -> key:value1+|value2
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
                key_index_callback((void *)idx_record, (unsigned char *)tok, strlen(tok), (void *)leafcnt);
            }
            break;
        case PATTERN_PREFIX:
            qType_string = "Prefix";
            tok          = substring(k_query, 0, strlen(k_query) - 1);
            art_iter_prefix(idioms_g->art_key_prefix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_callback, (void *)idx_record);
            break;
        case PATTERN_SUFFIX:
            qType_string = "Suffix";
            tok          = substring(k_query, 1, strlen(k_query));
#ifndef PDC_DART_SFX_TREE
            tok = reverse_str(tok);
            art_iter_prefix(idioms_g->art_key_suffix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_callback, (void *)idx_record);
#else
            leafcnt = (key_index_leaf_content_t *)art_search(idioms_g->art_key_prefix_tree_g,
                                                             (unsigned char *)tok, strlen(tok));
            if (leafcnt != NULL) {
                key_index_callback((void *)idx_record, (unsigned char *)tok, strlen(tok), (void *)leafcnt);
            }
#endif
            break;
        case PATTERN_MIDDLE:
            qType_string = "Infix";
            tok          = substring(k_query, 1, strlen(k_query) - 1);
#ifndef PDC_DART_SFX_TREE
            art_iter(idioms_g->art_key_prefix_tree_g, key_index_callback, (void *)idx_record);
#else
            art_iter_prefix(idioms_g->art_key_prefix_tree_g, (unsigned char *)tok, strlen(tok),
                            key_index_callback, (void *)idx_record);
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

int
dump_key_index_callback(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
}

perr_t
metadata_index_dump(char *dir_path)
{
    perr_t ret = SUCCEED;
    if (idioms_g == NULL) {
        return FAIL;
    }
    if (dir_path == NULL) {
        return FAIL;
    }
    char *file_path = (char *)calloc(strlen(dir_path) + 32, sizeof(char));
    sprintf(file_path, "%s/idioms_idx_%d.bin", dir_path, idioms_g->server_id_g);
    FILE *fp = fopen(file_path, "wb");
    if (fp == NULL) {
        printf("ERROR: failed to open file %s\n", file_path);
        return FAIL;
    }
    // prepare the bulki structure
    BULKI *bulki = BULKI_init(2);

    // dump the prefix tree
    char *        key1 = "prefix_tree";
    BULKI_Entity *bk1  = BULKI_ENTITY(key1, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *bv1  = empty_BULKI_Entity();

    art_iter(idioms_g->art_key_prefix_tree_g, dump_key_index_callback, bv1);
    BULKI_add(bulki, bk1, bv1);
#ifndef PDC_DART_SFX_TREE
    // dump the suffix tree
    char *        key2 = "suffix_tree";
    BULKI_Entity *bk2  = BULKI_ENTITY(key2, 1, PDC_STRING, PDC_CLS_ITEM);
    BULKI_Entity *bv2  = empty_BULKI_Entity();

    art_iter(idioms_g->art_key_suffix_tree_g, dump_key_index_callback, bv2);
    BULKI_add(bulki, bk2, bv2);
#endif
    void *buffer = BULKI_serialize(bulki);
    fwrite(buffer, 1, bulki->totalSize, fp);
    fclose(fp);
}

perr_t
metadata_index_recover(char *dir_path)
{
    perr_t ret = SUCCEED;
    if (idioms_g == NULL) {
        return FAIL;
    }
    if (dir_path == NULL) {
        return FAIL;
    }
    char *file_path = (char *)calloc(strlen(dir_path) + 32, sizeof(char));
    sprintf(file_path, "%s/idioms_idx_%d.bin", dir_path, idioms_g->server_id_g);
    FILE *fp = fopen(file_path, "rb");
    if (fp == NULL) {
        printf("ERROR: failed to open file %s\n", file_path);
        return FAIL;
    }
    // read the file into buffer
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    rewind(fp);
    void *buffer = PDC_malloc(file_size);
    fread(buffer, 1, file_size, fp);
    fclose(fp);

    // deserialize the buffer
    BULKI *bulki = BULKI_deserialize(buffer);
    if (bulki == NULL) {
        printf("ERROR: failed to deserialize the buffer\n");
        return FAIL;
    }
}