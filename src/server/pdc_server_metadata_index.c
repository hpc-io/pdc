#include "pdc_server_metadata_index.h"

#define DART_SERVER_DEBUG 0

// DART search
int64_t   indexed_word_count_g        = 0;
int64_t   server_request_count_g      = 0;
int64_t   request_per_unit_time_g     = 0;
double    unit_time_to_update_request = 5000.0; // ms.
art_tree *art_key_prefix_tree_g       = NULL;
art_tree *art_key_suffix_tree_g       = NULL;
size_t    num_kv_pairs_loaded_mdb     = 0;
size_t    num_attrs_loaded_mdb        = 0;

/****************************/
/* Initialize DART */
/****************************/
void
PDC_Server_dart_init()
{

    indexed_word_count_g   = 0;
    server_request_count_g = 0;
    art_key_prefix_tree_g  = (art_tree *)calloc(1, sizeof(art_tree));
    art_key_suffix_tree_g  = (art_tree *)calloc(1, sizeof(art_tree));

    art_tree_init(art_key_prefix_tree_g);
    art_tree_init(art_key_suffix_tree_g);
}

/****************************/
/* Create index item for KV in DART */
/****************************/

// #define PDC_DART_SFX_TREE

perr_t
create_prefix_index_for_attr_value(void **index, unsigned char *attr_value, void *data)
{
    perr_t ret = SUCCEED;
    if (*index == NULL) {
        *index = (art_tree *)PDC_calloc(1, sizeof(art_tree));
        art_tree_init(*index);
    }

    art_tree *art_value_prefix_tree = (art_tree *)*index;

    int  len        = strlen((const char *)attr_value);
    Set *obj_id_set = (Set *)art_search(art_value_prefix_tree, attr_value, len);
    if (obj_id_set == NULL) {
        obj_id_set = set_new(ui64_hash, ui64_equal);
        set_register_free_function(obj_id_set, free);
        art_insert(art_value_prefix_tree, attr_value, len, (void *)obj_id_set);
    }

    int indexed = set_insert(obj_id_set, data);

    if (indexed == -1) {
        return FAIL;
    }

    return ret;
}

art_tree *
create_index_for_attr_name(char *attr_name, char *attr_value, void *data)
{

    int                     len          = strlen(attr_name);
    key_index_leaf_content *leaf_content = NULL;
    art_tree *              nm_trie      = NULL;
    unsigned char *         nm_key       = NULL;

#ifndef PDC_DART_SFX_TREE
    int rr = 0;
    for (rr = 0; rr < 2; rr++) {
        nm_key  = (rr == 1) ? (unsigned char *)reverse_str(attr_name) : (unsigned char *)attr_name;
        nm_trie = (rr == 1) ? art_key_suffix_tree_g : art_key_prefix_tree_g;
#else
    int sub_loop_count = len; // should be 'len', but we already iterate all suffixes at client side
    nm_trie            = art_key_prefix_tree_g;
    for (int j = 0; j < sub_loop_count; j++) {
        nm_key         = (unsigned char *)substring(attr_name, j, len);
#endif
        key_index_leaf_content *leafcnt =
            (key_index_leaf_content *)art_search(nm_trie, nm_key, strlen((const char *)nm_key));
        if (leafcnt == NULL) {
            leafcnt = (key_index_leaf_content *)PDC_calloc(1, sizeof(key_index_leaf_content));
            leafcnt->extra_prefix_index = (art_tree *)PDC_calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)leafcnt->extra_prefix_index);
#ifndef PDC_DART_SFX_TREE
            // we only enable suffix index when suffix tree mode is off.
            leafcnt->extra_suffix_index = (art_tree *)PDC_calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)leafcnt->extra_suffix_index);
#endif
            // TODO: build local index for range query.
            leafcnt->extra_range_index = (art_tree *)PDC_calloc(1, sizeof(art_tree));
            art_tree_init((art_tree *)leafcnt->extra_range_index);

            art_insert(nm_trie, nm_key, strlen((const char *)nm_key), leafcnt);
        }

        art_tree *secondary_trie = NULL;

#ifndef PDC_DART_SFX_TREE
        int r = 0;
        for (r = 0; r < 2; r++) {
            unsigned char *val_key =
                (r == 1 ? (unsigned char *)reverse_str(attr_value) : (unsigned char *)attr_value);
            secondary_trie = (r == 1 ? (art_tree *)(leafcnt->extra_suffix_index)
                                     : (art_tree *)(leafcnt->extra_prefix_index));

#else
        secondary_trie = (art_tree *)(leafcnt->extra_prefix_index);
        int val_len    = strlen(attr_value);
        for (int jj = 0; jj < val_len; jj++) {
            unsigned char *val_key = (unsigned char *)substring(attr_value, jj, val_len);
#endif
            create_prefix_index_for_attr_value((void **)&secondary_trie, val_key, data);
        } // this matches with the 'r' loop or 'jj' loop
    }     // this matches with the 'rr' loop or 'j' loop
    return nm_trie;
}

perr_t
metadata_index_create(char *attr_key, char *attr_value, uint64_t obj_locator, int8_t index_type)
{
    perr_t      ret_value = FAIL;
    stopwatch_t timer;
    timer_start(&timer);
    uint64_t *data = (uint64_t *)calloc(1, sizeof(uint64_t));
    *data          = obj_locator;

    // if (index_type == DHT_FULL_HASH) {
    // FIXME: remember to check obj_locator type inside of this function below
    //     create_hash_table_for_keyword(attr_key, attr_value, strlen(attr_key), (void *)data);
    // }
    // else if (index_type == DHT_INITIAL_HASH) {
    // FIXME: remember to check obj_locator type inside of this function below
    //     create_hash_table_for_keyword(attr_key, attr_value, 1, (void *)data);
    // }
    // else if (index_type == DART_HASH) {
    create_index_for_attr_name(attr_key, attr_value, (void *)data);
    // }
    timer_pause(&timer);
    // if (DART_SERVER_DEBUG) {
    //     printf("[Server_Side_Insert_%d] Timer to insert a keyword %s : %s into index = %.4f
    //     microseconds\n",
    //            pdc_server_rank_g, attr_key, attr_value, timer_delta_us(&timer));
    // }
    indexed_word_count_g++;
    ret_value = SUCCEED;
    return ret_value;
}

/****************************/
/* Delete index item for KV in DART */
/****************************/

perr_t
delete_prefix_index_for_attr_value(void **index, unsigned char *attr_value, void *data)
{
    perr_t ret = SUCCEED;
    if (*index == NULL) {
        // println("The value prefix tree is NULL, there is nothing to delete.");
        return ret;
    }

    art_tree *art_value_prefix_tree = (art_tree *)*index;

    int  len        = strlen((const char *)attr_value);
    Set *obj_id_set = (Set *)art_search(art_value_prefix_tree, attr_value, len);
    if (obj_id_set == NULL) {
        // println("The obj_id_set is NULL, there nothing more to delete.");
        if (art_size(art_value_prefix_tree) == 0) {
            art_tree_destroy(*index);
        }
        return ret;
    }

    if (set_query(obj_id_set, data) != 0) {
        set_remove(obj_id_set, data);
    }

    if (set_num_entries(obj_id_set) == 0) {
        art_delete(art_value_prefix_tree, attr_value, len);
        set_free(obj_id_set);
    }
    return ret;
}

void
delete_index_for_attr_name(char *attr_name, char *attr_value, void *data)
{
    int                     len          = strlen(attr_name);
    key_index_leaf_content *leaf_content = NULL;
    art_tree *              nm_trie      = NULL;
    unsigned char *         nm_key       = NULL;

#ifndef PDC_DART_SFX_TREE
    int rr = 0;
    for (rr = 0; rr < 2; rr++) {
        nm_key  = rr == 1 ? (unsigned char *)reverse_str(attr_name) : (unsigned char *)attr_name;
        nm_trie = rr == 1 ? art_key_suffix_tree_g : art_key_prefix_tree_g;
#else
    int sub_loop_count = 1; // should be 'len', but we already iterate all suffixes at client side;
    nm_trie            = art_key_prefix_tree_g;
    for (int j = 0; j < sub_loop_count; j++) {
        nm_key = (unsigned char *)substring(attr_name, j, len);
#endif
        key_index_leaf_content *leafcnt =
            (key_index_leaf_content *)art_search(nm_trie, nm_key, strlen((const char *)nm_key));
        if (leafcnt == NULL) {
            art_delete(nm_trie, nm_key, strlen((const char *)nm_key));
        }
        else {
            art_tree *secondary_trie = NULL;
#ifndef PDC_DART_SFX_TREE
            int r = 0;
            for (r = 0; r < 2; r++) {
                secondary_trie = (r == 1 ? (art_tree *)(leafcnt->extra_suffix_index)
                                         : (art_tree *)(leafcnt->extra_prefix_index));
                unsigned char *val_key =
                    (r == 1 ? (unsigned char *)reverse_str(attr_value) : (unsigned char *)attr_value);
#else
            secondary_trie = (art_tree *)(leafcnt->extra_prefix_index);
            for (int jj = 0; jj < strlen(attr_value); jj++) {
                unsigned char *val_key = (unsigned char *)substring(attr_value, jj, strlen(attr_value));
#endif
                delete_prefix_index_for_attr_value((void **)&secondary_trie, val_key, data);
            }
            if (leafcnt->extra_suffix_index == NULL && leafcnt->extra_prefix_index == NULL) {
                art_delete(nm_trie, nm_key, len);
                leafcnt = NULL;
            }
            // TODO: deal with index for range query.
        } // this matches with the 'r' loop or 'jj' loop
    }     // this matches with the 'rr' loop or 'j' loop
}

perr_t
metadata_index_delete(char *attr_key, char *attr_value, uint64_t obj_locator, int8_t index_type)
{
    perr_t      ret_value = FAIL;
    stopwatch_t timer;
    timer_start(&timer);
    uint64_t *data = (uint64_t *)calloc(1, sizeof(uint64_t));
    *data          = obj_locator;

    // if (index_type == DHT_FULL_HASH) {
    //     delete_hash_table_for_keyword(attr_key, strlen(attr_key), (void *)obj_locator);
    // }
    // else if (index_type == DHT_INITIAL_HASH) {
    //     delete_hash_table_for_keyword(attr_key, 1, (void *)obj_locator);
    // }
    // else if (index_type == DART_HASH) {
    delete_index_for_attr_name(attr_key, attr_value, (void *)data);
    // }

    timer_pause(&timer);
    // if (DART_SERVER_DEBUG) {
    //     printf("[Server_Side_Delete_%d] Timer to delete a keyword %s : %s from index = %.4f
    //     microseconds\n",
    //            pdc_server_rank_g, attr_key, attr_value, timer_delta_us(&timer));
    // }
    indexed_word_count_g--;
    ret_value = SUCCEED;
    return ret_value;
}

/****************************/
/* DART Get Server Info */
/****************************/

perr_t
PDC_Server_dart_get_server_info(dart_get_server_info_in_t *in, dart_get_server_info_out_t *out)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);
    uint32_t serverId       = in->serverId;
    out->indexed_word_count = indexed_word_count_g;
    out->request_count      = server_request_count_g;
    FUNC_LEAVE(ret_value);
}

/**
 * The callback function performs on each prefix on secondary art_tree
 *
 *
 */
int
level_two_art_callback(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
    pdc_art_iterator_param_t *param = (pdc_art_iterator_param_t *)(data);
    // println("Level two start");
    if (param->level_two_infix != NULL) {
        if (contains((const char *)key, (const char *)param->level_two_infix) == 0) {
            return 0;
        }
    }
    if (value != NULL) {
        Set *       obj_id_set = (Set *)value;
        SetIterator value_set_iter;
        set_iterate(obj_id_set, &value_set_iter);

        while (set_iter_has_more(&value_set_iter)) {
            uint64_t *item      = (uint64_t *)set_iter_next(&value_set_iter);
            uint64_t *itemValue = (uint64_t *)calloc(1, sizeof(uint64_t));
            *itemValue          = *item;
            set_insert(param->out, itemValue);
        }
    }
    // println("Level two finish");
    return 0;
}

/**
 * The callback function performs on each prefix on a art_tree.
 *
 */
int
level_one_art_callback(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
    key_index_leaf_content *  leafcnt = (key_index_leaf_content *)value;
    pdc_art_iterator_param_t *param   = (pdc_art_iterator_param_t *)(data);

    if (param->level_one_infix != NULL) {
        if (contains((char *)key, param->level_one_infix) == 0) {
            return 0;
        }
    }

    char *secondary_query = param->query_str;
    // param->total_count = 0;
    // param->out = NULL;
    if (strchr(secondary_query, '~')) {
        // TODO: DO RANGE QUERY HERE. currently no solution for range query.
        //
    }
    else {
        // DO TEXT QUERY HERE.
        pattern_type_t level_two_ptn_type = determine_pattern_type(secondary_query);
        char *         tok                = NULL;
        switch (level_two_ptn_type) {
            case PATTERN_EXACT:
                tok = secondary_query;
                if (leafcnt->extra_prefix_index != NULL) {
                    Set *obj_id_set =
                        (Set *)art_search(leafcnt->extra_prefix_index, (unsigned char *)tok, strlen(tok));
                    if (obj_id_set != NULL) {
                        level_two_art_callback((void *)param, (unsigned char *)tok, strlen(tok),
                                               (void *)obj_id_set);
                    }
                }
                break;
            case PATTERN_PREFIX:
                tok = subrstr(secondary_query, strlen(secondary_query) - 1);
                if (leafcnt->extra_prefix_index != NULL) {
                    art_iter_prefix((art_tree *)leafcnt->extra_prefix_index, (unsigned char *)tok,
                                    strlen(tok), level_two_art_callback, param);
                }
                break;
            case PATTERN_SUFFIX:
                tok                      = substr(secondary_query, 1);
                art_tree *secondary_trie = NULL;
#ifndef PDC_DART_SFX_TREE
                tok            = reverse_str(tok);
                secondary_trie = (art_tree *)leafcnt->extra_suffix_index;
#else
                secondary_trie         = (art_tree *)leafcnt->extra_prefix_index;
#endif
                if (secondary_trie != NULL) {
#ifndef PDC_DART_SFX_TREE
                    art_iter_prefix(secondary_trie, (unsigned char *)tok, strlen(tok), level_two_art_callback,
                                    param);
#else
                    Set *obj_id_set = (Set *)art_search(secondary_trie, (unsigned char *)tok, strlen(tok));
                    if (obj_id_set != NULL) {
                        level_two_art_callback((void *)param, (unsigned char *)tok, strlen(tok),
                                               (void *)obj_id_set);
                    }
#endif
                }
                break;
            case PATTERN_MIDDLE:
                tok            = substring(secondary_query, 1, strlen(secondary_query) - 1);
                secondary_trie = (art_tree *)leafcnt->extra_prefix_index;
                if (secondary_trie != NULL) {
#ifndef PDC_DART_SFX_TREE
                    param->level_two_infix = tok;
                    art_iter(secondary_trie, level_two_art_callback, param);
#else
                    art_iter_prefix(secondary_trie, (unsigned char *)tok, strlen(tok), level_two_art_callback,
                                    param);
#endif
                }
                break;
            default:
                break;
        }
    }
    return 0;
}

perr_t
metadata_index_search(char *query, int index_type, uint64_t *n_obj_ids_ptr, uint64_t **buf_ptrs)
{

    perr_t      result = SUCCEED;
    stopwatch_t index_timer;

    char *kdelim_ptr = strchr(query, (int)'=');

    char *k_query = get_key(query, '=');
    char *v_query = get_value(query, '=');

    if (DART_SERVER_DEBUG) {
        println("[Server_Side_Query_%d] k_query = '%s' | v_query = '%s' ", pdc_server_rank_g, k_query,
                v_query);
    }

    pdc_art_iterator_param_t *param = (pdc_art_iterator_param_t *)calloc(1, sizeof(pdc_art_iterator_param_t));
    param->level_one_infix          = NULL;
    param->level_two_infix          = NULL;
    param->query_str                = v_query;
    param->out                      = set_new(ui64_hash, ui64_equal);
    set_register_free_function(param->out, free);

    timer_start(&index_timer);

    char *qType_string = "Exact";

    if (NULL == kdelim_ptr) {
        if (DART_SERVER_DEBUG) {
            println("[Server_Side_Query_%d]query string '%s' is not valid.", pdc_server_rank_g, query);
        }
        *n_obj_ids_ptr = 0;
        return result;
    }
    else {
        char *tok;
        // println("k_query %s, v_query %s", k_query, v_query);
        pattern_type_t          level_one_ptn_type = determine_pattern_type(k_query);
        key_index_leaf_content *leafcnt            = NULL;
        // if (index_type == DHT_FULL_HASH || index_type == DHT_INITIAL_HASH) {
        //     search_through_hash_table(k_query, index_type, level_one_ptn_type, param);
        // }
        // else {
        switch (level_one_ptn_type) {
            case PATTERN_EXACT:
                qType_string = "Exact";
                tok          = k_query;
                leafcnt = (key_index_leaf_content *)art_search(art_key_prefix_tree_g, (unsigned char *)tok,
                                                               strlen(tok));
                if (leafcnt != NULL) {
                    level_one_art_callback((void *)param, (unsigned char *)tok, strlen(tok), (void *)leafcnt);
                }
                break;
            case PATTERN_PREFIX:
                qType_string = "Prefix";
                tok          = subrstr(k_query, strlen(k_query) - 1);
                art_iter_prefix((art_tree *)art_key_prefix_tree_g, (unsigned char *)tok, strlen(tok),
                                level_one_art_callback, param);
                break;
            case PATTERN_SUFFIX:
                qType_string = "Suffix";
                tok          = substr(k_query, 1);
#ifndef PDC_DART_SFX_TREE
                tok = reverse_str(tok);
                art_iter_prefix((art_tree *)art_key_suffix_tree_g, (unsigned char *)tok, strlen(tok),
                                level_one_art_callback, param);
#else
                leafcnt = (key_index_leaf_content *)art_search(art_key_prefix_tree_g, (unsigned char *)tok,
                                                               strlen(tok));
                if (leafcnt != NULL) {
                    level_one_art_callback((void *)param, (unsigned char *)tok, strlen(tok), (void *)leafcnt);
                }
#endif
                break;
            case PATTERN_MIDDLE:
                qType_string = "Infix";
                tok          = substring(k_query, 1, strlen(k_query) - 1);
#ifndef PDC_DART_SFX_TREE
                param->level_one_infix = tok;
                art_iter(art_key_prefix_tree_g, level_one_art_callback, param);
#else
                art_iter_prefix(art_key_prefix_tree_g, (unsigned char *)tok, strlen(tok),
                                level_one_art_callback, param);
#endif
                break;
            default:
                break;
        }
        // }
    }

    uint32_t i = 0;

    *n_obj_ids_ptr = set_num_entries(param->out);
    *buf_ptrs      = (uint64_t *)calloc(*n_obj_ids_ptr, sizeof(uint64_t));

    SetIterator iter;
    set_iterate(param->out, &iter);
    while (set_iter_has_more(&iter)) {
        uint64_t *item = (uint64_t *)set_iter_next(&iter);
        (*buf_ptrs)[i] = *item;
        i++;
    }
    set_free(param->out);

    timer_pause(&index_timer);
    if (DART_SERVER_DEBUG) {
        printf("[Server_Side_%s_%d] Time to address query '%s' and get %d results  = %.4f microseconds\n",
               qType_string, pdc_server_rank_g, query, *n_obj_ids_ptr, timer_delta_us(&index_timer));
    }
    server_request_count_g++;
    return result;
}

perr_t
PDC_Server_dart_perform_one_server(dart_perform_one_server_in_t *in, dart_perform_one_server_out_t *out,
                                   uint64_t *n_obj_ids_ptr, uint64_t **buf_ptrs)
{
    perr_t                 result    = SUCCEED;
    dart_op_type_t         op_type   = in->op_type;
    dart_hash_algo_t       hash_algo = in->hash_algo;
    char *                 attr_key  = (char *)in->attr_key;
    char *                 attr_val  = (char *)in->attr_val;
    dart_object_ref_type_t ref_type  = in->obj_ref_type;

    uint64_t obj_locator = in->obj_primary_ref;
    if (ref_type == REF_PRIMARY_ID) {
        obj_locator = in->obj_primary_ref;
    }
    else if (ref_type == REF_SECONDARY_ID) {
        obj_locator = in->obj_secondary_ref;
    }
    else if (ref_type == REF_SERVER_ID) {
        obj_locator = in->obj_server_ref;
    }
    out->has_bulk = 0;
    // printf("Respond to: in->op_type=%d\n", in->op_type );
    if (op_type == OP_INSERT) {
        metadata_index_create(attr_key, attr_val, obj_locator, hash_algo);
    }
    else if (op_type == OP_DELETE) {
        metadata_index_delete(attr_key, attr_val, obj_locator, hash_algo);
    }
    else {
        char *query  = (char *)in->attr_key;
        result       = metadata_index_search(query, hash_algo, n_obj_ids_ptr, buf_ptrs);
        out->n_items = (*n_obj_ids_ptr);
        if ((*n_obj_ids_ptr) > 0) {
            out->has_bulk = 1;
        }
    }
    return result;
}

/**
 * This is a object ID set
 * |number of object IDs = n|object ID 1|...|object ID n|
 */
uint64_t
append_obj_id_set(Set *obj_id_set, FILE *stream)
{
    uint64_t num_obj_id = set_num_entries(obj_id_set);
    miqs_append_uint64(num_obj_id, stream);
    SetIterator iter;
    set_iterate(obj_id_set, &iter);
    while (set_iter_has_more(&iter)) {
        uint64_t *item = (uint64_t *)set_iter_next(&iter);
        miqs_append_uint64(*item, stream);
    }
    return num_obj_id;
}

/**
 * This is a string value node
 * |str_val|file_obj_pair_list|
 */
int
append_string_value_node(void *fh, const unsigned char *key, uint32_t key_len, void *value)
{
    FILE *stream = (FILE *)fh;
    // 1. append string value
    miqs_append_string_with_len((char *)key, (size_t)key_len, stream);
    // 2. append object ID set.
    Set *obj_id_set = (Set *)value;
    append_obj_id_set(obj_id_set, stream);
    return 0; // return 0 for art iteration to continue;
}

/**
 * This is the string value region
 * |type = 3|number of values = n|value_node_1|...|value_node_n|
 *
 * return number of strings in the string value tree
 */
int
append_string_value_tree(art_tree *art, FILE *stream)
{
    // 1. type
    miqs_append_type(3, stream);
    // 2. number of values
    uint64_t num_str_value = art_size(art);
    miqs_append_uint64(num_str_value, stream);
    // 3. value nodes
    int rst = art_iter(art, append_string_value_node, stream);
    return rst == 0 ? num_str_value : 0;
}

/**
 * return number of attribute values
 * This is an attribute node
 * |attr_name|attr_value_region|
 */
int
append_attr_name_node(void *fh, const unsigned char *key, uint32_t key_len, void *value)
{
    int   rst    = 0;
    FILE *stream = (FILE *)fh;
    // 1. attr name
    char *attr_name = (char *)key;
    miqs_append_string_with_len(attr_name, (size_t)key_len, stream);
    // 2. attr value region
    key_index_leaf_content *key_index_leaf = (key_index_leaf_content *)value;
    if (key_index_leaf->data_type == STRING) {
        rst = append_string_value_tree(key_index_leaf->extra_prefix_index, stream);
#ifndef PDC_DART_SFX_TREE
        rst = append_string_value_tree(key_index_leaf->extra_suffix_index, stream);
#endif
    }
    printf("number of attribute values = %d\n", rst);
    return 0; // return 0 for art iteration to continue;
}

/**
 * This is the "attribute region"
 * |number of attributes = n|attr_node 1|...|attr_node n|
 */
int
append_attr_root_tree(art_tree *art, FILE *stream)
{
    uint64_t num_str_value = art_size(art);
    miqs_append_uint64(num_str_value, stream);
    return art_iter(art, append_attr_name_node, stream);
}

perr_t
metadata_index_dump(uint32_t serverID)
{
    perr_t ret_value = SUCCEED;
    // 1. open file
    char file_name[1024];
    sprintf(file_name, "dart_index_%d.bin", serverID);
    FILE *stream = fopen(file_name, "wb");

    // 2. append attribute region
    append_attr_root_tree(art_key_prefix_tree_g, stream);
#ifndef PDC_DART_SFX_TREE
    append_attr_root_tree(art_key_suffix_tree_g, stream);
#endif
    // 3. close file
    fclose(stream);
    return ret_value;
}

int
read_attr_value_node(art_tree *art_value_index, FILE *stream)
{
    // 1. read value
    char *val = miqs_read_string(stream);

    Set *obj_id_set = set_new(ui64_hash, ui64_equal);
    set_register_free_function(obj_id_set, free);

    // 2. read object ID set
    uint64_t *num_obj_id = miqs_read_uint64(stream);
    uint64_t  i          = 0;
    for (i = 0; i < *num_obj_id; i++) {
        uint64_t *obj_id = miqs_read_uint64(stream);
        set_insert(obj_id_set, obj_id);
    }
    // 3. insert value into art tree
    art_insert(art_value_index, (const unsigned char *)val, strlen(val), (void *)obj_id_set);
    int rst = (set_num_entries(obj_id_set) == *num_obj_id) ? 0 : 1;
    return rst;
}

int
read_attr_values(art_tree *art_value_index, FILE *stream)
{
    int rst = 0;

    // key_index_leaf_node->extra_prefix_index = (art_tree *)calloc(1, sizeof(art_tree));
    art_tree_init(art_value_index);

    uint64_t *num_values = miqs_read_uint64(stream);
    uint64_t  i          = 0;
    for (i = 0; i < *num_values; i++) {
        rst = rst | read_attr_value_node(art_value_index, stream);
    }
    return rst;
}

int
read_attr_name_node(art_tree *art_key_index, FILE *stream)
{
    char *                  attr_name = miqs_read_string(stream);
    key_index_leaf_content *key_index_leaf_node =
        (key_index_leaf_content *)calloc(1, sizeof(key_index_leaf_content));
    int *                     tv   = miqs_read_int(stream);
    dart_indexed_value_type_t type = (dart_indexed_value_type_t)(*tv);
    key_index_leaf_node->data_type = type;
    art_insert(art_key_index, (const unsigned char *)attr_name, strlen(attr_name), key_index_leaf_node);

    key_index_leaf_node->extra_prefix_index = (art_tree *)calloc(1, sizeof(art_tree));
    int rst = read_attr_values(key_index_leaf_node->extra_prefix_index, stream);
#ifndef PDC_DART_SFX_TREE
    key_index_leaf_node->extra_suffix_index = (art_tree *)calloc(1, sizeof(art_tree));
    rst = rst | read_attr_values(key_index_leaf_node->extra_suffix_index, stream);
#endif
    return rst;
}

perr_t
metadata_index_recover(uint32_t serverID)
{
    perr_t ret_value = SUCCEED;

    // 1. open file
    char file_name[1024];
    sprintf(file_name, "dart_index_%d.bin", serverID);
    FILE *stream = fopen(file_name, "rb");

    // 2. read attribute region
    num_kv_pairs_loaded_mdb = 0;
    num_attrs_loaded_mdb    = 0;
    uint64_t *num_attrs     = miqs_read_uint64(stream);
    num_attrs_loaded_mdb    = (size_t)num_attrs;
    uint64_t i              = 0;
    int      rst            = 0;
    for (i = 0; i < *num_attrs; i++) {
        rst = rst | read_attr_name_node(art_key_prefix_tree_g, stream);
    }

#ifndef PDC_DART_SFX_TREE
    num_attrs            = miqs_read_uint64(stream);
    num_attrs_loaded_mdb = (size_t)num_attrs;
    rst                  = 0;
    for (i = 0; i < *num_attrs; i++) {
        rst = rst | read_attr_name_node(art_key_suffix_tree_g, stream);
    }

#endif
    // 3. close file
    fclose(stream);
    ret_value = (rst != 0);

    return ret_value;
}