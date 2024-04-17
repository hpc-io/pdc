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

/****************************/
/* Index Dump               */
/****************************/

// ********************* Index Dump and Load *********************

// The buffer will contain:
// |buffer_size|buffer_region|
// And each buffer_region will contain
// |num_keys|key_1|value_region|key_2|value_region|
// each value_region will contain
// |type_code_1|num_values_1|value_11|ID_set_region|value_12|ID_set_region|...|type_code_2|num_values_2|value_21|ID_set_region|value_22|ID_set_region|value22|ID_set_region|...
// each ID_set_region will contain
// |num_obj_id|obj_id_1|obj_id_2|...|obj_id_n|
void *
append_buffer(index_buffer_t *buffer, void *new_buf, uint64_t new_buf_size)
{
    if (sizeof(uint64_t) + buffer->buffer_size + new_buf_size > buffer->buffer_capacity) {
        size_t new_capacity = (buffer->buffer_size + new_buf_size + sizeof(uint64_t)) * 2;
        void * new_buffer   = realloc(buffer->buffer, buffer->buffer_capacity);
        if (new_buffer == NULL) {
            printf("ERROR: realloc failed\n");
            return NULL;
        }
        buffer->buffer_capacity = new_capacity;
        buffer->buffer          = new_buffer;
    }
    // append the size of the new buffer
    memcpy(buffer->buffer + buffer->buffer_size, &new_buf_size, sizeof(uint64_t));
    buffer->buffer_size += sizeof(uint64_t);
    memcpy(buffer->buffer + buffer->buffer_size, new_buf, new_buf_size);
    buffer->buffer_size += new_buf_size;
    return buffer->buffer;
}

/**
 * This is a object ID set
 * |number of object IDs = n|object ID 1|...|object ID n|
 *
 * validated.
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
    HashTable *               vnode_buf_ht = (HashTable *)data; // data is the parameter passed in
    // the hash table is used to store the buffer struct related to each vnode id.
    index_buffer_t *buffer = hash_table_lookup(vnode_buf_ht, &(leafcnt->virtural_node_id));
    if (buffer == NULL) {
        buffer                  = (index_buffer_t *)calloc(1, sizeof(index_buffer_t));
        buffer->buffer_capacity = 1024;
        buffer->buffer          = calloc(1, buffer->buffer_capacity);
        // note that the first 8 bytes are reserved for the number of keys, in the type of uint64_t.
        hash_table_insert(vnode_buf_ht, &(leafcnt->virtural_node_id), buffer);
    }
    int rst = 0;
    // 1. append attr name
    append_buffer(buffer, (void *)key, key_len);
    buffer->num_keys++;

    // 2. attr value type
    int8_t *type = (int8_t *)calloc(1, sizeof(int8_t));
    type[0]      = leafcnt->val_idx_dtype;
    append_buffer(buffer, type, sizeof(int8_t));

    // 3. attr value simple type, we probably don't need simple type.
    // int8_t *simple_type = (int8_t *)calloc(1, sizeof(int8_t));
    // simple_type[0]      = leafcnt->simple_value_type;
    // append_buffer(buffer, simple_type, sizeof(int8_t));
    if (getCompoundTypeFromBitmap(leafcnt->val_idx_dtype) == PDC_STRING) {
        if (leafcnt->primary_trie != NULL) {
            rst |= append_string_value_tree(leafcnt->primary_trie, buffer);
        }
        if (leafcnt->secondary_trie != NULL) {
            rst |= append_string_value_tree(leafcnt->secondary_trie, buffer);
        }
    }
    if (getNumericalTypeFromBitmap(leafcnt->val_idx_dtype) != PDC_UNKNOWN) {
        if (leafcnt->primary_rbt != NULL) {
            rst |= append_numeric_value_tree(leafcnt->primary_rbt, buffer);
        }
        if (leafcnt->secondary_rbt != NULL) {
            rst |= append_numeric_value_tree(leafcnt->secondary_rbt, buffer);
        }
    }
    // printf("number of attribute values = %d\n", rst);
    return 0; // return 0 for art iteration to continue;
}

/**
 * Append the IDIOMS root to the file
 *
 */
int
append_attr_root_tree(art_tree *art, char *dir_path, char *base_name, uint32_t serverID)
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
        uint64_t *      vid    = pair.key;
        index_buffer_t *buffer = pair.value;
        char            file_name[1024];
        // and this is why do we need to differentiate the file name by the server ID.
        sprintf(file_name, "%s/%s_%" PRIu32 "_%" PRIu64 ".bin", dir_path, base_name, serverID, *vid);
        LOG_INFO("Writing index to file_name: %s\n", file_name);
        FILE *stream = fopen(file_name, "wb");
        fwrite(&(buffer->buffer_size), sizeof(uint64_t), 1, stream);
        fwrite(buffer->buffer, 1, buffer->buffer_size, stream);
        fclose(stream);
    }
    return rst;
}

perr_t
idioms_metadata_index_dump(IDIOMS_t *idioms, char *dir_path, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;

    stopwatch_t timer;
    timer_start(&timer);

    // 2. append attribute region
    append_attr_root_tree(idioms->art_key_prefix_tree_g, dir_path, "idioms_prefix", serverID);

    append_attr_root_tree(idioms->art_key_suffix_tree_g, dir_path, "idioms_suffix", serverID);

    timer_pause(&timer);
    println("[IDIOMS_Index_Dump_%d] Timer to dump index = %.4f microseconds\n", serverID,
            timer_delta_us(&timer));
    return ret_value;
}

// *********************** Index Loading ***********************************

size_t
read_buffer_as_string(index_buffer_t *buffer, char **dst, size_t *size)
{
    uint64_t dsize;
    memcpy(&dsize, buffer->buffer, sizeof(uint64_t));
    buffer->buffer += sizeof(uint64_t);
    buffer->buffer_size -= sizeof(uint64_t);
    *size = dsize;

    *dst = calloc(1, (*size) + 1);
    memcpy(*dst, buffer->buffer, *size);
    (*dst)[*size] = '\0';
    buffer->buffer += *size;
    buffer->buffer_size -= *size;
    return *size;
}

size_t
read_buffer(index_buffer_t *buffer, void **dst, size_t *size)
{
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
read_attr_name_node(art_tree *art_key_index, char *dir_path, char *base_name, uint32_t serverID,
                    uint64_t vnode_id)
{
    char file_name[1024];
    sprintf(file_name, "%s/%s_%d_%llu.bin", dir_path, base_name, vnode_id, serverID);
    LOG_INFO("Loading Index from file_name: %s\n", file_name);
    // check file existence
    if (access(file_name, F_OK) == -1) {
        return FAIL;
    }
    FILE *stream = fopen(file_name, "rb");
    if (stream == NULL) {
        return FAIL;
    }
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
        read_buffer_as_string(buffer, &attr_name, &key_len);

        LOG_DEBUG("attr_name: %s\n", attr_name);

        // 2. read attr value type
        int8_t *type;
        size_t  len;
        read_buffer(buffer, (void **)&type, &len);
        leafcnt->val_idx_dtype = type[0];

        LOG_DEBUG("val_idx_dtype: %d\n", leafcnt->val_idx_dtype);

        // // 3. attr value simple type.
        // int8_t *simple_type;
        // read_buffer(buffer, (void **)&simple_type, &len);
        // leafcnt->simple_value_type = simple_type[0];

        // 4. read attr values
        void *value_index = NULL;
        if (leafcnt->val_idx_dtype == 3) {
            leafcnt->primary_trie = (art_tree *)calloc(1, sizeof(art_tree));
            value_index           = leafcnt->primary_trie;

            read_value_tree(value_index, leafcnt->val_idx_dtype, buffer);

            leafcnt->secondary_trie = (art_tree *)calloc(1, sizeof(art_tree));
            value_index             = leafcnt->secondary_trie;

            read_value_tree(value_index, leafcnt->val_idx_dtype, buffer);
        }
        else {
            libhl_cmp_callback_t compare_func = NULL;
            if (leafcnt->val_idx_dtype == 0) { // UINT64
                compare_func = LIBHL_CMP_CB(PDC_UINT64);
            }
            else if (leafcnt->val_idx_dtype == 1) { // INT64
                compare_func = LIBHL_CMP_CB(PDC_INT64);
            }
            else if (leafcnt->val_idx_dtype == 2) { // DOUBLE
                compare_func = LIBHL_CMP_CB(PDC_DOUBLE);
            }
            else {
                printf("ERROR: unsupported data type %d\n", leafcnt->val_idx_dtype);
                return FAIL;
            }
            leafcnt->primary_rbt = (rbt_t *)rbt_create(compare_func, free);
            value_index          = leafcnt->primary_rbt;

            read_value_tree(value_index, leafcnt->val_idx_dtype, buffer);
        }

        // 5. insert the key into the key trie along with the key_leaf_content.
        art_insert(art_key_index, (const unsigned char *)attr_name, key_len, leafcnt);
    }
    return rst;
}

void
init_dart_space_via_idioms(DART *dart, int num_server, int max_server_num_to_adapt)
{
    int extra_tree_height  = 0;
    int replication_factor = 3;
    replication_factor     = replication_factor > 0 ? replication_factor : 2;
    dart_space_init(dart, num_server, IDIOMS_DART_ALPHABET_SIZE, extra_tree_height, replication_factor,
                    num_server);
}

perr_t
idioms_metadata_index_recover(IDIOMS_t *idioms, char *dir_path, int num_server, uint32_t serverID)
{
    perr_t ret_value = SUCCEED;

    stopwatch_t timer;
    timer_start(&timer);

    DART *dart_info = (DART *)calloc(1, sizeof(DART));
    init_dart_space_via_idioms(dart_info, num_server, IDIOMS_MAX_SERVER_COUNT_TO_ADAPT);

    uint64_t *vid_array = NULL;
    size_t    num_vids  = get_vnode_ids_by_serverID(dart_info, serverID, &vid_array);

    IDIOMS_init(serverID, num_server);

    // load the attribute region for each vnode
    for (size_t vid = 0; vid < num_vids; vid++) {
        for (size_t sid = 0; sid < num_server; sid++) {
            read_attr_name_node(idioms->art_key_prefix_tree_g, dir_path, "idioms_prefix", sid,
                                vid_array[vid]);

            read_attr_name_node(idioms->art_key_suffix_tree_g, dir_path, "idioms_suffix", sid,
                                vid_array[vid]);
        }
    }

    timer_pause(&timer);
    println("[IDIOMS_Index_Recover_%d] Timer to recover index = %.4f microseconds\n", serverID,
            timer_delta_us(&timer));
    return ret_value;
}
