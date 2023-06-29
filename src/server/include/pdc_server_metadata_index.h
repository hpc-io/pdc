#ifndef PDC_SERVER_METADATA_INDEX_H
#define PDC_SERVER_METADATA_INDEX_H

#include "dart_core.h"
#include "pdc_client_server_common.h"

typedef struct {
    // On the leaf of ART, we maintain a hash table of IDs of all objects containing that key.
    HashTable *server_id_obj_id_table;

    dart_indexed_value_type_t data_type;
    // Also, for key lookup ART, we also maintain the pointer to the value tree
    void *extra_prefix_index;
    void *extra_suffix_index;
    void *extra_range_index;
    void *extra_infix_index;
} key_index_leaf_content;

typedef struct pdc_art_iterator_param {
    char *    query_str;
    char *    level_one_infix;
    char *    level_two_infix;
    uint32_t  total_count;
    hashset_t out;
} pdc_art_iterator_param_t;

void   PDC_Server_dart_init();
perr_t PDC_Server_metadata_index_create(metadata_index_create_in_t *in, metadata_index_create_out_t *out);
perr_t PDC_Server_metadata_index_delete(metadata_index_delete_in_t *in, metadata_index_delete_out_t *out);
perr_t PDC_Server_dart_get_server_info(dart_get_server_info_in_t *in, dart_get_server_info_out_t *out);
perr_t PDC_Server_metadata_index_search(metadata_index_search_in_t *in, metadata_index_search_out_t *out,
                                        uint64_t *n_obj_ids_ptr, uint64_t ***buf_ptrs);
perr_t PDC_Server_dart_perform_one_server(dart_perform_one_server_in_t * in,
                                          dart_perform_one_server_out_t *out, uint64_t *n_obj_ids_ptr,
                                          uint64_t ***buf_ptrs);

#endif /* PDC_SERVER_METADATA_INDEX_H */