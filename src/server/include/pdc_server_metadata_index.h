#ifndef PDC_SERVER_METADATA_INDEX_H
#define PDC_SERVER_METADATA_INDEX_H

#include "pdc_client_server_common.h"

// #include "hashtab.h"
#include "query_utils.h"
#include "timer_utils.h"
#include "art.h"
#include "pdc_set.h"
#include "pdc_hash.h"
#include "pdc_compare.h"
#include "dart_core.h"
#include "pdc_hash-table.h"
#include "bin_file_ops.h"

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
    char *   query_str;
    char *   level_one_infix;
    char *   level_two_infix;
    uint32_t total_count;
    Set *    out;
} pdc_art_iterator_param_t;

/**
 * @brief Initialize the ART index
 */
void PDC_Server_dart_init();

/**
 * @brief Get the server information for the metadata index
 * @param in [IN] Input parameters for the server info
 * @param out [OUT] Output parameters for the server info
 * @return perr_t SUCCESS on success, FAIL on failure
 */
perr_t PDC_Server_dart_get_server_info(dart_get_server_info_in_t *in, dart_get_server_info_out_t *out);

/**
 * @brief Perform various of DART operations on one single server.
 * @param in [IN] Input parameters for the DART operation
 * @param out [OUT] Output parameters for the DART operation
 * @return perr_t SUCCESS on success, FAIL on failure
 */
perr_t PDC_Server_dart_perform_one_server(dart_perform_one_server_in_t * in,
                                          dart_perform_one_server_out_t *out, uint64_t *n_obj_ids_ptr,
                                          uint64_t **buf_ptrs);

#endif /* PDC_SERVER_METADATA_INDEX_H */