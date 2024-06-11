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
#include "pdc_hash_table.h"
#include "bin_file_ops.h"

/**
 * @brief Initialize local index
 * @param num_server  The number of servers
 * @param server_id  The server ID
 */
void PDC_Server_metadata_index_init(uint32_t num_server, uint32_t server_id);

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
/**
 * @brief Dumping the index to a file.
 * @param checkpoint_dir  The directory path to store the index file.
 * @param serverID  The server ID.
 * @return perr_t SUCCESS on success, FAIL on failure
 */
perr_t metadata_index_dump(char *checkpoint_dir, uint32_t serverID);

/**
 * @brief Recovering the index from a file. Please initialize idioms before calling this function.
 * @param checkpiont_dir  The directory path to store the index file.
 * @param num_server  The number of servers.
 * @param serverID  The server ID.
 */
perr_t metadata_index_recover(char *checkpiont_dir, int num_server, uint32_t serverID);

#endif /* PDC_SERVER_METADATA_INDEX_H */