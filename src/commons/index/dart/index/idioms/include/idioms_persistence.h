#ifndef IDIOMS_PERSISTENCE_H
#define IDIOMS_PERSISTENCE_H

#include "idioms_local_index.h"

/**
 * @brief Dumping the index to a file.
 * @param dir_path  The directory path to store the index file.
 * @param serverID  The server ID.
 * @return perr_t SUCCESS on success, FAIL on failure
 */
perr_t idioms_metadata_index_dump(IDIOMS_t *idioms, char *dir_path, uint32_t serverID);

/**
 * @brief Recovering the index from a file. Please initialize idioms before calling this function.
 * @param dir_path  The directory path to store the index file.
 * @param num_server  The number of servers.
 * @param serverID  The server ID.
 * @return perr_t SUCCESS on success, FAIL on failure
 */
perr_t idioms_metadata_index_recover(IDIOMS_t *idioms, char *dir_path, int num_server, uint32_t serverID);

// /**
//  * @brief Initialize the DART space via idioms.
//  * @param dart  The DART space to be initialized.
//  * @param num_client  The number of clients.
//  * @param num_server  The number of servers.
//  * @param max_server_num_to_adapt  The maximum number of servers to adapt.
//  */
// void init_dart_space_via_idioms(DART *dart, int num_client, int num_server, int max_server_num_to_adapt);

#endif // IDIOMS_PERSISTENCE_H