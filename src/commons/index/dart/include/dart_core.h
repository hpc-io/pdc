/**
 * The purpose of this file is to provide all the function declarations in DART library.
 * DART should be designed as an library/framework handling the distributed inverted index.
 * The current version should focus on managing distributed inverted index in HPC environment.
 *
 * Alternative version will be provided using JVM compliant language.
 */

#ifndef DART_H
#define DART_H

#include <string.h>
#include "dart_utils.h"
#include "dart_algo.h"
#include "dart_math.h"
#include "thpool.h"
#include "string_utils.h"
#include "pdc_config.h"

typedef enum { NUMERIC = 1, TIME = 2, CHAR = 3, BINARY = 4 } dart_indexed_value_type_t;

typedef enum { DHT_FULL_HASH = 1, DHT_INITIAL_HASH = 2, DART_HASH = 3 } dart_hash_algo_t;

typedef enum {
    OP_INSERT       = 1,
    OP_EXACT_QUERY  = 2,
    OP_PREFIX_QUERY = 3,
    OP_SUFFIX_QUERY = 4,
    OP_INFIX_QUERY  = 5,
    OP_DELETE       = 6
} dart_op_type_t;

typedef enum { REF_PRIMARY_ID = 1, REF_SECONDARY_ID = 2, REF_SERVER_ID = 3 } dart_object_ref_type_t;

typedef struct {
    uint32_t  id;
    uint32_t  num_dst_server;
    uint32_t *dst_server_ids;
} dart_vnode;

typedef struct {
    int         alphabet_size;
    int         dart_tree_height;
    int         replication_factor;
    int         client_request_count;
    uint32_t    num_client;
    uint32_t    num_server;
    uint64_t    num_vnode;
    dart_vnode *vnodes;
    int         suffix_tree_mode;
} DART;

typedef struct {
    uint32_t id;
    DART *   dart;
} dart_client;

typedef struct {
    uint32_t id;
    int64_t  indexed_word_count;
    int64_t  request_count;
} dart_server;

typedef struct {
    uint32_t server_id;
    char *   key;
} index_hash_result_t;

// Defining a function pointer by which the server load information can be retrieved.
// The returning data type should be dart_server, which is a struct.
// The parameter should be a uint32_t.
// The function name can be anything.
typedef dart_server (*get_server_info_callback)(uint32_t server_id);

/**
 * Initialize the DART space.
 *
 *
 */
void dart_space_init(DART *dart, int num_client, int num_server, int alphabet_size, int extra_tree_height,
                     int replication_factor);

/**
 * This function make the client request counter increment by 1.
 * ** We should consider to make this as an atomic operation.
 */
int dart_client_request_count_incr(DART *dart_g);

/**
 * A utility function for dummies.
 * Get server id by given virtual node id.
 */
uint64_t get_server_id_by_vnode_id(DART *dart, uint64_t vnode_id);

/**
 * This function is for getting the base virtual node ID by a given string.
 *
 */
uint64_t get_base_virtual_node_id_by_string(DART *dart, char *str);

/**
 * This function is for getting the alternative virtual node ID.
 *
 */
uint64_t get_reconciled_vnode_id_with_power_of_two_choice_rehashing(DART *dart, uint64_t base_vnode_idx,
                                                                    char *                   word,
                                                                    get_server_info_callback get_server_cb);

/**
 * Get IDs of all virtual nodes of replicas by given string and overall tree-height setting.
 *
 *
 * return The length of the ID array.
 */
int get_replica_node_ids(DART *dart, uint64_t master_node_id, int is_physical, uint64_t **out);

/**
 * This function takes a keyword, creates an index record in DART and returns all server ids
 * including replica servers. The caller of this function should perform index create PRC call
 * to all the servers specified by the ID list.
 *
 * \param dart_g        [IN]            Pointer of Global state of DART
 * \param keyword       [IN]            The keyword which should be indexed.
 * \param get_server_cb [IN]            A callback function which determines the lighter-load server between
 * two servers. \param out           [OUT]           The pointer of the server ID array. \return The length of
 * the server ID.
 */
int get_server_ids_for_insert(DART *dart_g, char *keyword, get_server_info_callback get_server_cb,
                              uint64_t **out);

/**
 * This function gives all the server ID according to the given query token.
 * The query token should not contain any wildcard sign.
 *
 * Once the server IDs are retrieved, the query should be broadcasted to all these servers.
 *
 * Also, this function can be used for index updating and index deletion operations.
 *
 * Notice that, for index updating and index deletion operations, the given token must represent
 * an exact keyword, thus, the result will be an ID list of two servers which are the possible
 * locations of the specified keyword.
 *
 * Two different type of updating operations should be considered.
 *
 * 1. Add/Remove one ID from the set of IDs related to the keywords.
 * 2. Updating the attribute value of an object according to object ID.
 *
 * *** If bulk transfer may overwhelm the system,
 * *** We need to define a protocol for client and server to communicate without bulk transfer.
 *
 *
 * \param dart_g        [IN]        Pointer of Global state of DART
 * \param token         [IN]        The token which will searched for. It should not contain any wildcard
 * sign. \param op_type       [IN]        Give the operation type. \param out           [IN/OUT]    The
 * pointer of the server ID array. To accelerate, we require caller function to pass an ID array of length M
 * into this function, where M is the number of total physical servers. Then, this function will fill up this
 * array. \return              [OUT]       The valid length of the server ID array.
 */
int get_server_ids_for_query(DART *dart_g, char *token, dart_op_type_t is_prefix, uint64_t **out);

/**
 *  This is a routing function for different possible operations in DART.
 *  It will only generate the array of IDs of servers that client should touch.
 *
 *  The return value is the length of valid elements in the array.
 *  A for loop can use the return value as the upper bound to iterate all elements in the array.
 *
 * \param dart_g        [IN]        Pointer of Global state of DART
 * \param key           [IN]        The given key. For queries, it is already a bare keyword without '*'.
 * \param op_type       [IN]        Give the operation type.
 * \param get_server_cb [IN]        The callback function for getting the statistical information of a server.
 * Signature: dart_server (*get_server_info_callback)(uint32_t server_id) \param out           [IN/OUT]    The
 * pointer of the server ID array. To accelerate, we require caller function to pass an ID array of length M
 * into this function, where M is the number of total physical servers. Then, this function will fill up this
 * array. \return              [OUT]       The valid length of the server ID array.
 *
 */
int DART_hash(DART *dart_g, char *key, dart_op_type_t op_type, get_server_info_callback get_server_cb,
              index_hash_result_t **out);

/**
 * This function performs DHT hashing, solely for comparison purpose.
 *
 * out will take an array of the IDs of all servers which should be accessed
 * return value is the length of the array.
 */
int DHT_hash(DART *dart_g, size_t len, char *key, dart_op_type_t op_type, index_hash_result_t **out);

/**
 * This is a quick function to test if the operation type any type of operation that will write to the index.
 * These operations currently include: insert, delete.
 *
 * @param op_type [IN] The operation type.
 * @return 1 if the operation type is for index write operations, 0 otherwise.
 */
int is_index_write_op(dart_op_type_t op_type);

#endif // DART_H