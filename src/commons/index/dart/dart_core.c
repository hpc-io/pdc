#include "dart_utils.h"
#include "dart_algo.h"
#include "dart_math.h"
#include "dart_core.h"

threadpool dart_thpool_g;

threadpool
get_dart_temp_thpool(int count)
{
    return thpool_init(count);
}

threadpool
get_dart_thpool_g()
{
    return dart_thpool_g;
}

int
is_index_write_op(dart_op_type_t op_type)
{
    return (op_type == OP_INSERT || op_type == OP_DELETE);
}

void
dart_space_init(DART *dart, int num_client, int num_server, int alphabet_size, int extra_tree_height,
                int replication_factor, int max_server_num_to_adapt)
{
    if (dart == NULL) {
        dart = (DART *)calloc(1, sizeof(DART));
    }
    dart->alphabet_size = alphabet_size;
    // initialize clients;
    dart->num_client = num_client;
    // initialize servers;
    dart->num_server = num_server;

    double physical_node_num =
        max_server_num_to_adapt == 0 ? (double)num_server : (double)max_server_num_to_adapt;

    dart->dart_tree_height =
        (int)ceil(log_with_base((double)dart->alphabet_size, physical_node_num)) + 1 + extra_tree_height;
    // calculate number of all leaf nodes
    dart->num_vnode          = (uint64_t)pow(dart->alphabet_size, dart->dart_tree_height);
    dart->replication_factor = replication_factor;
    dart_thpool_g            = thpool_init(num_server);
    dart->suffix_tree_mode   = 1;
}
/**
 * A utility function for dummies.
 * Get server id by given virtual node id.
 */
uint64_t
get_server_id_by_vnode_id(DART *dart, uint64_t vnode_id)
{
    // printf("vnode_id = %d, dart->num_server = %d\n", vnode_id, dart->num_server);
    int num_vnode_per_server = dart->num_vnode / dart->num_server;
    return (vnode_id / num_vnode_per_server) % dart->num_server;
}

size_t
get_vnode_ids_by_serverID(DART *dart, uint32_t serverID, uint64_t **out)
{
    if (out == NULL) {
        return 0;
    }
    size_t    num_result = 0;
    uint64_t *temp_out   = (uint64_t *)calloc(dart->num_vnode, sizeof(uint64_t));
    int       vid        = 0;
    for (vid = 0; vid < dart->num_vnode; vid++) {
        if (get_server_id_by_vnode_id(dart, vid) == serverID) {
            temp_out[vid] = vid;
            num_result++;
        }
    }
    out[0] = (uint64_t *)calloc(num_result, sizeof(uint64_t));
    memcpy(out[0], temp_out, num_result * sizeof(uint64_t));
    free(temp_out);
    return num_result;
}

/**
 * This function make the client request counter increment by 1.
 *
 */
int
dart_client_request_count_incr(DART *dart_g)
{
    if (dart_g->client_request_count < 0) {
        dart_g->client_request_count = 0;
    }
    dart_g->client_request_count = dart_g->client_request_count + 1;
    return dart_g->client_request_count;
}

/**
 * This function is for getting the base virtual node ID by a given string.
 *
 */
uint64_t
get_base_virtual_node_id_by_string(DART *dart, char *str)
{
    int      n = 0;
    uint64_t c;
    uint64_t rst = 0;
    uint64_t i_t_n;
    int      met_end = 0;
    for (n = 1; n <= dart->dart_tree_height; n++) {
        if (str[n - 1] == '\0') { // test if the string ends.
            met_end = 1;
        }
        if (str[n - 1] != '\0' && met_end == 0) { // if not, we calculate the character index.
            i_t_n = ((uint64_t)str[n - 1]) % dart->alphabet_size;
        }
        c = (i_t_n) * ((uint64_t)uint32_pow(dart->alphabet_size, dart->dart_tree_height - n));
        rst += c;
    }
    return (rst % (uint64_t)dart->num_vnode);
}

// /**
//  * This function is for getting the alternative virtual node ID.
//  *
//  */
// uint64_t
// get_reconciled_vnode_id_with_power_of_two_choice_rehashing(DART *dart, uint64_t base_vnode_idx, char *word,
//                                                            get_server_info_callback get_server_cb)
// {

//     int ir_idx = (int)ceil((double)(dart->alphabet_size / 2));

//     // base virtual node address always in the first element of the array.
//     uint64_t rst = base_vnode_idx;

//     // determine the tree height.
//     int tree_height = dart->dart_tree_height;

//     // get serverID for base virtual node
//     uint64_t serverId = get_server_id_by_vnode_id(dart, base_vnode_idx);

//     // we first let reconciled virtual node to be the base virtual node.
//     uint64_t reconciled_vnode_idx = base_vnode_idx;

//     if (dart->dart_tree_height <= 1) {
//         return reconciled_vnode_idx;
//     }

//     // The procedure of picking alternative virtual node is important.
//     // We first need to know what is the character lying on the leaves of DART partition tree.
//     int last_c_index = tree_height - 1;

//     int post_leaf_index = 0;
//     int pre_leaf_index  = 0;

//     // The pre_leaf_index is the index of character right before the leaf character.
//     // The post_leaf_index is the index of character right after the leaf character.
//     if (strlen(word) <= tree_height) {
//         // if the word is not longer than the tree height, then there is no post-leaf character, therefore
//         // post_leaf_index should be 0
//         post_leaf_index = 0;
//         // then the last_c_index should be the index of the last character in the alphabet.
//         last_c_index = strlen(word) - 1;
//         if (strlen(word) <= 1) {
//             // if the word contains 0-1 character, there is no pre-leaf character and therefore
//             pre_leaf_index
//             // should be 0.
//             pre_leaf_index = 0;
//         }
//         else {
//             // otherwise, pre_leaf_index should be the index of proceeding character of the
//             // leaf-level character in the alphabet.
//             pre_leaf_index = (int)word[last_c_index - 1] % dart->alphabet_size;
//         }
//     }
//     else {
//         // if the length of the word exceeds the height of the DART partition tree,
//         // definitely, post-leaf character exists.
//         post_leaf_index = (int)word[last_c_index + 1] % dart->alphabet_size;
//         // but, there is a case where DART partition tree is of height 1.
//         // in this case, there will be no pre-leaf character.
//         if (tree_height <= 1) {
//             pre_leaf_index = 0;
//         }
//         else {
//             // otherwise, there will be a pre-leaf character.
//             pre_leaf_index = (int)word[last_c_index - 1] % dart->alphabet_size;
//         }
//     }
//     int leaf_index = (int)word[last_c_index] % dart->alphabet_size;

//     int leaf_post_sum  = leaf_index + pre_leaf_index + post_leaf_index;
//     int leaf_post_diff = abs(post_leaf_index - leaf_index - pre_leaf_index);

//     // int leaf_post_sum = leaf_index + pre_leaf_index + 0;
//     // int leaf_post_diff = abs(leaf_index-pre_leaf_index);

//     // We calculate the region size:
//     int region_size = dart->num_vnode / dart->alphabet_size; // d=1, rs = 1; d = 2, rs = k; d = 3, rs =k^2;
//     // We calculate the sub-region size:
//     int sub_region_size = region_size / dart->alphabet_size; // d=1, srs = 0; d = 2, srs = 1; d = 3, srs =
//     k;

//     // We calculate the major offset which possibly pick a virtual node in another sub-region.
//     int major_offset = (leaf_post_sum % dart->alphabet_size) * (sub_region_size);
//     // We calcuate the minor offset which will possibly pick a different virtual node within the same
//     // sub-region.
//     int minor_offset = leaf_post_diff;
//     // Finally the region offset will be some certain virtual node in one region.
//     // uint64_t region_offset = (reconciled_vnode_idx + (uint64_t)major_offset - (uint64_t)minor_offset)
//     //                         % (uint64_t)region_size;
//     uint64_t region_offset =
//         (reconciled_vnode_idx + (uint64_t)major_offset - (uint64_t)minor_offset) % (uint64_t)region_size;
//     // Invert region Index:  ceil(alphabet_size / 2);

//     int      n = 0;
//     uint64_t c;
//     // uint64_t rst = 0;
//     uint64_t i_t_n;
//     int      met_end = 0;
//     for (n = 1; n <= dart->dart_tree_height; n++) {
//         if (word[n - 1] == '\0') {
//             met_end = 1;
//         }
//         if (word[n - 1] != '\0' && met_end == 0) {
//             if (n == 1) {
//                 i_t_n = ((int)word[n - 1] + ir_idx) % dart->alphabet_size;
//             }
//             else if (n == (dart->dart_tree_height - 1)) {
//                 i_t_n = ((int)word[n - 1] + leaf_post_sum) % dart->alphabet_size;
//             }
//             else if (n == dart->dart_tree_height) {
//                 i_t_n = abs((int)word[n - 1] - leaf_post_diff) % dart->alphabet_size;
//             }
//         }
//         c = (i_t_n) * ((uint64_t)uint32_pow(dart->alphabet_size, dart->dart_tree_height - n));
//         rst += c;
//     }

//     int alterV = (rst % (uint64_t)dart->num_vnode);

//     // // We also calculate the region start position.
//     // uint64_t region_start = ((((int)word[0]+ir_idx) % dart->alphabet_size)) * region_size;//
//     // ((reconciled_vnode_idx)/region_size) * (region_size);
//     // // Finally, the reconciled vnode index is calculated.
//     // // reconciled_vnode_idx = (0 + region_start + region_offset) % dart->num_vnode;
//     // reconciled_vnode_idx = (reconciled_vnode_idx + region_start + region_offset) % dart->num_vnode;

//     // Only when inserting a word, we do such load detection.
//     // get alternative virtual node and therefore the alternative server ID.
//     int reconcile_serverId = get_server_id_by_vnode_id(dart, alterV);
//     if (get_server_cb != NULL) {
//         // Check both physical server to see which one has smaller number of indexed keywords on it.
//         dart_server origin_server     = get_server_cb(serverId);
//         dart_server reconciled_server = get_server_cb(reconcile_serverId);
//         // printf("For keyword %s, choosing between %d and %d\n", word, serverId, reconcile_serverId);

//         if (origin_server.indexed_word_count > reconciled_server.indexed_word_count) {
//             // printf("Reconcile happened. from %d to %d\n", vnode_idx , reconciled_vnode_idx);
//             rst = alterV;
//         }
//     }
//     else {
//         rst = alterV;
//     }
//     return rst;
// }

/**
 * This function is for getting the alternative virtual node ID.
 *
 */
uint64_t
get_reconciled_vnode_id_with_power_of_two_choice_rehashing_2(DART *dart, uint64_t base_vnode_idx, char *word,
                                                             get_server_info_callback get_server_cb)
{

    // base virtual node address always in the first element of the array.
    uint64_t rst = base_vnode_idx;

    // determine the tree height.
    int tree_height = dart->dart_tree_height;

    // get serverID for base virtual node
    uint64_t serverId = get_server_id_by_vnode_id(dart, base_vnode_idx);

    // we first let reconciled virtual node to be the base virtual node.
    uint64_t reconciled_vnode_idx = base_vnode_idx;

    if (dart->dart_tree_height <= 1) {
        return reconciled_vnode_idx;
    }

    // The procedure of picking alternative virtual node is important.
    // We first need to know what is the character lying on the leaves of DART partition tree.
    int last_c_index = tree_height - 1;

    int post_leaf_index = 0;
    int pre_leaf_index  = 0;

    // The pre_leaf_index is the index of character right before the leaf character.
    // The post_leaf_index is the index of character right after the leaf character.
    if (strlen(word) <= tree_height) {
        // if the word is not longer than the tree height, then there is no post-leaf character, therefore
        // post_leaf_index should be 0
        post_leaf_index = 0;
        // then the last_c_index should be the index of the last character in the alphabet.
        last_c_index = strlen(word) - 1;
        if (strlen(word) <= 1) {
            // if the word contains 0-1 character, there is no pre-leaf character and therefore pre_leaf_index
            // should be 0.
            pre_leaf_index = 0;
        }
        else {
            // otherwise, pre_leaf_index should be the index of proceeding character of the
            // leaf-level character in the alphabet.
            pre_leaf_index = (int)word[last_c_index - 1] % dart->alphabet_size;
        }
    }
    else {
        // if the length of the word exceeds the height of the DART partition tree,
        // definitely, post-leaf character exists.
        post_leaf_index = (int)word[last_c_index + 1] % dart->alphabet_size;
        // but, there is a case where DART partition tree is of height 1.
        // in this case, there will be no pre-leaf character.
        if (tree_height <= 1) {
            pre_leaf_index = 0;
        }
        else {
            // otherwise, there will be a pre-leaf character.
            pre_leaf_index = (int)word[last_c_index - 1] % dart->alphabet_size;
        }
    }
    int leaf_index = (int)word[last_c_index] % dart->alphabet_size;

    int leaf_post_sum  = leaf_index + pre_leaf_index + post_leaf_index;
    int leaf_post_diff = abs(post_leaf_index - leaf_index - pre_leaf_index);

    // int leaf_post_sum = leaf_index + pre_leaf_index + 0;
    // int leaf_post_diff = abs(leaf_index-pre_leaf_index);

    // We calculate the region size:
    int region_size = dart->num_vnode / dart->alphabet_size; // d=1, rs = 1; d = 2, rs = k; d = 3, rs =k^2;
    // We calculate the sub-region size:
    int sub_region_size = region_size / dart->alphabet_size; // d=1, srs = 0; d = 2, srs = 1; d = 3, srs = k;

    // We calculate the major offset which possibly pick a virtual node in another sub-region.
    int major_offset = (leaf_post_sum % dart->alphabet_size) * (sub_region_size);
    // We calcuate the minor offset which will possibly pick a different virtual node within the same
    // sub-region.
    int minor_offset = leaf_post_diff;
    // Finally the region offset will be some certain virtual node in one region.
    uint64_t region_offset =
        (reconciled_vnode_idx + (uint64_t)major_offset - (uint64_t)minor_offset) % (uint64_t)region_size;
    // Invert region Index:  ceil(alphabet_size / 2);

    int ir_idx = (int)ceil((double)(dart->alphabet_size / 2));

    // We also calculate the region start position.
    uint64_t region_start = ((((int)word[0] + ir_idx) % dart->alphabet_size)) *
                            region_size; // ((reconciled_vnode_idx)/region_size) * (region_size);
    // Finally, the reconciled vnode index is calculated.
    // reconciled_vnode_idx = (0 + region_start + region_offset) % dart->num_vnode;
    reconciled_vnode_idx = (reconciled_vnode_idx + region_start + region_offset) % dart->num_vnode;

    // Only when inserting a word, we do such load detection.
    // get alternative virtual node and therefore the alternative server ID.
    int reconcile_serverId = get_server_id_by_vnode_id(dart, reconciled_vnode_idx);
    if (get_server_cb != NULL) {
        // Check both physical server to see which one has smaller number of indexed keywords on it.
        dart_server origin_server     = get_server_cb(serverId);
        dart_server reconciled_server = get_server_cb(reconcile_serverId);
        // printf("For keyword %s, choosing between %d and %d\n", word, serverId, reconcile_serverId);

        if (origin_server.indexed_word_count > reconciled_server.indexed_word_count) {
            // printf("Reconcile happened. from %d to %d\n", vnode_idx , reconciled_vnode_idx);
            rst = reconciled_vnode_idx;
        }
    }
    else {
        rst = reconciled_vnode_idx;
    }
    return rst;
}

/**
 * Get IDs of all virtual nodes of replicas by given string and overall tree-height setting.
 * When is_physical = 1, it returns the IDs of all physical nodes of replicas.
 *
 * return The length of the ID array.
 */
int
get_replica_node_ids(DART *dart, uint64_t master_node_id, int is_physical, uint64_t **out)
{
    if (out == NULL) {
        return 0;
    }
    out[0] = (uint64_t *)calloc(dart->replication_factor, sizeof(uint64_t));

    out[0][0] = master_node_id;

    uint64_t vnode_distance = dart->num_vnode / dart->alphabet_size;

    int r = 1;
    for (r = 1; r < dart->replication_factor; r++) {
        out[0][r] = (out[0][0] + (uint64_t)r * vnode_distance) % dart->num_vnode;
    }

    if (is_physical == 1) {
        for (r = 0; r < dart->replication_factor; r++) {
            out[0][r] = get_server_id_by_vnode_id(dart, out[0][r]);
        }
    }

    return dart->replication_factor;
}

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
int
get_server_ids_for_insert(DART *dart_g, char *keyword, get_server_info_callback get_server_cb, uint64_t **out)
{
    if (out == NULL) {
        return 0;
    }
    int      rst                  = 0; // not success
    uint64_t base_virtual_node_id = get_base_virtual_node_id_by_string(dart_g, keyword);
    // For insert operations, we only return the reconciled virtual node.
    // The reconciled virtual node can be either the base virtual node or the alternative virtual node.
    uint64_t alter_virtual_node_id = get_reconciled_vnode_id_with_power_of_two_choice_rehashing_2(
        dart_g, base_virtual_node_id, keyword, get_server_cb);
    // We call the following function to calculate all the server IDs.
    int is_physical = 0;
    int rst_len     = get_replica_node_ids(dart_g, alter_virtual_node_id, is_physical, out);
    return rst_len;
}

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
int
get_server_ids_for_query(DART *dart_g, char *token, dart_op_type_t op_type, uint64_t **out)
{
    if (out == NULL) {
        return 0;
    }
    if (op_type == OP_INSERT) {
        return 0;
    } // For INSERT operation, we return nothing here.

    // We first eliminate possibility of INFIX query.
    // Note: if suffix tree mode is ON, we don't have to search all servers.
#ifndef PDC_DART_SFX_TREE
    if (op_type == OP_INFIX_QUERY) {
        out[0] = (uint64_t *)calloc(dart_g->num_server, sizeof(uint64_t));
        int i  = 0;
        for (i = 0; i < dart_g->num_server; i++) {
            out[0][i] = i;
        }
        return dart_g->num_server;
    }
#endif

    // for prefix/suffix query, we seek for proper parent region and only perform query broadcast for that
    // region. when suffix tree mode is ON: suffix query == exact query, infix query == prefix query.
    if (strlen(token) < dart_g->dart_tree_height && (op_type == OP_PREFIX_QUERY
#ifdef PDC_DART_SFX_TREE
                                                     || op_type == OP_INFIX_QUERY
#else
                                                     || op_type == OP_SUFFIX_QUERY
#endif
                                                     )) {

        // TODO: currently broadcast request to all virtual nodes in one region.
        // TODO: we have to consider if we can reduce the number of nodes we need to
        // access as the prefix length grows.
        int region_size  = dart_g->num_vnode / dart_g->alphabet_size;
        int region_start = ((((int)token[0] % dart_g->alphabet_size) + 0) * region_size);

        uint64_t server_start = get_server_id_by_vnode_id(dart_g, region_start);
        uint64_t server_end   = get_server_id_by_vnode_id(dart_g, region_start + region_size - 1);

        int num_srvs    = server_end - server_start + 1;
        out[0]          = (uint64_t *)calloc(num_srvs, sizeof(uint64_t));
        int num_srv_ids = 0;

        uint64_t srvId = server_start;
        for (; srvId <= server_end; srvId++) {
            out[0][num_srv_ids++] = srvId;
        }
        return num_srvs;
    }
    else {
        // this branch handles the following cases:
        // 1. exact search (suffix tree mode can be either ON or OFF):
        //    the logic is pretty much the same as insert operation,
        //    the only difference is to reserve two servers for query.
        // 2. suffix search with insufficient token length (in suffix tree mode),
        //    this is treated as an exact query.
        // 2. prefix/infix (suffix tree mode can be ON or OFF) with sufficiently long token,
        //    this is pretty much the same with exact queries.
        // 3. delete operation: we need to delete all replicas,
        //    including base replicas and alternative replicas.
        // Explanation: For exact search or prefix/suffix search, when the given token length is large enough,
        // we consider a different query procedure. in this case, there is no difference between a long
        // prefix/suffix/infix and an exact query against a long keyword.

        uint64_t base_virtual_node_id = get_base_virtual_node_id_by_string(dart_g, token);
        // For insert operations, we only return the reconciled virtual node.
        // The reconciled virtual node can be either the base virtual node or the
        // alternative virtual node.
        uint64_t reconciled_vnode_id = get_reconciled_vnode_id_with_power_of_two_choice_rehashing_2(
            dart_g, base_virtual_node_id, token, NULL);

        uint64_t *base_replicas;
        uint64_t *alter_replicas;

        int is_physical = 0;

        int num_base_reps  = get_replica_node_ids(dart_g, base_virtual_node_id, is_physical, &base_replicas);
        int num_alter_reps = get_replica_node_ids(dart_g, reconciled_vnode_id, is_physical, &alter_replicas);
        if (op_type == OP_DELETE) {
            // for delete operations, we need to perform delete on all replicas
            out[0] = (uint64_t *)calloc(num_base_reps + num_alter_reps, sizeof(uint64_t));
            int i  = 0;
            for (i = 0; i < num_base_reps; i++) {
                out[0][i] = base_replicas[i];
            }
            for (i = 0; i < num_alter_reps; i++) {
                out[0][num_base_reps + i] = alter_replicas[i];
            }
            return num_base_reps + num_alter_reps;
        }
        else {
            // for other query operations, we only provide two servers for them to access
            // and the server has to be any two of the possible replicas.
            int req_count = dart_client_request_count_incr(dart_g);
            int rep_index = req_count % num_base_reps;
            out[0]        = (uint64_t *)calloc(2, sizeof(uint64_t));
            int rst_size  = 1;
            out[0][0]     = base_replicas[rep_index];
            if (base_replicas[rep_index] != alter_replicas[rep_index]) {
                rst_size  = 2;
                out[0][1] = alter_replicas[rep_index];
            }
            return rst_size;
        }
    }
}

/**
 *  This is a routing function for different possible operations in DART.
 *  It will only generate the array of IDs of servers that client should touch.
 *
 *  The return value is the length of valid elements in the array.
 *  A for loop can use the return value as the upper bound to iterate all elements in the array.
 *
 * @param dart_g        [IN]        Pointer of Global state of DART
 * @param key           [IN]        The given key. For queries, it is already a bare keyword without '*'.
 * @param op_type       [IN]        Give the operation type.
 * @param get_server_cb [IN]        The callback function for getting the statistical information of a server.
 *                                  Signature: dart_server (*get_server_info_callback)(uint32_t server_id)
 * @param out           [IN/OUT]    The pointer of the server ID array. To accelerate, we require caller
 *                                  function to pass an ID array of length M into this function, where M is
 *                                  the number of total physical servers. Then, this function will fill up
 *                                  this array.
 * @return              [OUT]       The valid length of the server ID array.
 *
 */
int
DART_hash(DART *dart_g, char *key, dart_op_type_t op_type, get_server_info_callback get_server_cb,
          index_hash_result_t **out)
{
    int ret_value = 0;

    if (out == NULL) {
        return ret_value;
    }

    uint64_t *temp_out    = NULL;
    int       tmp_out_len = 0;
    char *    tok         = NULL;
    *out                  = NULL;

    // regardless of suffix tree mode, we only need to get the DART hash result for one time for query
    // operations.
    int loop_count = 1;
    if (is_index_write_op(op_type)) {
#ifdef PDC_DART_SFX_TREE
        // suffix tree mode is ON, we can iterate all suffixes for insert/delete operations.
        // if there are all N suffixes, we need to get the DART hash results for N times,
        // and each time we get the result for a single suffix.
        loop_count = strlen(key);
#else
        // suffix tree mode is OFF, we iterate the original string and its reverse. (2 iterations)
        loop_count = 2;
#endif
    }
    printf("[DART_hash] key = %s\n", key);
    int iter      = 0;
    int is_suffix = 0;
    for (iter = 0; iter < loop_count; iter++) {
#ifdef PDC_DART_SFX_TREE
        tok = substring(key, iter, strlen(key));
        // when suffix tree mode is ON, we store suffixes of the key to the suffix trie.
        is_suffix = iter > 0 ? 1 : 0;
#else
        tok        = iter > 0 ? reverse_str(key) : strdup(key);
        // when suffix tree mode is OFF, we store reversed string to the suffix trie.
        is_suffix = iter > 0 ? 1 : 0;
#endif
        /* ************ [START] CORE DART HASH FOR EVERY SINGLE TOKEN ************** */
        if (op_type == OP_INSERT) {
            // An INSERT operation happens
            tmp_out_len = get_server_ids_for_insert(dart_g, tok, get_server_cb, &temp_out);
        }
        else {
            // A query operation happens
            tmp_out_len = get_server_ids_for_query(dart_g, tok, op_type, &temp_out);
        }
        /* ************ [END] CORE DART HASH FOR EVERY SINGLE TOKEN ************** */
        // now we have the result of DART hash, we need to merge it with the previous results.
        ret_value += tmp_out_len;
        if (*out == NULL) {
            *out = (index_hash_result_t *)calloc(ret_value, sizeof(index_hash_result_t));
        }
        else {
            *out = (index_hash_result_t *)realloc(*out, ret_value * sizeof(index_hash_result_t));
        }
        for (int j = 0; j < tmp_out_len; j++) {
            (*out)[ret_value - tmp_out_len + j].virtual_node_id = temp_out[j];
            (*out)[ret_value - tmp_out_len + j].server_id = get_server_id_by_vnode_id(dart_g, temp_out[j]);
            (*out)[ret_value - tmp_out_len + j].key       = tok;
            (*out)[ret_value - tmp_out_len + j].is_suffix = is_suffix;
        }
        if (temp_out != NULL)
            free(temp_out);
    }
    return ret_value;
}

/**
 * This function performs DHT hashing, solely for comparison purpose.
 *
 */
int
DHT_hash(DART *dart_g, size_t len, char *key, dart_op_type_t op_type, index_hash_result_t **out)
{
    if (out == NULL) {
        return 0;
    }
    uint64_t hashVal   = djb2_hash(key, (int)len);
    uint64_t server_id = hashVal % (dart_g->num_server);
    int      i         = 0;
    *out               = NULL;

    int ret_value    = 0;
    int is_full_scan = 0;
    if (is_index_write_op(op_type)) {
        is_full_scan = 0;
    }
    else if (op_type == OP_EXACT_QUERY) {
        is_full_scan = 0;
    }
    else if (op_type == OP_PREFIX_QUERY || op_type == OP_SUFFIX_QUERY) {
        if (len > 1) {
            is_full_scan = 1;
        }
        else {
            is_full_scan = 0;
        }
    }
    else if (op_type == OP_INFIX_QUERY) {
        is_full_scan = 1;
    }

    // according to different scan type, we return different results.
    if (is_full_scan) {
        ret_value = dart_g->num_server;
        *out      = (index_hash_result_t *)calloc(ret_value, sizeof(index_hash_result_t));
        for (i = 0; i < dart_g->num_server; i++) {
            (*out)[i].server_id = i;
            (*out)[i].key       = key;
        }
    }
    else {
        ret_value           = 1;
        *out                = (index_hash_result_t *)calloc(ret_value, sizeof(index_hash_result_t));
        (*out)[0].server_id = server_id;
        (*out)[0].key       = key;
    }
    return ret_value;
}