/**
 * @file rbtree.h
 *
 * @brief Red/Black Tree
 *
 * Red/Black Tree implementation to store/access arbitrary data
 *
 * @todo extend the api to allow walking from/to a specific node
 *       (instead of only allowing to walk the entire tree)
 *
 */

#ifndef HL_RBTREE_H
#define HL_RBTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "comparators.h"
// #include "../../utils/profile/mem_perf.h"

/**
 * @brief Opaque structure representing the tree
 */
typedef struct _rbt_s rbt_t;

/**
 * @brief Callback that, if provided, will be called to release the value
 *        resources when an item is being overwritten or when removed from
 *        the tree
 * @param v the pointer to free
 */
typedef void (*rbt_free_value_callback_t)(void *v);

/**
 * @brief Create a new red/black tree
 * @param cmp_keys_cb   The comparator callback to use when comparing
 *                      keys (defaults to memcmp())
 * @param free_value_cb The callback used to release values when a node
 *                      is removed or overwritten
 * @return              A valid and initialized red/black tree (empty)
 */
rbt_t *rbt_create(libhl_cmp_callback_t cmp_keys_cb, rbt_free_value_callback_t free_value_cb);

/**
 * @brief Create a new red/black tree
 * @param dtype         The data type of the keys
 * @param free_value_cb The callback used to release values when a node
 *                      is removed or overwritten
 * @return              A valid and initialized red/black tree (empty)
 */
rbt_t *rbt_create_by_dtype(pdc_c_var_type_t dtype, rbt_free_value_callback_t free_value_cb);

/**
 * @brief Set the dtype to rbt
 * @param rbt   A valid pointer to an initialized rbt_t structure
 * @param dtype The data type of the keys
 * @return
 */
void rbt_set_dtype(rbt_t *rbt, pdc_c_var_type_t dtype);

/**
 * @brief Get the dtype of rbt
 * @param rbt   A valid pointer to an initialized rbt_t structure
 * @return dtype
 */
pdc_c_var_type_t rbt_get_dtype(rbt_t *rbt);

/**
 * @brief Release all the resources used by a red/black tree
 * @param rbt A valid pointer to an initialized rbt_t structure
 */
void rbt_destroy(rbt_t *rbt);

/**
 * @brief Add a new value into the tree
 * @param rbt   A valid pointer to an initialized rbt_t structure
 * @param key   The key of the node where to store the new value
 * @param klen  The size of the key
 * @param value The new value to store
 * @return 0 if a new node has been created successfully;
 *         1 if an existing node has been found and the value has been updated;
 *         -1 otherwise
 */
int rbt_add(rbt_t *rbt, void *key, size_t klen, void *value);

/**
 * @brief Remove a node from the tree
 * @param rbt   A valid pointer to an initialized rbt_t structure
 * @param key  The key of the node to remove
 * @param klen The size of the key
 * @param value If not NULL the address of the value hold by the removed node
 *              will be stored at the memory pointed by the 'value' argument.
 *              If NULL and a free_value_callback is set, the value hold by
 *              the removed node will be released using the free_value_callback
 * @return 0 on success; -1 otherwise
 */
int rbt_remove(rbt_t *rbt, void *key, size_t klen, void **value);

/**
 * @brief Find the value stored in the node node matching a specific key
 *        (if any)
 * @param rbt   A valid pointer to an initialized rbt_t structure
 * @param key   The key of the node where to store the new value
 * @param klen The size of the key
 * @param value A reference to the pointer which will set to point to the
 *              actual value if found
 * @return 0 on success and *value is set to point to the stored
 *         value and its size;\n-1 if not found
 */
int rbt_find(rbt_t *rbt, void *key, size_t klen, void **value);

typedef enum {
    RBT_WALK_STOP                = 0,
    RBT_WALK_CONTINUE            = 1,
    RBT_WALK_DELETE_AND_CONTINUE = -1,
    RBT_WALK_DELETE_AND_STOP     = -2
} rbt_walk_return_code_t;

/**
 * @brief Callback called for each node when walking the tree
 * @param rbt   A valid pointer to an initialized rbt_t structure
 * @param key  The key of the node where to store the new value
 * @param klen The size of the key
 * @param value The new value to store
 * @param priv  The private pointer passed to either rbt_walk()
 *              or rbt_walk_sorted()
 * @return
 *   RBT_WALK_CONTINUE If the walker can go ahead visiting the next node,\n
 *   RBT_WALK_STOP if the walker should stop and return\n
 *   RBT_WALK_DELETE_AND_CONTINUE if the current node should be removed and the
 *                                walker can go ahead\n
 *   RBT_WALK_DELETE_AND_STOP if the current node should be removed and the
 *                            walker should stop
 */
typedef rbt_walk_return_code_t (*rbt_walk_callback)(rbt_t *rbt, void *key, size_t klen, void *value,
                                                    void *priv);

/**
 * @brief Walk the entire tree and call the callback for each visited node
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *             callback when invoked.
 * @return The number of visited nodes
 */
int rbt_walk(rbt_t *rbt, rbt_walk_callback cb, void *priv);

/**
 * @brief Walk the entire tree visiting nodes in ascending order and call the callback
 *        for each visited node
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *             callback when invoked.
 * @return The number of visited nodes
 */
int rbt_walk_sorted(rbt_t *rbt, rbt_walk_callback cb, void *priv);

/**
 * @brief Walk the node with its key within the given range in the tree and call the callback for each visited
 * node
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param begin_key key to begin with
 * @param bgk_size size of the key to  begin with
 * @param end_key key to end with
 * @param edk_size size of the key to end with
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *             callback when invoked.
 * @param begin_inclusive whether the begin_key is inclusive or not
 * @param end_inclusive whether the end_key is inclusive or not
 * @return The number of visited nodes
 *
 */
int rbt_range_walk(rbt_t *rbt, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size,
                   rbt_walk_callback cb, void *priv, int begin_inclusive, int end_inclusive);

/**
 * @brief Walk the node with its key within the given range in the tree in ascending order and call the
 * callback for each visited node
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param begin_key key to begin with
 * @param bgk_size size of the key to  begin with
 * @param end_key key to end with
 * @param edk_size size of the key to end with
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *             callback when invoked.
 * @param begin_inclusive whether the begin_key is inclusive or not
 * @param end_inclusive whether the end_key is inclusive or not
 * @return The number of visited nodes
 */
int rbt_range_walk_sorted(rbt_t *rbt, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size,
                          rbt_walk_callback cb, void *priv, int begin_inclusive, int end_inclusive);

/**
 * @brief Walk the node with its key less than or equal to the given key in the tree and call the callback
 *
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param end_key key to end with
 * @param edk_size size of the key to end with
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *            callback when invoked.
 * @param end_inclusive whether the end_key is inclusive or not
 * @return The number of visited nodes
 */
int rbt_range_lt(rbt_t *rbt, void *end_key, size_t edk_size, rbt_walk_callback cb, void *priv,
                 int end_inclusive);

/**
 * @brief Walk the node with its key greater than or equal to the given key in the tree and call the callback
 * for each visited node
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param begin_key key to begin with
 * @param bgk_size size of the key to  begin with
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *           callback when invoked.
 * @param begin_inclusive whether the begin_key is inclusive or not
 * @return The number of visited nodes
 */
int rbt_range_gt(rbt_t *rbt, void *begin_key, size_t bgk_size, rbt_walk_callback cb, void *priv,
                 int begin_inclusive);

/**
 * @brief Walk the node with its key less than or equal to the given key in the tree in ascending order and
 * call the callback for each visited node
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param end_key key to end with
 * @param edk_size size of the key to end with
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *            callback when invoked.
 * @param end_inclusive whether the end_key is inclusive or not
 * @return The number of visited nodes
 */
int rbt_range_lt_sorted(rbt_t *rbt, void *end_key, size_t edk_size, rbt_walk_callback cb, void *priv,
                        int end_inclusive);

/**
 * @brief Walk the node with its key greater than or equal to the given key in the tree in ascending order and
 * call the callback for each visited node
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @param begin_key key to begin with
 * @param bgk_size size of the key to  begin with
 * @param cb   The callback to call for each visited node
 * @param priv A pointer to private data provided passed as argument to the
 *           callback when invoked.
 * @param begin_inclusive whether the begin_key is inclusive or not
 * @return The number of visited nodes
 */
int rbt_range_gt_sorted(rbt_t *rbt, void *begin_key, size_t bgk_size, rbt_walk_callback cb, void *priv,
                        int begin_inclusive);

/**
 * @brief Return the size of the tree, which is the number of nodes in the tree
 * @param rbt  A valid pointer to an initialized rbt_t structure
 * @return The number of nodes in the tree.
 */
uint64_t rbt_size(rbt_t *rbt);

// perf_info_t *get_perf_info_rbtree(rbt_t *index_root);

// void reset_perf_info_counters_rbtree(rbt_t *rbt);

// size_t get_mem_usage_by_all_rbtrees();

#ifdef DEBUG_RBTREE
/**
 * @brief Print out the whole tree on stdout (for debugging purposes only)
 */
void rbtree_print(rbtree_t *rbt);
#endif

#ifdef __cplusplus
}
#endif

#endif // HL_RBTREE_H

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
