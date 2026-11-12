#ifndef PDC_PHT_H
#define PDC_PHT_H

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include "pdc_hash_table.h"
#include "pdc_dllist.h"

#define ROOT_PREFIX "#"

typedef void *PrefixTableKey;
typedef void *PrefixTableValue;

/**
 * Hash function used to generate hash values for keys used in a hash
 * table.
 *
 * @param data  The data to generate a hash value for.
 * @return       The hash value.
 */
typedef unsigned int (*PrefixTableHashFunc)(PrefixTableKey data);

/**
 * Function used to compare two keys for equality.
 *
 * @return   Non-zero if the two keys are equal, zero if the keys are
 *           not equal.
 */
typedef int (*PrefixTableEqualFunc)(PrefixTableKey value1, PrefixTableKey value2);

/**
 * Type of function used to free keys when entries are removed from a
 * hash table.
 */
typedef void (*PrefixTableKeyFreeFunc)(PrefixTableKey value);

/**
 * Prefix table bucket structure
 */
struct _PrefixTableBucket {
    bool isLeaf; // true if this is a leaf node
    /**
     * Parent and siblings
     */
    char *parent;
    char *left;
    char *right;

    char *            prefix;
    DoublyLinkedList *store;
};

struct _PrefixTable {
    HashTable *          map;
    size_t               bucket_size;
    size_t               key_count;
    PrefixTableHashFunc  hash_cb;
    PrefixTableEqualFunc equal_cb;
};

typedef struct _PrefixTableBucket PrefixTableBucket;
typedef struct _PrefixTable       PrefixTable;

/**
 * Initializes a new prefix table.
 *
 * @param bucket_size  The size of each bucket in the prefix table.
 * @param hash_cb      Function to hash keys.
 * @param equal_cb     Function to compare keys for equality.
 * @return             A pointer to the initialized prefix table, or NULL on failure.
 */
PrefixTable *prefix_table_init(unsigned int bucket_size, PrefixTableHashFunc hash_cb,
                               PrefixTableEqualFunc equal_cb);

/**
 * Inserts a key-value pair into the prefix table.
 *
 * @param pht   The prefix table to insert into.
 * @param key   The key to insert.
 * @param value The value to associate with the key.
 * @return      0 on success, or a negative error code on failure.
 */
int prefix_table_insert(PrefixTable *pht, void *key, void *value);

/**
 * Destroys the prefix table and frees all associated resources.
 *
 * @param pht  The prefix table to destroy.
 */
void prefix_table_destroy(PrefixTable *pht);

/**
 * Initializes a new prefix table bucket.
 *
 * @param hash_cb   Hash function callback.
 * @param compare_cb  Key ordering callback; returns zero for equal keys.
 * @return          A pointer to the initialized bucket, or NULL on failure.
 */
PrefixTableBucket *prefix_table_bucket_init(PrefixTableHashFunc       hash_cb,
                                            DoublyLinkedListEqualFunc compare_cb);

/** Frees the bucket, its prefix strings, and its owned key/value sets. */
void prefix_table_bucket_destroy(void *bucket);

/**
 * Checks if a bucket is a leaf node.
 *
 * @param bucket  The bucket to check.
 * @return        true if the bucket is a leaf, false otherwise.
 */
bool prefix_table_bucket_is_leaf(PrefixTableBucket *bucket);

#endif /* PDC_PHT_H */
