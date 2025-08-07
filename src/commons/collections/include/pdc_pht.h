#define ROOT_PREFIX "#"
typedef void* PrefixTableKey;
typedef void* PrefixTableValue;

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
 * Prefix table data structure
 */
typedef struct _PrefixTable PrefixTable;

/**
 * Prefix table bucket structure
 */
typedef struct _PrefixTableBucket PrefixTableBucket;

/**
 * Initializes a new prefix table.
 *
 * @param bucket_size  The size of each bucket in the prefix table.
 * @param free_cb      Function to free keys when they are removed from the table.
 * @return             A pointer to the initialized prefix table, or NULL on failure.
 */
PrefixTable* prefix_table_init(unsigned int bucket_size, PrefixTableHashFunc hash_cb, PrefixTableEqualFunc equal_cb, PrefixTableKeyFreeFunc free_cb);

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
 * Searches for a key in the prefix table.
 *
 * @param pht   The prefix table to search.
 * @param key   The key to search for.
 * @param result Pointer to store the found value, or NULL if not found.
 * @return      0 on success, or a negative error code on failure.
 */
int prefix_table_exact_search(PrefixTable *pht, void *key, void **result);

/**
 * Searches for keys in a range within the prefix table.
 *
 * @param pht           The prefix table to search.
 * @param start_key     The starting key of the range.
 * @param include_start Whether to include the start key in the results.
 * @param end_key       The ending key of the range.
 * @param include_end   Whether to include the end key in the results.
 * @param result        Pointer to store the found values, or NULL if not found.
 * @return              0 on success, or a negative error code on failure.
 */
int prefix_table_range_search(PrefixTable *pht, void *start_key, bool include_start, void *end_key, bool include_end, void **result);

/**
 * Counts the number of keys in the prefix table.
 *
 * @param pht  The prefix table to count keys in.
 * @return     The number of keys in the prefix table.
 */
unsigned int prefix_table_count(PrefixTable *pht);

/**
 * Destroys the prefix table and frees all associated resources.
 *
 * @param pht  The prefix table to destroy.
 */
void prefix_table_destroy(PrefixTable *pht);