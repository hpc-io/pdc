#include "pdc_pht.h"

#ifdef __cplusplus
extern "C" {
#endif

PrefixTable *
prefix_table_init(unsigned int bucket_size, HashTableHashFunc hash_cb, HashTableEqualFunc equal_cb)
{
    PrefixTable *pht = (PrefixTable *)malloc(sizeof(PrefixTable));
    if (!pht) {
        return NULL; // Memory allocation failed
    }
    pht->bucket_size = bucket_size;
    pht->key_count   = 0;
    pht->map         = hash_table_new(hash_cb, equal_cb);
    if (!pht->map) {
        free(pht);
        return NULL; // Hash table creation failed
    }
    pht->hash_cb  = hash_cb;
    pht->equal_cb = equal_cb;
    return pht;
}

PrefixTableBucket *
prefix_table_bucket_init(PrefixTableHashFunc hash_cb, DoublyLinkedListEqualFunc compare_cb)
{
    PrefixTableBucket *bucket = (PrefixTableBucket *)malloc(sizeof(PrefixTableBucket));
    if (!bucket) {
        return NULL; // Memory allocation failed
    }
    bucket->isLeaf = true;
    bucket->parent = NULL;
    bucket->left   = NULL;
    bucket->right  = NULL;
    bucket->prefix = NULL;
    bucket->store  = dllist_init(hash_cb, compare_cb);
    if (!bucket->store) {
        free(bucket);
        return NULL;
    }
    return bucket;
}
int
prefix_table_insert(PrefixTable *pht, void *key, void *value)
{
    if (!pht || !key || !value) {
        return -1; // Invalid parameters
    }
    if (!hash_table_insert(pht->map, key, value))
        return -1;
    pht->key_count = hash_table_num_entries(pht->map);
    return 0;
}

void
prefix_table_destroy(PrefixTable *pht)
{
    if (!pht)
        return;
    hash_table_free(pht->map);
    free(pht);
}

bool
prefix_table_bucket_is_leaf(PrefixTableBucket *bucket)
{
    return bucket && bucket->isLeaf;
}

void
prefix_table_bucket_destroy(void *ptr)
{
    PrefixTableBucket *bucket = ptr;
    if (!bucket)
        return;
    dllist_destroy(bucket->store);
    free(bucket->prefix);
    free(bucket->parent);
    free(bucket->left);
    free(bucket->right);
    free(bucket);
}

#ifdef __cplusplus
}
#endif
