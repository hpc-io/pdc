#include "pdc_pht.h"


#ifdef __cplusplus
extern "C" {
#endif



PrefixTable* prefix_table_init(unsigned int bucket_size, HashTableHashFunc hash_cb, HashTableEqualFunc equal_cb){
    PrefixTable *pht = (PrefixTable *)malloc(sizeof(PrefixTable));
    if (!pht) {
        return NULL; // Memory allocation failed
    }
    pht->bucket_size = bucket_size;
    pht->key_count = 0;
    pht->map = hash_table_new(hash_cb, equal_cb);
    if (!pht->map) {
        free(pht);
        return NULL; // Hash table creation failed
    }
    pht->hash_cb = hash_cb;
    pht->equal_cb = equal_cb;
    return pht;
}

PrefixTableBucket* prefix_table_bucket_init(PrefixTableHashFunc hash_cb, PrefixTableEqualFunc equal_cb){
    PrefixTableBucket *bucket = (PrefixTableBucket *)malloc(sizeof(PrefixTableBucket));
    if (!bucket) {
        return NULL; // Memory allocation failed
    }
    bucket->isLeaf = true;
    bucket->parent = NULL;
    bucket->left = NULL;
    bucket->right = NULL;
    bucket->prefix = NULL;
    bucket->store = dllist_init(hash_cb, equal_cb);
    return bucket;
}
int prefix_table_insert(PrefixTable *pht, void *key, void *value){
    if (!pht || !key || !value) {
        return -1; // Invalid parameters
    }
    hash_table_insert(pht->map, key, value);
    pht->key_count++;
    return 0;
}

void prefix_table_destroy(PrefixTable *pht){
    hash_table_free(pht->map);
    free(pht);
}

void pht_add_to_store(PrefixTable *pht, void *key, void *value){
    hash_table_insert(pht->map, key, value);
}
bool prefix_table_bucket_is_leaf(PrefixTableBucket *bucket){
    return bucket->isLeaf;
}

#ifdef __cplusplus
}
#endif