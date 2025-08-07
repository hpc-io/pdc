#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <stdbool.h>
#include "pdc_hash_table.h"
#include "pdc_pht.h"
#include "pdc_dllist.h"

#ifdef __cplusplus
extern "C" {
#endif
struct _PrefixTableBucket {
    bool isLeaf; // true if this is a leaf node
    /**
     * Parent and siblings
     */
    char *parent;
    char *left;
    char *right;

    char *prefix;
    DoublyLinkedList *store;
};

struct _PrefixTable {
    HashTable *map;
    size_t bucket_size;
    size_t key_count;
};

PrefixTable* prefix_table_init(unsigned int bucket_size, PrefixTableHashFunc hash_cb, PrefixTableEqualFunc equal_cb, PrefixTableKeyFreeFunc free_cb){
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
    return pht;
}

PrefixTableBucket* prefix_table_bucket_init(PrefixTableHashFunc hash_cb, PrefixTableEqualFunc equal_cb, PrefixTableKeyFreeFunc free_cb){
    PrefixTableBucket *bucket = (PrefixTableBucket *)malloc(sizeof(PrefixTableBucket));
    if (!bucket) {
        return NULL; // Memory allocation failed
    }
    bucket->isLeaf = false;
    bucket->parent = NULL;
    bucket->left = NULL;
    bucket->right = NULL;
    bucket->prefix = NULL;
    bucket->store = dllist_init(hash_cb, equal_cb, free_cb);
    return bucket;
}

void prefix_table_destroy(PrefixTable *pht){
    hash_table_free(pht->map);
    free(pht);
}

void pht_add_to_store(PrefixTable *pht, void *key, void *value){
    hash_table_insert(pht->map, key, value);
}


#ifdef __cplusplus
}
#endif