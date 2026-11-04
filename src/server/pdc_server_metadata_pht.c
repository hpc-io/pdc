/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include <stdlib.h>
#include <string.h>
#include "pdc_server_metadata_pht.h"
#include "pdc_server.h"
#include "pdc_logger.h"

static uint32_t     metadata_server_id_g;
static PrefixTable *metadata_pht_key_g;

typedef struct {
    uint32_t      size;
    unsigned char data[];
} PhtValue;

static unsigned int
pht_value_hash(void *ptr)
{
    PhtValue *   value = ptr;
    unsigned int hash  = 5381;
    for (uint32_t i = 0; i < value->size; i++)
        hash = hash * 33 + value->data[i];
    return hash;
}

static int
pht_value_equal(void *left, void *right)
{
    PhtValue *a = left, *b = right;
    return a->size == b->size && memcmp(a->data, b->data, a->size) == 0;
}

static int
pht_key_compare(void *left, void *right)
{
    return strcmp(left, right);
}

void
PDC_Server_metadata_pht_init(uint32_t num_server, uint32_t server_id)
{
    (void)num_server;
    metadata_server_id_g = server_id;
    metadata_pht_key_g   = prefix_table_init(PHT_BUCKET_SIZE, string_hash, string_equal);
    if (metadata_pht_key_g)
        hash_table_register_free_functions(metadata_pht_key_g->map, NULL, prefix_table_bucket_destroy);
}

void
PDC_Server_metadata_pht_finalize(void)
{
    prefix_table_destroy(metadata_pht_key_g);
    metadata_pht_key_g = NULL;
}

perr_t
PDC_Server_check_prefix(metadata_check_prefix_in_t *in, metadata_check_prefix_out_t *out)
{
    perr_t ret_value = FAIL;
    *out             = (metadata_check_prefix_out_t){.ret = -1, .server_id = metadata_server_id_g};
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    if (metadata_pht_key_g && in && in->prefix) {
        PrefixTableBucket *bucket = hash_table_lookup(metadata_pht_key_g->map, in->prefix);
        out->found                = bucket != NULL;
        out->leaf                 = prefix_table_bucket_is_leaf(bucket);
        out->ret                  = 1;
        ret_value                 = SUCCEED;
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    return ret_value;
}

perr_t
PDC_Server_create_bucket(metadata_create_bucket_in_t *in, metadata_create_bucket_out_t *out)
{
    perr_t ret_value = FAIL;
    out->ret         = -1;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    if (!metadata_pht_key_g || !in || !in->prefix || !*in->prefix)
        goto done;
    if (!hash_table_lookup(metadata_pht_key_g->map, in->prefix)) {
        PrefixTableBucket *bucket = prefix_table_bucket_init(string_hash, pht_key_compare);
        if (!bucket)
            goto done;
        bucket->prefix = strdup(in->prefix);
        if (!bucket->prefix || prefix_table_insert(metadata_pht_key_g, bucket->prefix, bucket) != 0) {
            prefix_table_bucket_destroy(bucket);
            goto done;
        }
    }
    out->ret  = 1;
    ret_value = SUCCEED;
done:
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    return ret_value;
}

perr_t
PDC_Server_metadata_key_add(metadata_key_add_in_t *in, metadata_key_add_out_t *out)
{
    perr_t    ret_value = FAIL;
    PhtValue *value     = NULL;
    Set *     values;
    out->ret = -1;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    if (!metadata_pht_key_g || !in || !in->prefix || !in->key || !in->value || !in->size)
        goto done;
    PrefixTableBucket *bucket = hash_table_lookup(metadata_pht_key_g->map, in->prefix);
    if (!prefix_table_bucket_is_leaf(bucket))
        goto done;
    values = dllist_search_key(bucket->store, in->key);
    /* Splitting must redistribute every value before changing the parent. Until
     * that protocol exists, reject a new key without changing the bucket. */
    if (!values && bucket->store->count >= metadata_pht_key_g->bucket_size)
        goto done;
    value = malloc(sizeof(*value) + (size_t)in->size);
    if (!value)
        goto done;
    value->size = in->size;
    memcpy(value->data, in->value, in->size);
    if (values && set_query(values, value)) {
        out->ret  = 1;
        ret_value = SUCCEED;
        goto done;
    }
    if (values) {
        if (!set_insert(values, value))
            goto done;
    }
    else {
        char *key = strdup(in->key);
        values    = set_new(pht_value_hash, pht_value_equal);
        if (!key || !values) {
            free(key);
            if (values)
                set_free(values);
            goto done;
        }
        set_register_free_function(values, free);
        if (!set_insert(values, value)) {
            free(key);
            set_free(values);
            goto done;
        }
        value = NULL; /* The set now owns the copy. */
        if (dllist_insert(bucket->store, key, values) != 0) {
            free(key);
            set_free(values);
            goto done;
        }
    }
    value     = NULL;
    out->ret  = 1;
    ret_value = SUCCEED;
done:
    free(value);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    return ret_value;
}
