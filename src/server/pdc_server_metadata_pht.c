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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctype.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>

#include "pdc_config.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc_utlist.h"
#include "pdc_hash_table.h"
#include "pdc_dablooms.h"
#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server_metadata_pht.h"
#include "pdc_server.h"
#include "mercury_hash_table.h"
#include "pdc_malloc.h"
#include "string_utils.h"
#include "pdc_logger.h"
#include "pdc_pht.h"

uint32_t metadata_server_id_g  = 0;
uint32_t metadata_num_server_g = 0;

PrefixTable *metadata_pht_key_g = NULL;

unsigned int
pht_string_hash(void *vlocation)
{
    unsigned int   result = 5381;
    unsigned char *p;

    p = (unsigned char *)vlocation;

    while (*p != '\0') {
        result = (result << 5) + result + *p;
        ++p;
    }

    return result;
}

int
pht_string_comparator(const void *key1, const void *key2)
{
    char *ch1  = (char *)key1;
    char *ch2  = (char *)key2;
    int   diff = strcmp(ch1, ch2);
    return diff < 0 ? -1 : diff > 0 ? 1 : 0;
}

/****************************/
/* Initialize PHT */
/****************************/
void
PDC_Server_metadata_pht_init(uint32_t num_server, uint32_t server_id)
{
    FUNC_ENTER(NULL);

    metadata_server_id_g  = num_server;
    metadata_num_server_g = server_id;
    metadata_pht_key_g    = prefix_table_init(PHT_BUCKET_SIZE, string_hash, string_equal);

    FUNC_LEAVE_VOID();
}

perr_t
PDC_Server_check_prefix(metadata_check_prefix_in_t *in, metadata_check_prefix_out_t *out)
{
    perr_t ret_value = SUCCEED;
#ifdef ENABLE_MULTITHREAD
    int unlocked;
#endif
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    printf("PDC_Server_check_prefix: in->prefix = %s\n", in->prefix);
    printf("PDC_Server_check_prefix: metadata_pht_key_g = %d\n", metadata_pht_key_g->key_count);
    HashTableValue *value = hash_table_lookup(metadata_pht_key_g->map, in->prefix);
    printf("PDC_Server_check_prefix: value = %p\n", value);
    PrefixTableBucket *bucket = (PrefixTableBucket *)value;
    printf("PDC_Server_check_prefix: isLeaf = %d\n", bucket->isLeaf);
    out->found     = (bucket != NULL) ? 1 : 0;
    out->leaf      = prefix_table_bucket_is_leaf(bucket);
    out->ret       = 1;
    out->server_id = metadata_server_id_g;

done:
#ifdef ENABLE_MULTITHREAD
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING
    server_update_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_create_bucket(metadata_create_bucket_in_t *in, metadata_create_bucket_out_t *out)
{
    perr_t ret_value = SUCCEED;
#ifdef ENABLE_MULTITHREAD
    int unlocked;
#endif
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    if (metadata_pht_key_g == NULL) {
        perror("metadata_pht_key_g is NULL");
        ret_value = FAIL;
        goto done;
    }
    printf("PDC_Server_create_bucket: in->prefix = %s\n", in->prefix);
    PrefixTableBucket *bucket = hash_table_lookup(metadata_pht_key_g->map, in->prefix);
    if (bucket != NULL) {
        out->ret = 1;
        goto done; // already exists
    }
    PrefixTableBucket *new_bucket = prefix_table_bucket_init(pht_string_hash, pht_string_comparator);
    new_bucket->prefix            = strdup(in->prefix);
    hash_table_insert(metadata_pht_key_g->map, new_bucket->prefix, new_bucket);
    printf("PDC_Server_create_bucket: new_bucket = %p\n", new_bucket);
    out->ret = 1;
    if (bucket == NULL) {
        ret_value = FAIL;
        goto done;
    }
done:
#ifdef ENABLE_MULTITHREAD
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING
    server_update_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_bucket_split(metadata_create_bucket_in_t *in, metadata_create_bucket_out_t *out)
{
    perr_t ret_value = SUCCEED;
#ifdef ENABLE_MULTITHREAD
    int unlocked;
#endif
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    if (metadata_pht_key_g == NULL) {
        perror("metadata_pht_key_g is NULL");
        ret_value = FAIL;
        goto done;
    }
    printf("PDC_Server_create_bucket: in->prefix = %s\n", in->prefix);
    PrefixTableBucket *bucket = hash_table_lookup(metadata_pht_key_g->map, in->prefix);
    if (bucket != NULL) {
        out->ret = 1;
        goto done; // already exists
    }
    PrefixTableBucket *new_bucket = prefix_table_bucket_init(pht_string_hash, pht_string_comparator);
    new_bucket->prefix            = strdup(in->prefix);
    hash_table_insert(metadata_pht_key_g->map, new_bucket->prefix, new_bucket);
    printf("PDC_Server_create_bucket: new_bucket = %p\n", new_bucket);
    out->ret = 1;
    if (bucket == NULL) {
        ret_value = FAIL;
        goto done;
    }
done:
#ifdef ENABLE_MULTITHREAD
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING
    server_update_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_metadata_key_add(metadata_key_add_in_t *in, metadata_key_add_out_t *out)
{
    perr_t ret_value = SUCCEED;
#ifdef ENABLE_MULTITHREAD
    int unlocked;
#endif
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    printf("PDC_Server_metadata_key_add: in->prefix = %s\n", in->prefix);
    printf("PDC_Server_metadata_key_add: metadata_pht_key_g = %d\n", metadata_pht_key_g->key_count);
    HashTableValue *value = hash_table_lookup(metadata_pht_key_g->map, in->prefix);
    printf("PDC_Server_metadata_key_add: value = %p\n", value);
    PrefixTableBucket *bucket = (PrefixTableBucket *)value;
    if (bucket == NULL) {
        perror("bucket is NULL");
        ret_value = FAIL;
        goto done;
    }
    printf("PDC_Server_metadata_key_add: key = %s\n", in->key);
    Set *prevSet = dllist_search_key(bucket->store, in->key);
    if (prevSet != NULL) {
        set_insert(prevSet, in->value);
    }
    else {
        if (bucket->store->count >= PHT_BUCKET_SIZE)
            pht_bucket_split(bucket);
        Set *newSet = set_new(metadata_pht_key_g->hash_cb, metadata_pht_key_g->equal_cb);
        set_insert(newSet, in->value);
        dllist_insert(bucket->store, strdup(in->key), newSet);
    }
done:
#ifdef ENABLE_MULTITHREAD
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING
    server_update_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

int
pht_bucket_split(PrefixTableBucket *bucket)
{
    char *left_prefix  = malloc(sizeof(char) * (strlen(bucket->prefix) + 2));
    char *right_prefix = malloc(sizeof(char) * (strlen(bucket->prefix) + 2));
    sprintf(left_prefix, "%s0", bucket->prefix);
    sprintf(right_prefix, "%s1", bucket->prefix);
    uint32_t left_server_id, right_server_id;
    PDC_Server2Server_create_bucket(left_prefix, &left_server_id);
    PDC_Server2Server_create_bucket(right_prefix, &right_server_id);
    bucket->isLeaf = false;
    bucket->left   = left_prefix;
    bucket->right  = right_prefix;

    DoublyLinkedListItem *elt = NULL;
    DL_FOREACH(bucket->store->head, elt)
    {
        char     key_prefix = string_to_binary(elt->key);
        bool     is_left    = strcmp(key_prefix, left_prefix) == 0;
        bool     is_right   = strcmp(key_prefix, right_prefix) == 0;
        uint32_t server_id  = is_left ? left_server_id : right_server_id;
        if (is_left || is_right) {
            if (server_id == metadata_server_id_g) {
                PrefixTableBucket *child_bucket =
                    hash_table_lookup(metadata_pht_key_g->map, is_left ? left_prefix : right_prefix);
                if (child_bucket == NULL)
                    return -1; // error
                dllist_insert(child_bucket->store, elt->key, elt->value);
            }
            else {
                metadata_key_add_in_t  add_in;
                metadata_key_add_out_t add_out;
                add_in.prefix = is_left ? left_prefix : right_prefix;
                add_in.key    = elt->key;
                add_in.value  = elt->value;
                add_in.size   = sizeof(Set *);
                // PDC_Client_metadata_key_add(server_id, &add_in, &add_out);
            }
        }
    }
    // dllist_destroy(bucket->store);
    bucket->store = NULL;
    return 0;
}