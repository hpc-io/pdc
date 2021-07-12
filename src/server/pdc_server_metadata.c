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
#include "pdc_hash-table.h"
#include "pdc_dablooms.h"
#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server_metadata.h"
#include "pdc_server.h"

#define BLOOM_TYPE_T counting_bloom_t
#define BLOOM_NEW    new_counting_bloom
#define BLOOM_CHECK  counting_bloom_check
#define BLOOM_ADD    counting_bloom_add
#define BLOOM_REMOVE counting_bloom_remove
#define BLOOM_FREE   free_counting_bloom

// Global hash table for storing metadata
HashTable *metadata_hash_table_g  = NULL;
HashTable *container_hash_table_g = NULL;

// Debug statistics var
int      n_bloom_total_g            = 0;
int      n_bloom_maybe_g            = 0;
double   server_bloom_check_time_g  = 0.0;
double   server_bloom_insert_time_g = 0.0;
double   server_insert_time_g       = 0.0;
double   server_delete_time_g       = 0.0;
double   server_update_time_g       = 0.0;
double   server_hash_insert_time_g  = 0.0;
double   server_bloom_init_time_g   = 0.0;
uint32_t n_metadata_g               = 0;

pbool_t
PDC_region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2)
{
    pbool_t ret_value = 0;

    FUNC_ENTER(NULL);

    if (reg1.ndim != reg2.ndim)
        PGOTO_DONE(ret_value);
    if (reg1.ndim >= 1) {
        if (reg1.count_0 != reg2.count_0 || reg1.start_0 != reg2.start_0)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 2) {
        if (reg1.count_1 != reg2.count_1 || reg1.start_1 != reg2.start_1)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 3) {
        if (reg1.count_2 != reg2.count_2 || reg1.start_2 != reg2.start_2)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 4) {
        if (reg1.count_3 != reg2.count_3 || reg1.start_3 != reg2.start_3)
            PGOTO_DONE(ret_value);
    }
    ret_value = 1;
done:
    FUNC_LEAVE(ret_value);
}

/*
 * Check if two hash keys are equal
 *
 * \param vlocation1 [IN]       Hash table key
 * \param vlocation2 [IN]       Hash table key
 *
 * \return 1 if two keys are equal, 0 otherwise
 */
static int
PDC_Server_metadata_int_equal(void *vlocation1, void *vlocation2)
{
    return *((uint32_t *)vlocation1) == *((uint32_t *)vlocation2);
}

/*
 * Get hash key's location in hash table
 *
 * \param vlocation [IN]        Hash table key
 *
 * \return the location of hash key in the table
 */
static unsigned int
PDC_Server_metadata_int_hash(void *vlocation)
{
    return *((uint32_t *)vlocation);
}

/*
 * Free the hash key
 *
 * \param  key [IN]        Hash table key
 *
 * \return void
 */
static void
PDC_Server_metadata_int_hash_key_free(void *key)
{
    free((uint32_t *)key);
}

/*
 * Free metadata hash value
 *
 * \param  value [IN]        Hash table value
 *
 * \return void
 */
static void
PDC_Server_metadata_hash_value_free(void *value)
{
    pdc_metadata_t *           elt, *tmp;
    pdc_hash_table_entry_head *head;

    FUNC_ENTER(NULL);

    head = (pdc_hash_table_entry_head *)value;

    // Free bloom filter
    if (head->bloom != NULL) {
        BLOOM_FREE(head->bloom);
    }

    // Free metadata list
    if (is_restart_g == 0) {
        DL_FOREACH_SAFE(head->metadata, elt, tmp)
        {
            free(elt);
        }
    }
}

/*
 * Free container hash value
 *
 * \param  value [IN]        Hash table value
 *
 * \return void
 */
static void
PDC_Server_container_hash_value_free(void *value)
{
    pdc_cont_hash_table_entry_t *head = (pdc_cont_hash_table_entry_t *)value;
    if (head->obj_ids != NULL)
        free(head->obj_ids);
}

/*
 * Init the PDC metadata structure
 *
 * \param  a [IN]        PDC metadata structure
 *
 * \return void
 */
void
PDC_Server_metadata_init(pdc_metadata_t *a)
{
    int i;

    FUNC_ENTER(NULL);

    a->user_id     = 0;
    a->time_step   = 0;
    a->app_name[0] = 0;
    a->obj_name[0] = 0;

    a->obj_id  = 0;
    a->cont_id = 0;
    a->ndim    = 0;
    for (i = 0; i < DIM_MAX; i++)
        a->dims[i] = 0;

    a->create_time        = 0;
    a->last_modified_time = 0;
    a->tags[0]            = 0;
    a->data_location[0]   = 0;

    a->region_lock_head    = NULL;
    a->region_map_head     = NULL;
    a->region_buf_map_head = NULL;
    a->prev                = NULL;
    a->next                = NULL;
    a->transform_state     = 0;
    memset(&a->current_state, 0, sizeof(struct _pdc_transform_state));
}
// ^ hash table

/*
 * Concatenate the metadata's obj_name and time_step to one char array
 *
 * \param  metadata [IN]        PDC metadata structure pointer
 * \param  output   [OUT]       Concatenated char array pointer
 *
 * \return void
 */
static inline void
combine_obj_info_to_string(pdc_metadata_t *metadata, char *output)
{
    FUNC_ENTER(NULL);
    snprintf(output, TAG_LEN_MAX, "%s%d", metadata->obj_name, metadata->time_step);
}

/*
 * Get the metadata with obj ID from the metadata list
 *
 * \param  mlist[IN]         Metadata list head
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t *
find_metadata_by_id_from_list(pdc_metadata_t *mlist, uint64_t obj_id)
{
    pdc_metadata_t *ret_value, *elt;

    FUNC_ENTER(NULL);

    ret_value = NULL;
    if (mlist == NULL) {
        ret_value = NULL;
        goto done;
    }

    DL_FOREACH(mlist, elt)
    {
        if (elt->obj_id == obj_id) {
            ret_value = elt;
            goto done;
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

pdc_metadata_t *
find_metadata_by_id(uint64_t obj_id)
{
    pdc_metadata_t *           ret_value = NULL;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *           elt;
    HashTableIterator          hash_table_iter;
    HashTablePair              pair;
    int                        n_entry;

    FUNC_ENTER(NULL);

    if (metadata_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {

            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt)
            {
                if (elt->obj_id == obj_id) {
                    return elt;
                }
            }
        }
    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

pdc_metadata_t *
PDC_Server_get_obj_metadata(pdcid_t obj_id)
{
    pdc_metadata_t *ret_value = NULL;

    FUNC_ENTER(NULL);

    ret_value = find_metadata_by_id(obj_id);

    FUNC_LEAVE(ret_value);
}

int
PDC_Server_has_metadata(pdcid_t obj_id)
{
    if (obj_id / PDC_SERVER_ID_INTERVEL == (pdcid_t)pdc_server_rank_g + 1)
        return 1;
    return 0;
}

/*
 * Find if there is identical metadata exist in hash table
 *
 * \param  entry[IN]        Hash table entry of metadata
 * \param  a[IN]            Pointer to metadata to be checked against
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t *
find_identical_metadata(pdc_hash_table_entry_head *entry, pdc_metadata_t *a)
{
    pdc_metadata_t *ret_value = NULL;
    BLOOM_TYPE_T *  bloom;
    int             bloom_check;
    char            combined_string[TAG_LEN_MAX];
    pdc_metadata_t *elt;

    FUNC_ENTER(NULL);

    // Use bloom filter to quick check if current metadata is in the list
    if (entry->bloom != NULL && a->user_id != 0 && a->app_name[0] != 0) {
        bloom = entry->bloom;
        combine_obj_info_to_string(a, combined_string);

#ifdef ENABLE_TIMING
        struct timeval pdc_timer_start;
        struct timeval pdc_timer_end;
        double         ht_total_sec;

        gettimeofday(&pdc_timer_start, 0);
#endif

        bloom_check = BLOOM_CHECK(bloom, combined_string, strlen(combined_string));

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&pdc_bloom_time_mutex_g);
#endif

#ifdef ENABLE_TIMING
        server_bloom_check_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_unlock(&pdc_bloom_time_mutex_g);
#endif

        n_bloom_total_g++;
        if (bloom_check == 0) {
            ret_value = NULL;
            goto done;
        }
        else {
            // bloom filter says maybe, so need to check entire list
            n_bloom_maybe_g++;
            DL_FOREACH(entry->metadata, elt)
            {
                if (PDC_metadata_cmp(elt, a) == 0) {
                    ret_value = elt;
                    goto done;
                }
            }
        }
    }
    else {
        // Bloom has not been created
        DL_FOREACH(entry->metadata, elt)
        {
            if (PDC_metadata_cmp(elt, a) == 0) {
                ret_value = elt;
                goto done;
            }
        }
    } // if bloom==NULL

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Allocate a new object ID
 *
 * \return 64-bit integer of object ID
 */
static uint64_t
PDC_Server_gen_obj_id()
{
    uint64_t ret_value;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&gen_obj_id_mutex_g);
#endif

    ret_value = pdc_id_seq_g++;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&gen_obj_id_mutex_g);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_init_hash_table()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    // Metadata hash table
    metadata_hash_table_g = hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (metadata_hash_table_g == NULL) {
        printf("==PDC_SERVER: metadata_hash_table_g init error! Exit...\n");
        goto done;
    }
    hash_table_register_free_functions(metadata_hash_table_g, PDC_Server_metadata_int_hash_key_free,
                                       PDC_Server_metadata_hash_value_free);

    // Container hash table
    container_hash_table_g = hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (container_hash_table_g == NULL) {
        printf("==PDC_SERVER: container_hash_table_g init error! Exit...\n");
        goto done;
    }
    hash_table_register_free_functions(container_hash_table_g, PDC_Server_metadata_int_hash_key_free,
                                       PDC_Server_container_hash_value_free);

    is_hash_table_init_g = 1;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Remove a metadata from bloom filter
 *
 * \param  metadata[IN]     Metadata pointer of the remove target
 * \param  bloom[IN]        Bloom filter's pointer
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t
PDC_Server_remove_from_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (bloom == NULL) {
        printf("==PDC_SERVER: PDC_Server_remove_from_bloom(): bloom pointer is NULL\n");
        ret_value = FAIL;
        goto done;
    }

    char combined_string[TAG_LEN_MAX];
    combine_obj_info_to_string(metadata, combined_string);

    ret_value = BLOOM_REMOVE(bloom, combined_string, strlen(combined_string));
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_remove_from_bloom() - error\n", pdc_server_rank_g);
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Add a metadata to bloom filter
 *
 * \param  metadata[IN]     Metadata pointer of the target
 * \param  bloom[IN]        Bloom filter's pointer
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t
PDC_Server_add_to_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;
    char   combined_string[TAG_LEN_MAX];

    FUNC_ENTER(NULL);

    if (bloom == NULL) {
        goto done;
    }

    combine_obj_info_to_string(metadata, combined_string);

    ret_value = BLOOM_ADD(bloom, combined_string, strlen(combined_string));
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_add_to_bloom() - error \n", pdc_server_rank_g);
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Init a bloom filter
 *
 * \param  entry[IN]     Entry of the metadata hash table
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t
PDC_Server_bloom_init(pdc_hash_table_entry_head *entry)
{
    perr_t ret_value  = 0;
    int    capacity   = 500000;
    double error_rate = 0.05;

    FUNC_ENTER(NULL);

    // Init bloom filter
    n_bloom_maybe_g = 0;
    n_bloom_total_g = 0;

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    entry->bloom = (BLOOM_TYPE_T *)BLOOM_NEW(capacity, error_rate);
    if (!entry->bloom) {
        fprintf(stderr, "ERROR: Could not create bloom filter\n");
        ret_value = -1;
        goto done;
    }

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    server_bloom_init_time_g += ht_total_sec;
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_hash_table_list_insert(pdc_hash_table_entry_head *head, pdc_metadata_t *new)
{
    perr_t          ret_value = SUCCEED;
    pdc_metadata_t *elt;

    FUNC_ENTER(NULL);

    // add to bloom filter
    if (head->n_obj == CREATE_BLOOM_THRESHOLD) {
        PDC_Server_bloom_init(head);
        DL_FOREACH(head->metadata, elt)
        {
            PDC_Server_add_to_bloom(elt, head->bloom);
        }
        ret_value = PDC_Server_add_to_bloom(new, head->bloom);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_hash_table_list_insert() - error add to bloom\n",
                   pdc_server_rank_g);
            goto done;
        }
    }
    else if (head->n_obj >= CREATE_BLOOM_THRESHOLD || head->bloom != NULL) {
        ret_value = PDC_Server_add_to_bloom(new, head->bloom);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_hash_table_list_insert() - error add to bloom\n",
                   pdc_server_rank_g);
            goto done;
        }
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&insert_hash_table_mutex_g);
#endif
    // Currently $metadata is unique, insert to linked list
    DL_APPEND(head->metadata, new);
    head->n_obj++;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&insert_hash_table_mutex_g);
#endif

done:

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_hash_table_list_init(pdc_hash_table_entry_head *entry, uint32_t *hash_key)
{

    perr_t      ret_value = SUCCEED;
    hg_return_t ret;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    // Insert to hash table
    ret = hash_table_insert(metadata_hash_table_g, hash_key, entry);
    if (ret != 1) {
        fprintf(stderr, "PDC_Server_hash_table_list_init(): Error with hash table insert!\n");
        ret_value = FAIL;
        goto done;
    }

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    server_hash_insert_time_g += ht_total_sec;
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_add_tag_metadata(metadata_add_tag_in_t *in, metadata_add_tag_out_t *out)
{

    perr_t    ret_value = SUCCEED;
    uint32_t *hash_key  = NULL;
#ifdef ENABLE_MULTITHREAD
    int unlocked = 0;
#endif

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    hash_key = (uint32_t *)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key       = in->hash_value;
    uint64_t obj_id = in->obj_id;

    pdc_hash_table_entry_head *lookup_value;

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            // Check if there exist metadata identical to current one
            pdc_metadata_t *target;
            target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
            if (target != NULL) {
                // Check and find valid add_tag fields
                // Currently user_id, obj_name are not supported to be updated in this way
                // obj_name change is done through client with delete and add operation.
                if (in->new_tag != NULL && in->new_tag[0] != 0 &&
                    !(in->new_tag[0] == ' ' && in->new_tag[1] == 0)) {
                    // add a ',' to separate different tags
                    target->tags[strlen(target->tags) + 1] = 0;
                    target->tags[strlen(target->tags)]     = ',';
                    strcat(target->tags, in->new_tag);
                    out->ret = 1;
                }
                else
                    out->ret = -1;
            } // end if (target != NULL)
            else {
                // Object not found for deletion request
                printf("==PDC_SERVER: add tag target not found 1!\n");
                out->ret = -1;
            }

        } // end if lookup_value != NULL
        else {
            printf("==PDC_SERVER: add tag target not found 2!\n");
            out->ret = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
        ret_value = FAIL;
        out->ret  = -1;
        goto done;
    }

    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - error \n", pdc_server_rank_g, __func__);
        goto done;
    }

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

done:
#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out)
{
    perr_t                     ret_value = SUCCEED;
    uint64_t                   obj_id;
    pdc_hash_table_entry_head *lookup_value;
    uint32_t *                 hash_key = NULL;
    pdc_metadata_t *           target;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    hash_key = (uint32_t *)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    obj_id    = in->obj_id;

#ifdef ENABLE_MULTITHREAD
    int unlocked = 0;
    // Obtain lock for hash table
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            // Check if there exist metadata identical to current one
            target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
            if (target != NULL) {
                // Check and find valid update fields
                // Currently user_id, obj_name are not supported to be updated in this way
                // obj_name change is done through client with delete and add operation.
                if (in->new_metadata.time_step != -1)
                    target->time_step = in->new_metadata.time_step;
                if (in->new_metadata.app_name[0] != 0 &&
                    !(in->new_metadata.app_name[0] == ' ' && in->new_metadata.app_name[1] == 0))
                    strcpy(target->app_name, in->new_metadata.app_name);
                if (in->new_metadata.data_location[0] != 0 &&
                    !(in->new_metadata.data_location[0] == ' ' && in->new_metadata.data_location[1] == 0))
                    strcpy(target->data_location, in->new_metadata.data_location);
                if (in->new_metadata.tags[0] != 0 &&
                    !(in->new_metadata.tags[0] == ' ' && in->new_metadata.tags[1] == 0)) {
                    // add a ',' to separate different tags
                    target->tags[strlen(target->tags) + 1] = 0;
                    target->tags[strlen(target->tags)]     = ',';
                    strcat(target->tags, in->new_metadata.tags);
                }
                if (in->new_metadata.current_state != 0) {
                    target->transform_state          = in->new_metadata.current_state;
                    target->current_state.dtype      = in->new_metadata.t_dtype;
                    target->current_state.ndim       = in->new_metadata.t_ndim;
                    target->current_state.dims[0]    = in->new_metadata.t_dims0;
                    target->current_state.dims[1]    = in->new_metadata.t_dims1;
                    target->current_state.dims[2]    = in->new_metadata.t_dims2;
                    target->current_state.dims[3]    = in->new_metadata.t_dims3;
                    target->current_state.meta_index = in->new_metadata.t_meta_index;
                }
                out->ret = 1;
            } // if (lookup_value != NULL)
            else {
                // Object not found for deletion request
                ret_value = -1;
                out->ret  = -1;
            }

        } // if lookup_value != NULL
        else {
            ret_value = -1;
            out->ret  = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        out->ret  = -1;
        goto done;
    }

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

done:
#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out)
{
    perr_t            ret_value = FAIL;
    pdc_metadata_t *  elt;
    HashTableIterator hash_table_iter;
    HashTablePair     pair;
    uint64_t          target_obj_id;
    int               n_entry;

    FUNC_ENTER(NULL);

    out->ret = -1;

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    target_obj_id = in->obj_id;

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (container_hash_table_g != NULL) {
        pdc_cont_hash_table_entry_t *cont_entry;

        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(container_hash_table_g);
        hash_table_iterate(container_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair       = hash_table_iter_next(&hash_table_iter);
            cont_entry = pair.value;
            if (cont_entry == NULL)
                continue;

            if (cont_entry->cont_id == target_obj_id) {
                hash_table_remove(container_hash_table_g, &pair.key);
                out->ret  = 1;
                ret_value = SUCCEED;
                goto done;
            }
        }
    }
    if (out->ret == -1 && metadata_hash_table_g != NULL) {

        // Since we only have the obj id, need to iterate the entire hash table
        pdc_hash_table_entry_head *head;

        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {

            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt)
            {

                if (elt->obj_id == target_obj_id) {
                    // We found the delete target
                    // Check if there are more objects in this list
                    if (head->n_obj > 1) {
                        // Remove from bloom filter
                        if (head->bloom != NULL) {
                            PDC_Server_remove_from_bloom(elt, head->bloom);
                        }

                        // Remove from linked list
                        DL_DELETE(head->metadata, elt);
                        head->n_obj--;
                    }
                    else {
                        // This is the last item under the current entry, remove the hash entry
                        uint32_t hash_key = PDC_get_hash_by_name(elt->obj_name);
                        hash_table_remove(metadata_hash_table_g, &hash_key);
                    }
                    out->ret  = 1;
                    ret_value = SUCCEED;
                }
            } // DL_FOREACH
        }     // while
    }         // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        out->ret  = -1;
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
    server_delete_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif

    // Decrement total metadata count
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&n_metadata_mutex_g);
#endif
    n_metadata_g--;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif

#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out)
{
    perr_t          ret_value = SUCCEED;
    uint32_t *      hash_key  = NULL;
    pdc_metadata_t *target;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    hash_key = (uint32_t *)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t             metadata;
    strcpy(metadata.obj_name, in->obj_name);
    metadata.time_step   = in->time_step;
    metadata.app_name[0] = 0;
    metadata.user_id     = -1;
    metadata.obj_id      = 0;

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            // Check if there exist metadata identical to current one
            target = find_identical_metadata(lookup_value, &metadata);
            if (target != NULL) {
                if (lookup_value->n_obj > 1) {
                    // Remove from bloom filter
                    if (lookup_value->bloom != NULL) {
                        PDC_Server_remove_from_bloom(target, lookup_value->bloom);
                    }

                    // Remove from linked list
                    DL_DELETE(lookup_value->metadata, target);
                    lookup_value->n_obj--;
                }
                else {
                    // Remove from hash
                    hash_table_remove(metadata_hash_table_g, hash_key);
                }
                out->ret = 1;

            } // if (lookup_value != NULL)
            else {
                // Object not found for deletion request
                printf("==PDC_SERVER: delete target not found!\n");
                ret_value = -1;
                out->ret  = -1;
            }

        } // if lookup_value != NULL
        else {
            printf("==PDC_SERVER: delete target not found!\n");
            ret_value = -1;
            out->ret  = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        out->ret  = -1;
        goto done;
    }

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
    server_delete_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif
    // Decrement total metadata count
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&n_metadata_mutex_g);
#endif
    n_metadata_g--;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif

done:
#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{
    perr_t          ret_value = SUCCEED;
    pdc_metadata_t *metadata;
    uint32_t *      hash_key, i;
#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif
    // DEBUG
    int debug_flag = 0;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    metadata = (pdc_metadata_t *)malloc(sizeof(pdc_metadata_t));
    if (metadata == NULL) {
        printf("Cannot allocate pdc_metadata_t!\n");
        goto done;
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&total_mem_usage_mutex_g);
#endif
    total_mem_usage_g += sizeof(pdc_metadata_t);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&total_mem_usage_mutex_g);
#endif

    PDC_metadata_init(metadata);
    metadata->cont_id   = in->data.cont_id;
    metadata->data_type = in->data_type;
    metadata->user_id   = in->data.user_id;
    metadata->time_step = in->data.time_step;
    metadata->ndim      = in->data.ndim;
    metadata->dims[0]   = in->data.dims0;
    metadata->dims[1]   = in->data.dims1;
    metadata->dims[2]   = in->data.dims2;
    metadata->dims[3]   = in->data.dims3;
    for (i = metadata->ndim; i < DIM_MAX; i++)
        metadata->dims[i] = 0;

    strcpy(metadata->obj_name, in->data.obj_name);
    strcpy(metadata->app_name, in->data.app_name);
    strcpy(metadata->tags, in->data.tags);
    strcpy(metadata->data_location, in->data.data_location);

    hash_key = (uint32_t *)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t *           found_identical;

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (debug_flag == 1)
        printf("checking hash table with key=%d\n", *hash_key);

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            if (debug_flag == 1)
                printf("lookup_value not NULL!\n");
            // Check if there exist metadata identical to current one
            found_identical = find_identical_metadata(lookup_value, metadata);
            if (found_identical != NULL) {
                printf("==PDC_SERVER[%d]: Found identical metadata with name %s!\n", pdc_server_rank_g,
                       metadata->obj_name);
                out->obj_id = 0;
                free(metadata);
                goto done;
            }
            else {
                PDC_Server_hash_table_list_insert(lookup_value, metadata);
            }
        }
        else {
            // First entry for current hasy_key, init linked list, and insert to hash table
            if (debug_flag == 1) {
                printf("lookup_value is NULL! Init linked list\n");
            }
            fflush(stdout);

            pdc_hash_table_entry_head *entry =
                (pdc_hash_table_entry_head *)malloc(sizeof(pdc_hash_table_entry_head));
            entry->bloom    = NULL;
            entry->metadata = NULL;
            entry->n_obj    = 0;
            total_mem_usage_g += sizeof(pdc_hash_table_entry_head);

            PDC_Server_hash_table_list_init(entry, hash_key);
            PDC_Server_hash_table_list_insert(entry, metadata);
        }
    }
    else {
        printf("metadata_hash_table_g not initialized!\n");
        goto done;
    }

    // Generate object id (uint64_t)
    metadata->obj_id = PDC_Server_gen_obj_id();

#ifdef ENABLE_MULTITHREAD
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&n_metadata_mutex_g);
#endif
    n_metadata_g++;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif

    // Fill $out structure for returning the generated obj_id to client
    out->obj_id = metadata->obj_id;

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING
    server_insert_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif

done:
#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_print_all_metadata()
{
    perr_t                     ret_value = SUCCEED;
    HashTableIterator          hash_table_iter;
    pdc_metadata_t *           elt;
    pdc_hash_table_entry_head *head;
    HashTablePair              pair;

    FUNC_ENTER(NULL);

    hash_table_iterate(metadata_hash_table_g, &hash_table_iter);
    while (hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
        head = pair.value;
        DL_FOREACH(head->metadata, elt)
        {
            PDC_print_metadata(elt);
        }
    }
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_print_all_containers()
{
    int                          i;
    perr_t                       ret_value = SUCCEED;
    HashTableIterator            hash_table_iter;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    HashTablePair                pair;

    FUNC_ENTER(NULL);

    hash_table_iterate(container_hash_table_g, &hash_table_iter);
    while (hash_table_iter_has_more(&hash_table_iter)) {
        pair       = hash_table_iter_next(&hash_table_iter);
        cont_entry = pair.value;
        printf("Container [%s]:", cont_entry->cont_name);
        for (i = 0; i < cont_entry->n_obj; i++) {
            if (cont_entry->obj_ids[i] != 0) {
                printf("%" PRIu64 ", ", cont_entry->obj_ids[i]);
            }
        }
        printf("\n");
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_metadata_duplicate_check()
{
    perr_t                     ret_value = SUCCEED;
    HashTableIterator          hash_table_iter;
    HashTablePair              pair;
    int                        n_entry, count = 0;
    int                        all_maybe, all_total, all_entry;
    int                        has_dup_obj = 0;
    int                        all_dup_obj = 0;
    pdc_metadata_t *           elt, *elt_next;
    pdc_hash_table_entry_head *head;

    FUNC_ENTER(NULL);

    n_entry = hash_table_num_entries(metadata_hash_table_g);

#ifdef ENABLE_MPI
    MPI_Reduce(&n_bloom_maybe_g, &all_maybe, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&n_bloom_total_g, &all_total, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&n_entry, &all_entry, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_maybe   = n_bloom_maybe_g;
    all_total   = n_bloom_total_g;
    all_entry   = n_entry;
#endif

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: Bloom filter says maybe %d times out of %d\n", all_maybe, all_total);
        printf("==PDC_SERVER: Metadata duplicate check with %d hash entries ", all_entry);
    }

    fflush(stdout);

    hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

    while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
        head = pair.value;
        DL_SORT(head->metadata, PDC_metadata_cmp);
        // With sorted list, just compare each one with its next
        DL_FOREACH(head->metadata, elt)
        {
            elt_next = elt->next;
            if (elt_next != NULL) {
                if (PDC_metadata_cmp(elt, elt_next) == 0) {
                    has_dup_obj = 1;
                    ret_value   = FAIL;
                    goto done;
                }
            }
        }
        count++;
    }

    fflush(stdout);

done:
#ifdef ENABLE_MPI
    MPI_Reduce(&has_dup_obj, &all_dup_obj, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_dup_obj = has_dup_obj;
#endif
    if (pdc_server_rank_g == 0) {
        if (all_dup_obj > 0) {
            printf("  ...Found duplicates!\n");
        }
        else {
            printf("  ...No duplicates found!\n");
        }
    }

    FUNC_LEAVE(ret_value);
}

/*
 * Check if the metadata satisfies the constraint received from client
 *
 * \param  metadata[IN]       Metadata pointer
 * \param  constraints[IN]    Constraints received from client
 *
 * \return 1 if the metadata satisfies the constratins/-1 otherwise
 */
static int
is_metadata_satisfy_constraint(pdc_metadata_t *metadata, metadata_query_transfer_in_t *constraints)
{
    int ret_value = 1;

    FUNC_ENTER(NULL);

    if (constraints->user_id > 0 && constraints->user_id != metadata->user_id) {
        ret_value = -1;
        goto done;
    }
    if (strcmp(constraints->app_name, " ") != 0 && strcmp(metadata->app_name, constraints->app_name) != 0) {
        ret_value = -1;
        goto done;
    }
    if (strcmp(constraints->obj_name, " ") != 0 && strcmp(metadata->obj_name, constraints->obj_name) != 0) {
        ret_value = -1;
        goto done;
    }
    if (constraints->time_step_from > 0 && constraints->time_step_to > 0 &&
        (metadata->time_step < constraints->time_step_from ||
         metadata->time_step > constraints->time_step_to)) {
        ret_value = -1;
        goto done;
    }
    if (constraints->ndim > 0 && metadata->ndim != constraints->ndim) {
        ret_value = -1;
        goto done;
    }
    // TODO: Currently only supports searching with one tag
    if (strcmp(constraints->tags, " ") != 0 && strstr(metadata->tags, constraints->tags) == NULL) {
        ret_value = -1;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs)
{
    perr_t                     ret_value = FAIL;
    uint32_t                   i;
    uint32_t                   n_buf, iter = 0;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *           elt;
    HashTableIterator          hash_table_iter;
    int                        n_entry;
    HashTablePair              pair;

    FUNC_ENTER(NULL);

    // n_buf = n_metadata_g + 1 for potential padding array
    n_buf     = n_metadata_g + 1;
    *buf_ptrs = (void **)calloc(n_buf, sizeof(void *));
    for (i = 0; i < n_buf; i++) {
        (*buf_ptrs)[i] = (void *)calloc(1, sizeof(void *));
    }
    // TODO: free buf_ptrs
    if (metadata_hash_table_g != NULL) {

        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            DL_FOREACH(head->metadata, elt)
            {
                // List all objects, no need to check other constraints
                if (in->is_list_all == 1) {
                    (*buf_ptrs)[iter++] = elt;
                }
                // check if current metadata matches search constraint
                else if (is_metadata_satisfy_constraint(elt, in) == 1) {
                    (*buf_ptrs)[iter++] = elt;
                }
            }
        }
        *n_meta = iter;
    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        goto done;
    }

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_get_kvtag_query_result(pdc_kvtag_t *in, uint32_t *n_meta, uint64_t **obj_ids)
{
    perr_t                     ret_value = SUCCEED;
    uint32_t                   iter      = 0;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *           elt;
    pdc_kvtag_list_t *         kvtag_list_elt;
    HashTableIterator          hash_table_iter;
    int                        n_entry, is_name_match, is_value_match;
    HashTablePair              pair;
    uint32_t                   alloc_size = 100;

    FUNC_ENTER(NULL);

    *n_meta = 0;
    // TODO: free obj_ids
    *obj_ids = (void *)calloc(alloc_size, sizeof(uint64_t));

    if (metadata_hash_table_g != NULL) {

        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            DL_FOREACH(head->metadata, elt)
            {
                DL_FOREACH(elt->kvtag_list_head, kvtag_list_elt)
                {
                    is_name_match  = 0;
                    is_value_match = 0;
                    if (in->name[0] != ' ') {
                        if (strcmp(in->name, kvtag_list_elt->kvtag->name) == 0)
                            is_name_match = 1;
                        else
                            continue;
                    }
                    else
                        is_name_match = 1;

                    if (((char *)(in->value))[0] != ' ') {
                        if (memcmp(in->value, kvtag_list_elt->kvtag->value, in->size) == 0)
                            is_value_match = 1;
                        else
                            continue;
                    }
                    else
                        is_value_match = 1;

                    if (is_name_match == 1 && is_value_match == 1) {
                        if (iter >= alloc_size) {
                            alloc_size *= 2;
                            *obj_ids = (void *)realloc(*obj_ids, alloc_size * sizeof(uint64_t));
                        }
                        (*obj_ids)[iter++] = elt->obj_id;
                        break;
                    }

                } // End for each kvtag
            }     // End for each metadata
        }         // End while
        *n_meta = iter;
    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts,
                                     pdc_metadata_t **out)
{
    perr_t                     ret_value = SUCCEED;
    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t             metadata;
    const char *               name;

    FUNC_ENTER(NULL);

    *out = NULL;

    // Set up a metadata struct to query
    PDC_Server_metadata_init(&metadata);

    name = obj_name;

    strcpy(metadata.obj_name, name);
    metadata.time_step = ts;

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            *out = find_identical_metadata(lookup_value, &metadata);

            if (*out == NULL) {
                ret_value = FAIL;
                goto done;
            }
        }
        else {
            *out = NULL;
        }
    }
    else {
        printf("metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        goto done;
    }

    if (*out == NULL)
        printf("==PDC_SERVER[%d]: Queried object with name [%s] not found! \n", pdc_server_rank_g, name);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t **out)
{
    perr_t                     ret_value = SUCCEED;
    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t             metadata;
    const char *               name;

    FUNC_ENTER(NULL);

    *out = NULL;

    // Set up a metadata struct to query
    PDC_Server_metadata_init(&metadata);

    name = obj_name;

    strcpy(metadata.obj_name, name);
    // TODO: currently PDC_Client_query_metadata_name_timestep is not taking timestep for querying
    metadata.time_step = 0;

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            *out = find_identical_metadata(lookup_value, &metadata);

            if (*out == NULL) {
                ret_value = FAIL;
                goto done;
            }
        }
        else {
            *out = NULL;
        }
    }
    else {
        printf("metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        goto done;
    }

    if (*out == NULL)
        printf("==PDC_SERVER[%d]: Queried object with name [%s] not found! \n", pdc_server_rank_g, name);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta_ptr)
{
    perr_t ret_value = SUCCEED;

    pdc_hash_table_entry_head *head;
    pdc_metadata_t *           elt;
    HashTableIterator          hash_table_iter;
    int                        n_entry;
    HashTablePair              pair;

    FUNC_ENTER(NULL);

    *res_meta_ptr = NULL;

    if (metadata_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt)
            {
                if (elt->obj_id == obj_id) {
                    *res_meta_ptr = elt;
                    goto done;
                }
            }
        }
    }
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value     = FAIL;
        *res_meta_ptr = NULL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function for get the metadata by ID
 *
 * \param  callback_info[IN]     Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
PDC_Server_get_metadata_by_id_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                ret_value;
    hg_handle_t                handle;
    pdc_metadata_t *           meta = NULL;
    get_metadata_by_id_args_t *cb_args;
    get_metadata_by_id_out_t   output;

    FUNC_ENTER(NULL);

    cb_args = (get_metadata_by_id_args_t *)callback_info->arg;
    handle  = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_get_metadata_by_id_cb - error HG_Get_output\n",
               pdc_server_rank_g);
        goto done;
    }

    if (output.res_meta.obj_id != 0) {
        // TODO free metdata
        meta = (pdc_metadata_t *)malloc(sizeof(pdc_metadata_t));
        PDC_transfer_t_to_metadata_t(&output.res_meta, meta);
    }
    else {
        printf("==PDC_SERVER[%d]: %s - no valid metadata is retrieved\n", pdc_server_rank_g, __func__);
        goto done;
    }

    // Execute the callback function
    if (NULL != cb_args->cb) {
        ((region_list_t *)(cb_args->args))->meta = meta;
        cb_args->cb(cb_args->args, PDC_POSIX);
    }
    else {
        printf("==PDC_SERVER[%d]: %s NULL callback ptr\n", pdc_server_rank_g, __func__);
        goto done;
    }

done:
    HG_Free_output(handle, &output);
    free(cb_args);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_get_metadata_by_id_with_cb(uint64_t obj_id, perr_t (*cb)(), void *args)
{
    pdc_metadata_t *           res_meta_ptr = NULL;
    hg_return_t                hg_ret;
    perr_t                     ret_value = SUCCEED;
    uint32_t                   server_id = 0;
    hg_handle_t                get_metadata_by_id_handle;
    get_metadata_by_id_args_t *cb_args;
    get_metadata_by_id_in_t    in;

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        // Metadata object is local, no need to send update RPC
        ret_value = PDC_Server_get_local_metadata_by_id(obj_id, &res_meta_ptr);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_get_local_metadata_by_id FAILED!\n", pdc_server_rank_g);
            goto done;
        }

        ((region_list_t *)args)->meta = res_meta_ptr;
        // Call the callback function directly and pass in the result metadata ptr
        cb(args, PDC_POSIX);
    }
    else {
        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", pdc_server_rank_g,
                   server_id);
            ret_value = FAIL;
            goto done;
        }

        HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_metadata_by_id_register_id_g,
                  &get_metadata_by_id_handle);

        cb_args       = (get_metadata_by_id_args_t *)malloc(sizeof(get_metadata_by_id_args_t));
        in.obj_id     = obj_id;
        cb_args->cb   = cb;
        cb_args->args = args;

        // TODO: Include the callback function ptr and args in the cb_args
        hg_ret = HG_Forward(get_metadata_by_id_handle, PDC_Server_get_metadata_by_id_cb, &cb_args, &in);

        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - Could not forward\n", pdc_server_rank_g, __func__);
            res_meta_ptr = NULL;
            HG_Destroy(get_metadata_by_id_handle);
            return FAIL;
        }
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_create_container(gen_cont_id_in_t *in, gen_cont_id_out_t *out)
{
    perr_t    ret_value = SUCCEED;
    uint32_t *hash_key;

    FUNC_ENTER(NULL);

    out->cont_id = 0;

#ifdef ENABLE_TIMING
    // Timing
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&total_mem_usage_mutex_g);
#endif
    total_mem_usage_g += sizeof(uint32_t);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&total_mem_usage_mutex_g);
#endif

    pdc_cont_hash_table_entry_t *lookup_value;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_container_hash_table_mutex_g);
#endif

    if (container_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(container_hash_table_g, &in->hash_value);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            // Check if there exist container identical to current one
            out->cont_id = lookup_value->cont_id;
        }
        else {
            hash_key = (uint32_t *)malloc(sizeof(uint32_t));
            if (hash_key == NULL) {
                printf("Cannot allocate hash_key!\n");
                ret_value = FAIL;
                goto done;
            }
            *hash_key = in->hash_value;

            pdc_cont_hash_table_entry_t *entry =
                (pdc_cont_hash_table_entry_t *)calloc(1, sizeof(pdc_cont_hash_table_entry_t));
            strcpy(entry->cont_name, in->cont_name);
            entry->n_obj       = 0;
            entry->n_allocated = 0;
            entry->obj_ids     = NULL;
            entry->cont_id     = PDC_Server_gen_obj_id();
#ifdef ENABLE_MULTITHREAD
            hg_thread_mutex_lock(&total_mem_usage_mutex_g);
#endif
            total_mem_usage_g += sizeof(pdc_cont_hash_table_entry_t);
#ifdef ENABLE_MULTITHREAD
            hg_thread_mutex_unlock(&total_mem_usage_mutex_g);
#endif
            // Insert to hash table
            if (hash_table_insert(container_hash_table_g, hash_key, entry) != 1) {
                printf("==PDC_SERVER[%d]: %s - hash table insert failed\n", pdc_server_rank_g, __func__);
                ret_value = FAIL;
            }
            else
                out->cont_id = entry->cont_id;
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g,
               __func__);
        goto done;
    }

#ifdef ENABLE_MULTITHREAD
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_container_hash_table_mutex_g);
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    server_insert_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Delete a container by name
 *
 * \param  in[IN]       Input structure received from client, conatins metadata
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t
PDC_Server_delete_container_by_name(gen_cont_id_in_t *in, gen_cont_id_out_t *out)
{
    perr_t   ret_value = SUCCEED;
    uint32_t hash_key;

    FUNC_ENTER(NULL);

    out->cont_id = 0;

    hash_key = in->hash_value;

    pdc_cont_hash_table_entry_t *lookup_value;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_container_hash_table_mutex_g);
#endif

    if (container_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(container_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            // Check if there exist metadata identical to current one
            printf("==PDC_SERVER[%d]: Found existing container with same hash value, name=%s!\n",
                   pdc_server_rank_g, lookup_value->cont_name);
            out->cont_id = 0;
            goto done;
        }
        else {
            // Check if there exist metadata identical to current one
            printf("==PDC_SERVER[%d]: Did not found existing container with same hash value, name=%s!\n",
                   pdc_server_rank_g, lookup_value->cont_name);
            ret_value = FAIL;
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g,
               __func__);
        goto done;
    }

#ifdef ENABLE_MULTITHREAD
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_container_hash_table_mutex_g);
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_find_container_by_name(const char *cont_name, pdc_cont_hash_table_entry_t **out)
{
    perr_t   ret_value = SUCCEED;
    uint32_t hash_key;

    FUNC_ENTER(NULL);
    if (NULL == cont_name || NULL == out) {
        printf("==PDC_SERVER[%d]: %s - input is NULL! \n", pdc_server_rank_g, __func__);
        goto done;
    }

    if (container_hash_table_g != NULL) {
        // lookup
        hash_key = PDC_get_hash_by_name(cont_name);
        *out     = hash_table_lookup(container_hash_table_g, &hash_key);
        if (*out != NULL) {
            // Double check with name match
            if (strcmp(cont_name, (*out)->cont_name) != 0) {
                *out = NULL;
            }
        }
    }
    else {
        printf("container_hash_table_g not initialized!\n");
        ret_value = -1;
        goto done;
    }

    if (*out == NULL)
        printf("==PDC_SERVER[%d]: container [%s] not found! \n", pdc_server_rank_g, cont_name);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Search a container by obj id
 *
 * \param  cont_id[IN]        Container  ID
 * \param  out[OUT]           Pointer to the result container
 *
 * \return Non-negative on success/Negative on failure
 */

static perr_t
PDC_Server_find_container_by_id(uint64_t cont_id, pdc_cont_hash_table_entry_t **out)
{
    perr_t                       ret_value = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry;
    HashTableIterator            hash_table_iter;
    int                          n_entry;
    HashTablePair                pair;

    FUNC_ENTER(NULL);

    if (NULL == out) {
        printf("==PDC_SERVER[%d]: %s - input is NULL! \n", pdc_server_rank_g, __func__);
        goto done;
    }

    if (container_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(container_hash_table_g);
        hash_table_iterate(container_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair       = hash_table_iter_next(&hash_table_iter);
            cont_entry = pair.value;
            if (cont_entry == NULL)
                continue;

            if (cont_entry->cont_id == cont_id) {
                *out = cont_entry;
                goto done;
            }
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g,
               __func__);
        ret_value = FAIL;
        out       = NULL;
        goto done;
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_container_add_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id)
{
    perr_t                       ret_value    = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry   = NULL;
    int                          realloc_size = 0;

    FUNC_ENTER(NULL);
    ret_value = PDC_Server_find_container_by_id(cont_id, &cont_entry);

    if (cont_entry != NULL) {
        // Check if need to allocate space
        if (cont_entry->n_allocated == 0) {
            cont_entry->n_allocated = PDC_ALLOC_BASE_NUM;
            cont_entry->obj_ids     = (uint64_t *)calloc(sizeof(uint64_t), PDC_ALLOC_BASE_NUM);
            total_mem_usage_g += sizeof(uint64_t) * cont_entry->n_allocated;
        }
        else if (cont_entry->n_allocated < cont_entry->n_obj + n_obj) {
            // Extend the allocated space by twice its original size
            // or twice the n_obj size, whichever greater
            realloc_size = cont_entry->n_allocated > n_obj ? cont_entry->n_allocated : n_obj;
            realloc_size *= (sizeof(uint64_t) * 2);

            if (is_debug_g == 1) {
                printf("==PDC_SERVER[%d]: realloc from %d to %ld!\n", pdc_server_rank_g,
                       cont_entry->n_allocated, realloc_size / sizeof(uint64_t));
            }

            cont_entry->obj_ids = (uint64_t *)realloc(cont_entry->obj_ids, realloc_size);
            if (NULL == cont_entry->obj_ids) {
                printf("==PDC_SERVER[%d]: %s - ERROR with realloc!\n", pdc_server_rank_g, __func__);
                ret_value = FAIL;
                goto done;
            }
            total_mem_usage_g -= sizeof(uint64_t) * cont_entry->n_allocated;

            cont_entry->n_allocated = realloc_size / sizeof(uint64_t);
            total_mem_usage_g += sizeof(uint64_t) * cont_entry->n_allocated;
        }

        // Append the new ids
        memcpy(cont_entry->obj_ids + cont_entry->n_obj, obj_ids, n_obj * sizeof(uint64_t));
        cont_entry->n_obj += n_obj;

        // Debug prints
        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: add %d objects to container %" PRIu64 ", total %d !\n",
                   pdc_server_rank_g, n_obj, cont_id, cont_entry->n_obj - cont_entry->n_deleted);
        }

        // TODO: find duplicates
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container %" PRIu64 " not found!\n", pdc_server_rank_g, __func__,
               cont_id);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_container_del_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id)
{
    perr_t                       ret_value  = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    int                          i, j;
    int                          n_deletes = 0;

    FUNC_ENTER(NULL);
    ret_value = PDC_Server_find_container_by_id(cont_id, &cont_entry);

    if (cont_entry != NULL) {
        for (i = 0; i < n_obj; i++) {
            for (j = 0; j < cont_entry->n_obj; j++) {
                if (cont_entry->obj_ids[j] == obj_ids[i]) {
                    cont_entry->obj_ids[j] = 0;
                    cont_entry->n_deleted++;
                    n_deletes++;
                    break;
                }
            }
        }
        // Debug print
        printf("==PDC_SERVER[%d]: successfully deleted %d objects!\n", pdc_server_rank_g, n_deletes);

        if (n_deletes != n_obj) {
            printf("==PDC_SERVER[%d]: %s - %d objects are not found to be deleted!\n", pdc_server_rank_g,
                   __func__, n_obj - n_deletes);
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container %" PRIu64 " not found!\n", pdc_server_rank_g, __func__,
               cont_id);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_container_add_tags(uint64_t cont_id, char *tags)
{
    perr_t                       ret_value  = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;

    FUNC_ENTER(NULL);
    ret_value = PDC_Server_find_container_by_id(cont_id, &cont_entry);

    if (cont_entry != NULL) {

        if (tags != NULL) {
            strcat(cont_entry->tags, tags);
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container %" PRIu64 " not found!\n", pdc_server_rank_g, __func__,
               cont_id);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_copy_all_storage_meta(pdc_metadata_t *meta, region_storage_meta_t **storage_meta, int *n_region)
{
    perr_t         ret_value  = SUCCEED;
    region_list_t *region_elt = NULL, *region_head = NULL;
    int            i, region_cnt;

    FUNC_ENTER(NULL);

    if (NULL == meta || NULL == storage_meta) {
        ret_value = FAIL;
        goto done;
    }

    region_head = meta->storage_region_list_head;
    DL_COUNT(region_head, region_elt, region_cnt);
    *n_region     = region_cnt;
    *storage_meta = (region_storage_meta_t *)calloc(sizeof(region_storage_meta_t), region_cnt);

    i = 0;
    DL_FOREACH(region_head, region_elt)
    {
        (*storage_meta)[i].obj_id = meta->obj_id;
        (*storage_meta)[i].size   = region_elt->data_size;
        PDC_region_list_t_to_transfer(region_elt, &((*storage_meta)[i].region_transfer));

        // Check if cache available
        if (strstr(region_elt->cache_location, "PDCcache") != NULL) {
            strcpy((*storage_meta)[i].storage_location, region_elt->cache_location);
            (*storage_meta)[i].offset = region_elt->cache_offset;
        }
        else {
            strcpy((*storage_meta)[i].storage_location, region_elt->storage_location);
            (*storage_meta)[i].offset = region_elt->offset;
        }
        i++;
    }

done:
    FUNC_LEAVE(ret_value);
}

// Get storage metadata of objects with a list of names and transfer back to client
static perr_t
PDC_Server_get_storage_meta_by_names(query_read_names_args_t *args)
{
    hg_return_t             hg_ret = HG_SUCCESS;
    perr_t                  ret_value;
    hg_handle_t             rpc_handle;
    hg_bulk_t               bulk_handle;
    bulk_rpc_in_t           bulk_rpc_in;
    void **                 buf_ptrs;
    hg_size_t *             buf_sizes;
    uint32_t                client_id;
    char *                  obj_name;
    pdc_metadata_t *        meta = NULL;
    int                     i = 0, j = 0;
    region_storage_meta_t **all_storage_meta;
    int *                   all_nregion, total_region;
    FUNC_ENTER(NULL);

    // Get the storage meta for each queried object name
    all_storage_meta = (region_storage_meta_t **)calloc(sizeof(region_storage_meta_t *), args->cnt);
    all_nregion      = (int *)calloc(sizeof(int), args->cnt);

    total_region = 0;
    for (i = 0; i < args->cnt; i++) {
        obj_name = args->obj_names[i];

        // FIXME: currently assumes timestep 0
        PDC_Server_search_with_name_timestep(obj_name, PDC_get_hash_by_name(obj_name), 0, &meta);
        if (meta == NULL) {
            printf("==PDC_SERVER[%d]: No metadata with name [%s] found!\n", pdc_server_rank_g, obj_name);
            continue;
        }

        ret_value = PDC_copy_all_storage_meta(meta, &(all_storage_meta[i]), &(all_nregion[i]));
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: error when getting storage meta for [%s]!\n", pdc_server_rank_g,
                   obj_name);
            continue;
        }
        total_region += all_nregion[i];

        if (all_storage_meta[i]->storage_location[1] != 'g') {
            printf("==PDC_SERVER[%d]: error with storage meta for [%s], obj_id %" PRIu64 ", loc [%s], offset "
                   "%" PRIu64 "\n",
                   pdc_server_rank_g, obj_name, all_storage_meta[i]->obj_id,
                   all_storage_meta[i]->storage_location, all_storage_meta[i]->offset);
            fflush(stdout);
        }

    } // End for cnt

    // Now the storage meta is stored in all_storage_meta;
    client_id = args->client_id;
    if (PDC_Server_lookup_client(client_id) != SUCCEED) {
        printf("==PDC_SERVER[%d]: Error getting client %d addr via lookup\n", pdc_server_rank_g, client_id);
        ret_value = FAIL;
        goto done;
    }

    // Now we have all the storage metadata of the queried objects, send them back to client with
    // bulk transfer to args->origin_id
    // Prepare bulk ptrs, buf_ptrs[0] is task_id
    int nbuf  = total_region + 1;
    buf_sizes = (hg_size_t *)calloc(nbuf, sizeof(hg_size_t));
    buf_ptrs  = (void **)calloc(nbuf, sizeof(void *));

    buf_ptrs[0]  = &(args->client_seq_id);
    buf_sizes[0] = sizeof(int);

    int iter = 1;
    for (i = 0; i < args->cnt; i++) {
        for (j = 0; j < all_nregion[i]; j++) {
            buf_ptrs[iter]  = &(all_storage_meta[i][j]);
            buf_sizes[iter] = sizeof(region_storage_meta_t);
            iter++;
        }
    }

    /* Register memory */
    hg_ret = HG_Bulk_create(hg_class_g, nbuf, buf_ptrs, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g[client_id].addr == NULL) {
        printf("==PDC_SERVER[%d]: Error with client %d addr\n", pdc_server_rank_g, client_id);
        goto done;
    }

    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr,
                       send_client_storage_meta_rpc_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create handle\n");
        ret_value = FAIL;
        goto done;
    }

    /* Fill input structure */
    bulk_rpc_in.cnt         = total_region;
    bulk_rpc_in.seq_id      = args->client_seq_id;
    bulk_rpc_in.origin      = pdc_server_rank_g;
    bulk_rpc_in.bulk_handle = bulk_handle;

    hg_ret = HG_Forward(rpc_handle, PDC_check_int_ret_cb, NULL, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not forward call\n");
        ret_value = FAIL;
        goto done;
    }

    HG_Destroy(rpc_handle);

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

/*
 * Wrapper function for callback
 *
 * \param  callback_info[IN]              Callback info pointer
 *
 * \return Non-negative on success/Negative on failure
 */
hg_return_t
PDC_Server_query_read_names_clinet_cb(const struct hg_cb_info *callback_info)
{
    PDC_Server_get_storage_meta_by_names((query_read_names_args_t *)callback_info->arg);

    return HG_SUCCESS;
}

perr_t
PDC_free_cont_hash_table()
{
    if (container_hash_table_g != NULL)
        hash_table_free(container_hash_table_g);
    return SUCCEED;
}

/*
 * Add the kvtag received from one client to the corresponding metadata structure
 *
 * \param  in[IN]       Input structure received from client
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t
PDC_add_kvtag_to_list(pdc_kvtag_list_t **list_head, pdc_kvtag_t *tag)
{
    perr_t            ret_value = SUCCEED;
    pdc_kvtag_list_t *new_list_item;
    pdc_kvtag_t *     newtag;
    FUNC_ENTER(NULL);

    PDC_kvtag_dup(tag, &newtag);
    new_list_item        = (pdc_kvtag_list_t *)calloc(1, sizeof(pdc_kvtag_list_t));
    new_list_item->kvtag = newtag;
    DL_APPEND(*list_head, new_list_item);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_add_kvtag(metadata_add_kvtag_in_t *in, metadata_add_tag_out_t *out)
{

    perr_t   ret_value = SUCCEED;
    uint32_t hash_key;
    uint64_t obj_id;
#ifdef ENABLE_MULTITHREAD
    int unlocked;
#endif
    pdc_hash_table_entry_head *  lookup_value;
    pdc_cont_hash_table_entry_t *cont_lookup_value;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

    hash_key = in->hash_value;
    obj_id   = in->obj_id;

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);
    if (lookup_value != NULL) {
        pdc_metadata_t *target;
        target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
        if (target != NULL) {
            PDC_add_kvtag_to_list(&target->kvtag_list_head, &in->kvtag);
            out->ret = 1;
        } // if (lookup_value != NULL)
        else {
            // Object not found
            ret_value = FAIL;
            out->ret  = -1;
        }

    }      // if lookup_value != NULL
    else { // look for containers
        cont_lookup_value = hash_table_lookup(container_hash_table_g, &hash_key);
        if (cont_lookup_value != NULL) {
            PDC_add_kvtag_to_list(&cont_lookup_value->kvtag_list_head, &in->kvtag);
            out->ret = 1;
        }
        else {
            printf("==PDC_SERVER[%d]: add tag target %" PRIu64 " not found!\n", pdc_server_rank_g, obj_id);
            ret_value = FAIL;
            out->ret  = -1;
        }
    }

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

static perr_t
PDC_get_kvtag_value_from_list(pdc_kvtag_list_t **list_head, char *key, metadata_get_kvtag_out_t *out)
{
    perr_t            ret_value = SUCCEED;
    pdc_kvtag_list_t *elt;

    FUNC_ENTER(NULL);

    DL_FOREACH(*list_head, elt)
    {
        if (strcmp(elt->kvtag->name, key) == 0) {
            out->kvtag.name  = elt->kvtag->name;
            out->kvtag.size  = elt->kvtag->size;
            out->kvtag.value = elt->kvtag->value;
            break;
        }
    }

    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_get_kvtag(metadata_get_kvtag_in_t *in, metadata_get_kvtag_out_t *out)
{

    perr_t   ret_value = SUCCEED;
    uint32_t hash_key;
    uint64_t obj_id;
#ifdef ENABLE_MULTITHREAD
    int unlocked;
#endif
    pdc_hash_table_entry_head *  lookup_value;
    pdc_cont_hash_table_entry_t *cont_lookup_value;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

    hash_key = in->hash_value;
    obj_id   = in->obj_id;

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);
    if (lookup_value != NULL) {
        pdc_metadata_t *target;
        target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
        if (target != NULL) {
            PDC_get_kvtag_value_from_list(&target->kvtag_list_head, in->key, out);
            out->ret = 1;
        }
        else {
            ret_value = FAIL;
            out->ret  = -1;
        }
    }
    else {

        cont_lookup_value = hash_table_lookup(container_hash_table_g, &hash_key);
        if (cont_lookup_value != NULL) {
            PDC_get_kvtag_value_from_list(&cont_lookup_value->kvtag_list_head, in->key, out);
            out->ret = 1;
        }
        else {
            ret_value = FAIL;
            out->ret  = -1;
        }
    }

    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - error \n", pdc_server_rank_g, __func__);
        goto done;
    }

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

done:
#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_del_kvtag_value_from_list(pdc_kvtag_list_t **list_head, char *key)
{
    perr_t            ret_value = SUCCEED;
    pdc_kvtag_list_t *elt;

    FUNC_ENTER(NULL);

    DL_FOREACH(*list_head, elt)
    {
        if (strcmp(elt->kvtag->name, key) == 0) {
            free(elt->kvtag->name);
            free(elt->kvtag->value);
            free(elt->kvtag);
            DL_DELETE(*list_head, elt);
            free(elt);
            break;
        }
    }

    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_del_kvtag(metadata_get_kvtag_in_t *in, metadata_add_tag_out_t *out)
{

    perr_t   ret_value = SUCCEED;
    uint32_t hash_key;
    uint64_t obj_id;
#ifdef ENABLE_MULTITHREAD
    int unlocked;
#endif
    pdc_hash_table_entry_head *lookup_value;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
    double         ht_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

    hash_key = in->hash_value;
    obj_id   = in->obj_id;

#ifdef ENABLE_MULTITHREAD
    // Obtain lock for hash table
    unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);
    if (lookup_value != NULL) {
        pdc_metadata_t *target;
        target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
        if (target != NULL) {
            PDC_del_kvtag_value_from_list(&target->kvtag_list_head, in->key);
            out->ret = 1;
        }
        else {
            ret_value = FAIL;
            out->ret  = -1;
        }
    }
    else {
        ret_value = FAIL;
        out->ret  = -1;
    }

    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - error \n", pdc_server_rank_g, __func__);
        goto done;
    }

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

done:
#ifdef ENABLE_MULTITHREAD
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}
