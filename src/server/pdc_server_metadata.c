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

#include "utlist.h"
#include "hash-table.h"
#include "dablooms.h"

#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server_metadata.h"

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
int      n_bloom_total_g                = 0;
int      n_bloom_maybe_g                = 0;
double   server_bloom_check_time_g      = 0.0;
double   server_bloom_insert_time_g     = 0.0;
double   server_insert_time_g           = 0.0;
double   server_delete_time_g           = 0.0;
double   server_update_time_g           = 0.0;
double   server_hash_insert_time_g      = 0.0;
double   server_bloom_init_time_g       = 0.0;
uint32_t n_metadata_g                   = 0;


pbool_t region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2)
{
    pbool_t ret_value = 0;

    FUNC_ENTER(NULL);

    if(reg1.ndim != reg2.ndim)
        PGOTO_DONE(ret_value);
    if (reg1.ndim >= 1) {
        if(reg1.count_0 != reg2.count_0 || reg1.start_0 != reg2.start_0)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 2) {
        if(reg1.count_1 != reg2.count_1 || reg1.start_1 != reg2.start_1)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 3) {
        if(reg1.count_2 != reg2.count_2 || reg1.start_2 != reg2.start_2)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 4) {
        if(reg1.count_3 != reg2.count_3 || reg1.start_3 != reg2.start_3)
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
    return *((uint32_t *) vlocation1) == *((uint32_t *) vlocation2);
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
    return *((uint32_t *) vlocation);
}

/*
 * Free the hash key 
 *
 * \param  key [IN]        Hash table key
 *
 * \return void
 */
static void
PDC_Server_metadata_int_hash_key_free(void * key)
{
    free((uint32_t *) key);
}

/*
 * Free metadata hash value
 *
 * \param  value [IN]        Hash table value
 *
 * \return void
 */
static void
PDC_Server_metadata_hash_value_free(void * value)
{
    pdc_metadata_t *elt, *tmp;
    pdc_hash_table_entry_head *head;

    FUNC_ENTER(NULL);
    
    head = (pdc_hash_table_entry_head*) value;

    // Free bloom filter
    if (head->bloom != NULL) {
        BLOOM_FREE(head->bloom);
    }

    // Free metadata list
    if (is_restart_g == 0) {
        /* printf("freeing individual head->metadata\n"); */
        /* fflush(stdout); */
        DL_FOREACH_SAFE(head->metadata,elt,tmp) {
          /* DL_DELETE(head,elt); */
          free(elt);
        }
    }
    /* else { */
        /* printf("freeing head->metadata\n"); */
        /* fflush(stdout); */
        /* if (head->metadata != NULL) { */
        /*     free(head->metadata); */
        /* } */
    /* } */
}

/*
 * Free container hash value
 *
 * \param  value [IN]        Hash table value
 *
 * \return void
 */
static void
PDC_Server_container_hash_value_free(void * value)
{
    pdc_cont_hash_table_entry_t *head = (pdc_cont_hash_table_entry_t*) value;
    free(head->obj_ids);
}

/*
 * Init the PDC metadata structure
 *
 * \param  a [IN]        PDC metadata structure 
 *
 * \return void
 */
void PDC_Server_metadata_init(pdc_metadata_t* a)
{
    int i;
    
    FUNC_ENTER(NULL);
    
    a->user_id              = 0;
    a->time_step            = 0;
    a->app_name[0]          = 0;
    a->obj_name[0]          = 0;

    a->obj_id               = 0;
    a->ndim                 = 0;
    for (i = 0; i < DIM_MAX; i++) 
        a->dims[i] = 0;

    a->create_time          = 0;
    a->last_modified_time   = 0;
    a->tags[0]              = 0;
    a->data_location[0]     = 0;

    a->region_lock_head     = NULL;
    a->region_map_head      = NULL;
    a->region_buf_map_head  = NULL;
    a->region_obj_map_head  = NULL;
    a->prev                 = NULL;
    a->next                 = NULL;
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
static inline void combine_obj_info_to_string(pdc_metadata_t *metadata, char *output)
{
    /* sprintf(output, "%d%s%s%d", metadata->user_id, metadata->app_name, metadata->obj_name, metadata->time_step); */
    FUNC_ENTER(NULL);
    snprintf(output, TAG_LEN_MAX, "%s%d", metadata->obj_name, metadata->time_step);
}

/*
 * Get the metadata with obj ID and hash key
 *
 * \param  obj_id[IN]        Object ID
 * \param  hash_key[IN]      Hash value of the ID attributes
 *
 * \return NULL if no match is found/pointer to the metadata structure otherwise
 */
/* static pdc_metadata_t * find_metadata_by_id_and_hash_key(uint64_t obj_id, uint32_t hash_key) */ 
/* { */
/*     pdc_metadata_t *ret_value = NULL; */
/*     pdc_metadata_t *elt; */
/*     pdc_hash_table_entry_head *lookup_value = NULL; */
    
/*     FUNC_ENTER(NULL); */

/*     if (metadata_hash_table_g != NULL) { */
/*         lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key); */

/*         if (lookup_value == NULL) { */
/*             ret_value = NULL; */
/*             goto done; */
/*         } */

/*         DL_FOREACH(lookup_value->metadata, elt) { */
/*             if (elt->obj_id == obj_id) { */
/*                 ret_value = elt; */
/*                 goto done; */
/*             } */
/*         } */

/*     }  // if (metadata_hash_table_g != NULL) */
/*     else { */
/*         printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n"); */
/*         ret_value = NULL; */
/*         goto done; */
/*     } */
/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

/*
 * Get the metadata with obj ID from the metadata list
 *
 * \param  mlist[IN]         Metadata list head 
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t * find_metadata_by_id_from_list(pdc_metadata_t *mlist, uint64_t obj_id) 
{
    pdc_metadata_t *ret_value, *elt;
    
    FUNC_ENTER(NULL);

    ret_value = NULL;
    if (mlist == NULL) {
        ret_value = NULL;
        goto done;
    }

    DL_FOREACH(mlist, elt) {
        if (elt->obj_id == obj_id) {
            ret_value = elt;
            goto done;
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t get_remote_metadata_rpc_cb(const struct hg_cb_info *callback_info) */
/* { */
/*     hg_return_t ret_value = HG_SUCCESS; */
/*     hg_handle_t handle; */
/*     get_remote_metadata_out_t out; */
/*     struct get_remote_metadata_arg *get_meta_args; */

/*     FUNC_ENTER(NULL); */

/*     get_meta_args = (struct get_remote_metadata_arg*) callback_info->arg; */
/*     handle = callback_info->info.forward.handle; */

/*     /1* Get output from client *1/ */
/*     ret_value = HG_Get_output(handle, &out); */
/*     if (ret_value != HG_SUCCESS) { */
/*         printf("==PDC_SERVER[%d]: get_remote_metadata_rpc_cb - error with HG_Get_output\n", */
/*                 pdc_server_rank_g); */
/*         get_meta_args->data = NULL; */
/*         goto done; */
/*     } */

/*     if (out.ret.user_id == -1 && out.ret.obj_id == 0 && out.ret.time_step == -1) { */
/*         get_meta_args->data = NULL; */
/*     } */
/*     else { */
/*         get_meta_args->data = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t)); */
/*         if (get_meta_args->data == NULL) { */
/*             printf("==PDC_SERVER: ERROR - get_remote_metadata_rpc_cb() cannnot allocate space for client_lookup_args->data \n"); */
/*         } */

/*         // Now copy the received metadata info */
/*         ret_value = PDC_metadata_init(get_meta_args->data); */
/*         ret_value = pdc_transfer_t_to_metadata_t(&out.ret, get_meta_args->data); */
/*     } */

/* done: */
/*     HG_Free_output(handle, &out); */
/*     FUNC_LEAVE(ret_value); */
/* } */

/*
static pdc_metadata_t * find_remote_metadata_by_id(uint32_t meta_server_id, uint64_t obj_id)
{
    pdc_metadata_t *ret_value = NULL;
    hg_return_t  hg_ret = HG_SUCCESS;
    hg_handle_t get_remote_metadata_handle;
    get_remote_metadata_in_t in;
    struct get_remote_metadata_arg get_meta_args;

    FUNC_ENTER(NULL);

    in.obj_id = obj_id;

    HG_Create(hg_context_g, pdc_remote_server_info_g[meta_server_id].addr, get_remote_metadata_register_id_g, &get_remote_metadata_handle);

    hg_ret = HG_Forward(get_remote_metadata_handle, get_remote_metadata_rpc_cb, &get_meta_args, &in);
    if (hg_ret != HG_SUCCESS) {
        HG_Destroy(get_remote_metadata_handle);
        PGOTO_ERROR(FAIL, "find_remote_metadata_by_id(): Could not start HG_Forward()");
    }

printf("find_remote_metadata_by_id() calls PDC_Server_check_response()\n");
fflush(stdout);
    work_todo_g = 1;
    PDC_Server_check_response(&hg_context_g, &work_todo_g);
    if(get_meta_args.data == NULL) {
        PGOTO_ERROR(FAIL, "find_remote_metadata_by_id() got NULL metadata");
    }
    ret_value = get_meta_args.data;

done:
    HG_Destroy(get_remote_metadata_handle);
    FUNC_LEAVE(ret_value);
}
*/

/*
 * Get the metadata with the specified object ID by iteration of all metadata in the hash table
 *
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
pdc_metadata_t* find_metadata_by_id(uint64_t obj_id) 
{
    pdc_metadata_t *ret_value;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    HashTablePair pair;
    int n_entry;
    
    FUNC_ENTER(NULL);

    if (metadata_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {

            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt) {
                if (elt->obj_id == obj_id) {
                    return elt;
                }
            }
        }
    }  // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = NULL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Wrapper function of find_metadata_by_id().
 *
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id)
{
    pdc_metadata_t *ret_value = NULL;
    
    FUNC_ENTER(NULL);

    ret_value = find_metadata_by_id(obj_id);
    if (ret_value == NULL) {
        printf("==PDC_SERVER[%d]: PDC_Server_get_obj_metadata() - cannot find meta with id %" PRIu64 "\n",
                pdc_server_rank_g, obj_id);
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Find if there is identical metadata exist in hash table
 *
 * \param  entry[IN]        Hash table entry of metadata
 * \param  a[IN]            Pointer to metadata to be checked against
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t * find_identical_metadata(pdc_hash_table_entry_head *entry, pdc_metadata_t *a)
{
    pdc_metadata_t *ret_value = NULL;
    BLOOM_TYPE_T *bloom;
    int bloom_check;
    
    FUNC_ENTER(NULL);

/*     printf("==PDC_SERVER: querying with:\n"); */
/*     PDC_print_metadata(a); */
/*     fflush(stdout); */

    // Use bloom filter to quick check if current metadata is in the list
    if (entry->bloom != NULL && a->user_id != 0 && a->app_name[0] != 0) {
        /* printf("==PDC_SERVER: using bloom filter\n"); */
        /* fflush(stdout); */

        bloom = entry->bloom;

        char combined_string[ADDR_MAX];
        combine_obj_info_to_string(a, combined_string);
        /* printf("bloom_check: Combined string: %s\n", combined_string); */
        /* fflush(stdout); */

        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start;
        struct timeval  pdc_timer_end;
        double ht_total_sec;
 
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
            /* printf("==PDC_SERVER[%d]: Bloom filter says definitely not %s!\n", pdc_server_rank_g, combined_string); */
            /* fflush(stdout); */
            ret_value = NULL;
            goto done;
        }
        else {
            // bloom filter says maybe, so need to check entire list
            /* printf("==PDC_SERVER[%d]: Bloom filter says maybe for %s!\n", pdc_server_rank_g, combined_string); */
            /* fflush(stdout); */
            n_bloom_maybe_g++;
            pdc_metadata_t *elt;
            DL_FOREACH(entry->metadata, elt) {
                if (PDC_metadata_cmp(elt, a) == 0) {
                    /* printf("Identical metadata exist in Metadata store!\n"); */
                    /* PDC_print_metadata(a); */
                    ret_value = elt;
                    goto done;
                }
            }
        }

    }
    else {
        // Bloom has not been created
        /* printf("Bloom filter not created yet!\n"); */
        /* fflush(stdout); */
        pdc_metadata_t *elt;
        DL_FOREACH(entry->metadata, elt) {
            if (PDC_metadata_cmp(elt, a) == 0) {
                /* printf("Identical metadata exist in Metadata store!\n"); */
                /* PDC_print_metadata(a); */
                ret_value = elt;
                goto done;
            }
        }
    } // if bloom==NULL
    
done:
    /* int count; */
    /* DL_COUNT(lookup_value, elt, count); */
    /* printf("%d item(s) in list\n", count); */

    FUNC_LEAVE(ret_value);
} 



/*
 * Allocate a new object ID
 *
 * \return 64-bit integer of object ID
 */
static uint64_t PDC_Server_gen_obj_id()
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

/*
 * Init the hash table for metadata storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_init_hash_table()
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    // Metadata hash table
    metadata_hash_table_g = hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (metadata_hash_table_g == NULL) {
        printf("==PDC_SERVER: metadata_hash_table_g init error! Exit...\n");
        goto done;
    }
    hash_table_register_free_functions(metadata_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_metadata_hash_value_free);

    // Container hash table
    container_hash_table_g = hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (container_hash_table_g == NULL) {
        printf("==PDC_SERVER: container_hash_table_g init error! Exit...\n");
        goto done;
    }
    hash_table_register_free_functions(container_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_container_hash_value_free);

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
static perr_t PDC_Server_remove_from_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    if (bloom == NULL) {
        printf("==PDC_SERVER: PDC_Server_remove_from_bloom(): bloom pointer is NULL\n");
        ret_value = FAIL;
        goto done;
    }
 
    char combined_string[ADDR_MAX];
    combine_obj_info_to_string(metadata, combined_string);
    /* printf("==PDC_SERVER: PDC_Server_remove_from_bloom(): Combined string: %s\n", combined_string); */

    ret_value = BLOOM_REMOVE(bloom, combined_string, strlen(combined_string));
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_remove_from_bloom() - error\n",
                pdc_server_rank_g);
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
static perr_t PDC_Server_add_to_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;
    char combined_string[ADDR_MAX];
    
    FUNC_ENTER(NULL);

    if (bloom == NULL) {
        /* printf("==PDC_SERVER: PDC_Server_add_to_bloom(): bloom pointer is NULL\n"); */
        /* ret_value = FAIL; */
        goto done;
    }

    combine_obj_info_to_string(metadata, combined_string);
    /* printf("==PDC_SERVER[%d]: PDC_Server_add_to_bloom(): Combined string: %s\n", pdc_server_rank_g, combined_string); */
    /* fflush(stdout); */

    // Only add to bloom filter if it's definately not 
    /* int bloom_check; */
    /* bloom_check = BLOOM_CHECK(bloom, combined_string, strlen(combined_string)); */
    /* if (bloom_check == 0) { */
        ret_value = BLOOM_ADD(bloom, combined_string, strlen(combined_string));
    /* } */
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_add_to_bloom() - error \n",
                pdc_server_rank_g);
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
static perr_t PDC_Server_bloom_init(pdc_hash_table_entry_head *entry)
{
    perr_t      ret_value = 0;
    int capacity = 500000;
    double error_rate = 0.05;
    
    FUNC_ENTER(NULL);

    // Init bloom filter
    /* int capacity = 100000; */
    n_bloom_maybe_g = 0;
    n_bloom_total_g = 0;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;
    
    gettimeofday(&pdc_timer_start, 0);
#endif

    entry->bloom = (BLOOM_TYPE_T*)BLOOM_NEW(capacity, error_rate);
    if (!entry->bloom) {
        fprintf(stderr, "ERROR: Could not create bloom filter\n");
        ret_value = -1;
        goto done;
    }

    /* PDC_Server_add_to_bloom(entry, bloom); */

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    server_bloom_init_time_g += ht_total_sec;
#endif

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Insert a metadata to the metadata hash table
 *
 * \param  head[IN]      Head of the hash table
 * \param  new[IN]       Metadata pointer to be inserted
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_hash_table_list_insert(pdc_hash_table_entry_head *head, pdc_metadata_t *new)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *elt;
    
    FUNC_ENTER(NULL);

    // add to bloom filter
    if (head->n_obj == CREATE_BLOOM_THRESHOLD) {
        /* printf("==PDC_SERVER:Init bloom\n"); */
        /* fflush(stdout); */
        PDC_Server_bloom_init(head);
        DL_FOREACH(head->metadata, elt) {
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
        /* printf("==PDC_SERVER: Adding to bloom filter, %d existed\n", head->n_obj); */
        /* fflush(stdout); */
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
    /* printf("Adding to linked list\n"); */
    // Currently $metadata is unique, insert to linked list
    DL_APPEND(head->metadata, new);
    head->n_obj++;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&insert_hash_table_mutex_g);
#endif
    /* // Debug print */
    /* int count; */
    /* pdc_metadata_t *elt; */
    /* DL_COUNT(head, elt, count); */
    /* printf("Append one metadata, total=%d\n", count); */
done:
 
    FUNC_LEAVE(ret_value);
}

/*
 * Init a metadata list (doubly linked) under the given hash table entry
 *
 * \param  entry[IN]        An entry pointer of the hash table
 * \param  hash_key[IN]     Hash key of the entry
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_hash_table_list_init(pdc_hash_table_entry_head *entry, uint32_t *hash_key)
{
    
    perr_t      ret_value = SUCCEED;
    hg_return_t ret;
    
    FUNC_ENTER(NULL);

    /* printf("==PDC_SERVER[%d]: hash entry init for hash key [%u]\n", pdc_server_rank_g, *hash_key); */

/*     // Init head of linked list */
/*     metadata->prev = metadata;                                                                   \ */
/*     metadata->next = NULL; */   

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    // Insert to hash table
    ret = hash_table_insert(metadata_hash_table_g, hash_key, entry);
    if (ret != 1) {
        fprintf(stderr, "PDC_Server_hash_table_list_init(): Error with hash table insert!\n");
        ret_value = FAIL;
        goto done;
    }

    /* PDC_print_metadata(entry->metadata); */
    /* printf("entry n_obj=%d\n", entry->n_obj); */

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    server_hash_insert_time_g += ht_total_sec;
#endif

    /* PDC_Server_bloom_init(new); */

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Add the tag received from one client to the corresponding metadata structure
 *
 * \param  in[IN]       Input structure received from client
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_add_tag_metadata(metadata_add_tag_in_t *in, metadata_add_tag_out_t *out)
{

    FUNC_ENTER(NULL);

    perr_t ret_value;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER: Got add_tag request: hash=%u, obj_id=%" PRIu64 "\n", in->hash_value, in->obj_id); */

    uint32_t *hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    uint64_t obj_id = in->obj_id;

    pdc_hash_table_entry_head *lookup_value;

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("==PDC_SERVER: checking hash table with key=%d\n", *hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            /* printf("==PDC_SERVER: lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            pdc_metadata_t *target;
            target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
            if (target != NULL) {
                /* printf("==PDC_SERVER: Found add_tag target!\n"); */
                /* printf("Received add_tag info:\n"); */
                /* PDC_print_metadata(&in->new_metadata); */

                // Check and find valid add_tag fields
                // Currently user_id, obj_name are not supported to be updated in this way
                // obj_name change is done through client with delete and add operation.
                if (in->new_tag != NULL && in->new_tag[0] != 0 &&
                        !(in->new_tag[0] == ' ' && in->new_tag[1] == 0)) {
                    // add a ',' to separate different tags
                    /* printf("Previous tags: %s\n", target->tags); */
                    /* printf("Adding tags: %s\n", in->new_metadata.tags); */
                    target->tags[strlen(target->tags)+1] = 0;
                    target->tags[strlen(target->tags)] = ',';
                    strcat(target->tags, in->new_tag);
                    /* printf("Final tags: %s\n", target->tags); */
                }

                out->ret  = 1;

            } // if (lookup_value != NULL) 
            else {
                // Object not found for deletion request
                /* printf("==PDC_SERVER: add tag target not found!\n"); */
                ret_value = FAIL;
                out->ret  = -1;
            }
       
        } // if lookup_value != NULL
        else {
            /* printf("==PDC_SERVER: add tag target not found!\n"); */
            ret_value = FAIL;
            out->ret = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
        ret_value = FAIL;
        out->ret = -1;
        goto done;
    }

    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_add_tag_metadata() - error \n",
                pdc_server_rank_g);
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
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of add_tag_metadata_from_hash_table

/*
 * Update the metadata received from one client to the corresponding metadata structure
 *
 * \param  in[IN]       Input structure received from client
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out)
{
    perr_t ret_value;
    uint64_t obj_id;
    pdc_hash_table_entry_head *lookup_value;
    /* pdc_metadata_t *elt; */
    uint32_t *hash_key;
    pdc_metadata_t *target;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;
    
    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER: Got update request: hash=%u, obj_id=%" PRIu64 "\n", in->hash_value, in->obj_id); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    obj_id = in->obj_id;

#ifdef ENABLE_MULTITHREAD 
    int unlocked = 0;
    // Obtain lock for hash table
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("==PDC_SERVER: checking hash table with key=%d\n", *hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            /* printf("==PDC_SERVER: lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
            if (target != NULL) {
                /* printf("==PDC_SERVER: Found update target!\n"); */
                /* printf("Received update info:\n"); */
                /* PDC_print_metadata(&in->new_metadata); */
                /* printf("==PDC_SERVER: new dataloc: [%s]\n", in->new_metadata.data_location); */

                // Check and find valid update fields
                // Currently user_id, obj_name are not supported to be updated in this way
                // obj_name change is done through client with delete and add operation.
                if (in->new_metadata.time_step != -1) 
                    target->time_step = in->new_metadata.time_step;
                if (in->new_metadata.app_name[0] != 0 && 
                        !(in->new_metadata.app_name[0] == ' ' && in->new_metadata.app_name[1] == 0)) 
                    strcpy(target->app_name,      in->new_metadata.app_name);
                if (in->new_metadata.data_location[0] != 0 && 
                        !(in->new_metadata.data_location[0] == ' ' && in->new_metadata.data_location[1] == 0)) 
                    strcpy(target->data_location, in->new_metadata.data_location);
                if (in->new_metadata.tags[0] != 0 &&
                        !(in->new_metadata.tags[0] == ' ' && in->new_metadata.tags[1] == 0)) {
                    // add a ',' to separate different tags
                    /* printf("Previous tags: %s\n", target->tags); */
                    /* printf("Adding tags: %s\n", in->new_metadata.tags); */
                    target->tags[strlen(target->tags)+1] = 0;
                    target->tags[strlen(target->tags)] = ',';
                    strcat(target->tags, in->new_metadata.tags);
                    /* printf("Final tags: %s\n", target->tags); */
                }

                out->ret  = 1;
            } // if (lookup_value != NULL) 
            else {
                // Object not found for deletion request
                /* printf("==PDC_SERVER: update target not found!\n"); */
                ret_value = -1;
                out->ret  = -1;
            }
       
        } // if lookup_value != NULL
        else {
            /* printf("==PDC_SERVER: update target not found!\n"); */
            ret_value = -1;
            out->ret = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        out->ret = -1;
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
} // end of update_metadata_from_hash_table

/*
 * Delete metdata with the ID received from a client
 *
 * \param  in[IN]       Input structure received from client, conatins object ID
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out)
{
    perr_t ret_value = FAIL;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    HashTablePair pair;
    uint64_t target_obj_id;
    int n_entry;

    FUNC_ENTER(NULL);

    out->ret = -1;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif


    /* printf("==PDC_SERVER[%d]: Got delete by id request: obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->obj_id); */
    /* fflush(stdout); */

    target_obj_id = in->obj_id;

    /* printf("==PDC_SERVER: delete request name:%s ts=%d hash=%u\n", in->obj_name, in->time_step, in->hash_value); */

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {

        // Since we only have the obj id, need to iterate the entire hash table
        pdc_hash_table_entry_head *head; 

        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {

            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt) {

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
                        /* printf("==PDC_SERVER: delete from DL!\n"); */
                    }
                    else {
                        // This is the last item under the current entry, remove the hash entry 
                        /* printf("==PDC_SERVER: delete from hash table!\n"); */
                        uint32_t hash_key = PDC_get_hash_by_name(elt->obj_name);
                        hash_table_remove(metadata_hash_table_g, &hash_key);

                        // Free this item and delete hash table entry
                        /* if(is_restart_g != 1) { */
                        /*     free(elt); */
                        /* } */
                    }
                    out->ret  = 1;
                    ret_value = SUCCEED;
                }
            } // DL_FOREACH
        }  // while 
    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        out->ret = -1;
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
        n_metadata_g-- ;
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif

done:
    /* printf("==PDC_SERVER[%d]: Finished delete by id request: obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->obj_id); */
    /* fflush(stdout); */
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_delete_metadata_by_id


/*
 * Delete metdata from hash table with the ID received from a client
 *
 * \param  in[IN]       Input structure received from client, conatins object ID
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out)
{
    perr_t ret_value;
    uint32_t *hash_key;
    pdc_metadata_t *target;
    
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER[%d]: Got delete request: hash=%d, obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->hash_value, in->obj_id); */
    /* fflush(stdout); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    /* uint64_t obj_id = in->obj_id; */

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t metadata;
    strcpy(metadata.obj_name, in->obj_name);
    metadata.time_step = in->time_step;
    metadata.app_name[0] = 0;
    metadata.user_id = -1;
    metadata.obj_id = 0;
        
    /* printf("==PDC_SERVER: delete request name:%s ts=%d hash=%u\n", in->obj_name, in->time_step, in->hash_value); */

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("==PDC_SERVER: checking hash table with key=%d\n", *hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            /* printf("==PDC_SERVER: lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            target = find_identical_metadata(lookup_value, &metadata);
            if (target != NULL) {
                /* printf("==PDC_SERVER: Found delete target!\n"); */
                
                // Check if target is the only item in this linked list
                /* int curr_list_size; */
                /* DL_COUNT(lookup_value, elt, curr_list_size); */

                /* printf("==PDC_SERVER: still %d objects in current list\n", curr_list_size); */

                /* if (curr_list_size > 1) { */
                if (lookup_value->n_obj > 1) {
                    // Remove from bloom filter
                    if (lookup_value->bloom != NULL) {
                        PDC_Server_remove_from_bloom(target, lookup_value->bloom);
                    }

                    // Remove from linked list
                    DL_DELETE(lookup_value->metadata, target);
                    lookup_value->n_obj--;
                    /* printf("==PDC_SERVER: delete from DL!\n"); */
                }
                else {
                    // Remove from hash
                    /* printf("==PDC_SERVER: delete from hash table!\n"); */
                    hash_table_remove(metadata_hash_table_g, hash_key);

                    // Free this item and delete hash table entry
                    /* if(is_restart_g != 1) { */
                    /*     free(target); */
                    /* } */
                }
                out->ret  = 1;

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
            out->ret = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        out->ret = -1;
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
        n_metadata_g-- ;
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif

done:
    /* printf("==PDC_SERVER[%d]: Finished delete request: hash=%u, obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->hash_value, in->obj_id); */
    /* fflush(stdout); */
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
        
    FUNC_LEAVE(ret_value);
} // end of delete_metadata_from_hash_table

/*
 * Insert the metdata received from client to the hash table
 *
 * \param  in[IN]       Input structure received from client, conatins metadata 
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *metadata;
    uint32_t *hash_key, i;
    

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER[%d]: Got object creation request with name: %s\tHash=%u\n", */
    /*         pdc_server_rank_g, in->data.obj_name, in->hash_value); */
    /* printf("Full name check: %s\n", &in->obj_name[507]); */

    /* printf("%s\t%u\n", in->data.obj_name, in->hash_value); */
    metadata = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    if (metadata == NULL) {
        printf("Cannot allocate pdc_metadata_t!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(pdc_metadata_t);

    PDC_metadata_init(metadata);

    metadata->cont_id   = in->data.cont_id;
    metadata->user_id   = in->data.user_id;
    metadata->time_step = in->data.time_step;
    metadata->ndim      = in->data.ndim;
    metadata->dims[0]   = in->data.dims0;
    metadata->dims[1]   = in->data.dims1;
    metadata->dims[2]   = in->data.dims2;
    metadata->dims[3]   = in->data.dims3;
    for (i = metadata->ndim; i < DIM_MAX; i++) 
        metadata->dims[i] = 0;
    
       
    strcpy(metadata->obj_name,      in->data.obj_name);
    strcpy(metadata->app_name,      in->data.app_name);
    strcpy(metadata->tags,          in->data.tags);
    strcpy(metadata->data_location, in->data.data_location);
       
    // DEBUG
    int debug_flag = 0;
    /* PDC_print_metadata(metadata); */

    /* create_time              =; */
    /* last_modified_time       =; */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t *found_identical;

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
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
            /* found_identical = NULL; */
            found_identical = find_identical_metadata(lookup_value, metadata);
            if ( found_identical != NULL) {
                printf("==PDC_SERVER[%d]: Found identical metadata with name %s!\n", 
                        pdc_server_rank_g, metadata->obj_name);

                if (debug_flag == 1) {
                    /* PDC_print_metadata(metadata); */
                    /* PDC_print_metadata(found_identical); */
                }
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

            pdc_hash_table_entry_head *entry = (pdc_hash_table_entry_head*)malloc(sizeof(pdc_hash_table_entry_head));
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
        n_metadata_g++ ;
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif


    // Fill $out structure for returning the generated obj_id to client
    out->obj_id = metadata->obj_id;

    // Debug print metadata info
    /* PDC_print_metadata(metadata); */

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
    /* printf("==PDC_SERVER[%d]: inserted name %s hash key %u to hash table\n", pdc_server_rank_g, in->data.obj_name, *hash_key); */
    /* fflush(stdout); */
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
    FUNC_LEAVE(ret_value);
} // end of insert_metadata_to_hash_table

/*
 * Print all existing metadata in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_print_all_metadata()
{
    perr_t ret_value = SUCCEED;
    HashTableIterator hash_table_iter;
    pdc_metadata_t *elt;
    pdc_hash_table_entry_head *head;
    HashTablePair pair;
    
    FUNC_ENTER(NULL);

    hash_table_iterate(metadata_hash_table_g, &hash_table_iter);
    while (hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
        head = pair.value;
        DL_FOREACH(head->metadata, elt) {
            PDC_print_metadata(elt);
        }
    }
    FUNC_LEAVE(ret_value);
}

/*
 * Print all existing container in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_print_all_containers()
{
    int i;
    perr_t ret_value = SUCCEED;
    HashTableIterator hash_table_iter;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    HashTablePair pair;
    
    FUNC_ENTER(NULL);

    hash_table_iterate(container_hash_table_g, &hash_table_iter);
    while (hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
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


/*
 * Check for duplicates in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_metadata_duplicate_check()
{
    perr_t ret_value = SUCCEED;
    HashTableIterator hash_table_iter;
    HashTablePair pair;
    int n_entry, count = 0;
    int all_maybe, all_total, all_entry;
    int has_dup_obj = 0;
    int all_dup_obj = 0;
    pdc_metadata_t *elt, *elt_next;
    pdc_hash_table_entry_head *head;
    
    FUNC_ENTER(NULL);

    n_entry = hash_table_num_entries(metadata_hash_table_g);

    #ifdef ENABLE_MPI
        MPI_Reduce(&n_bloom_maybe_g, &all_maybe, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&n_bloom_total_g, &all_total, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&n_entry,         &all_entry, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    #else
        all_maybe = n_bloom_maybe_g;
        all_total = n_bloom_total_g;
        all_entry = n_entry;
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
        /* DL_COUNT(head, elt, dl_count); */
        /* if (pdc_server_rank_g == 0) { */
        /*     printf("  Hash entry[%d], with %d items\n", count, dl_count); */
        /* } */
        DL_SORT(head->metadata, PDC_metadata_cmp); 
        // With sorted list, just compare each one with its next
        DL_FOREACH(head->metadata, elt) {
            elt_next = elt->next;
            if (elt_next != NULL) {
                if (PDC_metadata_cmp(elt, elt_next) == 0) {
                    /* PDC_print_metadata(elt); */
                    has_dup_obj = 1;
                    ret_value = FAIL;
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
static int is_metadata_satisfy_constraint(pdc_metadata_t *metadata, metadata_query_transfer_in_t *constraints)
{
    int ret_value = 1;
    
    FUNC_ENTER(NULL);

    /* int     user_id; */
    /* char    *app_name; */
    /* char    *obj_name; */
    /* int     time_step_from; */
    /* int     time_step_to; */
    /* int     ndim; */
    /* char    *tags; */
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
        (metadata->time_step < constraints->time_step_from || metadata->time_step > constraints->time_step_to)
       ) {
        ret_value = -1;
        goto done;
    }
    if (constraints->ndim > 0 && metadata->ndim != constraints->ndim ) {
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

/*
 * Get the metadata that satisfies the query constraint
 *
 * \param  in[IN]           Input structure from client that contains the query constraint
 * \param  n_meta[OUT]      Number of metadata that satisfies the query constraint
 * \param  buf_ptrs[OUT]    Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs)
{
    perr_t ret_value = FAIL;
    uint32_t i;
    uint32_t n_buf, iter = 0;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    int n_entry;
    HashTablePair pair;

    
    FUNC_ENTER(NULL);

    // n_buf = n_metadata_g + 1 for potential padding array
    n_buf = n_metadata_g + 1;
    *buf_ptrs = (void**)calloc(n_buf, sizeof(void*));
    for (i = 0; i < n_buf; i++) {
        (*buf_ptrs)[i] = (void*)calloc(1, sizeof(void*));
    }
    // TODO: free buf_ptrs
    if (metadata_hash_table_g != NULL) {

        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);


        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            DL_FOREACH(head->metadata, elt) {
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

        /* printf("PDC_Server_get_partial_query_result: Total matching results: %d\n", *n_meta); */

    }  // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        goto done;
    }

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Seach the hash table with object name and hash key
 *
 * \param  obj_name[IN]     Name of the object to be searched
 * \param  hash_key[IN]     Hash value of the name string
 * \param  ts[IN]           Timestep value
 * \param  out[OUT]         Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts, 
                                            pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;
    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t metadata;
    const char *name;

    FUNC_ENTER(NULL);

    *out = NULL;

    // Set up a metadata struct to query
    PDC_Server_metadata_init(&metadata);

    name = obj_name;

    strcpy(metadata.obj_name, name);
    metadata.time_step = ts;

    /* printf("==PDC_SERVER[%d]: search with name [%s], hash key %u\n", pdc_server_rank_g, name, hash_key); */

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            /* printf("==PDC_SERVER: %s - lookup_value not NULL!\n", __func__); */
            // Check if there exist metadata identical to current one
            /* PDC_print_metadata(lookup_value->metadata); */
            /* if (lookup_value->bloom == NULL) { */
            /*     printf("bloom is NULL\n"); */
            /* } */
            *out = find_identical_metadata(lookup_value, &metadata);

            if (*out == NULL) {
                 /* printf("==PDC_SERVER[%d]: Queried object with name [%s] has no full match!\n", */
                 /* pdc_server_rank_g, obj_name); */
                /* fflush(stdout); */
                ret_value = FAIL;
                goto done;
            }
            /* else { */
                /* printf("==PDC_SERVER[%d]: name %s found in hash table \n", pdc_server_rank_g, name); */
                /* fflush(stdout); */
                /* PDC_print_metadata(*out); */
            /* } */
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

    /* PDC_print_metadata(*out); */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_search_with_name_timestep

/*
 * Seach the hash table with object name and hash key
 *
 * \param  obj_name[IN]     Name of the object to be searched
 * \param  hash_key[IN]     Hash value of the name string
 * \param  out[OUT]         Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out)
{
    perr_t ret_value = SUCCEED;
    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t metadata;
    const char *name;
    
    FUNC_ENTER(NULL);
    
    *out = NULL;

    // Set up a metadata struct to query
    PDC_Server_metadata_init(&metadata);
    
    name = obj_name;

    strcpy(metadata.obj_name, name);
    /* metadata.time_step = tmp_time_step; */
    // TODO: currently PDC_Client_query_metadata_name_timestep is not taking timestep for querying 
    metadata.time_step = 0;

    /* printf("==PDC_SERVER[%d]: search with name [%s], hash key %u\n", pdc_server_rank_g, name, hash_key); */

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            /* printf("==PDC_SERVER: PDC_Server_search_with_name_hash(): lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            /* PDC_print_metadata(lookup_value->metadata); */
            /* if (lookup_value->bloom == NULL) { */
            /*     printf("bloom is NULL\n"); */
            /* } */
            *out = find_identical_metadata(lookup_value, &metadata);

            if (*out == NULL) {
                 /* printf("==PDC_SERVER[%d]: Queried object with name [%s] has no full match!\n", */ 
                 /* pdc_server_rank_g, obj_name); */ 
                /* fflush(stdout); */
                ret_value = FAIL;
                goto done;
            }
            /* else { */
                /* printf("==PDC_SERVER[%d]: name %s found in hash table \n", pdc_server_rank_g, name); */
                /* fflush(stdout); */
                /* PDC_print_metadata(*out); */
            /* } */
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
    
    /* PDC_print_metadata(*out); */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Get metadata of the object ID received from client from local metadata hash table
 *
 * \param  obj_id[IN]           Object ID
 * \param  res_meta_ptrdata[IN]     Pointer of metadata of the specified object ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta_ptr)
{
    perr_t ret_value = SUCCEED;

    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    int n_entry;
    HashTablePair pair;

    
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
            DL_FOREACH(head->metadata, elt) {
                if (elt->obj_id == obj_id) {
                    *res_meta_ptr = elt;
                    goto done;
                }
            }
        }
    }  
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        *res_meta_ptr = NULL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_get_local_metadata_by_id

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
    hg_return_t                  ret_value;
    hg_handle_t                  handle;
    pdc_metadata_t              *meta = NULL;
    get_metadata_by_id_args_t   *cb_args;
    get_metadata_by_id_out_t     output;

    FUNC_ENTER(NULL);

    cb_args = (get_metadata_by_id_args_t*)callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_get_metadata_by_id_cb - error HG_Get_output\n", pdc_server_rank_g);
        goto done;
    }

    if (output.res_meta.obj_id != 0) {
        // TODO free metdata
        meta = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
        pdc_transfer_t_to_metadata_t(&output.res_meta, meta);
    }
    else {
        printf("==PDC_SERVER[%d]: %s - no valid metadata is retrieved\n", pdc_server_rank_g, __func__);
        goto done;
    }

    // Execute the callback function 
    if (NULL != cb_args->cb) {
        ((region_list_t*)(cb_args->args))->meta = meta;
        cb_args->cb(cb_args->args, POSIX);
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
} // PDC_Server_get_metadata_by_id_cb

/*
 * Get metadata of the object ID received from client from (possibly remtoe) metadata hash table
 *
 * \param  obj_id[IN]           Object ID
 * \param  res_meta_ptr[IN/OUT]     Pointer of metadata of the specified object ID
 * \param  cb[IN]               Callback function needs to be executed after getting the metadata
 * \param  args[IN]             Callback function input parameters
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_metadata_by_id_with_cb(uint64_t obj_id, perr_t (*cb)(), void *args)
{
    pdc_metadata_t             *res_meta_ptr = NULL;
    hg_return_t                 hg_ret;
    perr_t                      ret_value = SUCCEED;
    uint32_t                    server_id = 0;
    hg_handle_t                 get_metadata_by_id_handle;
    get_metadata_by_id_args_t   *cb_args;
    get_metadata_by_id_in_t     in;

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        // Metadata object is local, no need to send update RPC
        ret_value = PDC_Server_get_local_metadata_by_id(obj_id, &res_meta_ptr);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_get_local_metadata_by_id FAILED!\n", pdc_server_rank_g);
            goto done;
        }

        ((region_list_t*)args)->meta = res_meta_ptr;
        // Call the callback function directly and pass in the result metadata ptr
        cb(args, POSIX);
    }
    else {
        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                    pdc_server_rank_g, server_id);
            ret_value = FAIL;
            goto done;
        }

        HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_metadata_by_id_register_id_g,
                &get_metadata_by_id_handle);

        /* printf("Sending updated region loc to target\n"); */

        cb_args = (get_metadata_by_id_args_t *)malloc(sizeof(get_metadata_by_id_args_t));
        in.obj_id    = obj_id;
        cb_args->cb   = cb;
        cb_args->args = args;

        // TODO: Include the callback function ptr and args in the cb_args
        hg_ret = HG_Forward(get_metadata_by_id_handle, PDC_Server_get_metadata_by_id_cb, &cb_args, &in);

        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - Could not forward\n", 
                    pdc_server_rank_g, __func__);
            res_meta_ptr = NULL;
            HG_Destroy(get_metadata_by_id_handle);
            return FAIL;
        }

        // Wait for response from server
        /* work_todo_g = 1; */
        /* PDC_Server_check_response(&hg_context_g, &work_todo_g); */

//        HG_Destroy(get_metadata_by_id_handle);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_get_metadata_by_id_with_cb


/*
 * Get metadata of the object ID received from client from (possibly remtoe) metadata hash table
 *
 * \param  obj_id[IN]           Object ID
 * \param  res_meta_ptrdata[IN]     Pointer of metadata of the specified object ID
 *
 * \return Non-negative on success/Negative on failure
 */
/* perr_t PDC_Server_get_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta_ptr) */
/* { */
/*     hg_return_t hg_ret; */
/*     perr_t ret_value = SUCCEED; */
/*     uint32_t server_id = 0; */
/*     hg_handle_t get_metadata_by_id_handle; */

/*     FUNC_ENTER(NULL); */

/*     server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_size_g); */
/*     if (server_id == (uint32_t)pdc_server_rank_g) { */
/*         // Metadata object is local, no need to send update RPC */
/*         ret_value = PDC_Server_get_local_metadata_by_id(obj_id, res_meta_ptr); */
/*         if (ret_value != SUCCEED) { */
/*             printf("==PDC_SERVER[%d]: PDC_Server_get_local_metadata_by_id FAILED!\n", pdc_server_rank_g); */
/*             goto done; */
/*         } */
/*     } */
/*     else { */

/*         if (pdc_remote_server_info_g[server_id].addr_valid != 1) { */
/*             if (PDC_Server_lookup_server_id(server_id) != SUCCEED) { */
/*                 printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", */
/*                         pdc_server_rank_g, server_id); */
/*                 ret_value = FAIL; */
/*                 goto done; */
/*             } */
/*         } */

/*         HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_metadata_by_id_register_id_g, */
/*                 &get_metadata_by_id_handle); */

/*         /1* printf("Sending updated region loc to target\n"); *1/ */
/*         server_lookup_args_t lookup_args; */

/*         get_metadata_by_id_in_t in; */
/*         in.obj_id = obj_id; */

/*         hg_ret = HG_Forward(get_metadata_by_id_handle, PDC_Server_get_metadata_by_id_cb, &lookup_args, &in); */

/*         if (hg_ret != HG_SUCCESS) { */
/*             fprintf(stderr, "==PDC_SERVER[%d]: %s - Could not forward\n", */ 
/*                     pdc_server_rank_g, __func__); */
/*             *res_meta_ptr = NULL; */
/*             HG_Destroy(get_metadata_by_id_handle); */
/*             return FAIL; */
/*         } */

/*         // Wait for response from server */
/*         /1* work_todo_g = 1; *1/ */
/*         /1* PDC_Server_check_response(&hg_context_g, &work_todo_g); *1/ */

/*         // TODO: move to callback */
/*         // Retrieved metadata is stored in lookup_args */
/*         *res_meta_ptr = lookup_args.meta; */

/*         HG_Destroy(get_metadata_by_id_handle); */
/*     } */

/* done: */
/*     fflush(stdout); */
/*     FUNC_LEAVE(ret_value); */
/* } // end of PDC_Server_get_metadata_by_id */

/*
 * Test serialize/un-serialized code
 *
 * \return void
 */
/* void test_serialize() */
/* { */
/*     region_list_t **head = NULL, *a, *b, *c, *d; */
/*     head = (region_list_t**)malloc(sizeof(region_list_t*) * 4); */
/*     a = (region_list_t*)malloc(sizeof(region_list_t)); */
/*     b = (region_list_t*)malloc(sizeof(region_list_t)); */
/*     c = (region_list_t*)malloc(sizeof(region_list_t)); */
/*     d = (region_list_t*)malloc(sizeof(region_list_t)); */

/*     head[0] = a; */
/*     head[1] = b; */
/*     head[2] = c; */
/*     head[3] = d; */

/*     PDC_init_region_list(a); */
/*     PDC_init_region_list(b); */
/*     PDC_init_region_list(c); */
/*     PDC_init_region_list(d); */

/*     a->ndim = 2; */
/*     a->start[0] = 0; */
/*     a->start[1] = 4; */
/*     a->count[0] = 10; */
/*     a->count[1] = 14; */
/*     a->offset   = 1234; */
/*     snprintf(a->storage_location, ADDR_MAX, "%s", "/path/to/a/a/a/a/a"); */

/*     b->ndim = 2; */
/*     b->start[0] = 10; */
/*     b->start[1] = 14; */
/*     b->count[0] = 100; */
/*     b->count[1] = 104; */
/*     b->offset   = 12345; */
/*     snprintf(b->storage_location, "%s", ADDR_MAX, "/path/to/b/b"); */


/*     c->ndim = 2; */
/*     c->start[0] = 20; */
/*     c->start[1] = 21; */
/*     c->count[0] = 23; */
/*     c->count[1] = 24; */
/*     c->offset   = 123456; */
/*     snprintf(c->storage_location, ADDR_MAX, "%s", "/path/to/c/c/c/c"); */


/*     d->ndim = 2; */
/*     d->start[0] = 110; */
/*     d->start[1] = 111; */
/*     d->count[0] = 70; */
/*     d->count[1] = 71; */
/*     d->offset   = 1234567; */
/*     snprintf(d->storage_location, ADDR_MAX, "%s", "/path/to/d"); */

/*     uint32_t total_str_len = 0; */
/*     uint32_t n_region = 4; */
/*     PDC_get_serialized_size(head, n_region, &total_str_len); */

/*     void *buf = (void*)malloc(total_str_len); */

/*     PDC_serialize_regions_lists(head, n_region, buf, total_str_len); */

/*     region_list_t **regions = (region_list_t**)malloc(sizeof(region_list_t*) * PDC_MAX_OVERLAP_REGION_NUM); */
/*     uint32_t i; */
/*     for (i = 0; i < n_region; i++) { */
/*         regions[i] = (region_list_t*)malloc(sizeof(region_list_t)); */
/*         PDC_init_region_list(regions[i]); */
/*     } */

/*     PDC_unserialize_region_lists(buf, regions, &n_region); */

/* } */

/*
 * Create a container
 *
 * \param  in[IN]       Input structure received from client, conatins metadata 
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_create_container(gen_cont_id_in_t *in, gen_cont_id_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *metadata;
    uint32_t *hash_key, i;

    FUNC_ENTER(NULL);

    out->cont_id = 0;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER[%d]: Got container creation request with name: %s\n", */
    /*         pdc_server_rank_g, in->cont_name); */
    /* fflush(stdout); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;

    pdc_cont_hash_table_entry_t *lookup_value;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_container_hash_table_mutex_g);
#endif

    if (container_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(container_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            
            // Check if there exist container identical to current one
            printf("==PDC_SERVER[%d]: Found existing container with same name [%s]!\n", 
                    pdc_server_rank_g, lookup_value->cont_name);
            out->cont_id = lookup_value->cont_id;
        
        }
        else {
            pdc_cont_hash_table_entry_t *entry = (pdc_cont_hash_table_entry_t*)calloc(1, sizeof(pdc_cont_hash_table_entry_t));
            strcpy(entry->cont_name, in->cont_name);
            entry->n_obj = 0;
            entry->n_allocated = 128;
            entry->obj_ids = (uint64_t*)calloc(entry->n_allocated, sizeof(uint64_t));
            entry->cont_id = PDC_Server_gen_obj_id();

            total_mem_usage_g += sizeof(pdc_cont_hash_table_entry_t);
            total_mem_usage_g += sizeof(uint64_t)*entry->n_allocated;

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
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g, __func__);
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
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
    FUNC_LEAVE(ret_value);
} // end PDC_Server_create_container



/*
 * Delete a container by name
 *
 * \param  in[IN]       Input structure received from client, conatins metadata 
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_delete_container_by_name(gen_cont_id_in_t *in, gen_cont_id_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *metadata;
    uint32_t hash_key, i;

    FUNC_ENTER(NULL);

    out->cont_id = 0;

    /* printf("==PDC_SERVER[%d]: Got container creation request with name: %s\n", */
    /*         pdc_server_rank_g, in->data.cont_name); */
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
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g, __func__);
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_container_hash_table_mutex_g);
#endif

done:
    FUNC_LEAVE(ret_value);
} // end PDC_Server_delete_container

/*
 * Search a container by name
 *
 * \param  cont_name[IN]       Container name
 * \param  hash_key[IN]       Hash value of container name
 * \param  out[OUT]           Pointer to the result container
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_find_container_by_name(const char *cont_name, pdc_cont_hash_table_entry_t **out)
{
    perr_t ret_value = SUCCEED;
    uint32_t hash_key;

    FUNC_ENTER(NULL);
    if (NULL == cont_name || NULL == out) {
        printf("==PDC_SERVER[%d]: %s - input is NULL! \n", pdc_server_rank_g, __func__);
        goto done;
    }

    if (container_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", hash_key); */
        hash_key = PDC_get_hash_by_name(cont_name);
        *out = hash_table_lookup(container_hash_table_g, &hash_key);
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
} // end of PDC_Server_find_container_by_name

/*
 * Search a container by obj id 
 *
 * \param  cont_id[IN]        Container  ID
 * \param  out[OUT]           Pointer to the result container
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_find_container_by_id(uint64_t cont_id, pdc_cont_hash_table_entry_t **out)
{
    perr_t ret_value = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    int n_entry;
    HashTablePair pair;

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
            pair = hash_table_iter_next(&hash_table_iter);
            cont_entry = pair.value;
            if (cont_entry->cont_id == cont_id) {
                *out = cont_entry;
                goto done;
            }
        }
    }  
    else {
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        out = NULL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_find_container_by_id

/*
 * Add objects to a container 
 *
 * \param  n_obj[IN]           Number of objects to be added/deleted
 * \param  obj_ids[IN]        Pointer to object array with nobj objects
 * \param  cont_id[IN]        Container ID
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_container_add_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id)
{
    perr_t ret_value = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    int realloc_size = 0;

    FUNC_ENTER(NULL);
    ret_value = PDC_Server_find_container_by_id(cont_id, &cont_entry);

    if (cont_entry != NULL) {
        // Check if need to allocate space
        if (cont_entry->n_allocated == 0) {
            cont_entry->n_allocated = PDC_ALLOC_BASE_NUM;
            cont_entry->obj_ids = (uint64_t*)calloc(sizeof(uint64_t), PDC_ALLOC_BASE_NUM);
        }
        else if (cont_entry->n_allocated < cont_entry->n_obj + n_obj) {
            // Extend the allocated space by twice its original size 
            // or twice the n_obj size, whichever greater
            realloc_size = cont_entry->n_allocated > n_obj? cont_entry->n_allocated: n_obj;
            realloc_size *= (sizeof(uint64_t)*2);
            cont_entry->obj_ids = (uint64_t*)realloc(cont_entry->obj_ids, realloc_size);
            cont_entry->n_allocated = realloc_size / sizeof(uint64_t);
        }

        // Append the new ids
        memcpy(cont_entry->obj_ids+cont_entry->n_obj, obj_ids, n_obj*sizeof(uint64_t));
        cont_entry->n_obj += n_obj;
            
        // Debug prints
        /* printf("==PDC_SERVER[%d]: add %d objects to container %" PRIu64 ", total %d !\n", */ 
                /* pdc_server_rank_g, n_obj, cont_id, cont_entry->n_obj - cont_entry->n_deleted); */
        
        /* int i; */
        /* for (i = 0; i < cont_entry->n_obj; i++) */ 
        /*     printf(" %" PRIu64 ",", cont_entry->obj_ids[i]); */
        /* printf("\n"); */
 
        // TODO: find duplicates
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container %" PRIu64 " not found!\n", 
                pdc_server_rank_g, __func__, cont_id);
        ret_value = FAIL;
        goto done;
    }
   

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_container_add_objs

/*
 * Delete objects to a container 
 *
 * \param  n_obj[IN]           Number of objects to be added/deleted
 * \param  obj_ids[IN]        Pointer to object array with n_obj objects
 * \param  cont_id[IN]        Container ID
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_container_del_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id)
{
    perr_t ret_value = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    int realloc_size = 0, i, j;
    int n_deletes = 0;

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
        printf("==PDC_SERVER[%d]: successfully deleted %d objects!\n", 
                pdc_server_rank_g, n_deletes);

        if (n_deletes != n_obj) {
            printf("==PDC_SERVER[%d]: %s - %d objects are not found to be deleted!\n", 
                    pdc_server_rank_g, __func__, n_obj - n_deletes);
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container %" PRIu64 " not found!\n", 
                pdc_server_rank_g, __func__, cont_id);
        ret_value = FAIL;
        goto done;
    }

    // Debug prints
    /* printf("==PDC_SERVER[%d]: After deletion, container %" PRIu64 " has %d objects:\n", */ 
    /*         pdc_server_rank_g, cont_id, cont_entry->n_obj - cont_entry->n_deleted); */
    /* for (i = 0; i < cont_entry->n_obj; i++) { */
    /*     if (cont_entry->obj_ids[i] != 0) { */
    /*         printf(" %" PRIu64 ",", cont_entry->obj_ids[i]); */
    /*     } */
    /* } */
    /* printf("\n"); */
 
 
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_container_del_objs


