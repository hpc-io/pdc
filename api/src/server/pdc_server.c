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

#ifdef ENABLE_MPI
    #include "mpi.h"
#endif
#include "utlist.h"
#include "dablooms.h"

#include "mercury.h"
#include "mercury_macros.h"

// Mercury hash table and list
#include "mercury_hash_table.h"
#include "mercury_list.h"

#include "pdc_interface.h"
/* #include "pdc_client_connect.h" */
#include "pdc_client_server_common.h"
#include "pdc_server.h"

#ifdef ENABLE_MULTITHREAD 
// Mercury multithread
#include "mercury_thread.h"
#include "mercury_thread_pool.h"
#include "mercury_thread_mutex.h"

hg_thread_mutex_t pdc_metadata_hash_table_mutex_g;
/* hg_thread_mutex_t pdc_metadata_name_mark_hash_table_mutex_g; */
hg_thread_mutex_t pdc_time_mutex_g;
hg_thread_mutex_t pdc_bloom_time_mutex_g;
hg_thread_mutex_t n_metadata_mutex_g;
#endif

#define BLOOM_TYPE_T counting_bloom_t
#define BLOOM_NEW    new_counting_bloom
#define BLOOM_CHECK  counting_bloom_check
#define BLOOM_ADD    counting_bloom_add
#define BLOOM_REMOVE counting_bloom_remove
#define BLOOM_FREE   free_counting_bloom

hg_class_t *hg_class_g = NULL;
hg_context_t *hg_context_g = NULL;

// Global thread pool
hg_thread_pool_t *hg_test_thread_pool_g = NULL;

// Global hash table for storing metadata 
hg_hash_table_t *metadata_hash_table_g = NULL;
/* hg_hash_table_t *metadata_name_mark_hash_table_g = NULL; */

int is_hash_table_init_g = 0;
int is_restart_g = 0;

int pdc_server_rank_g = 0;
int pdc_server_size_g = 1;

char pdc_server_tmp_dir_g[ADDR_MAX];

// Debug statistics var
int n_bloom_total_g;
int n_bloom_maybe_g;
double server_bloom_check_time_g  = 0.0;
double server_bloom_insert_time_g = 0.0;
double server_insert_time_g       = 0.0;
double server_delete_time_g       = 0.0;
double server_update_time_g       = 0.0;
double server_hash_insert_time_g  = 0.0;
double server_bloom_init_time_g   = 0.0;

double total_mem_usage_g          = 0.0;

uint32_t n_metadata_g             = 0;



static int 
PDC_Server_metadata_int_equal(hg_hash_table_key_t vlocation1, hg_hash_table_key_t vlocation2)
{
    return *((uint32_t *) vlocation1) == *((uint32_t *) vlocation2);
}

static unsigned int
PDC_Server_metadata_int_hash(hg_hash_table_key_t vlocation)
{
    return *((uint32_t *) vlocation);
}

static void
PDC_Server_metadata_int_hash_key_free(hg_hash_table_key_t key)
{
    free((uint32_t *) key);
}

/* static void */
/* PDC_Server_metadata_name_mark_hash_value_free(hg_hash_table_value_t value) */
/* { */
/*     pdc_metadata_name_mark_t *elt, *tmp, *head; */

/*     head = (pdc_metadata_name_mark_t *) value; */

/*     // Free metadata list */
/*     DL_FOREACH_SAFE(head,elt,tmp) { */
/*       /1* DL_DELETE(head,elt); *1/ */
/*       free(elt); */
/*     } */
/* } */


static void
PDC_Server_metadata_hash_value_free(hg_hash_table_value_t value)
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

/* inline void PDC_Server_metadata_name_mark_init(pdc_metadata_t* a) */
/* { */
/*     a->prev                 = NULL; */
/*     a->next                 = NULL; */
/* } */

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
    a->prev                 = NULL;
    a->next                 = NULL;
}
// ^ hash table

static inline void combine_obj_info_to_string(pdc_metadata_t *metadata, char *output)
{
    /* sprintf(output, "%d%s%s%d", metadata->user_id, metadata->app_name, metadata->obj_name, metadata->time_step); */
    FUNC_ENTER(NULL);
    sprintf(output, "%s%d", metadata->obj_name, metadata->time_step);
}

/* static int find_identical_namemark(pdc_metadata_name_mark_t *mlist, pdc_metadata_name_mark_t *a) */
/* { */
/*     FUNC_ENTER(NULL); */

/*     perr_t ret_value; */

/*     pdc_metadata_name_mark_t *elt; */
/*     DL_FOREACH(mlist, elt) { */
/*         if (strcmp(elt->obj_name, a->obj_name) == 0) { */
/*             /1* printf("Identical namemark with name [%s] already exist in current Metadata store!\n", ); *1/ */
/*             ret_value = 1; */
/*             goto done; */
/*         } */
/*     } */

/*     ret_value = 0; */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */ 

static pdc_metadata_t * find_metadata_by_id_and_hash_key(uint64_t obj_id, uint32_t hash_key) 
{
    pdc_metadata_t *ret_value = NULL;
    pdc_metadata_t *elt;
    pdc_hash_table_entry_head *lookup_value = NULL;
    
    FUNC_ENTER(NULL);

    // TODO
    if (metadata_hash_table_g != NULL) {
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, &hash_key);

        if (lookup_value == NULL) {
            ret_value = NULL;
            goto done;
        }

        DL_FOREACH(lookup_value->metadata, elt) {
            if (elt->obj_id == obj_id) {
                ret_value = elt;
                goto done;
            }
        }

    }  // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
        ret_value = NULL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

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

// Iterate through all metadata stored in the hash table 
static pdc_metadata_t * find_metadata_by_id(uint64_t obj_id) 
{
    pdc_metadata_t *ret_value;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    hg_hash_table_iter_t hash_table_iter;
    int n_entry;
    
    FUNC_ENTER(NULL);

    if (metadata_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hg_hash_table_num_entries(metadata_hash_table_g);
        hg_hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hg_hash_table_iter_has_more(&hash_table_iter)) {

            head = hg_hash_table_iter_next(&hash_table_iter);
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt) {
                if (elt->obj_id == obj_id) {
                    return elt;
                }
            }
        }
    }  // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
        ret_value = NULL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id)
{
    pdc_metadata_t *ret_value = NULL;
    
    FUNC_ENTER(NULL);

    ret_value = find_metadata_by_id(obj_id);

done:
    FUNC_LEAVE(ret_value);
}

static pdc_metadata_t * find_identical_metadata(pdc_hash_table_entry_head *entry, pdc_metadata_t *a)
{
    pdc_metadata_t *ret_value = NULL;
    BLOOM_TYPE_T *bloom;
    int bloom_check;
    
    FUNC_ENTER(NULL);

/*     printf("==PDC_SERVER: quering with:\n"); */
/*     PDC_print_metadata(a); */
/*     fflush(stdout); */

    // Use bloom filter to quick check if current metadata is in the list
    if (entry->bloom != NULL && a->user_id != 0 && a->app_name[0] != 0) {
        /* printf("==PDC_SERVER: using bloom filter\n"); */
        /* fflush(stdout); */

        bloom = entry->bloom;

        char combined_string[PATH_MAX];
        combine_obj_info_to_string(a, combined_string);
        /* printf("bloom_check: Combined string: %s\n", combined_string); */
        /* fflush(stdout); */

#ifdef ENABLE_TIMING
        // Timing
        gettimeofday(&ht_total_start, 0);
#endif

        bloom_check = BLOOM_CHECK(bloom, combined_string, strlen(combined_string));

#ifdef ENABLE_TIMING
        // Timing
        struct timeval  ht_total_start;
        struct timeval  ht_total_end;
        long long ht_total_elapsed;
        double ht_total_sec;
        
        gettimeofday(&ht_total_end, 0);
        ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
        ht_total_sec        = ht_total_elapsed / 1000000.0;
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


void PDC_Server_print_version()
{
    unsigned major, minor, patch;
    
    FUNC_ENTER(NULL);
    
    HG_Version_get(&major, &minor, &patch);
    printf("Server running mercury version %u.%u-%u\n", major, minor, patch);
    return;
}

static uint64_t PDC_Server_gen_obj_id()
{
    uint64_t ret_value;
    
    FUNC_ENTER(NULL);
    
    ret_value = pdc_id_seq_g++;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_get_self_addr(hg_class_t* hg_class, char* self_addr_string)
{
    perr_t ret_value;
    hg_addr_t self_addr;
    hg_size_t self_addr_string_size = PATH_MAX;
    
    FUNC_ENTER(NULL);
 
    // Get self addr to tell client about 
    HG_Addr_self(hg_class, &self_addr);
    HG_Addr_to_string(hg_class, self_addr_string, &self_addr_string_size, self_addr);
    HG_Addr_free(hg_class, self_addr);

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_write_addr_to_file(char** addr_strings, int n)
{
    perr_t ret_value = SUCCEED;;
    char config_fname[PATH_MAX];
    int i;
    
    FUNC_ENTER(NULL);

    // write to file
    
    sprintf(config_fname, "%s/%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);
    FILE *na_config = fopen(config_fname, "w+");
    if (!na_config) {
        fprintf(stderr, "Could not open config file from: %s\n", config_fname);
        exit(0);
    }
    
    fprintf(na_config, "%d\n", n);
    for (i = 0; i < n; i++) {
        fprintf(na_config, "%s\n", addr_strings[i]);
    }
    fclose(na_config);

done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_init_hash_table()
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    metadata_hash_table_g = hg_hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (metadata_hash_table_g == NULL) {
        printf("==PDC_SERVER: metadata_hash_table_g init error! Exit...\n");
        exit(-1);
    }
    hg_hash_table_register_free_functions(metadata_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_metadata_hash_value_free);

    /* // Name marker hash table, reuse some functions from metadata_hash_table */
    /* metadata_name_mark_hash_table_g = hg_hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal); */
    /* if (metadata_name_mark_hash_table_g == NULL) { */
    /*     printf("==PDC_SERVER: metadata_name_mark_hash_table_g init error! Exit...\n"); */
    /*     exit(-1); */
    /* } */
    /* hg_hash_table_register_free_functions(metadata_name_mark_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_metadata_name_mark_hash_value_free); */

    is_hash_table_init_g = 1;

done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_remove_from_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    if (bloom == NULL) {
        printf("==PDC_SERVER: PDC_Server_remove_from_bloom(): bloom pointer is NULL\n");
        ret_value = FAIL;
        goto done;
    }
 
    char combined_string[PATH_MAX];
    combine_obj_info_to_string(metadata, combined_string);
    /* printf("==PDC_SERVER: PDC_Server_remove_from_bloom(): Combined string: %s\n", combined_string); */

    ret_value = BLOOM_REMOVE(bloom, combined_string, strlen(combined_string));

done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_add_to_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;
    char combined_string[PATH_MAX];
    
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

done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_bloom_init(pdc_hash_table_entry_head *entry, BLOOM_TYPE_T *bloom)
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
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    
    gettimeofday(&ht_total_start, 0);
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
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    server_bloom_init_time_g += ht_total_sec;
#endif

done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_hash_table_list_insert(pdc_hash_table_entry_head *head, pdc_metadata_t *new)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *elt;
    
    FUNC_ENTER(NULL);

    // add to bloom filter
    if (head->n_obj == CREATE_BLOOM_THRESHOLD) {
        /* printf("==PDC_SERVER:Init bloom\n"); */
        /* fflush(stdout); */
        PDC_Server_bloom_init(head, head->bloom);
        DL_FOREACH(head->metadata, elt) {
            PDC_Server_add_to_bloom(elt, head->bloom);
        }
        PDC_Server_add_to_bloom(new, head->bloom);
    }
    else if (head->n_obj >= CREATE_BLOOM_THRESHOLD || head->bloom != NULL) {
        /* printf("==PDC_SERVER: Adding to bloom filter, %d existed\n", head->n_obj); */
        /* fflush(stdout); */
        PDC_Server_add_to_bloom(new, head->bloom);
    }

    /* printf("Adding to linked list\n"); */
    // Currently $metadata is unique, insert to linked list
    DL_APPEND(head->metadata, new);
    head->n_obj++;

    /* // Debug print */
    /* int count; */
    /* pdc_metadata_t *elt; */
    /* DL_COUNT(head, elt, count); */
    /* printf("Append one metadata, total=%d\n", count); */
 
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Server_hash_table_list_init(pdc_hash_table_entry_head *entry, uint32_t *hash_key)
{
    
    perr_t      ret_value = 0;
    hg_return_t ret;
    
    FUNC_ENTER(NULL);

    /* printf("==PDC_SERVER[%d]: hash entry init for hash key [%u]\n", pdc_server_rank_g, *hash_key); */

/*     // Init head of linked list */
/*     metadata->prev = metadata;                                                                   \ */
/*     metadata->next = NULL; */   

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    gettimeofday(&ht_total_start, 0);
#endif

    // Insert to hash table
    ret = hg_hash_table_insert(metadata_hash_table_g, hash_key, entry);
    if (ret != 1) {
        fprintf(stderr, "PDC_Server_hash_table_list_init(): Error with hash table insert!\n");
        ret_value = -1;
        goto done;
    }

    /* PDC_print_metadata(entry->metadata); */
    /* printf("entry n_obj=%d\n", entry->n_obj); */

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;

    server_hash_insert_time_g += ht_total_sec;
#endif

    /* PDC_Server_bloom_init(new, entry->bloom); */

done:
    FUNC_LEAVE(ret_value);
}

/* perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out) { */
    
/*     FUNC_ENTER(NULL); */

/*     perr_t ret_value = SUCCEED; */
/*     hg_return_t hg_ret; */


/*     /1* printf("==PDC_SERVER: Insert obj name marker [%s]\n", in->obj_name); *1/ */

/*     uint32_t *hash_key = (uint32_t*)malloc(sizeof(uint32_t)); */
/*     if (hash_key == NULL) { */
/*         printf("Cannnot allocate hash_key!\n"); */
/*         goto done; */
/*     } */
/*     total_mem_usage_g += sizeof(uint32_t); */

/*     *hash_key = in->hash_value; */

/*     pdc_metadata_name_mark_t *namemark= (pdc_metadata_name_mark_t*)malloc(sizeof(pdc_metadata_name_mark_t)); */
/*     if (namemark == NULL) { */
/*         printf("==PDC_SERVER: ERROR - Cannnot allocate pdc_metadata_name_mark_t!\n"); */
/*         goto done; */
/*     } */
/*     total_mem_usage_g += sizeof(pdc_metadata_name_mark_t); */
/*     strcpy(namemark->obj_name, in->obj_name); */

/*     pdc_metadata_name_mark_t *lookup_value; */
/*     pdc_metadata_name_mark_t *elt; */

/* #ifdef ENABLE_MULTITHREAD */ 
/*     // Obtain lock for hash table */
/*     int unlocked = 0; */
/*     hg_thread_mutex_lock(&pdc_metadata_name_mark_hash_table_mutex_g); */
/* #endif */

/*     if (metadata_name_mark_hash_table_g != NULL) { */
/*         // lookup */
/*         /1* printf("checking hash table with key=%d\n", *hash_key); *1/ */
/*         lookup_value = hg_hash_table_lookup(metadata_name_mark_hash_table_g, hash_key); */

/*         // Is this hash value exist in the Hash table? */
/*         if (lookup_value != NULL) { */
/*             // Check if there exist namemark identical to current one */
/*             if (find_identical_namemark(lookup_value, namemark) == 1) { */
/*                 // If find same one, do nothing */
/*                 /1* printf("==PDC_SERVER: marker exist for [%s]\n", namemark->obj_name); *1/ */
/*                 ret_value = 0; */
/*                 free(namemark); */
/*             } */
/*             else { */
/*                 // Currently namemark is unique, insert to linked list */
/*                 DL_APPEND(lookup_value, namemark); */
/*             } */
        
/*             /1* free(hash_key); *1/ */
/*         } */
/*         else { */
/*             /1* printf("lookup_value is NULL!\n"); *1/ */
/*             // First entry for current hasy_key, init linked list */
/*             namemark->prev = namemark;                                                                   \ */
/*             namemark->next = NULL; */   

/*             // Insert to hash table */
/*             hg_ret = hg_hash_table_insert(metadata_name_mark_hash_table_g, hash_key, namemark); */
/*             if (hg_ret != 1) { */
/*                 fprintf(stderr, "==PDC_SERVER: ERROR - insert_obj_name_marker() error with hash table insert!\n"); */
/*                 ret_value = -1; */
/*                 goto done; */
/*             } */
/*         } */
/*     } */
/*     else { */
/*         printf("metadata_hash_table_g not initilized!\n"); */
/*         ret_value = -1; */
/*         goto done; */
/*     } */

/* #ifdef ENABLE_MULTITHREAD */ 
/*     // ^ Release hash table lock */
/*     hg_thread_mutex_unlock(&pdc_metadata_name_mark_hash_table_mutex_g); */
/* #endif */

/* done: */
/*     out->ret = 1; */
/*     FUNC_LEAVE(ret_value); */
/* } */

perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out)
{
    perr_t ret_value;
    uint64_t obj_id;
    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t *elt;
    int unlocked = 0;
    uint32_t *hash_key;
    pdc_metadata_t *target;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;
    
    gettimeofday(&ht_total_start, 0);
#endif

    /* printf("==PDC_SERVER: Got update request: hash=%d, obj_id=%llu\n", in->hash_value, in->obj_id); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannnot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    obj_id = in->obj_id;

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("==PDC_SERVER: checking hash table with key=%d\n", *hash_key); */
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            /* printf("==PDC_SERVER: lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
            if (target != NULL) {
                /* printf("==PDC_SERVER: Found update target!\n"); */

                // Check and find valid update fields
                // Currently user_id, obj_name are not supported to be updated in this way
                // obj_name change is done through client with delete and add operation.
                if (in->new_metadata.time_step != -1) 
                    target->time_step = in->new_metadata.time_step;
                if (in->new_metadata.app_name[0] != 0) 
                    strcpy(target->app_name,      in->new_metadata.app_name);
                if (in->new_metadata.data_location[0] != 0) 
                    strcpy(target->data_location, in->new_metadata.data_location);
                if (in->new_metadata.tags[0] != 0) 
                    strcpy(target->tags,          in->new_metadata.tags);

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
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
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
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
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
    FUNC_LEAVE(ret_value);
} // end of update_metadata_from_hash_table


perr_t delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out)
{
    perr_t ret_value = FAIL;
    pdc_metadata_t metadata;
    pdc_metadata_t *elt;
    hg_hash_table_iter_t hash_table_iter;
    uint64_t target_obj_id;
    int n_entry;

    FUNC_ENTER(NULL);

    out->ret = -1;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    gettimeofday(&ht_total_start, 0);
#endif


    /* printf("==PDC_SERVER[%d]: Got delete by id request: obj_id=%llu\n", pdc_server_rank_g, in->obj_id); */
    /* fflush(stdout); */

    target_obj_id = in->obj_id;

    // TODO
    metadata.obj_id = 0;
        
    /* printf("==PDC_SERVER: delete request name:%s ts=%d hash=%u\n", in->obj_name, in->time_step, in->hash_value); */

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {

        // Since we only have the obj id, need to iterate the entire hash table
        pdc_hash_table_entry_head *head; 

        n_entry = hg_hash_table_num_entries(metadata_hash_table_g);
        hg_hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hg_hash_table_iter_has_more(&hash_table_iter)) {

            head = hg_hash_table_iter_next(&hash_table_iter);
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
                        hg_hash_table_remove(metadata_hash_table_g, &hash_key);

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
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
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
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
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
    /* printf("==PDC_SERVER[%d]: Finished delete by id request: obj_id=%llu\n", pdc_server_rank_g, in->obj_id); */
    /* fflush(stdout); */
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    FUNC_LEAVE(ret_value);
} // end of delete_metadata_by_id


perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out)
{
    perr_t ret_value;
    uint32_t *hash_key;
    pdc_metadata_t *target;
    
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    gettimeofday(&ht_total_start, 0);
#endif

    /* printf("==PDC_SERVER[%d]: Got delete request: hash=%d, obj_id=%llu\n", pdc_server_rank_g, in->hash_value, in->obj_id); */
    /* fflush(stdout); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannnot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    /* uint64_t obj_id = in->obj_id; */

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t *elt;
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
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, hash_key);

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
                    hg_hash_table_remove(metadata_hash_table_g, hash_key);

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
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
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
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
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
    /* printf("==PDC_SERVER[%d]: Finished delete request: hash=%u, obj_id=%llu\n", pdc_server_rank_g, in->hash_value, in->obj_id); */
    /* fflush(stdout); */
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    FUNC_LEAVE(ret_value);
} // end of delete_metadata_from_hash_table


perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{
    perr_t ret_value;
    pdc_metadata_t *metadata;
    uint32_t *hash_key;
    
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double ht_total_sec;

    gettimeofday(&ht_total_start, 0);
#endif

    /* printf("Got object creation request with name: %s\tHash=%d\n", in->data.obj_name, in->hash_value); */
    /* printf("Full name check: %s\n", &in->obj_name[507]); */

    metadata = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    if (metadata == NULL) {
        printf("Cannnot allocate pdc_metadata_t!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(pdc_metadata_t);

    // TODO: [Future work] Both server and client gets it and do security check
    metadata->user_id        = in->data.user_id;
    metadata->time_step      = in->data.time_step;
    metadata->ndim           = in->data.ndim;
    int i;
    for (i = 0; i < metadata->ndim; i++) 
        metadata->dims[i]    =      in->data.dims[i];
    strcpy(metadata->obj_name,      in->data.obj_name);
    strcpy(metadata->app_name,      in->data.app_name);
    strcpy(metadata->tags,          in->data.tags);
    strcpy(metadata->data_location, in->data.data_location);
    metadata->region_lock_head = NULL;
    metadata->region_map_head  = NULL;
    // DEBUG
    /* int debug_flag = 1; */
    int debug_flag = 0;
    /* if (in->data.time_step >= 89808) { */
    /*     debug_flag = 1; */
    /*     /1* while (debug_flag) {;} *1/ */
    /*     PDC_print_metadata(metadata); */
    /* } */

    /* strcpy(metadata->data_location, in->data.data_location); */
    /* create_time              =; */
    /* last_modified_time       =; */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("Cannnot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t *elt, *found_identical;

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (debug_flag == 1) 
        printf("checking hash table with key=%d\n", *hash_key);

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            
            if (debug_flag == 1) 
                printf("lookup_value not NULL!\n");
            // Check if there exist metadata identical to current one
            /* found_identical = NULL; */
            found_identical = find_identical_metadata(lookup_value, metadata);
            if ( found_identical != NULL) {
                if (debug_flag == 1) {
                    printf("Found identical metadata!\n");
                    /* PDC_print_metadata(metadata); */
                    /* PDC_print_metadata(found_identical); */
                }
                ret_value = -1;
                out->ret  = -1;
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
        printf("metadata_hash_table_g not initilized!\n");
        ret_value = -1;
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
    out->ret = metadata->obj_id;

    // Debug print metadata info
    /* PDC_print_metadata(metadata); */

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed    = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    ht_total_sec        = ht_total_elapsed / 1000000.0;
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
    FUNC_LEAVE(ret_value);
} // end of insert_metadata_to_hash_table


static perr_t PDC_Server_metadata_duplicate_check()
{
    perr_t ret_value = SUCCEED;
    hg_hash_table_iter_t hash_table_iter;
    int n_entry, count = 0, dl_count;
    int all_maybe, all_total, all_entry;
    int has_dup_obj = 0;
    int all_dup_obj = 0;
    pdc_metadata_t *elt, *elt_next;
    pdc_hash_table_entry_head *head;
    
    FUNC_ENTER(NULL);

    n_entry = hg_hash_table_num_entries(metadata_hash_table_g);

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

    hg_hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

    while (n_entry != 0 && hg_hash_table_iter_has_more(&hash_table_iter)) {
        head = hg_hash_table_iter_next(&hash_table_iter);
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

perr_t PDC_Server_init(int port, hg_class_t **hg_class, hg_context_t **hg_context)
{
    perr_t ret_value = SUCCEED;;
    int status;
    int i;
    char *all_addr_strings_1d;
    char **all_addr_strings;
    char self_addr_string[PATH_MAX];
    char na_info_string[PATH_MAX];
    char hostname[1024];
    
    FUNC_ENTER(NULL);

    /* PDC_Server_print_version(); */

    // set server id start
    pdc_id_seq_g = pdc_id_seq_g * (pdc_server_rank_g+1);

    // Create server tmp dir
    
    status = mkdir(pdc_server_tmp_dir_g, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    if (pdc_server_rank_g == 0) {
        all_addr_strings_1d = (char* )malloc(sizeof(char ) * pdc_server_size_g * PATH_MAX);
        all_addr_strings    = (char**)malloc(sizeof(char*) * pdc_server_size_g );
        total_mem_usage_g += (sizeof(char) + sizeof(char*));
    }

    memset(hostname, 0, 1024);
    gethostname(hostname, 1023);
    sprintf(na_info_string, "bmi+tcp://%s:%d", hostname, port);
    /* sprintf(na_info_string, "cci+tcp://%s:%d", hostname, port); */
    if (pdc_server_rank_g == 0) 
        printf("\n==PDC_SERVER: using %.7s\n", na_info_string);
    
    if (!na_info_string) {
        fprintf(stderr, HG_PORT_NAME " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"tcp://127.0.0.1:22222\"\n");
        exit(0);
    }

    // Init server
    *hg_class = HG_Init(na_info_string, NA_TRUE);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        return FAIL;
    }

    // Create HG context 
    *hg_context = HG_Context_create(*hg_class);
    if (*hg_context == NULL) {
        printf("Error with HG_Context_create()\n");
        return FAIL;
    }

    // Get server address
    PDC_Server_get_self_addr(*hg_class, self_addr_string);
    /* printf("Server address is: %s\n", self_addr_string); */
    /* fflush(stdout); */

    // Gather addresses
#ifdef ENABLE_MPI
    MPI_Gather(self_addr_string, PATH_MAX, MPI_CHAR, all_addr_strings_1d, PATH_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
    if (pdc_server_rank_g == 0) {
        for (i = 0; i < pdc_server_size_g; i++) {
            all_addr_strings[i] = &all_addr_strings_1d[i*PATH_MAX];
            /* printf("%s\n", all_addr_strings[i]); */
        }
    }
#else 
    all_addr_strings[0] = self_addr_string;
#endif
    // Rank 0 write all addresses to one file
    if (pdc_server_rank_g == 0) {
        /* printf("========================\n"); */
        /* printf("Server address%s:\n", pdc_server_size_g ==1?"":"es"); */
        /* for (i = 0; i < pdc_server_size_g; i++) */ 
        /*     printf("%s\n", all_addr_strings[i]); */
        /* printf("========================\n"); */
        PDC_Server_write_addr_to_file(all_addr_strings, pdc_server_size_g);

        // Free
        free(all_addr_strings_1d);
        free(all_addr_strings);
    }
    fflush(stdout);

#ifdef ENABLE_MULTITHREAD
    // Init threadpool
    char *nthread_env = getenv("PDC_SERVER_NTHREAD"); 
    int n_thread; 
    if (nthread_env != NULL) 
        n_thread = atoi(nthread_env);
    
    if (n_thread < 1) 
        n_thread = 2;
    hg_thread_pool_init(n_thread, &hg_test_thread_pool_g);
    if (pdc_server_rank_g == 0) {
        printf("\n==PDC_SERVER: Starting server with %d threads...\n", n_thread);
        fflush(stdout);
    }
#else
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: without multi-thread!\n");
        fflush(stdout);
    }
#endif

    // TODO: support restart with different number of servers than previous run 
    char checkpoint_file[PATH_MAX];
    if (is_restart_g == 1) {
        sprintf(checkpoint_file, "%s/%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);

#ifdef ENABLE_TIMING 
        // Timing
        struct timeval  ht_total_start;
        struct timeval  ht_total_end;
        long long ht_total_elapsed;
        double restart_time, all_restart_time;
        gettimeofday(&ht_total_start, 0);
#endif

        PDC_Server_restart(checkpoint_file);

#ifdef ENABLE_TIMING 
        // Timing
        gettimeofday(&ht_total_end, 0);
        ht_total_elapsed = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
        restart_time = ht_total_elapsed / 1000000.0;

    #ifdef ENABLE_MPI
        MPI_Reduce(&restart_time, &all_restart_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    #else
        all_restart_time = restart_time;
    #endif
        if (pdc_server_rank_g == 0) 
            printf("==PDC_SERVER: total restart time = %.6f\n", all_restart_time);
#endif
    }
    else {
        // We are starting a brand new server
        if (is_hash_table_init_g != 1) {
            // Hash table init
            PDC_Server_init_hash_table();
        }
    }

    // Initalize atomic variable to finalize server 
    hg_atomic_set32(&close_server_g, 0);

    n_metadata_g = 0;

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_init

perr_t PDC_Server_finalize()
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    // Debug: check duplicates
    /* PDC_Server_metadata_duplicate_check(); */
    /* fflush(stdout); */

    // Free hash table
    if(metadata_hash_table_g != NULL)
        hg_hash_table_free(metadata_hash_table_g);

/*     if(metadata_name_mark_hash_table_g != NULL) */
/*         hg_hash_table_free(metadata_name_mark_hash_table_g); */
#ifdef ENABLE_TIMING 

    double all_bloom_check_time_max, all_bloom_check_time_min, all_insert_time_max, all_insert_time_min;
    double all_server_bloom_init_time_min,  all_server_bloom_init_time_max;
    double all_server_bloom_insert_time_min,  all_server_bloom_insert_time_max;
    double all_server_hash_insert_time_min, all_server_hash_insert_time_max;

    #ifdef ENABLE_MPI
    MPI_Reduce(&server_bloom_check_time_g, &all_bloom_check_time_max,        1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_bloom_check_time_g, &all_bloom_check_time_min,        1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_insert_time_g,      &all_insert_time_max,             1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_insert_time_g,      &all_insert_time_min,             1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    MPI_Reduce(&server_bloom_init_time_g,  &all_server_bloom_init_time_max,  1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_bloom_init_time_g,  &all_server_bloom_init_time_min,  1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_hash_insert_time_g, &all_server_hash_insert_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_hash_insert_time_g, &all_server_hash_insert_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    #else
    all_bloom_check_time_min        = server_bloom_check_time_g;
    all_bloom_check_time_max        = server_bloom_check_time_g;
    all_insert_time_max             = server_insert_time_g;
    all_insert_time_min             = server_insert_time_g;
    all_server_bloom_init_time_min  = server_bloom_init_time_g;
    all_server_bloom_init_time_max  = server_bloom_init_time_g;
    all_server_hash_insert_time_max = server_hash_insert_time_g;
    all_server_hash_insert_time_min = server_hash_insert_time_g;
    #endif
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: total bloom check time = %.6f, %.6f\n", all_bloom_check_time_min, all_bloom_check_time_max);
        printf("==PDC_SERVER: total insert      time = %.6f, %.6f\n", all_insert_time_min, all_insert_time_max);
        printf("==PDC_SERVER: total hash insert time = %.6f, %.6f\n", all_server_hash_insert_time_min, all_server_hash_insert_time_max);
        printf("==PDC_SERVER: total bloom init  time = %.6f, %.6f\n", all_server_bloom_init_time_min, all_server_bloom_init_time_max);
        printf("==PDC_SERVER: total memory usage     = %.2f MB\n", total_mem_usage_g/1048576.0);
        fflush(stdout);
    }
    // TODO: remove server tmp dir?

#endif

done:
    FUNC_LEAVE(ret_value);
}

static HG_THREAD_RETURN_TYPE
hg_progress_thread(void *arg)
{
    /* pthread_t tid = pthread_self(); */
    pid_t tid;
    /* tid = syscall(SYS_gettid); */
    hg_context_t *context = (hg_context_t*)arg;
    HG_THREAD_RETURN_TYPE tret = (HG_THREAD_RETURN_TYPE) 0;
    hg_return_t ret = HG_SUCCESS;
    
    FUNC_ENTER(NULL);

    do {
        if (hg_atomic_get32(&close_server_g)) break;

        ret = HG_Progress(context, 100);
        /* printf("thread [%d]\n", tid); */
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    hg_thread_exit(tret);

    return tret;
}

// Backup in-memory DHT/Bloom to persist storage
perr_t PDC_Server_checkpoint(char *filename)
{
    perr_t ret_value = SUCCEED;;
    pdc_metadata_t *elt;
    pdc_hash_table_entry_head *head;
    int count, n_entry, checkpoint_count = 0;
    uint32_t hash_key;
    
    FUNC_ENTER(NULL);

    if (pdc_server_rank_g == 0) {
        printf("\n\n==PDC_SERVER: Start checkpoint process [%s]\n", filename);
    }

    FILE *file = fopen(filename, "w+");
    if (file==NULL) {fputs("==PDC_SERVER: PDC_Server_checkpoint() - Checkpoint file open error", stderr); return -1;}

    // DHT
    n_entry = hg_hash_table_num_entries(metadata_hash_table_g);
    /* printf("%d entries\n", n_entry); */
    fwrite(&n_entry, sizeof(int), 1, file);

    hg_hash_table_iter_t hash_table_iter;
    hg_hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

    while (n_entry != 0 && hg_hash_table_iter_has_more(&hash_table_iter)) {
        head = hg_hash_table_iter_next(&hash_table_iter);
        /* printf("count=%d\n", head->n_obj); */
        /* fflush(stdout); */

        fwrite(&head->n_obj, sizeof(int), 1, file);
        // TODO: find a way to get hash_key from hash table istead of calculating again.
        hash_key = PDC_get_hash_by_name(head->metadata->obj_name);
        fwrite(&hash_key, sizeof(uint32_t), 1, file);
        // Iterate every metadata structure in current entry
        DL_FOREACH(head->metadata, elt) {
            /* printf("==PDC_SERVER: Writing one metadata...\n"); */
            /* PDC_print_metadata(elt); */
            fwrite(elt, sizeof(pdc_metadata_t), 1, file);
            checkpoint_count++;
        }
    }

    fclose(file);

    int all_checkpoint_count;
#ifdef ENABLE_MPI
    MPI_Reduce(&checkpoint_count, &all_checkpoint_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_checkpoint_count = checkpoint_count;
#endif
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: checkpointed %d objects\n", all_checkpoint_count);
    }

done:
    FUNC_LEAVE(ret_value);
}

// Restart in-memory DHT/Bloom filters from persist storage
perr_t PDC_Server_restart(char *filename)
{
    perr_t ret_value = 1;
    int n_entry, count, i, j, nobj = 0, all_nobj = 0;
    pdc_metadata_t *metadata, *elt;
    pdc_hash_table_entry_head *entry;
    uint32_t *hash_key;
    
    FUNC_ENTER(NULL);

    FILE *file = fopen(filename, "r");
    if (file==NULL) {fputs("==PDC_SERVER: PDC_Server_restart() - Checkpoint file not available\n", stderr); return -1;}

    // init hash table
    PDC_Server_init_hash_table();
    fread(&n_entry, sizeof(int), 1, file);
    /* printf("%d entries\n", n_entry); */

    while (n_entry>0) {
        fread(&count, sizeof(int), 1, file);
        /* printf("Count:%d\n", count); */

        hash_key = (uint32_t *)malloc(sizeof(uint32_t));
        fread(hash_key, sizeof(uint32_t), 1, file);
        /* printf("Hash key is %u\n", *hash_key); */
        total_mem_usage_g += sizeof(uint32_t);

        // Reconstruct hash table
        entry = (pdc_hash_table_entry_head*)malloc(sizeof(pdc_hash_table_entry_head));
        entry->n_obj = 0;
        entry->bloom = NULL;
        entry->metadata = NULL;
        metadata = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t) * count);
        fread(metadata, sizeof(pdc_metadata_t), count, file);
        nobj += count;
        total_mem_usage_g += sizeof(pdc_hash_table_entry_head);
        total_mem_usage_g += (sizeof(pdc_metadata_t)*count);

        // Debug print for loaded metadata from checkpoint file
        /* for (i = 0; i < count; i++) { */
        /*     elt = metadata + i; */
        /*     PDC_print_metadata(elt); */
        /* } */

        // Init hash table metadata (w/ bloom) with first obj
        PDC_Server_hash_table_list_init(entry, hash_key);

        entry->metadata = NULL;

        // Insert the rest objs to the linked list
        for (i = 0; i < count; i++) {
            elt = metadata + i;
            // Add to hash list and bloom filter
            PDC_Server_hash_table_list_insert(entry, elt);
        }
        n_entry--;

        /* free(metadata); */
    }

    fclose(file);

#ifdef ENABLE_MPI
    MPI_Reduce(&nobj, &all_nobj, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_nobj = nobj;
#endif 
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: Server restarted from saved session, successfully loaded %d objects...\n", all_nobj);
    }
    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

// Multithread Mercury
static perr_t PDC_Server_multithread_loop(hg_class_t *class, hg_context_t *context)
{
    perr_t ret_value = SUCCEED;
    hg_thread_t progress_thread;
    hg_return_t ret = HG_SUCCESS;
    
    FUNC_ENTER(NULL);
    
    hg_thread_create(&progress_thread, hg_progress_thread, context);

    do {
        if (hg_atomic_get32(&close_server_g)) break;

        ret = HG_Trigger(context, 0, 1, NULL);
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    hg_thread_join(progress_thread);

    // Destory pool
    hg_thread_pool_destroy(hg_test_thread_pool_g);
    /* hg_thread_mutex_destroy(&close_server_g); */

done:
    FUNC_LEAVE(ret_value);
}

// No threading
static perr_t PDC_Server_loop(hg_class_t *hg_class, hg_context_t *hg_context)
{
    perr_t ret_value = SUCCEED;;
    hg_return_t hg_ret;
    unsigned int actual_count;
    
    FUNC_ENTER(NULL);
    
    /* Poke progress engine and check for events */
    do {
        actual_count = 0;
        do {
            /* hg_ret = HG_Trigger(hg_context, 1024/1* timeout *1/, 4096/1* max count *1/, &actual_count); */
            hg_ret = HG_Trigger(hg_context, 0/* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (hg_atomic_cas32(&close_server_g, 1, 1)) break;
        /* if (hg_atomic_get32(&close_server_g)) { */
        /*     /1* printf("\n==PDC_SERVER[%d]: Close server request received\n", pdc_server_rank_g); *1/ */
            /* fflush(stdout); */
        /*     ret_value = SUCCEED; */
        /*     goto done; */
        /* } */
        hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME);

    } while (hg_ret == HG_SUCCESS);

    if (hg_ret == HG_SUCCESS) 
        ret_value = SUCCEED;
    else
        ret_value = FAIL;

done:
    FUNC_LEAVE(ret_value);
}

/* For 1D boxes (intervals) we have: */
/* box1 = (xmin1, xmax1) */
/* box2 = (xmin2, xmax2) */
/* overlapping1D(box1,box2) = xmax1 >= xmin2 and xmax2 >= xmin1 */

/* For 2D boxes (rectangles) we have: */
/* box1 = (x:(xmin1,xmax1),y:(ymin1,ymax1)) */
/* box2 = (x:(xmin2,xmax2),y:(ymin2,ymax2)) */
/* overlapping2D(box1,box2) = overlapping1D(box1.x, box2.x) and */ 
/*                            overlapping1D(box1.y, box2.y) */

/* For 3D boxes we have: */
/* box1 = (x:(xmin1,xmax1),y:(ymin1,ymax1),z:(zmin1,zmax1)) */
/* box2 = (x:(xmin2,xmax2),y:(ymin2,ymax2),z:(zmin2,zmax2)) */
/* overlapping3D(box1,box2) = overlapping1D(box1.x, box2.x) and */ 
/*                            overlapping1D(box1.y, box2.y) and */
/*                            overlapping1D(box1.z, box2.z) */
 
static int is_overlap_1D(uint64_t xmin1, uint64_t xmax1, uint64_t xmin2, uint64_t xmax2)
{
    int ret_value = -1;
    
    FUNC_ENTER(NULL);

    if (xmax1 >= xmin2 && xmax2 >= xmin1) {
        ret_value = 1;
    }

    return ret_value;
}

static int is_overlap_2D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, 
                         uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2)
{
    int ret_value = -1;
    
    FUNC_ENTER(NULL);
    
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2 ) == 1 &&                              
        is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1) {
        ret_value = 1;
    }

    return ret_value;
}

static int is_overlap_3D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t zmin1, uint64_t zmax1,
                         uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2, uint64_t zmin2, uint64_t zmax2)
{
    int ret_value = -1;
    
    FUNC_ENTER(NULL);
    
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && 
        is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1 && 
        is_overlap_1D(zmin1, zmax1, zmin2, zmax2) == 1 ) 
    {
        ret_value = 1;
    }

    return ret_value;
}

static int is_overlap_4D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t zmin1, uint64_t zmax1,
                         uint64_t mmin1, uint64_t mmax1,
                         uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2, uint64_t zmin2, uint64_t zmax2,
                         uint64_t mmin2, uint64_t mmax2 )
{
    int ret_value = -1;
    
    FUNC_ENTER(NULL);
    
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && 
        is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1 && 
        is_overlap_1D(zmin1, zmax1, zmin2, zmax2) == 1 && 
        is_overlap_1D(mmin1, mmax1, mmin2, mmax2) == 1 ) 
    {
        ret_value = 1;
    }

    return ret_value;
}

// TODO: stride is not supported yet
static int is_contiguous_region_overlap(region_list_t *a, region_list_t *b)
{
    int ret_value = 1;
    
    FUNC_ENTER(NULL);

    if (a == NULL || b == NULL) {
        printf("==PDC_SERVER: is_region_identical() - passed NULL value!\n");
        ret_value = -1;
        goto done;
    }

    /* printf("==PDC_SERVER: is_contiguous_region_overlap adim=%d, bdim=%d\n", a->ndim, b->ndim); */
    if (a->ndim != b->ndim || a->ndim <= 0 || b->ndim <= 0) {
        ret_value = -1;
        goto done;
    }

    uint64_t xmin1, xmin2, xmax1, xmax2;
    uint64_t ymin1, ymin2, ymax1, ymax2;
    uint64_t zmin1, zmin2, zmax1, zmax2;
    uint64_t mmin1, mmin2, mmax1, mmax2;

    if (a->ndim >= 1) {
        xmin1 = a->start[0];
        xmax1 = a->start[0] + a->count[0] - 1;
        xmin2 = b->start[0];
        xmax2 = b->start[0] + b->count[0] - 1;
        /* printf("xmin1, xmax1, xmin2, xmax2: %llu %llu %llu %llu\n", xmin1, xmax1, xmin2, xmax2); */
    }
    if (a->ndim >= 2) {
        ymin1 = a->start[1];
        ymax1 = a->start[1] + a->count[1] - 1;
        ymin2 = b->start[1];
        ymax2 = b->start[1] + b->count[1] - 1;
        /* printf("ymin1, ymax1, ymin2, ymax2: %llu %llu %llu %llu\n", ymin1, ymax1, ymin2, ymax2); */
    }
    if (a->ndim >= 3) {
        zmin1 = a->start[2];
        zmax1 = a->start[2] + a->count[2] - 1;
        zmin2 = b->start[2];
        zmax2 = b->start[2] + b->count[2] - 1;
        /* printf("zmin1, zmax1, zmin2, zmax2: %llu %llu %llu %llu\n", zmin1, zmax1, zmin2, zmax2); */
    }
    if (a->ndim >= 4) {
        mmin1 = a->start[3];
        mmax1 = a->start[3] + a->count[3] - 1;
        mmin2 = b->start[3];
        mmax2 = b->start[3] + b->count[3] - 1;
    }
 
    if (a->ndim == 1) {
        ret_value = is_overlap_1D(xmin1, xmax1, xmin2, xmax2);
    }
    else if (a->ndim == 2) {
        ret_value = is_overlap_2D(xmin1, xmax1, ymin1, ymax1, xmin2, xmax2, ymin2, ymax2);
    }
    else if (a->ndim == 3) {
        ret_value = is_overlap_3D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax2, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2);
    }
    else if (a->ndim == 4) {
        ret_value = is_overlap_4D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax2, mmin1, mmax1, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2, mmin2, mmax2);
    }

done:
    FUNC_LEAVE(ret_value);
}


static int is_region_identical(region_list_t *a, region_list_t *b)
{
    int ret_value = -1;
    int i;
    
    FUNC_ENTER(NULL);

    if (a == NULL || b == NULL) {
        printf("==PDC_SERVER: is_region_identical() - passed NULL value!\n");
        ret_value = -1;
        goto done;
    }

    if (a->ndim != b->ndim) {
        ret_value = -1;
        goto done;
    }

    for (i = 0; i < a->ndim; i++) {
        if (a->start[i] != b->start[i] || a->count[i] != b->count[i] || a->stride[i] != b->stride[i] ) {
            ret_value = -1;
            goto done;
        }
    }

    ret_value = 1;

done:
    FUNC_LEAVE(ret_value);
}


perr_t PDC_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out)
{
    perr_t ret_value;
    uint64_t target_obj_id;
    int ndim;
    int lock_op;
    region_list_t *request_region;
    pdc_metadata_t *target_obj;
    
    FUNC_ENTER(NULL);
    
    /* printf("==PDC_SERVER: received lock request,                                \ */
    /*         obj_id=%llu, op=%d, ndim=%d, start=%llu count=%llu stride=%d\n", */ 
    /*         in->obj_id, in->lock_op, in->region.ndim, */ 
    /*         in->region.start_0, in->region.count_0, in->region.stride_0); */

    target_obj_id = in->obj_id;
    ndim = in->region.ndim;
    lock_op = in->lock_op;

    // Convert transferred lock region to structure
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    request_region->ndim = ndim;

    if (ndim >=1) {
        request_region->start[0]  = in->region.start_0;
        request_region->count[0]  = in->region.count_0;
        request_region->stride[0] = in->region.stride_0;
    }
    if (ndim >=2) {
        request_region->start[1]  = in->region.start_1;
        request_region->count[1]  = in->region.count_1;
        request_region->stride[1] = in->region.stride_1;
    }
    if (ndim >=3) {
        request_region->start[2]  = in->region.start_2;
        request_region->count[2]  = in->region.count_2;
        request_region->stride[2] = in->region.stride_2;
    }
    if (ndim >=4) {
        request_region->start[3]  = in->region.start_3;
        request_region->count[3]  = in->region.count_3;
        request_region->stride[3] = in->region.stride_3;
    }
    

    // Locate target metadata structure
    target_obj = find_metadata_by_id(target_obj_id);
    if (target_obj == NULL) {
        printf("==PDC_SERVER: PDC_Server_region_lock - requested object (id=%llu) does not exist\n", in->obj_id);
        ret_value = -1;
        out->ret = -1;
        goto done;
    }


    region_list_t *elt, *tmp;
    if (lock_op == PDC_LOCK_OP_OBTAIN) {
        /* printf("==PDC_SERVER: obtaining lock ... "); */
        // Go through all existing locks to check for overlapping
        // Note: currently only assumes contiguous region
        DL_FOREACH(target_obj->region_lock_head, elt) {
            if (is_contiguous_region_overlap(elt, request_region) == 1) {
                /* printf("rejected! (found overlapping regions)\n"); */
                out->ret = -1;
                goto done;
            }
        }
        // No overlaps found
        DL_APPEND(target_obj->region_lock_head, request_region);
        out->ret = 1;
        /* printf("granted\n"); */
        goto done;
    }
    else if (lock_op == PDC_LOCK_OP_RELEASE) {
        /* printf("==PDC_SERVER: releasing lock ... "); */
        // Find the lock region in the list and remove it
        DL_FOREACH_SAFE(target_obj->region_lock_head, elt, tmp) {
            if (is_region_identical(request_region, elt) == 1) {
                // Found the requested region lock, remove from the linked list
                DL_DELETE(target_obj->region_lock_head, elt);
                free(request_region);
                free(elt);
                out->ret = 1;
                /* printf("released!\n"); */
                goto done;
            }
        }
        // Request release lock region not found
        /* printf("requested release region/object does not exist\n"); */
    }
    else {
        printf("==PDC_SERVER: lock opreation %d not supported!\n", in->lock_op);
        out->ret = -1;
        goto done;
    }

    out->ret = 1;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

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

perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs)
{
    perr_t ret_value = FAIL;
    int i;
    uint32_t n_buf, iter = 0;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    hg_hash_table_iter_t hash_table_iter;
    int n_entry;
    
    FUNC_ENTER(NULL);

    // n_buf = n_metadata_g + 1 for potential padding array
    n_buf = n_metadata_g + 1;
    *buf_ptrs = (void**)malloc(n_buf * sizeof(void*));
    for (i = 0; i < n_buf; i++) {
        (*buf_ptrs)[i] = (void*)malloc(sizeof(void*));
    }
    // TODO: free buf_ptrs
    if (metadata_hash_table_g != NULL) {

        n_entry = hg_hash_table_num_entries(metadata_hash_table_g);
        hg_hash_table_iterate(metadata_hash_table_g, &hash_table_iter);


        while (n_entry != 0 && hg_hash_table_iter_has_more(&hash_table_iter)) {
            head = hg_hash_table_iter_next(&hash_table_iter);
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
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
        ret_value = FAIL;
        goto done;
    }

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

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
    metadata.time_step = 0;

    /* printf("==PDC_SERVER[%d]: search with name [%s], hash value %u\n", pdc_server_rank_g, name, hash_key); */
    /* fflush(stdout); */

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", hash_key); */
        lookup_value = hg_hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            /* printf("==PDC_SERVER: PDC_Server_search_with_name_hash(): lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            /* PDC_print_metadata(lookup_value->metadata); */
            /* if (lookup_value->bloom == NULL) { */
            /*     printf("bloom is NULL\n"); */
            /* } */
            /* fflush(stdout); */
            *out = find_identical_metadata(lookup_value, &metadata);

            if (*out == NULL) {
                /* printf("==PDC_SERVER[%d]: Queried object with name [%s] has no full match!\n", pdc_server_rank_g, obj_name); */
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
            // First entry for current hasy_key, init linked list, and insert to hash table
            *out = NULL;
            /* printf("==PDC_SERVER[%d]: Queried name %s hash %u not found in hash table \n", pdc_server_rank_g, name, hash_key); */
            /* fflush(stdout); */
        }

    }
    else {
        printf("metadata_hash_table_g not initilized!\n");
        ret_value = -1;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

int main(int argc, char *argv[])
{
    int port;
    char *tmp_dir;
    perr_t ret;
    
    FUNC_ENTER(NULL);
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_server_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_server_size_g);
#else
    pdc_server_rank_g = 0;
    pdc_server_size_g = 1;
#endif

    /* hg_class_t *hg_class = NULL; */
    /* hg_context_t *hg_context = NULL; */

    is_restart_g = 0;
    port = pdc_server_rank_g % 32 + 7000 ;
    /* printf("rank=%d, port=%d\n", pdc_server_rank_g,port); */

    // Set up tmp dir
    tmp_dir = getenv("PDC_TMPDIR");
    if (tmp_dir == NULL) 
        strcpy(pdc_server_tmp_dir_g, "./pdc_tmp");
    else 
        strcpy(pdc_server_tmp_dir_g, tmp_dir);

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER[%d]: using %s as tmp dir \n", pdc_server_rank_g, pdc_server_tmp_dir_g);
    }

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  start;
    struct timeval  end;
    long long elapsed;
    double server_init_time, all_server_init_time;
    gettimeofday(&start, 0);
#endif

    if (argc > 1) {
        if (strcmp(argv[1], "restart") == 0) 
            is_restart_g = 1;
    }
    ret = PDC_Server_init(port, &hg_class_g, &hg_context_g);
    if (ret != SUCCEED || hg_class_g == NULL || hg_context_g == NULL) {
        printf("Error with Mercury init, exit...\n");
        ret = FAIL;
        goto done;
    }

    // Register RPC
    client_test_connect_register(hg_class_g);
    gen_obj_id_register(hg_class_g);
    close_server_register(hg_class_g);
    /* send_obj_name_marker_register(hg_class_g); */
    metadata_query_register(hg_class_g);
    metadata_delete_register(hg_class_g);
    metadata_delete_by_id_register(hg_class_g);
    metadata_update_register(hg_class_g);
    region_lock_register(hg_class_g);
    //bulk
    query_partial_register(hg_class_g);

    gen_reg_map_notification_register(hg_class_g);
    gen_reg_unmap_notification_register(hg_class_g);
    gen_obj_unmap_notification_register(hg_class_g);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&end, 0);
    elapsed = (end.tv_sec-start.tv_sec)*1000000LL + end.tv_usec-start.tv_usec;
    server_init_time = elapsed / 1000000.0;
#endif


    if (pdc_server_rank_g == 0) {
#ifdef ENABLE_TIMING 
        printf("==PDC_SERVER: total startup time = %.6f\n", server_init_time);
#endif
        printf("==PDC_SERVER: Server ready!\n\n\n");
    }
    fflush(stdout);

#ifdef ENABLE_MULTITHREAD
    PDC_Server_multithread_loop(hg_class_g, hg_context_g);
#else
    PDC_Server_loop(hg_class_g, hg_context_g);
#endif

    /* if (pdc_server_rank_g == 0) { */
    /*     printf("==PDC_SERVER: Work done, finalizing\n"); */
    /*     fflush(stdout); */
    /* } */

    // Finalize 
    HG_Context_destroy(hg_context_g);
    HG_Finalize(hg_class_g);

#ifdef ENABLE_CHECKPOINT
    // TODO: instead of checkpoint at app finalize time, try checkpoint with a time countdown or # of objects
    char checkpoint_file[PATH_MAX];
    sprintf(checkpoint_file, "%s/%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);

    #ifdef ENABLE_TIMING 
    // Timing
    struct timeval  ht_total_start;
    struct timeval  ht_total_end;
    long long ht_total_elapsed;
    double checkpoint_time, all_checkpoint_time;
    gettimeofday(&ht_total_start, 0);
    #endif

    PDC_Server_checkpoint(checkpoint_file);

    #ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&ht_total_end, 0);
    ht_total_elapsed = (ht_total_end.tv_sec-ht_total_start.tv_sec)*1000000LL + ht_total_end.tv_usec-ht_total_start.tv_usec;
    checkpoint_time = ht_total_elapsed / 1000000.0;

        #ifdef ENABLE_MPI
    MPI_Reduce(&checkpoint_time, &all_checkpoint_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        #else
    all_checkpoint_time = checkpoint_time;
        #endif

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: total checkpoint  time = %.6f\n", all_checkpoint_time);
    }

    #endif
#endif

done:
    PDC_Server_finalize();
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: exiting...\n");
        /* printf("==PDC_SERVER: [%d] exiting...\n", pdc_server_rank_g); */
    }

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
