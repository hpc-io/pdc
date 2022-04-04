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

#ifndef PDC_SERVER_METADATA_H
#define PDC_SERVER_METADATA_H

#include <time.h>

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"
#include "mercury_atomic.h"

#include "pdc_hash-table.h"

#include "pdc_server_common.h"
#include "pdc_client_server_common.h"

#define CREATE_BLOOM_THRESHOLD 64

/*****************************/
/* Library-private Variables */
/*****************************/
extern int           pdc_server_rank_g;
extern int           pdc_server_size_g;
extern char          pdc_server_tmp_dir_g[ADDR_MAX];
extern uint32_t      n_metadata_g;
extern HashTable *   metadata_hash_table_g;
extern HashTable *   container_hash_table_g;
extern hg_class_t *  hg_class_g;
extern hg_context_t *hg_context_g;
extern int           is_debug_g;

extern hg_id_t                   get_metadata_by_id_register_id_g;
extern hg_id_t                   send_client_storage_meta_rpc_register_id_g;
extern pdc_client_info_t *       pdc_client_info_g;
extern pdc_remote_server_info_t *pdc_remote_server_info_g;
extern double                    total_mem_usage_g;
extern int                       is_hash_table_init_g;
extern int                       is_restart_g;

/****************************/
/* Library Private Typedefs */
/****************************/
typedef struct pdc_hash_table_entry_head {
    int             n_obj;
    void *          bloom;
    pdc_metadata_t *metadata;
} pdc_hash_table_entry_head;

typedef struct pdc_cont_hash_table_entry_t {
    uint64_t          cont_id;
    char              cont_name[ADDR_MAX];
    int               n_obj;
    int               n_deleted;
    int               n_allocated;
    uint64_t *        obj_ids;
    char              tags[TAG_LEN_MAX];
    pdc_kvtag_list_t *kvtag_list_head;
} pdc_cont_hash_table_entry_t;

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * Insert the metdata received from client to the hash table
 *
 * \param in [IN]               Input structure received from client, conatins metadata
 * \param out [IN]              Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out);

/**
 * Metadata server process buffer map
 *
 * \param in [IN]               In struct
 * \param new_buf_map_ptr [IN]  Pointer to buf map struct
 * \param handle [IN]           Handle
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Meta_Server_buf_map(buf_map_in_t *in, region_buf_map_t *new_buf_map_ptr, hg_handle_t *handle);

/**
 * Metadata server process buffer unmap
 *
 * \param in [IN]               In struct
 * \param handle [IN]           Handle
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Meta_Server_buf_unmap(buf_unmap_in_t *in, hg_handle_t *handle);

/**
 * Check if two regions are identical
 *
 * \param reg1 [IN]             Region to compare
 * \param reg2 [IN]             Region to compare
 *
 * \return 1 if they are the same/-1 otherwise
 */
pbool_t PDC_region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2);

/**
 * Check if two regions overlap
 *
 * \param a [IN]                Region to check overlap
 * \param b [IN]                Region to check overlap
 *
 * \return 1 if they overlap/-1 otherwise
 */
int PDC_Server_is_contiguous_region_overlap(region_list_t *a, region_list_t *b);

/**
 * Seach the hash table with object name and hash key
 *
 * \param obj_name [IN]         Name of the object to be searched
 * \param hash_key [IN]         Hash value of the name string
 * \param out[OUT]              Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t **out);

/**
 * Seach the hash table with object name and hash key at a timestep
 *
 * \param obj_name [IN]         Name of the object to be searched
 * \param hash_key [IN]         Hash value of the name string
 * \param ts [IN]               Timestep value
 * \param out[OUT]              Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts,
                                            pdc_metadata_t **out);

/**
 * Get the metadata that satisfies the query constraint
 *
 * \param in [IN]               Input structure from client that contains the query constraint
 * \param n_meta [OUT]          Number of metadata that satisfies the query constraint
 * \param buf_ptrs [OUT]        Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta,
                                           void ***buf_ptrs);

/**
 * Get the metadata that satisfies the query constraint
 *
 * \param in [IN]               Input structure from client that contains the query constraint
 * \param n_meta [OUT]          Number of metadata that satisfies the query constraint
 * \param buf_ptrs [OUT]        Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_kvtag_query_result(pdc_kvtag_t *in, uint32_t *n_meta, uint64_t **buf_ptrs);

/**
 * Get the kvtag with the given key
 *
 * \param in [IN]               Input structure from client
 * \param out [OUT]             Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_kvtag(metadata_get_kvtag_in_t *in, metadata_get_kvtag_out_t *out);

/**
 * Delete the kvtag with the given key
 *
 * \param in [IN]               Input structure from client
 * \param out [OUT]             Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_del_kvtag(metadata_get_kvtag_in_t *in, metadata_add_tag_out_t *out);

/**
 * Wrapper function of find_metadata_by_id().
 *
 * \param obj_id [IN]           Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id);

/**
 * Delete metdata with the ID received from a client
 *
 * \param in [IN]               Input structure received from client, conatins object ID
 * \param out [OUT]             Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out);

/**
 * Create a container
 *
 * \param in [IN]               Input structure received from client, conatins metadata
 * \param out [OUT]             Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_create_container(gen_cont_id_in_t *in, gen_cont_id_out_t *out);

/**
 * Search a container by name
 *
 * \param cont_name [IN]        Container name
 * \param hash_key [OUT]        Hash value of container name
 * \param out [OUT]             Pointer to the result container
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_find_container_by_name(const char *cont_name, pdc_cont_hash_table_entry_t **out);

/**
 * Delete objects to a container
 *
 * \param n_obj [IN]            Number of objects to be added/deleted
 * \param obj_ids [IN]          Pointer to object array with n_obj objects
 * \param cont_id [IN]          Container ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_container_del_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id);

/**
 * Delete the kvtag with the given key
 *
 * \param n_obj [IN]            Number of objects to be added
 * \param obj_ids [IN]          Pointer to object array with nobj objects
 * \param cont_id [IN]          Container ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_container_add_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id);

/**
 * Print all existing metadata in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_print_all_metadata();

/**
 * Print all existing container in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_print_all_containers();

/**
 * Check for duplicates in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_metadata_duplicate_check();

/**
 * Init the hash table for metadata storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_init_hash_table();

/**
 * Init a metadata list (doubly linked) under the given hash table entry
 *
 * \param entry [IN]            An entry pointer of the hash table
 * \param hash_key [IN]         Hash key of the entry
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_hash_table_list_init(pdc_hash_table_entry_head *entry, uint32_t *hash_key);

/**
 * Insert a metadata to the metadata hash table
 *
 * \param head [IN]             Head of the hash table
 * \param new [IN]              Metadata pointer to be inserted
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_hash_table_list_insert(pdc_hash_table_entry_head *head, pdc_metadata_t *new);

/**
 * Get the metadata with the specified object ID by iteration of all metadata in the hash table
 *
 * \param obj_id [IN]           Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
pdc_metadata_t *find_metadata_by_id(uint64_t obj_id);

/**
 * Get metadata of the object ID received from client from (possibly remtoe) metadata hash table
 *
 * \param obj_id [IN]           Object ID
 * \param res_meta_ptr[IN/OUT]  Pointer of metadata of the specified object ID
 * \param cb [IN]               Callback function needs to be executed after getting the metadata
 * \param args [IN]             Callback function input parameters
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_metadata_by_id_with_cb(uint64_t obj_id, perr_t (*cb)(), void *args);

/**
 * Check if an object has metadata in current server
 *
 * \param obj_id [IN]           Object ID
 *
 * \return 1 if metadata is stored locally/-1 otherwise
 */
int PDC_Server_has_metadata(pdcid_t obj_id);

/**
 * ******
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_query_read_names_clinet_cb(const struct hg_cb_info *callback_info);

/**
 * Add tags to a container
 *
 * \param cont_id [IN]          Container ID
 * \param tags [IN]             Pointer to the tags string
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_container_add_tags(uint64_t cont_id, char *tags);

/**
 * Free a container from hash table
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_free_cont_hash_table();

/**
 * Add the kvtag received from one client to the corresponding metadata structure
 *
 * \param in [IN]               Input structure received from client
 * \param out [IN]              Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_add_kvtag(metadata_add_kvtag_in_t *in, metadata_add_tag_out_t *out);

#endif /* PDC_SERVER_METADATA_H */
