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

#ifndef PDC_SERVER_DATA_H
#define PDC_SERVER_DATA_H

#include "pdc_server_common.h"
#include "pdc_client_server_common.h"
#include "pdc_query.h"
#include <sys/time.h>
#include <pthread.h>

#ifdef ENABLE_FASTBIT
#include "iapi.h"
#endif

#define PDC_MAX_OVERLAP_REGION_NUM 8 // max number of regions for PDC_Server_get_storage_location_of_region()
#define PDC_BULK_XFER_INIT_NALLOC  128

/***************************/
/* Library Private Structs */
/***************************/
struct buf_map_server_lookup_args_t {
    int                      server_id;
    uint32_t                 client_id;
    int                      ret_int;
    char *                   ret_string;
    void *                   void_buf;
    char *                   server_addr;
    pdc_metadata_t *         meta;
    region_list_t **         region_lists;
    uint32_t                 n_loc;
    struct transfer_buf_map *buf_map_args;
} buf_map_server_lookup_args_t;

struct obj_map_server_lookup_args_t {
    int                      server_id;
    uint32_t                 client_id;
    int                      ret_int;
    char *                   ret_string;
    void *                   void_buf;
    char *                   server_addr;
    pdc_metadata_t *         meta;
    region_list_t **         region_lists;
    uint32_t                 n_loc;
    struct transfer_obj_map *obj_map_args;
} obj_map_server_lookup_args_t;

struct transfer_buf_unmap {
    hg_handle_t    handle;
    buf_unmap_in_t in;
};

struct buf_unmap_server_lookup_args_t {
    int                        server_id;
    uint32_t                   client_id;
    int                        ret_int;
    char *                     ret_string;
    void *                     void_buf;
    char *                     server_addr;
    pdc_metadata_t *           meta;
    region_list_t **           region_lists;
    uint32_t                   n_loc;
    struct transfer_buf_unmap *buf_unmap_args;
} buf_unmap_server_lookup_args_t;

struct obj_unmap_server_lookup_args_t {
    int                        server_id;
    uint32_t                   client_id;
    int                        ret_int;
    char *                     ret_string;
    void *                     void_buf;
    char *                     server_addr;
    pdc_metadata_t *           meta;
    region_list_t **           region_lists;
    uint32_t                   n_loc;
    struct transfer_obj_unmap *obj_unmap_args;
} obj_unmap_server_lookup_args_t;

struct server_region_update_args {
    int ret;
};

struct get_remote_metadata_arg {
    pdc_metadata_t *data;
};

struct transfer_lock_args {
    hg_handle_t      handle;
    region_lock_in_t in;
    int              ret;
};

struct transfer_unlock_args {
    hg_handle_t                       handle;
    region_lock_in_t                  in;
    int                               ret;
    struct buf_map_release_bulk_args *bulk_args;
};

struct transfer_buf_map_args {
    hg_handle_t  handle;
    buf_map_in_t in;
    int          ret;
};

struct transfer_buf_unmap_args {
    hg_handle_t    handle;
    buf_unmap_in_t in;
    int            ret;
};

/****************************/
/* Library Private Typedefs */
/****************************/
typedef struct notify_multi_io_args_t {
    hg_bulk_t      bulk_handle;
    void *         buf_sizes;
    void *         buf_ptrs;
    region_list_t *region_list;
} notify_multi_io_args_t;

typedef struct bulk_xfer_data_t {
    void **    buf_ptrs;
    hg_size_t *buf_sizes;
    int        n_alloc;
    int        idx;
    uint64_t   obj_id;
    int        target_id;
    int        origin_id;
} bulk_xfer_data_t;

// Linked list used to defer bulk update
typedef struct update_storage_meta_list_t {
    bulk_xfer_data_t *storage_meta_bulk_xfer_data;
    int               is_updated;

    struct update_storage_meta_list_t *prev;
    struct update_storage_meta_list_t *next;
} update_storage_meta_list_t;

typedef struct storage_meta_query_one_name_args_t storage_meta_query_one_name_args_t;

typedef struct accumulate_storage_meta_t {
    int                                  client_id;
    int                                  client_seq_id;
    int                                  n_total;
    int                                  n_accumulated;
    storage_meta_query_one_name_args_t **storage_meta;
} accumulate_storage_meta_t;

typedef struct storage_meta_query_one_name_args_t {
    char *         name;
    int            n_res;
    int            seq_id;
    region_list_t *req_region;
    region_list_t *overlap_storage_region_list;
    perr_t (*cb)();
    void *                     cb_args;
    accumulate_storage_meta_t *accu_meta;
} storage_meta_query_one_name_args_t;

typedef struct server_read_check_out_t {
    int            ret;
    int            is_cache_to_bb;
    region_list_t *region;
    char *         shm_addr;
} server_read_check_out_t;

// Data query
typedef struct query_task_t {
    pdc_query_t *      query;
    int                query_id;
    int                manager;
    int                client_id;
    int                n_sent_server;
    int                n_unique_obj;
    uint64_t *         obj_ids;
    int                n_recv_obj;
    int                ndim;
    pdc_query_get_op_t get_op;
    region_list_t *    region_constraint;
    uint64_t           total_elem;
    int *              invalid_region_ids;
    int                ninvalid_region;
    int                prev_server_id;
    int                next_server_id;

    // Result
    int        is_done;
    int        n_recv;
    uint64_t   nhits;
    uint64_t * coords;
    uint64_t **coords_arr;
    uint64_t * n_hits_from_server;

    // Data read
    int       n_read_data_region;
    void **   data_arr;
    uint64_t *my_read_coords;
    uint64_t  my_nread_coords;
    uint64_t  my_read_obj_id;
    void *    my_data;
    int       client_seq_id;

    struct query_task_t *prev;
    struct query_task_t *next;
} query_task_t;

typedef struct cache_storage_region_t {
    uint64_t       obj_id;
    pdc_var_type_t data_type;
    region_list_t *storage_region_head;

    struct cache_storage_region_t *prev;
    struct cache_storage_region_t *next;
} cache_storage_region_t;

/*****************************/
/* Library-private Variables */
/*****************************/
extern int                       pdc_server_rank_g;
extern int                       pdc_server_size_g;
extern char                      pdc_server_tmp_dir_g[ADDR_MAX];
extern double                    server_write_time_g;
extern double                    server_read_time_g;
extern double                    server_get_storage_info_time_g;
extern double                    server_fopen_time_g;
extern double                    server_fsync_time_g;
extern double                    server_total_io_time_g;
extern double                    fread_total_MB;
extern double                    fwrite_total_MB;
extern double                    server_update_region_location_time_g;
extern double                    server_io_elapsed_time_g;
extern int                       n_fwrite_g;
extern int                       n_fread_g;
extern int                       n_fopen_g;
extern int                       n_get_remote_storage_meta_g;
extern int                       update_remote_region_count_g;
extern int                       update_local_region_count_g;
extern int                       pdc_nost_per_file_g;
extern int                       nclient_per_node;
extern int                       write_to_bb_percentage_g;
extern int                       pdc_buffered_bulk_update_total_g;
extern int                       pdc_nbuffered_bulk_update_g;
extern int                       n_check_write_finish_returned_g;
extern int                       buffer_read_request_total_g;
extern int                       buffer_write_request_total_g;
extern int                       buffer_write_request_num_g;
extern int                       buffer_read_request_num_g;
extern int                       is_server_direct_io_g;
extern pdc_task_list_t *         pdc_server_agg_task_head_g;
extern pdc_task_list_t *         pdc_server_s2s_task_head_g;
extern int                       pdc_server_task_id_g;
extern hg_class_t *              hg_class_g;
extern hg_context_t *            hg_context_g;
extern pdc_remote_server_info_t *pdc_remote_server_info_g;
extern int                       is_debug_g;
extern int                       n_read_from_bb_g;
extern int                       read_from_bb_size_g;
extern int                       gen_hist_g;

extern pdc_data_server_io_list_t * pdc_data_server_read_list_head_g;
extern pdc_data_server_io_list_t * pdc_data_server_write_list_head_g;
extern update_storage_meta_list_t *pdc_update_storage_meta_list_head_g;
extern pdc_client_info_t *         pdc_client_info_g;
extern int                         pdc_client_num_g;
extern double                      total_mem_usage_g;
extern int                         lustre_stripe_size_mb_g;

extern hg_id_t get_remote_metadata_register_id_g;
extern hg_id_t buf_map_server_register_id_g;
extern hg_id_t buf_unmap_server_register_id_g;
extern hg_id_t server_lookup_client_register_id_g;
extern hg_id_t server_lookup_remote_server_register_id_g;
extern hg_id_t notify_io_complete_register_id_g;
extern hg_id_t update_region_loc_register_id_g;
extern hg_id_t notify_region_update_register_id_g;
extern hg_id_t get_storage_info_register_id_g;
extern hg_id_t bulk_rpc_register_id_g;
extern hg_id_t storage_meta_name_query_register_id_g;
extern hg_id_t get_storage_meta_name_query_bulk_result_rpc_register_id_g;
extern hg_id_t notify_client_multi_io_complete_rpc_register_id_g;
extern hg_id_t send_data_query_region_register_id_g;
extern hg_id_t send_read_sel_obj_id_rpc_register_id_g;
extern hg_id_t send_nhits_register_id_g;
extern hg_id_t send_bulk_rpc_register_id_g;
extern char *  gBinningOption;
extern int     gen_fastbit_idx_g;
extern int     use_fastbit_idx_g;

#define PDC_SERVER_CACHE

#ifdef PDC_SERVER_CACHE
typedef struct pdc_region_cache {
    struct pdc_region_info * region_cache_info;
    struct pdc_region_cache *next;
} pdc_region_cache;

typedef struct pdc_obj_cache {
    struct pdc_obj_cache *next;
    uint64_t              obj_id;
    pdc_region_cache *    region_cache;
    pdc_region_cache *    region_cache_end;
    struct timeval        timestamp;
} pdc_obj_cache;

#define PDC_REGION_CONTAINED       0
#define PDC_REGION_CONTAINED_BY    1
#define PDC_REGION_PARTIAL_OVERLAP 2
#define PDC_REGION_NO_OVERLAP      3
#define PDC_MERGE_FAILED           4
#define PDC_MERGE_SUCCESS          5

pdc_obj_cache *obj_cache_list, *obj_cache_list_end;

hg_thread_mutex_t pdc_obj_cache_list_mutex;
pthread_t         pdc_recycle_thread;
pthread_mutex_t   pdc_cache_mutex;
int               pdc_recycle_close_flag;

int   PDC_region_cache_flush_all();
int   PDC_region_fetch(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit);
int   PDC_region_cache_register(uint64_t obj_id, const char *buf, size_t buf_size, const uint64_t *offset,
                                const uint64_t *size, int ndim, size_t unit);
void *PDC_region_cache_clock_cycle(void *ptr);
#endif

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * Data server process buffer map
 *
 * \param in [IN]               Info struct
 * \param in [IN]               In struct
 * \param request_region [IN]   Region to map
 * \param data_ptr [IN]         Data pointer to memory in the server
 *
 * \return Non-negative on success/Negative on failure
 */
region_buf_map_t *PDC_Data_Server_buf_map(const struct hg_info *info, buf_map_in_t *in,
                                          region_list_t *request_region, void *data_ptr);

/**
 * Data server process buffer unmap
 *
 * \param in [IN]               Info struct
 * \param new_buf_map_ptr [IN]  Pointer to buf unmap struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Data_Server_buf_unmap(const struct hg_info *info, buf_unmap_in_t *in);

/**
 * Server checks if unmap is done
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Data_Server_check_unmap();

/**
 * Region release process in data server
 *
 * \param in [IN]               Region lock input struct
 * \param out [IN]              Region lock output struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Data_Server_region_release(region_lock_in_t *in, region_lock_out_t *out);

/**
 * Lock a reigon
 *
 * \param in [IN]               Lock region information received from the client
 * \param out [IN]              Output stucture to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Data_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out, hg_handle_t *handle);

/**
 * Check region lock status
 *
 * \param mapped_region [IN]    Region struct
 * \param lock_status [OUT]     Region is locked or released
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status);

/**
 * Insert the metdata received from client to the hash table
 *
 * \param obj_id [IN]           Object ID of the request
 * \param region [IN]           Request region
 * \param n_loc [OUT]           Number of storage locations of the target region
 * \param overlap_rg_loc [OUT]  List of region locations
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_local_storage_location_of_region(uint64_t obj_id, region_list_t *region,
                                                       uint32_t *n_loc, region_list_t **overlap_rg_loc);

/**
 * Perform the IO request with different IO system
 * after the server has accumulated requests from all node local clients
 *
 * \param region_list_head [IN] List of IO requests
 * \param plugin [IN]           IO system to be used
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_regions_io(region_list_t *region_list_head, _pdc_io_plugin_t plugin);

/**
 * Server retrieves region struct by object ID
 *
 * \param obj_id [IN]           Object ID
 *
 * \return Region struct/NULL on failure
 */
data_server_region_t *PDC_Server_get_obj_region(pdcid_t obj_id);

/**
 * ***********
 *
 * \param obj_id [IN]           Object ID
 * \param region [IN]           Region
 * \param type_size [IN]        Size of data type
 */
void *PDC_Server_maybe_allocate_region_buf_ptr(pdcid_t obj_id, region_info_transfer_t region,
                                               size_t type_size);

/**
 * Server retrieves data pointer of the region
 *
 * \param obj_id [IN]           Object ID
 * \param region [IN]           Region
 */
void *PDC_Server_get_region_buf_ptr(pdcid_t obj_id, region_info_transfer_t region);

/**
 * Perform the IO request via shared memory
 *
 * \param callback_info[IN]     Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info);

/**
 * Callback function for get storage info.
 *
 * \param callback_info[IN]         Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_count_write_check_update_storage_meta_cb(const struct hg_cb_info *callback_info);

/**
 * Write data out to desired storage
 *
 * \param obj_id [IN]           Object ID
 * \param region_info [IN]      Region information
 * \param buf [IN]              Data staring address
 * \param unit [IN]             Size of data type
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_write_out(uint64_t obj_id, struct pdc_region_info *region_info, void *buf,
                                 size_t unit);

/**
 * Read data from desired storage
 *
 * \param obj_id [IN]           Object ID
 * \param region_info [IN]      Region information
 * \param buf [IN]              Data starting address
 * \param unit [IN]             Size of data type
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_read_from(uint64_t obj_id, struct pdc_region_info *region_info, void *buf,
                                 size_t unit);

/**
 * Read data from desired storage
 *
 * \param obj_id [IN]           Object ID
 * \param region_info [IN]      Region information
 * \param buf [IN]              Data staring address
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_read_in(uint64_t obj_id, struct pdc_region_info *region_info, void *buf);

/**
 * Server writes buffer to storage of one region without client involvement
 * Read with POSIX within one file
 *
 * \param obj_id [IN]           Object ID
 * \param region_info [IN]      Region info of IO request
 * \param buf [IN]              Data buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_write_direct(uint64_t obj_id, struct pdc_region_info *region_info, void *buf);

/**
 * Server reads buffer from storage of one region without client involvement
 *
 * \param obj_id [IN]           Object ID
 * \param region_info [IN]      Region info of IO request
 * \param buf [OUT]             Data buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_read_direct(uint64_t obj_id, struct pdc_region_info *region_info, void *buf);

/**
 * Callback function for the notify region update
 *
 * \param meta_id [IN]          Metadata ID
 * \param reg_id [IN]           Region ID
 * \param client_id [IN]        Client's MPI rank
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_notify_region_update_to_client(uint64_t meta_id, uint64_t reg_id, int32_t client_id);

/**
 * Check if a previous read request has been completed
 *
 * \param in [IN]               Input structure received from client containing the IO request info
 * \param out [IN]              Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_read_check(data_server_read_check_in_t *in, server_read_check_out_t *out);

/**
 * Check if a previous write request has been completed
 *
 * \param in [IN]               Input structure received from client containing the IO request info
 * \param out [IN]              Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out);

/**
 * Update the storage location information of the corresponding metadata that is stored locally
 *
 * \param region[IN]            Region info of the data that's been written by server
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id, int type);

/**
 * Get metadata of the object ID received from client from local metadata hash table
 *
 * \param obj_id[IN]            Object ID
 * \param res_meta_ptrdata[OUT] Pointer of metadata of the specified object ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta);

/**
 * Server writes buffer to storage with POSIX within one file
 *
 * \param region[IN]            Region info of the data that's been written by server
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_posix_one_file_io(region_list_t *region);

/**
 * Update the storage location information of the corresponding metadata that may be stored in a
 * remote server, using Mercury bulk transfer.
 *
 * \param region_ptrs[IN]       Pointers to the regions that need to be updated
 * \param cnt [IN]              Number of pointers in region_ptrs
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_add_region_storage_meta_to_bulk_buf(region_list_t *region, bulk_xfer_data_t *bulk_data);

/**
 * Insert the metdata received from client to the hash table
 *
 * \param in [IN]               Input structure received from client, conatins metadata
 * \param out [IN]              Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storage_meta_bulk(bulk_xfer_data_t *bulk_data);

/**
 * Callback function for server to update the metadata stored locally,
 * can be called by the server itself, or through bulk transfer callback.
 *
 * \param bulk_ptrs [IN]        Region meta struct
 * \param cnt [IN]              Count
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storage_meta_bulk_local(update_region_storage_meta_bulk_t **bulk_ptrs,
                                                        int                                 cnt);

/**
 * Query and read entire objects with a list of object names, received from a client
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_query_read_names_cb(const struct hg_cb_info *callback_info);

/**
 * MPI VERSION. Update the storage location information of the corresponding metadata that may be stored in a
 * remote server, using Mercury bulk transfer.
 *
 * \param bulk_data[IN]         Bulk data ptr, obtained with PDC_Server_add_region_storage_meta_to_bulk_buf
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storage_meta_bulk_mpi(bulk_xfer_data_t *bulk_data);

/**
 * Close the shared memory
 *
 * \param region [IN]           Pointer to region
 * \param is_remove [OUT]       Region is removed or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_close_shm(region_list_t *region, int is_remove);

/**
 * Update the storage location information of the corresponding metadata that may be stored in a
 * remote server, using Mercury bulk transfer.
 *
 * \param bulk_data [IN]        Bulk data ptr, obtained with PDC_Server_add_region_storage_meta_to_bulk_buf
 * \param cb [IN]               Callback function pointer
 * \param meta_list_target [IN] Storage metadata struct
 * \param bulk_data[IN]         Region metadata updated or not
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storage_meta_bulk_with_cb(bulk_xfer_data_t *bulk_data, perr_t (*cb)(),
                                                          update_storage_meta_list_t *meta_list_target,
                                                          int *                       n_updated);

/**
 * **********
 *

 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_storage_meta_name_query_bulk_respond(const struct hg_cb_info *callback_info);

/**
 * ***********
 *
 * \param task_id [IN]          Task ID
 * \param n_regions [IN]        Number of regions
 * \param region_list_head [IN] Region head
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_proc_storage_meta_bulk(int task_id, int n_regions, region_list_t *region_list_head);

/**
 * Set the Lustre stripe count/size of a given path
 *
 * \param path [IN]             Directory to be set with Lustre stripe/count
 * \param stripe_count [IN]     Stripe count
 * \param stripe_size_MB [IN]   Stripe size in MB
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_set_lustre_stripe(const char *path, int stripe_count, int stripe_size_MB);

/**
 * **********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_bulk_cleanup_cb(const struct hg_cb_info *callback_info);

/**
 * Callback function for IO complete notification send to client
 *
 * \param client_id [IN]        Target client's MPI rank
 * \param client_seq_id [IN]    *****
 * \param n_completed [IN]      *********
 * \param completed_rg_list[IN] ******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_notify_client_multi_io_complete(uint32_t client_id, int client_seq_id, int n_completed,
                                                  region_list_t *completed_rg_list);

/**
 * Data server processes lock release request
 *
 * \param obj_id [IN]           Object ID
 * \param region [IN]           Region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_release_lock_request(uint64_t obj_id, struct pdc_region_info *region);

/**
 * ********
 *
 * \param cnt [IN]              ********
 * \param buf_cp [IN]           ********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_add_client_shm_to_cache(int cnt, void *buf_cp);

/**
 * *******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Close_cache_file();

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_cache_region_to_bb_cb(const struct hg_cb_info *callback_info);

/**
 * *******
 *
 * \param dtype [IN]            Data type
 * \param n [IN]                *******
 * \param data [IN]             *******
 * \param sample_pct [IN]       *******
 * \param min [IN]              *******
 * \param max [IN]              *******
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_sample_min_max(pdc_var_type_t dtype, uint64_t n, void *data, double sample_pct, double *min,
                          double *max);

/**
 * *******
 *
 * \param dtype [IN]            *******
 * \param n [IN]                *******
 * \param data [IN]             *******
 *
 * \return *******
 */
pdc_histogram_t *PDC_gen_hist(pdc_var_type_t dtype, uint64_t n, void *data);

/**
 * ******
 *
 * \param hist [IN]             *********
 */
void PDC_free_hist(pdc_histogram_t *hist);

/**
 * Insert the metdata received from client to the hash table
 *
 * \param dtype [IN]            Data type
 * \param nbin [IN]             ********
 * \param min [IN]              *******
 * \param max [IN]              *******
 *
 * \return ********
 */
pdc_histogram_t *PDC_create_hist(pdc_var_type_t dtype, int nbin, double min, double max);

/**
 * Insert the metdata received from client to the hash table
 *
 * \param hist [IN]             ********
 * \param dtype [IN]            Data type
 * \param n [IN]                ********
 * \param data [IN]             ********
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_hist_incr_all(pdc_histogram_t *hist, pdc_var_type_t dtype, uint64_t n, void *data);

/**
 * ********
 *
 * \param hist [IN]             ********
 */
void PDC_print_hist(pdc_histogram_t *hist);

/**
 * Update the storage location information of the corresponding metadata that may be stored in a
 * remote server.
 *
 * \param region [IN]           Region to update
 * \param type [IN]             Type
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storagelocation_offset(region_list_t *region, int type);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_recv_data_query(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_recv_data_query_region(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_recv_nhits(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_recv_coords(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_recv_query_metadata_bulk(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_recv_get_sel_data(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_recv_read_coords(const struct hg_cb_info *callback_info);

/**
 * ********
 *
 * \param callback_info [IN]    Mercury callback info
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
hg_return_t PDC_Server_recv_read_sel_obj_data(const struct hg_cb_info *callback_info);

#ifdef ENABLE_FASTBIT
/**
 * *******
 *
 * \param ctx [IN]              ********
 * \param start [IN]            ********
 * \param count [IN]            ********
 * \param buf [IN]              ********
 *
 * \return HG_SUCCESS or corresponding HG error code
 */
int PDC_bmreader(void *ctx, uint64_t start, uint64_t count, uint32_t *buf);

/**
 * *******
 *
 * \param idx_name [IN]         ********
 * \param obj_id [IN]           Object ID
 * \param dtype [IN]            Data type
 * \param timestep [IN]         The time step
 * \param ndim [IN]             Number of dimensions
 * \param dims [IN]             Dimension info
 * \param start [IN]            ********
 * \param count [IN]            ********
 * \param keys [IN]             ********
 * \param offsets [IN]          ********
 *
 * \return *******
 */
int PDC_load_fastbit_index(char *idx_name, uint64_t obj_id, FastBitDataType dtype, int timestep,
                           uint64_t ndim, uint64_t *dims, uint64_t *start, uint64_t *count, uint32_t **bms,
                           double **keys, int64_t **offsets);

/**
 * *******
 *
 * \param out [IN]              ********
 * \param prefix [IN]           ********
 * \param obj_id [IN]           Object ID
 * \param timestep [IN]         The time step
 * \param ndim [IN]             Number of dimensions
 * \param start [IN]            ********
 * \param count [IN]            ********
 */
void PDC_gen_fastbit_idx_name(char *out, char *prefix, uint64_t obj_id, int timestep, int ndim,
                              uint64_t *start, uint64_t *count);
#endif

#endif /* PDC_SERVER_DATA_H */
