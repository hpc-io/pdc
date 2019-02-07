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

#define PDC_MAX_OVERLAP_REGION_NUM 8 // max number of regions for PDC_Server_get_storage_location_of_region() 
#define PDC_BULK_XFER_INIT_NALLOC 128


struct buf_map_server_lookup_args_t {
    int             server_id;
    uint32_t        client_id;
    int             ret_int;
    char            *ret_string;
    void            *void_buf;
    char            *server_addr;
    pdc_metadata_t  *meta;
    region_list_t   **region_lists;
    uint32_t        n_loc;
    struct transfer_buf_map *buf_map_args; 
} buf_map_server_lookup_args_t;

struct obj_map_server_lookup_args_t {
    int             server_id;
    uint32_t        client_id;
    int             ret_int;
    char            *ret_string;
    void            *void_buf;
    char            *server_addr;
    pdc_metadata_t  *meta;
    region_list_t   **region_lists;
    uint32_t        n_loc;
    struct transfer_obj_map *obj_map_args;
} obj_map_server_lookup_args_t;

struct transfer_buf_unmap {
    hg_handle_t     handle;
    buf_unmap_in_t  in;
};

struct buf_unmap_server_lookup_args_t {
    int             server_id;
    uint32_t        client_id;
    int             ret_int;
    char            *ret_string;
    void            *void_buf;
    char            *server_addr;
    pdc_metadata_t  *meta;
    region_list_t   **region_lists;
    uint32_t        n_loc;
    struct transfer_buf_unmap *buf_unmap_args;
} buf_unmap_server_lookup_args_t;

struct obj_unmap_server_lookup_args_t {
    int             server_id;
    uint32_t        client_id;
    int             ret_int;
    char            *ret_string;
    void            *void_buf;
    char            *server_addr;
    pdc_metadata_t  *meta;
    region_list_t   **region_lists;
    uint32_t        n_loc;
    struct transfer_obj_unmap *obj_unmap_args;
} obj_unmap_server_lookup_args_t;

typedef struct server_reg_lock_args_t{
    int lock;
} server_reg_lock_args_t;

struct server_region_update_args {
    int             ret;
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
    hg_handle_t      handle;
    region_lock_in_t in;
    int              ret;
    struct buf_map_release_bulk_args *bulk_args;
};

struct transfer_buf_map_args {
    hg_handle_t     handle;
    buf_map_in_t    in;
    int             ret;
};

struct transfer_buf_unmap_args {
    hg_handle_t     handle;
    buf_unmap_in_t  in;
    int             ret;
};

typedef struct notify_multi_io_args_t {
    hg_bulk_t bulk_handle;
    void *buf_sizes;
    void *buf_ptrs;
    region_list_t *region_list;
} notify_multi_io_args_t;


typedef struct bulk_xfer_data_t {
    void     **buf_ptrs;
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
    int is_updated;

    struct update_storage_meta_list_t *prev;
    struct update_storage_meta_list_t *next;
} update_storage_meta_list_t;

typedef struct storage_meta_query_one_name_args_t storage_meta_query_one_name_args_t;

typedef struct accumulate_storage_meta_t {
    int      client_id;
    int      client_seq_id;
    int      n_total;
    int      n_accumulated;
    storage_meta_query_one_name_args_t **storage_meta;
} accumulate_storage_meta_t;

typedef struct storage_meta_query_one_name_args_t{
    char     *name;
    int      n_res;
    int      seq_id;
    region_list_t *req_region;
    region_list_t *overlap_storage_region_list;
    perr_t   (*cb)();
    void     *cb_args;
    accumulate_storage_meta_t *accu_meta;
} storage_meta_query_one_name_args_t;

typedef struct server_read_check_out_t {
    int              ret;
    int              is_cache_to_bb;
    region_list_t   *region;
    char            *shm_addr;
} server_read_check_out_t; 

extern int    pdc_server_rank_g;
extern int    pdc_server_size_g;
extern char   pdc_server_tmp_dir_g[ADDR_MAX];
extern double server_write_time_g                  ;
extern double server_read_time_g                   ;
extern double server_get_storage_info_time_g       ;
extern double server_fopen_time_g                  ;
extern double server_fsync_time_g                  ;
extern double server_total_io_time_g               ;
extern double fread_total_MB                       ;
extern double fwrite_total_MB                      ;
extern double server_update_region_location_time_g ;
extern double server_io_elapsed_time_g             ;
extern int    n_fwrite_g                           ;
extern int    n_fread_g                            ;
extern int    n_fopen_g                            ;
extern int    n_get_remote_storage_meta_g          ;
extern int    update_remote_region_count_g         ;
extern int    update_local_region_count_g          ;
extern int    pdc_nost_per_file_g                  ;
extern int    nclient_per_node                     ;
extern int    write_to_bb_percentage_g             ;
extern int pdc_buffered_bulk_update_total_g        ;
extern int pdc_nbuffered_bulk_update_g             ;
extern int n_check_write_finish_returned_g         ;
extern int buffer_read_request_total_g             ;
extern int buffer_write_request_total_g            ;
extern int buffer_write_request_num_g              ;
extern int buffer_read_request_num_g               ;
extern int is_server_direct_io_g                   ;
extern pdc_task_list_t *pdc_server_agg_task_head_g ;
extern pdc_task_list_t *pdc_server_s2s_task_head_g ;
extern int              pdc_server_task_id_g       ;
extern hg_class_t   *hg_class_g;
extern hg_context_t *hg_context_g;
extern pdc_remote_server_info_t *pdc_remote_server_info_g;
extern int is_debug_g;
extern int n_read_from_bb_g;
extern int read_from_bb_size_g;

extern pdc_data_server_io_list_t  *pdc_data_server_read_list_head_g   ;
extern pdc_data_server_io_list_t  *pdc_data_server_write_list_head_g  ;
extern update_storage_meta_list_t *pdc_update_storage_meta_list_head_g;
extern pdc_client_info_t        *pdc_client_info_g;
extern int pdc_client_num_g;
extern double total_mem_usage_g;

extern hg_id_t get_remote_metadata_register_id_g;
extern hg_id_t buf_map_server_register_id_g;
extern hg_id_t buf_unmap_server_register_id_g;
extern hg_id_t server_lookup_client_register_id_g;
extern hg_id_t server_lookup_remote_server_register_id_g;
extern hg_id_t notify_io_complete_register_id_g;
extern hg_id_t update_region_loc_register_id_g;
extern hg_id_t notify_region_update_register_id_g;
extern hg_id_t get_reg_lock_register_id_g;
extern hg_id_t get_storage_info_register_id_g;
extern hg_id_t bulk_rpc_register_id_g;
extern hg_id_t storage_meta_name_query_register_id_g;
extern hg_id_t get_storage_meta_name_query_bulk_result_rpc_register_id_g;
extern hg_id_t notify_client_multi_io_complete_rpc_register_id_g;




perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out);
perr_t PDC_Meta_Server_buf_map(buf_map_in_t *in, region_buf_map_t *new_buf_map_ptr, hg_handle_t *handle);
perr_t PDC_Meta_Server_buf_unmap(buf_unmap_in_t *in, hg_handle_t *handle);

perr_t PDC_Data_Server_buf_unmap(const struct hg_info *info, buf_unmap_in_t *in);
perr_t PDC_Data_Server_region_release(region_lock_in_t *in, region_lock_out_t *out);
perr_t PDC_Data_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out, hg_handle_t *handle);
perr_t PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status);
perr_t PDC_Server_get_local_storage_location_of_region(uint64_t obj_id, region_list_t *region,
        uint32_t *n_loc, region_list_t **overlap_region_loc);
perr_t PDC_Server_regions_io(region_list_t *region_list_head, PDC_io_plugin_t plugin);
data_server_region_t *PDC_Server_get_obj_region(pdcid_t obj_id);
region_buf_map_t *PDC_Data_Server_buf_map(const struct hg_info *info, buf_map_in_t *in, region_list_t *request_region, void *data_ptr);
void *PDC_Server_get_region_buf_ptr(pdcid_t obj_id, region_info_transfer_t region);
hg_return_t PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info);
hg_return_t PDC_Server_count_write_check_update_storage_meta_cb (const struct hg_cb_info *callback_info);
perr_t PDC_Server_data_write_out(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_read_from(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_read_in(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_write_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_read_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_SERVER_notify_region_update_to_client(uint64_t meta_id, uint64_t reg_id, int32_t client_id);

perr_t PDC_Server_read_check(data_server_read_check_in_t *in, server_read_check_out_t *out);
perr_t PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out);

perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id, int type);
perr_t PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta);

perr_t PDC_Server_posix_one_file_io(region_list_t* region);

perr_t PDC_Server_add_region_storage_meta_to_bulk_buf(region_list_t *region, bulk_xfer_data_t *bulk_data);
perr_t PDC_Server_update_region_storage_meta_bulk(bulk_xfer_data_t *bulk_data);
perr_t PDC_Server_update_region_storage_meta_bulk_local(update_region_storage_meta_bulk_t **bulk_ptrs, int cnt);

hg_return_t PDC_Server_query_read_names_cb(const struct hg_cb_info *callback_info);
perr_t PDC_Server_update_region_storage_meta_bulk_mpi(bulk_xfer_data_t *bulk_data);
perr_t PDC_Server_close_shm(region_list_t *region, int is_remove);

perr_t PDC_Server_update_region_storage_meta_bulk_with_cb(bulk_xfer_data_t *bulk_data, perr_t (*cb)(), update_storage_meta_list_t *meta_list_target, int *n_updated);

hg_return_t PDC_Server_storage_meta_name_query_bulk_respond(const struct hg_cb_info *callback_info);
perr_t PDC_Server_proc_storage_meta_bulk(int task_id, int n_regions, region_list_t *region_list_head);
perr_t PDC_Server_set_lustre_stripe(const char *path, int stripe_count, int stripe_size_MB);
hg_return_t PDC_Server_bulk_cleanup_cb(const struct hg_cb_info *callback_info);

perr_t PDC_Server_notify_client_multi_io_complete(uint32_t client_id, int client_seq_id, int n_completed, 
                                                  region_list_t *completed_region_list);
perr_t PDC_Server_release_lock_request(uint64_t obj_id, struct PDC_region_info *region);

perr_t PDC_Server_add_client_shm_to_cache(int origin, int cnt, void *buf_cp);

perr_t PDC_Close_cache_file();

hg_return_t PDC_cache_region_to_bb_cb (const struct hg_cb_info *callback_info) ;

perr_t PDC_sample_min_max(PDC_var_type_t dtype, uint64_t n, void *data, double sample_pct, double *min, double *max);

pdc_histogram_t *PDC_gen_hist(PDC_var_type_t dtype, uint64_t n, void *data);

void PDC_free_hist(pdc_histogram_t *hist);

pdc_histogram_t *PDC_create_hist(PDC_var_type_t dtype, int nbin, double min, double max);

perr_t PDC_hist_incr_all(pdc_histogram_t *hist, PDC_var_type_t dtype, uint64_t n, void *data);

void PDC_print_hist(pdc_histogram_t *hist);


#endif /* PDC_SERVER_DATA_H */

