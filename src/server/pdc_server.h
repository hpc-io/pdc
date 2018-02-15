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

#ifndef PDC_SERVER_H
#define PDC_SERVER_H

#include <time.h>

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"

#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"
#include "mercury_list.h"

#include "pdc_client_server_common.h"

#define CREATE_BLOOM_THRESHOLD  64
#define PDC_MAX_OVERLAP_REGION_NUM 8 // max number of supported regions for PDC_Server_get_storage_location_of_region() 
#define PDC_STR_DELIM            7

static hg_atomic_int32_t pdc_num_reg;
extern hg_class_t *hg_class_g;

int PDC_Server_is_contiguous_region_overlap(region_list_t *a, region_list_t *b);
pbool_t region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2);
perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out);
/* perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out); */
perr_t PDC_Meta_Server_buf_map(buf_map_in_t *in, region_buf_map_t *new_buf_map_ptr, hg_handle_t *handle);
perr_t PDC_Meta_Server_buf_unmap(buf_unmap_in_t *in, hg_handle_t *handle);
perr_t PDC_Data_Server_buf_unmap(buf_unmap_in_t *in);
perr_t PDC_Data_Server_region_release(struct buf_map_release_bulk_args *bulk_args, region_lock_out_t *out);
perr_t PDC_Meta_Server_region_release(region_lock_in_t *in, region_lock_out_t *out);
perr_t PDC_Data_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out);
perr_t PDC_Meta_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out);
perr_t PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status);
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out);

perr_t PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts, 
                                            pdc_metadata_t** out);
perr_t PDC_Server_checkpoint(char *filename);
perr_t PDC_Server_restart(char *filename);
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs);
hg_return_t PDC_Server_get_client_addr(const struct hg_cb_info *callback_info);
pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id);
perr_t PDC_Server_get_local_storage_location_of_region(uint64_t obj_id, region_list_t *region,
        uint32_t *n_loc, region_list_t **overlap_region_loc);
perr_t PDC_Server_get_total_str_len(region_list_t** regions, uint32_t n_region, uint32_t *len);
perr_t PDC_Server_serialize_regions_info(region_list_t** regions, uint32_t n_region, void *buf);

perr_t PDC_Server_regions_io(region_list_t *region_list_head, PDC_io_plugin_t plugin);

perr_t PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out);
data_server_region_t *PDC_Server_get_obj_region(pdcid_t obj_id);
region_buf_map_t *PDC_Data_Server_buf_map(const struct hg_info *info, buf_map_in_t *in, region_list_t *request_region, void *data_ptr);
void *PDC_Server_get_region_ptr(pdcid_t obj_id, region_info_transfer_t region);

hg_return_t PDC_Server_work_done_cb(const struct hg_cb_info *callback_info);
hg_return_t PDC_Server_s2s_send_work_done_cb(const struct hg_cb_info *callback_info);
hg_return_t PDC_Server_s2s_recv_work_done_cb(const struct hg_cb_info *callback_info);
hg_return_t PDC_Server_count_write_check_update_storage_meta_cb(const struct hg_cb_info *callback_info);
/* typedef struct pdc_metadata_name_mark_t { */
/*     char obj_name[ADDR_MAX]; */
/*     struct pdc_metadata_name_mark_t *next; */
/*     struct pdc_metadata_name_mark_t *prev; */
/* } pdc_metadata_name_mark_t; */

typedef struct pdc_hash_table_entry_head {
    int n_obj;
    void *bloom;
    pdc_metadata_t *metadata;
} pdc_hash_table_entry_head;

/* 
 * Data server related
 */

typedef struct server_lookup_args_t {
    int             server_id;
    uint32_t        client_id;
    int             ret_int;
    char            *ret_string;
    void            *void_buf;
    char            *server_addr;
    pdc_metadata_t  *meta;
    region_list_t   **region_lists;
    uint32_t        n_loc;
} server_lookup_args_t;

struct transfer_buf_map {
    hg_handle_t     handle;
    buf_map_in_t    in;
};

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

typedef struct pdc_client_info_t {
    char            addr_string[ADDR_MAX];
    int             addr_valid;
    hg_addr_t       addr;
} pdc_client_info_t;
 
typedef struct pdc_remote_server_info_t {
    char            *addr_string;
    int             addr_valid;
    hg_addr_t       addr;
} pdc_remote_server_info_t;
 
extern hg_thread_mutex_t pdc_client_connect_mutex_g;

typedef struct pdc_data_server_io_list_t {
    uint64_t obj_id;
    char  path[ADDR_MAX];
    char  bb_path[ADDR_MAX];
    int   total;
    int   count;
    int   ndim;
    uint64_t dims[DIM_MAX];
    uint64_t total_size;
    region_list_t *region_list_head;
    int   is_io_done;
    int   is_shm_closed;

    struct pdc_data_server_io_list_t *prev;
    struct pdc_data_server_io_list_t *next;
} pdc_data_server_io_list_t;

#define PDC_BULK_XFER_INIT_NALLOC 128
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

    struct update_storage_meta_list_t *prev;
    struct update_storage_meta_list_t *next;
} update_storage_meta_list_t;



perr_t PDC_Server_unserialize_regions_info(void *buf, region_list_t** regions, uint32_t *n_region);
hg_return_t PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info);

hg_return_t PDC_Server_count_write_check_update_storage_meta_cb (const struct hg_cb_info *callback_info);
perr_t PDC_Server_data_write_out(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_read_in(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_write_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_read_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_SERVER_notify_region_update_to_client(uint64_t meta_id, uint64_t reg_id, int32_t client_id);

perr_t PDC_Server_read_check(data_server_read_check_in_t *in, data_server_read_check_out_t *out);
perr_t PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out);

perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id);
perr_t PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta);

perr_t PDC_Server_posix_one_file_io(region_list_t* region);

perr_t PDC_Server_add_region_storage_meta_to_bulk_buf(region_list_t *region, bulk_xfer_data_t *bulk_data);
perr_t PDC_Server_update_region_storage_meta_bulk(bulk_xfer_data_t *bulk_data);
perr_t PDC_Server_update_region_storage_meta_bulk_local(update_region_storage_meta_bulk_t **bulk_ptrs, int cnt);
perr_t PDC_Server_set_close(void);
perr_t PDC_Server_update_region_storage_meta_bulk_mpi(bulk_xfer_data_t *bulk_data);
perr_t PDC_Server_close_shm(region_list_t *region);

#endif /* PDC_SERVER_H */
