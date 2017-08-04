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
#define PDC_MAX_OVERLAP_REGION_NUM 128 // max number of supported regions for PDC_Server_get_storage_location_of_region() 
#define PDC_STR_DELIM            7

static pdc_cnt_t pdc_num_reg;
extern hg_class_t *hg_class_g;


perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out);
/* perr_t insert_obj_name_marker(send_obj_name_marker_in_t *in, send_obj_name_marker_out_t *out); */
perr_t PDC_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out);
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out);
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
} server_lookup_args_t;

typedef struct pdc_client_info_t {
    char            addr_string[ADDR_MAX];
    int             addr_valid;
    hg_addr_t       addr;
    int             server_lookup_client_handle_valid;
    hg_handle_t     server_lookup_client_handle;
    int             notify_io_complete_handle_valid;
    hg_handle_t     notify_io_complete_handle;
    int             notify_region_update_handle_valid;
    hg_handle_t     notify_region_update_handle;
} pdc_client_info_t;
 
typedef struct pdc_remote_server_info_t {
    char            *addr_string;
    int             addr_valid;
    hg_addr_t       addr;
    int             server_lookup_remote_server_handle_valid;
    hg_handle_t     server_lookup_remote_server_handle;
    int             update_region_loc_handle_valid;
    hg_handle_t     update_region_loc_handle;
    int             get_metadata_by_id_handle_valid;
    hg_handle_t     get_metadata_by_id_handle;
    int             get_storage_info_handle_valid;
    hg_handle_t     get_storage_info_handle;
} pdc_remote_server_info_t;
 
extern hg_thread_mutex_t pdc_client_connect_mutex_g;

typedef struct pdc_data_server_io_list_t {
    uint64_t obj_id;
    char  path[ADDR_MAX];
    int   total;
    int   count;
    int   ndim;
    int   dims[DIM_MAX];
    uint64_t total_size;
    region_list_t *region_list_head;

    struct pdc_data_server_io_list_t *prev;
    struct pdc_data_server_io_list_t *next;
} pdc_data_server_io_list_t;


perr_t PDC_Server_unserialize_regions_info(void *buf, region_list_t** regions, uint32_t *n_region);
hg_return_t PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info);

perr_t PDC_Server_data_write_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);
perr_t PDC_Server_data_read_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf);

perr_t PDC_Server_read_check(data_server_read_check_in_t *in, data_server_read_check_out_t *out);
perr_t PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out);

perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region);
perr_t PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta);

perr_t PDC_Server_posix_one_file_io(region_list_t* region);

#endif /* PDC_SERVER_H */
