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

#ifndef PDC_CLIENT_SERVER_COMMON_H
#define PDC_CLIENT_SERVER_COMMON_H

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "pdc_linkedlist.h"
#include "pdc_private.h"
#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"

#include "mercury_thread_pool.h"
#include "mercury_thread_condition.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_list.h"

#include "pdc_obj_pkg.h"
#include "pdc_prop_pkg.h"
#include "pdc_analysis_and_transforms.h"
#include "pdc_analysis_support.h"

#ifdef ENABLE_MULTITHREAD 
hg_thread_mutex_t pdc_client_info_mutex_g;
hg_thread_mutex_t lock_list_mutex_g;
hg_thread_mutex_t meta_buf_map_mutex_g;
hg_thread_mutex_t meta_obj_map_mutex_g;
#endif

#define PAGE_SIZE                           4096
#define ADDR_MAX                            256
#define DIM_MAX                             4
#define TAG_LEN_MAX                         2048
#define PDC_SERVER_ID_INTERVEL              1000000
#define PDC_SERVER_MAX_PROC_PER_NODE        32
#define PDC_SERIALIZE_MAX_SIZE              256
#define PDC_MAX_CORE_PER_NODE               68           // Cori KNL has 68 cores per node, Haswell 32
#define PDC_MAX_TRIAL_NUM                   10
#define PDC_STR_DELIM                       7
#define PDC_SEQ_ID_INIT_VALUE               1000
#define PDC_UPDATE_CACHE                    111
#define PDC_UPDATE_STORAGE                  101


/* #define pdc_server_tmp_dir_g  "./pdc_tmp" */
/* extern char pdc_server_tmp_dir_g[ADDR_MAX]; */
#define pdc_server_cfg_name_g "server.cfg"

#define ADD_OBJ 1
#define DEL_OBJ 2

extern uint64_t pdc_id_seq_g;
extern int pdc_server_rank_g;

#define    PDC_LOCK_OP_OBTAIN  0
#define    PDC_LOCK_OP_RELEASE 1

#define PDC_MAX(x, y) (((x) > (y)) ? (x) : (y))
#define PDC_MIN(x, y) (((x) < (y)) ? (x) : (y))

typedef enum { POSIX=0, DAOS=1 }       PDC_io_plugin_t;

typedef enum { NONE=0, 
               LUSTRE=1, 
               BB=2, 
               MEM=3 
             } PDC_data_loc_t;

typedef struct pdc_metadata_t pdc_metadata_t;
typedef struct region_list_t region_list_t;

typedef struct pdc_kvtag_list_t {
    pdc_kvtag_t *kvtag;

    struct pdc_kvtag_list_t *prev;
    struct pdc_kvtag_list_t *next;
} pdc_kvtag_list_t;

typedef struct get_storage_loc_args_t{
    perr_t        (*cb)();
    void          *cb_args;
    region_list_t **region_lists;
    hg_handle_t    rpc_handle;
    uint32_t      *n_loc;
    void          *void_buf;
} get_storage_loc_args_t;

typedef struct update_region_storage_meta_bulk_args_t{
    perr_t   (*cb)();
    void     *meta_list_target;
    int      *n_updated;
    int       server_id;
    hg_bulk_t bulk_handle;
    hg_handle_t rpc_handle;
} update_region_storage_meta_bulk_args_t;

typedef struct get_metadata_by_id_args_t{
    perr_t          (*cb)();
    void            *args;
} get_metadata_by_id_args_t;

typedef struct region_list_t {
    size_t ndim;
    uint64_t start[DIM_MAX];
    uint64_t count[DIM_MAX];
    /* uint64_t stride[DIM_MAX]; */

    uint32_t client_ids[PDC_SERVER_MAX_PROC_PER_NODE];

    uint64_t data_size;
    int      is_data_ready;
    char     shm_addr[ADDR_MAX];
    int      shm_fd;
    char    *buf;
    PDC_data_loc_t data_loc_type;
    char     storage_location[ADDR_MAX];
    uint64_t offset;
    struct region_list_t *overlap_storage_regions;
    uint32_t n_overlap_storage_region;
    hg_atomic_int32_t      buf_map_refcount;
    int      reg_dirty_from_buf;
    hg_handle_t lock_handle;
    PDC_access_t access_type;
    hg_bulk_t bulk_handle;
    hg_addr_t addr;
    uint64_t  obj_id;
    uint64_t  reg_id;
    uint64_t  from_obj_id;
    int32_t   client_id;
    int       is_io_done;
    int       is_shm_closed;

    char      cache_location[ADDR_MAX];
    uint64_t  cache_offset;

    pdc_metadata_t *meta;

    int    seq_id;
    struct region_list_t *prev;
    struct region_list_t *next;
    // 32 attributes, need to match init and deep_cp routines
} region_list_t;

// Similar structure PDC_region_info_t defined in pdc_obj_pkg.h
// TODO: currently only support upto four dimensions
typedef struct region_info_transfer_t {
    size_t ndim;
    uint64_t start_0, start_1, start_2, start_3;
    uint64_t count_0, count_1, count_2, count_3;
} region_info_transfer_t;

typedef struct pdc_metadata_transfer_t {
    int32_t        user_id;
    const char    *app_name;
    const char    *obj_name;
    int            time_step;

    uint64_t       cont_id;
    uint64_t       obj_id;
    int8_t         data_type;
  
    size_t         ndim;
    uint64_t       dims0, dims1, dims2, dims3;

    const char    *tags;
    const char    *data_location;

    /* time_t      create_time; */
    /* time_t      last_modified_time; */
} pdc_metadata_transfer_t;

typedef struct metadata_query_transfer_in_t{
    int     is_list_all;

    int     user_id;                // Both server and client gets it and do security check
    const char    *app_name;
    const char    *obj_name;

    int     time_step_from;
    int     time_step_to;

    size_t  ndim;

    /* time_t  create_time_from; */
    /* time_t  create_time_to; */
    /* time_t  last_modified_time_from; */
    /* time_t  last_modified_time_to; */

    const char    *tags;
} metadata_query_transfer_in_t;

typedef struct {
    uint64_t                    obj_id;
    char                        shm_addr[ADDR_MAX];
} client_read_info_t;

typedef struct {
    int32_t client_id;
    int32_t nclient;
    char client_addr[ADDR_MAX];
} client_test_connect_args;

typedef struct PDC_mapping_info {
    pdcid_t                          remote_obj_id;         /* target of object id */
    pdcid_t                          remote_reg_id;         /* target of region id */
    int32_t                          remote_client_id;
    size_t                           remote_ndim;
    region_info_transfer_t           remote_region;
    hg_bulk_t                        remote_bulk_handle;
    hg_addr_t                        remote_addr;
    pdcid_t                          from_obj_id;
    PDC_LIST_ENTRY(PDC_mapping_info) entry;
} PDC_mapping_info_t;

typedef struct region_map_t {
// if keeping the struct of origin of region is needed?
    hg_atomic_int32_t                mapping_count;        /* count the number of mapping of this region */
    pdcid_t                          local_obj_id;         /* origin of object id */
    pdcid_t                          local_reg_id;         /* origin of region id */
    region_info_transfer_t           local_region;
    size_t                           local_ndim;
    uint64_t                        *local_reg_size;
    hg_addr_t                        local_addr;
    hg_bulk_t                        local_bulk_handle;
    PDC_var_type_t                   local_data_type;
    PDC_LIST_HEAD(PDC_mapping_info)  ids;                  /* Head of list of IDs */

    struct region_map_t             *prev;
    struct region_map_t             *next;
} region_map_t;

typedef struct region_buf_map_t {
    void                            *remote_data_ptr;
    pdcid_t                          remote_obj_id;         /* target of object id */
    pdcid_t                          remote_reg_id;         /* target of region id */
    int32_t                          remote_client_id;
    size_t                           remote_ndim;
    size_t                           remote_unit;
    region_info_transfer_t           remote_region_unit;
    region_info_transfer_t           remote_region_nounit;
    struct buf_map_release_bulk_args *bulk_args;

    pdcid_t                          local_reg_id;         /* origin of region id */
    region_info_transfer_t           local_region;
    size_t                           local_ndim;
    uint64_t                        *local_reg_size;
    hg_addr_t                        local_addr;
    hg_bulk_t                        local_bulk_handle;
    PDC_var_type_t                   local_data_type;

    struct region_buf_map_t         *prev;
    struct region_buf_map_t         *next;
} region_buf_map_t;

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
    int   is_shm_closed;

    struct pdc_data_server_io_list_t *prev;
    struct pdc_data_server_io_list_t *next;
} pdc_data_server_io_list_t;

typedef struct data_server_region_t {
    uint64_t obj_id;
    int fd;                           // file handle

    // For region lock list
    region_list_t *region_lock_head;
    // For buf to obj map
    region_buf_map_t *region_buf_map_head;
    // For lock request list
    region_list_t *region_lock_request_head;
    // For region storage list
//    region_list_t *region_storage_head;
    // For region map
    region_map_t *region_map_head;
    // For non-mapped object analysis
    // Used primarily as a local_temp
    void *obj_data_ptr;

    struct data_server_region_t *prev;
    struct data_server_region_t *next;
} data_server_region_t;

// For storing metadata
typedef struct pdc_metadata_t {
    int     user_id;                // Both server and client gets it and do security check
    char    app_name[ADDR_MAX];
    char    obj_name[ADDR_MAX];
    int     time_step;
    // Above four are the unique identifier for objects

    PDC_var_type_t data_type;
    uint64_t obj_id;
    uint64_t cont_id;
    time_t  create_time;
    time_t  last_modified_time;

    char    tags[TAG_LEN_MAX];
    pdc_kvtag_list_t *kvtag_list_head;
    char    data_location[ADDR_MAX];

    size_t   ndim;
    uint64_t dims[DIM_MAX];

    // For region storage list
    region_list_t *storage_region_list_head;

    // For region lock list
    region_list_t *region_lock_head;

    // For region map
    region_map_t *region_map_head;

    // For buf to obj map
    region_buf_map_t *region_buf_map_head;

    // For hash table list
    struct pdc_metadata_t *prev;
    struct pdc_metadata_t *next;
    void *bloom;

} pdc_metadata_t;

typedef struct {
    PDC_access_t                io_type;   
    uint32_t                    client_id;
    int32_t                     nclient;
    int32_t                     nbuffer_request;
    pdc_metadata_t              meta;
    region_list_t               region;
    int32_t                     cache_percentage;
} data_server_io_info_t;

typedef struct {
    hg_const_string_t    obj_name;
    uint32_t             hash_value;
    int32_t              time_step;
} metadata_query_in_t;

typedef struct {
    pdc_metadata_transfer_t ret;
} metadata_query_out_t;

typedef struct {
    uint64_t                obj_id;
    uint32_t                hash_value;
    hg_const_string_t       new_tag;
} metadata_add_tag_in_t;

typedef struct {
    int32_t            ret;
} metadata_add_tag_out_t;

/* typedef struct pdc_kvtag_t { */
/*     char            *name; */
/*     uint32_t        size; */
/*     void            *value; */
/* } pdc_kvtag_t; */

typedef struct {
    uint64_t                obj_id;
    uint32_t                hash_value;
    pdc_kvtag_t             kvtag;
} metadata_add_kvtag_in_t;

typedef struct {
    uint64_t                obj_id;
    uint32_t                hash_value;
    hg_string_t             key;
} metadata_del_kvtag_in_t;

typedef struct {
    uint64_t                obj_id;
    uint32_t                hash_value;
    hg_string_t             key;
} metadata_get_kvtag_in_t;

typedef struct {
    int                     ret;
    pdc_kvtag_t             kvtag;
} metadata_get_kvtag_out_t;

typedef struct {
    uint64_t                obj_id;
    uint32_t                hash_value;
    pdc_metadata_transfer_t new_metadata;
} metadata_update_in_t;

typedef struct {
    int32_t            ret;
} metadata_update_out_t;

typedef struct {
    uint64_t           obj_id;
} metadata_delete_by_id_in_t;

typedef struct {
    int32_t            ret;
} metadata_delete_by_id_out_t;

typedef struct {
    int32_t            ret;
} region_lock_out_t;

static hg_return_t
hg_proc_pdc_kvtag_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    pdc_kvtag_t *struct_data = (pdc_kvtag_t*) data;

    ret = hg_proc_hg_string_t(proc, &struct_data->name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }

    ret = hg_proc_uint32_t(proc, &struct_data->size);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }

    if (struct_data->size) {
        switch(hg_proc_get_op(proc)) {
            case HG_DECODE:
                struct_data->value = malloc(struct_data->size);
                HG_FALLTHROUGH();
            case HG_ENCODE:
                ret = hg_proc_raw(proc, struct_data->value, struct_data->size);
                break;
            case HG_FREE:
                free(struct_data->value);
            default:
                break;
        }
    }

    return ret;
}

typedef struct {
    uint32_t                    client_id;
    char                        shm_addr[ADDR_MAX];
    uint64_t                    size;
} pdc_shm_info_t;

typedef struct {
    uint32_t                    client_id;
    hg_string_t                 shm_addr;
    uint64_t                    size;
} send_shm_in_t;



static hg_return_t
hg_proc_region_info_transfer_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    region_info_transfer_t *struct_data = (region_info_transfer_t*) data;

    ret = hg_proc_hg_size_t(proc, &struct_data->ndim);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    
    ret = hg_proc_uint64_t(proc, &struct_data->start_0);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->start_1);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->start_2);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->start_3);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }

    ret = hg_proc_uint64_t(proc, &struct_data->count_0);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->count_1);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->count_2);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->count_3);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }

    /* ret = hg_proc_uint64_t(proc, &struct_data->stride_0); */
    /* if (ret != HG_SUCCESS) { */
    /*     HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    /* ret = hg_proc_uint64_t(proc, &struct_data->stride_1); */
    /* if (ret != HG_SUCCESS) { */
    /*     HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    /* ret = hg_proc_uint64_t(proc, &struct_data->stride_2); */
    /* if (ret != HG_SUCCESS) { */
    /*     HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    /* ret = hg_proc_uint64_t(proc, &struct_data->stride_3); */
    /* if (ret != HG_SUCCESS) { */
    /*     HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    return ret;
}

typedef struct {
    uint32_t                    meta_server_id;
    uint64_t                    obj_id;
//    int32_t                     lock_op;
    uint8_t                     access_type;
    pdcid_t                     local_reg_id;
    region_info_transfer_t      region;
    uint8_t                     mapping;
    uint8_t                     data_type;
    uint8_t                     lock_mode;
} region_lock_in_t;

static HG_INLINE hg_return_t
hg_proc_region_lock_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    region_lock_in_t *struct_data = (region_lock_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->meta_server_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
/*
    ret = hg_proc_uint32_t(proc, &struct_data->lock_op);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
*/
    ret = hg_proc_uint8_t(proc, &struct_data->access_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->local_reg_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint8_t(proc, &struct_data->mapping);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint8_t(proc, &struct_data->data_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint8_t(proc, &struct_data->lock_mode);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_region_lock_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    region_lock_out_t *struct_data = (region_lock_out_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    hg_string_t          obj_name;
    int32_t              time_step;
    uint32_t             hash_value;
} metadata_delete_in_t;

typedef struct {
    int32_t            ret;
} metadata_delete_out_t;

static HG_INLINE hg_return_t
hg_proc_metadata_query_transfer_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_query_transfer_in_t *struct_data = (metadata_query_transfer_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->is_list_all);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->user_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->app_name);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->time_step_from);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->time_step_to);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }

    ret = hg_proc_hg_size_t(proc, &struct_data->ndim);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    /* ret = hg_proc_int32_t(proc, &struct_data->create_time_from); */
    /* if (ret != HG_SUCCESS) { */
	/* HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    /* ret = hg_proc_int32_t(proc, &struct_data->create_time_to); */
    /* if (ret != HG_SUCCESS) { */
	/* HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    /* ret = hg_proc_int32_t(proc, &struct_data->last_modified_time_from); */
    /* if (ret != HG_SUCCESS) { */
	/* HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    /* ret = hg_proc_int32_t(proc, &struct_data->last_modified_time_to); */
    /* if (ret != HG_SUCCESS) { */
	/* HG_LOG_ERROR("Proc error"); */
    /*     return ret; */
    /* } */
    ret = hg_proc_hg_string_t(proc, &struct_data->tags);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    int32_t     ret;
    hg_bulk_t   bulk_handle;
} metadata_query_transfer_out_t;

static HG_INLINE hg_return_t
hg_proc_metadata_query_transfer_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_query_transfer_out_t *struct_data = (metadata_query_transfer_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_bulk_t(proc, &struct_data->bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}


static hg_return_t
hg_proc_pdc_metadata_transfer_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    pdc_metadata_transfer_t *struct_data = (pdc_metadata_transfer_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->user_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->app_name);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc app_name error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc obj_name error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->time_step);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->cont_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int8_t(proc, &struct_data->data_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_size_t(proc, &struct_data->ndim);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims0);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims1);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims2);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims3);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->data_location);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc data_location error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->tags);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc tags error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_add_tag_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_add_tag_in_t *struct_data = (metadata_add_tag_in_t*) data;

    ret = hg_proc_hg_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->new_tag);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_get_kvtag_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_get_kvtag_in_t *struct_data = (metadata_get_kvtag_in_t*) data;

    ret = hg_proc_hg_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->key);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_add_tag_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_add_tag_out_t *struct_data = (metadata_add_tag_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}


static HG_INLINE hg_return_t
hg_proc_metadata_get_kvtag_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_get_kvtag_out_t *struct_data = (metadata_get_kvtag_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_pdc_kvtag_t(proc, &struct_data->kvtag);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }

    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_add_kvtag_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_add_kvtag_in_t *struct_data = (metadata_add_kvtag_in_t*) data;

    ret = hg_proc_hg_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }

    /* switch(hg_proc_get_op(proc)) { */
    /*     case HG_DECODE: */
    /*         struct_data->kvtag = malloc(sizeof(pdc_kvtag_t)); */
    /* } */
 
    ret = hg_proc_pdc_kvtag_t(proc, &struct_data->kvtag);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_update_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_update_in_t *struct_data = (metadata_update_in_t*) data;

    ret = hg_proc_hg_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->new_metadata);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_update_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_update_out_t *struct_data = (metadata_update_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_delete_by_id_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_delete_by_id_in_t *struct_data = (metadata_delete_by_id_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_delete_by_id_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_delete_by_id_out_t *struct_data = (metadata_delete_by_id_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_delete_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_delete_in_t *struct_data = (metadata_delete_in_t*) data;

    ret = hg_proc_hg_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->time_step);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_delete_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_delete_out_t *struct_data = (metadata_delete_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_query_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_query_in_t *struct_data = (metadata_query_in_t*) data;

    ret = hg_proc_hg_const_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->time_step);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_query_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    metadata_query_out_t *struct_data = (metadata_query_out_t*) data;

    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    hg_const_string_t cont_name;
    uint32_t          hash_value;
} gen_cont_id_in_t;

typedef struct {
    uint64_t cont_id;
} gen_cont_id_out_t;

static HG_INLINE hg_return_t
hg_proc_gen_cont_id_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    gen_cont_id_in_t *struct_data = (gen_cont_id_in_t*) data;

    ret = hg_proc_hg_const_string_t(proc, &struct_data->cont_name);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }

    return ret;
}

static HG_INLINE hg_return_t
hg_proc_gen_cont_id_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    gen_cont_id_out_t *struct_data = (gen_cont_id_out_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->cont_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}


typedef struct {
    pdc_metadata_transfer_t data;
    uint32_t hash_value;
    int8_t data_type;
} gen_obj_id_in_t;

typedef struct {
    uint64_t obj_id;
} gen_obj_id_out_t;

typedef struct {
    int32_t server_id;
    int32_t nserver;
    hg_string_t server_addr;
} server_lookup_client_in_t;

typedef struct {
    int32_t ret;
} server_lookup_client_out_t;


typedef struct {
    int32_t server_id;
} server_lookup_remote_server_in_t;

typedef struct {
    int32_t ret;
} server_lookup_remote_server_out_t;


typedef struct {
    uint32_t client_id;
    int32_t nclient;
    hg_string_t client_addr;
} client_test_connect_in_t;

typedef struct {
    int32_t ret;
} client_test_connect_out_t;


typedef struct {
    int32_t  io_type;
    uint64_t obj_id;
    hg_string_t shm_addr;
} notify_io_complete_in_t;

typedef struct {
    int32_t ret;
} notify_io_complete_out_t;

typedef struct {
    uint64_t obj_id;
    uint64_t reg_id;
} notify_region_update_in_t;

typedef struct {
    int32_t ret;
} notify_region_update_out_t;

typedef struct {
    uint32_t client_id;
} close_server_in_t;

typedef struct {
    int32_t ret;
} close_server_out_t;

typedef struct {
    uint64_t        obj_id;
} get_remote_metadata_in_t;

typedef struct {
    pdc_metadata_transfer_t ret;
} get_remote_metadata_out_t; 

typedef struct {
    uint32_t        meta_server_id;
    uint64_t        local_reg_id;
    uint64_t        remote_obj_id;
    PDC_var_type_t  local_type;
    PDC_var_type_t  remote_type;
    size_t          ndim;
    size_t          remote_unit;
    hg_bulk_t       local_bulk_handle;
    region_info_transfer_t      remote_region_unit;
    region_info_transfer_t      remote_region_nounit;
    region_info_transfer_t      local_region;
} buf_map_in_t;

typedef struct {
    int32_t ret;
} buf_map_out_t;

typedef struct {
    uint64_t                    local_obj_id;
    uint64_t                    local_reg_id;
    uint64_t                    remote_obj_id;
    PDC_var_type_t              local_type;
    PDC_var_type_t              remote_type;
    size_t                      ndim;
    hg_bulk_t                   local_bulk_handle;
    hg_bulk_t                   remote_bulk_handle;
    region_info_transfer_t      remote_region;
    region_info_transfer_t      local_region;
} reg_map_in_t;

typedef struct {
    int32_t ret;
} reg_map_out_t;

typedef struct {
    uint32_t                    meta_server_id;
    uint64_t                    remote_obj_id;
    uint64_t                    remote_reg_id;
    region_info_transfer_t      remote_region;
} buf_unmap_in_t;

typedef struct {
    int32_t ret;
} buf_unmap_out_t;

typedef struct {
    uint64_t                    local_obj_id;
    uint64_t                    local_reg_id;
    region_info_transfer_t      local_region;
} reg_unmap_in_t;

typedef struct {
    int32_t ret;
} reg_unmap_out_t;

static HG_INLINE hg_return_t
hg_proc_gen_obj_id_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    gen_obj_id_in_t *struct_data = (gen_obj_id_in_t*) data;

    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->data);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int8_t(proc, &struct_data->data_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_gen_obj_id_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    gen_obj_id_out_t *struct_data = (gen_obj_id_out_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_server_lookup_remote_server_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    server_lookup_remote_server_in_t *struct_data = (server_lookup_remote_server_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->server_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_server_lookup_remote_server_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    server_lookup_remote_server_out_t *struct_data = (server_lookup_remote_server_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_server_lookup_client_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    server_lookup_client_in_t *struct_data = (server_lookup_client_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->server_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->nserver);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->server_addr);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_server_lookup_client_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    server_lookup_client_out_t *struct_data = (server_lookup_client_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    return ret;
}


static HG_INLINE hg_return_t
hg_proc_client_test_connect_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    client_test_connect_in_t *struct_data = (client_test_connect_in_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->nclient);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->client_addr);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_client_test_connect_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    client_test_connect_out_t *struct_data = (client_test_connect_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_notify_io_complete_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    notify_io_complete_in_t *struct_data = (notify_io_complete_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->io_type);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->shm_addr);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_notify_io_complete_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    notify_io_complete_out_t *struct_data = (notify_io_complete_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_notify_region_update_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    notify_region_update_in_t *struct_data = (notify_region_update_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
    }    
    ret = hg_proc_uint64_t(proc, &struct_data->reg_id);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_notify_region_update_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    notify_region_update_out_t *struct_data = (notify_region_update_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_close_server_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    close_server_in_t *struct_data = (close_server_in_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_close_server_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    close_server_out_t *struct_data = (close_server_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}


// Bulk
/* Define bulk_rpc_in_t */
typedef struct bulk_rpc_in_t {
    hg_int32_t cnt;
    hg_int32_t origin;
    hg_int32_t seq_id;
    hg_bulk_t bulk_handle;
} bulk_rpc_in_t;

/* Define hg_proc_bulk_rpc_in_t */
static HG_INLINE hg_return_t
hg_proc_bulk_rpc_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    bulk_rpc_in_t *struct_data = (bulk_rpc_in_t *) data;

    ret = hg_proc_int32_t(proc, &struct_data->cnt);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->seq_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->origin);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_bulk_t(proc, &struct_data->bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

/* Define bulk_rpc_out_t */
typedef struct {
    hg_uint64_t ret;
} bulk_rpc_out_t;

/* Define hg_proc_bulk_rpc_out_t */
static HG_INLINE hg_return_t
hg_proc_bulk_rpc_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    bulk_rpc_out_t *struct_data = (bulk_rpc_out_t *) data;

    ret = hg_proc_uint64_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}
// End of bulk

static HG_INLINE hg_return_t
hg_proc_get_remote_metadata_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_remote_metadata_in_t *struct_data = (get_remote_metadata_in_t *) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_get_remote_metadata_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_remote_metadata_out_t *struct_data = (get_remote_metadata_out_t *) data;

    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_buf_map_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    buf_map_in_t *struct_data = (buf_map_in_t *) data;

    ret = hg_proc_int32_t(proc, &struct_data->meta_server_id);
    if (ret != HG_SUCCESS) {
       HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->local_reg_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->remote_obj_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint8_t(proc, &struct_data->local_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint8_t(proc, &struct_data->remote_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_size_t(proc, &struct_data->ndim);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_size_t(proc, &struct_data->remote_unit);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_bulk_t(proc, &struct_data->local_bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->remote_region_unit);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->remote_region_nounit);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->local_region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_buf_map_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    buf_map_out_t *struct_data = (buf_map_out_t *) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_reg_map_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    reg_map_in_t *struct_data = (reg_map_in_t *) data;

    ret = hg_proc_uint64_t(proc, &struct_data->local_obj_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->local_reg_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->remote_obj_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint8_t(proc, &struct_data->local_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint8_t(proc, &struct_data->remote_type);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_size_t(proc, &struct_data->ndim);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_bulk_t(proc, &struct_data->local_bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_bulk_t(proc, &struct_data->remote_bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->remote_region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->local_region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_reg_map_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    reg_map_out_t *struct_data = (reg_map_out_t *) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_buf_unmap_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    buf_unmap_in_t *struct_data = (buf_unmap_in_t *) data;

    ret = hg_proc_uint32_t(proc, &struct_data->meta_server_id);
    if (ret != HG_SUCCESS) {
       HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->remote_obj_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->remote_reg_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->remote_region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_buf_unmap_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    buf_unmap_out_t *struct_data = (buf_unmap_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_reg_unmap_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    reg_unmap_in_t *struct_data = (reg_unmap_in_t *) data;

    ret = hg_proc_uint64_t(proc, &struct_data->local_obj_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->local_reg_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->local_region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_reg_unmap_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    reg_unmap_out_t *struct_data = (reg_unmap_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

/*
 * Data Server related
 */

/* MERCURY_GEN_PROC(data_server_read_in_t,  ((int32_t)(nclient)) ((hg_uint64_t)(meta_id)) ((region_info_transfer_t)(region))) */
/* MERCURY_GEN_PROC(data_server_read_out_t, ((int32_t)(ret)) ) */
typedef struct {
    uint32_t                    client_id;
    int32_t                     nclient;
    int32_t                     nupdate;
    pdc_metadata_transfer_t     meta;
    region_info_transfer_t      region;
    int32_t                     cache_percentage;
} data_server_read_in_t;

typedef struct {
    int32_t            ret;
} data_server_read_out_t;

static HG_INLINE hg_return_t
hg_proc_data_server_read_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_read_in_t *struct_data = (data_server_read_in_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->nclient);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->nupdate);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->meta);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->cache_percentage);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_data_server_read_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_read_out_t *struct_data = (data_server_read_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    return ret;
}

// Data server write
typedef struct {
    uint32_t                    client_id;
    int32_t                     nclient;    // Number of client requests expected to buffer before actual IO
    int32_t                     nupdate;    // Number of write requests expected to buffer before meta update
    hg_string_t                 shm_addr;
    pdc_metadata_transfer_t     meta;
    region_info_transfer_t      region;
} data_server_write_in_t;

typedef struct {
    int32_t            ret;
} data_server_write_out_t;

static HG_INLINE hg_return_t
hg_proc_data_server_write_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_write_in_t *struct_data = (data_server_write_in_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->nclient);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->nupdate);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->shm_addr);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->meta);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_data_server_write_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_write_out_t *struct_data = (data_server_write_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    return ret;
}

typedef struct {
    uint32_t                    client_id;
    pdc_metadata_transfer_t     meta;
    region_info_transfer_t      region;
} data_server_read_check_in_t;

typedef struct {
    int32_t            ret;
    hg_string_t  shm_addr;
} data_server_read_check_out_t;

static HG_INLINE hg_return_t
hg_proc_data_server_read_check_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_read_check_in_t *struct_data = (data_server_read_check_in_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->meta);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_data_server_read_check_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_read_check_out_t *struct_data = (data_server_read_check_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->shm_addr);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    uint32_t                    client_id;
    pdc_metadata_transfer_t     meta;
    region_info_transfer_t      region;
} data_server_write_check_in_t;

typedef struct {
    int32_t            ret;
} data_server_write_check_out_t;

static HG_INLINE hg_return_t
hg_proc_data_server_write_check_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_write_check_in_t *struct_data = (data_server_write_check_in_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->meta);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_data_server_write_check_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    data_server_write_check_out_t *struct_data = (data_server_write_check_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    uint64_t                    obj_id;
    region_info_transfer_t      region_transfer;
    char                        storage_location[ADDR_MAX];
    uint64_t                    offset;
    uint64_t                    size;
} region_storage_meta_t;

typedef struct {
    region_info_transfer_t      region_transfer;
    char                        storage_location[ADDR_MAX];
    uint64_t                    offset;
} update_region_storage_meta_bulk_t;

typedef struct {
    uint64_t                    obj_id;
    hg_string_t                 storage_location;
    uint64_t                    offset;
    region_info_transfer_t      region;
    int                         type;
} update_region_loc_in_t;

typedef struct {
    int32_t            ret;
} update_region_loc_out_t;

static HG_INLINE hg_return_t
hg_proc_update_region_loc_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    update_region_loc_in_t *struct_data = (update_region_loc_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->storage_location);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->offset);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->type);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_update_region_loc_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    update_region_loc_out_t *struct_data = (update_region_loc_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    pdcid_t                remote_obj_id;
    pdcid_t                remote_reg_id;
    int32_t                remote_client_id;
    region_info_transfer_t remote_region;
    hg_bulk_t              remote_bulk_handle;
//    hg_addr_t              remote_addr;
    pdcid_t       from_obj_id;
} get_reg_lock_status_in_t;

typedef struct {
    int lock;
} get_reg_lock_status_out_t;

typedef struct {
    uint64_t obj_id;
} get_metadata_by_id_in_t;

typedef struct {
    pdc_metadata_transfer_t res_meta;
} get_metadata_by_id_out_t;

static HG_INLINE hg_return_t
hg_proc_get_reg_lock_status_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_reg_lock_status_in_t *struct_data = (get_reg_lock_status_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->remote_obj_id);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->remote_reg_id);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->remote_client_id);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->remote_region);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
        return ret;
    } 
    ret = hg_proc_hg_bulk_t(proc, &struct_data->remote_bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    } 
    ret = hg_proc_uint64_t(proc, &struct_data->from_obj_id);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_get_reg_lock_status_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_reg_lock_status_out_t *struct_data = (get_reg_lock_status_out_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->lock);
    if (ret != HG_SUCCESS) {
    HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_get_metadata_by_id_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_metadata_by_id_in_t *struct_data = (get_metadata_by_id_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_get_metadata_by_id_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_metadata_by_id_out_t *struct_data = (get_metadata_by_id_out_t*) data;

    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->res_meta);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

// For generic serialized data transfer
typedef struct {
    hg_string_t buf;
} pdc_serialized_data_t;

static HG_INLINE hg_return_t
hg_proc_pdc_serialized_data_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    pdc_serialized_data_t *struct_data = (pdc_serialized_data_t*) data;

    ret = hg_proc_hg_string_t(proc, &struct_data->buf);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    region_info_transfer_t region_transfer;
    hg_string_t            storage_loc;
    uint64_t               file_offset;
} get_storage_info_single_out_t;

static HG_INLINE hg_return_t
hg_proc_get_storage_info_single_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_storage_info_single_out_t *struct_data = (get_storage_info_single_out_t*) data;
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region_transfer);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->storage_loc);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->file_offset);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}


typedef struct {
    uint64_t obj_id;
    region_info_transfer_t req_region;
} get_storage_info_in_t;

static HG_INLINE hg_return_t
hg_proc_get_storage_info_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    get_storage_info_in_t *struct_data = (get_storage_info_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->req_region);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}
 typedef struct {
    int origin;
} pdc_int_send_t;

static HG_INLINE hg_return_t
hg_proc_pdc_int_send_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    pdc_int_send_t *struct_data = (pdc_int_send_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->origin);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
    }
    return ret;
}
typedef struct {
    int ret;
} pdc_int_ret_t;

static HG_INLINE hg_return_t
hg_proc_pdc_int_ret_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    pdc_int_ret_t *struct_data = (pdc_int_ret_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
    }
    return ret;
}

typedef struct {
    hg_string_t buf;
    pdc_metadata_transfer_t meta;
} pdc_aggregated_io_to_server_t;

static HG_INLINE hg_return_t
hg_proc_pdc_aggregated_io_to_server_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    pdc_aggregated_io_to_server_t *struct_data = (pdc_aggregated_io_to_server_t*) data;

    ret = hg_proc_hg_string_t(proc, &struct_data->buf);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_pdc_metadata_transfer_t(proc, &struct_data->meta);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

hg_id_t gen_obj_id_register(hg_class_t *hg_class);
hg_id_t client_test_connect_register(hg_class_t *hg_class);
hg_id_t server_lookup_client_register(hg_class_t *hg_class);
hg_id_t close_server_register(hg_class_t *hg_class);
hg_id_t metadata_query_register(hg_class_t *hg_class);
hg_id_t container_query_register(hg_class_t *hg_class);
hg_id_t metadata_delete_register(hg_class_t *hg_class);
hg_id_t metadata_delete_by_id_register(hg_class_t *hg_class);
hg_id_t metadata_update_register(hg_class_t *hg_class);
hg_id_t metadata_add_tag_register(hg_class_t *hg_class);
hg_id_t metadata_add_kvtag_register(hg_class_t *hg_class);
hg_id_t metadata_del_kvtag_register(hg_class_t *hg_class);
hg_id_t metadata_get_kvtag_register(hg_class_t *hg_class);
hg_id_t get_remote_metadata_register(hg_class_t *hg_class_g);
hg_id_t region_lock_register(hg_class_t *hg_class);

hg_id_t reg_unmap_register(hg_class_t *hg_class);
hg_id_t obj_unmap_register(hg_class_t *hg_class);
hg_id_t data_server_write_register(hg_class_t *hg_class);
hg_id_t notify_region_update_register(hg_class_t *hg_class);
hg_id_t region_release_register(hg_class_t *hg_class);
hg_id_t buf_map_server_register(hg_class_t *hg_class);
hg_id_t buf_unmap_server_register(hg_class_t *hg_class);
hg_id_t obj_map_server_register(hg_class_t *hg_class);
hg_id_t obj_unmap_server_register(hg_class_t *hg_class);
hg_id_t get_reg_lock_register(hg_class_t *hg_class);

hg_id_t test_bulk_xfer_register(hg_class_t *hg_class);
hg_id_t server_lookup_remote_server_register(hg_class_t *hg_class);
hg_id_t update_region_loc_register(hg_class_t *hg_class);
hg_id_t get_metadata_by_id_register(hg_class_t *hg_class);
hg_id_t get_reg_lock_status_register(hg_class_t *hg_class);
hg_id_t get_storage_info_register(hg_class_t *hg_class);
hg_id_t bulk_rpc_register(hg_class_t *hg_class);
hg_id_t gen_cont_id_register(hg_class_t *hg_class);
hg_id_t cont_add_del_objs_rpc_register(hg_class_t *hg_class);
hg_id_t query_read_obj_name_rpc_register(hg_class_t *hg_class);
hg_id_t server_checkpoing_rpc_register(hg_class_t *hg_class);
hg_id_t send_shm_register(hg_class_t *hg_class);
hg_id_t query_read_obj_name_client_rpc_register(hg_class_t *hg_class);
hg_id_t cont_add_tags_rpc_register(hg_class_t *hg_class);
hg_id_t obj_data_iterator_register(hg_class_t *hg_class);
hg_id_t analysis_ftn_register(hg_class_t *hg_class);
hg_id_t transform_ftn_register(hg_class_t *hg_class);

hg_id_t send_shm_bulk_rpc_register(hg_class_t *hg_class);

//bulk
hg_id_t query_partial_register(hg_class_t *hg_class);
hg_id_t query_kvtag_register(hg_class_t *hg_class);
hg_id_t notify_io_complete_register(hg_class_t *hg_class);
hg_id_t data_server_read_register(hg_class_t *hg_class);

hg_id_t get_storage_meta_name_query_bulk_result_rpc_register(hg_class_t *hg_class);
hg_id_t notify_client_multi_io_complete_rpc_register(hg_class_t *hg_class);

hg_id_t send_client_storage_meta_rpc_register(hg_class_t *hg_class);

struct bulk_args_t {
    int cnt;
    int op;
    uint64_t cont_id;
    hg_handle_t handle;
    hg_bulk_t   bulk_handle;
    size_t nbytes;
    int origin;
    hg_atomic_int32_t completed_transfers;
    size_t ret;
    pdc_metadata_t **meta_arr;
    uint32_t        n_meta;
    uint64_t       *obj_ids;
    int client_seq_id;
};

struct buf_map_release_bulk_args {
    hg_handle_t handle;
    void  *data_buf;
    region_lock_in_t in;
    pdcid_t remote_obj_id;         /* target of object id */
    pdcid_t remote_reg_id;         /* target of region id */
    int32_t remote_client_id;
    struct PDC_region_info *remote_reg_info;  //
    region_info_transfer_t remote_region;
    hg_bulk_t remote_bulk_handle;
    hg_bulk_t local_bulk_handle;    //
    hg_addr_t local_addr;          //
    struct hg_thread_work work;
    hg_thread_mutex_t work_mutex;
    hg_thread_cond_t work_cond;
    int work_completed;
};

struct lock_bulk_args {
    hg_handle_t handle;
    region_lock_in_t in;
    struct PDC_region_info *server_region;
    void  *data_buf;
    region_map_t *mapping_list;
    hg_addr_t addr;
};

struct region_lock_update_bulk_args {
    hg_handle_t handle;
    region_lock_in_t in;
    pdcid_t remote_obj_id;
    pdcid_t remote_reg_id;
    int32_t remote_client_id;
    void  *data_buf;
    struct PDC_region_info *server_region;
};

struct region_update_bulk_args {
    hg_atomic_int32_t refcount;   // to track how many unlocked mapped region for data transfer
    hg_handle_t       handle;
    hg_bulk_t         bulk_handle;
    pdcid_t           remote_obj_id;
    pdcid_t           remote_reg_id;
    int32_t           remote_client_id;
    struct lock_bulk_args *args;
};

struct buf_region_update_bulk_args {
    hg_handle_t       handle;
    hg_bulk_t         bulk_handle;
    pdcid_t           remote_obj_id;
    pdcid_t           remote_reg_id;
    int32_t           remote_client_id;
    struct buf_map_release_bulk_args *args;
    int32_t           lock_ret;
};


hg_id_t reg_map_register(hg_class_t *hg_class);
hg_id_t reg_unmap_register(hg_class_t *hg_class);
hg_id_t buf_map_register(hg_class_t *hg_class);
hg_id_t buf_unmap_register(hg_class_t *hg_class);
hg_id_t obj_map_register(hg_class_t *hg_class);
hg_id_t obj_unmap_register(hg_class_t *hg_class);

double   PDC_get_elapsed_time_double(struct timeval *tstart, struct timeval *tend);
perr_t   delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out);
perr_t   PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out);
perr_t   PDC_Server_add_tag_metadata(metadata_add_tag_in_t *in, metadata_add_tag_out_t *out);
perr_t   PDC_get_self_addr(hg_class_t* hg_class, char* self_addr_string);
uint32_t PDC_get_server_by_obj_id(uint64_t obj_id, int n_server);
uint32_t PDC_get_hash_by_name(const char *name);
uint32_t PDC_get_server_by_name(char *name, int n_server);
int      PDC_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b);
perr_t   PDC_metadata_init(pdc_metadata_t *a);
void     PDC_print_metadata(pdc_metadata_t *a);
void     PDC_print_region_list(region_list_t *a);
void     PDC_print_storage_region_list(region_list_t *a);
uint64_t pdc_get_region_size(region_list_t *a);
perr_t   PDC_init_region_list(region_list_t *a);
int      PDC_is_same_region_list(region_list_t *a, region_list_t *b);
int      PDC_is_same_region_transfer(region_info_transfer_t *a, region_info_transfer_t *b);
uint32_t PDC_get_local_server_id(int my_rank, int n_client_per_server, int n_server);

perr_t pdc_metadata_t_to_transfer_t(pdc_metadata_t *meta, pdc_metadata_transfer_t *transfer);
perr_t pdc_transfer_t_to_metadata_t(pdc_metadata_transfer_t *transfer, pdc_metadata_t *meta);

perr_t pdc_region_info_to_list_t(struct PDC_region_info *region, region_list_t *list);
perr_t pdc_region_transfer_t_to_list_t(region_info_transfer_t *transfer, region_list_t *region);
perr_t pdc_region_list_t_to_transfer(region_list_t *region, region_info_transfer_t *transfer);
perr_t pdc_region_list_t_deep_cp(region_list_t *from, region_list_t *to);

perr_t pdc_region_info_t_to_transfer(struct PDC_region_info *region, region_info_transfer_t *transfer);
perr_t pdc_region_info_t_to_transfer_unit(struct PDC_region_info *region, region_info_transfer_t *transfer, size_t unit);
perr_t pdc_region_transfer_t_to_region_info(region_info_transfer_t *transfer, struct PDC_region_info *region);

perr_t PDC_serialize_regions_lists(region_list_t** regions, uint32_t n_region, void *buf, uint32_t buf_size);
perr_t PDC_unserialize_region_lists(void *buf, region_list_t** regions, uint32_t *n_region);
perr_t PDC_get_serialized_size(region_list_t** regions, uint32_t n_region, uint32_t *len);

perr_t PDC_replace_zero_chars(signed char *buf, uint32_t buf_size);
perr_t PDC_replace_char_fill_values(signed char *buf, uint32_t buf_size);

void pdc_mkdir(const char *dir);

extern hg_atomic_int32_t  close_server_g;

hg_id_t data_server_write_check_register(hg_class_t *hg_class);
hg_id_t data_server_read_register(hg_class_t *hg_class);

hg_id_t data_server_read_check_register(hg_class_t *hg_class);
hg_id_t data_server_read_register(hg_class_t *hg_class);


hg_id_t storage_meta_name_query_rpc_register(hg_class_t *hg_class);

//extern char *find_in_path(char *workingDir, char *application);


int pdc_msleep(unsigned long milisec);

// Container add/del objects
typedef struct {
    hg_int32_t op;
    hg_int32_t cnt;
    hg_int32_t origin;
    hg_uint64_t cont_id;
    hg_bulk_t bulk_handle;
} cont_add_del_objs_rpc_in_t;

/* Define hg_proc_cont_add_del_objs_rpc_in_t*/
static HG_INLINE hg_return_t
hg_proc_cont_add_del_objs_rpc_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    cont_add_del_objs_rpc_in_t *struct_data = (cont_add_del_objs_rpc_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->op);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->cnt);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->origin);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->cont_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_bulk_t(proc, &struct_data->bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    hg_uint32_t ret;
} cont_add_del_objs_rpc_out_t;

/* Define hg_proc_bulk_rpc_out_t */
static HG_INLINE hg_return_t
hg_proc_cont_add_del_objs_rpc_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    bulk_rpc_out_t *struct_data = (bulk_rpc_out_t *) data;

    ret = hg_proc_uint32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    hg_uint64_t cont_id;
    hg_string_t tags;
} cont_add_tags_rpc_in_t;

static HG_INLINE hg_return_t
hg_proc_cont_add_tags_rpc_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    cont_add_tags_rpc_in_t *struct_data = (cont_add_tags_rpc_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->cont_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_string_t(proc, &struct_data->tags);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

// Query and read obj 
typedef struct {
    hg_int32_t client_seq_id;
    hg_int32_t cnt;
    hg_int32_t total_size;
    hg_int32_t origin;
    hg_bulk_t bulk_handle;
} query_read_obj_name_in_t;

static HG_INLINE hg_return_t
hg_proc_query_read_obj_name_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    query_read_obj_name_in_t *struct_data = (query_read_obj_name_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->client_seq_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->cnt);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->total_size);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->origin);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_bulk_t(proc, &struct_data->bulk_handle);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    hg_uint32_t ret;
} query_read_obj_name_out_t;

static HG_INLINE hg_return_t
hg_proc_query_read_obj_name_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    bulk_rpc_out_t *struct_data = (bulk_rpc_out_t *) data;

    ret = hg_proc_uint32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct {
    hg_string_t    cont_name;
    uint32_t       hash_value;
} container_query_in_t;

typedef struct {
    uint64_t cont_id;
} container_query_out_t;

static HG_INLINE hg_return_t
hg_proc_container_query_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    container_query_in_t *struct_data = (container_query_in_t*) data;

    ret = hg_proc_hg_string_t(proc, &struct_data->cont_name);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_container_query_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    container_query_out_t *struct_data = (container_query_out_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->cont_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

typedef struct query_read_names_args_t {
    int  client_id;
    int  client_seq_id;
    int  cnt;
    int  is_select_all;
    char **obj_names;
    char *obj_names_1d;
} query_read_names_args_t;

// Server to server storage meta query with name
typedef struct {
    hg_string_t    obj_name;
    int            task_id;
    int            origin_id;
} storage_meta_name_query_in_t;

static HG_INLINE hg_return_t
hg_proc_storage_meta_name_query_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    storage_meta_name_query_in_t *struct_data = (storage_meta_name_query_in_t*) data;

    ret = hg_proc_hg_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->task_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->origin_id);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

hg_return_t pdc_check_int_ret_cb(const struct hg_cb_info *callback_info);
int PDC_region_list_seq_id_cmp(region_list_t *a, region_list_t *b);

typedef struct pdc_task_list_t {
    int task_id;
    perr_t   (*cb)();
    void     *cb_args;

    struct pdc_task_list_t *prev;
    struct pdc_task_list_t *next;
} pdc_task_list_t;

/* int PDC_add_task_to_list(pdc_task_list_t **target_list, perr_t (*cb)(), void *cb_args, int *curr_task_id, hg_thread_mutex_t *mutex); */
/* perr_t PDC_del_task_from_list(pdc_task_list_t **target_list, pdc_task_list_t *del, hg_thread_mutex_t *mutex); */
/* pdc_task_list_t *PDC_find_task_from_list(pdc_task_list_t** target_list, int id, hg_thread_mutex_t *mutex); */
int PDC_is_valid_task_id(int id);
int PDC_is_valid_obj_id(uint64_t id);

perr_t PDC_Client_query_read_complete(char *shm_addrs, int size, int n_shm, int seq_id);

typedef struct process_bulk_storage_meta_args_t{
    int origin_id;
    int n_storage_meta;
    int seq_id;
    region_storage_meta_t *all_storage_meta;
} process_bulk_storage_meta_args_t;

int is_contiguous_start_count_overlap(uint32_t ndim, uint64_t *a_start, uint64_t *a_count, uint64_t *b_start, uint64_t *b_count);
perr_t get_overlap_start_count(uint32_t ndim, uint64_t *start_a, uint64_t *count_a, 
                                                     uint64_t *start_b, uint64_t *count_b, 
                                       uint64_t *overlap_start, uint64_t *overlap_count);
int is_contiguous_region_overlap(region_list_t *a, region_list_t *b);


perr_t PDC_create_shm_segment(region_list_t *region);
perr_t PDC_create_shm_segment_ind(uint64_t size, char *shm_addr, void **buf);


hg_id_t cont_add_tags_rpc_register(hg_class_t *hg_class);
perr_t PDC_kvtag_dup(pdc_kvtag_t *from, pdc_kvtag_t **to);
perr_t PDC_free_kvtag(pdc_kvtag_t **kvtag);

#endif /* PDC_CLIENT_SERVER_COMMON_H */
