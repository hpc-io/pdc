#include <time.h>

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"

#include "mercury_thread_pool.h"
#include "mercury_atomic.h"
#include "mercury_thread_mutex.h"
#include "mercury_hash_table.h"
#include "mercury_list.h"


#ifndef PDC_CLIENT_SERVER_COMMON_H
#define PDC_CLIENT_SERVER_COMMON_H

#define ADDR_MAX 64
#define DIM_MAX  16
#define PDC_SERVER_ID_INTERVEL 1000000

/* #define pdc_server_tmp_dir_g  "./pdc_tmp" */
extern char pdc_server_tmp_dir_g[ADDR_MAX];
#define pdc_server_cfg_name_g "server.cfg"

extern uint64_t pdc_id_seq_g;
extern int pdc_server_rank_g;

#define    PDC_LOCK_OP_OBTAIN  0
#define    PDC_LOCK_OP_RELEASE 1

typedef struct region_list_t {
    size_t ndim;
    uint64_t start[DIM_MAX];
    uint64_t count[DIM_MAX];
    uint64_t stride[DIM_MAX];

    struct region_list_t *prev;
    struct region_list_t *next;
} region_list_t;

// Similar structure PDC_region_info_t defined in pdc_obj_pkg.h
// TODO: currently only support upto four dimensions
typedef struct {
    size_t ndim;
    uint64_t start_0, start_1, start_2, start_3;
    uint64_t count_0, count_1, count_2, count_3;
    uint64_t stride_0, stride_1, stride_2, stride_3;
} region_info_transfer_t;

// For storing metadata
typedef struct pdc_metadata_t {
    int     user_id;                // Both server and client gets it and do security check
    char    app_name[ADDR_MAX];
    char    obj_name[ADDR_MAX];
    int     time_step;
    // Above four are the unique identifier for objects

    uint64_t obj_id;
    time_t  create_time;
    time_t  last_modified_time;

    int     ndim;
    int     dims[DIM_MAX];

    char    tags[128];
    char    data_location[ADDR_MAX];

    // For region lock
    region_list_t *region_lock_head;

    // For hash table list
    struct pdc_metadata_t *prev;
    struct pdc_metadata_t *next;
    void *bloom;

} pdc_metadata_t;

typedef struct pdc_metadata_transfer_t {
    int32_t     user_id;
    const char  *app_name;
    const char  *obj_name;
    int32_t     time_step;

    uint64_t    obj_id;

    int32_t     ndim;
    uint64_t    dims[DIM_MAX];

    const char  *tags;
    const char  *data_location;
    /* time_t      create_time; */
    /* time_t      last_modified_time; */
} pdc_metadata_transfer_t;

typedef struct metadata_query_transfer_in_t{
    int     is_list_all;

    int     user_id;                // Both server and client gets it and do security check
    char    *app_name;
    char    *obj_name;

    int     time_step_from;
    int     time_step_to;

    int     ndim;

    /* time_t  create_time_from; */
    /* time_t  create_time_to; */
    /* time_t  last_modified_time_from; */
    /* time_t  last_modified_time_to; */

    char    *tags;
} metadata_query_transfer_in_t;


#ifdef HG_HAS_BOOST
MERCURY_GEN_STRUCT_PROC( pdc_metadata_transfer_t, ((int32_t)(user_id)) ((int32_t)(time_step)) ((uint64_t)(obj_id)) ((int32_t)(ndim)) ((uint64_t)(dims[DIM_MAX])) ((hg_const_string_t)(app_name)) ((hg_const_string_t)(obj_name)) ((hg_const_string_t)(data_location)) ((hg_const_string_t)(tags)) )

MERCURY_GEN_PROC( gen_obj_id_in_t, ((pdc_metadata_transfer_t)(data)) ((uint32_t)(hash_value)) )
MERCURY_GEN_PROC( gen_obj_id_out_t, ((uint64_t)(ret)) )

/* MERCURY_GEN_PROC( send_obj_name_marker_in_t, ((hg_const_string_t)(obj_name)) ((uint32_t)(hash_value)) ) */
/* MERCURY_GEN_PROC( send_obj_name_marker_out_t, ((int32_t)(ret)) ) */

MERCURY_GEN_PROC( client_test_connect_in_t,  ((int32_t)(client_id)) )
MERCURY_GEN_PROC( client_test_connect_out_t, ((int32_t)(ret))  )

MERCURY_GEN_PROC( close_server_in_t,  ((int32_t)(client_id)) )
MERCURY_GEN_PROC( close_server_out_t, ((int32_t)(ret))  )

MERCURY_GEN_STRUCT_PROC( metadata_query_transfer_in_t, ((int32_t)(is_list_all)) ((int32_t)(user_id)) ((hg_const_string_t)(app_name)) ((hg_const_string_t)(obj_name)) ((int32_t)(time_step_from)) ((int32_t)(time_step_to)) ((int32_t)(ndim)) ((hg_const_string_t)(tags)) )
/* MERCURY_GEN_STRUCT_PROC( metadata_query_transfer_in_t, ((int32_t)(user_id)) ((hg_const_string_t)(app_name)) ((hg_const_string_t)(obj_name)) ((int32_t)(time_step_from)) ((int32_t)(time_step_to)) ((int32_t)(ndim)) ((int32_t)(create_time_from)) ((int32_t)(create_time_to)) ((int32_t)(last_modified_time_from)) ((int32_t)(last_modified_time_to)) ((hg_const_string_t)(tags)) ) */
MERCURY_GEN_PROC( metadata_query_transfer_out_t, ((hg_bulk_t)(bulk_handle)) ((int32_t)(ret)) )

MERCURY_GEN_PROC( metadata_query_in_t, ((hg_const_string_t)(obj_name)) ((uint32_t)(hash_value)) )
MERCURY_GEN_PROC( metadata_query_out_t, ((pdc_metadata_transfer_t)(ret)) )

MERCURY_GEN_PROC( metadata_delete_by_id_in_t, ((uint64_t)(obj_id)) )
MERCURY_GEN_PROC( metadata_delete_by_id_out_t, ((int32_t)(ret)) )

MERCURY_GEN_PROC( metadata_delete_in_t, ((hg_const_string_t)(obj_name)) ((int32_t)(time_step)) ((uint32_t)(hash_value)) )
MERCURY_GEN_PROC( metadata_delete_out_t, ((int32_t)(ret)) )

MERCURY_GEN_PROC( metadata_update_in_t, ((uint64_t)(obj_id)) ((uint32_t)(hash_value)) ((pdc_metadata_transfer_t)(new_metadata)) )
MERCURY_GEN_PROC( metadata_update_out_t, ((int32_t)(ret)) )

MERCURY_GEN_STRUCT_PROC( region_info_transfer_t, ((hg_size_t)(ndim)) ((uint64_t)(start_0)) ((uint64_t)(start_1)) ((uint64_t)(start_2)) ((uint64_t)(start_3))  ((uint64_t)(count_0)) ((uint64_t)(count_1)) ((uint64_t)(count_2)) ((uint64_t)(count_3)) ((uint64_t)(stride_0)) ((uint64_t)(stride_1)) ((uint64_t)(stride_2)) ((uint64_t)(stride_3)) )

MERCURY_GEN_PROC( region_lock_in_t, ((uint64_t)(obj_id)) ((int32_t)(lock_op)) ((region_info_transfer_t)(region)) )
MERCURY_GEN_PROC( region_lock_out_t, ((int32_t)(ret)) )

// Bulk
MERCURY_GEN_PROC(bulk_write_in_t,  ((hg_int32_t)(cnt)) ((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(bulk_write_out_t, ((hg_uint64_t)(ret)) )

#else

typedef struct {
    hg_const_string_t    obj_name;
    uint32_t             hash_value;
} metadata_query_in_t;

typedef struct {
    pdc_metadata_transfer_t ret;
} metadata_query_out_t;

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
    uint64_t                    obj_id;
    int32_t                     lock_op;
    region_info_transfer_t      region;
} region_lock_in_t;

typedef struct {
    int32_t            ret;
} region_lock_out_t;

static HG_INLINE hg_return_t
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

    ret = hg_proc_uint64_t(proc, &struct_data->stride_0);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->stride_1);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->stride_2);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->stride_3);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_region_lock_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    region_lock_in_t *struct_data = (region_lock_in_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_uint32_t(proc, &struct_data->lock_op);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_region_info_transfer_t(proc, &struct_data->region);
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
    }
    return ret;
}


typedef struct {
    hg_const_string_t    obj_name;
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
    ret = hg_proc_hg_const_string_t(proc, &struct_data->app_name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->obj_name);
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
    ret = hg_proc_uint32_t(proc, &struct_data->ndim);
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
    ret = hg_proc_hg_const_string_t(proc, &struct_data->tags);
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


static HG_INLINE hg_return_t
hg_proc_pdc_metadata_transfer_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    pdc_metadata_transfer_t *struct_data = (pdc_metadata_transfer_t*) data;

    ret = hg_proc_uint32_t(proc, &struct_data->user_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->app_name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_int32_t(proc, &struct_data->time_step);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_uint32_t(proc, &struct_data->ndim);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_uint64_t(proc, &struct_data->dims);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_uint64_t(proc, &struct_data->obj_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->data_location);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->tags);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
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
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
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
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_metadata_delete_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    metadata_delete_in_t *struct_data = (metadata_delete_in_t*) data;

    ret = hg_proc_hg_const_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_int32_t(proc, &struct_data->time_step);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
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
    }
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
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
    pdc_metadata_transfer_t data;
    uint32_t hash_value;
} gen_obj_id_in_t;

typedef struct {
    uint64_t ret;
} gen_obj_id_out_t;


/* typedef struct { */
/*     hg_const_string_t    obj_name; */
/*     uint32_t             hash_value; */
/* } send_obj_name_marker_in_t; */

/* typedef struct { */
/*     int32_t ret; */
/* } send_obj_name_marker_out_t; */

typedef struct {
    int32_t client_id;
} client_test_connect_in_t;

typedef struct {
    int32_t ret;
} client_test_connect_out_t;

typedef struct {
    int32_t client_id;
} close_server_in_t;

typedef struct {
    int32_t ret;
} close_server_out_t;


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
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_gen_obj_id_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    gen_obj_id_out_t *struct_data = (gen_obj_id_out_t*) data;

    ret = hg_proc_uint64_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    return ret;
}

/* static HG_INLINE hg_return_t */
/* hg_proc_send_obj_name_marker_in_t(hg_proc_t proc, void *data) */
/* { */
/*     hg_return_t ret; */
/*     send_obj_name_marker_in_t *struct_data = (send_obj_name_marker_in_t*) data; */

/*     ret = hg_proc_hg_const_string_t(proc, &struct_data->obj_name); */
/*     if (ret != HG_SUCCESS) { */
/* 	HG_LOG_ERROR("Proc error"); */
/*     } */
/*     ret = hg_proc_uint32_t(proc, &struct_data->hash_value); */
/*     if (ret != HG_SUCCESS) { */
/* 	HG_LOG_ERROR("Proc error"); */
/*     } */
/*     return ret; */
/* } */

/* static HG_INLINE hg_return_t */
/* hg_proc_send_obj_name_marker_out_t(hg_proc_t proc, void *data) */
/* { */
/*     hg_return_t ret; */
/*     send_obj_name_marker_out_t *struct_data = (send_obj_name_marker_out_t*) data; */

/*     ret = hg_proc_int32_t(proc, &struct_data->ret); */
/*     if (ret != HG_SUCCESS) { */
/* 	HG_LOG_ERROR("Proc error"); */
/*     } */
/*     return ret; */
/* } */

static HG_INLINE hg_return_t
hg_proc_client_test_connect_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    client_test_connect_in_t *struct_data = (client_test_connect_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
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
    }
    return ret;
}

static HG_INLINE hg_return_t
hg_proc_close_server_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    close_server_in_t *struct_data = (close_server_in_t*) data;

    ret = hg_proc_int32_t(proc, &struct_data->client_id);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
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
    }
    return ret;
}


// Bulk
/* Define bulk_write_in_t */
typedef struct {
    hg_int32_t cnt;
    hg_bulk_t bulk_handle;
} bulk_write_in_t;

/* Define hg_proc_bulk_write_in_t */
static HG_INLINE hg_return_t
hg_proc_bulk_write_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    bulk_write_in_t *struct_data = (bulk_write_in_t *) data;

    ret = hg_proc_int32_t(proc, &struct_data->cnt);
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

/* Define bulk_write_out_t */
typedef struct {
    hg_uint64_t ret;
} bulk_write_out_t;

/* Define hg_proc_bulk_write_out_t */
static HG_INLINE hg_return_t
hg_proc_bulk_write_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret = HG_SUCCESS;
    bulk_write_out_t *struct_data = (bulk_write_out_t *) data;

    ret = hg_proc_uint64_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

hg_id_t test_bulk_xfer_register(hg_class_t *hg_class);

// End of bulk

#endif

hg_id_t gen_obj_id_register(hg_class_t *hg_class);
hg_id_t send_obj_name_marker_register(hg_class_t *hg_class);
hg_id_t client_test_connect_register(hg_class_t *hg_class);
hg_id_t close_server_register(hg_class_t *hg_class);
hg_id_t metadata_query_register(hg_class_t *hg_class);
hg_id_t metadata_delete_register(hg_class_t *hg_class);
hg_id_t metadata_delete_by_id_register(hg_class_t *hg_class);
hg_id_t metadata_update_register(hg_class_t *hg_class);
hg_id_t region_lock_register(hg_class_t *hg_class);

//bulk
hg_id_t query_partial_register(hg_class_t *hg_class);

struct hg_test_bulk_args {
    int cnt;
    hg_handle_t handle;
    size_t nbytes;
    hg_atomic_int32_t completed_transfers;
    size_t ret;
};

perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out);
perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out);

uint32_t PDC_get_server_by_obj_id(uint64_t obj_id, int n_server);
uint32_t PDC_get_hash_by_name(const char *name);
int      PDC_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b);
void     PDC_print_metadata(pdc_metadata_t *a);


extern hg_hash_table_t   *metadata_hash_table_g;
extern hg_atomic_int32_t  close_server_g;

#endif /* PDC_CLIENT_SERVER_COMMON_H */
