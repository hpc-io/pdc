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

/* #define pdc_server_tmp_dir_g  "./pdc_tmp" */
extern char pdc_server_tmp_dir_g[ADDR_MAX];
#define pdc_server_cfg_name_g "server.cfg"


extern uint64_t pdc_id_seq_g;
extern int pdc_server_rank_g;

// For storing metadata
typedef struct pdc_metadata_t {
    int     user_id;                // Both server and client gets it and do security check
    char    app_name[ADDR_MAX];
    char    obj_name[ADDR_MAX];
    int     time_step;
    // Above four are the unique identifier for objects

    uint64_t obj_id;
    char    obj_data_location[128];
    time_t  create_time;
    time_t  last_modified_time;

    char    tags[128];

    // For hash table list
    struct pdc_metadata_t *prev;
    struct pdc_metadata_t *next;
    void *bloom;

} pdc_metadata_t;

typedef struct pdc_metadata_transfer_t {
    int         user_id;
    int         time_step;
    uint64_t    obj_id;
    const char  *app_name;
    const char  *obj_name;
    const char  *data_location;
    const char  *tags;
    /* time_t      create_time; */
    /* time_t      last_modified_time; */
} pdc_metadata_transfer_t;


#ifdef HG_HAS_BOOST
MERCURY_GEN_STRUCT_PROC( pdc_metadata_transfer_t, ((int32_t)(user_id)) ((int32_t)(time_step)) ((uint64_t)(obj_id)) ((hg_const_string_t)(app_name)) ((hg_const_string_t)(obj_name)) ((hg_const_string_t)(data_location)) ((hg_const_string_t)(tags)) )

MERCURY_GEN_PROC( gen_obj_id_in_t, ((pdc_metadata_transfer_t)(data)) ((uint32_t)(hash_value)) )
MERCURY_GEN_PROC( gen_obj_id_out_t, ((uint64_t)(ret)) )

MERCURY_GEN_PROC( send_obj_name_marker_in_t, ((hg_const_string_t)(obj_name)) ((uint32_t)(hash_value)) )
MERCURY_GEN_PROC( send_obj_name_marker_out_t, ((int32_t)(ret)) )

MERCURY_GEN_PROC( client_test_connect_in_t,  ((int32_t)(client_id)) )
MERCURY_GEN_PROC( client_test_connect_out_t, ((int32_t)(ret))  )

MERCURY_GEN_PROC( close_server_in_t,  ((int32_t)(client_id)) )
MERCURY_GEN_PROC( close_server_out_t, ((int32_t)(ret))  )

MERCURY_GEN_PROC( metadata_query_in_t, ((hg_const_string_t)(obj_name)) ((uint32_t)(hash_value)) )
MERCURY_GEN_PROC( metadata_query_out_t, ((pdc_metadata_transfer_t)(ret)) )

MERCURY_GEN_PROC( metadata_delete_in_t, ((hg_const_string_t)(obj_name)) ((int32_t)(time_step)) ((uint32_t)(hash_value)) )
MERCURY_GEN_PROC( metadata_delete_out_t, ((int32_t)(ret)) )

MERCURY_GEN_PROC( metadata_update_in_t, ((uint64_t)(obj_id)) ((uint32_t)(hash_value)) ((pdc_metadata_transfer_t)(new_metadata)) )
MERCURY_GEN_PROC( metadata_update_out_t, ((int32_t)(ret)) )


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
    hg_const_string_t    obj_name;
    int32_t              time_step;
    uint32_t             hash_value;
} metadata_delete_in_t;

typedef struct {
    int32_t            ret;
} metadata_delete_out_t;

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
    ret = hg_proc_uint32_t(proc, &struct_data->time_step);
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


typedef struct {
    hg_const_string_t    obj_name;
    uint32_t             hash_value;
} send_obj_name_marker_in_t;

typedef struct {
    int32_t ret;
} send_obj_name_marker_out_t;

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

static HG_INLINE hg_return_t
hg_proc_send_obj_name_marker_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    send_obj_name_marker_in_t *struct_data = (send_obj_name_marker_in_t*) data;

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
hg_proc_send_obj_name_marker_out_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    send_obj_name_marker_out_t *struct_data = (send_obj_name_marker_out_t*) data;

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
#endif

hg_id_t gen_obj_id_register(hg_class_t *hg_class);
hg_id_t send_obj_name_marker_register(hg_class_t *hg_class);
hg_id_t client_test_connect_register(hg_class_t *hg_class);
hg_id_t close_server_register(hg_class_t *hg_class);
hg_id_t metadata_query_register(hg_class_t *hg_class);
hg_id_t metadata_delete_register(hg_class_t *hg_class);
hg_id_t metadata_update_register(hg_class_t *hg_class);
perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out);
perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out);


uint32_t PDC_get_hash_by_name(const char *name);
int      PDC_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b);
void     PDC_print_metadata(pdc_metadata_t *a);


extern hg_hash_table_t   *metadata_hash_table_g;
extern hg_atomic_int32_t  close_server_g;

#endif /* PDC_CLIENT_SERVER_COMMON_H */
