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

#define pdc_server_cfg_name_g "server.cfg"
#define pdc_server_tmp_dir_g  "./pdc_tmp"

#define ADDR_MAX 128

extern uint64_t pdc_id_seq_g;

// For storing metadata
typedef struct pdc_metadata_t {
    int     user_id;                // Both server and client gets it and do security check
    char    app_name[PATH_MAX];
    char    obj_name[PATH_MAX];
    /* char    *app_name; */
    /* char    *obj_name; */
    int     time_step;
    // Above four are the unique identifier for objects

    int     obj_id;
    /* char    *obj_data_location; */
    char    obj_data_location[PATH_MAX];
    time_t  create_time;
    time_t  last_modified_time;

    char    tags[PATH_MAX];

    // For hash table list
    struct pdc_metadata_t *prev;
    struct pdc_metadata_t *next;
    void *bloom;

} pdc_metadata_t;


#ifdef HG_HAS_BOOST
MERCURY_GEN_PROC( gen_obj_id_in_t, ((uint32_t)(user_id)) ((hg_const_string_t)(app_name)) ((hg_const_string_t)(obj_name)) ((uint32_t)(time_step)) ((uint32_t)(hash_value)) ((hg_const_string_t)(tags)) )
MERCURY_GEN_PROC( gen_obj_id_out_t, ((uint64_t)(ret)) )
#else
typedef struct {
    uint32_t             user_id;
    hg_const_string_t    app_name;
    hg_const_string_t    obj_name;
    uint32_t             time_step;
    uint32_t             hash_value;
    hg_const_string_t    tags;
} gen_obj_id_in_t;

typedef struct {
    uint64_t ret;
} gen_obj_id_out_t;

static HG_INLINE hg_return_t
hg_proc_gen_obj_id_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    gen_obj_id_in_t *struct_data = (gen_obj_id_in_t*) data;

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
    ret = hg_proc_uint32_t(proc, &struct_data->hash_value);
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
#endif

hg_id_t gen_obj_id_register(hg_class_t *hg_class);
int PDC_Server_metadata_cmp(pdc_metadata_t *a, pdc_metadata_t *b);
void PDC_Server_print_metadata(pdc_metadata_t *a);

extern hg_hash_table_t *metadata_hash_table_g;

extern hg_atomic_int32_t close_server_g;

#endif /* PDC_CLIENT_SERVER_COMMON_H */
