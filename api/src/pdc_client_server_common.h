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

// For storing metadata
typedef struct pdc_metadata_t {
    char    obj_name[PATH_MAX];
    char    app_name[PATH_MAX];
    /* char    *obj_name; */
    /* char    *app_name; */
    int     user_id;
    int     time_step;
    // Above four are the unique identifier for objects

    int     obj_id;
    /* char    *obj_data_location; */
    char    obj_data_location[PATH_MAX];
    time_t  create_time;
    time_t  last_modified_time;

    // For hash table list
    /* pdc_metadata_t *prev; */
    /* pdc_metadata_t *next; */
    /* HG_LIST_ENTRY(pdc_metadata_t) entry; */

} pdc_metadata_t;

/* HG_LIST_HEAD_DECL(my_head, my_entry); */


#ifdef HG_HAS_BOOST
MERCURY_GEN_PROC( gen_obj_id_in_t,  ((hg_const_string_t)(obj_name)) ((int32_t)(hash_value)) )
MERCURY_GEN_PROC( gen_obj_id_out_t, ((uint64_t)(ret)) )
#else
typedef struct {
    hg_const_string_t   obj_name;
    int32_t             hash_value;
} gen_obj_id_in_t;

typedef struct {
    uint64_t ret;
} gen_obj_id_out_t;

static HG_INLINE hg_return_t
hg_proc_gen_obj_id_in_t(hg_proc_t proc, void *data)
{
    hg_return_t ret;
    gen_obj_id_in_t *struct_data = (gen_obj_id_in_t*) data;

    ret = hg_proc_hg_const_string_t(proc, &struct_data->obj_name);
    if (ret != HG_SUCCESS) {
	HG_LOG_ERROR("Proc error");
    }
    ret = hg_proc_rpc_handle_t(proc, &struct_data->hash_value);
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

extern hg_hash_table_t *metadata_hash_table_g;

extern hg_atomic_int32_t close_server_g;

#endif /* PDC_CLIENT_SERVER_COMMON_H */
