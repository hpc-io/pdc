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
#include "pdc_config.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#ifdef PDC_HAS_CRAY_DRC
#include <rdmacred.h>
#endif

#include "pdc_utlist.h"
#include "pdc_id_pkg.h"
#include "pdc_cont_pkg.h"
#include "pdc_prop_pkg.h"
#include "pdc_obj_pkg.h"
#include "pdc_cont.h"
#include "pdc_region.h"
#include "pdc_interface.h"
#include "pdc_analysis_pkg.h"
#include "pdc_transforms_common.h"
#include "pdc_client_connect.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

#include "mercury.h"
#include "mercury_macros.h"

#include "string_utils.h"
#include "dart_core.h"
#include "timer_utils.h"
#include "query_utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <inttypes.h>
#include <math.h>
#include <sys/time.h>
#include <errno.h>

int                    is_client_debug_g      = 0;
pdc_server_selection_t pdc_server_selection_g = PDC_SERVER_DEFAULT;
int                    pdc_client_mpi_rank_g  = 0;
int                    pdc_client_mpi_size_g  = 1;
int                    dart_insert_count      = 0;

// FIXME: this is a temporary solution for printing out debug info, like memory usage.
int memory_debug_g = 0; // when it is no longer 0, stop printing debug info.

int64_t *server_time_total_g;
int64_t *server_call_count_g;
int64_t *server_mem_usage_g;

#ifdef ENABLE_MPI
MPI_Comm PDC_SAME_NODE_COMM_g;
MPI_Comm PDC_CLIENT_COMM_WORLD_g;
#endif

int pdc_client_same_node_rank_g = 0;
int pdc_client_same_node_size_g = 1;

int pdc_server_num_g;
int pdc_nclient_per_server_g = 0;

char                     pdc_client_tmp_dir_g[ADDR_MAX];
struct _pdc_server_info *pdc_server_info_g     = NULL;
static int *             debug_server_id_count = NULL;

int                 pdc_io_request_seq_id = PDC_SEQ_ID_INIT_VALUE;
struct pdc_request *pdc_io_request_list_g = NULL;

struct _pdc_query_result_list *pdcquery_result_list_head_g = NULL;

double memcpy_time_g = 0.0;
double read_time_g   = 0.0;
double query_time_g  = 0.0;

int    nfopen_g       = 0;
int    nread_bb_g     = 0;
double read_bb_size_g = 0.0;

static int           mercury_has_init_g = 0;
static hg_class_t *  send_class_g       = NULL;
static hg_context_t *send_context_g     = NULL;
int                  query_id_g         = 0;

// flags for RPC request and Bulk transfer.
// When a work is put in the queue, increase todo_g by 1
// When a work is done and popped from the queue, decrease (todo_g) by 1.
static hg_atomic_int32_t atomic_work_todo_g;
hg_atomic_int32_t        bulk_todo_g;
// When a work is initialized, set done_g flag to 0.
// When a work is done, set done_g flag to 1 using atomic cas operation.
static hg_atomic_int32_t response_done_g;
hg_atomic_int32_t        bulk_transfer_done_g;

// global variables for DART
static DART *                 dart_g;
static dart_hash_algo_t       dart_hash_algo_g    = DART_HASH;
static dart_object_ref_type_t dart_obj_ref_type_g = REF_PRIMARY_ID;

// global variables for Mercury RPC registration
static hg_id_t client_test_connect_register_id_g;
static hg_id_t gen_obj_register_id_g;
static hg_id_t gen_cont_register_id_g;
static hg_id_t close_server_register_id_g;
static hg_id_t flush_obj_register_id_g;
static hg_id_t flush_obj_all_register_id_g;
static hg_id_t metadata_query_register_id_g;
static hg_id_t obj_reset_dims_register_id_g;
static hg_id_t container_query_register_id_g;
static hg_id_t metadata_delete_register_id_g;
static hg_id_t metadata_delete_by_id_register_id_g;
static hg_id_t metadata_update_register_id_g;
static hg_id_t metadata_add_tag_register_id_g;
static hg_id_t metadata_add_kvtag_register_id_g;
static hg_id_t metadata_del_kvtag_register_id_g;
static hg_id_t metadata_get_kvtag_register_id_g;
static hg_id_t region_lock_register_id_g;
static hg_id_t region_release_register_id_g;
static hg_id_t transform_region_release_register_id_g;
static hg_id_t region_transform_release_register_id_g;
static hg_id_t region_analysis_release_register_id_g;
static hg_id_t data_server_read_register_id_g;
static hg_id_t data_server_read_check_register_id_g;
static hg_id_t data_server_write_check_register_id_g;
static hg_id_t data_server_write_register_id_g;
static hg_id_t server_checkpoint_rpc_register_id_g;
static hg_id_t send_shm_register_id_g;
static hg_id_t send_rpc_register_id_g;

// bulk
static hg_id_t query_partial_register_id_g;
static hg_id_t query_kvtag_register_id_g;

static hg_id_t transfer_request_register_id_g;
static hg_id_t transfer_request_all_register_id_g;
static hg_id_t transfer_request_metadata_query_register_id_g;
static hg_id_t transfer_request_metadata_query2_register_id_g;
static hg_id_t transfer_request_wait_all_register_id_g;
static hg_id_t transfer_request_status_register_id_g;
static hg_id_t transfer_request_wait_register_id_g;
static hg_id_t buf_map_register_id_g;
static hg_id_t buf_unmap_register_id_g;

static hg_id_t cont_add_del_objs_rpc_register_id_g;
static hg_id_t cont_add_tags_rpc_register_id_g;
static hg_id_t query_read_obj_name_register_id_g;
static hg_id_t query_read_obj_name_client_register_id_g;
static hg_id_t send_region_storage_meta_shm_bulk_rpc_register_id_g;

// data query
static hg_id_t send_data_query_register_id_g;
static hg_id_t get_sel_data_register_id_g;

// DART index
static hg_id_t dart_get_server_info_g;
static hg_id_t dart_perform_one_server_g;

int                        cache_percentage_g       = 0;
int                        cache_count_g            = 0;
int                        cache_total_g            = 0;
pdc_data_server_io_list_t *client_cache_list_head_g = NULL;

static uint64_t  object_selection_query_counter_g = 0;
static pthread_t hg_progress_tid_g;
static int       hg_progress_flag_g     = -1; // -1 thread unintialized, 0 thread created, 1 terminate thread
static int       hg_progress_task_cnt_g = 0;

/*
 *
 * Client Functions
 *
 */
static inline uint32_t
get_server_id_by_hash_name(const char *name)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE((uint32_t)(PDC_get_hash_by_name(name) % pdc_server_num_g));
}

static inline uint32_t
get_server_id_by_obj_id(uint64_t obj_id)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE((uint32_t)((obj_id / PDC_SERVER_ID_INTERVEL - 1) % pdc_server_num_g));
}

uint32_t
PDC_get_client_data_server()
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(PDC_CLIENT_DATA_SERVER());
}

int
PDC_Client_get_var_type_size(pdc_var_type_t dtype)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(PDC_get_var_type_size(dtype));
}

// Generic function to check the return value (RPC receipt) is 1
hg_return_t
pdc_client_check_int_ret_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value = HG_SUCCESS;
    pdc_int_ret_t                   output;
    hg_handle_t                     handle;
    struct _pdc_client_lookup_args *lookup_args;

    handle      = callback_info->info.forward.handle;
    lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");

    if (output.ret != 1)
        LOG_ERROR("Return value [%d] is NOT expected\n", output.ret);

    if (lookup_args != NULL)
        lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static void *
hg_progress_fn(void *foo)
{
    FUNC_ENTER(NULL);

    hg_return_t   ret;
    unsigned int  actual_count;
    hg_context_t *hg_context = (hg_context_t *)foo;

    while (hg_progress_flag_g != 1) {
        do {
            ret = HG_Trigger(hg_context, 0, 1, &actual_count);
        } while ((ret == HG_SUCCESS) && actual_count && hg_progress_flag_g != 1);
        if (hg_progress_flag_g != 1)
            HG_Progress(hg_context, 100);
        usleep(1000);
    }

    FUNC_LEAVE(NULL);
}

perr_t
PDC_Client_transfer_pthread_create()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (hg_progress_flag_g == -1) {
        pthread_create(&hg_progress_tid_g, NULL, hg_progress_fn, send_context_g);
        hg_progress_flag_g = 0;
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_pthread_terminate()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (hg_progress_flag_g == 0 && hg_progress_task_cnt_g == 0) {
        hg_progress_flag_g = 1;
        pthread_join(hg_progress_tid_g, NULL);
        hg_progress_flag_g = -1;
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_pthread_cnt_add(int n)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    hg_progress_task_cnt_g += n;

    FUNC_LEAVE(ret_value);
}

// Check if all work has been processed
// Using global variable $atomic_work_todo_g
perr_t
PDC_Client_check_response(hg_context_t **hg_context)
{
    FUNC_ENTER(NULL);

    perr_t       ret_value = SUCCEED;
    hg_return_t  hg_ret;
    unsigned int actual_count;

    do {
        do {
            hg_ret = HG_Trigger(*hg_context, 0 /* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (hg_atomic_get32(&atomic_work_todo_g) <= 0)
            break;

        hg_ret = HG_Progress(*hg_context, 100);
    } while (hg_ret == HG_SUCCESS || hg_ret == HG_TIMEOUT);

    ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}

// Block and wait for all work processed by pthread
inline static void
PDC_Client_wait_pthread_progress()
{
    FUNC_ENTER(NULL);

    while (hg_atomic_get32(&atomic_work_todo_g) > 0) {
        usleep(1000);
    }

    FUNC_LEAVE_VOID();
}

perr_t
PDC_Client_read_server_addr_from_file()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    max_tries = 9, sleeptime = 1;
    int    i = 0, is_server_ready = 0;
    char * p;
    FILE * na_config = NULL;
    char   config_fname[PATH_MAX];
    char   n_server_string[PATH_MAX];

    if (pdc_client_mpi_rank_g == 0) {
        sprintf(config_fname, "%s/%s", pdc_client_tmp_dir_g, pdc_server_cfg_name_g);

        for (i = 0; i < max_tries; i++) {
            if (access(config_fname, F_OK) != -1) {
                is_server_ready = 1;
                break;
            }
            LOG_WARNING("Config file from default location [%s] not available, "
                        "waiting %d seconds\n",
                        config_fname, sleeptime);
            sleep(sleeptime);
            sleeptime *= 2;
        }
        if (is_server_ready != 1)
            PGOTO_ERROR(FAIL, "Server is not ready");

        na_config = fopen(config_fname, "r");
        if (!na_config)
            PGOTO_ERROR(FAIL, "Could not open config file from default location: %s", config_fname);

        // Get the first line as $pdc_server_num_g
        if (fgets(n_server_string, PATH_MAX, na_config) == NULL) {
            PGOTO_ERROR(FAIL, "Get first line failed");
        }
        pdc_server_num_g = atoi(n_server_string);
    }

#ifdef ENABLE_MPI
    MPI_Bcast(&pdc_server_num_g, 1, MPI_INT, 0, PDC_CLIENT_COMM_WORLD_g);
#endif

    if (pdc_server_num_g == 0) {
        LOG_ERROR("Server number error %d\n", pdc_server_num_g);
        FUNC_LEAVE(-1);
    }

    // Allocate $pdc_server_info_g
    pdc_server_info_g =
        (struct _pdc_server_info *)PDC_calloc(sizeof(struct _pdc_server_info), pdc_server_num_g);

    i = 0;
    while (i < pdc_server_num_g) {
        if (pdc_client_mpi_rank_g == 0) {
            if (fgets(pdc_server_info_g[i].addr_string, ADDR_MAX, na_config) == NULL) {
                PGOTO_ERROR(FAIL, "Get first line failed");
            }
            p = strrchr(pdc_server_info_g[i].addr_string, '\n');
            if (p != NULL)
                *p = '\0';
        }

#ifdef ENABLE_MPI
        MPI_Bcast(pdc_server_info_g[i].addr_string, ADDR_MAX, MPI_CHAR, 0, PDC_CLIENT_COMM_WORLD_g);
#endif

        i++;
    }

    if (pdc_client_mpi_rank_g == 0) {
        fclose(na_config);
    }

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_flush_obj_all_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t         ret_value = HG_SUCCESS;
    hg_handle_t         handle;
    flush_obj_all_out_t output;
    int *               rpc_return;

    handle     = callback_info->info.forward.handle;
    rpc_return = (int *)callback_info->arg;

    ret_value   = HG_Get_output(handle, &output);
    *rpc_return = output.ret;
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
obj_reset_dims_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                      ret_value = HG_SUCCESS;
    hg_handle_t                      handle;
    struct _pdc_obj_reset_dims_args *region_transfer_args;
    obj_reset_dims_out_t             output;

    region_transfer_args = (struct _pdc_obj_reset_dims_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "obj_reset_dims_rpc_cb error with HG_Get_output");
    }
    region_transfer_args->ret = output.ret;
done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_flush_obj_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t     ret_value = HG_SUCCESS;
    hg_handle_t     handle;
    flush_obj_out_t output;
    int *           rpc_return;

    handle     = callback_info->info.forward.handle;
    rpc_return = (int *)callback_info->arg;

    ret_value   = HG_Get_output(handle, &output);
    *rpc_return = output.ret;

    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_close_all_server_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t        ret_value = HG_SUCCESS;
    hg_handle_t        handle;
    close_server_out_t output;
    int *              rpc_return;

    handle     = callback_info->info.forward.handle;
    rpc_return = (int *)callback_info->arg;

    ret_value   = HG_Get_output(handle, &output);
    *rpc_return = output.ret;
    if (ret_value != HG_SUCCESS || output.ret != 88)
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_transfer_request_metadata_query_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                                       ret_value = HG_SUCCESS;
    hg_handle_t                                       handle;
    struct _pdc_transfer_request_metadata_query_args *region_transfer_args;
    transfer_request_metadata_query_out_t             output;

    region_transfer_args = (struct _pdc_transfer_request_metadata_query_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }
    region_transfer_args->query_id       = output.query_id;
    region_transfer_args->total_buf_size = output.total_buf_size;
    region_transfer_args->ret            = output.ret;
done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t

client_send_transfer_request_metadata_query2_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                                        ret_value = HG_SUCCESS;
    hg_handle_t                                        handle;
    struct _pdc_transfer_request_metadata_query2_args *region_transfer_args;
    transfer_request_metadata_query2_out_t             output;

    region_transfer_args = (struct _pdc_transfer_request_metadata_query2_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }
    region_transfer_args->ret = output.ret;
done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_transfer_request_all_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                            ret_value = HG_SUCCESS;
    hg_handle_t                            handle;
    struct _pdc_transfer_request_all_args *region_transfer_args;
    transfer_request_all_out_t             output;

    region_transfer_args = (struct _pdc_transfer_request_all_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }

    region_transfer_args->ret         = output.ret;
    region_transfer_args->metadata_id = output.metadata_id;
done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_transfer_request_wait_all_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                                 ret_value = HG_SUCCESS;
    hg_handle_t                                 handle;
    struct _pdc_transfer_request_wait_all_args *region_transfer_args;
    transfer_request_wait_all_out_t             output;

    region_transfer_args = (struct _pdc_transfer_request_wait_all_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }
    region_transfer_args->ret = output.ret;
done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_transfer_request_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                        ret_value = HG_SUCCESS;
    hg_handle_t                        handle;
    struct _pdc_transfer_request_args *region_transfer_args;
    transfer_request_out_t             output;

    region_transfer_args = (struct _pdc_transfer_request_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }

    region_transfer_args->ret         = output.ret;
    region_transfer_args->metadata_id = output.metadata_id;
done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_transfer_request_status_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                               ret_value = HG_SUCCESS;
    hg_handle_t                               handle;
    struct _pdc_transfer_request_status_args *region_transfer_args;
    transfer_request_status_out_t             output;

    region_transfer_args = (struct _pdc_transfer_request_status_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }

    region_transfer_args->ret    = output.ret;
    region_transfer_args->status = output.status;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_send_transfer_request_wait_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;

    hg_handle_t                             handle;
    struct _pdc_transfer_request_wait_args *region_transfer_args;
    transfer_request_wait_out_t             output;

    region_transfer_args = (struct _pdc_transfer_request_wait_args *)callback_info->arg;
    handle               = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_transfer_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }

    region_transfer_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_buf_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t               ret_value = HG_SUCCESS;
    hg_handle_t               handle;
    struct _pdc_buf_map_args *region_unmap_args;
    buf_unmap_out_t           output;

    region_unmap_args = (struct _pdc_buf_map_args *)callback_info->arg;
    handle            = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        region_unmap_args->ret = -1;
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");
    }

    region_unmap_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_buf_map_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t               ret_value = HG_SUCCESS;
    hg_handle_t               handle;
    struct _pdc_buf_map_args *buf_map_args;
    buf_map_out_t             output;

    buf_map_args = (struct _pdc_buf_map_args *)callback_info->arg;
    handle       = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        buf_map_args->ret = -1;
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");
    }

    buf_map_args->ret = output.ret;

done:
    hg_atomic_set32(&atomic_work_todo_g, 0);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_test_connect_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value          = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t                     handle             = callback_info->info.forward.handle;

    client_test_connect_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");
    }

    client_lookup_args->ret = output.ret;

done:
    hg_atomic_set32(&atomic_work_todo_g, 0);
    HG_Free_output(handle, &output);
    HG_Destroy(callback_info->info.forward.handle);

    FUNC_LEAVE(ret_value);
}

// Callback function for HG_Addr_lookup()
static hg_return_t
client_test_connect_lookup_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value = HG_SUCCESS;
    uint32_t                        server_id;
    struct _pdc_client_lookup_args *client_lookup_args;
    client_test_connect_in_t        in;
    hg_handle_t                     client_test_handle;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    server_id          = client_lookup_args->server_id;

    pdc_server_info_g[server_id].addr       = callback_info->info.lookup.addr;
    pdc_server_info_g[server_id].addr_valid = 1;

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, client_test_connect_register_id_g,
              &client_test_handle);

    // Fill input structure
    in.client_id   = pdc_client_mpi_rank_g;
    in.nclient     = pdc_client_mpi_size_g;
    in.client_addr = client_lookup_args->client_addr;
    in.is_init     = client_lookup_args->is_init;

    ret_value = HG_Forward(client_test_handle, client_test_connect_rpc_cb, client_lookup_args, &in);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not start HG_Forward");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_lookup_server(int server_id, int is_init)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    struct _pdc_client_lookup_args lookup_args;
    char                           self_addr[ADDR_MAX];
    char *                         target_addr_string;

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Error with server id input %d", server_id);

    ret_value = PDC_get_self_addr(send_class_g, self_addr);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(ret_value, "Error getting self addr");

    lookup_args.client_id   = pdc_client_mpi_rank_g;
    lookup_args.server_id   = server_id;
    lookup_args.client_addr = self_addr;
    lookup_args.is_init     = is_init;
    target_addr_string      = pdc_server_info_g[lookup_args.server_id].addr_string;

    if (is_client_debug_g == 1)
        LOG_DEBUG("Testing connection to server %d [%s]\n", lookup_args.server_id, target_addr_string);

    hg_ret = HG_Addr_lookup(send_context_g, client_test_connect_lookup_cb, &lookup_args, target_addr_string,
                            HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Connection to server failed");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (is_client_debug_g == 1)
        LOG_DEBUG("Connected to server %5d\n", lookup_args.server_id);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_try_lookup_server(int server_id, int is_init)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    n_retry   = 1;

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Invalid server ID %d", server_id);

    while (pdc_server_info_g[server_id].addr_valid != 1) {
        if (n_retry > PDC_MAX_TRIAL_NUM)
            break;
        ret_value = PDC_Client_lookup_server(server_id, is_init);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_lookup_server");
        n_retry++;
    }

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
send_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    struct _pdc_client_lookup_args *client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t                     handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    metadata_add_tag_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "metadata_add_tag_rpc_cb error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_send_rpc(int server_id)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    handle;
    send_rpc_in_t                  in;

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Invalid server ID %d", server_id);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, send_rpc_register_id_g, &handle);

    in.value = pdc_client_mpi_rank_g;

    hg_ret = HG_Forward(handle, send_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "HG_Forward Error");

    // No need to wait
    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);

    PDC_Client_check_response(&send_context_g);

done:
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value = HG_SUCCESS;
    hg_handle_t                     handle;
    struct _pdc_client_lookup_args *client_lookup_args;
    gen_obj_id_out_t                output;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->obj_id = 0;
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");
    }
    client_lookup_args->obj_id = output.obj_id;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_region_lock_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value = HG_SUCCESS;
    hg_handle_t                   handle;
    struct _pdc_region_lock_args *client_lookup_args;
    region_lock_out_t             output;

    client_lookup_args = (struct _pdc_region_lock_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");
    }

    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_region_release_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value = HG_SUCCESS;
    hg_handle_t                     handle;
    struct _pdc_client_lookup_args *client_lookup_args;
    region_lock_out_t               output;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error with HG_Get_output");

    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Bulk
// Callback after bulk transfer is received by client
static hg_return_t
hg_test_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t         ret_value = HG_SUCCESS;
    struct bulk_args_t *bulk_args;
    hg_bulk_t           local_bulk_handle;
    uint32_t            i;
    void *              buf = NULL;
    void **             ids_buf;
    uint32_t            n_meta;
    uint64_t            buf_sizes[2] = {0, 0};
    uint64_t *          ids_buf_sizes;
    uint32_t            actual_cnt;
    pdc_metadata_t *    meta_ptr;
    uint64_t *          u64_arr_ptr;
    uint32_t            bulk_sgnum;

    bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    local_bulk_handle = hg_cb_info->info.bulk.local_handle;

    if (hg_cb_info->ret == HG_CANCELED)
        LOG_INFO("HG_Bulk_transfer() was successfully canceled\n");
    else if (hg_cb_info->ret != HG_SUCCESS)
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");

    n_meta = bulk_args->n_meta;

    if (hg_cb_info->ret == HG_SUCCESS) {
        if (bulk_args->is_id == 1) {
            bulk_sgnum    = HG_Bulk_get_segment_count(local_bulk_handle);
            ids_buf       = (void **)PDC_calloc(sizeof(void *), bulk_sgnum);
            ids_buf_sizes = (uint64_t *)PDC_calloc(sizeof(uint64_t), bulk_sgnum);
            HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, bulk_sgnum, ids_buf,
                           ids_buf_sizes, &actual_cnt);

            u64_arr_ptr        = ((uint64_t **)(ids_buf))[0];
            bulk_args->obj_ids = (uint64_t *)PDC_calloc(sizeof(uint64_t), n_meta);
            for (i = 0; i < n_meta; i++) {
                bulk_args->obj_ids[i] = *u64_arr_ptr;
                u64_arr_ptr++;
            }
        }
        else {
            HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, buf_sizes,
                           &actual_cnt);
            meta_ptr            = (pdc_metadata_t *)(buf);
            bulk_args->meta_arr = (pdc_metadata_t **)PDC_calloc(sizeof(pdc_metadata_t *), n_meta);
            for (i = 0; i < n_meta; i++) {
                bulk_args->meta_arr[i] = meta_ptr;
                meta_ptr++;
            }
        }
    }

    // Free block handle
    ret_value = HG_Bulk_free(local_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

done:
    hg_atomic_decr32(&bulk_todo_g);
    // checking the following flag will make all bulk transfers globally sequential.
    hg_atomic_cas32(&bulk_transfer_done_g, 0, 1);
    // checking the following flag will make sure the current bulk transfer is done.
    hg_atomic_cas32(&(bulk_args->bulk_done_flag), 0, 1);

    FUNC_LEAVE(ret_value);
}

// Bulk
// No need to have multi-threading
int
PDC_Client_check_bulk(hg_context_t *hg_context)
{
    FUNC_ENTER(NULL);

    int          ret_value;
    hg_return_t  hg_ret;
    unsigned int actual_count;

    /* Poke progress engine and check for events */
    do {
        actual_count = 0;
        do {
            hg_ret = HG_Trigger(hg_context, 0 /* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (hg_atomic_get32(&bulk_todo_g) <= 0)
            break;
        hg_ret = HG_Progress(hg_context, 100);

    } while (hg_ret == HG_SUCCESS || hg_ret == HG_TIMEOUT);

    if (hg_ret == HG_SUCCESS)
        ret_value = SUCCEED;
    else
        ret_value = FAIL;

    FUNC_LEAVE(ret_value);
}

#ifdef PDC_HAS_CRAY_DRC

/* Convert value to string */
#define DRC_ERROR_STRING_MACRO(def, value, string)                                                           \
    if (value == def)                                                                                        \
    string = #def

static const char *
drc_strerror(int errnum)
{
    FUNC_ENTER(NULL);

    char *      ret_value = NULL;
    const char *errstring = "UNDEFINED";

    DRC_ERROR_STRING_MACRO(DRC_SUCCESS, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_EINVAL, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_EPERM, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_ENOSPC, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_ECONNREFUSED, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_ALREADY_GRANTED, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_CRED_NOT_FOUND, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_CRED_CREATE_FAILURE, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_CRED_EXTERNAL_FAILURE, errnum, errstring);
    DRC_ERROR_STRING_MACRO(DRC_BAD_TOKEN, errnum, errstring);

    ret_value = errstring;

    FUNC_LEAVE(ret_value);
}
#endif

// Init Mercury class and context
// Register gen_obj_id rpc
perr_t
PDC_Client_mercury_init(hg_class_t **hg_class, hg_context_t **hg_context, int port)
{
    FUNC_ENTER(NULL);

    perr_t  ret_value = SUCCEED;
    char    na_info_string[NA_STRING_INFO_LEN];
    char *  hostname;
    pbool_t free_hostname = false;
    int     local_server_id;

    /* Set the default mercury transport
     * but enable overriding that to any of:
     *   "ofi+gni"
     *   "ofi+tcp"
     *   "cci+tcp"
     */
    struct hg_init_info init_info            = {0};
    char *              default_hg_transport = "ofi+tcp";
    char *              hg_transport;
#ifdef PDC_HAS_CRAY_DRC
    uint32_t          credential, cookie;
    drc_info_handle_t credential_info;
    char              pdc_auth_key[256] = {'\0'};
    char *            auth_key;
    int               rc;
#endif

    if ((hg_transport = getenv("HG_TRANSPORT")) == NULL) {
        hg_transport = default_hg_transport;
        if (pdc_client_mpi_rank_g == 0)
            LOG_INFO("Environment variable HG_TRANSPORT was NOT set, default to %s\n", default_hg_transport);
    }
    else
        LOG_INFO("Environment variable HG_TRANSPORT was set\n");
    if ((hostname = getenv("HG_HOST")) == NULL) {
        hostname = PDC_malloc(HOSTNAME_LEN);
        memset(hostname, 0, HOSTNAME_LEN);
        gethostname(hostname, HOSTNAME_LEN - 1);
        free_hostname = true;
        if (pdc_client_mpi_rank_g == 0)
            LOG_INFO("Environment variable HG_HOST was NOT set, default to %s\n", hostname);
    }
    else
        LOG_INFO("Environment variable HG_HOST was set\n");

    sprintf(na_info_string, "%s://%s:%d", hg_transport, hostname, port);

    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("Connection string: %s\n", na_info_string);
    if (free_hostname)
        hostname = (char *)PDC_free(hostname);

// gni starts here
#ifdef PDC_HAS_CRAY_DRC
    /* Acquire credential */
    if (pdc_client_mpi_rank_g == 0) {
        credential = atoi(getenv("PDC_DRC_KEY"));
    }
    MPI_Bcast(&credential, 1, MPI_UINT32_T, 0, PDC_CLIENT_COMM_WORLD_g);

    rc = drc_access(credential, 0, &credential_info);

drc_access_again:
    if (rc != DRC_SUCCESS) { /* failed to access credential */
        if (rc == -DRC_EINVAL) {
            sleep(1);
            PGOTO_DONE(drc_access_again);
        }
        LOG_ERROR("client drc_access() failed (%d, %s)", rc, drc_strerror(-rc));
        PGOTO_ERROR(FAIL, "Client drc_access() failed");
    }
    cookie = drc_get_first_cookie(credential_info);

    if (pdc_client_mpi_rank_g == 0) {
        LOG_INFO("# Credential is %u\n", credential);
        LOG_INFO("# Cookie is %u\n", cookie);
    }
    sprintf(pdc_auth_key, "%u", cookie);
    init_info.na_init_info.auth_key = strdup(pdc_auth_key);
#endif
    // gni ends

    /* Initialize Mercury with the desired network abstraction class */
//    *hg_class = HG_Init(na_info_string, HG_TRUE);
#ifndef ENABLE_MULTITHREAD
    init_info.na_init_info.progress_mode = NA_NO_BLOCK; // busy mode
#endif

// #ifndef PDC_HAS_CRAY_DRC
#ifdef PDC_HAS_SHARED_SERVER
    init_info.auto_sm = HG_TRUE;
#endif
    *hg_class = HG_Init_opt(na_info_string, HG_TRUE, &init_info);
    if (*hg_class == NULL)
        PGOTO_ERROR(FAIL, "Error with HG_Init()");

    /* Create HG context */
    *hg_context = HG_Context_create(*hg_class);

    // Register RPC
    client_test_connect_register_id_g = PDC_client_test_connect_register(*hg_class);
    gen_obj_register_id_g             = PDC_gen_obj_id_register(*hg_class);
    gen_cont_register_id_g            = PDC_gen_cont_id_register(*hg_class);
    close_server_register_id_g        = PDC_close_server_register(*hg_class);
    flush_obj_register_id_g           = PDC_flush_obj_register(*hg_class);
    flush_obj_all_register_id_g       = PDC_flush_obj_all_register(*hg_class);
    obj_reset_dims_register_id_g      = PDC_obj_reset_dims_register(*hg_class);
    // HG_Registered_disable_response(*hg_class, close_server_register_id_g, HG_TRUE);

    metadata_query_register_id_g           = PDC_metadata_query_register(*hg_class);
    container_query_register_id_g          = PDC_container_query_register(*hg_class);
    metadata_delete_register_id_g          = PDC_metadata_delete_register(*hg_class);
    metadata_delete_by_id_register_id_g    = PDC_metadata_delete_by_id_register(*hg_class);
    metadata_update_register_id_g          = PDC_metadata_update_register(*hg_class);
    metadata_add_tag_register_id_g         = PDC_metadata_add_tag_register(*hg_class);
    metadata_add_kvtag_register_id_g       = PDC_metadata_add_kvtag_register(*hg_class);
    metadata_del_kvtag_register_id_g       = PDC_metadata_del_kvtag_register(*hg_class);
    metadata_get_kvtag_register_id_g       = PDC_metadata_get_kvtag_register(*hg_class);
    region_lock_register_id_g              = PDC_region_lock_register(*hg_class);
    region_release_register_id_g           = PDC_region_release_register(*hg_class);
    transform_region_release_register_id_g = PDC_transform_region_release_register(*hg_class);
    region_transform_release_register_id_g = PDC_region_transform_release_register(*hg_class);
    region_analysis_release_register_id_g  = PDC_region_analysis_release_register(*hg_class);
    data_server_read_register_id_g         = PDC_data_server_read_register(*hg_class);
    data_server_read_check_register_id_g   = PDC_data_server_read_check_register(*hg_class);
    data_server_write_check_register_id_g  = PDC_data_server_write_check_register(*hg_class);
    data_server_write_register_id_g        = PDC_data_server_write_register(*hg_class);
    server_checkpoint_rpc_register_id_g    = PDC_server_checkpoint_rpc_register(*hg_class);
    send_shm_register_id_g                 = PDC_send_shm_register(*hg_class);
    send_rpc_register_id_g                 = PDC_send_rpc_register(*hg_class);

    // bulk
    query_partial_register_id_g = PDC_query_partial_register(*hg_class);
    query_kvtag_register_id_g   = PDC_query_kvtag_register(*hg_class);

    cont_add_del_objs_rpc_register_id_g      = PDC_cont_add_del_objs_rpc_register(*hg_class);
    cont_add_tags_rpc_register_id_g          = PDC_cont_add_tags_rpc_register(*hg_class);
    query_read_obj_name_register_id_g        = PDC_query_read_obj_name_rpc_register(*hg_class);
    query_read_obj_name_client_register_id_g = PDC_query_read_obj_name_client_rpc_register(*hg_class);
    send_region_storage_meta_shm_bulk_rpc_register_id_g = PDC_send_shm_bulk_rpc_register(*hg_class);

    // Map
    transfer_request_register_id_g                 = PDC_transfer_request_register(*hg_class);
    transfer_request_all_register_id_g             = PDC_transfer_request_all_register(*hg_class);
    transfer_request_metadata_query_register_id_g  = PDC_transfer_request_metadata_query_register(*hg_class);
    transfer_request_metadata_query2_register_id_g = PDC_transfer_request_metadata_query2_register(*hg_class);
    transfer_request_status_register_id_g          = PDC_transfer_request_status_register(*hg_class);
    transfer_request_wait_all_register_id_g        = PDC_transfer_request_wait_all_register(*hg_class);
    transfer_request_wait_register_id_g            = PDC_transfer_request_wait_register(*hg_class);
    buf_map_register_id_g                          = PDC_buf_map_register(*hg_class);
    buf_unmap_register_id_g                        = PDC_buf_unmap_register(*hg_class);

    // Analysis and Transforms
    analysis_ftn_register_id_g         = PDC_analysis_ftn_register(*hg_class);
    transform_ftn_register_id_g        = PDC_transform_ftn_register(*hg_class);
    object_data_iterator_register_id_g = PDC_obj_data_iterator_register(*hg_class);

    PDC_server_lookup_client_register(*hg_class);
    PDC_notify_io_complete_register(*hg_class);
    PDC_notify_region_update_register(*hg_class);
    PDC_notify_client_multi_io_complete_rpc_register(*hg_class);

    // Recv from server
    PDC_send_nhits_register(*hg_class);
    PDC_send_bulk_rpc_register(*hg_class);

    // Server to client RPC register
    PDC_send_client_storage_meta_rpc_register(*hg_class);

    // Data query
    send_data_query_register_id_g = PDC_send_data_query_rpc_register(*hg_class);
    get_sel_data_register_id_g    = PDC_get_sel_data_rpc_register(*hg_class);

    // DART Index
    dart_get_server_info_g    = PDC_dart_get_server_info_register(*hg_class);
    dart_perform_one_server_g = PDC_dart_perform_one_server_register(*hg_class);

#ifdef ENABLE_MULTITHREAD
    /* Mutex initialization for the client versions of these... */
    /* The Server versions gets initialized in pdc_server.c */
    hg_thread_mutex_init(&pdc_client_info_mutex_g);
    hg_thread_mutex_init(&lock_list_mutex_g);
    hg_thread_mutex_init(&meta_buf_map_mutex_g);
    hg_thread_mutex_init(&meta_obj_map_mutex_g);
#endif

    char *client_lookup_env = getenv("PDC_CLIENT_LOOKUP");
    if (client_lookup_env == NULL) {
        // Each client connect to its node local server only at start time
        local_server_id =
            PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);
        if (PDC_Client_try_lookup_server(local_server_id, 1) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error lookup server %d", local_server_id);
    }
    else if (strcmp(client_lookup_env, "ALL") == 0) {
        if (pdc_client_mpi_rank_g == 0)
            LOG_INFO("Client lookup all servers at start time\n");
        for (local_server_id = 0; local_server_id < pdc_server_num_g; local_server_id++) {
            if (pdc_client_mpi_size_g > 1000)
                PDC_msleep(pdc_client_mpi_rank_g % 300);
            if (PDC_Client_try_lookup_server(local_server_id, 1) != SUCCEED)
                PGOTO_ERROR(FAIL, "Error lookup server %d", local_server_id);
        }
    }
    else {
        if (pdc_client_mpi_rank_g == 0)
            LOG_INFO("Client lookup server at start time disabled\n");
    }

    if (is_client_debug_g == 1 && pdc_client_mpi_rank_g == 0)
        LOG_INFO("Successfully established connection to %d PDC metadata server%s\n\n", pdc_server_num_g,
                 pdc_client_mpi_size_g == 1 ? "" : "s");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_init()
{
    FUNC_ENTER(NULL);

    perr_t ret_value  = SUCCEED;
    pdc_server_info_g = NULL;
    char *   tmp_dir;
    uint32_t port;
    int      is_mpi_init = 0;

    // Get up tmp dir env var
    tmp_dir = getenv("PDC_TMPDIR");
    if (tmp_dir == NULL)
        strcpy(pdc_client_tmp_dir_g, "./pdc_tmp");
    else
        strcpy(pdc_client_tmp_dir_g, tmp_dir);

    // Get debug environment var
    char *is_debug_env = getenv("PDC_DEBUG");
    if (is_debug_env != NULL) {
        is_client_debug_g = atoi(is_debug_env);
    }

    pdc_client_mpi_rank_g = 0;
    pdc_client_mpi_size_g = 1;

    pdc_client_same_node_rank_g = 0;
    pdc_client_same_node_size_g = 1;

#ifdef ENABLE_MPI
    MPI_Initialized(&is_mpi_init);
    if (is_mpi_init != 1)
        MPI_Init(NULL, NULL);
    MPI_Comm_dup(MPI_COMM_WORLD, &PDC_CLIENT_COMM_WORLD_g);
    MPI_Comm_rank(PDC_CLIENT_COMM_WORLD_g, &pdc_client_mpi_rank_g);
    MPI_Comm_size(PDC_CLIENT_COMM_WORLD_g, &pdc_client_mpi_size_g);
    LOG_DEBUG("My client rank = %d, client communicator size = %d\n", pdc_client_mpi_rank_g,
              pdc_client_mpi_size_g);
#endif

    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("PDC_DEBUG set to %d\n", is_client_debug_g);

    // get server address and fill in $pdc_server_info_g
    if (PDC_Client_read_server_addr_from_file() != SUCCEED) {
        LOG_ERROR("Error getting PDC Metadata servers info, exiting...");
        exit(0);
    }

#ifdef ENABLE_MPI
    // Split the PDC_CLIENT_COMM_WORLD_g communicator, MPI_Comm_split_type requires MPI-3
    MPI_Comm_split_type(PDC_CLIENT_COMM_WORLD_g, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL,
                        &PDC_SAME_NODE_COMM_g);

    MPI_Comm_rank(PDC_SAME_NODE_COMM_g, &pdc_client_same_node_rank_g);
    MPI_Comm_size(PDC_SAME_NODE_COMM_g, &pdc_client_same_node_size_g);

    pdc_nclient_per_server_g = pdc_client_same_node_size_g;
#endif
    // Get the number of clients per server(node) through environment variable
    tmp_dir = getenv("PDC_NCLIENT_PER_SERVER");
    if (tmp_dir == NULL)
        pdc_nclient_per_server_g = pdc_client_mpi_size_g / pdc_server_num_g;
    else
        pdc_nclient_per_server_g = atoi(tmp_dir);

    if (pdc_nclient_per_server_g <= 0)
        pdc_nclient_per_server_g = 1;

    PDC_set_execution_locus(CLIENT_MEMORY);

    if (pdc_client_mpi_rank_g == 0) {
        LOG_INFO("Found %d PDC Metadata servers, running with %d PDC clients\n", pdc_server_num_g,
                 pdc_client_mpi_size_g);
    }

    // Init debug info
    if (pdc_server_num_g > 0) {
        debug_server_id_count = (int *)PDC_malloc(sizeof(int) * pdc_server_num_g);
        memset(debug_server_id_count, 0, sizeof(int) * pdc_server_num_g);
    }
    else
        LOG_ERROR("Server number not properly initialized\n");

    // Cori KNL has 68 cores per node, Haswell 32
    port = pdc_client_mpi_rank_g % PDC_MAX_CORE_PER_NODE + 8000;
    if (mercury_has_init_g == 0) {
        // Init Mercury network connection
        ret_value = PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
        if (ret_value != SUCCEED || send_class_g == NULL || send_context_g == NULL) {
            PGOTO_ERROR(FAIL, "Error with PDC_Client_mercury_init, exiting...");
        }

        hg_atomic_init32(&atomic_work_todo_g, 0);
        hg_atomic_init32(&response_done_g, 0);
        hg_atomic_init32(&bulk_todo_g, 0);
        hg_atomic_init32(&bulk_transfer_done_g, 0);
        mercury_has_init_g = 1;
    }

    if (pdc_client_mpi_rank_g == 0) {
        LOG_INFO("Using [%s] as tmp dir, %d clients per server\n", pdc_client_tmp_dir_g,
                 pdc_nclient_per_server_g);
    }

    if (mercury_has_init_g) {
        srand(time(NULL));

        /* Initialize DART space */
        dart_g = (DART *)PDC_calloc(1, sizeof(DART));
        dart_space_init(dart_g, pdc_server_num_g);

        server_time_total_g = (int64_t *)PDC_calloc(pdc_server_num_g, sizeof(int64_t));
        server_call_count_g = (int64_t *)PDC_calloc(pdc_server_num_g, sizeof(int64_t));
        server_mem_usage_g  = (int64_t *)PDC_calloc(pdc_server_num_g, sizeof(int64_t));
    }

done:
    FUNC_LEAVE(ret_value);
}

int
PDC_get_nproc_per_node()
{
    FUNC_ENTER(NULL);

    int ret_value = 0;
    ret_value     = pdc_client_same_node_size_g;

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_destroy_all_handles()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_finalize()
{
    FUNC_ENTER(NULL);

    hg_return_t hg_ret;
    perr_t      ret_value = SUCCEED;
    int         i;

    // Finalize Mercury
    for (i = 0; i < pdc_server_num_g; i++) {
        if (pdc_server_info_g[i].addr_valid) {
            HG_Addr_free(send_class_g, pdc_server_info_g[i].addr);
            pdc_server_info_g[i].addr_valid = 0;
        }
    }

    if (pdc_server_info_g != NULL)
        pdc_server_info_g = (struct _pdc_server_info *)PDC_free(pdc_server_info_g);

    // Terminate thread
    if (hg_progress_flag_g == 0) {
        hg_progress_flag_g = 1;
        pthread_join(hg_progress_tid_g, NULL);
        hg_progress_flag_g = -1;
    }

#ifndef ENABLE_MPI
    for (i = 0; i < pdc_server_num_g; i++) {
        LOG_INFO("  Server%3d, %d\n", i, debug_server_id_count[i]);
    }
#endif
    // free debug info
    if (debug_server_id_count != NULL)
        debug_server_id_count = (int *)PDC_free(debug_server_id_count);

#ifdef ENABLE_TIMING
    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("T_memcpy: %.2f\n", memcpy_time_g);
#endif

    if (HG_Context_destroy(send_context_g) != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error with HG_Context_destroy");

    if (HG_Finalize(send_class_g) != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error with HG_Finalize");

done:
    FUNC_LEAVE(ret_value);
}

// Bulk
static hg_return_t
metadata_query_bulk_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value;
    struct bulk_args_t *          client_lookup_args;
    hg_handle_t                   handle;
    metadata_query_transfer_out_t output;
    uint32_t                      n_meta;
    hg_op_id_t                    hg_bulk_op_id;
    hg_bulk_t                     local_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t                     origin_bulk_handle = HG_BULK_NULL;
    const struct hg_info *        hg_info            = NULL;
    struct bulk_args_t *          bulk_args;
    void *                        recv_meta;

    client_lookup_args = (struct bulk_args_t *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");

    n_meta                     = output.ret;
    client_lookup_args->n_meta = n_meta;
    if (n_meta == 0) {
        client_lookup_args->meta_arr = NULL;
        PGOTO_DONE(ret_value);
    }
    else
        client_lookup_args->meta_arr = (pdc_metadata_t **)PDC_calloc(n_meta, sizeof(pdc_metadata_t *));

    // We have received the bulk handle from server (server uses hg_respond)
    // Use this to initiate a bulk transfer
    origin_bulk_handle = output.bulk_handle;
    hg_info            = HG_Get_info(handle);

    bulk_args = (struct bulk_args_t *)PDC_malloc(sizeof(struct bulk_args_t));

    bulk_args->handle = handle;
    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->n_meta = client_lookup_args->n_meta;

    recv_meta = (void *)PDC_calloc(sizeof(pdc_metadata_t), n_meta);

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, (void **)&recv_meta, (hg_size_t *)&bulk_args->nbytes,
                   HG_BULK_READWRITE, &local_bulk_handle);

    /* Pull bulk data */
    ret_value =
        HG_Bulk_transfer(hg_info->context, hg_test_bulk_transfer_cb, bulk_args, HG_BULK_PULL, hg_info->addr,
                         origin_bulk_handle, 0, local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not read bulk data");

    // loop
    hg_atomic_incr32(&bulk_todo_g);
    PDC_Client_check_bulk(send_context_g);

    client_lookup_args->meta_arr = bulk_args->meta_arr;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// bulk test
perr_t
PDC_Client_list_all(int *n_res, pdc_metadata_t ***out)
{
    FUNC_ENTER(NULL);

    perr_t ret_value;
    ret_value = PDC_partial_query(1, -1, NULL, NULL, -1, -1, -1, NULL, n_res, out);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_partial_query(int is_list_all, int user_id, const char *app_name, const char *obj_name,
                  int time_step_from, int time_step_to, int ndim, const char *tags, int *n_res,
                  pdc_metadata_t ***out)
{
    FUNC_ENTER(NULL);

    perr_t             ret_value = SUCCEED;
    hg_return_t        hg_ret;
    int                n_recv = 0;
    uint32_t           i, server_id = 0, my_server_start, my_server_end, my_server_count;
    size_t             out_size = 0;
    hg_handle_t        query_partial_handle;
    struct bulk_args_t lookup_args;

    // Fill input structure
    metadata_query_transfer_in_t in;
    in.is_list_all    = is_list_all;
    in.user_id        = -1;
    in.app_name       = " ";
    in.obj_name       = " ";
    in.time_step_from = -1;
    in.time_step_to   = -1;
    in.ndim           = -1;
    in.tags           = " ";

    if (is_list_all != 1) {
        in.user_id        = user_id;
        in.ndim           = ndim;
        in.time_step_from = time_step_from;
        in.time_step_to   = time_step_to;
        if (app_name != NULL)
            in.app_name = app_name;
        if (obj_name != NULL)
            in.obj_name = obj_name;
        if (tags != NULL)
            in.tags = tags;
    }

    *out   = NULL;
    *n_res = 0;

    if (pdc_server_num_g > pdc_client_mpi_size_g) {
        my_server_count = pdc_server_num_g / pdc_client_mpi_size_g;
        my_server_start = pdc_client_mpi_rank_g * my_server_count;
        my_server_end   = my_server_start + my_server_count;
        if (pdc_client_mpi_rank_g == pdc_client_mpi_size_g - 1) {
            my_server_end += pdc_server_num_g % pdc_client_mpi_size_g;
        }
    }
    else {
        my_server_start = pdc_client_mpi_rank_g;
        my_server_end   = my_server_start + 1;
        if (pdc_client_mpi_rank_g >= pdc_server_num_g) {
            my_server_end = -1;
        }
    }

    for (server_id = my_server_start; server_id < my_server_end; server_id++) {
        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_partial_register_id_g,
                           &query_partial_handle);

        if (query_partial_handle == NULL)
            PGOTO_ERROR(FAIL, "Error with query_partial_handle");

        hg_ret = HG_Forward(query_partial_handle, metadata_query_bulk_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not start HG_Forward");

        hg_atomic_set32(&bulk_transfer_done_g, 0);

        // Wait for response from server
        hg_atomic_set32(&atomic_work_todo_g, 1);
        PDC_Client_check_response(&send_context_g);

        if (lookup_args.n_meta == 0)
            continue;

        // We do not have the results ready yet, need to wait.
        while (1) {
            if (hg_atomic_get32(&bulk_transfer_done_g))
                break;
        }

        if (*out == NULL) {
            out_size = sizeof(pdc_metadata_t *) * (lookup_args.n_meta);
            *out     = (pdc_metadata_t **)PDC_malloc(out_size);
        }
        else {
            out_size += sizeof(pdc_metadata_t *) * (lookup_args.n_meta);
            *out = (pdc_metadata_t **)PDC_realloc(*out, out_size);
        }

        *n_res += lookup_args.n_meta;
        for (i = 0; i < lookup_args.n_meta; i++) {
            (*out)[n_recv] = lookup_args.meta_arr[i];
            n_recv++;
        }

        HG_Destroy(query_partial_handle);
    }

    // TODO: need to be careful when freeing the lookup_args, as it include the results returned to user
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_tag(const char *tags, int *n_res, pdc_metadata_t ***out)
{
    FUNC_ENTER(NULL);

    perr_t             ret_value = SUCCEED;
    hg_return_t        hg_ret;
    int                n_recv = 0;
    uint32_t           i;
    int                server_id = 0;
    size_t             out_size  = 0;
    hg_handle_t        query_partial_handle;
    struct bulk_args_t lookup_args;

    if (tags == NULL)
        PGOTO_ERROR(FAIL, "tags was NULL");

    // Fill input structure
    metadata_query_transfer_in_t in;
    in.is_list_all    = 0;
    in.user_id        = -1;
    in.app_name       = " ";
    in.obj_name       = " ";
    in.time_step_from = -1;
    in.time_step_to   = -1;
    in.ndim           = 0;
    in.tags           = tags;

    *out   = NULL;
    *n_res = 0;

    for (server_id = 0; server_id < pdc_server_num_g; server_id++) {
        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_partial_register_id_g,
                           &query_partial_handle);

        if (query_partial_handle == NULL)
            PGOTO_ERROR(FAIL, "Error with query_partial_handle");

        hg_ret = HG_Forward(query_partial_handle, metadata_query_bulk_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

        hg_atomic_set32(&bulk_transfer_done_g, 0);

        // Wait for response from server
        hg_atomic_set32(&atomic_work_todo_g, 1);
        PDC_Client_check_response(&send_context_g);

        if ((lookup_args.n_meta) == 0)
            continue;

        // We do not have the results ready yet, need to wait.
        while (1) {
            if (hg_atomic_get32(&bulk_transfer_done_g))
                break;
        }

        if (*out == NULL) {
            out_size = sizeof(pdc_metadata_t *) * ((lookup_args.n_meta));
            *out     = (pdc_metadata_t **)PDC_malloc(out_size);
        }
        else {
            out_size += sizeof(pdc_metadata_t *) * ((lookup_args.n_meta));
            *out = (pdc_metadata_t **)PDC_realloc(*out, out_size);
        }

        *n_res += (lookup_args.n_meta);
        for (i = 0; i < lookup_args.n_meta; i++) {
            (*out)[n_recv] = lookup_args.meta_arr[i];
            n_recv++;
        }

        HG_Destroy(query_partial_handle);
    } // for server_id

    // TODO: need to be careful when freeing the lookup_args, as it include the results returned to user
done:
    FUNC_LEAVE(ret_value);
}

// Gets executed after a receving queried metadata from server
static hg_return_t
metadata_query_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                      ret_value;
    struct _pdc_metadata_query_args *client_lookup_args;
    hg_handle_t                      handle;
    metadata_query_out_t             output;

    client_lookup_args = (struct _pdc_metadata_query_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "metadata_query_rpc_cb error with HG_Get_output");

    if (output.ret.user_id == -1 && output.ret.obj_id == 0 && output.ret.time_step == -1) {
        client_lookup_args->data = NULL;
    }
    else {
        client_lookup_args->data = (pdc_metadata_t *)PDC_malloc(sizeof(pdc_metadata_t));
        if (client_lookup_args->data == NULL)
            PGOTO_ERROR(HG_OTHER_ERROR, "Cannnot allocate space for client_lookup_args->data");

        // Now copy the received metadata info
        ret_value = PDC_metadata_init(client_lookup_args->data);
        ret_value = PDC_transfer_t_to_metadata_t(&output.ret, client_lookup_args->data);
    }

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
metadata_delete_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;
    metadata_delete_out_t           output;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "Error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
metadata_delete_by_id_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;
    metadata_delete_by_id_out_t     output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "Error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
metadata_add_tag_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value;

    struct _pdc_client_lookup_args *client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t                     handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    metadata_add_tag_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "Error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_add_tag(pdcid_t obj_id, const char *tag)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    hg_handle_t                    metadata_add_tag_handle;
    uint64_t                       meta_id;
    uint32_t                       server_id;
    struct _pdc_obj_info *         obj_prop;
    struct _pdc_client_lookup_args lookup_args;
    metadata_add_tag_in_t          in;

    if (tag == NULL || tag[0] == 0)
        PGOTO_ERROR(FAIL, "Invalid tag content");

    obj_prop  = PDC_obj_get_info(obj_id);
    meta_id   = obj_prop->obj_info_pub->meta_id;
    server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_add_tag_register_id_g,
              &metadata_add_tag_handle);

    // Fill input structure
    in.obj_id     = meta_id;
    in.hash_value = PDC_get_hash_by_name(obj_prop->obj_info_pub->name);
    in.new_tag    = tag;

    hg_ret = HG_Forward(metadata_add_tag_handle, metadata_add_tag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error with HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        PGOTO_ERROR(FAIL, "Add tag NOT successful");

done:
    HG_Destroy(metadata_add_tag_handle);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed

static hg_return_t
metadata_update_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;
    metadata_update_out_t           output;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "metadata_update_rpc_cb error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_update_metadata(pdc_metadata_t *old, pdc_metadata_t *new)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    int                            hash_name_value;
    uint32_t                       server_id = 0;
    metadata_update_in_t           in;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    metadata_update_handle;

    if (old == NULL || new == NULL)
        PGOTO_ERROR(FAIL, "old or new was NULL");

    hash_name_value = PDC_get_hash_by_name(old->obj_name);
    server_id       = (hash_name_value + old->time_step);
    server_id %= pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_update_register_id_g,
              &metadata_update_handle);

    // Fill input structure
    in.obj_id     = old->obj_id;
    in.hash_value = hash_name_value;

    in.new_metadata.user_id   = 0;
    in.new_metadata.time_step = new->time_step;
    in.new_metadata.obj_id    = new->obj_id;
    in.new_metadata.obj_name  = old->obj_name;

    if (strcmp(new->data_location, "") == 0 || new->data_location[0] == 0)
        in.new_metadata.data_location = " ";
    else
        in.new_metadata.data_location = new->data_location;

    if (strcmp(new->app_name, "") == 0 || new->app_name[0] == 0)
        in.new_metadata.app_name = " ";
    else
        in.new_metadata.app_name = new->app_name;

    if (strcmp(new->tags, "") == 0 || new->tags[0] == 0 || old->tags == new->tags)
        in.new_metadata.tags = " ";
    else
        in.new_metadata.tags = new->tags;

    in.new_metadata.data_type = new->data_type;
    in.new_metadata.ndim      = new->ndim;
    in.new_metadata.dims0     = new->dims[0];
    in.new_metadata.dims1     = new->dims[1];
    in.new_metadata.dims2     = new->dims[2];
    in.new_metadata.dims3     = new->dims[3];

    // New fields to support transform state changes
    // and possibly provenance info.
    in.new_metadata.current_state   = new->transform_state;
    in.new_metadata.t_storage_order = new->current_state.storage_order;
    in.new_metadata.t_dtype         = new->current_state.dtype;
    in.new_metadata.t_ndim          = new->current_state.ndim;
    in.new_metadata.t_dims0         = new->current_state.dims[0];
    in.new_metadata.t_dims1         = new->current_state.dims[1];
    in.new_metadata.t_dims2         = new->current_state.dims[2];
    in.new_metadata.t_dims3         = new->current_state.dims[3];
    in.new_metadata.t_meta_index    = new->current_state.meta_index;

    hg_ret = HG_Forward(metadata_update_handle, metadata_update_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        PGOTO_ERROR(FAIL, "Update NOT successful");

done:
    HG_Destroy(metadata_update_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_delete_metadata_by_id(uint64_t obj_id)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    metadata_delete_by_id_in_t     in;
    uint32_t                       server_id;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    metadata_delete_by_id_handle;

    // Fill input structure
    in.obj_id = obj_id;
    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    if (server_id >= (uint32_t)pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Error with server id: %u", server_id);
    else
        debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_by_id_register_id_g,
                       &metadata_delete_by_id_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    hg_ret = HG_Forward(metadata_delete_by_id_handle, metadata_delete_by_id_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret < 0)
        PGOTO_ERROR(FAIL, "Failed to delete_by_id");

done:
    HG_Destroy(metadata_delete_by_id_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_delete_metadata(char *delete_name, pdcid_t obj_delete_prop)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    struct _pdc_obj_prop *         delete_prop;
    metadata_delete_in_t           in;
    int                            hash_name_value;
    uint32_t                       server_id;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    metadata_delete_handle;

    delete_prop = PDC_obj_prop_get_info(obj_delete_prop);
    // Fill input structure
    in.obj_name  = delete_name;
    in.time_step = delete_prop->time_step;

    hash_name_value = PDC_get_hash_by_name(delete_name);
    server_id       = (hash_name_value + in.time_step);
    server_id %= pdc_server_num_g;

    in.hash_value = hash_name_value;

    // Debug statistics for counting number of messages sent to each server.
    if (server_id >= (uint32_t)pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Error with server id: %u", server_id);
    else
        debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_register_id_g,
                       &metadata_delete_handle);
    hg_ret = HG_Forward(metadata_delete_handle, metadata_delete_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        LOG_ERROR("Delete NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_delete_handle);

    FUNC_LEAVE(ret_value);
}

// Search metadata using incomplete ID attributes
// Currently it's only using obj_name, and search from all servers
perr_t
PDC_Client_query_metadata_name_only(const char *obj_name, pdc_metadata_t **out)
{
    FUNC_ENTER(NULL);

    perr_t                            ret_value = SUCCEED;
    hg_return_t                       hg_ret    = 0;
    metadata_query_in_t               in;
    struct _pdc_metadata_query_args **lookup_args;
    uint32_t                          server_id;
    uint32_t                          i, count = 0;
    hg_handle_t *                     metadata_query_handle;

    metadata_query_handle = (hg_handle_t *)PDC_malloc(sizeof(hg_handle_t) * pdc_server_num_g);

    // Fill input structure
    in.obj_name   = obj_name;
    in.time_step  = 0;
    in.hash_value = PDC_get_hash_by_name(obj_name);

    lookup_args = (struct _pdc_metadata_query_args **)PDC_malloc(sizeof(struct _pdc_metadata_query_args *) *
                                                                 pdc_server_num_g);

    for (server_id = 0; server_id < (uint32_t)pdc_server_num_g; server_id++) {
        lookup_args[server_id] =
            (struct _pdc_metadata_query_args *)PDC_malloc(sizeof(struct _pdc_metadata_query_args));

        // Debug statistics for counting number of messages sent to each server.
        debug_server_id_count[server_id]++;

        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g,
                  &metadata_query_handle[server_id]);

        hg_ret =
            HG_Forward(metadata_query_handle[server_id], metadata_query_rpc_cb, lookup_args[server_id], &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_Client_query_metadata_name_only(): Could not start HG_Forward()");
    }

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, pdc_server_num_g);
    PDC_Client_check_response(&send_context_g);

    for (i = 0; i < (uint32_t)pdc_server_num_g; i++) {
        if (lookup_args[i]->data != NULL) {
            *out = lookup_args[i]->data;
            count++;
        }
        // TODO lookup_args[i] are not freed
        HG_Destroy(metadata_query_handle[i]);
    }

done:
    metadata_query_handle = (hg_handle_t *)PDC_free(metadata_query_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_metadata_name_timestep(const char *obj_name, int time_step, pdc_metadata_t **out,
                                        uint32_t *metadata_server_id)
{
    FUNC_ENTER(NULL);

    perr_t                          ret_value = SUCCEED;
    hg_return_t                     hg_ret    = 0;
    uint32_t                        hash_name_value;
    uint32_t                        server_id;
    metadata_query_in_t             in;
    struct _pdc_metadata_query_args lookup_args;
    hg_handle_t                     metadata_query_handle;

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(obj_name);
    server_id       = (hash_name_value + time_step);
    server_id %= pdc_server_num_g;

    *metadata_server_id = server_id;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g,
              &metadata_query_handle);

    // Fill input structure
    in.obj_name   = obj_name;
    in.hash_value = PDC_get_hash_by_name(obj_name);
    in.time_step  = time_step;

    lookup_args.data = (pdc_metadata_t *)PDC_malloc(sizeof(pdc_metadata_t));
    if (lookup_args.data == NULL)
        PGOTO_ERROR(FAIL, "Cannnot allocate space for client_lookup_args->data");

    hg_ret = HG_Forward(metadata_query_handle, metadata_query_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);
    *out = lookup_args.data;

    /**
     * Not necessarily an error. If the obj does not exist
     * then this will be NULL. Still need to return as FAIL
     * otherwise calling code will expect *out to be non-NULL.
     */
    if (*out == NULL) {
        LOG_INFO("Object metadata does not exist\n");
        PGOTO_DONE(FAIL);
    }

done:
    HG_Destroy(metadata_query_handle);

    FUNC_LEAVE(ret_value);
}

// Only let one process per node to do the actual query, then broadcast to all others
perr_t
PDC_Client_query_metadata_name_timestep_agg(const char *obj_name, int time_step, pdc_metadata_t **out,
                                            uint32_t *metadata_server_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

#ifdef ENABLE_MPI
    if (pdc_client_mpi_rank_g == 0)
        ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out, metadata_server_id);

    MPI_Bcast(&ret_value, 1, MPI_INT, 0, PDC_CLIENT_COMM_WORLD_g);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with query [%s]", obj_name);

    if (pdc_client_mpi_rank_g != 0)
        *out = (pdc_metadata_t *)PDC_calloc(1, sizeof(pdc_metadata_t));

    MPI_Bcast(*out, sizeof(pdc_metadata_t), MPI_CHAR, 0, PDC_CLIENT_COMM_WORLD_g);
    MPI_Bcast(metadata_server_id, 1, MPI_UINT32_T, 0, PDC_CLIENT_COMM_WORLD_g);
#else
    ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out, metadata_server_id);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with query [%s]", obj_name);
#endif

done:
    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an container (object) id
perr_t
PDC_Client_create_cont_id(const char *cont_name, pdcid_t cont_create_prop ATTRIBUTE(unused), pdcid_t *cont_id)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    uint32_t                       server_id = 0;
    gen_cont_id_in_t               in;
    uint32_t                       hash_name_value;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif

    if (cont_name == NULL)
        PGOTO_ERROR(FAIL, "Cannot create container with empty name");

    // Fill input structure
    memset(&in, 0, sizeof(in));

    hash_name_value = PDC_get_hash_by_name(cont_name);
    in.hash_value   = hash_name_value;
    in.cont_name    = cont_name;

    // Calculate server id
    server_id = hash_name_value;
    server_id %= pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret =
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_cont_register_id_g, &rpc_handle);

    hg_ret = HG_Forward(rpc_handle, client_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == 0) {
        *cont_id = 0;
        PGOTO_DONE(FAIL);
    }

    *cont_id  = lookup_args.obj_id;
    ret_value = SUCCEED;

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_timings.PDCclient_cont_create_rpc += end - start;
    pdc_timestamp_register(pdc_client_create_cont_timestamps, function_start, end);
#endif

done:
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

// Only one rand sends the request, others wait for MPI broadcast
perr_t
PDC_Client_create_cont_id_mpi(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (pdc_client_mpi_rank_g == 0) {
        ret_value = PDC_Client_create_cont_id(cont_name, cont_create_prop, cont_id);
    }
#ifdef ENABLE_MPI
    MPI_Bcast(cont_id, 1, MPI_LONG_LONG, 0, PDC_CLIENT_COMM_WORLD_g);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_obj_reset_dims(const char *obj_name, int time_step, int ndim, uint64_t *dims, int *reset)
{
    FUNC_ENTER(NULL);

    perr_t                          ret_value = SUCCEED;
    hg_return_t                     hg_ret    = 0;
    uint32_t                        hash_name_value;
    uint32_t                        server_id;
    obj_reset_dims_in_t             in;
    struct _pdc_obj_reset_dims_args lookup_args;
    hg_handle_t                     obj_reset_dims_handle;

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(obj_name);
    server_id       = (hash_name_value + time_step);
    server_id %= pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, obj_reset_dims_register_id_g,
              &obj_reset_dims_handle);

    // Fill input structure
    in.obj_name   = obj_name;
    in.hash_value = PDC_get_hash_by_name(obj_name);
    in.time_step  = time_step;
    in.ndim       = ndim;
    if (in.ndim >= 1) {
        in.dims0 = dims[0];
    }
    if (in.ndim >= 2) {
        in.dims1 = dims[1];
    }
    if (in.ndim >= 3) {
        in.dims2 = dims[2];
    }

    hg_ret = HG_Forward(obj_reset_dims_handle, obj_reset_dims_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error[%d] Could not start HG_Forward()", pdc_client_mpi_rank_g);

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);
    if (lookup_args.ret == 2) {
        *reset = 1;
    }
    else {
        *reset = 0;
    }
done:
    HG_Destroy(obj_reset_dims_handle);

    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
perr_t
PDC_Client_send_name_recv_id(const char *obj_name, uint64_t cont_id, pdcid_t obj_create_prop,
                             pdcid_t *meta_id, uint32_t *data_server_id, uint32_t *metadata_server_id)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    uint32_t                       server_id   = 0;
    struct _pdc_obj_prop *         create_prop = NULL;
    gen_obj_id_in_t                in;
    uint32_t                       hash_name_value;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif

    create_prop = PDC_obj_prop_get_info(obj_create_prop);

    if (obj_name == NULL)
        PGOTO_ERROR(FAIL, "Cannot create object with empty object name");

    // Fill input structure
    memset(&in, 0, sizeof(in));
    in.data.obj_name         = obj_name;
    in.data.cont_id          = cont_id;
    in.data.time_step        = create_prop->time_step;
    in.data.user_id          = create_prop->user_id;
    in.data_type             = create_prop->obj_prop_pub->type;
    in.data.data_server_id   = PDC_CLIENT_DATA_SERVER();
    in.data.region_partition = create_prop->obj_prop_pub->region_partition;
    LOG_DEBUG("prepare for sending region partition %d with obj name %s\n", (int)in.data.region_partition,
              obj_name);
    *data_server_id = in.data.data_server_id;
    LOG_DEBUG("pdc_client_mpi_rank_g = %d, pdc_nclient_per_server_g = %d, pdc_server_num_g = %d, "
              "data_server_id = %u\n",
              (int)pdc_client_mpi_rank_g, (int)pdc_nclient_per_server_g, (int)pdc_server_num_g,
              (unsigned)in.data.data_server_id);

    if ((in.data.ndim = create_prop->obj_prop_pub->ndim) > 0) {
        if (in.data.ndim >= 1)
            in.data.dims0 = create_prop->obj_prop_pub->dims[0];
        if (in.data.ndim >= 2)
            in.data.dims1 = create_prop->obj_prop_pub->dims[1];
        if (in.data.ndim >= 3)
            in.data.dims2 = create_prop->obj_prop_pub->dims[2];
        if (in.data.ndim >= 4)
            in.data.dims3 = create_prop->obj_prop_pub->dims[3];
    }

    if (create_prop->tags == NULL)
        in.data.tags = " ";
    else
        in.data.tags = create_prop->tags;

    if (create_prop->app_name == NULL)
        in.data.app_name = "Noname";
    else
        in.data.app_name = create_prop->app_name;

    if (create_prop->data_loc == NULL)
        in.data.data_location = " ";
    else
        in.data.data_location = create_prop->data_loc;

    hash_name_value = PDC_get_hash_by_name(obj_name);
    in.hash_value   = hash_name_value;

    // Compute server id
    server_id = (hash_name_value + in.data.time_step);
    server_id %= pdc_server_num_g;

    *metadata_server_id = server_id;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (is_client_debug_g == 1)
        LOG_INFO("obj [%s] to be created on server%u\n", obj_name, server_id);
    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    // We have already filled in the pdc_server_info_g[server_id].addr in previous
    // client_test_connect_lookup_cb
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_register_id_g, &rpc_handle);

    hg_ret = HG_Forward(rpc_handle, client_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == 0) {
        *meta_id = 0;
        PGOTO_DONE(FAIL);
    }

    *meta_id  = lookup_args.obj_id;
    ret_value = SUCCEED;

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_timings.PDCclient_obj_create_rpc += end - start;
    pdc_timestamp_register(pdc_client_create_obj_timestamps, function_start, end);
#endif

done:
    if (create_prop)
        PDC_obj_prop_free(create_prop);
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_close_all_server()
{
    FUNC_ENTER(NULL);

    uint64_t          ret_value = SUCCEED;
    hg_return_t       hg_ret    = HG_SUCCESS;
    uint32_t          server_id = 0;
    uint32_t          i;
    close_server_in_t in;
    hg_handle_t       close_server_handle;
    int               rpc_return;

    if (pdc_client_mpi_size_g < pdc_server_num_g) {
        if (pdc_client_mpi_rank_g == 0)
            LOG_INFO("Run close_server with equal ranks of servers (%d) for faster checkpoint\n",
                     pdc_server_num_g);
    }

    if (pdc_client_mpi_size_g >= pdc_server_num_g) {
        if (pdc_client_mpi_rank_g < pdc_server_num_g) {
            server_id = pdc_client_mpi_rank_g;
            if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
                PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

            HG_Create(send_context_g, pdc_server_info_g[server_id].addr, close_server_register_id_g,
                      &close_server_handle);

            // Fill input structure
            in.client_id = 0;
            hg_ret = HG_Forward(close_server_handle, client_send_close_all_server_rpc_cb, &rpc_return, &in);
            if (hg_ret != HG_SUCCESS)
                PGOTO_ERROR(FAIL, "PDC_Client_close_all_server(): Could not start HG_Forward()");

            // Wait for response from server
            hg_atomic_set32(&atomic_work_todo_g, 1);
            PDC_Client_check_response(&send_context_g);

            hg_ret = HG_Destroy(close_server_handle);
            if (hg_ret != HG_SUCCESS)
                PGOTO_ERROR(FAIL, "PDC_Client_close_all_server(): Could not destroy handle");
        } // End pdc_client_mpi_rank_g < pdc_server_num_g
    }     // End pdc_client_mpi_size_g >= pdc_server_num_g
    else {
        if (pdc_client_mpi_rank_g == 0) {
            for (i = 0; i < (uint32_t)pdc_server_num_g; i++) {
                server_id = pdc_server_num_g - 1 - i;
                if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
                    PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

                HG_Create(send_context_g, pdc_server_info_g[server_id].addr, close_server_register_id_g,
                          &close_server_handle);

                // Fill input structure
                in.client_id = 0;
                hg_ret =
                    HG_Forward(close_server_handle, client_send_close_all_server_rpc_cb, &rpc_return, &in);
                if (hg_ret != HG_SUCCESS)
                    PGOTO_ERROR(FAIL, "PDC_Client_close_all_server(): Could not start HG_Forward()");

                // Wait for response from server

                hg_atomic_set32(&atomic_work_todo_g, 1);
                PDC_Client_check_response(&send_context_g);

                hg_ret = HG_Destroy(close_server_handle);
                if (hg_ret != HG_SUCCESS)
                    PGOTO_ERROR(FAIL, "PDC_Client_close_all_server(): Could not destroy handle");
            }
        } // End of mpi_rank == 0
    }     // End pdc_client_mpi_size_g < pdc_server_num_g

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_buf_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id, struct pdc_region_info *reginfo,
                     pdc_var_type_t data_type, struct _pdc_obj_info *object_info)
{
    FUNC_ENTER(NULL);

    perr_t                   ret_value = SUCCEED;
    hg_return_t              hg_ret    = HG_SUCCESS;
    buf_unmap_in_t           in;
    size_t                   unit;
    uint32_t                 data_server_id, meta_server_id;
    struct _pdc_buf_map_args unmap_args;

    hg_handle_t client_send_buf_unmap_handle;
#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    // Fill input structure
    in.remote_obj_id = remote_obj_id;
    in.remote_reg_id = remote_reg_id;

    unit = PDC_get_var_type_size(data_type);
    PDC_region_info_t_to_transfer_unit(reginfo, &(in.remote_region), unit);

    // Compute metadata server id
    data_server_id    = ((pdc_metadata_t *)object_info->metadata)->data_server_id;
    meta_server_id    = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[data_server_id]++;

    if (PDC_Client_try_lookup_server(data_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr, buf_unmap_register_id_g,
              &client_send_buf_unmap_handle);
    hg_ret = HG_Forward(client_send_buf_unmap_handle, client_send_buf_unmap_rpc_cb, &unmap_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_buf_unmap(): Could not start HG_Forward()");
#ifdef PDC_TIMING
    pdc_timings.PDCbuf_obj_unmap_rpc += MPI_Wtime() - start;
#endif
    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
#ifdef PDC_TIMING
    start = MPI_Wtime();
#endif
    PDC_Client_check_response(&send_context_g);
#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_timings.PDCbuf_obj_unmap_rpc_wait += end - start;
    pdc_timestamp_register(pdc_client_buf_obj_unmap_timestamps, function_start, end);
#endif
    if (unmap_args.ret != 1)
        PGOTO_ERROR(FAIL, "buf unmap failed");

done:
    HG_Destroy(client_send_buf_unmap_handle);

    FUNC_LEAVE(ret_value);
}

static perr_t
pack_region_metadata(int ndim, uint64_t *offset, uint64_t *size, region_info_transfer_t *transfer)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    transfer->ndim   = ndim;

    PDC_copy_region_desc(offset, transfer->start, ndim, ndim);
    PDC_copy_region_desc(size, transfer->count, ndim, ndim);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_flush_obj(uint64_t obj_id)
{
    FUNC_ENTER(NULL);

    perr_t         ret_value = SUCCEED;
    hg_return_t    hg_ret    = HG_SUCCESS;
    uint32_t       server_id = 0;
    uint32_t       i;
    flush_obj_in_t in;
    hg_handle_t    flush_obj_handle;
    int            rpc_return;

    for (i = 0; i < (uint32_t)pdc_server_num_g; i++) {
        server_id = pdc_server_num_g - 1 - i;
        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, flush_obj_register_id_g,
                  &flush_obj_handle);

        // Fill input structure
        in.obj_id = obj_id;
        hg_ret    = HG_Forward(flush_obj_handle, client_send_flush_obj_rpc_cb, &rpc_return, &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_Client_flush_obj(): Could not start HG_Forward()");

        // Wait for response from server

        hg_atomic_set32(&atomic_work_todo_g, 1);
        PDC_Client_check_response(&send_context_g);

        hg_ret = HG_Destroy(flush_obj_handle);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_Client_flush_obj(): Could not destroy handle");
    }
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_flush_obj_all()
{
    FUNC_ENTER(NULL);

    perr_t             ret_value = SUCCEED;
    hg_return_t        hg_ret    = HG_SUCCESS;
    uint32_t           server_id = 0;
    uint32_t           i;
    flush_obj_all_in_t in;
    hg_handle_t        flush_obj_all_handle;
    int                rpc_return;

    for (i = 0; i < (uint32_t)pdc_server_num_g; i++) {
        server_id = pdc_server_num_g - 1 - i;
        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, flush_obj_all_register_id_g,
                  &flush_obj_all_handle);

        // Fill input structure
        in.tag = 44;
        hg_ret = HG_Forward(flush_obj_all_handle, client_send_flush_obj_all_rpc_cb, &rpc_return, &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not start HG_Forward");

        // Wait for response from server

        hg_atomic_set32(&atomic_work_todo_g, 1);
        PDC_Client_check_response(&send_context_g);

        hg_ret = HG_Destroy(flush_obj_all_handle);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_Client_flush_obj_all(): Could not destroy handle");
    }
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_request_all(hg_bulk_t *bulk_handle, int n_objs, pdc_access_t access_type,
                                uint32_t data_server_id, char *bulk_buf, hg_size_t bulk_size,
                                uint64_t *metadata_id,
#ifdef ENABLE_MPI
                                MPI_Comm comm)
#else
                                int comm)
#endif
{
    FUNC_ENTER(NULL);

    perr_t                                ret_value = SUCCEED;
    hg_return_t                           hg_ret    = HG_SUCCESS;
    transfer_request_all_in_t             in;
    hg_class_t *                          hg_class;
    int                                   i;
    hg_handle_t                           client_send_transfer_request_all_handle;
    struct _pdc_transfer_request_all_args transfer_args;
    char                                  cur_time[64];

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    if (!(access_type == PDC_WRITE || access_type == PDC_READ))
        PGOTO_ERROR(FAIL, "Invalid PDC type");
    in.n_objs         = n_objs;
    in.access_type    = access_type;
    in.total_buf_size = bulk_size;
    in.client_id      = pdc_client_mpi_rank_g;

    debug_server_id_count[data_server_id]++;

    hg_class = HG_Context_get_class(send_context_g);

    if (PDC_Client_try_lookup_server(data_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr,
                       transfer_request_all_register_id_g, &client_send_transfer_request_all_handle);

    // Create bulk handles
    hg_ret       = HG_Bulk_create(hg_class, 1, (void **)&bulk_buf, &bulk_size, HG_BULK_READWRITE,
                            &(in.local_bulk_handle));
    *bulk_handle = in.local_bulk_handle;
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create local bulk data handle");

    hg_atomic_set32(&atomic_work_todo_g, 1);

    hg_ret = HG_Forward(client_send_transfer_request_all_handle, client_send_transfer_request_all_rpc_cb,
                        &transfer_args, &in);

    /* #ifdef ENABLE_MPI */
    /*     if (comm != 0) */
    /*         MPI_Barrier(comm); */
    /* #endif */

    PDC_Client_transfer_pthread_create();

#ifdef PDC_TIMING
    if (access_type == PDC_READ) {
        pdc_timings.PDCtransfer_request_start_all_read_rpc += MPI_Wtime() - start;
    }
    else {
        pdc_timings.PDCtransfer_request_start_all_write_rpc += MPI_Wtime() - start;
    }
    start = MPI_Wtime();
#endif

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_transfer_request_all(): Could not start HG_Forward()");

    PDC_Client_wait_pthread_progress();

    /* #ifdef ENABLE_MPI */
    /*     if (comm != 0) */
    /*         MPI_Barrier(comm); */
    /* #endif */

#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (access_type == PDC_READ) {
        pdc_timings.PDCtransfer_request_start_all_read_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_transfer_request_start_all_read_timestamps, function_start, end);
    }
    else {
        pdc_timings.PDCtransfer_request_start_all_write_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_transfer_request_start_all_write_timestamps, function_start, end);
    }
#endif
    for (i = 0; i < n_objs; ++i) {
        metadata_id[i] = transfer_args.metadata_id + i;
    }
    if (transfer_args.ret != 1)
        PGOTO_ERROR(FAIL, "Transfer request failed");

    HG_Destroy(client_send_transfer_request_all_handle);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_request_metadata_query2(hg_bulk_t *bulk_handle, char *buf, uint64_t total_buf_size,
                                            uint64_t query_id, uint32_t metadata_server_id)
{
    FUNC_ENTER(NULL);

    perr_t                                            ret_value = SUCCEED;
    hg_return_t                                       hg_ret    = HG_SUCCESS;
    transfer_request_metadata_query2_in_t             in;
    hg_class_t *                                      hg_class;
    hg_handle_t                                       client_send_transfer_request_metadata_query2_handle;
    struct _pdc_transfer_request_metadata_query2_args transfer_args;

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    in.query_id       = query_id;
    in.total_buf_size = total_buf_size;

    // Compute metadata server id
    debug_server_id_count[metadata_server_id]++;

    hg_class = HG_Context_get_class(send_context_g);

    if (PDC_Client_try_lookup_server(metadata_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[metadata_server_id].addr,
                       transfer_request_metadata_query2_register_id_g,
                       &client_send_transfer_request_metadata_query2_handle);

    // Create bulk handles
    // For sending metadata
    hg_ret = HG_Bulk_create(hg_class, 1, (void **)&buf, (hg_size_t *)&(in.total_buf_size), HG_BULK_READWRITE,
                            &(in.local_bulk_handle));
    *bulk_handle = in.local_bulk_handle;
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create local bulk data handle");

    hg_ret = HG_Forward(client_send_transfer_request_metadata_query2_handle,
                        client_send_transfer_request_metadata_query2_rpc_cb, &transfer_args, &in);

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_transfer_request_metadata_query2(): Could not start HG_Forward()");
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (transfer_args.ret != 1)
        PGOTO_ERROR(FAIL, "transfer_request_metadata_query2 failed");

    HG_Destroy(client_send_transfer_request_metadata_query2_handle);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_timings.PDCtransfer_request_metadata_query_rpc += end - start;
    pdc_timestamp_register(pdc_client_transfer_request_metadata_query_timestamps, function_start, end);
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_request_metadata_query(hg_bulk_t *bulk_handle, char *buf, uint64_t total_buf_size,
                                           int n_objs, uint32_t metadata_server_id, uint8_t is_write,
                                           uint64_t *output_buf_size, uint64_t *query_id)
{
    FUNC_ENTER(NULL);

    perr_t                                           ret_value = SUCCEED;
    hg_return_t                                      hg_ret    = HG_SUCCESS;
    transfer_request_metadata_query_in_t             in;
    hg_class_t *                                     hg_class;
    hg_handle_t                                      client_send_transfer_request_metadata_query_handle;
    struct _pdc_transfer_request_metadata_query_args transfer_args;

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    in.n_objs         = n_objs;
    in.total_buf_size = total_buf_size;
    in.is_write       = is_write;

    // Compute metadata server id

    debug_server_id_count[metadata_server_id]++;

    hg_class = HG_Context_get_class(send_context_g);

    if (PDC_Client_try_lookup_server(metadata_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[metadata_server_id].addr,
                       transfer_request_metadata_query_register_id_g,
                       &client_send_transfer_request_metadata_query_handle);

    // Create bulk handles
    // For sending metadata
    hg_ret = HG_Bulk_create(hg_class, 1, (void **)&buf, (hg_size_t *)&(in.total_buf_size), HG_BULK_READWRITE,
                            &(in.local_bulk_handle));
    *bulk_handle = in.local_bulk_handle;
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create local bulk data handle");

    hg_ret = HG_Forward(client_send_transfer_request_metadata_query_handle,
                        client_send_transfer_request_metadata_query_rpc_cb, &transfer_args, &in);

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_transfer_request_metadata_query(): Could not start HG_Forward()");
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);
    if (transfer_args.ret != 1)
        PGOTO_ERROR(FAIL, "Transfer_request_metadata_query failed");

    *output_buf_size = transfer_args.total_buf_size;
    *query_id        = transfer_args.query_id;

    HG_Destroy(client_send_transfer_request_metadata_query_handle);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_timings.PDCtransfer_request_metadata_query_rpc += end - start;
    pdc_timestamp_register(pdc_client_transfer_request_metadata_query_timestamps, function_start, end);
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_request_wait_all(hg_bulk_t *bulk_handle, int n_objs, pdcid_t *transfer_request_id,
                                     uint32_t data_server_id)
{
    FUNC_ENTER(NULL);

    perr_t                                     ret_value = SUCCEED;
    hg_return_t                                hg_ret    = HG_SUCCESS;
    transfer_request_wait_all_in_t             in;
    hg_class_t *                               hg_class;
    hg_handle_t                                client_send_transfer_request_wait_all_handle;
    struct _pdc_transfer_request_wait_all_args transfer_args;
    char                                       cur_time[64];

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    in.n_objs         = n_objs;
    in.total_buf_size = sizeof(pdcid_t) * n_objs;

    // Compute metadata server id

    debug_server_id_count[data_server_id]++;

    hg_class = HG_Context_get_class(send_context_g);

    if (PDC_Client_try_lookup_server(data_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");
    hg_ret =
        HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr,
                  transfer_request_wait_all_register_id_g, &client_send_transfer_request_wait_all_handle);

    // Create bulk handles
    // For sending metadata
    hg_ret = HG_Bulk_create(hg_class, 1, (void **)&transfer_request_id, (hg_size_t *)&(in.total_buf_size),
                            HG_BULK_READWRITE, &(in.local_bulk_handle));
    *bulk_handle = in.local_bulk_handle;
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create local bulk data handle");

    hg_ret = HG_Forward(client_send_transfer_request_wait_all_handle,
                        client_send_transfer_request_wait_all_rpc_cb, &transfer_args, &in);

#ifdef PDC_TIMING
    pdc_timings.PDCtransfer_request_wait_all_rpc += MPI_Wtime() - start;
    start = MPI_Wtime();
#endif
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (transfer_args.ret != 1)
        PGOTO_ERROR(FAIL, "Transfer request wait all failed");

    HG_Destroy(client_send_transfer_request_wait_all_handle);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_timings.PDCtransfer_request_wait_all_rpc_wait += end - start;
    pdc_timestamp_register(pdc_client_transfer_request_wait_all_timestamps, function_start, end);
#endif

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_request(hg_bulk_t *bulk_handle, void *buf, pdcid_t obj_id, uint32_t data_server_id,
                            int obj_ndim, uint64_t *obj_dims, int remote_ndim, uint64_t *remote_offset,
                            uint64_t *remote_size, size_t unit, pdc_access_t access_type,
                            pdcid_t *metadata_id)
{
    FUNC_ENTER(NULL);

    perr_t                            ret_value = SUCCEED;
    hg_return_t                       hg_ret    = HG_SUCCESS;
    transfer_request_in_t             in;
    hg_class_t *                      hg_class;
    uint32_t                          meta_server_id;
    hg_size_t                         total_data_size;
    int                               i;
    hg_handle_t                       client_send_transfer_request_handle;
    struct _pdc_transfer_request_args transfer_args;
    char                              cur_time[64];

    FUNC_ENTER(NULL);
#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    if (!(access_type == PDC_WRITE || access_type == PDC_READ))
        PGOTO_ERROR(FAIL, "Invalid PDC type");

    LOG_DEBUG("rank = %d, data_server_id = %u\n", pdc_client_mpi_rank_g, data_server_id);
    in.access_type = access_type;
    in.remote_unit = unit;
    in.obj_id      = obj_id;

    in.obj_ndim = obj_ndim;
    PDC_copy_region_desc(obj_dims, in.obj_dims, in.obj_ndim, in.obj_ndim);

    // Compute metadata server id
    meta_server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_num_g);

    in.meta_server_id = meta_server_id;

    hg_class = HG_Context_get_class(send_context_g);

    debug_server_id_count[data_server_id]++;

    total_data_size = unit;
    for (i = 0; i < remote_ndim; ++i) {
        total_data_size *= remote_size[i];
    }

    pack_region_metadata(remote_ndim, remote_offset, remote_size, &(in.remote_region));

    if (PDC_Client_try_lookup_server(data_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr, transfer_request_register_id_g,
                       &client_send_transfer_request_handle);

    // Create bulk handle
    hg_ret = HG_Bulk_create(hg_class, 1, (void **)&buf, (hg_size_t *)&total_data_size, HG_BULK_READWRITE,
                            &(in.local_bulk_handle));
    *bulk_handle = in.local_bulk_handle;

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create local bulk data handle");

    hg_atomic_set32(&atomic_work_todo_g, 1);

    hg_ret = HG_Forward(client_send_transfer_request_handle, client_send_transfer_request_rpc_cb,
                        &transfer_args, &in);

#ifdef PDC_TIMING
    if (access_type == PDC_READ) {
        pdc_timings.PDCtransfer_request_start_read_rpc += MPI_Wtime() - start;
    }
    else {
        pdc_timings.PDCtransfer_request_start_write_rpc += MPI_Wtime() - start;
    }
    start = MPI_Wtime();
#endif

    PDC_Client_transfer_pthread_create();

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_transfer_request(): Could not start HG_Forward()");

    PDC_Client_wait_pthread_progress();

#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (access_type == PDC_READ) {
        pdc_timings.PDCtransfer_request_start_read_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_transfer_request_start_read_timestamps, function_start, end);
    }
    else {
        pdc_timings.PDCtransfer_request_start_write_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_transfer_request_start_write_timestamps, function_start, end);
    }
#endif
    *metadata_id = transfer_args.metadata_id;

    if (transfer_args.ret != 1)
        PGOTO_ERROR(FAIL, "Transfer request failed");

    HG_Destroy(client_send_transfer_request_handle);
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_request_status(pdcid_t transfer_request_id, uint32_t data_server_id,
                                   pdc_transfer_status_t *completed)
{
    FUNC_ENTER(NULL);

    perr_t                                   ret_value = SUCCEED;
    hg_return_t                              hg_ret    = HG_SUCCESS;
    transfer_request_status_in_t             in;
    hg_handle_t                              client_send_transfer_request_status_handle;
    struct _pdc_transfer_request_status_args transfer_args;

    in.transfer_request_id = transfer_request_id;

    if (PDC_Client_try_lookup_server(data_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr,
                       transfer_request_status_register_id_g, &client_send_transfer_request_status_handle);

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create local bulk data handle");

    hg_ret = HG_Forward(client_send_transfer_request_status_handle,
                        client_send_transfer_request_status_rpc_cb, &transfer_args, &in);

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (transfer_args.ret != 1)
        PGOTO_ERROR(FAIL, "Transfer request failed");

    HG_Destroy(client_send_transfer_request_status_handle);
    *completed = transfer_args.status;
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_transfer_request_wait(pdcid_t transfer_request_id, uint32_t data_server_id, int access_type)

{
    FUNC_ENTER(NULL);

    perr_t                                 ret_value = SUCCEED;
    hg_return_t                            hg_ret    = HG_SUCCESS;
    transfer_request_wait_in_t             in;
    hg_handle_t                            client_send_transfer_request_wait_handle;
    struct _pdc_transfer_request_wait_args transfer_args;
    char                                   cur_time[64];

    // Join the thread of trasfer start
    /* if (hg_progress_flag_g == 0) { */
    /*     hg_progress_flag_g = 1; */
    /*     pthread_join(hg_progress_tid_g, NULL); */
    /*     hg_progress_flag_g = -1; */
    /* } */

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif

    debug_server_id_count[data_server_id]++;

    in.transfer_request_id = transfer_request_id;
    in.access_type         = access_type;

    if (PDC_Client_try_lookup_server(data_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr,
                       transfer_request_wait_register_id_g, &client_send_transfer_request_wait_handle);

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create local bulk data handle");

    hg_ret = HG_Forward(client_send_transfer_request_wait_handle, client_send_transfer_request_wait_rpc_cb,
                        &transfer_args, &in);
#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (access_type == PDC_READ) {
        pdc_timings.PDCtransfer_request_wait_read_rpc += end - start;
    }
    else {
        pdc_timings.PDCtransfer_request_wait_write_rpc += end - start;
    }
    start = MPI_Wtime();
#endif

    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_transfer_request(): Could not start HG_Forward()");
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (access_type == PDC_READ) {
        pdc_timings.PDCtransfer_request_wait_read_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_transfer_request_wait_read_timestamps, function_start, end);
    }
    else {
        pdc_timings.PDCtransfer_request_wait_write_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_transfer_request_wait_write_timestamps, function_start, end);
    }
#endif

    if (transfer_args.ret != 1)
        PGOTO_ERROR(FAIL, "Transfer request failed");

    HG_Destroy(client_send_transfer_request_wait_handle);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_buf_map(pdcid_t local_region_id, pdcid_t remote_obj_id, size_t ndim, uint64_t *local_dims,
                   uint64_t *local_offset, pdc_var_type_t local_type, void *local_data,
                   pdc_var_type_t remote_type, struct pdc_region_info *local_region,
                   struct pdc_region_info *remote_region, struct _pdc_obj_info *object_info)
{
    FUNC_ENTER(NULL);

    perr_t       ret_value = SUCCEED;
    hg_return_t  hg_ret    = HG_SUCCESS;
    buf_map_in_t in;
    uint32_t     data_server_id, meta_server_id;
    hg_class_t * hg_class;

    hg_uint32_t              i, j;
    hg_uint32_t              local_count;
    void **                  data_ptrs = NULL;
    size_t *                 data_size = NULL;
    size_t                   unit, unit_to;
    struct _pdc_buf_map_args map_args;
    hg_handle_t              client_send_buf_map_handle;

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif

    in.local_reg_id  = local_region_id;
    in.remote_obj_id = remote_obj_id;
    in.local_type    = local_type;
    in.remote_type   = remote_type;
    in.ndim          = ndim;

    // Compute metadata server id
    data_server_id = ((pdc_metadata_t *)object_info->metadata)->data_server_id;
    meta_server_id = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);

    in.meta_server_id = meta_server_id;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[data_server_id]++;

    hg_class = HG_Context_get_class(send_context_g);

    unit = PDC_get_var_type_size(local_type);
    PDC_region_info_t_to_transfer_unit(local_region, &(in.local_region), unit);

    unit_to = PDC_get_var_type_size(remote_type);
    PDC_region_info_t_to_transfer_unit(remote_region, &(in.remote_region_unit), unit_to);
    PDC_region_info_t_to_transfer(remote_region, &(in.remote_region_nounit));
    in.remote_unit = unit_to;

    if (ndim == 1 && local_offset[0] == 0) {
        local_count = 1;
        data_ptrs   = (void **)PDC_malloc(sizeof(void *));
        data_size   = (size_t *)PDC_malloc(sizeof(size_t));
        *data_ptrs  = local_data;
        *data_size  = unit * local_dims[0];
    }
    else if (ndim == 1) {
        local_count = 1;
        data_ptrs   = (void **)PDC_malloc(sizeof(void *));
        data_size   = (size_t *)PDC_malloc(sizeof(size_t));
        *data_ptrs  = local_data + unit * local_offset[0];
        *data_size  = unit * local_dims[0];
        LOG_DEBUG("offset size = %d, local dim = %d, unit = %d, data_ptrs[0] = %d, data_ptrs[1] = %d\n",
                  (int)local_offset[0], (int)local_dims[0], (int)unit, ((int *)data_ptrs)[0],
                  ((int *)data_ptrs)[1]);
    }
    else if (ndim == 2 && local_offset[1] == 0) {
        local_count = 1;
        data_ptrs   = (void **)PDC_malloc(sizeof(void *));
        data_size   = (size_t *)PDC_malloc(sizeof(size_t));
        *data_ptrs  = local_data;
        *data_size  = unit * local_dims[0] * local_dims[1];
    }
    else if (ndim == 2) {
        local_count  = local_dims[0];
        data_ptrs    = (void **)PDC_malloc(local_count * sizeof(void *));
        data_size    = (size_t *)PDC_malloc(local_count * sizeof(size_t));
        data_ptrs[0] = local_data + unit * (local_dims[1] * local_offset[0] + local_offset[1]);
        data_size[0] = local_dims[1];
        data_size[0] = unit * local_dims[1];
        for (i = 1; i < local_dims[0]; i++) {
            data_ptrs[i] = data_ptrs[i - 1] + unit * local_dims[1];
            data_size[i] = data_size[0];
        }
    }
    else if (ndim == 3 && local_offset[2] == 0) {
        local_count = 1;
        data_ptrs   = (void **)PDC_malloc(sizeof(void *));
        data_size   = (size_t *)PDC_malloc(sizeof(size_t));
        *data_ptrs  = local_data;
        *data_size  = unit * local_dims[0] * local_dims[1] * local_dims[2];
    }
    else if (ndim == 3) {
        local_count  = local_dims[0] * local_dims[1];
        data_ptrs    = (void **)PDC_malloc(local_count * sizeof(void *));
        data_size    = (size_t *)PDC_malloc(local_count * sizeof(size_t));
        data_ptrs[0] = local_data + unit * (local_dims[2] * local_dims[1] * local_offset[0] +
                                            local_dims[2] * local_offset[1] + local_offset[2]);
        data_size[0] = local_dims[2];
        data_size[0] = unit * local_dims[2];
        for (i = 0; i < local_dims[0] - 1; i++) {
            for (j = 0; j < local_dims[1] - 1; j++) {
                data_ptrs[i * local_dims[1] + j + 1] =
                    data_ptrs[i * local_dims[1] + j] + unit * local_dims[2];
                data_size[i * local_dims[1] + j + 1] = unit * local_dims[2];
            }
            data_ptrs[i * local_dims[1] + local_dims[1]] =
                data_ptrs[i * local_dims[1]] + unit * local_dims[2] * local_dims[1];
            data_size[i * local_dims[1] + local_dims[1]] = data_size[0];
        }
        i = local_dims[0] - 1;
        for (j = 0; j < local_dims[1] - 1; j++) {
            data_ptrs[i * local_dims[1] + j + 1] = data_ptrs[i * local_dims[1] + j] + unit * local_dims[2];
            data_size[i * local_dims[1] + j + 1] = data_size[0];
        }
    }
    else
        PGOTO_ERROR(FAIL, "Mapping for array of dimension greater than 4 is not supproted");

    if (PDC_Client_try_lookup_server(data_server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr, buf_map_register_id_g,
              &client_send_buf_map_handle);

    // Create bulk handle and release in PDC_Data_Server_buf_unmap()
    hg_ret = HG_Bulk_create(hg_class, local_count, (void **)data_ptrs, (hg_size_t *)data_size,
                            HG_BULK_READWRITE, &(in.local_bulk_handle));
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_buf_map(): Could not create local bulk data handle");

    hg_ret = HG_Forward(client_send_buf_map_handle, client_send_buf_map_rpc_cb, &map_args, &in);
#ifdef PDC_TIMING
    pdc_timings.PDCbuf_obj_map_rpc += MPI_Wtime() - start;
#endif
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_buf_map(): Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
#ifdef PDC_TIMING
    start = MPI_Wtime();
#endif
    PDC_Client_check_response(&send_context_g);
#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_timings.PDCbuf_obj_map_rpc_wait += end - start;
    pdc_timestamp_register(pdc_client_buf_obj_map_timestamps, function_start, end);
#endif
    if (map_args.ret != 1)
        PGOTO_ERROR(FAIL, "buf map failed");

done:
    data_ptrs = (void **)PDC_free(data_ptrs);
    data_size = (size_t *)PDC_free(data_size);
    HG_Destroy(client_send_buf_map_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_region_lock(pdcid_t remote_obj_id, struct _pdc_obj_info *object_info,
                       struct pdc_region_info *region_info, pdc_access_t access_type,
                       pdc_lock_mode_t lock_mode, pdc_var_type_t data_type, pbool_t *status)
{
    FUNC_ENTER(NULL);

    perr_t                       ret_value = SUCCEED;
    hg_return_t                  hg_ret;
    uint32_t                     server_id, meta_server_id;
    region_lock_in_t             in;
    struct _pdc_region_lock_args lookup_args;
    hg_handle_t                  region_lock_handle;

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    server_id      = ((pdc_metadata_t *)object_info->metadata)->data_server_id;
    meta_server_id = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);
    // Compute local data server id
    in.meta_server_id = meta_server_id;
    in.lock_mode      = lock_mode;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // Fill input structure
    in.obj_id = object_info->obj_info_pub->meta_id;

    in.access_type  = access_type;
    in.mapping      = region_info->mapping;
    in.local_reg_id = region_info->local_id;
    size_t ndim     = region_info->ndim;
    in.data_type    = data_type;

    if (ndim >= 4 || ndim <= 0)
        PGOTO_ERROR(FAIL, "Dimension %lu is not supported", ndim);

    in.data_unit = PDC_get_var_type_size(data_type);
    PDC_region_info_t_to_transfer_unit(region_info, &(in.region), in.data_unit);

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_lock_register_id_g,
              &region_lock_handle);

    hg_ret = HG_Forward(region_lock_handle, client_region_lock_rpc_cb, &lookup_args, &in);
#ifdef PDC_TIMING
    if (access_type == PDC_READ) {
        pdc_timings.PDCreg_obtain_lock_read_rpc += MPI_Wtime() - start;
    }
    else {
        pdc_timings.PDCreg_obtain_lock_write_rpc += MPI_Wtime() - start;
    }

#endif
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
#ifdef PDC_TIMING
    start = MPI_Wtime();
#endif
    PDC_Client_check_response(&send_context_g);
#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (access_type == PDC_READ) {
        pdc_timings.PDCreg_obtain_lock_read_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_obtain_lock_read_timestamps, function_start, end);
    }
    else {
        pdc_timings.PDCreg_obtain_lock_write_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_obtain_lock_write_timestamps, function_start, end);
    }
#endif
    // Now the return value is stored in lookup_args.ret
    if (lookup_args.ret == 1) {
        *status   = TRUE;
        ret_value = SUCCEED;
    }
    else {
        *status   = FALSE;
        ret_value = FAIL;
    }

done:
    HG_Destroy(region_lock_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_region_release(pdcid_t remote_obj_id, struct _pdc_obj_info *object_info,
                          struct pdc_region_info *region_info, pdc_access_t access_type,
                          pdc_var_type_t data_type, pbool_t *status)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    // int readyState = 0, currentState;
    hg_return_t      hg_ret;
    uint32_t         server_id, meta_server_id;
    region_lock_in_t in;
    // size_t                         type_extent;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    region_release_handle = HG_HANDLE_NULL;

#ifdef PDC_TIMING
    double start          = MPI_Wtime(), end;
    double function_start = start;
#endif
    // Compute data server and metadata server ids.
    server_id         = ((pdc_metadata_t *)object_info->metadata)->data_server_id;
    meta_server_id    = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // Fill input structure
    in.obj_id       = object_info->obj_info_pub->meta_id;
    in.access_type  = access_type;
    in.mapping      = region_info->mapping;
    in.local_reg_id = region_info->local_id;
    in.data_type    = data_type;
    size_t ndim     = region_info->ndim;

    if (ndim >= 4 || ndim <= 0)
        PGOTO_ERROR(FAIL, "Dimension %lu is not supported", ndim);

    in.data_unit = PDC_get_var_type_size(data_type);
    PDC_region_info_t_to_transfer_unit(region_info, &(in.region), in.data_unit);
    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_release_register_id_g,
              &region_release_handle);

    hg_ret = HG_Forward(region_release_handle, client_region_release_rpc_cb, &lookup_args, &in);
#ifdef PDC_TIMING
    if (access_type == PDC_READ) {
        pdc_timings.PDCreg_release_lock_read_rpc += MPI_Wtime() - start;
    }
    else {
        pdc_timings.PDCreg_release_lock_write_rpc += MPI_Wtime() - start;
    }

    start = MPI_Wtime();

#endif
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);
#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (access_type == PDC_READ) {
        pdc_timings.PDCreg_release_lock_read_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_release_lock_read_timestamps, function_start, end);
    }
    else {
        pdc_timings.PDCreg_release_lock_write_rpc_wait += end - start;
        pdc_timestamp_register(pdc_client_release_lock_write_timestamps, function_start, end);
    }
#endif
    // Now the return value is stored in lookup_args.ret
    if (lookup_args.ret == 1) {
        *status   = TRUE;
        ret_value = SUCCEED;
    }
    else {
        *status   = FALSE;
        ret_value = FAIL;
    }

done:
    if (region_release_handle != HG_HANDLE_NULL)
        HG_Destroy(region_release_handle);

    FUNC_LEAVE(ret_value);
}

/*
 * Data server related
 */

static hg_return_t
data_server_read_check_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_read_check_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret        = -1;
        client_lookup_args->ret_string = " ";
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");
    }

    client_lookup_args->ret = output.ret;
    if (output.shm_addr != NULL) {
        client_lookup_args->ret_string = (char *)PDC_malloc(strlen(output.shm_addr) + 1);
        strcpy(client_lookup_args->ret_string, output.shm_addr);
    }
    else
        client_lookup_args->ret_string = NULL;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// This function is used with server push notification
hg_return_t
PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;

    int                     shm_fd        = -1; // file descriptor, from shm_open()
    uint32_t                i             = 0;
    char *                  shm_base      = NULL; // base address, from mmap()
    char *                  shm_addr      = NULL;
    uint64_t                data_size     = 1;
    client_read_info_t *    read_info     = NULL;
    struct pdc_request *    elt           = NULL;
    struct pdc_region_info *target_region = NULL;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
#endif

    read_info = (client_read_info_t *)callback_info->arg;

    shm_addr = read_info->shm_addr;

    // TODO: Need to find the correct request
    DL_FOREACH(pdc_io_request_list_g, elt)
    {
        if (((pdc_metadata_t *)elt->metadata)->obj_id == read_info->obj_id && elt->access_type == PDC_READ) {
            target_region = elt->region;
            break;
        }
    }

    if (target_region == NULL)
        PGOTO_ERROR(FAIL, "Request region not found");

    data_size = 1;
    for (i = 0; i < target_region->ndim; i++) {
        data_size *= target_region->size[i];
    }

    /* open the shared memory segment as if it was a file */
    shm_fd = shm_open(shm_addr, O_RDONLY, 0666);
    if (shm_fd == -1)
        PGOTO_ERROR(FAIL, "Shared memory open failed [%s]", shm_addr);

    /* map the shared memory segment to the address space of the process */
    shm_base = mmap(0, data_size, PROT_READ, MAP_SHARED, shm_fd, 0);
    if (shm_base == MAP_FAILED) {
        LOG_ERROR("Map failed\n");
        ret_value = FAIL;
        goto close;
    }

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_start, 0);
#endif

    // Copy data
    memcpy(elt->buf, shm_base, data_size);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

#endif

    /* remove the mapped shared memory segment from the address space of the process */
    if (munmap(shm_base, data_size) == -1) {
    }

close:
    /* close the shared memory segment as if it was a file */
    if (close(shm_fd) == -1)
        PGOTO_ERROR(FAIL, "Close failed");

    /* remove the shared memory segment from the file system */
    if (shm_unlink(shm_addr) == -1)
        PGOTO_ERROR(FAIL, "Error removing %s", shm_addr);

done:
    hg_atomic_decr32(&atomic_work_todo_g);

    FUNC_LEAVE(ret_value);
}

// This is used with polling approach to get data from server read
perr_t
PDC_Client_data_server_read_check(int server_id, uint32_t client_id, pdc_metadata_t *meta,
                                  struct pdc_region_info *region, int *status, void *buf)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    struct _pdc_client_lookup_args lookup_args;
    data_server_read_check_in_t    in;
    uint32_t                       i;
    uint64_t                       read_size = 1;
    hg_handle_t                    data_server_read_check_handle;
    int                            shm_fd;   // file descriptor, from shm_open()
    char *                         shm_base; // base address, from mmap()
    char *                         shm_addr;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
#endif

    if (meta == NULL || region == NULL || status == NULL || buf == NULL)
        PGOTO_ERROR(FAIL, "NULL input");

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Invalid server id %d/%d", server_id, pdc_server_num_g);

    // Dummy value fill
    in.client_id = client_id;
    PDC_metadata_t_to_transfer_t(meta, &in.meta);
    PDC_region_info_t_to_transfer(region, &in.region);

    for (i = 0; i < region->ndim; i++) {
        read_size *= region->size[i];
    }

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_check_register_id_g,
              &data_server_read_check_handle);

    hg_ret = HG_Forward(data_server_read_check_handle, data_server_read_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    *status = lookup_args.ret;
    if (lookup_args.ret != 1 && lookup_args.ret != 111) {
        ret_value = SUCCEED;
        if (is_client_debug_g)
            LOG_DEBUG("IO request has not been fulfilled by server\n");
        HG_Destroy(data_server_read_check_handle);
        if (lookup_args.ret == -1)
            ret_value = FAIL;
        PGOTO_DONE(ret_value);
    }
    else {
        shm_addr = lookup_args.ret_string;

        /* open the shared memory segment as if it was a file */
        shm_fd = shm_open(shm_addr, O_RDONLY, 0666);
        if (shm_fd == -1)
            PGOTO_ERROR(FAIL, "Shared memory open failed [%s]", shm_addr);

        /* map the shared memory segment to the address space of the process */
        shm_base = mmap(0, read_size, PROT_READ, MAP_SHARED, shm_fd, 0);
        if (shm_base == MAP_FAILED)
            PGOTO_ERROR(FAIL, "Map failed: %s", strerror(errno));

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_start, 0);
#endif

        // Copy data
        memcpy(buf, shm_base, read_size);

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

        /* remove the mapped shared memory segment from the address space of the process */
        if (munmap(shm_base, read_size) == -1)
            PGOTO_ERROR(FAIL, "Unmap failed: %s", strerror(errno));
        HG_Destroy(data_server_read_check_handle);
    }

    /* close the shared memory segment as if it was a file */
    if (close(shm_fd) == -1)
        PGOTO_ERROR(FAIL, "Close failed");

    lookup_args.ret_string = (char *)PDC_free(lookup_args.ret_string);

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_read_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args;
    data_server_read_out_t          output;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "data_server_read_rpc_cb error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_data_server_read(struct pdc_request *request)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    struct _pdc_client_lookup_args lookup_args;
    data_server_read_in_t          in;
    hg_handle_t                    data_server_read_handle;
    int                            server_id, n_client, n_update;
    pdc_metadata_t *               meta;
    struct pdc_region_info *       region;

    server_id = request->server_id;
    n_client  = request->n_client;
    n_update  = request->n_update;
    meta      = request->metadata;
    region    = request->region;

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Invalid server ID %d/%d", server_id, pdc_server_num_g);

    if (meta == NULL || region == NULL)
        PGOTO_ERROR(FAIL, "Invalid metadata or region");

    // TODO TEMPWORK
    char *tmp_env = getenv("PDC_CACHE_PERCENTAGE");
    int   cache_percentage;
    if (tmp_env != NULL)
        cache_percentage = atoi(tmp_env);
    else
        cache_percentage = 0;

    // Dummy value fill
    in.client_id        = pdc_client_mpi_rank_g;
    in.nclient          = n_client;
    in.nupdate          = n_update;
    in.cache_percentage = cache_percentage;
    if (request->n_update == 0)
        request->n_update = 1; // Only set to default value if it is not set prior
    PDC_metadata_t_to_transfer_t(meta, &in.meta);
    PDC_region_info_t_to_transfer(region, &in.region);

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_register_id_g,
              &data_server_read_handle);

    hg_ret = HG_Forward(data_server_read_handle, data_server_read_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        PGOTO_ERROR(FAIL, "Error from server");

done:
    HG_Destroy(data_server_read_handle);

    FUNC_LEAVE(ret_value);
}

/*
 * Close the shared memory
 */
perr_t
PDC_Client_close_shm(struct pdc_request *req)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (req == NULL || req->shm_fd == 0 || req->shm_base == NULL)
        PGOTO_ERROR(FAIL, "Invalid input");

    /* remove the mapped memory segment from the address space of the process */
    if (munmap(req->shm_base, req->shm_size) == -1)
        PGOTO_ERROR(FAIL, "Unmap failed");

    /* close the shared memory segment as if it was a file */
    if (close(req->shm_fd) == -1)
        PGOTO_ERROR(FAIL, "close shm failed");

done:

    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_write_check_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;

    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_write_check_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "data_server_write_check_rpc_cb error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_data_server_write_check(struct pdc_request *request, int *status)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    struct _pdc_client_lookup_args lookup_args;
    data_server_write_check_in_t   in;
    hg_handle_t                    data_server_write_check_handle;
    int                            server_id;
    pdc_metadata_t *               meta;
    struct pdc_region_info *       region;
    uint64_t                       write_size = 1;
    uint32_t                       i;

    server_id = request->server_id;
    meta      = request->metadata;
    region    = request->region;

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Invalid server id %d/%d", server_id, pdc_server_num_g);

    in.client_id = pdc_client_mpi_rank_g;
    PDC_metadata_t_to_transfer_t(meta, &in.meta);
    PDC_region_info_t_to_transfer(region, &in.region);

    for (i = 0; i < region->ndim; i++) {
        write_size *= region->size[i];
    }

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_write_check_register_id_g,
              &data_server_write_check_handle);

    hg_ret = HG_Forward(data_server_write_check_handle, data_server_write_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    *status = lookup_args.ret;
    if (lookup_args.ret != 1) {

        ret_value = SUCCEED;
        if (is_client_debug_g == 1)
            PGOTO_ERROR(FAIL, "IO request not done by server yet");

        if (lookup_args.ret == -1)
            PGOTO_DONE(FAIL);
    }
    else {
        // Close shm
        PDC_Client_close_shm(request);
    }

done:
    HG_Destroy(data_server_write_check_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_write_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                     ret_value = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;
    data_server_write_out_t         output;

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "data_server_write_rpc_cb error with HG_Get_output");
    }

    client_lookup_args->ret = output.ret;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_data_server_write(struct pdc_request *request)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    uint32_t                       i;
    uint64_t                       region_size = 1;
    struct _pdc_client_lookup_args lookup_args;
    data_server_write_in_t         in;
    int                            server_ret;
    hg_handle_t                    data_server_write_handle;
    int                            server_id, n_client, n_update;
    pdc_metadata_t *               meta;
    struct pdc_region_info *       region;
    void *                         buf;

    int rnd;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
#endif

    server_id = request->server_id;
    n_client  = request->n_client;

    n_update = request->n_update;
    meta     = request->metadata;
    region   = request->region;
    buf      = request->buf;

    if (NULL == meta || NULL == region || NULL == buf)
        PGOTO_ERROR(FAIL, "Input NULL");

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Invalid server id %d/%d", server_id, pdc_server_num_g);

    if (region->ndim > 4)
        PGOTO_ERROR(FAIL, "Invalid dim %lu", region->ndim);

    // Calculate region size
    for (i = 0; i < region->ndim; i++) {
        region_size *= region->size[i];
        if (region_size == 0)
            PGOTO_ERROR(FAIL, "size[%d]=0", i);
    }

    // Create shared memory
    // Shared memory address is /objID_ServerID_ClientID_rand
    rnd = rand();
    sprintf(request->shm_addr, "/%" PRIu64 "_c%d_s%d_%d", meta->obj_id, pdc_client_mpi_rank_g, server_id,
            rnd);

    /* create the shared memory segment as if it was a file */
    request->shm_fd = shm_open(request->shm_addr, O_CREAT | O_RDWR, 0666);
    if (request->shm_fd == -1)
        PGOTO_ERROR(FAIL, "Shared memory creation with shm_open failed");

    /* configure the size of the shared memory segment */
    if (ftruncate(request->shm_fd, region_size) != 0) {
        PGOTO_ERROR(FAIL, "Memory truncate failed");
    }
    request->shm_size = region_size;

    /* map the shared memory segment to the address space of the process */
    request->shm_base = mmap(0, region_size, PROT_READ | PROT_WRITE, MAP_SHARED, request->shm_fd, 0);
    if (request->shm_base == MAP_FAILED)
        PGOTO_ERROR(FAIL, "Shared memory mmap failed, region size = %" PRIu64 "", region_size);
        // close and shm_unlink?

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_start, 0);
#endif

    // Copy the user's buffer to shm that can be accessed by data server
    memcpy(request->shm_base, buf, region_size);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

    // Aggregate the send request when clients of same node are sending requests of same object
    // First check the obj ID are the same among the node local ranks

    // Normal send to server by each process
    meta->data_location[0] = ' ';
    meta->data_location[1] = 0;

    in.client_id = pdc_client_mpi_rank_g;
    in.nclient   = n_client;
    in.nupdate   = n_update;
    in.shm_addr  = request->shm_addr;
    PDC_metadata_t_to_transfer_t(meta, &in.meta);
    PDC_region_info_t_to_transfer(region, &in.region);

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_write_register_id_g,
                       &data_server_write_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not HG_Create()");

    hg_ret = HG_Forward(data_server_write_handle, data_server_write_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        HG_Destroy(data_server_write_handle);
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");
    }

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    HG_Destroy(data_server_write_handle);

    server_ret = lookup_args.ret;

    if (server_ret != 1)
        PGOTO_ERROR(FAIL, "Error from server");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_test(struct pdc_request *request, int *completed)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (request == NULL || completed == NULL)
        PGOTO_ERROR(FAIL, "request and/or completed is NULL");

    if (request->access_type == PDC_READ) {
        ret_value =
            PDC_Client_data_server_read_check(request->server_id, pdc_client_mpi_rank_g, request->metadata,
                                              request->region, completed, request->buf);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "Read check error");
    }
    else if (request->access_type == PDC_WRITE) {
        ret_value = PDC_Client_data_server_write_check(request, completed);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "Write check error");
    }
    else
        PGOTO_ERROR(FAIL, "Error with request access type");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_wait(struct pdc_request *request, unsigned long max_wait_ms, unsigned long check_interval_ms)
{
    FUNC_ENTER(NULL);

    perr_t         ret_value = SUCCEED;
    int            completed = 0;
    struct timeval start_time;
    struct timeval end_time;
    unsigned long  elapsed_ms;
    int            cnt = 0;

    gettimeofday(&start_time, 0);
    // TODO: Calculate region size and estimate the wait time
    // Write is 4-5x faster
    while (completed == 0 && cnt < PDC_MAX_TRIAL_NUM) {
        ret_value = PDC_Client_test(request, &completed);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "PDC_Client_test error");

        gettimeofday(&end_time, 0);
        elapsed_ms =
            ((end_time.tv_sec - start_time.tv_sec) * 1000000LL + end_time.tv_usec - start_time.tv_usec) /
            1000;
        if (elapsed_ms > max_wait_ms) {
            LOG_WARNING("Exceeded max IO request waiting time...\n");
            break;
        }
        PDC_msleep(check_interval_ms);
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_wait(struct pdc_request *request, unsigned long max_wait_ms, unsigned long check_interval_ms)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDC_Client_wait(request, max_wait_ms, check_interval_ms);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_iwrite(pdc_metadata_t *meta, struct pdc_region_info *region, struct pdc_request *request,
                  void *buf)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    request->server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    if (request->n_client == 0)
        request->n_client = pdc_nclient_per_server_g; // Set by env var PDC_NCLIENT_PER_SERVER, default 1
    if (request->n_update == 0)
        request->n_update = 1; // Only set to default value if it is not set prior
    request->access_type = PDC_WRITE;
    request->metadata    = meta;
    request->region      = region;
    request->buf         = buf;

    ret_value = PDC_Client_data_server_write(request);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_iwrite(void *meta, struct pdc_region_info *region, struct pdc_request *request, void *buf)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDC_Client_iwrite((pdc_metadata_t *)meta, region, request, buf);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Client_work_done_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);

    // server_lookup_client_out_t *validate = callback_info->arg;
    hg_atomic_decr32(&atomic_work_todo_g);

    FUNC_LEAVE(HG_SUCCESS);
}

// PDC_Client_write is done using PDC_Client_iwrite and PDC_Client_wait
perr_t
PDC_Client_write(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    FUNC_ENTER(NULL);

    struct pdc_request request;
    perr_t             ret_value = SUCCEED;

    request.n_update = 1;
    request.n_client = 1;
    ret_value        = PDC_Client_iwrite(meta, region, &request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_Client_write - PDC_Client_iwrite error");

    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_Client_write - PDC_Client_wait error");

done:
    FUNC_LEAVE(ret_value);
}

// PDC_Client_write is done using PDC_Client_iwrite and PDC_Client_wait
perr_t
PDC_Client_write_id(pdcid_t local_obj_id, struct pdc_region_info *region, void *buf)
{
    FUNC_ENTER(NULL);

    struct pdc_request   request;
    struct _pdc_id_info *info;

    struct _pdc_obj_info *object;
    pdc_metadata_t *      meta;
    perr_t                ret_value = SUCCEED;

    if ((info = PDC_find_id(local_obj_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", local_obj_id);

    object = (struct _pdc_obj_info *)(info->obj_ptr);
    meta   = object->metadata;
    if (meta == NULL)
        PGOTO_ERROR(FAIL, "metadata is NULL");

    request.n_update = 1;
    request.n_client = 1;
    ret_value        = PDC_Client_iwrite(meta, region, &request, buf);

    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_Client_write - PDC_Client_iwrite error");

    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_Client_write - PDC_Client_wait error");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_iread(pdc_metadata_t *meta, struct pdc_region_info *region, struct pdc_request *request, void *buf)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    request->server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    if (request->n_client == 0)
        request->n_client = pdc_nclient_per_server_g; // Set by env var PDC_NCLIENT_PER_SERVER, default 1
    if (request->n_update == 0)
        request->n_update = 1; // Only set to default value if it is not set prior
    request->access_type = PDC_READ;
    request->metadata    = meta;
    request->region      = region;
    request->buf         = buf;

    ret_value = PDC_Client_data_server_read(request);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_Client_iread- PDC_Client_data_server_read error");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_read(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    FUNC_ENTER(NULL);

    perr_t             ret_value = SUCCEED;
    struct pdc_request request;

    request.n_update = 1;
    ret_value        = PDC_Client_iread(meta, region, &request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_Client_iread error");

    PDC_msleep(500);

    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_Client_wait error");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_write_wait_notify(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    FUNC_ENTER(NULL);

    perr_t              ret_value = SUCCEED;
    struct pdc_request *request   = (struct pdc_request *)PDC_malloc(sizeof(struct pdc_request));

    ret_value = PDC_Client_iwrite(meta, region, request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to send write request to server");

    DL_PREPEND(pdc_io_request_list_g, request);
    LOG_INFO("Finished sending write request to server, waiting for notification\n");

    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    LOG_INFO("Received write finish notification\n");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_read_wait_notify(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    FUNC_ENTER(NULL);

    perr_t              ret_value = SUCCEED;
    struct pdc_request *request   = (struct pdc_request *)PDC_malloc(sizeof(struct pdc_request));

    ret_value = PDC_Client_iread(meta, region, request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to send read request to server");

    DL_PREPEND(pdc_io_request_list_g, request);

    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
PDC_Client_add_del_objects_to_container_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                             ret_value = HG_SUCCESS;
    hg_handle_t                             handle    = callback_info->info.forward.handle;
    pdc_int_ret_t                           bulk_rpc_ret;
    update_region_storage_meta_bulk_args_t *cb_args;

    cb_args = (update_region_storage_meta_bulk_args_t *)callback_info->arg;
    // Sent the bulk handle with rpc and get a response
    ret_value = HG_Get_output(handle, &bulk_rpc_ret);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get output");

    ret_value = HG_Free_output(handle, &bulk_rpc_ret);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free output");

    /* Free memory handle */
    ret_value = HG_Bulk_free(cb_args->bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free bulk data handle");

    HG_Destroy(cb_args->rpc_handle);

done:
    hg_atomic_decr32(&atomic_work_todo_g);

    FUNC_LEAVE(ret_value);
}

// Add/delete a number of objects to one container
perr_t
PDC_Client_add_del_objects_to_container(int nobj, uint64_t *obj_ids, uint64_t cont_meta_id, int op)
{
    FUNC_ENTER(NULL);

    perr_t                     ret_value = SUCCEED;
    hg_return_t                hg_ret    = HG_SUCCESS;
    hg_handle_t                rpc_handle;
    hg_bulk_t                  bulk_handle;
    uint32_t                   server_id;
    hg_size_t                  buf_sizes[3];
    cont_add_del_objs_rpc_in_t bulk_rpc_in;
    // Reuse the existing args structure
    update_region_storage_meta_bulk_args_t cb_args;

    server_id = PDC_get_server_by_obj_id(cont_meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    // Send the bulk handle to the target with RPC
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, cont_add_del_objs_rpc_register_id_g,
                       &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    buf_sizes[0] = sizeof(uint64_t) * nobj;

    /* Register memory */
    hg_ret = HG_Bulk_create(send_class_g, 1, (void **)&obj_ids, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create bulk data handle");

    /* Fill input structure */
    bulk_rpc_in.op          = op;
    bulk_rpc_in.cnt         = nobj;
    bulk_rpc_in.origin      = pdc_client_mpi_rank_g;
    bulk_rpc_in.cont_id     = cont_meta_id;
    bulk_rpc_in.bulk_handle = bulk_handle;

    cb_args.bulk_handle = bulk_handle;
    cb_args.rpc_handle  = rpc_handle;

    /* Forward call to remote addr */
    hg_ret = HG_Forward(rpc_handle, PDC_Client_add_del_objects_to_container_cb, &cb_args, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not forward call");

    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

done:
    FUNC_LEAVE(ret_value);
}

// Add a number of objects to one container
perr_t
PDC_Client_add_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id)
{
    FUNC_ENTER(NULL);

    perr_t               ret_value = SUCCEED;
    int                  i;
    uint64_t *           obj_ids;
    uint64_t             cont_meta_id;
    struct _pdc_id_info *id_info = NULL;

    obj_ids = (uint64_t *)PDC_malloc(sizeof(uint64_t) * nobj);
    for (i = 0; i < nobj; i++) {
        if ((id_info = PDC_find_id(local_obj_ids[i])) == NULL) {
            LOG_ERROR("Failed to find PDC ID: %d\n", local_obj_ids[i]);
            continue;
        }
        obj_ids[i] = ((struct _pdc_obj_info *)(id_info->obj_ptr))->obj_info_pub->meta_id;
    }

    if ((id_info = PDC_find_id(local_cont_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", local_cont_id);
    cont_meta_id = ((struct _pdc_cont_info *)(id_info->obj_ptr))->cont_info_pub->meta_id;

    ret_value = PDC_Client_add_del_objects_to_container(nobj, obj_ids, cont_meta_id, ADD_OBJ);

done:
    FUNC_LEAVE(ret_value);
}

// Delete a number of objects to one container
perr_t
PDC_Client_del_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id)
{
    FUNC_ENTER(NULL);

    perr_t               ret_value = SUCCEED;
    int                  i;
    uint64_t *           obj_ids;
    uint64_t             cont_meta_id;
    struct _pdc_id_info *id_info = NULL;

    obj_ids = (uint64_t *)PDC_malloc(sizeof(uint64_t) * nobj);
    for (i = 0; i < nobj; i++) {
        if ((id_info = PDC_find_id(local_obj_ids[i])) == NULL) {
            LOG_ERROR("Failed to find PDC ID: %d\n", local_obj_ids[i]);
            continue;
        }
        obj_ids[i] = ((struct _pdc_obj_info *)(id_info->obj_ptr))->obj_info_pub->meta_id;
    }

    if ((id_info = PDC_find_id(local_cont_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", local_cont_id);
    cont_meta_id = ((struct _pdc_cont_info *)(id_info->obj_ptr))->cont_info_pub->meta_id;

    ret_value = PDC_Client_add_del_objects_to_container(nobj, obj_ids, cont_meta_id, DEL_OBJ);

done:
    FUNC_LEAVE(ret_value);
}

// Add/delete a number of objects to one container
perr_t
PDC_Client_add_tags_to_container(pdcid_t cont_id, char *tags)
{
    FUNC_ENTER(NULL);

    perr_t                 ret_value = SUCCEED;
    hg_return_t            hg_ret    = HG_SUCCESS;
    hg_handle_t            rpc_handle;
    uint32_t               server_id;
    struct _pdc_id_info *  info;
    struct _pdc_cont_info *object;
    uint64_t               cont_meta_id;
    cont_add_tags_rpc_in_t add_tag_rpc_in;

    if ((info = PDC_find_id(cont_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", cont_id);

    object       = (struct _pdc_cont_info *)(info->obj_ptr);
    cont_meta_id = object->cont_info_pub->meta_id;

    server_id = PDC_get_server_by_obj_id(cont_meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    // Send the bulk handle to the target with RPC
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, cont_add_tags_rpc_register_id_g,
                       &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    add_tag_rpc_in.cont_id = cont_meta_id;

    add_tag_rpc_in.tags = tags;

    /* Forward call to remote addr */
    hg_ret = HG_Forward(rpc_handle, pdc_client_check_int_ret_cb, NULL, &add_tag_rpc_in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not forward call");

    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    ret_value = SUCCEED;

done:
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
container_query_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                       ret_value;
    struct _pdc_container_query_args *client_lookup_args;
    hg_handle_t                       handle;
    container_query_out_t             output;

    client_lookup_args = (struct _pdc_container_query_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");

    client_lookup_args->cont_id = output.cont_id;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Query container with name, retrieve container ID from server
perr_t
PDC_Client_query_container_name(const char *cont_name, uint64_t *cont_meta_id)
{
    FUNC_ENTER(NULL);

    perr_t                           ret_value = SUCCEED;
    hg_return_t                      hg_ret    = 0;
    uint32_t                         hash_name_value, server_id;
    container_query_in_t             in;
    struct _pdc_container_query_args lookup_args;
    hg_handle_t                      container_query_handle;

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(cont_name);
    server_id       = hash_name_value % pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, container_query_register_id_g,
              &container_query_handle);

    // Fill input structure
    in.cont_name = cont_name;

    in.hash_value = hash_name_value;

    hg_ret = HG_Forward(container_query_handle, container_query_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    *cont_meta_id = lookup_args.cont_id;

done:
    HG_Destroy(container_query_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_container_name_col(const char *cont_name, uint64_t *cont_meta_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

#ifdef ENABLE_MPI
    if (pdc_client_mpi_rank_g == 0) {
        ret_value = PDC_Client_query_container_name(cont_name, cont_meta_id);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_query_container_name");
    }

    MPI_Bcast(cont_meta_id, 1, MPI_LONG_LONG, 0, PDC_CLIENT_COMM_WORLD_g);
#else
    PGOTO_ERROR(FAIL, "Calling MPI collective operation without enabling MPI");
#endif

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
PDC_Client_query_read_obj_name_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                             ret_value = HG_SUCCESS;
    hg_handle_t                             handle    = callback_info->info.forward.handle;
    pdc_int_ret_t                           bulk_rpc_ret;
    update_region_storage_meta_bulk_args_t *cb_args;

    cb_args = (update_region_storage_meta_bulk_args_t *)callback_info->arg;

    // Sent the bulk handle with rpc and get a response
    ret_value = HG_Get_output(handle, &bulk_rpc_ret);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not get output");

    ret_value = HG_Free_output(handle, &bulk_rpc_ret);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free output");

    /* Free memory handle */
    ret_value = HG_Bulk_free(cb_args->bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free bulk data handle");

    /* Free other malloced resources*/
    HG_Destroy(cb_args->rpc_handle);

done:
    hg_atomic_decr32(&atomic_work_todo_g);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_add_request_to_list(struct pdc_request **list_head, struct pdc_request *request)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (list_head == NULL || request == NULL)
        PGOTO_ERROR(FAIL, "Invalid input");

    request->seq_id = pdc_io_request_seq_id++;
    DL_PREPEND(*list_head, request);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_del_request_from_list(struct pdc_request **list_head, struct pdc_request *request)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (list_head == NULL || request == NULL)
        PGOTO_ERROR(FAIL, "Invalid input");

    request->seq_id = pdc_io_request_seq_id++;
    DL_DELETE(*list_head, request);

done:
    FUNC_LEAVE(ret_value);
}

struct pdc_request *
PDC_find_request_from_list_by_seq_id(struct pdc_request **list_head, int seq_id)
{
    FUNC_ENTER(NULL);

    struct pdc_request *ret_value = NULL;
    struct pdc_request *elt;

    if (list_head == NULL || seq_id < PDC_SEQ_ID_INIT_VALUE || seq_id > 99999)
        PGOTO_ERROR(NULL, "Invalid input");

    DL_FOREACH(*list_head, elt)
    if (elt->seq_id == seq_id)
        ret_value = elt;

done:
    FUNC_LEAVE(ret_value);
}

// Query and read objects with obj name, read data is stored in user provided buf
perr_t
PDC_Client_query_name_read_entire_obj(int nobj, char **obj_names, void ***out_buf, uint64_t *out_buf_sizes)
{
    FUNC_ENTER(NULL);

    perr_t                   ret_value = SUCCEED;
    hg_return_t              hg_ret    = HG_SUCCESS;
    hg_handle_t              rpc_handle;
    hg_bulk_t                bulk_handle;
    uint32_t                 server_id;
    uint64_t *               buf_sizes, total_size;
    int                      i;
    query_read_obj_name_in_t bulk_rpc_in;
    // Reuse the existing args structure
    update_region_storage_meta_bulk_args_t cb_args;
    struct pdc_request *                   request;

    if (nobj == 0 || obj_names == NULL || out_buf == NULL || out_buf_sizes == NULL)

        PGOTO_ERROR(FAIL, "Invalid input");

    server_id = PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    // Send the bulk handle to the target with RPC
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_read_obj_name_register_id_g,
                       &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    total_size = 0;
    buf_sizes  = (uint64_t *)PDC_calloc(sizeof(uint64_t), nobj);
    for (i = 0; i < nobj; i++) {
        buf_sizes[i] = strlen(obj_names[i]) + 1;
        total_size += buf_sizes[i];
    }

    /* Register memory */
    hg_ret =
        HG_Bulk_create(send_class_g, nobj, (void **)obj_names, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create bulk data handle");

    request               = (struct pdc_request *)PDC_calloc(1, sizeof(struct pdc_request));
    request->server_id    = server_id;
    request->access_type  = PDC_READ;
    request->n_buf_arr    = nobj;
    request->buf_arr      = out_buf;
    request->shm_size_arr = out_buf_sizes;
    PDC_add_request_to_list(&pdc_io_request_list_g, request);

    /* Fill input structure */
    bulk_rpc_in.client_seq_id = request->seq_id;
    bulk_rpc_in.cnt           = nobj;
    bulk_rpc_in.total_size    = total_size;
    bulk_rpc_in.origin        = pdc_client_mpi_rank_g;
    bulk_rpc_in.bulk_handle   = bulk_handle;

    cb_args.bulk_handle = bulk_handle;
    cb_args.rpc_handle  = rpc_handle;

    /* Forward call to remote addr */
    hg_ret = HG_Forward(rpc_handle, PDC_Client_query_read_obj_name_cb, &cb_args, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not forward call");

    // Wait for RPC response
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    // Wait for server to complete all reads
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

done:
    FUNC_LEAVE(ret_value);
}

// Copies the data from server's shm to user buffer
// Assumes the shm_addrs are avialable
perr_t
PDC_Client_complete_read_request(int nbuf, struct pdc_request *req)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    i;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
#endif

    FUNC_ENTER(NULL);

    *req->buf_arr     = (void **)PDC_calloc(nbuf, sizeof(void *));
    req->shm_fd_arr   = (int *)PDC_calloc(nbuf, sizeof(int));
    req->shm_base_arr = (char **)PDC_calloc(nbuf, sizeof(char *));

    for (i = 0; i < nbuf; i++) {
        /* open the shared memory segment as if it was a file */
        req->shm_fd_arr[i] = shm_open(req->shm_addr_arr[i], O_RDONLY, 0666);
        if (req->shm_fd_arr[i] == -1) {
            LOG_ERROR("Shared memory open failed [%s]\n", req->shm_addr_arr[i]);
            continue;
        }

        /* map the shared memory segment to the address space of the process */
        req->shm_base_arr[i] = mmap(0, (req->shm_size_arr)[i], PROT_READ, MAP_SHARED, req->shm_fd_arr[i], 0);
        if (req->shm_base_arr[i] == MAP_FAILED) {
            LOG_ERROR("Map failed\n");
            continue;
        }

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_start, 0);
#endif

        // Copy data
        (*req->buf_arr)[i] = (void *)PDC_malloc((req->shm_size_arr)[i]);
        memcpy((*req->buf_arr)[i], req->shm_base_arr[i], (req->shm_size_arr)[i]);

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

        /* remove the mapped shared memory segment from the address space of the process */
        errno = 0;
        if (munmap(req->shm_base_arr[i], (req->shm_size_arr)[i]) == -1) {
            LOG_ERROR("Unmap failed: %s\n", strerror(errno));
            continue;
        }

        /* close the shared memory segment as if it was a file */
        if (close(req->shm_fd_arr[i]) == -1) {
            LOG_ERROR("Close failed\n");
            continue;
        }

        /* remove the shared memory segment from the file system */
        if (shm_unlink(req->shm_addr_arr[i]) == -1) {
            LOG_ERROR("Error removing %s\n", req->shm_addr_arr[i]);
            continue;
        }
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_read_complete(char *shm_addrs, int size, int n_shm, int seq_id)
{
    FUNC_ENTER(NULL);

    perr_t              ret_value = SUCCEED;
    int                 i, cnt;
    struct pdc_request *request;

    request = PDC_find_request_from_list_by_seq_id(&pdc_io_request_list_g, seq_id);
    if (request == NULL)
        PGOTO_ERROR(FAIL, "Cannot find previous request");

    request->shm_addr_arr = (char **)PDC_calloc(n_shm, sizeof(char *));
    cnt                   = 0;
    for (i = 0; i < size - 1; i++) {

        if (i == 0 || (i > 1 && shm_addrs[i - 1] == 0)) {
            request->shm_addr_arr[cnt] = &shm_addrs[i];
            i += strlen(&shm_addrs[i]);
            cnt++;
            if (cnt >= n_shm)
                break;
        }
    }
    request->shm_size_arr = (uint64_t *)(&shm_addrs[i + 1]);

    PDC_Client_complete_read_request(n_shm, request);

    PDC_del_request_from_list(&pdc_io_request_list_g, request);

done:
    hg_atomic_decr32(&atomic_work_todo_g);

    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
perr_t
PDC_Client_server_checkpoint(uint32_t server_id)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    pdc_int_send_t                 in;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, server_checkpoint_rpc_register_id_g,
                       &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    in.origin = pdc_client_mpi_rank_g;
    hg_ret    = HG_Forward(rpc_handle, pdc_client_check_int_ret_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start forward to server");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

done:
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_all_server_checkpoint()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    i;

    if (pdc_server_num_g == 0)
        PGOTO_ERROR(FAIL, "Server number not initialized");

    // only let client rank 0 send all requests
    if (pdc_client_mpi_rank_g != 0)
        PGOTO_DONE(ret_value);

    for (i = 0; i < pdc_server_num_g; i++)
        ret_value = PDC_Client_server_checkpoint(i);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_send_client_shm_info(uint32_t server_id, char *shm_addr, uint64_t size)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value;
    hg_return_t                    hg_ret;
    send_shm_in_t                  in;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret =
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, send_shm_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    in.client_id = pdc_client_mpi_rank_g;

    in.shm_addr = shm_addr;
    in.size     = size;

    hg_ret = HG_Forward(rpc_handle, pdc_client_check_int_ret_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not forward to server");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

done:
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

static region_list_t *
PDC_get_storage_meta_from_io_list(pdc_data_server_io_list_t **list, region_storage_meta_t *storage_meta)
{
    FUNC_ENTER(NULL);

    pdc_data_server_io_list_t *io_list_elt, *io_list_target = NULL;
    region_list_t *            ret_value = NULL;

    FUNC_ENTER(NULL);

    DL_FOREACH(*list, io_list_elt)
    {
        if (storage_meta->obj_id == io_list_elt->obj_id) {
            io_list_target = io_list_elt;
            break;
        }
    }

    if (io_list_target)
        ret_value = io_list_target->region_list_head;

    // TODO: currently assumes 1 region per object

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_add_storage_meta_to_io_list(pdc_data_server_io_list_t **list, region_storage_meta_t *storage_meta,
                                void *buf)
{
    FUNC_ENTER(NULL);

    pdc_data_server_io_list_t *io_list_elt, *io_list_target = NULL;
    region_list_t *            new_region;
    int                        j;
    perr_t                     ret_value = SUCCEED;

    DL_FOREACH(*list, io_list_elt)
    if (storage_meta->obj_id == io_list_elt->obj_id) {
        io_list_target = io_list_elt;
        break;
    }

    // If not found, create and insert one to the read list
    if (NULL == io_list_target) {
        io_list_target = (pdc_data_server_io_list_t *)PDC_calloc(1, sizeof(pdc_data_server_io_list_t));
        io_list_target->obj_id = storage_meta->obj_id;
        io_list_target->total  = 0;
        io_list_target->count  = 0;
        io_list_target->ndim   = storage_meta->region_transfer.ndim;
        // TODO
        for (j = 0; j < io_list_target->ndim; j++)
            io_list_target->dims[j] = 0;

        DL_APPEND(*list, io_list_target);
    }
    io_list_target->total++;
    io_list_target->count++;

    new_region = (region_list_t *)PDC_calloc(1, sizeof(region_list_t));
    PDC_region_transfer_t_to_list_t(&storage_meta->region_transfer, new_region);
    strcpy(new_region->shm_addr, storage_meta->storage_location);
    new_region->offset        = storage_meta->offset;
    new_region->data_size     = storage_meta->size;
    new_region->is_data_ready = 1;
    new_region->is_io_done    = 1;
    new_region->buf           = buf;
    DL_PREPEND(io_list_target->region_list_head, new_region);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_send_region_storage_meta_shm(uint32_t server_id, int n, region_storage_meta_t *storage_meta)
{
    FUNC_ENTER(NULL);

    perr_t                                 ret_value;
    hg_return_t                            hg_ret;
    bulk_rpc_in_t                          bulk_rpc_in;
    hg_handle_t                            rpc_handle;
    size_t                                 buf_sizes;
    update_region_storage_meta_bulk_args_t cb_args;
    hg_bulk_t                              bulk_handle;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr,
                       send_region_storage_meta_shm_bulk_rpc_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    buf_sizes = n * sizeof(region_storage_meta_t);
    hg_ret    = HG_Bulk_create(send_class_g, 1, (void **)&storage_meta, (const hg_size_t *)&buf_sizes,
                            HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create bulk data handle");

    /* Fill input structure */
    bulk_rpc_in.cnt         = n;
    bulk_rpc_in.origin      = pdc_client_mpi_rank_g;
    bulk_rpc_in.bulk_handle = bulk_handle;

    /* Forward call to remote addr */
    cb_args.bulk_handle = bulk_handle;
    cb_args.rpc_handle  = rpc_handle;
    hg_ret = HG_Forward(rpc_handle, /* reuse */ PDC_Client_query_read_obj_name_cb, &cb_args, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not forward call");

    // Wait for RPC response
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_cp_data_to_local_server(int nobj, region_storage_meta_t **all_storage_meta, void ***buf_arr,
                                   size_t *size_arr)
{
    FUNC_ENTER(NULL);

    perr_t                 ret_value = SUCCEED;
    uint32_t               ndim, server_id;
    uint64_t               total_size = 0, cp_loc = 0;
    void *                 buf = NULL;
    char                   shm_addr[ADDR_MAX];
    int                    i, *total_obj = NULL, ntotal_obj = nobj, *recvcounts = NULL, *displs = NULL;
    region_storage_meta_t *all_region_storage_meta_1d = NULL, *my_region_storage_meta_1d = NULL;

    for (i = 0; i < nobj; i++) {
        total_size += size_arr[i];
        // Add padding for shared memory access, as it must be multiple of page size
        if (size_arr[i] % PAGE_SIZE != 0)
            total_size += PAGE_SIZE - (size_arr[i] % PAGE_SIZE);
    }

    // Create 1 big shm segment
    ret_value = PDC_create_shm_segment_ind(total_size, shm_addr, &buf);

    // Copy data to the shm segment
    for (i = 0; i < nobj; i++) {
        if (NULL == all_storage_meta[i]) {
            LOG_ERROR("NULL storage meta for %dth object\n", i);
            continue;
        }
        ndim = all_storage_meta[i]->region_transfer.ndim;
        if (ndim != 1) {
            LOG_ERROR("PDC only supports for 1D data now (%u)\n", ndim);
            continue;
        }

        // Update the storage location and offset with the shared memory
        memcpy(all_storage_meta[i]->storage_location, shm_addr, ADDR_MAX);
        all_storage_meta[i]->offset = cp_loc;

        memcpy(buf + cp_loc, (*buf_arr)[i], size_arr[i]);
        cp_loc += size_arr[i];
        if (size_arr[i] % PAGE_SIZE != 0)
            cp_loc += PAGE_SIZE - (size_arr[i] % PAGE_SIZE);

        ret_value = PDC_add_storage_meta_to_io_list(&client_cache_list_head_g, all_storage_meta[i], buf);
    }

#ifdef ENABLE_MPI
    displs     = (int *)PDC_malloc(sizeof(int) * pdc_client_same_node_size_g);
    recvcounts = (int *)PDC_malloc(sizeof(int) * pdc_client_same_node_size_g);
    total_obj  = (int *)PDC_malloc(sizeof(int) * pdc_client_same_node_size_g);

    // Gather number of objects to each client
    MPI_Allgather(&nobj, 1, MPI_INT, total_obj, 1, MPI_INT, PDC_SAME_NODE_COMM_g);

    ntotal_obj = 0;
    if (pdc_client_same_node_rank_g == 0) {
        for (i = 0; i < pdc_client_same_node_size_g; i++) {
            ntotal_obj += total_obj[i];
            recvcounts[i] = total_obj[i] * sizeof(region_storage_meta_t);
            if (i == 0)
                displs[i] = 0;
            else
                displs[i] = displs[i - 1] + recvcounts[i - 1];
        }

        all_region_storage_meta_1d =
            (region_storage_meta_t *)PDC_malloc(ntotal_obj * sizeof(region_storage_meta_t));
    }

    // Copy data to 1 buffer
    my_region_storage_meta_1d = (region_storage_meta_t *)PDC_malloc(nobj * sizeof(region_storage_meta_t));
    for (i = 0; i < nobj; i++)
        memcpy(&my_region_storage_meta_1d[i], all_storage_meta[i], sizeof(region_storage_meta_t));

    // Gather all object names to rank 0 of each node
    MPI_Gatherv(my_region_storage_meta_1d, nobj * sizeof(region_storage_meta_t), MPI_CHAR,
                all_region_storage_meta_1d, recvcounts, displs, MPI_CHAR, 0, PDC_SAME_NODE_COMM_g);

    // Send to node local data server
    if (pdc_client_same_node_rank_g == 0) {

        server_id =
            PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);
        ret_value = PDC_send_region_storage_meta_shm(server_id, ntotal_obj, all_region_storage_meta_1d);
    }

    displs                    = (int *)PDC_free(displs);
    recvcounts                = (int *)PDC_free(recvcounts);
    total_obj                 = (int *)PDC_free(total_obj);
    my_region_storage_meta_1d = (region_storage_meta_t *)PDC_free(my_region_storage_meta_1d);
    if (pdc_client_same_node_rank_g == 0)
        all_region_storage_meta_1d = (region_storage_meta_t *)PDC_free(all_region_storage_meta_1d);

#else
    // send to node local server
    all_region_storage_meta_1d = (region_storage_meta_t *)PDC_calloc(nobj, sizeof(region_storage_meta_t));
    for (i = 0; i < nobj; i++)
        memcpy(&all_region_storage_meta_1d[i], all_storage_meta[i], sizeof(region_storage_meta_t));

    server_id = PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);
    ret_value = PDC_send_region_storage_meta_shm(server_id, nobj, all_region_storage_meta_1d);

    all_region_storage_meta_1d = (region_storage_meta_t *)PDC_free(all_region_storage_meta_1d);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_read_with_storage_meta(int nobj, region_storage_meta_t **all_storage_meta, void ***buf_arr,
                                  size_t *size_arr)
{
    FUNC_ENTER(NULL);

    perr_t         ret_value = SUCCEED;
    int            i;
    char *         fname, *prev_fname;
    FILE *         fp_read = NULL;
    uint32_t       ndim;
    uint64_t       req_start, req_count, storage_start, storage_count, file_offset, buf_size;
    size_t         read_bytes;
    region_list_t *cache_region = NULL;

    *buf_arr = (void **)PDC_calloc(sizeof(void *), nobj);

    cache_count_g = 0;
    cache_total_g = nobj * cache_percentage_g / 100;

    // TODO: support for multi-dimensional data
    // Now read the data object one by one
    prev_fname = NULL;
    for (i = 0; i < nobj; i++) {
        if (NULL == all_storage_meta[i]) {
            LOG_ERROR("NULL storage meta for %dth object\n", i);
            continue;
        }

        ndim = all_storage_meta[i]->region_transfer.ndim;
        if (ndim != 1) {
            LOG_ERROR("PDC only supports 1D data now (%u)\n", ndim);
            continue;
        }

        // Check if there is local cache
        if (cache_count_g < cache_total_g) {
            cache_region = PDC_get_storage_meta_from_io_list(&client_cache_list_head_g, all_storage_meta[i]);
            if (cache_region != NULL) {
                buf_size      = all_storage_meta[i]->size;
                (*buf_arr)[i] = PDC_malloc(buf_size);
                memcpy((*buf_arr)[i], cache_region->buf, buf_size);
                cache_count_g++;
                continue;
            }
        }

        fname = all_storage_meta[i]->storage_location;
        // Only opens a new file if necessary
        if (NULL == prev_fname || strcmp(fname, prev_fname) != 0) {
            if (fp_read != NULL) {
                fclose(fp_read);
                fp_read = NULL;
            }
            fp_read = fopen(fname, "r");
            nfopen_g++;
            if (fp_read == NULL) {
                LOG_ERROR("fopen failed [%s] objid %" PRIu64 "\n", fname, all_storage_meta[i]->obj_id);
                prev_fname = fname;
                continue;
            }
        }
        prev_fname = fname;

        // TODO: currently assumes 1d data and 1 storage region per object
        storage_start = all_storage_meta[i]->region_transfer.start[0];
        storage_count = all_storage_meta[i]->region_transfer.count[0];
        req_start     = storage_start;
        req_count     = storage_count;
        file_offset   = all_storage_meta[i]->offset;
        buf_size      = all_storage_meta[i]->size;

        // malloc the buf array, this array should be freed by user afterwards.
        (*buf_arr)[i] = PDC_malloc(buf_size);
        PDC_Client_read_overlap_regions(ndim, &req_start, &req_count, &storage_start, &storage_count, fp_read,
                                        file_offset, (*buf_arr)[i], &read_bytes);
        size_arr[i] = read_bytes;
        if (read_bytes != buf_size) {
            LOG_ERROR("Actual read size %zu is not expected %" PRIu64 "\n", read_bytes, buf_size);
        }

        if (strstr(fname, "PDCcacheBB") != NULL) {
            nread_bb_g++;
            read_bb_size_g += read_bytes / 1048576.0;
        }
    }

    if (fp_read != NULL)
        fclose(fp_read);
    FUNC_LEAVE(ret_value);
}

// Query and retrieve all the storage regions of objects with their names
// It is possible that an object has multiple storage regions, they will be stored sequintially in
// storage_meta The storage_meta is also ordered based on the order in obj_names, the mapping can be
// easily formed by checking the obj_id in region_storage_meta_t
perr_t
PDC_Client_query_multi_storage_info(int nobj, char **obj_names, region_storage_meta_t ***all_storage_meta)
{
    FUNC_ENTER(NULL);

    perr_t      ret_value = SUCCEED;
    hg_return_t hg_ret    = HG_SUCCESS;
    hg_handle_t rpc_handle;

    hg_bulk_t                bulk_handle;
    uint32_t                 server_id;
    uint64_t *               buf_sizes = NULL, total_size;
    int                      i, j, loc, iter, *n_obj_name_by_server = NULL;
    int **                   obj_names_server_seq_mapping = NULL, *obj_names_server_seq_mapping_1d;
    int                      send_n_request               = 0;
    char ***                 obj_names_by_server          = NULL;
    char **                  obj_names_by_server_2d       = NULL;
    query_read_obj_name_in_t bulk_rpc_in;
    update_region_storage_meta_bulk_args_t cb_args;
    struct pdc_request **                  requests, *request;

    if (nobj == 0)
        PGOTO_DONE(SUCCEED);
    else if (obj_names == NULL || all_storage_meta == NULL)
        PGOTO_ERROR(FAIL, "Invalid input");

    // One request to each metadata server
    requests = (struct pdc_request **)PDC_calloc(sizeof(struct pdc_request *), pdc_server_num_g);

    obj_names_by_server             = (char ***)PDC_calloc(sizeof(char **), pdc_server_num_g);
    n_obj_name_by_server            = (int *)PDC_calloc(sizeof(int), pdc_server_num_g);
    obj_names_server_seq_mapping    = (int **)PDC_calloc(sizeof(int *), pdc_server_num_g);
    obj_names_server_seq_mapping_1d = (int *)PDC_calloc(sizeof(int), nobj * pdc_server_num_g);
    for (i = 0; i < pdc_server_num_g; i++) {
        obj_names_by_server[i]          = (char **)PDC_calloc(sizeof(char *), nobj);
        obj_names_server_seq_mapping[i] = obj_names_server_seq_mapping_1d + i * nobj;
    }

    // Sort obj_names based on their metadata server id
    for (i = 0; i < nobj; i++) {
        server_id = PDC_get_server_by_name(obj_names[i], pdc_server_num_g);
        obj_names_by_server[server_id][n_obj_name_by_server[server_id]]          = obj_names[i];
        obj_names_server_seq_mapping[server_id][n_obj_name_by_server[server_id]] = i;
        n_obj_name_by_server[server_id]++;
    }

    // Now send the corresponding names to each server that has its metadata
    for (iter = 0; iter < pdc_server_num_g; iter++) {
        // Avoid everyone sends request to the same metadata server at the same time
        server_id = (iter + pdc_client_mpi_rank_g) % pdc_server_num_g;

        if (n_obj_name_by_server[server_id] == 0) {
            continue;
        }
        send_n_request++;
        debug_server_id_count[server_id]++;

        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        // Send the bulk handle to the target with RPC
        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr,
                           query_read_obj_name_client_register_id_g, &rpc_handle);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not create handle");

        total_size = 0;
        buf_sizes  = (uint64_t *)PDC_calloc(sizeof(uint64_t), n_obj_name_by_server[server_id]);
        for (i = 0; i < n_obj_name_by_server[server_id]; i++) {
            buf_sizes[i] = strlen(obj_names_by_server[server_id][i]) + 1;
            total_size += buf_sizes[i];
        }

        hg_ret = HG_Bulk_create(send_class_g, n_obj_name_by_server[server_id],
                                (void **)obj_names_by_server[server_id], buf_sizes, HG_BULK_READ_ONLY,
                                &bulk_handle);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not create bulk data handle");

        requests[server_id] = (struct pdc_request *)PDC_calloc(1, sizeof(struct pdc_request));

        requests[server_id]->server_id   = server_id;
        requests[server_id]->access_type = PDC_READ;
        requests[server_id]->n_buf_arr   = n_obj_name_by_server[server_id];
        requests[server_id]->buf_arr_idx = obj_names_server_seq_mapping[server_id];
        PDC_add_request_to_list(&pdc_io_request_list_g, requests[server_id]);

        /* Fill input structure */
        bulk_rpc_in.client_seq_id = requests[server_id]->seq_id;
        bulk_rpc_in.cnt           = n_obj_name_by_server[server_id];
        bulk_rpc_in.total_size    = total_size;
        bulk_rpc_in.origin        = pdc_client_mpi_rank_g;
        bulk_rpc_in.bulk_handle   = bulk_handle;

        cb_args.bulk_handle = bulk_handle;
        cb_args.rpc_handle  = rpc_handle;

        /* Forward call to remote addr */
        hg_ret = HG_Forward(rpc_handle, PDC_Client_query_read_obj_name_cb, &cb_args, &bulk_rpc_in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not forward call");

        // Wait for RPC response
        hg_atomic_set32(&atomic_work_todo_g, 1);
        PDC_Client_check_response(&send_context_g);

        // Wait for server initiated bulk xfer
        hg_atomic_set32(&atomic_work_todo_g, 1);
        PDC_Client_check_response(&send_context_g);
    } // End for each meta server

    // Now we have all the storage meta stored in the requests structure
    // Reorgaze them and fill the output buffer
    (*all_storage_meta) = (region_storage_meta_t **)PDC_calloc(sizeof(region_storage_meta_t *), nobj);
    for (iter = 0; iter < pdc_server_num_g; iter++) {
        request = requests[iter];
        if (request == NULL || request->storage_meta == NULL)
            continue;

        // Number of storage meta received
        // TODO: currently assumes 1 storage region per object, see the other comment
        for (j = 0; j < ((process_bulk_storage_meta_args_t *)request->storage_meta)->n_storage_meta; j++) {
            loc = obj_names_server_seq_mapping[iter][j];
            (*all_storage_meta)[loc] =
                &(((process_bulk_storage_meta_args_t *)request->storage_meta)->all_storage_meta[j]);
        }
    }

done:
    if (NULL != obj_names_by_server)
        obj_names_by_server = (char ***)PDC_free(obj_names_by_server);
    if (NULL != n_obj_name_by_server)
        n_obj_name_by_server = (int *)PDC_free(n_obj_name_by_server);
    if (NULL != obj_names_by_server_2d)
        obj_names_by_server_2d = (char **)PDC_free(obj_names_by_server_2d);

    FUNC_LEAVE(ret_value);
}

#if defined(ENABLE_MPI) && defined(ENABLE_TIMING)
perr_t
PDC_get_io_stats_mpi(double read_time, double query_time, int nfopen)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    double read_time_max, read_time_min, read_time_avg, reduce_overhead;
    double query_time_max, query_time_min, query_time_avg;
    int    nfopen_min, nfopen_max, nfopen_avg;

    reduce_overhead = MPI_Wtime();
    MPI_Reduce(&read_time, &read_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, PDC_CLIENT_COMM_WORLD_g);
    MPI_Reduce(&read_time, &read_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, PDC_CLIENT_COMM_WORLD_g);
    MPI_Reduce(&read_time, &read_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, PDC_CLIENT_COMM_WORLD_g);
    read_time_avg /= pdc_client_mpi_size_g;

    MPI_Reduce(&query_time, &query_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, PDC_CLIENT_COMM_WORLD_g);
    MPI_Reduce(&query_time, &query_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, PDC_CLIENT_COMM_WORLD_g);
    MPI_Reduce(&query_time, &query_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, PDC_CLIENT_COMM_WORLD_g);
    query_time_avg /= pdc_client_mpi_size_g;

    MPI_Reduce(&nfopen, &nfopen_max, 1, MPI_INT, MPI_MAX, 0, PDC_CLIENT_COMM_WORLD_g);
    MPI_Reduce(&nfopen, &nfopen_min, 1, MPI_INT, MPI_MIN, 0, PDC_CLIENT_COMM_WORLD_g);
    MPI_Reduce(&nfopen, &nfopen_avg, 1, MPI_INT, MPI_SUM, 0, PDC_CLIENT_COMM_WORLD_g);
    nfopen_avg /= pdc_client_mpi_size_g;

    reduce_overhead = MPI_Wtime() - reduce_overhead;
    if (pdc_client_mpi_rank_g == 0) {
        LOG_INFO("IO STATS (MIN, AVG, MAX)\n"
                 "              #fopen   (%d, %d, %d)\n"
                 "              Tquery   (%6.4f, %6.4f, %6.4f)\n"
                 "              #readBB %d, size %.2f MB\n"
                 "              Tread    (%6.4f, %6.4f, %6.4f)\nMPI overhead %.4f\n",
                 nfopen_min, nfopen_avg, nfopen_max, query_time_min, query_time_avg, query_time_max,
                 nread_bb_g, read_bb_size_g, read_time_min, read_time_avg, read_time_max, reduce_overhead);
    }

    FUNC_LEAVE(ret_value);
}
#endif

perr_t
PDC_Client_query_name_read_entire_obj_client(int nobj, char **obj_names, void ***out_buf,
                                             uint64_t *out_buf_sizes)
{
    FUNC_ENTER(NULL);

    perr_t                  ret_value        = SUCCEED;
    region_storage_meta_t **all_storage_meta = NULL;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer1;
    struct timeval pdc_timer2;
    double         query_time, read_time;
#endif

    if (nobj == 0 || obj_names == NULL || out_buf == NULL || out_buf_sizes == NULL)
        PGOTO_ERROR(FAIL, "Invalid input");

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer1, 0);
#endif

    // Get the storage info for all objects, query results store in all_storage_meta
    ret_value = PDC_Client_query_multi_storage_info(nobj, obj_names, &all_storage_meta);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer2, 0);
    query_time = PDC_get_elapsed_time_double(&pdc_timer1, &pdc_timer2);
    query_time_g += query_time;
#endif

    // Now we have all the storage metadata of all requests, start reading them all
    ret_value = PDC_Client_read_with_storage_meta(nobj, all_storage_meta, out_buf, (size_t *)out_buf_sizes);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer1, 0);
    read_time = PDC_get_elapsed_time_double(&pdc_timer2, &pdc_timer1);
    read_time_g += read_time;
#ifdef ENABLE_MPI
    PDC_get_io_stats_mpi(read_time, query_time, nfopen_g);
#endif
#endif

    if (cache_percentage_g == 100)
        ret_value =
            PDC_Client_cp_data_to_local_server(nobj, all_storage_meta, out_buf, (size_t *)out_buf_sizes);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_name_read_entire_obj_client_agg(int my_nobj, char **my_obj_names, void ***out_buf,
                                                 size_t *out_buf_sizes)
{
    FUNC_ENTER(NULL);

    perr_t                  ret_value = SUCCEED;
    char **                 all_names = my_obj_names;
    char *                  local_names_1d, *all_names_1d = NULL;
    int *                   total_obj = NULL, i, ntotal_obj = my_nobj, *recvcounts = NULL, *displs = NULL;
    size_t                  max_name_len     = 64;
    region_storage_meta_t **all_storage_meta = NULL, **my_storage_meta = NULL;
    region_storage_meta_t * my_storage_meta_1d = NULL, *res_storage_meta_1d = NULL;

#ifdef ENABLE_TIMING
    struct timeval pdc_timer1;
    struct timeval pdc_timer2;
    double         query_time, read_time;
#endif

#ifdef ENABLE_MPI
    local_names_1d = (char *)PDC_calloc(my_nobj, max_name_len);
    for (i = 0; i < my_nobj; i++) {
        if (strlen(my_obj_names[i]) > max_name_len)
            LOG_ERROR("Object name longer than %lu [%s]\n", max_name_len, my_obj_names[i]);
        strncpy(local_names_1d + i * max_name_len, my_obj_names[i], max_name_len - 1);
    }

    displs     = (int *)PDC_malloc(sizeof(int) * pdc_client_same_node_size_g);
    recvcounts = (int *)PDC_malloc(sizeof(int) * pdc_client_same_node_size_g);
    total_obj  = (int *)PDC_malloc(sizeof(int) * pdc_client_same_node_size_g);
    MPI_Allgather(&my_nobj, 1, MPI_INT, total_obj, 1, MPI_INT, PDC_SAME_NODE_COMM_g);

    ntotal_obj = 0;
    if (pdc_client_same_node_rank_g == 0) {
        for (i = 0; i < pdc_client_same_node_size_g; i++) {

            ntotal_obj += total_obj[i];
            recvcounts[i] = total_obj[i] * max_name_len;
            if (i == 0)
                displs[i] = 0;

            else
                displs[i] = displs[i - 1] + recvcounts[i - 1];
        }
    }

    if (pdc_client_same_node_rank_g == 0) {
        all_names    = (char **)PDC_calloc(sizeof(char *), ntotal_obj);
        all_names_1d = (char *)PDC_malloc(ntotal_obj * max_name_len);
        for (i = 0; i < ntotal_obj; i++)
            all_names[i] = all_names_1d + i * max_name_len;
    }

    // Gather all object names to rank 0 of each node
    MPI_Gatherv(local_names_1d, my_nobj * max_name_len, MPI_CHAR, all_names_1d, recvcounts, displs, MPI_CHAR,
                0, PDC_SAME_NODE_COMM_g);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer1, 0);
#endif

    // rank 0 on each node sends the query
    if (pdc_client_same_node_rank_g == 0) {
        ret_value = PDC_Client_query_multi_storage_info(ntotal_obj, all_names, &all_storage_meta);

        // Copy the result to the result array for scatter
        // res_storage_meta_1d = (region_storage_meta_t *)PDC_calloc(sizeof(region_storage_meta_t),
        // ntotal_obj);
        res_storage_meta_1d = (region_storage_meta_t *)PDC_malloc(sizeof(region_storage_meta_t) * ntotal_obj);
        for (i = 0; i < ntotal_obj; i++)
            memcpy(&res_storage_meta_1d[i], all_storage_meta[i], sizeof(region_storage_meta_t));
    }

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer2, 0);
    query_time = PDC_get_elapsed_time_double(&pdc_timer1, &pdc_timer2);
    query_time_g += query_time;
#endif

    // allocate space for storage meta results
    my_storage_meta    = (region_storage_meta_t **)PDC_calloc(sizeof(region_storage_meta_t *), my_nobj);
    my_storage_meta_1d = (region_storage_meta_t *)PDC_calloc(sizeof(region_storage_meta_t), my_nobj);
    for (i = 0; i < my_nobj; i++)
        my_storage_meta[i] = &(my_storage_meta_1d[i]);

    // Now rank 0 of each node distribute the query result
    for (i = 0; i < pdc_client_same_node_size_g; i++) {
        recvcounts[i] = total_obj[i] * sizeof(region_storage_meta_t);
        if (i == 0)
            displs[i] = 0;
        else
            displs[i] = displs[i - 1] + recvcounts[i - 1];
    }
    MPI_Scatterv(res_storage_meta_1d, recvcounts, displs, MPI_CHAR, my_storage_meta_1d,
                 my_nobj * sizeof(region_storage_meta_t), MPI_CHAR, 0, PDC_SAME_NODE_COMM_g);

    // Read
#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer2, 0);
#endif

    // Now we have all the storage metadata of all requests, start reading them all
    ret_value = PDC_Client_read_with_storage_meta(my_nobj, my_storage_meta, out_buf, out_buf_sizes);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer1, 0);
    read_time = PDC_get_elapsed_time_double(&pdc_timer2, &pdc_timer1);
    read_time_g += read_time;
#ifdef ENABLE_MPI
    PDC_get_io_stats_mpi(read_time, query_time, nfopen_g);
#endif
#endif

    if (cache_percentage_g == 100)
        ret_value = PDC_Client_cp_data_to_local_server(ntotal_obj, my_storage_meta, out_buf, out_buf_sizes);

#else
    // MPI is disabled
    ret_value = PDC_Client_query_name_read_entire_obj_client(my_nobj, my_obj_names, out_buf,
                                                             (uint64_t *)out_buf_sizes);
#endif

#ifdef ENABLE_MPI
    if (pdc_client_same_node_rank_g == 0) {
        all_names    = (char **)PDC_free(all_names);
        all_names_1d = (char *)PDC_free(all_names_1d);
    }
    recvcounts = (int *)PDC_free(recvcounts);
    displs     = (int *)PDC_free(displs);
    total_obj  = (int *)PDC_free(total_obj);
    if (NULL != my_storage_meta)
        my_storage_meta = (region_storage_meta_t **)PDC_free(my_storage_meta);
    if (NULL != my_storage_meta_1d)
        my_storage_meta_1d = (region_storage_meta_t *)PDC_free(my_storage_meta_1d);
#endif

    FUNC_LEAVE(ret_value);
}

// Process the storage metadata received from bulk transfer
perr_t
PDC_Client_recv_bulk_storage_meta(process_bulk_storage_meta_args_t *process_args)
{
    FUNC_ENTER(NULL);

    perr_t              ret_value = SUCCEED;
    struct pdc_request *request;

    if (NULL == process_args)
        PGOTO_ERROR(FAIL, "NULL input");

    // Now find the task and assign the storage meta to corresponding query request
    request = PDC_find_request_from_list_by_seq_id(&pdc_io_request_list_g, process_args->seq_id);
    if (NULL == request)
        PGOTO_ERROR(FAIL, "Cannot find previous IO request");

    // Attach the received storage meta to the request
    request->storage_meta = process_args;

    ret_value = PDC_del_request_from_list(&pdc_io_request_list_g, request);

done:
    hg_atomic_decr32(&atomic_work_todo_g);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Client_recv_bulk_storage_meta_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    PDC_Client_recv_bulk_storage_meta((process_bulk_storage_meta_args_t *)callback_info->arg);

    FUNC_LEAVE(HG_SUCCESS);
}

perr_t
PDC_Client_read_overlap_regions(uint32_t ndim, uint64_t *req_start, uint64_t *req_count,
                                uint64_t *storage_start, uint64_t *storage_count, FILE *fp,
                                uint64_t file_offset, void *buf, size_t *total_read_bytes)
{
    FUNC_ENTER(NULL);

    perr_t   ret_value              = SUCCEED;
    uint64_t overlap_start[DIM_MAX] = {0}, overlap_count[DIM_MAX] = {0};
    uint64_t buf_start[DIM_MAX]              = {0};
    uint64_t storage_start_physical[DIM_MAX] = {0};
    uint64_t buf_offset = 0, storage_offset = file_offset, total_bytes = 0, read_bytes = 0, row_offset = 0;
    uint64_t i = 0, j = 0;
    int      is_all_selected = 0;
    int      n_contig_read   = 0;
    double   n_contig_MB     = 0.0;
    uint64_t cur_off;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start1;
    struct timeval pdc_timer_end1;
#endif

    *total_read_bytes = 0;
    if (ndim > 3 || ndim <= 0)
        PGOTO_ERROR(FAIL, "dim=%" PRIu32 " unsupported yet", ndim);

    // Get the actual start and count of region in storage
    if (PDC_get_overlap_start_count(ndim, req_start, req_count, storage_start, storage_count, overlap_start,
                                    overlap_count) != SUCCEED)
        PGOTO_ERROR(FAIL, "PDC_get_overlap_start_count failed");

    total_bytes = 1;
    for (i = 0; i < ndim; i++) {
        total_bytes *= overlap_count[i];
        buf_start[i]              = overlap_start[i] - req_start[i];
        storage_start_physical[i] = overlap_start[i] - storage_start[i];
        if (i == 0) {
            buf_offset = buf_start[0];
            storage_offset += storage_start_physical[0];
        }
        else if (i == 1) {
            buf_offset += buf_start[1] * req_count[0];
            storage_offset += storage_start_physical[1] * storage_count[0];
        }
        else if (i == 2) {
            buf_offset += buf_start[2] * req_count[0] * req_count[1];
            storage_offset += storage_start_physical[2] * storage_count[0] * storage_count[1];
        }
    }

    // Check if the entire storage region is selected
    is_all_selected = 1;
    for (i = 0; i < ndim; i++) {
        if (overlap_start[i] != storage_start[i] || overlap_count[i] != storage_count[i]) {
            is_all_selected = -1;
            break;
        }
    }

    // TODO: additional optimization to check if any dimension is entirely selected
    if (ndim == 1 || is_all_selected == 1) {
        // Can read the entire storage region at once

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_start1, 0);
#endif

        // Check if current file ptr is at correct pos
        cur_off = (uint64_t)ftell(fp);
        if (cur_off != storage_offset) {
            fseek(fp, storage_offset, SEEK_SET);
        }

        if (is_client_debug_g == 1) {
            LOG_DEBUG("Read storage offset %" PRIu64 ", buf_offset  %" PRIu64 "\n", storage_offset,
                      buf_offset);
        }

        read_bytes = fread(buf + buf_offset, 1, total_bytes, fp);

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end1, 0);
        double region_read_time1 = PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
        if (is_client_debug_g)
            LOG_DEBUG("fseek + fread %" PRIu64 " bytes, %.2fs\n", read_bytes, region_read_time1);
#endif

        n_contig_MB += read_bytes / 1048576.0;
        n_contig_read++;
        if (read_bytes != total_bytes)
            PGOTO_ERROR(FAIL, "fread failed actual read bytes %" PRIu64 ", should be %" PRIu64 "\n",
                        read_bytes, total_bytes);

        *total_read_bytes += read_bytes;

        if (is_client_debug_g == 1) {
            LOG_DEBUG("Read entire storage region, size=%" PRIu64 "\n", read_bytes);
        }
    } // end if
    else {
        // NOTE: assuming row major, read overlapping region row by row
        if (ndim == 2) {
            row_offset = 0;
            fseek(fp, storage_offset, SEEK_SET);
            for (i = 0; i < overlap_count[1]; i++) {
                // Move to next row's begining position
                if (i != 0) {
                    fseek(fp, storage_count[0] - overlap_count[0], SEEK_CUR);
                    row_offset = i * req_count[0];
                }
                read_bytes = fread(buf + buf_offset + row_offset, 1, overlap_count[0], fp);
                n_contig_MB += read_bytes / 1048576.0;
                n_contig_read++;
                if (read_bytes != overlap_count[0])
                    PGOTO_ERROR(FAIL, "fread failed");

                *total_read_bytes += read_bytes;
            } // for each row
        }     // ndim=2
        else if (ndim == 3) {

            if (is_client_debug_g == 1) {
                LOG_INFO("read count: %" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n", overlap_count[0],
                         overlap_count[1], overlap_count[2]);
            }

            uint64_t buf_serialize_offset;
            for (j = 0; j < overlap_count[2]; j++) {

                fseek(fp, storage_offset + j * storage_count[0] * storage_count[1], SEEK_SET);
                for (i = 0; i < overlap_count[1]; i++) {

                    // Move to next row's begining position
                    if (i != 0)
                        fseek(fp, storage_count[0] - overlap_count[0], SEEK_CUR);

                    buf_serialize_offset = buf_offset + i * req_count[0] + j * req_count[0] * req_count[1];
                    if (is_client_debug_g == 1) {
                        LOG_INFO("Read to buf offset: %" PRIu64 "\n", buf_serialize_offset);
                    }

                    read_bytes = fread(buf + buf_serialize_offset, 1, overlap_count[0], fp);
                    n_contig_MB += read_bytes / 1048576.0;
                    n_contig_read++;
                    if (read_bytes != overlap_count[0])
                        PGOTO_ERROR(FAIL, "fread failed");

                    *total_read_bytes += read_bytes;
                    if (is_client_debug_g == 1) {
                        LOG_INFO("z: %" PRIu64 ", j: %" PRIu64 ", Read data size=%" PRIu64 ": [%.*s]\n", j, i,
                                 overlap_count[0], (int)overlap_count[0], (char *)buf + buf_serialize_offset);
                    }
                } // for each row
            }
        }
    } // end else (ndim != 1 && !is_all_selected);

    if (total_bytes != *total_read_bytes)
        PGOTO_ERROR(FAIL, "Read size error");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_name_read_entire_obj_client_agg_cache_iter(int my_nobj, char **my_obj_names, void ***out_buf,
                                                            size_t *out_buf_sizes, int cache_percentage)
{
    FUNC_ENTER(NULL);

    perr_t ret_value   = SUCCEED;
    cache_percentage_g = cache_percentage;
    ret_value =
        PDC_Client_query_name_read_entire_obj_client_agg(my_nobj, my_obj_names, out_buf, out_buf_sizes);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_add_kvtag(pdcid_t obj_id, pdc_kvtag_t *kvtag, int is_cont)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    uint64_t                       meta_id;
    uint32_t                       server_id;
    hg_handle_t                    metadata_add_kvtag_handle;
    metadata_add_kvtag_in_t        in;
    struct _pdc_obj_info *         obj_prop;
    struct _pdc_cont_info *        cont_prop;
    struct _pdc_client_lookup_args lookup_args;

    if (is_cont == 0) {
        obj_prop      = PDC_obj_get_info(obj_id);
        meta_id       = obj_prop->obj_info_pub->meta_id;
        in.obj_id     = meta_id;
        in.hash_value = PDC_get_hash_by_name(obj_prop->obj_info_pub->name);
    }
    else {
        cont_prop     = PDC_cont_get_info(obj_id);
        meta_id       = cont_prop->cont_info_pub->meta_id;
        in.obj_id     = meta_id;
        in.hash_value = PDC_get_hash_by_name(cont_prop->cont_info_pub->name);
    }

    server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_add_kvtag_register_id_g,
              &metadata_add_kvtag_handle);

    // Fill input structure

    if (kvtag != NULL && kvtag != NULL && kvtag->size != 0) {
        in.kvtag.name  = kvtag->name;
        in.kvtag.value = kvtag->value;
        in.kvtag.type  = kvtag->type;
        in.kvtag.size  = kvtag->size;
    }
    else
        PGOTO_ERROR(FAIL, "Invalid tag content");

    hg_ret = HG_Forward(metadata_add_kvtag_handle, metadata_add_tag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        LOG_ERROR("Add kvtag NOT successful, ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_add_kvtag_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
metadata_get_kvtag_rpc_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                 ret_value          = HG_SUCCESS;
    struct _pdc_get_kvtag_args *client_lookup_args = (struct _pdc_get_kvtag_args *)callback_info->arg;
    hg_handle_t                 handle             = callback_info->info.forward.handle;
    metadata_get_kvtag_out_t    output;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "Error with HG_Get_output");
    }
    client_lookup_args->ret = output.ret;
    if (output.kvtag.name)
        client_lookup_args->kvtag->name = strdup(output.kvtag.name);
    client_lookup_args->kvtag->size = output.kvtag.size;
    client_lookup_args->kvtag->type = output.kvtag.type;
    if (output.kvtag.size > 0) {
        client_lookup_args->kvtag->value = PDC_malloc(output.kvtag.size);
        memcpy(client_lookup_args->kvtag->value, output.kvtag.value, output.kvtag.size);
    }
    else
        client_lookup_args->kvtag->value = NULL;

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_get_kvtag(pdcid_t obj_id, char *tag_name, pdc_kvtag_t **kvtag, int is_cont)
{
    FUNC_ENTER(NULL);

    perr_t                     ret_value = SUCCEED;
    hg_return_t                hg_ret    = 0;
    uint64_t                   meta_id;
    uint32_t                   server_id;
    hg_handle_t                metadata_get_kvtag_handle;
    metadata_get_kvtag_in_t    in;
    struct _pdc_get_kvtag_args lookup_args;
    struct _pdc_obj_info *     obj_prop;
    struct _pdc_cont_info *    cont_prop;

    if (is_cont == 0) {
        obj_prop      = PDC_obj_get_info(obj_id);
        meta_id       = obj_prop->obj_info_pub->meta_id;
        in.obj_id     = meta_id;
        in.hash_value = PDC_get_hash_by_name(obj_prop->obj_info_pub->name);
    }
    else {
        cont_prop     = PDC_cont_get_info(obj_id);
        meta_id       = cont_prop->cont_info_pub->meta_id;
        in.obj_id     = meta_id;
        in.hash_value = PDC_get_hash_by_name(cont_prop->cont_info_pub->name);
    }

    server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_get_kvtag_register_id_g,
              &metadata_get_kvtag_handle);

    if (tag_name != NULL && kvtag != NULL) {
        in.key = tag_name;
    }
    else
        PGOTO_ERROR(FAIL, "PDC_get_kvtag: invalid tag content");

    *kvtag            = (pdc_kvtag_t *)PDC_malloc(sizeof(pdc_kvtag_t));
    lookup_args.kvtag = *kvtag;
    hg_ret            = HG_Forward(metadata_get_kvtag_handle, metadata_get_kvtag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_get_kvtag: Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        LOG_INFO("Get kvtag NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_get_kvtag_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
kvtag_query_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    FUNC_ENTER(NULL);

    hg_return_t         ret_value = HG_SUCCESS;
    struct bulk_args_t *bulk_args;
    hg_bulk_t           origin_bulk_handle = hg_cb_info->info.bulk.origin_handle;
    hg_bulk_t           local_bulk_handle  = hg_cb_info->info.bulk.local_handle;
    uint32_t            n_meta, actual_cnt;
    void *              buf = NULL;
    uint64_t            buf_sizes[1];
    uint32_t            bulk_sgnum;
    uint64_t *          ids_buf_sizes;
    void **             ids_buf;
    uint64_t *          u64_arr_ptr;

    bulk_args = (struct bulk_args_t *)hg_cb_info->arg;

    n_meta = bulk_args->n_meta;

    if (hg_cb_info->ret == HG_SUCCESS) {
        bulk_sgnum    = HG_Bulk_get_segment_count(local_bulk_handle);
        ids_buf       = (void **)PDC_calloc(sizeof(void *), bulk_sgnum);
        ids_buf_sizes = (uint64_t *)PDC_calloc(sizeof(uint64_t), bulk_sgnum);
        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, bulk_sgnum, ids_buf,
                       ids_buf_sizes, &actual_cnt);

        u64_arr_ptr        = ((uint64_t **)(ids_buf))[0];
        bulk_args->obj_ids = (uint64_t *)PDC_calloc(sizeof(uint64_t), n_meta);
        for (int i = 0; i < n_meta; i++) {
            bulk_args->obj_ids[i] = *u64_arr_ptr;
            u64_arr_ptr++;
        }
    }
    else
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error with bulk handle");

    // Free local bulk handle
    ret_value = HG_Bulk_free(local_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

    ret_value = HG_Bulk_free(origin_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

done:
    hg_atomic_decr32(&bulk_todo_g);
    hg_atomic_cas32(&bulk_transfer_done_g, 0, 1);
    HG_Destroy(bulk_args->handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
kvtag_query_forward_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value;
    struct bulk_args_t *          bulk_arg;
    hg_handle_t                   handle;
    metadata_query_transfer_out_t output;
    uint32_t                      n_meta;
    hg_op_id_t                    hg_bulk_op_id;
    hg_bulk_t                     local_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t                     origin_bulk_handle = HG_BULK_NULL;
    const struct hg_info *        hg_info            = NULL;

    bulk_arg = (struct bulk_args_t *)callback_info->arg;
    handle   = callback_info->info.forward.handle;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error HG_Get_output");

    bulk_arg->server_time_elapsed       = output.server_time_elapsed;
    bulk_arg->server_memory_consumption = output.server_memory_consumption;

    if (output.bulk_handle == HG_BULK_NULL || output.ret == 0) {
        hg_atomic_decr32(&bulk_todo_g);
        bulk_arg->n_meta  = 0;
        bulk_arg->obj_ids = NULL;
        HG_Free_output(handle, &output);
        HG_Destroy(handle);
        PGOTO_DONE(ret_value);
    }

    n_meta           = output.ret;
    bulk_arg->n_meta = n_meta;

    // We have received the bulk handle from server (server uses hg_respond)
    origin_bulk_handle = output.bulk_handle;
    hg_info            = HG_Get_info(handle);

    bulk_arg->handle  = handle;
    bulk_arg->nbytes  = HG_Bulk_get_size(origin_bulk_handle);
    bulk_arg->obj_ids = bulk_arg->obj_ids;

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, NULL, (hg_size_t *)&bulk_arg->nbytes, HG_BULK_READWRITE,
                   &local_bulk_handle);

    /* Pull bulk data */
    ret_value =
        HG_Bulk_transfer(hg_info->context, kvtag_query_bulk_cb, bulk_arg, HG_BULK_PULL, hg_info->addr,
                         origin_bulk_handle, 0, local_bulk_handle, 0, bulk_arg->nbytes, &hg_bulk_op_id);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not read bulk data");

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Client_query_kvtag_server(uint32_t server_id, const pdc_kvtag_t *kvtag, int *n_res, uint64_t **out)
{
    FUNC_ENTER(NULL);

    perr_t              ret_value = SUCCEED;
    hg_return_t         hg_ret;
    hg_handle_t         query_kvtag_server_handle;
    pdc_kvtag_t         in;
    struct bulk_args_t *bulk_arg;

    if (kvtag == NULL)
        PGOTO_ERROR(FAIL, "kvtag is NULL");
    if (n_res == NULL)
        PGOTO_ERROR(FAIL, "n_res is NULL");
    if (out == NULL)
        PGOTO_ERROR(FAIL, "out is NULL");

    if (kvtag->name == NULL)
        in.name = " ";
    else
        in.name = kvtag->name;

    if (kvtag->value == NULL) {
        in.value = " ";
        in.type  = PDC_STRING;
        in.size  = 1;
    }
    else {
        in.value = kvtag->value;
        in.type  = kvtag->type;
        in.size  = kvtag->size;
    }

    *out   = NULL;
    *n_res = 0;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_kvtag_register_id_g,
                       &query_kvtag_server_handle);

    bulk_arg = (struct bulk_args_t *)PDC_calloc(1, sizeof(struct bulk_args_t));
    if (query_kvtag_server_handle == NULL)
        PGOTO_ERROR(FAIL, "Error with query_kvtag_server_handle");

    hg_ret = HG_Forward(query_kvtag_server_handle, kvtag_query_forward_cb, bulk_arg, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_client_list_all(): Could not start HG_Forward()");

    hg_atomic_set32(&bulk_transfer_done_g, 0);

    // Wait for response from server
    hg_atomic_incr32(&bulk_todo_g);
    PDC_Client_check_bulk(send_context_g);

    server_call_count_g[server_id]++;
    server_time_total_g[server_id] += bulk_arg->server_time_elapsed;
    server_mem_usage_g[server_id] = bulk_arg->server_memory_consumption;

    *n_res = bulk_arg->n_meta;
    if (*n_res > 0)
        *out = bulk_arg->obj_ids;
    bulk_arg = (struct bulk_args_t *)PDC_free(bulk_arg);
    // TODO: need to be careful when freeing the lookup_args, as it include the results returned to user

done:
    FUNC_LEAVE(ret_value);
}

// Single client query all servers
perr_t
PDC_Client_query_kvtag(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids)
{
    FUNC_ENTER(NULL);

    perr_t    ret_value = SUCCEED;
    int32_t   i;
    int       nmeta    = 0;
    uint64_t *temp_ids = NULL;
    uint32_t  server_id;

    *n_res   = 0;
    *pdc_ids = NULL;

    for (i = 0; i < pdc_server_num_g; i++) {
        // TODO: when there are multiple clients issuing different queries concurrently, try to balance
        // the server workload by having different clients sending queries with a different order
        ret_value = PDC_Client_query_kvtag_server((uint32_t)i, kvtag, &nmeta, &temp_ids);
        if (ret_value != SUCCEED) {
            PGOTO_ERROR(FAIL, "Error with PDC_Client_query_kvtag_server to server %d", i);
        }
        if (i == 0) {
            *pdc_ids = temp_ids;
        }
        else if (nmeta > 0) {
            *pdc_ids = (uint64_t *)PDC_realloc(*pdc_ids, sizeof(uint64_t) * (*n_res + nmeta));
            memcpy(*pdc_ids + (*n_res), temp_ids, nmeta * sizeof(uint64_t));
            temp_ids = (uint64_t *)PDC_free(temp_ids);
        }
        *n_res = *n_res + nmeta;
    }
done:
    FUNC_LEAVE(ret_value);
}

// Delete a tag specified by a name, and whether it is from a container or an object
static perr_t
PDCtag_delete(pdcid_t obj_id, char *tag_name, int is_cont)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    uint64_t                       meta_id;
    uint32_t                       server_id;
    hg_handle_t                    metadata_del_kvtag_handle;
    metadata_get_kvtag_in_t        in;
    struct _pdc_obj_info *         obj_prop;
    struct _pdc_cont_info *        cont_prop;
    struct _pdc_client_lookup_args lookup_args;

    if (is_cont) {
        cont_prop = PDC_cont_get_info(obj_id);
        meta_id   = cont_prop->cont_info_pub->meta_id;
    }
    else {
        obj_prop = PDC_obj_get_info(obj_id);
        meta_id  = obj_prop->obj_info_pub->meta_id;
    }

    server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);

    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_del_kvtag_register_id_g,
              &metadata_del_kvtag_handle);

    // Fill input structure
    in.obj_id = meta_id;

    if (is_cont)
        in.hash_value = PDC_get_hash_by_name(cont_prop->cont_info_pub->name);
    else
        in.hash_value = PDC_get_hash_by_name(obj_prop->obj_info_pub->name);
    in.key = tag_name;

    // reuse metadata_add_tag_rpc_cb here since it only checks the return value
    hg_ret = HG_Forward(metadata_del_kvtag_handle, metadata_add_tag_rpc_cb /*reuse*/, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_del_kvtag_metadata_with_name(): Could not start HG_Forward()");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        LOG_INFO("del kvtag NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_del_kvtag_handle);

    FUNC_LEAVE(ret_value);
}

/* - -------------------------------- */
/* New Simple Object Access Interface */
/* - -------------------------------- */
// Create a container with specified name
pdcid_t
PDCcont_put(const char *cont_name, pdcid_t pdc)
{
    FUNC_ENTER(NULL);

    perr_t  ret_value;
    pdcid_t cont_id = 0, cont_prop;

    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);

#ifdef ENABLE_MPI

    ret_value = PDC_Client_create_cont_id_mpi(cont_name, cont_prop, &cont_id);
#else
    ret_value = PDC_Client_create_cont_id(cont_name, cont_prop, &cont_id);
#endif
    if (ret_value != SUCCEED)
        PGOTO_ERROR(0, "Error with PDC_Client_create_cont_id");

done:
    FUNC_LEAVE(cont_id);
}

pdcid_t
PDCcont_get_id(const char *cont_name, pdcid_t pdc_id)
{
    FUNC_ENTER(NULL);

    pdcid_t  cont_id;
    uint64_t cont_meta_id;

    PDC_Client_query_container_name(cont_name, &cont_meta_id);
    cont_id = PDC_cont_create_local(pdc_id, cont_name, cont_meta_id);

    FUNC_LEAVE(cont_id);
}

// Get container name
perr_t
PDCcont_get(pdcid_t cont_id ATTRIBUTE(unused), char **cont_name ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    LOG_ERROR("PDCcont_get not implemented\n");

    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_del(pdcid_t cont_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDC_Client_del_metadata(cont_id, 1);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_del_objects_to_container");

done:
    FUNC_LEAVE(ret_value);
}

// Put a number of objects to a container
perr_t
PDCcont_put_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDC_Client_add_objects_to_container(nobj, obj_ids, cont_id);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_add_objects_to_container");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_del_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDC_Client_del_objects_to_container(nobj, obj_ids, cont_id);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_del_objects_to_container");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_get_objids(pdcid_t cont_id ATTRIBUTE(unused), int *nobj ATTRIBUTE(unused),
                   pdcid_t **obj_ids ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_put_tag(pdcid_t cont_id, char *tag_name, void *tag_value, pdc_var_type_t value_type,
                psize_t value_size)
{
    FUNC_ENTER(NULL);

    perr_t      ret_value = SUCCEED;
    pdc_kvtag_t kvtag;

    kvtag.name  = tag_name;
    kvtag.value = (void *)tag_value;
    kvtag.type  = value_type;
    kvtag.size  = (uint64_t)value_size;

    ret_value = PDC_add_kvtag(cont_id, &kvtag, 1);
    if (ret_value != SUCCEED)

        PGOTO_ERROR(FAIL, "Error with PDCcont_put_tag");

done:

    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_get_tag(pdcid_t cont_id, char *tag_name, void **tag_value, pdc_var_type_t *value_type,
                psize_t *value_size)
{
    FUNC_ENTER(NULL);

    perr_t       ret_value = SUCCEED;
    pdc_kvtag_t *kvtag     = NULL;

    ret_value = PDC_get_kvtag(cont_id, tag_name, &kvtag, 1);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_get_kvtag");

    *tag_value  = kvtag->value;
    *value_type = kvtag->type;
    *value_size = kvtag->size;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_del_tag(pdcid_t cont_id, char *tag_name)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    ret_value = PDCtag_delete(cont_id, tag_name, 1);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDCtag_delete");

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCobj_put_data(const char *obj_name, void *data, uint64_t size, pdcid_t cont_id)
{
    FUNC_ENTER(NULL);

    pdcid_t ret_value = 0;
    pdcid_t obj_id, obj_prop, obj_region;
    perr_t  ret;
    // pdc_metadata_t *meta;
    struct _pdc_cont_info *info    = NULL;
    struct _pdc_id_info *  id_info = NULL;
    pdcid_t                transfer_request;

    if ((id_info = PDC_find_id(cont_id)) == NULL)
        PGOTO_ERROR(0, "Failed to find PDC ID: %d", cont_id);
    info = (struct _pdc_cont_info *)(id_info->obj_ptr);

    obj_prop = PDCprop_create(PDC_OBJ_CREATE, info->cont_pt->pdc->local_id);
    PDCprop_set_obj_type(obj_prop, PDC_CHAR);
    PDCprop_set_obj_dims(obj_prop, 1, &size);

    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);

    obj_id = PDCobj_create(cont_id, obj_name, obj_prop);
    if (obj_id <= 0)
        PGOTO_ERROR(0, "Error creating object [%s]", obj_name);

    uint64_t offset = 0;
    // size = ceil(size/sizeof(int));
    obj_region = PDCregion_create(1, &offset, &size);

    transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj_id, obj_region, obj_region);
    if (transfer_request == 0) {
        PGOTO_ERROR(0, "Error with region transfer create for obj [%s]", obj_name);
    }

    ret = PDCregion_transfer_start(transfer_request);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "Error with region transfer start for obj [%s]", obj_name);
    }
    ret = PDCregion_transfer_wait(transfer_request);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "Error with region transfer wait for obj [%s]", obj_name);
    }

    ret = PDCregion_transfer_close(transfer_request);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "Error with region transfer close for obj [%s]", obj_name);
    }

    ret = PDCregion_close(obj_region);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "Error with PDCregion_close for obj [%s]", obj_name);
    }

    ret = PDCprop_close(obj_prop);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "Error with PDCprop_close for obj [%s]", obj_name);
    }

    ret_value = obj_id;
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_get_data(pdcid_t obj_id, void *data, uint64_t size)
{
    FUNC_ENTER(NULL);

    perr_t   ret_value = SUCCEED;
    uint64_t offset    = 0;
    pdcid_t  reg;
    pdcid_t  transfer_request;

    reg              = PDCregion_create(1, &offset, &size);
    transfer_request = PDCregion_transfer_create(data, PDC_READ, obj_id, reg, reg);
    if (transfer_request == 0) {
        PGOTO_DONE(ret_value);
    }
    ret_value = PDCregion_transfer_start(transfer_request);
    if (ret_value != SUCCEED) {
        PGOTO_ERROR(FAIL, "Error when calling PDCregion_transfer_start");
    }
    ret_value = PDCregion_transfer_wait(transfer_request);
    if (ret_value != SUCCEED) {
        PGOTO_ERROR(FAIL, "Error when calling PDCregion_transfer_wait");
    }
    ret_value = PDCregion_transfer_close(transfer_request);
    if (ret_value != SUCCEED) {
        PGOTO_ERROR(FAIL, "Error when calling PDCregion_transfer_close");
    }
    ret_value = PDCregion_close(reg);
    if (ret_value != SUCCEED) {
        PGOTO_ERROR(FAIL, "Error when calling PDCregion_close");
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_del_metadata(pdcid_t obj_id, int is_cont)
{
    FUNC_ENTER(NULL);

    perr_t                 ret_value = SUCCEED;
    uint64_t               meta_id;
    struct _pdc_obj_info * obj_prop;
    struct _pdc_cont_info *cont_prop;

    if (is_cont) {
        cont_prop = PDC_cont_get_info(obj_id);
        meta_id   = cont_prop->cont_info_pub->meta_id;
    }
    else {
        obj_prop = PDC_obj_get_info(obj_id);
        meta_id  = obj_prop->obj_info_pub->meta_id;
    }

    ret_value = PDC_Client_delete_metadata_by_id(meta_id);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_delete_metadata_by_id");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_put_tag(pdcid_t obj_id, char *tag_name, void *tag_value, pdc_var_type_t value_type, psize_t value_size)
{
    FUNC_ENTER(NULL);

    perr_t      ret_value = SUCCEED;
    pdc_kvtag_t kvtag;

    kvtag.name  = tag_name;
    kvtag.value = (void *)tag_value;
    kvtag.type  = value_type;
    kvtag.size  = (uint64_t)value_size;

    ret_value = PDC_add_kvtag(obj_id, &kvtag, 0);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_add_kvtag");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_get_tag(pdcid_t obj_id, char *tag_name, void **tag_value, pdc_var_type_t *value_type,
               psize_t *value_size)
{
    FUNC_ENTER(NULL);

    perr_t       ret_value = SUCCEED;
    pdc_kvtag_t *kvtag     = NULL;

    ret_value = PDC_get_kvtag(obj_id, tag_name, &kvtag, 0);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_get_kvtag");

    *tag_value  = kvtag->value;
    *value_type = kvtag->type;
    *value_size = kvtag->size;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_del_tag(pdcid_t obj_id, char *tag_name)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    ret_value = PDCtag_delete(obj_id, tag_name, 0);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_del_kvtag");

done:
    FUNC_LEAVE(ret_value);
}

void
PDC_get_server_from_query(pdc_query_t *query, uint32_t *servers, int32_t *n)
{
    FUNC_ENTER(NULL);

    uint32_t id;
    int32_t  i, exist;

    if (NULL == query)
        PGOTO_DONE_VOID;

    if (NULL == query->left && NULL == query->right) {
        exist = 0;
        id    = PDC_get_server_by_obj_id(query->constraint->obj_id, pdc_server_num_g);
        for (i = 0; i < *n; i++) {
            if (servers[i] == id) {
                exist = 1;
                break;
            }
        }
        if (exist == 0) {
            servers[*n] = id;
            (*n)++;
        }
        PGOTO_DONE_VOID;
    }
    PDC_get_server_from_query(query->left, servers, n);
    PDC_get_server_from_query(query->right, servers, n);

done:
    FUNC_LEAVE_VOID();
}

int
gen_query_id()
{
    FUNC_ENTER(NULL);

    int ret_value = 0;
    ret_value     = rand();

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_recv_nhits(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                    ret_value = HG_SUCCESS;
    send_nhits_t *                 in        = (send_nhits_t *)callback_info->arg;
    struct _pdc_query_result_list *result_elt;

    LOG_INFO("Received %" PRIu64 " hits from server\n", in->nhits);

    DL_FOREACH(pdcquery_result_list_head_g, result_elt)
    {
        if (result_elt->query_id == in->query_id) {
            result_elt->nhits = in->nhits;
            break;
        }
    }

    hg_atomic_decr32(&atomic_work_todo_g);
    in = (send_nhits_t *)PDC_free(in);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_send_data_query(pdc_query_t *query, pdc_query_get_op_t get_op, uint64_t *nhits, pdc_selection_t *sel,
                    void *data ATTRIBUTE(unused))
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value      = SUCCEED;
    hg_return_t                    hg_ret         = 0;
    uint32_t *                     target_servers = NULL;
    int                            i, server_id, next_server = 0, prev_server = 0, ntarget = 0;
    hg_handle_t                    handle;
    pdc_query_xfer_t *             query_xfer;
    struct _pdc_client_lookup_args lookup_args;
    struct _pdc_query_result_list *result;

    query_xfer = PDC_serialize_query(query);
    if (query_xfer == NULL)
        PGOTO_ERROR(FAIL, "Error with PDC_serialize_query");

    // Find unique server IDs that has metadata of the queried objects
    target_servers = (uint32_t *)PDC_calloc(pdc_server_num_g, sizeof(uint32_t));
    PDC_get_server_from_query(query, target_servers, &ntarget);
    query_xfer->n_unique_obj = ntarget;
    query_xfer->query_id     = gen_query_id();
    query_xfer->client_id    = pdc_client_mpi_rank_g;
    query_xfer->manager      = target_servers[0];
    query_xfer->get_op       = (int)get_op;

    result           = (struct _pdc_query_result_list *)PDC_calloc(1, sizeof(struct _pdc_query_result_list));
    result->query_id = query_xfer->query_id;
    DL_APPEND(pdcquery_result_list_head_g, result);

    // Send query to all servers
    for (server_id = 0; server_id < pdc_server_num_g; server_id++) {
        debug_server_id_count[server_id]++;

        for (i = 0; i < ntarget; i++) {
            if ((uint32_t)server_id == target_servers[i]) {
                if (i > 0)
                    prev_server = target_servers[i - 1];
                if (i < ntarget - 1)
                    next_server = target_servers[i + 1];
                break;
            }
            next_server = -1;
            prev_server = -1;
        }
        query_xfer->next_server_id = next_server;
        query_xfer->prev_server_id = prev_server;

        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, send_data_query_register_id_g, &handle);

        hg_ret = HG_Forward(handle, pdc_client_check_int_ret_cb, &lookup_args, query_xfer);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_Client_del_kvtag_metadata_with_name(): Could not start HG_Forward()");

        // Wait for response from server
        hg_atomic_set32(&atomic_work_todo_g, 1);
        PDC_Client_check_response(&send_context_g);

        if (lookup_args.ret != 1)
            PGOTO_ERROR(FAIL, "Send data query to server %u failed ... ret_value = %d", server_id,
                        lookup_args.ret);

        HG_Destroy(handle);
    }

    // Wait for server to send query result
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (nhits)
        *nhits = result->nhits;
    if (sel) {
        sel->query_id     = query_xfer->query_id;
        sel->nhits        = result->nhits;
        sel->coords       = result->coords;
        sel->ndim         = result->ndim;
        sel->coords_alloc = result->nhits * result->ndim;
    }

done:
    if (target_servers)
        target_servers = (uint32_t *)PDC_free(target_servers);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_recv_coords(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                    ret_value         = HG_SUCCESS;
    hg_bulk_t                      local_bulk_handle = callback_info->info.bulk.local_handle;
    struct bulk_args_t *           bulk_args         = (struct bulk_args_t *)callback_info->arg;
    struct _pdc_query_result_list *result_elt;
    uint64_t                       nhits = 0;
    uint32_t                       ndim;
    int                            query_id, origin;
    void *                         buf;
    pdc_int_ret_t                  out;

    out.ret = 1;

    if (callback_info->ret != HG_SUCCESS) {
        out.ret = -1;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    }
    else {
        nhits    = bulk_args->cnt;
        ndim     = bulk_args->ndim;
        query_id = bulk_args->query_id;
        origin   = bulk_args->origin;

        LOG_INFO("Received %" PRIu64 " coords from server %d\n", nhits, origin);

        if (nhits > 0) {
            ret_value = HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1,
                                       (void **)&buf, NULL, NULL);
        }

        DL_FOREACH(pdcquery_result_list_head_g, result_elt)
        {
            if (result_elt->query_id == query_id) {
                result_elt->ndim   = ndim;
                result_elt->nhits  = nhits;
                result_elt->coords = (uint64_t *)PDC_malloc(nhits * ndim * sizeof(uint64_t));
                memcpy(result_elt->coords, buf, nhits * ndim * sizeof(uint64_t));
                break;
            }
        }

        if (result_elt == NULL)
            PGOTO_ERROR(HG_OTHER_ERROR, "Invalid task ID");
    } // End else

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    if (nhits > 0) {
        ret_value = HG_Bulk_free(local_bulk_handle);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value, "Could not free HG bulk handle");
    }

    ret_value = HG_Respond(bulk_args->handle, NULL, NULL, &out);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

void
PDCselection_free(pdc_selection_t *sel)
{
    FUNC_ENTER(NULL);

    if (sel->coords_alloc > 0 && sel->coords)
        sel->coords = (uint64_t *)PDC_free(sel->coords);

    FUNC_LEAVE_VOID();
}

perr_t
PDC_Client_get_sel_data(pdcid_t obj_id, pdc_selection_t *sel, void *data)
{
    FUNC_ENTER(NULL);

    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    hg_handle_t                    handle;
    int                            i;
    uint32_t                       server_id;
    uint64_t                       meta_id, off;
    get_sel_data_rpc_in_t          in;
    struct _pdc_client_lookup_args lookup_args;
    struct _pdc_obj_info *         obj_prop;
    struct _pdc_query_result_list *result_elt;

    if (sel == NULL)
        PGOTO_ERROR(FAIL, "NULL input");

    if (PDC_find_id(obj_id) != NULL) {
        obj_prop = PDC_obj_get_info(obj_id);
        meta_id  = obj_prop->obj_info_pub->meta_id;
    }
    else
        meta_id = obj_id;

    in.query_id = sel->query_id;
    in.obj_id   = meta_id;
    in.origin   = pdc_client_mpi_rank_g;
    server_id   = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, get_sel_data_register_id_g, &handle);

    hg_ret = HG_Forward(handle, pdc_client_check_int_ret_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Error with HG_Forward");

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1) {
        PGOTO_ERROR(FAIL, "Send data selection to server %u failed ... ret_value = %d", server_id,
                    lookup_args.ret);
    }

    // Wait for server to send data
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    // Copy the result to user's buffer
    DL_FOREACH(pdcquery_result_list_head_g, result_elt)
    {
        if (result_elt->query_id == in.query_id) {
            off = 0;
            for (i = 0; i < pdc_server_num_g; i++) {
                if (result_elt->data_arr[i] != NULL) {
                    memcpy(data + off, result_elt->data_arr[i], result_elt->data_arr_size[i]);
                    off += result_elt->data_arr_size[i];
                    result_elt->data_arr[i] = (void **)PDC_free(result_elt->data_arr[i]);
                }
            }
            result_elt->recv_data_nhits = 0;
            break;
        }
    }

done:
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_recv_read_coords_data(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                    ret_value         = HG_SUCCESS;
    hg_bulk_t                      local_bulk_handle = callback_info->info.bulk.local_handle;
    struct bulk_args_t *           bulk_args         = (struct bulk_args_t *)callback_info->arg;
    struct _pdc_query_result_list *result_elt;
    uint64_t                       nhits = 0;
    int                            query_id, seq_id;
    void *                         buf;
    pdc_int_ret_t                  out;

    out.ret = 1;

    if (callback_info->ret != HG_SUCCESS) {
        out.ret = -1;
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");
    }
    else {
        nhits    = bulk_args->cnt;
        query_id = bulk_args->query_id;
        seq_id   = bulk_args->client_seq_id;

        if (nhits > 0) {
            ret_value = HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1,
                                       (void **)&buf, NULL, NULL);
        }

        DL_FOREACH(pdcquery_result_list_head_g, result_elt)
        {
            if (result_elt->query_id == query_id) {
                if (result_elt->data_arr == NULL) {
                    result_elt->data_arr      = PDC_calloc(sizeof(void *), pdc_server_num_g);
                    result_elt->data_arr_size = PDC_calloc(sizeof(uint64_t *), pdc_server_num_g);
                }

                result_elt->data_arr[seq_id] = PDC_malloc(bulk_args->nbytes);
                memcpy(result_elt->data_arr[seq_id], buf, bulk_args->nbytes);
                result_elt->data_arr_size[seq_id] = bulk_args->nbytes;
                result_elt->recv_data_nhits += nhits;
                break;
            }
        }
        if (result_elt == NULL)
            PGOTO_ERROR(HG_OTHER_ERROR, "Invalid task ID");

        if (result_elt->recv_data_nhits == result_elt->nhits) {
            hg_atomic_decr32(&atomic_work_todo_g);
        }
        else if (result_elt->recv_data_nhits > result_elt->nhits) {
            PGOTO_ERROR(HG_OTHER_ERROR, "Received more results data than expected");
            hg_atomic_decr32(&atomic_work_todo_g);
        }
    } // End else

done:
    if (nhits > 0) {
        ret_value = HG_Bulk_free(local_bulk_handle);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value, "Could not free HG bulk handle");
    }

    ret_value = HG_Respond(bulk_args->handle, NULL, NULL, &out);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

    HG_Destroy(bulk_args->handle);
    bulk_args = (struct bulk_args_t *)PDC_free(bulk_args);

    FUNC_LEAVE(ret_value);
}

void
report_avg_server_profiling_rst()
{
    FUNC_ENTER(NULL);

    for (int i = 0; i < pdc_server_num_g; i++) {

        double avg_srv_time = server_call_count_g[i] > 0
                                  ? (double)(server_time_total_g[i]) / (double)(server_call_count_g[i])
                                  : 0.0;
        double srv_mem_usage = server_mem_usage_g[i] / 1024.0 / 1024.0;
        LOG_INFO("Server %d, avg profiling time: %.4f ms, memory usage: %.4f MB\n", i, avg_srv_time / 1000.0,
                 srv_mem_usage);
    }

    FUNC_LEAVE_VOID();
}

/******************** METADATA INDEX BEGINS *******************************/

// get_server_info_callback
static hg_return_t
client_dart_get_server_info_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                        ret_value = HG_SUCCESS;
    hg_handle_t                        handle;
    struct client_genetic_lookup_args *client_lookup_args;

    dart_get_server_info_out_t output;

    /* LOG_INFO("Entered client_rpc_cb()"); */
    client_lookup_args = (struct client_genetic_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value                        = HG_Get_output(handle, &output);
    client_lookup_args->int64_value1 = output.indexed_word_count;
    client_lookup_args->int64_value2 = output.request_count;

    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

void
dart_retrieve_server_info_cb(dart_server *server_ptr)
{
    FUNC_ENTER(NULL);

    perr_t srv_lookup_rst = PDC_Client_try_lookup_server(server_ptr->id, 0);
    if (srv_lookup_rst == FAIL)
        PGOTO_ERROR_VOID("the server %d cannot be connected", server_ptr->id);

    // Mercury comm here.
    hg_handle_t dart_get_server_info_handle;
    HG_Create(send_context_g, pdc_server_info_g[server_ptr->id].addr, dart_get_server_info_g,
              &dart_get_server_info_handle);
    dart_get_server_info_in_t in;
    in.serverId = server_ptr->id;
    struct client_genetic_lookup_args lookup_args;
    hg_return_t                       hg_ret =
        HG_Forward(dart_get_server_info_handle, client_dart_get_server_info_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        LOG_INFO("dart_get_server_info_g(): Could not start HG_Forward() on serverId = %ld with host = %s\n",
                 server_ptr->id, pdc_server_info_g[server_ptr->id].addr_string);
        HG_Destroy(dart_get_server_info_handle);
        FUNC_LEAVE_VOID();
    }

    // Wait for response from server
    hg_atomic_set32(&atomic_work_todo_g, 1);
    PDC_Client_check_response(&send_context_g);

    server_ptr->indexed_word_count = lookup_args.int64_value1;
    server_ptr->request_count      = lookup_args.int64_value2;

done:
    HG_Destroy(dart_get_server_info_handle);

    FUNC_LEAVE_VOID();
}

DART *
get_dart_g()
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(dart_g);
}

// Bulk
static hg_return_t
dart_perform_one_server_on_receive_cb(const struct hg_cb_info *callback_info)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value;
    struct bulk_args_t *          client_lookup_args;
    hg_handle_t                   handle;
    dart_perform_one_server_out_t output;
    uint32_t                      n_meta;
    hg_op_id_t                    hg_bulk_op_id;

    hg_bulk_t             local_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t             origin_bulk_handle = HG_BULK_NULL;
    const struct hg_info *hg_info            = NULL;
    struct bulk_args_t *  bulk_args;
    void *                recv_meta;

    client_lookup_args = (struct bulk_args_t *)callback_info->arg;
    handle             = callback_info->info.forward.handle;
    ret_value          = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        LOG_INFO("dart_perform_one_server_on_receive_cb - error HG_Get_output\n");
        client_lookup_args->n_meta = 0;
        PGOTO_DONE(ret_value);
    }

    client_lookup_args->server_time_elapsed       = output.server_time_elapsed;
    client_lookup_args->server_memory_consumption = output.server_memory_consumption;

    if (ret_value == HG_SUCCESS && output.has_bulk == 0) {
        client_lookup_args->n_meta = 0;
        PGOTO_DONE(ret_value);
    }

    n_meta                     = output.n_items;
    client_lookup_args->n_meta = n_meta;

    if (n_meta == 0) {
        client_lookup_args->obj_ids = NULL;
        client_lookup_args->n_meta  = 0;
        PGOTO_DONE(ret_value);
    }

    // Prepare to receive BULK data.
    origin_bulk_handle = output.bulk_handle;
    hg_info            = HG_Get_info(handle);

    client_lookup_args->handle = handle;
    client_lookup_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);

    /* LOG_INFO("nbytes=%u\n", bulk_args->nbytes); */

    if (client_lookup_args->is_id == 1) {
        recv_meta = (void *)PDC_calloc(n_meta, sizeof(uint64_t));
    }
    else {
        // throw an error
        LOG_ERROR("DART queries can only retrieve object IDs. Please check "
                  "client_lookup_args->is_id\n");
        PGOTO_DONE(ret_value);
    }

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, (void **)&recv_meta, (hg_size_t *)&client_lookup_args->nbytes,
                   HG_BULK_READWRITE, &local_bulk_handle);

    /* Pull bulk data */
    ret_value = HG_Bulk_transfer(hg_info->context, hg_test_bulk_transfer_cb, client_lookup_args, HG_BULK_PULL,
                                 hg_info->addr, origin_bulk_handle, 0, local_bulk_handle, 0,
                                 client_lookup_args->nbytes, &hg_bulk_op_id);

    if (ret_value != HG_SUCCESS) {
        client_lookup_args->n_meta = 0;
        PGOTO_ERROR(FAIL, "Could not read bulk data");
    }

    hg_atomic_init32(&(client_lookup_args->bulk_done_flag), 0);
    hg_atomic_incr32(&bulk_todo_g);

    // loop
    PDC_Client_check_bulk(send_context_g);

    while (1) {
        if (hg_atomic_get32(&(client_lookup_args->bulk_done_flag)) == 1) {
            break;
        }
    }

done:
    hg_atomic_decr32(&atomic_work_todo_g);
    HG_Free_output(handle, &output);
    // we destroy the handle here. There will be no need to clean it up in the loop from the request
    // initiator function
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

perr_t
_dart_send_request_to_one_server(int server_id, dart_perform_one_server_in_t *dart_in,
                                 struct bulk_args_t *lookup_args_ptr, hg_handle_t *handle)
{
    FUNC_ENTER(NULL);

    hg_return_t hg_ret;
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, dart_perform_one_server_g, handle);
    if (handle == NULL) {
        LOG_INFO("Error with _dart_send_request_to_one_server\n");
        FUNC_LEAVE(FAIL);
    }

    lookup_args_ptr->server_id = server_id;

    hg_ret = HG_Forward(*handle, dart_perform_one_server_on_receive_cb, lookup_args_ptr, dart_in);
    hg_atomic_incr32(&atomic_work_todo_g);
    if (hg_ret != HG_SUCCESS) {
        LOG_INFO("_dart_send_request_to_one_server(): Could not start HG_Forward()\n");
        hg_atomic_decr32(&atomic_work_todo_g);
        FUNC_LEAVE(FAIL);
    }

    // waiting for response and get the results if any.
    // Wait for response from server
    PDC_Client_check_response(&send_context_g); // This will block until all requests are done.

    FUNC_LEAVE(SUCCEED);
}

int
_aggregate_dart_results_from_all_servers(struct bulk_args_t *lookup_args, Set *output_set, int num_requests)
{
    FUNC_ENTER(NULL);

    int total_num_results = 0;
    for (int i = 0; i < num_requests; i++) {
        // aggregate result only for query operations
        if (lookup_args[i].n_meta == 0) {
            continue;
        }
        if (lookup_args[i].is_id == 1) {
            int n_meta = lookup_args[i].n_meta;
            for (int k = 0; k < n_meta; k++) {
                uint64_t *id = (uint64_t *)PDC_malloc(sizeof(uint64_t));
                *id          = lookup_args[i].obj_ids[k];
                set_insert(output_set, id);
            }
            total_num_results += n_meta;
        }
    }

    FUNC_LEAVE(total_num_results);
}

int
get_dart_insert_count()
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(dart_insert_count);
}

uint64_t
dart_perform_on_servers(index_hash_result_t **hash_result, int num_servers,
                        dart_perform_one_server_in_t *dart_in, Set *output_set)
{

    FUNC_ENTER(NULL);

    struct bulk_args_t *lookup_args =
        (struct bulk_args_t *)PDC_calloc(num_servers, sizeof(struct bulk_args_t));
    uint64_t       ret_value            = 0;
    hg_handle_t *  dart_request_handles = (hg_handle_t *)PDC_calloc(num_servers, sizeof(hg_handle_t));
    int            num_requests         = 0;
    uint32_t       total_n_meta         = 0;
    dart_op_type_t op_type              = dart_in->op_type;

    dart_in->src_client_id = pdc_client_mpi_rank_g;

    stopwatch_t timer;
    timer_start(&timer);
    // send the requests to the required servers.
    for (int i = 0; i < num_servers; i++) {
        int server_id = (*hash_result)[i].server_id;
        if (PDC_Client_try_lookup_server(server_id, 0) != SUCCEED)
            PGOTO_ERROR(FAIL, "Error with PDC_Client_try_lookup_server");

        lookup_args[i].is_id   = 1;
        lookup_args[i].op_type = op_type;

        if (is_index_write_op(op_type)) {
            dart_in->vnode_id         = (*hash_result)[i].virtual_node_id;
            dart_in->attr_key         = strdup((*hash_result)[i].key);
            dart_in->inserting_suffix = (*hash_result)[i].is_suffix;
            dart_insert_count++;
        }

        _dart_send_request_to_one_server(server_id, dart_in, &(lookup_args[i]), &(dart_request_handles[i]));

        num_requests++;
    }

    // aggregate results when executing queries.
    if ((!is_index_write_op(op_type)) && output_set != NULL) {
        total_n_meta = _aggregate_dart_results_from_all_servers(lookup_args, output_set, num_servers);
        ret_value    = total_n_meta;
    }
    timer_pause(&timer);

    if (!is_index_write_op(op_type)) {
        for (int i = 0; i < num_servers; i++) {
            int srv_id = lookup_args[i].server_id;
            server_time_total_g[srv_id] += lookup_args[i].server_time_elapsed;
            server_call_count_g[srv_id] += 1;
            server_mem_usage_g[srv_id] = lookup_args[i].server_memory_consumption;
        }
    }
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_search_obj_ref_through_dart(dart_hash_algo_t hash_algo, char *query_string,
                                       dart_object_ref_type_t ref_type, int *n_res, uint64_t **out)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (n_res == NULL || out == NULL) {
        FUNC_LEAVE(ret_value);
    }

    stopwatch_t timer;
    timer_start(&timer);

    char *         k_query = get_key(query_string, '=');
    char *         v_query = get_value(query_string, '=');
    char *         tok     = NULL;
    dart_op_type_t dart_op;

    dart_determine_query_token_by_key_query(k_query, &tok, &dart_op);

    if (tok == NULL) {
        LOG_ERROR("tok was NULL\n");
        ret_value = FAIL;
        FUNC_LEAVE(ret_value);
    }

    out[0] = NULL;

    dart_perform_one_server_in_t input_param;
    input_param.op_type      = dart_op;
    input_param.hash_algo    = hash_algo;
    input_param.attr_key     = query_string;
    input_param.attr_val     = v_query;
    input_param.attr_vsize   = strlen(v_query);
    input_param.attr_vtype   = PDC_STRING;
    input_param.obj_ref_type = ref_type;

    // TODO: see if timestamp can help
    // input_param.timestamp = get_timestamp_us();
    input_param.timestamp = 1;

    index_hash_result_t *hash_result = NULL;
    int                  num_servers = 0;

    if (hash_algo == DART_HASH) {
        num_servers = DART_hash(dart_g, tok, dart_op, NULL, &hash_result);
    }
    else if (hash_algo == DHT_FULL_HASH) {
        num_servers = DHT_hash(dart_g, strlen(tok), tok, dart_op, &hash_result);
    }
    else if (hash_algo == DHT_INITIAL_HASH) {
        num_servers = DHT_hash(dart_g, 1, tok, dart_op, &hash_result);
    }

    // Prepare the hashset for collecting deduplicated result if needed.
    int  i          = 0;
    Set *result_set = NULL;
    if (!is_index_write_op(input_param.op_type)) {
        result_set = set_new(ui64_hash, ui64_equal);
        set_register_free_function(result_set, free);
    }

    uint64_t total_count = dart_perform_on_servers(&hash_result, num_servers, &input_param, result_set);

    // Pick deduplicated result.
    *n_res = set_num_entries(result_set);
    if (*n_res > 0) {
        *out               = (uint64_t *)PDC_calloc(*n_res, sizeof(uint64_t));
        uint64_t **set_arr = (uint64_t **)set_to_array(result_set);
        for (i = 0; i < *n_res; i++) {
            (*out)[i] = set_arr[i][0];
        }
        set_arr = (uint64_t **)PDC_free(set_arr);
    }
    set_free(result_set);

    // done:
    k_query = (char *)PDC_free(k_query);
    v_query = (char *)PDC_free(v_query);

    if (tok != NULL)
        tok = (char *)PDC_free(tok);

    timer_pause(&timer);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_delete_obj_ref_from_dart(dart_hash_algo_t hash_algo, char *attr_key, void *attr_val,
                                    size_t attr_vsize, pdc_c_var_type_t attr_vtype,
                                    dart_object_ref_type_t ref_type, uint64_t data)
{
    FUNC_ENTER(NULL);

    perr_t                       ret_value = SUCCEED;
    dart_perform_one_server_in_t input_param;
    input_param.op_type      = OP_DELETE;
    input_param.hash_algo    = hash_algo;
    input_param.attr_key     = attr_key;
    input_param.attr_val     = attr_val;
    input_param.attr_vsize   = attr_vsize;
    input_param.attr_vtype   = attr_vtype;
    input_param.obj_ref_type = ref_type;
    // FIXME: temporarily ugly implementation here, some assignment can be ignored
    // and save some bytes for data transfer.
    input_param.obj_primary_ref   = data;
    input_param.obj_secondary_ref = data;
    input_param.obj_server_ref    = data;
    // TODO: see if timestamp can help
    // input_param.timestamp = get_timestamp_us();
    input_param.timestamp = 1;

    int                  num_servers = 0;
    index_hash_result_t *hash_result = NULL;
    if (hash_algo == DART_HASH) {
        num_servers = DART_hash(dart_g, attr_key, OP_DELETE, NULL, &hash_result);
    }
    else if (hash_algo == DHT_FULL_HASH) {
        num_servers = DHT_hash(dart_g, strlen(attr_key), attr_key, OP_DELETE, &hash_result);
    }
    else if (hash_algo == DHT_INITIAL_HASH) {
        num_servers = DHT_hash(dart_g, 1, attr_key, OP_DELETE, &hash_result);
    }

    dart_perform_on_servers(&hash_result, num_servers, &input_param, NULL);

    // done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_insert_obj_ref_into_dart(dart_hash_algo_t hash_algo, char *attr_key, void *attr_val,
                                    size_t attr_vsize, pdc_c_var_type_t attr_vtype,
                                    dart_object_ref_type_t ref_type, uint64_t data)
{
    FUNC_ENTER(NULL);

    perr_t                       ret_value = SUCCEED;
    dart_perform_one_server_in_t input_param;
    input_param.op_type      = OP_INSERT;
    input_param.hash_algo    = hash_algo;
    input_param.attr_key     = attr_key;
    input_param.attr_val     = attr_val;
    input_param.attr_vsize   = attr_vsize;
    input_param.attr_vtype   = attr_vtype;
    input_param.obj_ref_type = ref_type;
    // FIXME: temporarily ugly implementation here, some assignment can be ignored
    // and save some bytes for data transfer.
    input_param.obj_primary_ref   = data;
    input_param.obj_secondary_ref = data;
    input_param.obj_server_ref    = data;
    // TODO: see if timestamp can help
    // input_param.timestamp = get_timestamp_us();
    input_param.timestamp = 1;

    int                  num_servers = 0;
    index_hash_result_t *hash_result = NULL;
    if (hash_algo == DART_HASH) {
        // suffix-tree mode switch will be set during this call.
        num_servers = DART_hash(dart_g, attr_key, OP_INSERT, dart_retrieve_server_info_cb, &hash_result);
    }
    else if (hash_algo == DHT_FULL_HASH) {
        num_servers = DHT_hash(dart_g, strlen(attr_key), attr_key, OP_INSERT, &hash_result);
    }
    else if (hash_algo == DHT_INITIAL_HASH) {
        num_servers = DHT_hash(dart_g, 1, attr_key, OP_INSERT, &hash_result);
    }

    dart_perform_on_servers(&hash_result, num_servers, &input_param, NULL);

    // done:
    FUNC_LEAVE(ret_value);
}

/******************** METADATA INDEX ENDS ***********************************/

/******************** Collective Object Selection Query Starts *******************************/

// #define ENABLE_MPI
#ifdef ENABLE_MPI

void
get_first_sender(int *first_sender_global_rank, int *num_groups, int *sender_group_id, int *rank_in_group,
                 int *sender_group_size, int *prefer_custom_exchange)
{
    FUNC_ENTER(NULL);

    if (pdc_client_mpi_size_g >= pdc_server_num_g) {
        *sender_group_size        = pdc_server_num_g;
        *num_groups               = pdc_client_mpi_size_g / (*sender_group_size);
        *rank_in_group            = object_selection_query_counter_g % (*sender_group_size);
        *sender_group_id          = 0;
        *first_sender_global_rank = (*rank_in_group);
        *prefer_custom_exchange   = 1;
    }
    else {
        *num_groups               = 1;
        *first_sender_global_rank = object_selection_query_counter_g % pdc_client_mpi_size_g;
        *sender_group_id          = 0;
        *rank_in_group            = (*first_sender_global_rank);
        *sender_group_size        = pdc_client_mpi_size_g;
        *prefer_custom_exchange   = 0;
    }

    FUNC_LEAVE_VOID();
}

void
PDC_assign_server(int32_t *my_server_start, int32_t *my_server_end, int32_t *my_server_count)
{
    FUNC_ENTER(NULL);

    if (pdc_server_num_g > pdc_client_mpi_size_g) {
        *my_server_count = pdc_server_num_g / pdc_client_mpi_size_g;
        *my_server_start = pdc_client_mpi_rank_g * (*my_server_count);
        *my_server_end   = *my_server_start + (*my_server_count);
        if (pdc_client_mpi_rank_g == pdc_client_mpi_size_g - 1) {
            (*my_server_end) += pdc_server_num_g % pdc_client_mpi_size_g;
        }
    }
    else {
        *my_server_start = pdc_client_mpi_rank_g;
        *my_server_end   = *my_server_start + 1;
        if (pdc_client_mpi_rank_g >= pdc_server_num_g) {
            *my_server_end = 0;
        }
    }

    FUNC_LEAVE_VOID();
}

// All clients collectively query all servers, each client gets partial results
perr_t
PDC_Client_query_kvtag_col(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids, int *query_sent)
{
    FUNC_ENTER(NULL);

    perr_t    ret_value = SUCCEED;
    int32_t   my_server_start, my_server_end, my_server_count;
    int32_t   i;
    int       nmeta    = 0;
    uint64_t *temp_ids = NULL;

    PDC_assign_server(&my_server_start, &my_server_end, &my_server_count);

    *n_res      = 0;
    *pdc_ids    = NULL;
    *query_sent = 1;
    for (i = my_server_start; i < my_server_end; i++) {
        if (i >= pdc_server_num_g) {
            *query_sent = 0;
            break;
        }
        temp_ids  = NULL;
        ret_value = PDC_Client_query_kvtag_server((uint32_t)i, kvtag, &nmeta, &temp_ids);
        if (ret_value != SUCCEED) {
            PGOTO_ERROR(FAIL, "Querying server %u", i);
        }
        if (i == my_server_start) {
            *pdc_ids = temp_ids;
        }
        else if (nmeta > 0) {
            *pdc_ids = (uint64_t *)PDC_realloc(*pdc_ids, sizeof(uint64_t) * (*n_res + nmeta));
            memcpy(*pdc_ids + (*n_res), temp_ids, nmeta * sizeof(uint64_t));
            temp_ids = (uint64_t *)PDC_free(temp_ids);
        }
        *n_res      = *n_res + nmeta;
        *query_sent = 1;
    }

done:
    FUNC_LEAVE(ret_value);
}

void
_standard_all_gather_result(int query_sent, int *n_res, uint64_t **pdc_ids, MPI_Comm world_comm)
{
    FUNC_ENTER(NULL);

    int    i = 0, ntotal = 0, *disp = NULL;
    double stime = 0.0, duration = 0.0;
    stime = MPI_Wtime();

    int *all_nmeta_array = (int *)PDC_calloc(pdc_client_mpi_size_g, sizeof(int));
    MPI_Allgather(n_res, 1, MPI_INT, all_nmeta_array, 1, MPI_INT, world_comm);

    duration = MPI_Wtime() - stime;

    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("Time for MPI_Allgather for Syncing ID count: %.4f ms\n", duration * 1000.0);

    disp   = (int *)PDC_calloc(pdc_client_mpi_size_g, sizeof(int));
    ntotal = 0;
    for (i = 0; i < pdc_client_mpi_size_g; i++) {
        disp[i] = ntotal;
        ntotal += all_nmeta_array[i];
    }

    uint64_t *all_ids = (uint64_t *)PDC_malloc(ntotal * sizeof(uint64_t));
    MPI_Allgatherv(*pdc_ids, *n_res, MPI_UINT64_T, all_ids, all_nmeta_array, disp, MPI_UINT64_T, world_comm);

    if (*pdc_ids)
        *pdc_ids = (uint64_t *)PDC_free(*pdc_ids);

    *n_res   = ntotal;
    *pdc_ids = all_ids;

    all_nmeta_array = (int *)PDC_free(all_nmeta_array);
    disp            = (int *)PDC_free(disp);

    FUNC_LEAVE_VOID();
}

void
_customized_all_gather_result(int query_sent, int *n_res, uint64_t **pdc_ids, MPI_Comm world_comm)
{
    FUNC_ENTER(NULL);

    int    i = 0, *all_nmeta = NULL, ntotal = 0, *disp = NULL;
    double stime = 0.0, duration = 0.0;

    stime = MPI_Wtime();
    // First, let's get the number of results from each client.

    // In the case where the total number of clients is far larger than the total number of servers,
    // say 20x larger, since not all ranks are participating in the query, we need to limit the MPI
    // communication to those ranks only. This can help reduce the communication overhead, especially
    // when the number of ranks is far larger than the number of servers.

    int      sub_comm_color = query_sent == 1 ? 1 : 0;
    MPI_Comm sub_comm;
    MPI_Comm_split(world_comm, sub_comm_color, pdc_client_mpi_rank_g, &sub_comm);
    int sub_comm_rank, sub_comm_size;
    MPI_Comm_rank(sub_comm, &sub_comm_rank);
    MPI_Comm_size(sub_comm, &sub_comm_size);

    int  n_sent_ranks  = sub_comm_color == 1 ? sub_comm_size : pdc_client_mpi_size_g - sub_comm_size;
    int  sub_n_obj_len = n_sent_ranks + 1; // the last element is the first rank who sent the query.
    int *sub_n_obj_arr = (int *)PDC_calloc(sub_n_obj_len, sizeof(int));
    // FIXME: how to get the global rank number of the first rank who sent the query?
    // currently, we use 0, since each time when PDC_Client_query_kvtag_col runs, it is always using the
    // first N ranks to send the query, where N is the number of servers.
    sub_n_obj_arr[sub_n_obj_len - 1] = 0;

    if (sub_comm_color == 1) {
        // the result is first gathered among the ranks who sent the requests.
        MPI_Allgather(n_res, 1, MPI_INT, sub_n_obj_arr, 1, MPI_INT,
                      sub_comm); // get the number of results
    }

    MPI_Barrier(world_comm);
    // FIXME: check root for MPI_Bcast accociated with the world_comm.
    int root = sub_n_obj_arr[sub_n_obj_len - 1];
    MPI_Bcast(sub_n_obj_arr, sub_n_obj_len, MPI_INT, root, world_comm);
    // now all ranks in the world_comm should know about the number of results from each rank who sent the
    // query.
    duration = MPI_Wtime() - stime;
    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("Time for MPI_Allgather for Syncing ID count: %.4f ms\n", duration * 1000.0);

    // Okay, now each rank of the WORLD_COMM knows about the number of results from the clients who sent
    // queries.
    // Let's calculate the total number of results, and the displacement for each client.
    MPI_Barrier(world_comm);

    all_nmeta = (int *)PDC_calloc(pdc_client_mpi_size_g, sizeof(int));
    disp      = (int *)PDC_calloc(pdc_client_mpi_size_g, sizeof(int));
    ntotal    = 0;
    for (i = 0; i < sub_n_obj_len - 1; i++) {
        all_nmeta[i] = sub_n_obj_arr[i];
        disp[i]      = ntotal;
        ntotal += all_nmeta[i];
    }

    // Finally, let's gather all the results. Since each client is getting a partial result which can be
    // of different size, we need to use MPI_Allgatherv for gathering variable-size arrays from different
    // clients.
    uint64_t *all_ids = (uint64_t *)PDC_malloc(ntotal * sizeof(uint64_t));

    MPI_Allgatherv(*pdc_ids, *n_res, MPI_UINT64_T, all_ids, all_nmeta, disp, MPI_UINT64_T, world_comm);

    MPI_Comm_free(&sub_comm);

    // Now, let's return the result to the caller
    *pdc_ids = all_ids;
    *n_res   = ntotal;

    FUNC_LEAVE_VOID();
}

// All clients collectively query all servers, all clients get all results
perr_t
PDC_Client_query_kvtag_mpi(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids, MPI_Comm world_comm)
{
    FUNC_ENTER(NULL);

    int local_increment = 1;
    MPI_Scan(&local_increment, &object_selection_query_counter_g, 1, MPI_INT, MPI_SUM, world_comm);
    perr_t ret_value = SUCCEED;
    int    i, query_sent = 0;
    double stime = 0.0, duration = 0.0;

    MPI_Barrier(world_comm);
    stime = MPI_Wtime();

    ret_value = PDC_Client_query_kvtag_col(kvtag, n_res, pdc_ids, &query_sent);

    MPI_Barrier(world_comm);
    duration = MPI_Wtime() - stime;

    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("Time for C/S communication: %.4f ms\n", duration * 1000.0);

    if (*n_res <= 0) {
        *n_res   = 0;
        *pdc_ids = NULL;
    }

    if (pdc_client_mpi_size_g == 1)
        PGOTO_DONE_VOID;

    MPI_Barrier(world_comm);
    stime = MPI_Wtime();
    // perform all gather to get the complete result.
    _standard_all_gather_result(query_sent, n_res, pdc_ids, world_comm);

    duration = MPI_Wtime() - stime;
    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("Time for MPI_Allgatherv for Syncing ID array: %.4f ms\n", duration * 1000.0);

    // deducplicating result with a Set.
    Set *result_set = set_new(ui64_hash, ui64_equal);
    set_register_free_function(result_set, free);
    for (i = 0; i < *n_res; i++) {
        uint64_t *id = (uint64_t *)PDC_malloc(sizeof(uint64_t));
        *id          = (*pdc_ids)[i];
        set_insert(result_set, id);
    }
    *pdc_ids = (uint64_t *)PDC_free(*pdc_ids);
    // Pick deduplicated result.
    *n_res = set_num_entries(result_set);
    if (*n_res > 0) {
        *pdc_ids           = (uint64_t *)PDC_calloc(*n_res, sizeof(uint64_t));
        uint64_t **set_arr = (uint64_t **)set_to_array(result_set);
        for (i = 0; i < *n_res; i++) {
            (*pdc_ids)[i] = set_arr[i][0];
        }
        set_arr = (uint64_t **)PDC_free(set_arr);
    }
    set_free(result_set);

done:
    FUNC_LEAVE(ret_value);
}

void
_standard_bcast_result(int root, int *n_res, uint64_t **out, MPI_Comm world_comm)
{
    FUNC_ENTER(NULL);

    double stime = 0.0, duration = 0.0;

    stime = MPI_Wtime();
    // broadcast n_res to all other ranks from root
    MPI_Bcast(n_res, 1, MPI_INT, root, world_comm);

    duration = MPI_Wtime() - stime;

    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("Time for MPI_Bcast for Syncing ID count: %.4f ms\n", duration * 1000.0);

    if (pdc_client_mpi_rank_g != root)
        *out = (uint64_t *)PDC_calloc(*n_res, sizeof(uint64_t));

    // broadcast the result to all other ranks
    MPI_Bcast(*out, *n_res, MPI_UINT64_T, root, world_comm);

    FUNC_LEAVE_VOID();
}

void
_customized_bcast_result(int first_sender_global_rank, int num_groups, int sender_group_id, int rank_in_group,
                         int sender_group_size, int *n_res, uint64_t **out, MPI_Comm world_comm)
{
    FUNC_ENTER(NULL);

    double stime = 0.0, duration = 0.0;
    int    group_head_comm_color;

    stime = MPI_Wtime();

    // FIXME: needs to be examined and fixed.

    if (num_groups > 1) {
        group_head_comm_color = pdc_client_mpi_rank_g % sender_group_size == rank_in_group;
    }
    else {
        group_head_comm_color = 0;
    }

    // Note: we should set comm to be MPI_COMM_WORLD since all assumptions are
    // made with the total number of client ranks. let's select n ranks to be the
    // sender ranks, where n is the number of servers.
    MPI_Comm group_head_comm;
    MPI_Comm_split(world_comm, group_head_comm_color, pdc_client_mpi_rank_g, &group_head_comm);
    int group_head_comm_rank, group_head_comm_size;
    MPI_Comm_rank(group_head_comm, &group_head_comm_rank);
    MPI_Comm_size(group_head_comm, &group_head_comm_size);

    // broadcast result size among group_head_comm
    if (group_head_comm_color == 1) {
        MPI_Bcast(n_res, 1, MPI_INT, rank_in_group, group_head_comm);
    }

    // now, all the n sender ranks has the result. Let's broadcast the result to all other ranks.
    // suppose number of servers is 16, then the groups can be
    // 0-15, 16-31, 32-47, 48-63, 64-68...
    // rank/16 = 0, 1, 2, 3(including 48-63 and 64-68), ...,
    // this means we can divide all client ranks into rank/#server groups.
    // within each group, we can perform a BCAST, using the first rank as the root.
    int      group_color = pdc_client_mpi_rank_g / pdc_server_num_g;
    MPI_Comm group_comm;
    MPI_Comm_split(world_comm, group_color, pdc_client_mpi_rank_g, &group_comm);
    int group_rank, group_size;
    MPI_Comm_rank(group_comm, &group_rank);
    MPI_Comm_size(group_comm, &group_size);

    MPI_Bcast(n_res, 1, MPI_INT, rank_in_group, group_comm);

    MPI_Barrier(world_comm);
    duration = MPI_Wtime() - stime;

    if (pdc_client_mpi_rank_g == 0)
        LOG_INFO("Time for MPI_Bcast for Syncing ID count: %.4f ms\n", duration * 1000.0);

    // Okay, now each rank of the WORLD_COMM knows about the number of results from the sender ranks, and
    // the root for WORLD_COMM. Let's perform BCAST for the array data. for those ranks that are not the
    // root, allocate memory for the object IDs.
    if (*out == NULL) {
        *out = (uint64_t *)PDC_calloc(*n_res, sizeof(uint64_t));
    }

    MPI_Bcast(*out, *n_res, MPI_UINT64_T, first_sender_global_rank, world_comm);

    MPI_Comm_free(&group_head_comm);
    MPI_Comm_free(&group_comm);

    FUNC_LEAVE_VOID();
}

perr_t
PDC_Client_search_obj_ref_through_dart_mpi(dart_hash_algo_t hash_algo, char *query_string,
                                           dart_object_ref_type_t ref_type, int *n_res, uint64_t **out,
                                           MPI_Comm world_comm)
{
    FUNC_ENTER(NULL);

    int local_increment = 1;
    MPI_Scan(&local_increment, &object_selection_query_counter_g, 1, MPI_INT, MPI_SUM, world_comm);
    perr_t ret_value = SUCCEED;

    if (n_res == NULL || out == NULL) {
        ret_value = FAIL;
        FUNC_LEAVE(ret_value);
    }

    int       n_obj = 0;
    uint64_t *dart_out;
    double    stime = 0.0, duration = 0.0;

    // Let's calcualte an approprate root.
    // FIXME: needs to be examined and fixed. Currently, first_sender_global_rank is different in
    // different ranks. this is not correct.
    int first_sender_global_rank, num_groups, sender_group_id, rank_in_group, sender_group_size,
        prefer_custom_exchange;

    get_first_sender(&first_sender_global_rank, &num_groups, &sender_group_id, &rank_in_group,
                     &sender_group_size, &prefer_custom_exchange);

    // broadcast first_sender_global_rank to all other ranks
    MPI_Bcast(&first_sender_global_rank, 1, MPI_INT, 0, world_comm);

    MPI_Barrier(world_comm);
    stime = MPI_Wtime();

    // let the root send the query
    if (pdc_client_mpi_rank_g == first_sender_global_rank)
        PDC_Client_search_obj_ref_through_dart(hash_algo, query_string, ref_type, &n_obj, &dart_out);

    duration = MPI_Wtime() - stime;
    if (pdc_client_mpi_rank_g == first_sender_global_rank)
        LOG_INFO("Time for C/S communication: %.4f ms\n", duration * 1000.0);

    MPI_Barrier(world_comm);
    stime = MPI_Wtime();

    // Now, let's broadcast the result to all other ranks.
    _standard_bcast_result(first_sender_global_rank, &n_obj, &dart_out, world_comm);

    duration = MPI_Wtime() - stime;

    if (pdc_client_mpi_rank_g == first_sender_global_rank)
        LOG_INFO("Time for MPI_Bcast for Syncing ID array: %.4f ms\n", duration * 1000.0);

    *n_res = n_obj;
    *out   = dart_out;
    FUNC_LEAVE(ret_value);
}
#endif

/******************** Collective Object Selection Query Ends *******************************/
