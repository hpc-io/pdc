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

#include "../server/pdc_utlist.h"
#include "pdc_id_pkg.h"
#include "pdc_prop_pkg.h"
#include "pdc_obj_pkg.h"
#include "pdc_cont.h"
#include "pdc_region.h"
#include "pdc_interface.h"
#include "pdc_analysis_pkg.h"
#include "pdc_transforms_common.h"
#include "pdc_client_connect.h"

#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_hl.h"

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

#include "pdc_timing.h"

int                    is_client_debug_g      = 0;
pdc_server_selection_t pdc_server_selection_g = PDC_SERVER_DEFAULT;
int                    pdc_client_mpi_rank_g  = 0;
int                    pdc_client_mpi_size_g  = 1;

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
static int           work_todo_g        = 0;
int                  query_id_g         = 0;

static hg_id_t client_test_connect_register_id_g;
static hg_id_t gen_obj_register_id_g;
static hg_id_t gen_cont_register_id_g;
static hg_id_t close_server_register_id_g;
static hg_id_t metadata_query_register_id_g;
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

// bulk
static hg_id_t    query_partial_register_id_g;
static hg_id_t    query_kvtag_register_id_g;
static int        bulk_todo_g = 0;
hg_atomic_int32_t bulk_transfer_done_g;

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

int                        cache_percentage_g       = 0;
int                        cache_count_g            = 0;
int                        cache_total_g            = 0;
pdc_data_server_io_list_t *client_cache_list_head_g = NULL;

/*
 *
 * Client Functions
 *
 */
static inline uint32_t
get_server_id_by_hash_name(const char *name)
{
    return (uint32_t)(PDC_get_hash_by_name(name) % pdc_server_num_g);
}

static inline uint32_t
get_server_id_by_obj_id(uint64_t obj_id)
{
    return (uint32_t)((obj_id / PDC_SERVER_ID_INTERVEL - 1) % pdc_server_num_g);
}

// Generic function to check the return value (RPC receipt) is 1
hg_return_t
pdc_client_check_int_ret_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    pdc_int_ret_t                   output;
    hg_handle_t                     handle;
    struct _pdc_client_lookup_args *lookup_args;

    FUNC_ENTER(NULL);

    handle      = callback_info->info.forward.handle;
    lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "==Error with HG_Get_output");

    if (output.ret != 1)
        printf("==%s() - Return value [%d] is NOT expected\n", __func__, output.ret);

    if (lookup_args != NULL)
        lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Check if all work has been processed
// Using global variable $mercury_work_todo_g
perr_t
PDC_Client_check_response(hg_context_t **hg_context)
{
    perr_t       ret_value = SUCCEED;
    hg_return_t  hg_ret;
    unsigned int actual_count;

    FUNC_ENTER(NULL);

    do {
        do {
            hg_ret = HG_Trigger(*hg_context, 0 /* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (work_todo_g <= 0)
            break;

        hg_ret = HG_Progress(*hg_context, HG_MAX_IDLE_TIME);
    } while (hg_ret == HG_SUCCESS || hg_ret == HG_TIMEOUT);

    ret_value = SUCCEED;

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_read_server_addr_from_file()
{
    perr_t ret_value = SUCCEED;
    int    max_tries = 9, sleeptime = 1;
    int    i = 0, is_server_ready = 0;
    char * p;
    FILE * na_config = NULL;
    char   config_fname[PATH_MAX];
    char   n_server_string[PATH_MAX];

    FUNC_ENTER(NULL);

    if (pdc_client_mpi_rank_g == 0) {
        sprintf(config_fname, "%s/%s", pdc_client_tmp_dir_g, pdc_server_cfg_name_g);

        for (i = 0; i < max_tries; i++) {
            if (access(config_fname, F_OK) != -1) {
                is_server_ready = 1;
                break;
            }
            printf("==PDC_CLIENT[%d]: Config file from default location [%s] not available, "
                   "waiting %d seconds\n",
                   pdc_client_mpi_rank_g, config_fname, sleeptime);
            fflush(stdout);
            sleep(sleeptime);
            sleeptime *= 2;
        }
        if (is_server_ready != 1)
            PGOTO_ERROR(FAIL, "server is not ready");

        na_config = fopen(config_fname, "r");
        if (!na_config)
            PGOTO_ERROR(FAIL, "Could not open config file from default location: %s", config_fname);

        // Get the first line as $pdc_server_num_g
        if (fgets(n_server_string, PATH_MAX, na_config) == NULL) {
            PGOTO_ERROR(FAIL, "Get first line failed\n");
        }
        pdc_server_num_g = atoi(n_server_string);
    }

#ifdef ENABLE_MPI
    MPI_Bcast(&pdc_server_num_g, 1, MPI_INT, 0, PDC_CLIENT_COMM_WORLD_g);
#endif

    if (pdc_server_num_g == 0) {
        printf("==PDC_CLIENT[%d]: server number error %d\n", pdc_client_mpi_rank_g, pdc_server_num_g);
        return -1;
    }

    // Allocate $pdc_server_info_g
    pdc_server_info_g = (struct _pdc_server_info *)calloc(sizeof(struct _pdc_server_info), pdc_server_num_g);

    i = 0;
    while (i < pdc_server_num_g) {
        if (pdc_client_mpi_rank_g == 0) {
            if (fgets(pdc_server_info_g[i].addr_string, ADDR_MAX, na_config) == NULL) {
                PGOTO_ERROR(FAIL, "Get first line failed\n");
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
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_buf_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t               ret_value = HG_SUCCESS;
    hg_handle_t               handle;
    struct _pdc_buf_map_args *region_unmap_args;
    buf_unmap_out_t           output;

    FUNC_ENTER(NULL);

    region_unmap_args = (struct _pdc_buf_map_args *)callback_info->arg;
    handle            = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: client_send_buf_unmap_rpc_cb error with HG_Get_output\n",
               pdc_client_mpi_rank_g);
        fflush(stdout);
        region_unmap_args->ret = -1;
        PGOTO_DONE(ret_value);
    }

    region_unmap_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_buf_map_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t               ret_value = HG_SUCCESS;
    hg_handle_t               handle;
    struct _pdc_buf_map_args *buf_map_args;
    buf_map_out_t             output;

    FUNC_ENTER(NULL);

    buf_map_args = (struct _pdc_buf_map_args *)callback_info->arg;
    handle       = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        buf_map_args->ret = -1;
        PGOTO_ERROR(ret_value, "PDC_CLIENT[%d]: client_send_buf_map_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }

    buf_map_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g = 0;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_test_connect_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;

    FUNC_ENTER(NULL);

    struct _pdc_client_lookup_args *client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t                     handle             = callback_info->info.forward.handle;

    client_test_connect_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "PDC_CLIENT[%d]: error with HG_Get_output", pdc_client_mpi_rank_g);
    }

    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g = 0;
    HG_Free_output(handle, &output);
    HG_Destroy(callback_info->info.forward.handle);

    FUNC_LEAVE(ret_value);
}

// Callback function for HG_Addr_lookup()
static hg_return_t
client_test_connect_lookup_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    uint32_t                        server_id;
    struct _pdc_client_lookup_args *client_lookup_args;
    client_test_connect_in_t        in;
    hg_handle_t                     client_test_handle;

    FUNC_ENTER(NULL);

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

    ret_value = HG_Forward(client_test_handle, client_test_connect_rpc_cb, client_lookup_args, &in);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not start HG_Forward");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_lookup_server(int server_id)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    struct _pdc_client_lookup_args lookup_args;
    char                           self_addr[ADDR_MAX];
    char *                         target_addr_string;

    FUNC_ENTER(NULL);

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with server id input %d", pdc_client_mpi_rank_g,
                    server_id);

    ret_value = PDC_get_self_addr(send_class_g, self_addr);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: ERROR geting self addr", pdc_client_mpi_rank_g);

    lookup_args.client_id   = pdc_client_mpi_rank_g;
    lookup_args.server_id   = server_id;
    lookup_args.client_addr = self_addr;
    target_addr_string      = pdc_server_info_g[lookup_args.server_id].addr_string;

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT[%d]: - Testing connection to server %d [%s]\n", pdc_client_mpi_rank_g,
               lookup_args.server_id, target_addr_string);
        fflush(stdout);
    }

    hg_ret = HG_Addr_lookup(send_context_g, client_test_connect_lookup_cb, &lookup_args, target_addr_string,
                            HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: Connection to server FAILED!");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT[%5d]: - connected to server %5d\n", pdc_client_mpi_rank_g,
               lookup_args.server_id);
        fflush(stdout);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_try_lookup_server(int server_id)
{
    perr_t ret_value = SUCCEED;
    int    n_retry   = 1;

    FUNC_ENTER(NULL);

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid server ID %d", pdc_client_mpi_rank_g, server_id);

    while (pdc_server_info_g[server_id].addr_valid != 1) {
        if (n_retry > PDC_MAX_TRIAL_NUM)
            break;
        ret_value = PDC_Client_lookup_server(server_id);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with PDC_Client_lookup_server", pdc_client_mpi_rank_g);
        n_retry++;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    hg_handle_t                     handle;
    struct _pdc_client_lookup_args *client_lookup_args;
    gen_obj_id_out_t                output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->obj_id = 0;
        PGOTO_ERROR(ret_value, "PDC_CLIENT[%d]: client_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->obj_id = output.obj_id;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_region_lock_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                   ret_value = HG_SUCCESS;
    hg_handle_t                   handle;
    struct _pdc_region_lock_args *client_lookup_args;
    region_lock_out_t             output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_region_lock_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "PDC_CLIENT[%d]: error with HG_Get_output", pdc_client_mpi_rank_g);
    }

    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_region_release_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    hg_handle_t                     handle;
    struct _pdc_client_lookup_args *client_lookup_args;
    region_lock_out_t               output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: client_region_release_rpc_cb - HG_Get_output error!");

    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}
/*
static hg_return_t
client_region_release_with_transform_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                        ret_value = HG_SUCCESS;
    hg_handle_t                        handle;
    struct _pdc_client_transform_args *transform_args;
    region_lock_out_t                  output;
    size_t                             size;
    size_t (*this_transform)(void *, pdc_var_type_t, int, uint64_t *, void **, pdc_var_type_t) = NULL;
    void *result;

    FUNC_ENTER(NULL);

    transform_args = (struct _pdc_client_transform_args *)callback_info->arg;
    result         = transform_args->transform_result;
    handle         = callback_info->info.forward.handle;
    this_transform = transform_args->this_transform->ftnPtr;
    size           = this_transform(transform_args->data, transform_args->this_transform->dest_type,
                          transform_args->region_info->ndim, transform_args->region_info->size, &result,
                          transform_args->this_transform->dest_type);

    transform_args->size = size;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "==PDC_CLIENT: client_region_release_rpc_cb - HG_Get_output error!");

    transform_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}
*/
// Bulk
// Callback after bulk transfer is received by client
static hg_return_t
hg_test_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t         ret_value = HG_SUCCESS;
    struct bulk_args_t *bulk_args;
    hg_bulk_t           local_bulk_handle;
    uint32_t            i;
    void *              buf = NULL;
    uint32_t            n_meta;
    uint64_t            buf_sizes[2] = {0, 0};
    uint32_t            actual_cnt;
    pdc_metadata_t *    meta_ptr;

    FUNC_ENTER(NULL);

    bulk_args         = (struct bulk_args_t *)hg_cb_info->arg;
    local_bulk_handle = hg_cb_info->info.bulk.local_handle;

    if (hg_cb_info->ret == HG_CANCELED)
        printf("HG_Bulk_transfer() was successfully canceled\n");
    else if (hg_cb_info->ret != HG_SUCCESS)
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "Error in callback");

    n_meta = bulk_args->n_meta;

    if (hg_cb_info->ret == HG_SUCCESS) {
        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, buf_sizes,
                       &actual_cnt);

        meta_ptr            = (pdc_metadata_t *)(buf);
        bulk_args->meta_arr = (pdc_metadata_t **)calloc(sizeof(pdc_metadata_t *), n_meta);
        for (i = 0; i < n_meta; i++) {
            bulk_args->meta_arr[i] = meta_ptr;
            meta_ptr++;
        }
    }

    bulk_todo_g--;
    hg_atomic_set32(&bulk_transfer_done_g, 1);

    // Free block handle
    ret_value = HG_Bulk_free(local_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

done:
    fflush(stdout);
    free(bulk_args);

    FUNC_LEAVE(ret_value);
}

// Bulk
// No need to have multi-threading
int
PDC_Client_check_bulk(hg_context_t *hg_context)
{
    int          ret_value;
    hg_return_t  hg_ret;
    unsigned int actual_count;

    FUNC_ENTER(NULL);

    /* Poke progress engine and check for events */
    do {
        actual_count = 0;
        do {
            hg_ret = HG_Trigger(hg_context, 0 /* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (bulk_todo_g <= 0)
            break;
        hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME);

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
    char *      ret_value = NULL;
    const char *errstring = "UNDEFINED";

    FUNC_ENTER(NULL);

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
    perr_t ret_value = SUCCEED;
    char   na_info_string[PATH_MAX];
    char   hostname[ADDR_MAX];
    int    local_server_id;
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

    FUNC_ENTER(NULL);

    if ((hg_transport = getenv("HG_TRANSPORT")) == NULL) {
        hg_transport = default_hg_transport;
    }
    memset(hostname, 0, sizeof(hostname));
    gethostname(hostname, sizeof(hostname));
    sprintf(na_info_string, "%s://%s:%d", hg_transport, hostname, port);
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: using %.7s\n", na_info_string);
        fflush(stdout);
    }

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
            goto drc_access_again;
        }
        printf("client drc_access() failed (%d, %s)", rc, drc_strerror(-rc));
        fflush(stdout);
        PGOTO_ERROR(FAIL, "client drc_access() failed");
    }
    cookie = drc_get_first_cookie(credential_info);

    if (pdc_client_mpi_rank_g == 0) {
        printf("# Credential is %u\n", credential);
        printf("# Cookie is %u\n", cookie);
        fflush(stdout);
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

//#ifndef PDC_HAS_CRAY_DRC
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
    HG_Registered_disable_response(*hg_class, close_server_register_id_g, HG_TRUE);

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

    // bulk
    query_partial_register_id_g = PDC_query_partial_register(*hg_class);
    query_kvtag_register_id_g   = PDC_query_kvtag_register(*hg_class);

    cont_add_del_objs_rpc_register_id_g      = PDC_cont_add_del_objs_rpc_register(*hg_class);
    cont_add_tags_rpc_register_id_g          = PDC_cont_add_tags_rpc_register(*hg_class);
    query_read_obj_name_register_id_g        = PDC_query_read_obj_name_rpc_register(*hg_class);
    query_read_obj_name_client_register_id_g = PDC_query_read_obj_name_client_rpc_register(*hg_class);
    send_region_storage_meta_shm_bulk_rpc_register_id_g = PDC_send_shm_bulk_rpc_register(*hg_class);

    // Map
    buf_map_register_id_g   = PDC_buf_map_register(*hg_class);
    buf_unmap_register_id_g = PDC_buf_unmap_register(*hg_class);

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

#ifdef ENABLE_MULTITHREAD
    /* Mutex initialization for the client versions of these... */
    /* The Server versions gets initialized in pdc_server.c */
    hg_thread_mutex_init(&pdc_client_info_mutex_g);
    hg_thread_mutex_init(&lock_list_mutex_g);
    hg_thread_mutex_init(&meta_buf_map_mutex_g);
    hg_thread_mutex_init(&meta_obj_map_mutex_g);
#endif
    // Client 0 looks up all servers, others only lookup their node local server
    char *client_lookup_env = getenv("PDC_CLIENT_LOOKUP");
    if (client_lookup_env == NULL || strcmp(client_lookup_env, "ALL") == 0) {
        if (pdc_client_mpi_rank_g == 0)
            printf("==PDC_CLIENT[%d]: Client lookup all servers at start time!\n", pdc_client_mpi_rank_g);
        for (local_server_id = 0; local_server_id < pdc_server_num_g; local_server_id++) {
            if (pdc_client_mpi_size_g > 1000)
                PDC_msleep(pdc_client_mpi_rank_g % 300);
            if (PDC_Client_try_lookup_server(local_server_id) != SUCCEED)
                PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR lookup server %d", pdc_client_mpi_rank_g,
                            local_server_id);
        }
    }
    else if (client_lookup_env != NULL && strcmp(client_lookup_env, "NONE") == 0) {
        if (pdc_client_mpi_rank_g == 0)
            printf("==PDC_CLIENT[%d]: Client lookup server at start time disabled!\n", pdc_client_mpi_rank_g);
    }
    else {
        // Each client connect to its node local server only at start time
        local_server_id =
            PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);
        if (PDC_Client_try_lookup_server(local_server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR lookup server %d\n", pdc_client_mpi_rank_g,
                        local_server_id);
    }

    if (is_client_debug_g == 1 && pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT[%d]: Successfully established connection to %d PDC metadata server%s\n\n",
               pdc_client_mpi_rank_g, pdc_server_num_g, pdc_client_mpi_size_g == 1 ? "" : "s");
        fflush(stdout);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_init()
{
    perr_t ret_value  = SUCCEED;
    pdc_server_info_g = NULL;
    char *   tmp_dir;
    uint32_t port;
    int      is_mpi_init = 0;

    FUNC_ENTER(NULL);
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
#endif

    if (pdc_client_mpi_rank_g == 0)
        printf("==PDC_CLIENT: PDC_DEBUG set to %d!\n", is_client_debug_g);

    // get server address and fill in $pdc_server_info_g
    if (PDC_Client_read_server_addr_from_file() != SUCCEED) {
        printf("==PDC_CLIENT[%d]: Error getting PDC Metadata servers info, exiting ...", pdc_server_num_g);
        exit(0);
    }

#ifdef ENABLE_MPI
    // Split the PDC_CLIENT_COMM_WORLD_g communicator, MPI_Comm_split_type requires MPI-3
    MPI_Comm_split_type(PDC_CLIENT_COMM_WORLD_g, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL,
                        &PDC_SAME_NODE_COMM_g);

    MPI_Comm_rank(PDC_SAME_NODE_COMM_g, &pdc_client_same_node_rank_g);
    MPI_Comm_size(PDC_SAME_NODE_COMM_g, &pdc_client_same_node_size_g);

    pdc_nclient_per_server_g = pdc_client_same_node_size_g;
#else
    // Get the number of clients per server(node) through environment variable
    tmp_dir = getenv("PDC_NCLIENT_PER_SERVER");
    if (tmp_dir == NULL)
        pdc_nclient_per_server_g = pdc_client_mpi_size_g / pdc_server_num_g;
    else
        pdc_nclient_per_server_g = atoi(tmp_dir);

    if (pdc_nclient_per_server_g <= 0)
        pdc_nclient_per_server_g = 1;
#endif

    PDC_set_execution_locus(CLIENT_MEMORY);

    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT[0]: Found %d PDC Metadata servers, running with %d PDC clients\n",
               pdc_server_num_g, pdc_client_mpi_size_g);
    }

    // Init debug info
    if (pdc_server_num_g > 0) {
        debug_server_id_count = (int *)malloc(sizeof(int) * pdc_server_num_g);
        memset(debug_server_id_count, 0, sizeof(int) * pdc_server_num_g);
    }
    else
        printf("==PDC_CLIENT: Server number not properly initialized!\n");

    // Cori KNL has 68 cores per node, Haswell 32
    port = pdc_client_mpi_rank_g % PDC_MAX_CORE_PER_NODE + 8000;
    if (mercury_has_init_g == 0) {
        // Init Mercury network connection
        ret_value = PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
        if (ret_value != SUCCEED || send_class_g == NULL || send_context_g == NULL)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Error with Mercury Init, exiting...", pdc_client_mpi_rank_g);

        mercury_has_init_g = 1;
    }

    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT[%d]: using [%s] as tmp dir, %d clients per server\n", pdc_client_mpi_rank_g,
               pdc_client_tmp_dir_g, pdc_nclient_per_server_g);
    }

    srand(time(NULL));

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

int
PDC_get_nproc_per_node()
{
    int ret_value = 0;

    FUNC_ENTER(NULL);

    ret_value = pdc_client_same_node_size_g;

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_destroy_all_handles()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_finalize()
{
    hg_return_t hg_ret;
    perr_t      ret_value = SUCCEED;
    int         i;

    FUNC_ENTER(NULL);

    // Finalize Mercury
    for (i = 0; i < pdc_server_num_g; i++) {
        if (pdc_server_info_g[i].addr_valid) {
            HG_Addr_free(send_class_g, pdc_server_info_g[i].addr);
            pdc_server_info_g[i].addr_valid = 0;
        }
    }

    if (pdc_server_info_g != NULL)
        free(pdc_server_info_g);

#ifndef ENABLE_MPI
    for (i = 0; i < pdc_server_num_g; i++) {
        printf("  Server%3d, %d\n", i, debug_server_id_count[i]);
    }
#endif
    // free debug info
    if (debug_server_id_count != NULL)
        free(debug_server_id_count);

#ifdef ENABLE_TIMING
    if (pdc_client_mpi_rank_g == 0)
        printf("==PDC_CLIENT[%d]: T_memcpy: %.2f\n", pdc_client_mpi_rank_g, memcpy_time_g);
#endif

    hg_ret = HG_Context_destroy(send_context_g);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with HG_Context_destroy", pdc_client_mpi_rank_g);

    hg_ret = HG_Finalize(send_class_g);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with HG_Finalize", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Bulk
static hg_return_t
metadata_query_bulk_cb(const struct hg_cb_info *callback_info)
{
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

    FUNC_ENTER(NULL);

    client_lookup_args = (struct bulk_args_t *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: - error HG_Get_output", pdc_client_mpi_rank_g);

    n_meta                     = output.ret;
    client_lookup_args->n_meta = n_meta;
    if (n_meta == 0) {
        client_lookup_args->meta_arr = NULL;
        PGOTO_DONE(ret_value);
    }
    else
        client_lookup_args->meta_arr = (pdc_metadata_t **)calloc(n_meta, sizeof(pdc_metadata_t *));

    // We have received the bulk handle from server (server uses hg_respond)
    // Use this to initiate a bulk transfer
    origin_bulk_handle = output.bulk_handle;
    hg_info            = HG_Get_info(handle);

    bulk_args = (struct bulk_args_t *)malloc(sizeof(struct bulk_args_t));

    bulk_args->handle = handle;
    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->n_meta = client_lookup_args->n_meta;

    recv_meta = (void *)calloc(sizeof(pdc_metadata_t), n_meta);

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
    bulk_todo_g = 1;
    PDC_Client_check_bulk(send_context_g);

    client_lookup_args->meta_arr = bulk_args->meta_arr;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// bulk test
perr_t
PDC_Client_list_all(int *n_res, pdc_metadata_t ***out)
{
    perr_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = PDC_partial_query(1, -1, NULL, NULL, -1, -1, -1, NULL, n_res, out);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_partial_query(int is_list_all, int user_id, const char *app_name, const char *obj_name,
                  int time_step_from, int time_step_to, int ndim, const char *tags, int *n_res,
                  pdc_metadata_t ***out)
{
    perr_t             ret_value = SUCCEED;
    hg_return_t        hg_ret;
    int                n_recv = 0;
    uint32_t           i, server_id = 0, my_server_start, my_server_end, my_server_count;
    size_t             out_size = 0;
    hg_handle_t        query_partial_handle;
    struct bulk_args_t lookup_args;

    FUNC_ENTER(NULL);

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
        if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_partial_register_id_g,
                           &query_partial_handle);

        if (query_partial_handle == NULL)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: Error with query_partial_handle", pdc_client_mpi_rank_g);

        hg_ret = HG_Forward(query_partial_handle, metadata_query_bulk_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_client_list_all(): Could not start HG_Forward()");

        hg_atomic_set32(&bulk_transfer_done_g, 0);

        // Wait for response from server
        work_todo_g = 1;
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
            *out     = (pdc_metadata_t **)malloc(out_size);
        }
        else {
            out_size += sizeof(pdc_metadata_t *) * (lookup_args.n_meta);
            *out = (pdc_metadata_t **)realloc(*out, out_size);
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
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_tag(const char *tags, int *n_res, pdc_metadata_t ***out)
{
    perr_t             ret_value = SUCCEED;
    hg_return_t        hg_ret;
    int                n_recv = 0;
    uint32_t           i;
    int                server_id = 0;
    size_t             out_size  = 0;
    hg_handle_t        query_partial_handle;
    struct bulk_args_t lookup_args;

    FUNC_ENTER(NULL);

    if (tags == NULL)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: input tag is NULL!", pdc_client_mpi_rank_g);

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
        if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_partial_register_id_g,
                           &query_partial_handle);

        if (query_partial_handle == NULL)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: Error with query_partial_handle", pdc_client_mpi_rank_g);

        hg_ret = HG_Forward(query_partial_handle, metadata_query_bulk_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_client_list_all(): Could not start HG_Forward()");

        hg_atomic_set32(&bulk_transfer_done_g, 0);

        // Wait for response from server
        work_todo_g = 1;
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
            *out     = (pdc_metadata_t **)malloc(out_size);
        }
        else {
            out_size += sizeof(pdc_metadata_t *) * ((lookup_args.n_meta));
            *out = (pdc_metadata_t **)realloc(*out, out_size);
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
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Gets executed after a receving queried metadata from server
static hg_return_t
metadata_query_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                      ret_value;
    struct _pdc_metadata_query_args *client_lookup_args;
    hg_handle_t                      handle;
    metadata_query_out_t             output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_metadata_query_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: metadata_query_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);

    if (output.ret.user_id == -1 && output.ret.obj_id == 0 && output.ret.time_step == -1) {
        client_lookup_args->data = NULL;
    }
    else {
        client_lookup_args->data = (pdc_metadata_t *)malloc(sizeof(pdc_metadata_t));
        if (client_lookup_args->data == NULL)
            PGOTO_ERROR(HG_OTHER_ERROR,
                        "==PDC_CLIENT[%d]: - cannnot allocate space for client_lookup_args->data",
                        pdc_client_mpi_rank_g);

        // Now copy the received metadata info
        ret_value = PDC_metadata_init(client_lookup_args->data);
        ret_value = PDC_transfer_t_to_metadata_t(&output.ret, client_lookup_args->data);
    }

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
metadata_delete_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;
    metadata_delete_out_t           output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_CLIENT[%d]: metadata_delete_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
metadata_delete_by_id_rpc_cb(const struct hg_cb_info *callback_info)
{
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
        PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_CLIENT[%d]: metadata_delete_by_id_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
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
        PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_CLIENT[%d]: metadata_add_tag_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_add_tag(pdcid_t obj_id, const char *tag)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    hg_handle_t                    metadata_add_tag_handle;
    uint64_t                       meta_id;
    uint32_t                       server_id;
    struct _pdc_obj_info *         obj_prop;
    struct _pdc_client_lookup_args lookup_args;
    metadata_add_tag_in_t          in;

    FUNC_ENTER(NULL);

    if (tag == NULL || tag[0] == 0)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: - invalid tag content!", pdc_client_mpi_rank_g);

    obj_prop  = PDC_obj_get_info(obj_id);
    meta_id   = obj_prop->obj_info_pub->meta_id;
    server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_add_tag_register_id_g,
              &metadata_add_tag_handle);

    // Fill input structure
    in.obj_id     = meta_id;
    in.hash_value = PDC_get_hash_by_name(obj_prop->obj_info_pub->name);
    in.new_tag    = tag;

    hg_ret = HG_Forward(metadata_add_tag_handle, metadata_add_tag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: - HG_Forward Error!", pdc_client_mpi_rank_g);

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: - add tag NOT successful ...", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    HG_Destroy(metadata_add_tag_handle);

    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed

static hg_return_t
metadata_update_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;
    metadata_update_out_t           output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_CLIENT[%d]: metadata_update_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_update_metadata(pdc_metadata_t *old, pdc_metadata_t *new)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    int                            hash_name_value;
    uint32_t                       server_id = 0;
    metadata_update_in_t           in;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    metadata_update_handle;

    FUNC_ENTER(NULL);

    if (old == NULL || new == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC_Client_update_metadata() - NULL inputs!");

    hash_name_value = PDC_get_hash_by_name(old->obj_name);
    server_id       = (hash_name_value + old->time_step);
    server_id %= pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

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
        PGOTO_ERROR(FAIL, "PDC_Client_update_metadata_with_name(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        PGOTO_ERROR(FAIL, "PDC_CLIENT: update NOT successful ...");

done:
    fflush(stdout);
    HG_Destroy(metadata_update_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_delete_metadata_by_id(uint64_t obj_id)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    metadata_delete_by_id_in_t     in;
    uint32_t                       server_id;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    metadata_delete_by_id_handle;

    FUNC_ENTER(NULL);

    // Fill input structure
    in.obj_id = obj_id;
    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    if (server_id >= (uint32_t)pdc_server_num_g)
        PGOTO_ERROR(FAIL, "Error with server id: %u", server_id);
    else
        debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_by_id_register_id_g,
                       &metadata_delete_by_id_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_Client_delete_by_id_metadata_with_name(): Could not create handle");

    hg_ret = HG_Forward(metadata_delete_by_id_handle, metadata_delete_by_id_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_delete_by_id_metadata_with_name(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret < 0)
        PGOTO_ERROR(FAIL, "PDC_CLIENT: delete_by_id NOT successful ...");

done:
    fflush(stdout);
    HG_Destroy(metadata_delete_by_id_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_delete_metadata(char *delete_name, pdcid_t obj_delete_prop)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    struct _pdc_obj_prop *         delete_prop;
    metadata_delete_in_t           in;
    int                            hash_name_value;
    uint32_t                       server_id;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    metadata_delete_handle;

    FUNC_ENTER(NULL);

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

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_register_id_g,
                       &metadata_delete_handle);
    hg_ret = HG_Forward(metadata_delete_handle, metadata_delete_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_delete_metadata_with_name(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        printf("PDC_CLIENT: delete NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    fflush(stdout);
    HG_Destroy(metadata_delete_handle);

    FUNC_LEAVE(ret_value);
}

// Search metadata using incomplete ID attributes
// Currently it's only using obj_name, and search from all servers
perr_t
PDC_Client_query_metadata_name_only(const char *obj_name, pdc_metadata_t **out)
{
    perr_t                            ret_value = SUCCEED;
    hg_return_t                       hg_ret    = 0;
    metadata_query_in_t               in;
    struct _pdc_metadata_query_args **lookup_args;
    uint32_t                          server_id;
    uint32_t                          i, count = 0;
    hg_handle_t *                     metadata_query_handle;

    FUNC_ENTER(NULL);

    metadata_query_handle = (hg_handle_t *)malloc(sizeof(hg_handle_t) * pdc_server_num_g);

    // Fill input structure
    in.obj_name   = obj_name;
    in.time_step  = 0;
    in.hash_value = PDC_get_hash_by_name(obj_name);

    lookup_args = (struct _pdc_metadata_query_args **)malloc(sizeof(struct _pdc_metadata_query_args *) *
                                                             pdc_server_num_g);

    for (server_id = 0; server_id < (uint32_t)pdc_server_num_g; server_id++) {
        lookup_args[server_id] =
            (struct _pdc_metadata_query_args *)malloc(sizeof(struct _pdc_metadata_query_args));

        // Debug statistics for counting number of messages sent to each server.
        debug_server_id_count[server_id]++;

        if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g,
                  &metadata_query_handle[server_id]);

        hg_ret =
            HG_Forward(metadata_query_handle[server_id], metadata_query_rpc_cb, lookup_args[server_id], &in);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_Client_query_metadata_name_only(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = pdc_server_num_g;
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
    fflush(stdout);
    free(metadata_query_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_metadata_name_timestep(const char *obj_name, int time_step, pdc_metadata_t **out)
{
    perr_t                          ret_value = SUCCEED;
    hg_return_t                     hg_ret    = 0;
    uint32_t                        hash_name_value;
    uint32_t                        server_id;
    metadata_query_in_t             in;
    struct _pdc_metadata_query_args lookup_args;
    hg_handle_t                     metadata_query_handle;

    FUNC_ENTER(NULL);

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(obj_name);
    server_id       = (hash_name_value + time_step);
    server_id %= pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g,
              &metadata_query_handle);

    // Fill input structure
    in.obj_name   = obj_name;
    in.hash_value = PDC_get_hash_by_name(obj_name);
    in.time_step  = time_step;

    lookup_args.data = (pdc_metadata_t *)malloc(sizeof(pdc_metadata_t));
    if (lookup_args.data == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: ERROR - PDC_Client_query_metadata_with_name() "
                          "cannnot allocate space for client_lookup_args->data");

    hg_ret = HG_Forward(metadata_query_handle, metadata_query_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL,
                    "==PDC_CLIENT[%d] - PDC_Client_query_metadata_with_name(): Could not start HG_Forward()",
                    pdc_client_mpi_rank_g);

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);
    *out = lookup_args.data;

done:
    fflush(stdout);
    HG_Destroy(metadata_query_handle);

    FUNC_LEAVE(ret_value);
}

// Only let one process per node to do the actual query, then broadcast to all others
perr_t
PDC_Client_query_metadata_name_timestep_agg_same_node(const char *obj_name, int time_step,
                                                      pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    if (pdc_client_same_node_rank_g == 0) {
        ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out);
        if (ret_value != SUCCEED || NULL == *out) {
            *out = (pdc_metadata_t *)calloc(1, sizeof(pdc_metadata_t));
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: - ERROR with query [%s]", pdc_client_mpi_rank_g, obj_name);
        }
    }
    else
        *out = (pdc_metadata_t *)calloc(1, sizeof(pdc_metadata_t));

    MPI_Bcast(*out, sizeof(pdc_metadata_t), MPI_CHAR, 0, PDC_SAME_NODE_COMM_g);

#else
    ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_metadata_name_timestep_agg(const char *obj_name, int time_step, pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    if (pdc_client_mpi_rank_g == 0) {
        ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out);
        if (ret_value != SUCCEED || NULL == *out) {
            *out = (pdc_metadata_t *)calloc(1, sizeof(pdc_metadata_t));
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: - ERROR with query [%s]", pdc_client_mpi_rank_g, obj_name);
        }
    }
    else
        *out = (pdc_metadata_t *)calloc(1, sizeof(pdc_metadata_t));

    MPI_Bcast(*out, sizeof(pdc_metadata_t), MPI_CHAR, 0, PDC_CLIENT_COMM_WORLD_g);

#else
    ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_query_name_timestep_agg(const char *obj_name, int time_step, void **out)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_query_metadata_name_timestep_agg(obj_name, time_step, (pdc_metadata_t **)out);

    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an container (object) id
perr_t
PDC_Client_create_cont_id(const char *cont_name, pdcid_t cont_create_prop ATTRIBUTE(unused), pdcid_t *cont_id)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    uint32_t                       server_id = 0;
    gen_cont_id_in_t               in;
    uint32_t                       hash_name_value;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

    FUNC_ENTER(NULL);

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

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    hg_ret =
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_cont_register_id_g, &rpc_handle);

    hg_ret = HG_Forward(rpc_handle, client_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == 0) {
        *cont_id = 0;
        PGOTO_DONE(FAIL);
    }

    *cont_id  = lookup_args.obj_id;
    ret_value = SUCCEED;

done:
    fflush(stdout);
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

// Only one rand sends the request, others wait for MPI broadcast
perr_t
PDC_Client_create_cont_id_mpi(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (pdc_client_mpi_rank_g == 0) {
        ret_value = PDC_Client_create_cont_id(cont_name, cont_create_prop, cont_id);
    }
#ifdef ENABLE_MPI
    MPI_Bcast(cont_id, 1, MPI_LONG_LONG, 0, PDC_CLIENT_COMM_WORLD_g);
#endif

    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
perr_t
PDC_Client_send_name_recv_id(const char *obj_name, uint64_t cont_id, pdcid_t obj_create_prop,
                             pdcid_t *meta_id)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    uint32_t                       server_id   = 0;
    struct _pdc_obj_prop *         create_prop = NULL;
    gen_obj_id_in_t                in;
    uint32_t                       hash_name_value;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

    FUNC_ENTER(NULL);

    create_prop = PDC_obj_prop_get_info(obj_create_prop);

    if (obj_name == NULL)
        PGOTO_ERROR(FAIL, "Cannot create object with empty object name");

    // Fill input structure
    memset(&in, 0, sizeof(in));
    in.data.obj_name  = obj_name;
    in.data.cont_id   = cont_id;
    in.data.time_step = create_prop->time_step;
    in.data.user_id   = create_prop->user_id;
    in.data_type      = create_prop->obj_prop_pub->type;

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

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT[%d]: obj [%s] to be created on server%u\n", pdc_client_mpi_rank_g, obj_name,
               server_id);
        fflush(stdout);
    }
    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    // We have already filled in the pdc_server_info_g[server_id].addr in previous
    // client_test_connect_lookup_cb
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_register_id_g, &rpc_handle);

    hg_ret = HG_Forward(rpc_handle, client_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == 0) {
        *meta_id = 0;
        PGOTO_DONE(FAIL);
    }

    *meta_id  = lookup_args.obj_id;
    ret_value = SUCCEED;

done:
    fflush(stdout);
    if (create_prop)
        PDC_obj_prop_free(create_prop);
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_close_all_server()
{
    uint64_t          ret_value = SUCCEED;
    hg_return_t       hg_ret    = HG_SUCCESS;
    uint32_t          server_id = 0;
    uint32_t          i;
    close_server_in_t in;
    hg_handle_t       close_server_handle;

    FUNC_ENTER(NULL);

    if (pdc_client_mpi_rank_g == 0) {
        for (i = 0; i < (uint32_t)pdc_server_num_g; i++) {
            server_id = i;

            if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
                PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server",
                            pdc_client_mpi_rank_g);

            HG_Create(send_context_g, pdc_server_info_g[server_id].addr, close_server_register_id_g,
                      &close_server_handle);

            // Fill input structure
            in.client_id = 0;
            hg_ret       = HG_Forward(close_server_handle, NULL, NULL, &in);
            if (hg_ret != HG_SUCCESS)
                PGOTO_ERROR(FAIL, "PDC_Client_close_all_server(): Could not start HG_Forward()");

            // Wait for response from server
            hg_ret = HG_Destroy(close_server_handle);
            if (hg_ret != HG_SUCCESS)
                PGOTO_ERROR(FAIL, "PDC_Client_close_all_server(): Could not destroy handle");
        }
    } // end of mpi_rank == 0

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_buf_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id, struct pdc_region_info *reginfo,
                     pdc_var_type_t data_type)
{
    perr_t                   ret_value = SUCCEED;
    hg_return_t              hg_ret    = HG_SUCCESS;
    buf_unmap_in_t           in;
    size_t                   unit;
    uint32_t                 data_server_id, meta_server_id;
    struct _pdc_buf_map_args unmap_args;
    hg_handle_t              client_send_buf_unmap_handle;

    FUNC_ENTER(NULL);

    // Fill input structure
    in.remote_obj_id = remote_obj_id;
    in.remote_reg_id = remote_reg_id;

    unit = PDC_get_var_type_size(data_type);
    PDC_region_info_t_to_transfer_unit(reginfo, &(in.remote_region), unit);

    // Compute metadata server id
    meta_server_id    = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    // Compute local data server id
    data_server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[data_server_id]++;

    if (PDC_Client_try_lookup_server(data_server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr, buf_unmap_register_id_g,
              &client_send_buf_unmap_handle);
#if PDC_TIMING == 1
    double start = MPI_Wtime(), end;
#endif
    hg_ret = HG_Forward(client_send_buf_unmap_handle, client_send_buf_unmap_rpc_cb, &unmap_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_buf_unmap(): Could not start HG_Forward()");
#if PDC_TIMING == 1
    timings.PDCbuf_obj_unmap_rpc += MPI_Wtime() - start;
#endif
    // Wait for response from server
    work_todo_g = 1;
#if PDC_TIMING == 1
    start = MPI_Wtime();
#endif
    PDC_Client_check_response(&send_context_g);
#if PDC_TIMING == 1
    end = MPI_Wtime();
    timings.PDCbuf_obj_unmap_rpc_wait += end - start;
    pdc_timestamp_register(client_buf_obj_unmap_timestamps, start, end);
#endif
    if (unmap_args.ret != 1)
        PGOTO_ERROR(FAIL, "PDC_CLIENT: buf unmap failed...");

done:
    fflush(stdout);
    HG_Destroy(client_send_buf_unmap_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_buf_map(pdcid_t local_region_id, pdcid_t remote_obj_id, size_t ndim, uint64_t *local_dims,
                   uint64_t *local_offset, pdc_var_type_t local_type, void *local_data,
                   pdc_var_type_t remote_type, struct pdc_region_info *local_region,
                   struct pdc_region_info *remote_region)
{
    perr_t                   ret_value = SUCCEED;
    hg_return_t              hg_ret    = HG_SUCCESS;
    buf_map_in_t             in;
    uint32_t                 data_server_id, meta_server_id;
    hg_class_t *             hg_class;
    hg_uint32_t              i, j;
    hg_uint32_t              local_count;
    void **                  data_ptrs = NULL;
    size_t *                 data_size = NULL;
    size_t                   unit, unit_to;
    struct _pdc_buf_map_args map_args;
    hg_handle_t              client_send_buf_map_handle;

    FUNC_ENTER(NULL);

    in.local_reg_id  = local_region_id;
    in.remote_obj_id = remote_obj_id;
    in.local_type    = local_type;
    in.remote_type   = remote_type;
    in.ndim          = ndim;

    // Compute metadata server id
    meta_server_id    = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    // Compute data server id
    data_server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;

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
        data_ptrs   = (void **)malloc(sizeof(void *));
        data_size   = (size_t *)malloc(sizeof(size_t));
        *data_ptrs  = local_data;
        *data_size  = unit * local_dims[0];
    }
    else if (ndim == 1) {
        local_count = 1;
        data_ptrs   = (void **)malloc(sizeof(void *));
        data_size   = (size_t *)malloc(sizeof(size_t));
        *data_ptrs  = local_data + unit * local_offset[0];
        *data_size  = unit * local_dims[0];
        // printf("offset size = %d, local dim = %d, unit = %d, data_ptrs[0] = %d, data_ptrs[1] = %d\n",
        // (int)local_offset[0], (int) local_dims[0], (int) unit, ((int*)data_ptrs)[0], ((int*)data_ptrs)[1]
        // );
    }
    else if (ndim == 2 && local_offset[1] == 0) {
        local_count = 1;
        data_ptrs   = (void **)malloc(sizeof(void *));
        data_size   = (size_t *)malloc(sizeof(size_t));
        *data_ptrs  = local_data;
        *data_size  = unit * local_dims[0] * local_dims[1];
    }
    else if (ndim == 2) {
        local_count  = local_dims[0];
        data_ptrs    = (void **)malloc(local_count * sizeof(void *));
        data_size    = (size_t *)malloc(local_count * sizeof(size_t));
        data_ptrs[0] = local_data + unit * (local_dims[1] * local_offset[0] + local_offset[1]);
        data_size[0] = local_dims[1];
        data_size[0] = unit * local_dims[1];
        for (i = 1; i < local_dims[0]; i++) {
            data_ptrs[i] = data_ptrs[i - 1] + unit * local_dims[1];
            data_size[i] = data_size[0];
        }
        /* data_size[0] *= unit; */
        // printf("offset size = %d, local dim = %d %d, unit = %d, data_size = %d %d\n", (int)local_offset[0],
        // (int) local_dims[0], (int) local_dims[1], (int) unit, (int)data_size[0], (int)data_size[1] );
    }
    else if (ndim == 3 && local_offset[2] == 0) {
        local_count = 1;
        data_ptrs   = (void **)malloc(sizeof(void *));
        data_size   = (size_t *)malloc(sizeof(size_t));
        *data_ptrs  = local_data;
        *data_size  = unit * local_dims[0] * local_dims[1] * local_dims[2];
    }
    else if (ndim == 3) {
        local_count  = local_dims[0] * local_dims[1];
        data_ptrs    = (void **)malloc(local_count * sizeof(void *));
        data_size    = (size_t *)malloc(local_count * sizeof(size_t));
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
        /* data_size[0] *= unit; */
        /* printf("offset size = %d, local dim = %d %d %d, unit = %d, data_size = %d %d %d\n",
         * (int)local_offset[0], (int)local_dims[0], (int)local_dims[1], (int) local_dims[2], (int) unit,
         * (int)data_size[0], (int)data_size[1] , (int)data_size[2]); */
    }
    else
        PGOTO_ERROR(FAIL, "mapping for array of dimension greater than 4 is not supproted");

    if (PDC_Client_try_lookup_server(data_server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr, buf_map_register_id_g,
              &client_send_buf_map_handle);

    // Create bulk handle and release in PDC_Data_Server_buf_unmap()
    hg_ret = HG_Bulk_create(hg_class, local_count, (void **)data_ptrs, (hg_size_t *)data_size,
                            HG_BULK_READWRITE, &(in.local_bulk_handle));
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_buf_map(): Could not create local bulk data handle");
#if PDC_TIMING == 1
    double start = MPI_Wtime(), end;
#endif
    hg_ret = HG_Forward(client_send_buf_map_handle, client_send_buf_map_rpc_cb, &map_args, &in);
#if PDC_TIMING == 1
    timings.PDCbuf_obj_map_rpc += MPI_Wtime() - start;
#endif
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_buf_map(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
#if PDC_TIMING == 1
    start = MPI_Wtime();
#endif
    PDC_Client_check_response(&send_context_g);
#if PDC_TIMING == 1
    end = MPI_Wtime();
    timings.PDCbuf_obj_map_rpc_wait += end - start;
    pdc_timestamp_register(client_buf_obj_map_timestamps, start, end);
#endif
    if (map_args.ret != 1)
        PGOTO_ERROR(FAIL, "PDC_CLIENT: buf map failed...");

done:
    fflush(stdout);
    free(data_ptrs);
    free(data_size);
    HG_Destroy(client_send_buf_map_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_region_lock(struct _pdc_obj_info *object_info, struct pdc_region_info *region_info,
                       pdc_access_t access_type, pdc_lock_mode_t lock_mode, pdc_var_type_t data_type,
                       pbool_t *status)
{
    perr_t                       ret_value = SUCCEED;
    hg_return_t                  hg_ret;
    uint32_t                     server_id, meta_server_id;
    region_lock_in_t             in;
    struct _pdc_region_lock_args lookup_args;
    hg_handle_t                  region_lock_handle;

    FUNC_ENTER(NULL);

    // Compute local data server id
    if (pdc_server_selection_g != PDC_SERVER_DEFAULT) {
        server_id      = object_info->obj_info_pub->server_id;
        meta_server_id = server_id;
    }
    else {
        // Compute metadata server id
        meta_server_id = PDC_get_server_by_obj_id(object_info->obj_info_pub->meta_id, pdc_server_num_g);
        // Compute local data server id
        server_id = PDC_CLIENT_DATA_SERVER();
    }
    // Compute local data server id
    in.meta_server_id = meta_server_id;
    in.lock_mode      = lock_mode;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // Fill input structure
    in.obj_id       = object_info->obj_info_pub->meta_id;
    in.access_type  = access_type;
    in.mapping      = region_info->mapping;
    in.local_reg_id = region_info->local_id;
    size_t ndim     = region_info->ndim;
    in.data_type    = data_type;

    if (ndim >= 4 || ndim <= 0)
        PGOTO_ERROR(FAIL, "Dimension %lu is not supported", ndim);

    in.data_unit = PDC_get_var_type_size(data_type);
    PDC_region_info_t_to_transfer_unit(region_info, &(in.region), in.data_unit);

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_lock_register_id_g,
              &region_lock_handle);
#if PDC_TIMING == 1
    double start = MPI_Wtime(), end;
#endif
    hg_ret = HG_Forward(region_lock_handle, client_region_lock_rpc_cb, &lookup_args, &in);
#if PDC_TIMING == 1
    timings.PDCreg_obtain_lock_rpc += MPI_Wtime() - start;
#endif
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
#if PDC_TIMING == 1
    start = MPI_Wtime();
#endif
    PDC_Client_check_response(&send_context_g);
#if PDC_TIMING == 1
    end = MPI_Wtime();
    timings.PDCreg_obtain_lock_rpc_wait += end - start;
    pdc_timestamp_register(client_obtain_lock_timestamps, start, end);
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
    fflush(stdout);
    HG_Destroy(region_lock_handle);

    FUNC_LEAVE(ret_value);
}
/*
static perr_t
pdc_region_release_with_server_transform(struct _pdc_obj_info *  object_info,
                                         struct pdc_region_info *region_info, pdc_access_t access_type,
                                         pdc_var_type_t data_type, size_t type_extent, int transform_state,
                                         struct _pdc_region_transform_ftn_info *this_transform,
                                         size_t client_transform_size, void *client_transform_result,
                                         pbool_t *status)
{
    perr_t                         ret_value = SUCCEED;
    void **                        data_ptrs = NULL;
    size_t                         unit = 1, *data_size = NULL;
    hg_class_t *                   hg_class = NULL;
    hg_return_t                    hg_ret;
    uint32_t                       server_id, meta_server_id;
    region_transform_and_lock_in_t in;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    region_release_handle = HG_HANDLE_NULL;

    FUNC_ENTER(NULL);

    // Compute local data server id
    if (pdc_server_selection_g != PDC_SERVER_DEFAULT) {
        server_id      = object_info->obj_info_pub->server_id;
        meta_server_id = server_id;
    }
    else {
        // Compute metadata server id
        meta_server_id = PDC_get_server_by_obj_id(object_info->obj_info_pub->meta_id, pdc_server_num_g);
        // Compute local data server id
        server_id = PDC_CLIENT_DATA_SERVER();
    }
    if (this_transform == NULL)
        in.transform_id = -1;
    else {
        in.transform_id = this_transform->meta_index;
        in.data_type    = this_transform->type;
    }
    in.dest_type           = data_type;
    in.meta_server_id      = meta_server_id;
    in.obj_id              = object_info->obj_info_pub->meta_id;
    in.access_type         = access_type;
    in.mapping             = region_info->mapping;
    in.local_reg_id        = region_info->local_id;
    in.transform_state     = transform_state;
    in.transform_data_size = client_transform_size;
    in.client_data_ptr     = (uint64_t)client_transform_result;

    data_ptrs  = (void **)malloc(sizeof(void *));
    data_size  = (size_t *)malloc(sizeof(size_t));
    *data_ptrs = client_transform_result;
    *data_size = client_transform_size;

    hg_class = HG_Context_get_class(send_context_g);

    unit = type_extent;
    PDC_region_info_t_to_transfer_unit(region_info, &(in.region), unit);

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    // Create a bulk handle for the temp buffer used by the transform
    hg_ret = HG_Bulk_create(hg_class, 1, (void **)data_ptrs, (hg_size_t *)data_size, HG_BULK_READWRITE,
                            &(in.local_bulk_handle));
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "HG_Bulk_create() failed");

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, transform_region_release_register_id_g,
              &region_release_handle);

    hg_ret = HG_Forward(region_release_handle, client_region_release_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // RPC is complete
    // We can free up the bulk handle
    // We can also free the local transform result...
    HG_Bulk_free(in.local_bulk_handle);
    if (transform_state && client_transform_result)
        free(client_transform_result);

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
    if (data_ptrs)
        free(data_ptrs);
    if (data_size)
        free(data_size);

    if (region_release_handle != HG_HANDLE_NULL)
        HG_Destroy(region_release_handle);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
pdc_region_release_with_server_analysis(struct _pdc_obj_info *  object_info,
                                        struct pdc_region_info *region_info, pdc_access_t access_type,
                                        pdc_var_type_t data_type, size_t type_extent,
                                        struct _pdc_region_analysis_ftn_info *registry, pbool_t *status)
{
    perr_t                         ret_value = SUCCEED;
    size_t                         unit;
    size_t                         output_extent;
    pdc_var_type_t                 output_datatype;
    hg_return_t                    hg_ret;
    uint32_t                       server_id, meta_server_id;
    region_analysis_and_lock_in_t  in;
    struct _pdc_client_lookup_args lookup_args;
    struct _pdc_iterator_info *    inputIter, *outputIter;
    hg_handle_t                    region_release_handle = HG_HANDLE_NULL;
    pdcid_t                        result_obj            = 0;
    struct _pdc_id_info *          obj_info;
    struct _pdc_obj_prop *         obj_prop;

    FUNC_ENTER(NULL);

    if (pdc_server_selection_g != PDC_SERVER_DEFAULT) {
        server_id      = object_info->obj_info_pub->server_id;
        meta_server_id = server_id;
    }
    else {
        // Compute metadata server id
        meta_server_id = PDC_get_server_by_obj_id(object_info->obj_info_pub->meta_id, pdc_server_num_g);
        // Compute local data server id
        server_id = PDC_CLIENT_DATA_SERVER();
    }
    in.meta_server_id = meta_server_id;
    in.obj_id         = object_info->obj_info_pub->meta_id;
    in.access_type    = access_type;
    in.local_reg_id   = region_info->local_id;
    in.mapping        = region_info->mapping;
    in.data_type      = data_type;
    in.lock_mode      = 0;
    unit              = type_extent;

    PDC_region_info_t_to_transfer_unit(region_info, &(in.region), unit);
    in.type_extent         = unit;
    in.analysis_meta_index = registry->meta_index;
    inputIter              = &PDC_Block_iterator_cache[registry->object_id[0]];
    in.input_iter          = inputIter->meta_id;
    in.n_args              = registry->n_args;
    outputIter             = &PDC_Block_iterator_cache[registry->object_id[registry->n_args - 1]];
    output_datatype        = PDC_Block_iterator_cache[registry->object_id[registry->n_args - 1]].pdc_datatype;
    output_extent          = PDC_get_var_type_size(output_datatype);
    in.output_type_extent  = output_extent;

    result_obj       = outputIter->objectId;
    obj_info         = PDC_find_id(result_obj);
    obj_prop         = (struct _pdc_obj_prop *)(obj_info->obj_ptr);
    in.output_obj_id = obj_prop->obj_prop_pub->obj_prop_id;
    in.output_iter   = outputIter->meta_id;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_analysis_release_register_id_g,
              &region_release_handle);

    hg_ret = HG_Forward(region_release_handle, client_region_release_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

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
    fflush(stdout);
    if (region_release_handle != HG_HANDLE_NULL)
        HG_Destroy(region_release_handle);

    FUNC_LEAVE(ret_value);
}
*/

/*
// This function supports transforms which are to occur
// post-READ (mapping operations) on the client.
static perr_t
pdc_region_release_with_client_transform(struct _pdc_obj_info *  object_info,
                                         struct pdc_region_info *region_info, pdc_access_t access_type,
                                         pdc_var_type_t data_type, int type_extent, int transform_state,
                                         struct _pdc_region_transform_ftn_info *this_transform,
                                         size_t *client_transform_size, void *client_transform_result,
                                         pbool_t *status)
{
    perr_t                            ret_value = SUCCEED;
    void **                           data_ptrs = NULL;
    size_t                            unit = 1, *data_size = NULL;
    hg_class_t *                      hg_class = NULL;
    hg_return_t                       hg_ret;
    uint32_t                          server_id, meta_server_id;
    region_transform_and_lock_in_t    in;
    struct _pdc_client_transform_args transform_args;
    hg_handle_t                       region_release_handle = HG_HANDLE_NULL;

    FUNC_ENTER(NULL);

    // Compute local data server id
    if (pdc_server_selection_g != PDC_SERVER_DEFAULT) {
        server_id      = object_info->obj_info_pub->server_id;
        meta_server_id = server_id;
    }
    else {
        // Compute metadata server id
        meta_server_id = PDC_get_server_by_obj_id(object_info->obj_info_pub->meta_id, pdc_server_num_g);
        // Compute local data server id
        server_id = PDC_CLIENT_DATA_SERVER();
    }
    if (this_transform == NULL)
        in.transform_id = -1;
    else {
        in.transform_id = this_transform->meta_index;
        in.data_type    = this_transform->type;
    }
    if (object_info->obj_pt->data_state == 0)
        in.dest_type = data_type;
    else
        in.dest_type = object_info->obj_pt->transform_prop.dtype;

    in.meta_server_id      = meta_server_id;
    in.obj_id              = object_info->obj_info_pub->meta_id;
    in.access_type         = access_type;
    in.mapping             = region_info->mapping;
    in.local_reg_id        = region_info->local_id;
    in.transform_state     = transform_state;
    in.transform_data_size = *client_transform_size;

    data_ptrs    = (void **)malloc(sizeof(void *));
    data_size    = (size_t *)malloc(sizeof(size_t));
    data_ptrs[0] = malloc(*client_transform_size);
    data_size[0] = *client_transform_size;

    hg_class = HG_Context_get_class(send_context_g);

    // The following values need to specify the actual region info
    // since the server uses that to locate the matching server info
    // which includes the relevant file pointer, etc...
    unit = type_extent;
    PDC_region_info_t_to_transfer_unit(region_info, &(in.region), unit);

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    transform_args.data             = data_ptrs[0];
    transform_args.transform_result = client_transform_result;
    transform_args.size             = *client_transform_size;
    transform_args.this_transform   = this_transform;
    transform_args.transform_state  = transform_state;
    transform_args.type_extent      = type_extent;
    transform_args.region_info      = region_info;

    // Create a bulk handle for the temp buffer used by the transform
    hg_ret = HG_Bulk_create(hg_class, 1, (void **)data_ptrs, (hg_size_t *)data_size, HG_BULK_READWRITE,
                            &(in.local_bulk_handle));

    transform_args.local_bulk_handle = in.local_bulk_handle;

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, transform_region_release_register_id_g,
              &region_release_handle);

    hg_ret = HG_Forward(region_release_handle, client_region_release_with_transform_cb, &transform_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    ret_value = HG_Bulk_free(in.local_bulk_handle);

    // Now the return value is stored in lookup_args.ret
    if (transform_args.ret == 1) {
        // Update size with what the transform produced...
        *client_transform_size = transform_args.size;
        *status                = TRUE;
        ret_value              = SUCCEED;
    }
    else {
        *status   = FALSE;
        ret_value = FAIL;
    }

done:
    if (data_ptrs) {
        free(data_ptrs[0]);
        free(data_ptrs);
    }
    if (data_size)
        free(data_size);

    if (region_release_handle != HG_HANDLE_NULL)
        HG_Destroy(region_release_handle);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static void
update_metadata(struct _pdc_obj_info *object_info, pdc_var_type_t data_type, size_t type_extent,
                int transform_state, size_t transform_size, struct _pdc_region_transform_ftn_info *transform)
{
    pdc_metadata_t meta;
    size_t         expected_size;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    // This is symetric with how the query_metadata works, i.e.
    // MPI rank 0 does some work and broadcasts the results.  In
    // this example, we don't need to broadcast anything, but only
    // a single rank should update the metadata.
    //
    // FIXME:  The above may or may not be correct, e.g. when
    // using PDCobj_create_mpi, each MPI rank does the same work,
    // but the total object extent is the sum of all rank instances.
    // Does the metadata reflect this correctly?

    if (pdc_client_mpi_rank_g == 0) {
        PDC_metadata_init(&meta);
        strcpy(meta.obj_name, object_info->obj_info_pub->name);
        meta.time_step = object_info->obj_pt->time_step;
        meta.obj_id    = object_info->obj_info_pub->meta_id;
        meta.cont_id   = object_info->cont->cont_info_pub->meta_id;
        meta.data_type = data_type;
        meta.ndim      = object_info->obj_pt->obj_prop_pub->ndim;

        meta.dims[0]  = object_info->obj_pt->obj_prop_pub->dims[0];
        expected_size = (size_t)meta.dims[0] * type_extent;
        if (meta.ndim > 1) {
            meta.dims[1] = object_info->obj_pt->obj_prop_pub->dims[1];
            expected_size *= (size_t)meta.dims[1];
        }
        if (meta.ndim > 2) {
            meta.dims[2] = object_info->obj_pt->obj_prop_pub->dims[2];
            expected_size *= (size_t)meta.dims[2];
        }
        if (meta.ndim > 3) {
            meta.dims[3] = object_info->obj_pt->obj_prop_pub->dims[3];
            expected_size *= (size_t)meta.dims[3];
        }

        meta.transform_state          = transform_state;
        meta.current_state.meta_index = transform->meta_index;

        if (transform_size != expected_size) {
            meta.current_state.dtype   = PDC_INT8;
            meta.current_state.ndim    = 1;
            meta.current_state.dims[0] = transform_size;
            meta.current_state.dims[1] = 0;
            meta.current_state.dims[2] = 0;
            meta.current_state.dims[3] = 0;
        }
        else {
            meta.current_state.dtype   = data_type;
            meta.current_state.ndim    = meta.ndim;
            meta.current_state.dims[0] = meta.dims[0];
            meta.current_state.dims[1] = meta.dims[1];
            meta.current_state.dims[2] = meta.dims[2];
            meta.current_state.dims[3] = meta.dims[3];
        }
        PDC_Client_update_metadata(&meta, &meta);
    }
#endif

    FUNC_LEAVE_VOID;
}
*/

/*
static size_t
get_transform_size(struct _pdc_transform_state *transform_state)
{
    size_t ret_value = 0;
    size_t i, transform_size;
    int    type_size;

    FUNC_ENTER(NULL);

    type_size      = PDC_get_var_type_size(transform_state->dtype);
    transform_size = transform_state->dims[0] * type_size;
    for (i = 1; i < transform_state->ndim; i++)
        transform_size *= transform_state->dims[i];

    ret_value = transform_size;

    FUNC_LEAVE(ret_value);
}

static hg_return_t
maybe_run_transform(struct _pdc_obj_info *object_info, struct pdc_region_info *region_info,
                    pdc_access_t access_type, pdc_var_type_t data_type, int *readyState, int *transform_index,
                    void **transform_result, size_t *transform_size)
{
    pbool_t                                 status    = TRUE;
    perr_t                                  ret_value = SUCCEED;
    size_t                                  i, client_transform_size = 0, server_transform_size = 0;
    pdc_var_type_t                          dest_type = PDC_UNKNOWN;
    struct _pdc_region_transform_ftn_info **registry  = NULL;
    int                                     k, type_size, registered_count = PDC_get_transforms(&registry);

    // FIXME: In theory, the transforms will be enabled ONLY
    // when the identified readyState value is reached.
    // For now we are relying on the ordering in which
    // functions are registered.  To fix this, after each
    // transform, we should restart the scan loop to choose
    // a "next" transform if there is one...

    FUNC_ENTER(NULL);

    if (access_type == PDC_WRITE) {
        for (k = 0; k < registered_count; k++) {
            if ((registry[k]->op_type == PDC_DATA_MAP) && (registry[k]->when == DATA_OUT) &&
                (registry[k]->readyState == *readyState)) {
                size_t (*this_transform)(void *, pdc_var_type_t, int, uint64_t *, void **, pdc_var_type_t) =
                    registry[k]->ftnPtr;
                dest_type         = registry[k]->dest_type;
                *transform_result = registry[k]->result;
                client_transform_size =
                    this_transform(registry[k]->data, registry[k]->type, region_info->ndim, region_info->size,
                                   transform_result, dest_type);
                *readyState      = registry[k]->nextState;
                *transform_index = k;
            }
        }
        //Check next for SERVER (post-data-xfer) transforms
        for (k = 0; k < registered_count; k++) {
            if ((registry[k]->dest_region == region_info) && (registry[k]->op_type == PDC_DATA_MAP) &&
                (registry[k]->when == DATA_IN) && (registry[k]->readyState == *readyState)) {
                if (client_transform_size == 0) {
                    // No previous transforms, so just use the current region data
                    *transform_result     = registry[k]->data;
                    server_transform_size = region_info->size[0];
                    for (i = 1; i < region_info->ndim; i++)
                        server_transform_size *= region_info->size[i];
                }
                else
                    server_transform_size = region_info->size[0];
                *transform_index = k;
            }
        }

        type_size = PDC_get_var_type_size(registry[*transform_index]->type);

        //Client side transform only
        if ((client_transform_size > 0) && (server_transform_size == 0)) {
            *transform_size = client_transform_size;
            ret_value       = pdc_region_release_with_server_transform(
                object_info, region_info, access_type, data_type, type_size, *readyState, NULL,
                client_transform_size, *transform_result, &status);
        }
        //Client and Server side transforms
        else if ((client_transform_size > 0) && (server_transform_size > 0)) {
            *transform_size = server_transform_size;
            ret_value       = pdc_region_release_with_server_transform(
                object_info, region_info, access_type, data_type, type_size, *readyState,
                registry[*transform_index], client_transform_size, *transform_result, &status);
        }
        //Server side transform only
        else if (server_transform_size > 0) {
            *transform_size = server_transform_size;
            ret_value       = pdc_region_release_with_server_transform(
                object_info, region_info, access_type, data_type, type_size, *readyState,
                registry[*transform_index], server_transform_size, *transform_result, &status);
        }
    }
    else if (access_type == PDC_READ) {
        for (k = 0; k < registered_count; k++) {
            // Check for SERVER (pre-data-xfer) transforms
            if ((registry[k]->op_type == PDC_DATA_MAP) && (registry[k]->when == DATA_OUT) &&
                (registry[k]->readyState == *readyState))
                puts("Server READ transform identified");
        }
        for (k = 0; k < registered_count; k++) {
            //Check for CLIENT (post-data-xfer) transforms
            if ((registry[k]->dest_region == region_info) && (registry[k]->op_type == PDC_DATA_MAP) &&
                (registry[k]->when == DATA_IN) && (registry[k]->readyState == *readyState)) {

                type_size         = PDC_get_var_type_size(registry[k]->type);
                *transform_result = registry[k]->data;
                *transform_size   = get_transform_size(&object_info->obj_pt->transform_prop);
                ret_value         = pdc_region_release_with_client_transform(
                    object_info, region_info, access_type, data_type, type_size, *readyState, registry[k],
                    transform_size, registry[k]->data, &status);
                if (ret_value == SUCCEED) {
                    *readyState      = registry[k]->nextState;
                    *transform_index = k;
                }
            }
        }
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
*/

perr_t
PDC_Client_region_release(struct _pdc_obj_info *object_info, struct pdc_region_info *region_info,
                          pdc_access_t access_type, pdc_var_type_t data_type, pbool_t *status)
{
    perr_t ret_value = SUCCEED;
    // int readyState = 0, currentState;
    hg_return_t      hg_ret;
    uint32_t         server_id, meta_server_id;
    region_lock_in_t in;
    // size_t                         type_extent;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    region_release_handle = HG_HANDLE_NULL;
    // void *transform_result = NULL;
    // size_t transform_size = 0;
    // struct _pdc_region_transform_ftn_info **registry = NULL;
    // int transform_index;
    // int k, registered_count;
    // struct _pdc_region_analysis_ftn_info **analysis_registry;

    FUNC_ENTER(NULL);

    // type_extent = object_info->obj_pt->type_extent;
    /*
        if (region_info->registered_op & PDC_TRANSFORM) {
            transform_index = -1;
            PDC_get_transforms(&registry);
            // Get the current data_state of this object::
            readyState = currentState = object_info->obj_pt->data_state;
            ret_value = maybe_run_transform(object_info, region_info, access_type, data_type,
                            &currentState, &transform_index, &transform_result, &transform_size);
            if ((ret_value == SUCCEED) && (readyState != currentState)) {
                update_metadata(object_info, data_type, type_extent, currentState,
                                transform_size, registry[transform_index]);
                PGOTO_DONE(ret_value);
            }
        }
        // FIXME:  What if the user has coupled a transform with an analysis function?
        // How do we provide the transformed data into the analysis?
        if (region_info->registered_op & PDC_ANALYSIS) {
            registered_count = PDC_get_analysis_registry(&analysis_registry);
            for (k=0; k < registered_count; k++) {
                if (analysis_registry[k]->region_id[0] == region_info->local_id) {
                    ret_value = pdc_region_release_with_server_analysis(object_info, region_info,
                        access_type, data_type, type_extent, analysis_registry[k], status);
                    PGOTO_DONE(ret_value);
                }
            }
        }
    */
    // Compute data server and metadata server ids.
    if (pdc_server_selection_g != PDC_SERVER_DEFAULT) {
        server_id      = object_info->obj_info_pub->server_id;
        meta_server_id = server_id;
    }
    else {
        // Compute metadata server id
        meta_server_id = PDC_get_server_by_obj_id(object_info->obj_info_pub->meta_id, pdc_server_num_g);
        // Compute local data server id
        server_id = PDC_CLIENT_DATA_SERVER();
    }
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
    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_release_register_id_g,
              &region_release_handle);
#if PDC_TIMING == 1
    double start = MPI_Wtime(), end;
#endif
    hg_ret = HG_Forward(region_release_handle, client_region_release_rpc_cb, &lookup_args, &in);
#if PDC_TIMING == 1
    timings.PDCreg_release_lock_rpc += MPI_Wtime() - start;
#endif
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_send_name_to_server(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
#if PDC_TIMING == 1
    start = MPI_Wtime();
#endif
    PDC_Client_check_response(&send_context_g);
#if PDC_TIMING == 1
    end = MPI_Wtime();
    timings.PDCreg_release_lock_rpc_wait += end - start;
    pdc_timestamp_register(client_release_lock_timestamps, start, end);
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
    fflush(stdout);
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
    hg_return_t                     ret_value = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_read_check_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret        = -1;
        client_lookup_args->ret_string = " ";
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: error with HG_Get_output", pdc_client_mpi_rank_g);
    }

    client_lookup_args->ret = output.ret;
    if (output.shm_addr != NULL) {
        client_lookup_args->ret_string = (char *)malloc(strlen(output.shm_addr) + 1);
        strcpy(client_lookup_args->ret_string, output.shm_addr);
    }
    else
        client_lookup_args->ret_string = NULL;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// This function is used with server push notification
hg_return_t
PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info)
{
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

    FUNC_ENTER(NULL);

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
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: request region not found!", pdc_client_mpi_rank_g);

    // Calculate data_size, TODO: this should be done in other places?
    data_size = 1;
    for (i = 0; i < target_region->ndim; i++) {
        data_size *= target_region->size[i];
    }

    /* open the shared memory segment as if it was a file */
    shm_fd = shm_open(shm_addr, O_RDONLY, 0666);
    if (shm_fd == -1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: - Shared memory open failed [%s]!", pdc_client_mpi_rank_g,
                    shm_addr);

    /* map the shared memory segment to the address space of the process */
    shm_base = mmap(0, data_size, PROT_READ, MAP_SHARED, shm_fd, 0);
    if (shm_base == MAP_FAILED) {
        // printf("==PDC_CLIENT: Map failed: %s\n", strerror(errno));
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
    //   PGOTO_ERROR(FAIL, "==PDC_CLIENT: Unmap failed: %s", strerror(errno));

close:
    /* close the shared memory segment as if it was a file */
    if (close(shm_fd) == -1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: Close failed!");

    /* remove the shared memory segment from the file system */
    if (shm_unlink(shm_addr) == -1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: Error removing %s", shm_addr);

done:
    fflush(stdout);
    work_todo_g--;

    FUNC_LEAVE(ret_value);
}

// This is used with polling approach to get data from server read
perr_t
PDC_Client_data_server_read_check(int server_id, uint32_t client_id, pdc_metadata_t *meta,
                                  struct pdc_region_info *region, int *status, void *buf)
{
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

    FUNC_ENTER(NULL);

    if (meta == NULL || region == NULL || status == NULL || buf == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: NULL input", pdc_client_mpi_rank_g);

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid server id %d/%d", pdc_client_mpi_rank_g, server_id,
                    pdc_server_num_g);

    // Dummy value fill
    in.client_id = client_id;
    PDC_metadata_t_to_transfer_t(meta, &in.meta);
    PDC_region_info_t_to_transfer(region, &in.region);

    for (i = 0; i < region->ndim; i++) {
        read_size *= region->size[i];
    }

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_check_register_id_g,
              &data_server_read_check_handle);

    hg_ret = HG_Forward(data_server_read_check_handle, data_server_read_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "== Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    *status = lookup_args.ret;
    if (lookup_args.ret != 1 && lookup_args.ret != 111) {
        ret_value = SUCCEED;
        if (is_client_debug_g)
            printf("==PDC_CLIENT[%d]: %s- IO request has not been fulfilled by server\n",
                   pdc_client_mpi_rank_g, __func__);
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
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Shared memory open failed [%s]!", pdc_client_mpi_rank_g,
                        shm_addr);

        /* map the shared memory segment to the address space of the process */
        shm_base = mmap(0, read_size, PROT_READ, MAP_SHARED, shm_fd, 0);
        if (shm_base == MAP_FAILED) {
            // PGOTO_ERROR(FAIL, "==PDC_CLIENT: Map failed: %s", strerror(errno));
        }
        // close and unlink?

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
        if (munmap(shm_base, read_size) == -1) {
            // PGOTO_ERROR(FAIL, "==PDC_CLIENT: Unmap failed: %s", strerror(errno));
        }
        HG_Destroy(data_server_read_check_handle);
    } // end of check io

    // close:
    /* close the shared memory segment as if it was a file */
    if (close(shm_fd) == -1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: Close failed!");

    // TODO: need to make sure server has cached it to BB
    free(lookup_args.ret_string);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_read_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args;
    data_server_read_out_t          output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: data_server_read_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_data_server_read(struct pdc_request *request)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    struct _pdc_client_lookup_args lookup_args;
    data_server_read_in_t          in;
    hg_handle_t                    data_server_read_handle;
    int                            server_id, n_client, n_update;
    pdc_metadata_t *               meta;
    struct pdc_region_info *       region;

    FUNC_ENTER(NULL);

    server_id = request->server_id;
    n_client  = request->n_client;
    n_update  = request->n_update;
    meta      = request->metadata;
    region    = request->region;

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "PDC_CLIENT[%d]: invalid server id %d/%d", pdc_client_mpi_rank_g, server_id,
                    pdc_server_num_g);

    if (meta == NULL || region == NULL)
        PGOTO_ERROR(FAIL, "PDC_CLIENT[%d]: invalid metadata or region", pdc_client_mpi_rank_g);

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

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_register_id_g,
              &data_server_read_handle);

    hg_ret = HG_Forward(data_server_read_handle, data_server_read_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        PGOTO_ERROR(FAIL, "PDC_CLIENT: ERROR from server");

done:
    fflush(stdout);
    HG_Destroy(data_server_read_handle);

    FUNC_LEAVE(ret_value);
}

/*
 * Close the shared memory
 */
perr_t
PDC_Client_close_shm(struct pdc_request *req)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (req == NULL || req->shm_fd == 0 || req->shm_base == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid input", pdc_client_mpi_rank_g);

    /* remove the mapped memory segment from the address space of the process */
    if (munmap(req->shm_base, req->shm_size) == -1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Unmap failed", pdc_client_mpi_rank_g);

    /* close the shared memory segment as if it was a file */
    if (close(req->shm_fd) == -1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: close shm failed", pdc_client_mpi_rank_g);

    /* remove the shared memory segment from the file system */
    // TODO: fix error
    /* if (shm_unlink(req->shm_addr) == -1) */
    /*     PGOTO_ERROR(FAIL, "==PDC_CLIENT: Error removing %s", req->shm_addr); */

done:

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_write_check_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_write_check_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: data_server_write_check_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_data_server_write_check(struct pdc_request *request, int *status)
{
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

    FUNC_ENTER(NULL);

    server_id = request->server_id;
    meta      = request->metadata;
    region    = request->region;

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "PDC_CLIENT[%d]: invalid server id %d/%d", pdc_client_mpi_rank_g, server_id,
                    pdc_server_num_g);

    in.client_id = pdc_client_mpi_rank_g;
    PDC_metadata_t_to_transfer_t(meta, &in.meta);
    PDC_region_info_t_to_transfer(region, &in.region);

    for (i = 0; i < region->ndim; i++) {
        write_size *= region->size[i];
    }

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_write_check_register_id_g,
              &data_server_write_check_handle);

    hg_ret = HG_Forward(data_server_write_check_handle, data_server_write_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    *status = lookup_args.ret;
    if (lookup_args.ret != 1) {
        ret_value = SUCCEED;
        if (is_client_debug_g == 1)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: IO request not done by server yet", pdc_client_mpi_rank_g);

        if (lookup_args.ret == -1)
            PGOTO_DONE(FAIL);
    }
    else {
        // Close shm
        PDC_Client_close_shm(request);
    }

done:
    fflush(stdout);
    HG_Destroy(data_server_write_check_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_write_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    struct _pdc_client_lookup_args *client_lookup_args;
    hg_handle_t                     handle;
    data_server_write_out_t         output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_client_lookup_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: data_server_write_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }

    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_data_server_write(struct pdc_request *request)
{
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
    int                            rnd;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
#endif

    FUNC_ENTER(NULL);

    server_id = request->server_id;
    n_client  = request->n_client;
    n_update  = request->n_update;
    meta      = request->metadata;
    region    = request->region;
    buf       = request->buf;

    if (NULL == meta || NULL == region || NULL == buf)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: input NULL", pdc_client_mpi_rank_g);

    if (server_id < 0 || server_id >= pdc_server_num_g)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid server id %d/%d", pdc_client_mpi_rank_g, server_id,
                    pdc_server_num_g);

    if (region->ndim > 4)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid dim %lu", pdc_client_mpi_rank_g, region->ndim);

    // Calculate region size
    for (i = 0; i < region->ndim; i++) {
        region_size *= region->size[i];
        if (region_size == 0)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: size[%d]=0", pdc_client_mpi_rank_g, i);
    }

    // Create shared memory
    // Shared memory address is /objID_ServerID_ClientID_rand
    rnd = rand();
    sprintf(request->shm_addr, "/%" PRIu64 "_c%d_s%d_%d", meta->obj_id, pdc_client_mpi_rank_g, server_id,
            rnd);

    /* create the shared memory segment as if it was a file */
    request->shm_fd = shm_open(request->shm_addr, O_CREAT | O_RDWR, 0666);
    if (request->shm_fd == -1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: Shared memory creation with shm_open failed");

    /* configure the size of the shared memory segment */
    if (ftruncate(request->shm_fd, region_size) != 0) {
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: Memory truncate failed");
    }
    request->shm_size = region_size;

    /* map the shared memory segment to the address space of the process */
    request->shm_base = mmap(0, region_size, PROT_READ | PROT_WRITE, MAP_SHARED, request->shm_fd, 0);
    if (request->shm_base == MAP_FAILED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: Shared memory mmap failed, region size = %" PRIu64 "", region_size);
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

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_write_register_id_g,
                       &data_server_write_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==Could not HG_Create()");

    hg_ret = HG_Forward(data_server_write_handle, data_server_write_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        HG_Destroy(data_server_write_handle);
        PGOTO_ERROR(FAIL, "==Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    HG_Destroy(data_server_write_handle);

    server_ret = lookup_args.ret;

    if (server_ret != 1)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR from server", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_test(struct pdc_request *request, int *completed)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (request == NULL || completed == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: request and/or completed is NULL!");

    if (request->access_type == PDC_READ) {
        ret_value =
            PDC_Client_data_server_read_check(request->server_id, pdc_client_mpi_rank_g, request->metadata,
                                              request->region, completed, request->buf);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC Client read check ERROR!");
    }
    else if (request->access_type == PDC_WRITE) {
        ret_value = PDC_Client_data_server_write_check(request, completed);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC Client write check ERROR!");
    }
    else
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: error with request access type!");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_wait(struct pdc_request *request, unsigned long max_wait_ms, unsigned long check_interval_ms)
{
    perr_t         ret_value = SUCCEED;
    int            completed = 0;
    struct timeval start_time;
    struct timeval end_time;
    unsigned long  elapsed_ms;
    int            cnt = 0;

    FUNC_ENTER(NULL);

    gettimeofday(&start_time, 0);
    // TODO: Calculate region size and estimate the wait time
    // Write is 4-5x faster

    while (completed == 0 && cnt < PDC_MAX_TRIAL_NUM) {
        ret_value = PDC_Client_test(request, &completed);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: PDC_Client_test ERROR!", pdc_client_mpi_rank_g);

        gettimeofday(&end_time, 0);
        elapsed_ms =
            ((end_time.tv_sec - start_time.tv_sec) * 1000000LL + end_time.tv_usec - start_time.tv_usec) /
            1000;
        if (elapsed_ms > max_wait_ms) {
            printf("==PDC_CLIENT[%d]: exceeded max IO request waiting time...\n", pdc_client_mpi_rank_g);
            break;
        }
        PDC_msleep(check_interval_ms);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_wait(struct pdc_request *request, unsigned long max_wait_ms, unsigned long check_interval_ms)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_wait(request, max_wait_ms, check_interval_ms);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_iwrite(pdc_metadata_t *meta, struct pdc_region_info *region, struct pdc_request *request,
                  void *buf)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

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
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_iwrite((pdc_metadata_t *)meta, region, request, buf);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Client_work_done_cb(const struct hg_cb_info *callback_info ATTRIBUTE(unused))
{
    // server_lookup_client_out_t *validate = callback_info->arg;
    work_todo_g--;

    return HG_SUCCESS;
}

// PDC_Client_write is done using PDC_Client_iwrite and PDC_Client_wait
perr_t
PDC_Client_write(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    struct pdc_request request;
    perr_t             ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    request.n_update = 1;
    request.n_client = 1;
    ret_value        = PDC_Client_iwrite(meta, region, &request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC_Client_write - PDC_Client_iwrite error");

    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC_Client_write - PDC_Client_wait error");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// PDC_Client_write is done using PDC_Client_iwrite and PDC_Client_wait
perr_t
PDC_Client_write_id(pdcid_t local_obj_id, struct pdc_region_info *region, void *buf)
{
    struct pdc_request    request;
    struct _pdc_id_info * info;
    struct _pdc_obj_info *object;
    pdc_metadata_t *      meta;
    perr_t                ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    info = PDC_find_id(local_obj_id);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: obj_id %" PRIu64 " invalid!", pdc_client_mpi_rank_g,
                    local_obj_id);

    object = (struct _pdc_obj_info *)(info->obj_ptr);
    meta   = object->metadata;
    if (meta == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: metadata is NULL!", pdc_client_mpi_rank_g);

    request.n_update = 1;
    request.n_client = 1;
    ret_value        = PDC_Client_iwrite(meta, region, &request, buf);

    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC_Client_write - PDC_Client_iwrite error");

    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC_Client_write - PDC_Client_wait error");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_iread(pdc_metadata_t *meta, struct pdc_region_info *region, struct pdc_request *request, void *buf)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

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
        PGOTO_ERROR(FAIL, "==PDC_CLIENT: PDC_Client_iread- PDC_Client_data_server_read error");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_read(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    perr_t             ret_value = SUCCEED;
    struct pdc_request request;

    FUNC_ENTER(NULL);

    request.n_update = 1;
    ret_value        = PDC_Client_iread(meta, region, &request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: PDC_Client_iread error", pdc_client_mpi_rank_g);

    PDC_msleep(500);

    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: PDC_Client_wait error", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_write_wait_notify(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    perr_t              ret_value = SUCCEED;
    struct pdc_request *request   = (struct pdc_request *)malloc(sizeof(struct pdc_request));

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_iwrite(meta, region, request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Failed to send write request to server", pdc_client_mpi_rank_g);

    DL_PREPEND(pdc_io_request_list_g, request);
    printf("==PDC_CLIENT[%d]: Finished sending write request to server, waiting for notification\n",
           pdc_client_mpi_rank_g);
    fflush(stdout);

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    printf("==PDC_CLIENT[%d]: received write finish notification\n", pdc_client_mpi_rank_g);
    fflush(stdout);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_read_wait_notify(pdc_metadata_t *meta, struct pdc_region_info *region, void *buf)
{
    perr_t              ret_value = SUCCEED;
    struct pdc_request *request   = (struct pdc_request *)malloc(sizeof(struct pdc_request));

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_iread(meta, region, request, buf);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Faile to send read request to server", pdc_client_mpi_rank_g);

    DL_PREPEND(pdc_io_request_list_g, request);

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static hg_return_t
PDC_Client_add_del_objects_to_container_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                             ret_value = HG_SUCCESS;
    hg_handle_t                             handle    = callback_info->info.forward.handle;
    pdc_int_ret_t                           bulk_rpc_ret;
    update_region_storage_meta_bulk_args_t *cb_args;

    FUNC_ENTER(NULL);

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
    fflush(stdout);
    work_todo_g--;

    FUNC_LEAVE(ret_value);
}

// Add/delete a number of objects to one container
perr_t
PDC_Client_add_del_objects_to_container(int nobj, uint64_t *obj_ids, uint64_t cont_meta_id, int op)
{
    perr_t                     ret_value = SUCCEED;
    hg_return_t                hg_ret    = HG_SUCCESS;
    hg_handle_t                rpc_handle;
    hg_bulk_t                  bulk_handle;
    uint32_t                   server_id;
    hg_size_t                  buf_sizes[3];
    cont_add_del_objs_rpc_in_t bulk_rpc_in;
    // Reuse the existing args structure
    update_region_storage_meta_bulk_args_t cb_args;

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_obj_id(cont_meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

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

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Add a number of objects to one container
perr_t
PDC_Client_add_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id)
{
    perr_t               ret_value = SUCCEED;
    int                  i;
    uint64_t *           obj_ids;
    uint64_t             cont_meta_id;
    struct _pdc_id_info *id_info = NULL;

    FUNC_ENTER(NULL);

    obj_ids = (uint64_t *)malloc(sizeof(uint64_t) * nobj);
    for (i = 0; i < nobj; i++) {
        id_info    = PDC_find_id(local_obj_ids[i]);
        obj_ids[i] = ((struct _pdc_obj_info *)(id_info->obj_ptr))->obj_info_pub->meta_id;
    }

    id_info      = PDC_find_id(local_cont_id);
    cont_meta_id = ((struct _pdc_cont_info *)(id_info->obj_ptr))->cont_info_pub->meta_id;

    ret_value = PDC_Client_add_del_objects_to_container(nobj, obj_ids, cont_meta_id, ADD_OBJ);

    FUNC_LEAVE(ret_value);
}

// Delete a number of objects to one container
perr_t
PDC_Client_del_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id)
{
    perr_t               ret_value = SUCCEED;
    int                  i;
    uint64_t *           obj_ids;
    uint64_t             cont_meta_id;
    struct _pdc_id_info *id_info = NULL;

    FUNC_ENTER(NULL);

    obj_ids = (uint64_t *)malloc(sizeof(uint64_t) * nobj);
    for (i = 0; i < nobj; i++) {
        id_info    = PDC_find_id(local_obj_ids[i]);
        obj_ids[i] = ((struct _pdc_obj_info *)(id_info->obj_ptr))->obj_info_pub->meta_id;
    }

    id_info      = PDC_find_id(local_cont_id);
    cont_meta_id = ((struct _pdc_cont_info *)(id_info->obj_ptr))->cont_info_pub->meta_id;

    ret_value = PDC_Client_add_del_objects_to_container(nobj, obj_ids, cont_meta_id, DEL_OBJ);

    FUNC_LEAVE(ret_value);
}

// Add/delete a number of objects to one container
perr_t
PDC_Client_add_tags_to_container(pdcid_t cont_id, char *tags)
{
    perr_t                 ret_value = SUCCEED;
    hg_return_t            hg_ret    = HG_SUCCESS;
    hg_handle_t            rpc_handle;
    uint32_t               server_id;
    struct _pdc_id_info *  info;
    struct _pdc_cont_info *object;
    uint64_t               cont_meta_id;
    cont_add_tags_rpc_in_t add_tag_rpc_in;

    FUNC_ENTER(NULL);

    info = PDC_find_id(cont_id);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: cont_id %" PRIu64 " invalid!", pdc_client_mpi_rank_g, cont_id);

    object       = (struct _pdc_cont_info *)(info->obj_ptr);
    cont_meta_id = object->cont_info_pub->meta_id;

    server_id = PDC_get_server_by_obj_id(cont_meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    // Send the bulk handle to the target with RPC
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, cont_add_tags_rpc_register_id_g,
                       &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    add_tag_rpc_in.cont_id = cont_meta_id;
    add_tag_rpc_in.tags    = tags;

    /* Forward call to remote addr */
    hg_ret = HG_Forward(rpc_handle, pdc_client_check_int_ret_cb, NULL, &add_tag_rpc_in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not forward call");

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    ret_value = SUCCEED;

done:
    fflush(stdout);
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
container_query_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                       ret_value;
    struct _pdc_container_query_args *client_lookup_args;
    hg_handle_t                       handle;
    container_query_out_t             output;

    FUNC_ENTER(NULL);

    client_lookup_args = (struct _pdc_container_query_args *)callback_info->arg;
    handle             = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: error with HG_Get_output", pdc_client_mpi_rank_g);

    client_lookup_args->cont_id = output.cont_id;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

// Query container with name, retrieve container ID from server
perr_t
PDC_Client_query_container_name(const char *cont_name, uint64_t *cont_meta_id)
{
    perr_t                           ret_value = SUCCEED;
    hg_return_t                      hg_ret    = 0;
    uint32_t                         hash_name_value, server_id;
    container_query_in_t             in;
    struct _pdc_container_query_args lookup_args;
    hg_handle_t                      container_query_handle;

    FUNC_ENTER(NULL);

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(cont_name);
    server_id       = hash_name_value % pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, container_query_register_id_g,
              &container_query_handle);

    // Fill input structure
    in.cont_name  = cont_name;
    in.hash_value = hash_name_value;

    hg_ret = HG_Forward(container_query_handle, container_query_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL,
                    "==PDC_CLIENT[%d] - PDC_Client_query_container_with_name(): Could not start HG_Forward()",
                    pdc_client_mpi_rank_g);

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    *cont_meta_id = lookup_args.cont_id;

done:
    fflush(stdout);
    HG_Destroy(container_query_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_container_name_col(const char *cont_name, uint64_t *cont_meta_id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    if (pdc_client_mpi_rank_g == 0) {
        ret_value = PDC_Client_query_container_name(cont_name, cont_meta_id);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with PDC_Client_query_container_name",
                        pdc_client_mpi_rank_g);
    }

    MPI_Bcast(cont_meta_id, 1, MPI_LONG_LONG, 0, PDC_CLIENT_COMM_WORLD_g);
#else
    printf("==PDC_CLIENT[%d]: Calling MPI collective operation without enabling MPI!\n",
           pdc_client_mpi_rank_g);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static hg_return_t
PDC_Client_query_read_obj_name_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                             ret_value = HG_SUCCESS;
    hg_handle_t                             handle    = callback_info->info.forward.handle;
    pdc_int_ret_t                           bulk_rpc_ret;
    update_region_storage_meta_bulk_args_t *cb_args;

    FUNC_ENTER(NULL);

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
    fflush(stdout);
    work_todo_g--;

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_add_request_to_list(struct pdc_request **list_head, struct pdc_request *request)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (list_head == NULL || request == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid input", pdc_client_mpi_rank_g);

    request->seq_id = pdc_io_request_seq_id++;
    DL_PREPEND(*list_head, request);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_del_request_from_list(struct pdc_request **list_head, struct pdc_request *request)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (list_head == NULL || request == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid input", pdc_client_mpi_rank_g);

    request->seq_id = pdc_io_request_seq_id++;
    DL_DELETE(*list_head, request);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct pdc_request *
PDC_find_request_from_list_by_seq_id(struct pdc_request **list_head, int seq_id)
{
    struct pdc_request *ret_value = NULL;
    struct pdc_request *elt;

    FUNC_ENTER(NULL);

    if (list_head == NULL || seq_id < PDC_SEQ_ID_INIT_VALUE || seq_id > 99999)
        PGOTO_ERROR(NULL, "==PDC_CLIENT[%d]: invalid input", pdc_client_mpi_rank_g);

    DL_FOREACH(*list_head, elt)
    if (elt->seq_id == seq_id)
        ret_value = elt;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Query and read objects with obj name, read data is stored in user provided buf
perr_t
PDC_Client_query_name_read_entire_obj(int nobj, char **obj_names, void ***out_buf, uint64_t *out_buf_sizes)
{

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

    FUNC_ENTER(NULL);

    if (nobj == 0 || obj_names == NULL || out_buf == NULL || out_buf_sizes == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid input", pdc_client_mpi_rank_g);

    server_id = PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    // Send the bulk handle to the target with RPC
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_read_obj_name_register_id_g,
                       &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create handle");

    total_size = 0;
    buf_sizes  = (uint64_t *)calloc(sizeof(uint64_t), nobj);
    for (i = 0; i < nobj; i++) {
        buf_sizes[i] = strlen(obj_names[i]) + 1;
        total_size += buf_sizes[i];
    }

    /* Register memory */
    hg_ret =
        HG_Bulk_create(send_class_g, nobj, (void **)obj_names, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "Could not create bulk data handle");

    request               = (struct pdc_request *)calloc(1, sizeof(struct pdc_request));
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
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Wait for server to complete all reads
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Copies the data from server's shm to user buffer
// Assumes the shm_addrs are avialable
perr_t
PDC_Client_complete_read_request(int nbuf, struct pdc_request *req)
{
    perr_t ret_value = SUCCEED;
    int    i;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start;
    struct timeval pdc_timer_end;
#endif

    FUNC_ENTER(NULL);

    *req->buf_arr     = (void **)calloc(nbuf, sizeof(void *));
    req->shm_fd_arr   = (int *)calloc(nbuf, sizeof(int));
    req->shm_base_arr = (char **)calloc(nbuf, sizeof(char *));

    for (i = 0; i < nbuf; i++) {
        /* open the shared memory segment as if it was a file */
        req->shm_fd_arr[i] = shm_open(req->shm_addr_arr[i], O_RDONLY, 0666);
        if (req->shm_fd_arr[i] == -1) {
            printf("==PDC_CLIENT[%d]: %s - Shared memory open failed [%s]!\n", pdc_client_mpi_rank_g,
                   __func__, req->shm_addr_arr[i]);
            continue;
        }

        /* map the shared memory segment to the address space of the process */
        req->shm_base_arr[i] = mmap(0, (req->shm_size_arr)[i], PROT_READ, MAP_SHARED, req->shm_fd_arr[i], 0);
        if (req->shm_base_arr[i] == MAP_FAILED) {
            // printf("==PDC_CLIENT: Map failed: %s\n", strerror(errno));
            continue;
        }

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_start, 0);
#endif

        // Copy data
        (*req->buf_arr)[i] = (void *)malloc((req->shm_size_arr)[i]);
        memcpy((*req->buf_arr)[i], req->shm_base_arr[i], (req->shm_size_arr)[i]);

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

        /* remove the mapped shared memory segment from the address space of the process */
        if (munmap(req->shm_base_arr[i], (req->shm_size_arr)[i]) == -1) {
        }
        // PGOTO_ERROR(FAIL, "==PDC_CLIENT: Unmap failed: %s", strerror(errno));

        /* close the shared memory segment as if it was a file */
        if (close(req->shm_fd_arr[i]) == -1) {
            printf("==PDC_CLIENT: Close failed!\n");
            continue;
        }

        /* remove the shared memory segment from the file system */
        if (shm_unlink(req->shm_addr_arr[i]) == -1) {
            printf("==PDC_CLIENT: Error removing %s\n", req->shm_addr_arr[i]);
            continue;
        }
    }

    // done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_read_complete(char *shm_addrs, int size, int n_shm, int seq_id)
{
    perr_t              ret_value = SUCCEED;
    int                 i, cnt;
    struct pdc_request *request;

    FUNC_ENTER(NULL);

    request = PDC_find_request_from_list_by_seq_id(&pdc_io_request_list_g, seq_id);
    if (request == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: cannot find previous request", pdc_client_mpi_rank_g);

    request->shm_addr_arr = (char **)calloc(n_shm, sizeof(char *));
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
    fflush(stdout);
    work_todo_g--;

    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
perr_t
PDC_Client_server_checkpoint(uint32_t server_id)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret;
    pdc_int_send_t                 in;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

    FUNC_ENTER(NULL);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, server_checkpoint_rpc_register_id_g,
                       &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Could not create handle", pdc_client_mpi_rank_g);

    in.origin = pdc_client_mpi_rank_g;
    hg_ret    = HG_Forward(rpc_handle, pdc_client_check_int_ret_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Could not start forward to server", pdc_client_mpi_rank_g);

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    fflush(stdout);
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_all_server_checkpoint()
{
    perr_t ret_value = SUCCEED;
    int    i;

    FUNC_ENTER(NULL);

    if (pdc_server_num_g == 0)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: server number not initialized!", pdc_client_mpi_rank_g);

    // only let client rank 0 send all requests
    if (pdc_client_mpi_rank_g != 0)
        PGOTO_DONE(ret_value);

    for (i = 0; i < pdc_server_num_g; i++)
        ret_value = PDC_Client_server_checkpoint(i);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_attach_metadata_to_local_obj(const char *obj_name, uint64_t obj_id, uint64_t cont_id,
                                        struct _pdc_obj_info *obj_info)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    obj_info->metadata                              = (pdc_metadata_t *)calloc(1, sizeof(pdc_metadata_t));
    ((pdc_metadata_t *)obj_info->metadata)->user_id = obj_info->obj_pt->user_id;
    if (NULL != obj_info->obj_pt->app_name)
        strcpy(((pdc_metadata_t *)obj_info->metadata)->app_name, obj_info->obj_pt->app_name);
    if (NULL != obj_name)
        strcpy(((pdc_metadata_t *)obj_info->metadata)->obj_name, obj_name);
    ((pdc_metadata_t *)obj_info->metadata)->time_step = obj_info->obj_pt->time_step;
    ((pdc_metadata_t *)obj_info->metadata)->obj_id    = obj_id;
    ((pdc_metadata_t *)obj_info->metadata)->cont_id   = cont_id;
    if (NULL != obj_info->obj_pt->tags)
        strcpy(((pdc_metadata_t *)obj_info->metadata)->tags, obj_info->obj_pt->tags);
    if (NULL != obj_info->obj_pt->data_loc)
        strcpy(((pdc_metadata_t *)obj_info->metadata)->data_location, obj_info->obj_pt->data_loc);
    ((pdc_metadata_t *)obj_info->metadata)->ndim = obj_info->obj_pt->obj_prop_pub->ndim;
    if (NULL != obj_info->obj_pt->obj_prop_pub->dims)
        memcpy(((pdc_metadata_t *)obj_info->metadata)->dims, obj_info->obj_pt->obj_prop_pub->dims,
               sizeof(uint64_t) * obj_info->obj_pt->obj_prop_pub->ndim);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_send_client_shm_info(uint32_t server_id, char *shm_addr, uint64_t size)
{
    perr_t                         ret_value;
    hg_return_t                    hg_ret;
    send_shm_in_t                  in;
    struct _pdc_client_lookup_args lookup_args;
    hg_handle_t                    rpc_handle;

    FUNC_ENTER(NULL);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    hg_ret =
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, send_shm_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Could not create handle", pdc_client_mpi_rank_g);

    in.client_id = pdc_client_mpi_rank_g;
    in.shm_addr  = shm_addr;
    in.size      = size;

    hg_ret = HG_Forward(rpc_handle, pdc_client_check_int_ret_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Could not forward to server", pdc_client_mpi_rank_g);

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    fflush(stdout);
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

static region_list_t *
PDC_get_storage_meta_from_io_list(pdc_data_server_io_list_t **list, region_storage_meta_t *storage_meta)
{
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
    pdc_data_server_io_list_t *io_list_elt, *io_list_target = NULL;
    region_list_t *            new_region;
    int                        j;
    perr_t                     ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    DL_FOREACH(*list, io_list_elt)
    if (storage_meta->obj_id == io_list_elt->obj_id) {
        io_list_target = io_list_elt;
        break;
    }

    // If not found, create and insert one to the read list
    if (NULL == io_list_target) {
        io_list_target         = (pdc_data_server_io_list_t *)calloc(1, sizeof(pdc_data_server_io_list_t));
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

    new_region = (region_list_t *)calloc(1, sizeof(region_list_t));
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
    perr_t                                 ret_value;
    hg_return_t                            hg_ret;
    bulk_rpc_in_t                          bulk_rpc_in;
    hg_handle_t                            rpc_handle;
    size_t                                 buf_sizes;
    update_region_storage_meta_bulk_args_t cb_args;
    hg_bulk_t                              bulk_handle;

    FUNC_ENTER(NULL);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

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
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_cp_data_to_local_server(int nobj, region_storage_meta_t **all_storage_meta, void ***buf_arr,
                                   size_t *size_arr)

{
    perr_t                 ret_value = SUCCEED;
    uint32_t               ndim, server_id;
    uint64_t               total_size = 0, cp_loc = 0;
    void *                 buf = NULL;
    char                   shm_addr[ADDR_MAX];
    int                    i, *total_obj = NULL, ntotal_obj = nobj, *recvcounts = NULL, *displs = NULL;
    region_storage_meta_t *all_region_storage_meta_1d = NULL, *my_region_storage_meta_1d = NULL;

    FUNC_ENTER(NULL);

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
            printf("==PDC_CLIENT[%d]: NULL storage meta for %dth object!\n", pdc_client_mpi_rank_g, i);
            fflush(stdout);
            continue;
        }
        ndim = all_storage_meta[i]->region_transfer.ndim;
        if (ndim != 1) {
            printf("==PDC_CLIENT[%d]: only support for 1D data now (%u)!\n", pdc_client_mpi_rank_g, ndim);
            fflush(stdout);
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
    displs     = (int *)malloc(sizeof(int) * pdc_client_same_node_size_g);
    recvcounts = (int *)malloc(sizeof(int) * pdc_client_same_node_size_g);
    total_obj  = (int *)malloc(sizeof(int) * pdc_client_same_node_size_g);

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
            (region_storage_meta_t *)malloc(ntotal_obj * sizeof(region_storage_meta_t));
    }

    // Copy data to 1 buffer
    my_region_storage_meta_1d = (region_storage_meta_t *)malloc(nobj * sizeof(region_storage_meta_t));
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

    free(displs);
    free(recvcounts);
    free(total_obj);
    free(my_region_storage_meta_1d);
    if (pdc_client_same_node_rank_g == 0)
        free(all_region_storage_meta_1d);

#else
    // send to node local server
    all_region_storage_meta_1d = (region_storage_meta_t *)calloc(nobj, sizeof(region_storage_meta_t));
    for (i = 0; i < nobj; i++)
        memcpy(&all_region_storage_meta_1d[i], all_storage_meta[i], sizeof(region_storage_meta_t));

    server_id = PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);
    ret_value = PDC_send_region_storage_meta_shm(server_id, nobj, all_region_storage_meta_1d);

    free(all_region_storage_meta_1d);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_read_with_storage_meta(int nobj, region_storage_meta_t **all_storage_meta, void ***buf_arr,
                                  size_t *size_arr)

{
    perr_t         ret_value = SUCCEED;
    int            i;
    char *         fname, *prev_fname;
    FILE *         fp_read = NULL;
    uint32_t       ndim;
    uint64_t       req_start, req_count, storage_start, storage_count, file_offset, buf_size;
    size_t         read_bytes;
    region_list_t *cache_region = NULL;

    FUNC_ENTER(NULL);

    *buf_arr = (void **)calloc(sizeof(void *), nobj);

    cache_count_g = 0;
    cache_total_g = nobj * cache_percentage_g / 100;

    // TODO: support for multi-dimensional data
    // Now read the data object one by one
    prev_fname = NULL;
    for (i = 0; i < nobj; i++) {
        if (NULL == all_storage_meta[i]) {
            printf("==PDC_CLIENT[%d]: NULL storage meta for %dth object!\n", pdc_client_mpi_rank_g, i);
            continue;
        }

        ndim = all_storage_meta[i]->region_transfer.ndim;
        if (ndim != 1) {
            printf("==PDC_CLIENT[%d]: only support for 1D data now (%u)!\n", pdc_client_mpi_rank_g, ndim);
            continue;
        }

        // Check if there is local cache
        if (cache_count_g < cache_total_g) {
            cache_region = PDC_get_storage_meta_from_io_list(&client_cache_list_head_g, all_storage_meta[i]);
            if (cache_region != NULL) {
                buf_size      = all_storage_meta[i]->size;
                (*buf_arr)[i] = malloc(buf_size);
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
                printf("==PDC_CLIENT[%d]: %s - fopen failed [%s] objid %" PRIu64 "\n", pdc_client_mpi_rank_g,
                       __func__, fname, all_storage_meta[i]->obj_id);
                prev_fname = fname;
                continue;
            }
        }
        prev_fname = fname;

        // TODO: currently assumes 1d data and 1 storage region per object
        storage_start = all_storage_meta[i]->region_transfer.start_0;
        storage_count = all_storage_meta[i]->region_transfer.count_0;
        req_start     = storage_start;
        req_count     = storage_count;
        file_offset   = all_storage_meta[i]->offset;
        buf_size      = all_storage_meta[i]->size;

        // malloc the buf array, this array should be freed by user afterwards.
        (*buf_arr)[i] = malloc(buf_size);
        PDC_Client_read_overlap_regions(ndim, &req_start, &req_count, &storage_start, &storage_count, fp_read,
                                        file_offset, (*buf_arr)[i], &read_bytes);
        size_arr[i] = read_bytes;
        if (read_bytes != buf_size) {
            printf("==PDC_CLIENT[%d]: actual read size %zu is not expected %" PRIu64 "\n",
                   pdc_client_mpi_rank_g, read_bytes, buf_size);
        }

        if (strstr(fname, "PDCcacheBB") != NULL) {
            nread_bb_g++;
            read_bb_size_g += read_bytes / 1048576.0;
        }

        /* printf("==PDC_CLIENT[%d]:          read data to buf[%d] %lu bytes done\n\n", */
        /*         pdc_client_mpi_rank_g, i, read_bytes); */
    }

    fflush(stdout);
    if (fp_read != NULL)
        fclose(fp_read);
    FUNC_LEAVE(ret_value);
}

// Query and retrieve all the storage regions of objects with their names
// It is possible that an object has multiple storage regions, they will be stored sequintially in
// storage_meta The storage_meta is also ordered based on the order in obj_names, the mapping can be easily
// formed by checking the obj_id in region_storage_meta_t
perr_t
PDC_Client_query_multi_storage_info(int nobj, char **obj_names, region_storage_meta_t ***all_storage_meta)
{
    perr_t                   ret_value = SUCCEED;
    hg_return_t              hg_ret    = HG_SUCCESS;
    hg_handle_t              rpc_handle;
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

    FUNC_ENTER(NULL);

    if (nobj == 0)
        PGOTO_DONE(SUCCEED);
    else if (obj_names == NULL || all_storage_meta == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid input", pdc_client_mpi_rank_g);

    // One request to each metadata server
    requests = (struct pdc_request **)calloc(sizeof(struct pdc_request *), pdc_server_num_g);

    obj_names_by_server             = (char ***)calloc(sizeof(char **), pdc_server_num_g);
    n_obj_name_by_server            = (int *)calloc(sizeof(int), pdc_server_num_g);
    obj_names_server_seq_mapping    = (int **)calloc(sizeof(int *), pdc_server_num_g);
    obj_names_server_seq_mapping_1d = (int *)calloc(sizeof(int), nobj * pdc_server_num_g);
    for (i = 0; i < pdc_server_num_g; i++) {
        obj_names_by_server[i]          = (char **)calloc(sizeof(char *), nobj);
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

        if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with PDC_Client_try_lookup_server",
                        pdc_client_mpi_rank_g);

        // Send the bulk handle to the target with RPC
        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr,
                           query_read_obj_name_client_register_id_g, &rpc_handle);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not create handle");

        total_size = 0;
        buf_sizes  = (uint64_t *)calloc(sizeof(uint64_t), n_obj_name_by_server[server_id]);
        for (i = 0; i < n_obj_name_by_server[server_id]; i++) {
            buf_sizes[i] = strlen(obj_names_by_server[server_id][i]) + 1;
            total_size += buf_sizes[i];
        }

        hg_ret = HG_Bulk_create(send_class_g, n_obj_name_by_server[server_id],
                                (void **)obj_names_by_server[server_id], buf_sizes, HG_BULK_READ_ONLY,
                                &bulk_handle);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "Could not create bulk data handle");

        requests[server_id]              = (struct pdc_request *)calloc(1, sizeof(struct pdc_request));
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
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);

        // Wait for server initiated bulk xfer
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);
    } // End for each meta server

    // Now we have all the storage meta stored in the requests structure
    // Reorgaze them and fill the output buffer
    (*all_storage_meta) = (region_storage_meta_t **)calloc(sizeof(region_storage_meta_t *), nobj);
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
    fflush(stdout);
    if (NULL != obj_names_by_server)
        free(obj_names_by_server);
    if (NULL != n_obj_name_by_server)
        free(n_obj_name_by_server);
    if (NULL != obj_names_by_server_2d)
        free(obj_names_by_server_2d);

    FUNC_LEAVE(ret_value);
}

#if defined(ENABLE_MPI) && defined(ENABLE_TIMING)
perr_t
PDC_get_io_stats_mpi(double read_time, double query_time, int nfopen)
{
    perr_t ret_value = SUCCEED;
    double read_time_max, read_time_min, read_time_avg, reduce_overhead;
    double query_time_max, query_time_min, query_time_avg;
    int    nfopen_min, nfopen_max, nfopen_avg;

    FUNC_ENTER(NULL);

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
        printf("==PDC_CLIENT: IO STATS (MIN, AVG, MAX)\n"
               "              #fopen   (%d, %d, %d)\n"
               "              Tquery   (%6.4f, %6.4f, %6.4f)\n"
               "              #readBB %d, size %.2f MB\n"
               "              Tread    (%6.4f, %6.4f, %6.4f)\nMPI overhead %.4f\n",
               nfopen_min, nfopen_avg, nfopen_max, query_time_min, query_time_avg, query_time_max, nread_bb_g,
               read_bb_size_g, read_time_min, read_time_avg, read_time_max, reduce_overhead);
    }

    FUNC_LEAVE(ret_value);
}
#endif

perr_t
PDC_Client_query_name_read_entire_obj_client(int nobj, char **obj_names, void ***out_buf,
                                             uint64_t *out_buf_sizes)
{
    perr_t                  ret_value        = SUCCEED;
    region_storage_meta_t **all_storage_meta = NULL;
#ifdef ENABLE_TIMING
    struct timeval pdc_timer1;
    struct timeval pdc_timer2;
    double         query_time, read_time;
#endif

    FUNC_ENTER(NULL);

    if (nobj == 0 || obj_names == NULL || out_buf == NULL || out_buf_sizes == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: invalid input", pdc_client_mpi_rank_g);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer1, 0);
#endif

    // Get the storage info for all objects, query results store in all_storage_meta
    ret_value = PDC_Client_query_multi_storage_info(nobj, obj_names, &all_storage_meta);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer2, 0);
    query_time = PDC_get_elapsed_time_double(&pdc_timer1, &pdc_timer2);
    query_time_g += query_time;
    /* printf("==PDC_CLIENT[%d]: query time %.4f\n", pdc_client_mpi_rank_g, query_time); */
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
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_name_read_entire_obj_client_agg(int my_nobj, char **my_obj_names, void ***out_buf,
                                                 size_t *out_buf_sizes)
{
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

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    local_names_1d = (char *)calloc(my_nobj, max_name_len);
    for (i = 0; i < my_nobj; i++) {
        if (strlen(my_obj_names[i]) > max_name_len)
            printf("==PDC_CLIENT[%2d]: object name longer than %lu [%s]!\n", pdc_client_mpi_rank_g,
                   max_name_len, my_obj_names[i]);
        strncpy(local_names_1d + i * max_name_len, my_obj_names[i], max_name_len - 1);
    }

    displs     = (int *)malloc(sizeof(int) * pdc_client_same_node_size_g);
    recvcounts = (int *)malloc(sizeof(int) * pdc_client_same_node_size_g);
    total_obj  = (int *)malloc(sizeof(int) * pdc_client_same_node_size_g);
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
        all_names    = (char **)calloc(sizeof(char *), ntotal_obj);
        all_names_1d = (char *)malloc(ntotal_obj * max_name_len);
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
        // res_storage_meta_1d = (region_storage_meta_t *)calloc(sizeof(region_storage_meta_t), ntotal_obj);
        res_storage_meta_1d = (region_storage_meta_t *)malloc(sizeof(region_storage_meta_t) * ntotal_obj);
        for (i = 0; i < ntotal_obj; i++)
            memcpy(&res_storage_meta_1d[i], all_storage_meta[i], sizeof(region_storage_meta_t));
    }

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer2, 0);
    query_time = PDC_get_elapsed_time_double(&pdc_timer1, &pdc_timer2);
    query_time_g += query_time;
#endif

    // allocate space for storage meta results
    my_storage_meta    = (region_storage_meta_t **)calloc(sizeof(region_storage_meta_t *), my_nobj);
    my_storage_meta_1d = (region_storage_meta_t *)calloc(sizeof(region_storage_meta_t), my_nobj);
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
        free(all_names);
        free(all_names_1d);
    }
    free(recvcounts);
    free(displs);
    free(total_obj);
    if (NULL != my_storage_meta)
        free(my_storage_meta);
    if (NULL != my_storage_meta_1d)
        free(my_storage_meta_1d);
#endif

    FUNC_LEAVE(ret_value);
}

// Process the storage metadata received from bulk transfer
perr_t
PDC_Client_recv_bulk_storage_meta(process_bulk_storage_meta_args_t *process_args)
{
    perr_t              ret_value = SUCCEED;
    struct pdc_request *request;

    FUNC_ENTER(NULL);

    if (NULL == process_args)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: NULL input", pdc_client_mpi_rank_g);

    // Now find the task and assign the storage meta to corresponding query request
    request = PDC_find_request_from_list_by_seq_id(&pdc_io_request_list_g, process_args->seq_id);
    if (NULL == request)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: cannot find previous IO request", pdc_client_mpi_rank_g);

    // Attach the received storage meta to the request
    request->storage_meta = process_args;

    ret_value = PDC_del_request_from_list(&pdc_io_request_list_g, request);

done:
    fflush(stdout);
    work_todo_g--;

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Client_recv_bulk_storage_meta_cb(const struct hg_cb_info *callback_info)
{
    PDC_Client_recv_bulk_storage_meta((process_bulk_storage_meta_args_t *)callback_info->arg);

    return HG_SUCCESS;
}

perr_t
PDC_Client_read_overlap_regions(uint32_t ndim, uint64_t *req_start, uint64_t *req_count,
                                uint64_t *storage_start, uint64_t *storage_count, FILE *fp,
                                uint64_t file_offset, void *buf, size_t *total_read_bytes)
{
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

    FUNC_ENTER(NULL);

    *total_read_bytes = 0;
    if (ndim > 3 || ndim <= 0)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: dim=%" PRIu32 " unsupported yet!", pdc_client_mpi_rank_g, ndim);

    // Get the actual start and count of region in storage
    if (PDC_get_overlap_start_count(ndim, req_start, req_count, storage_start, storage_count, overlap_start,
                                    overlap_count) != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: PDC_get_overlap_start_count FAILED!", pdc_client_mpi_rank_g);

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
            printf("==PDC_CLIENT[%d]: read storage offset %" PRIu64 ", buf_offset  %" PRIu64 "\n",
                   pdc_client_mpi_rank_g, storage_offset, buf_offset);
        }

        read_bytes = fread(buf + buf_offset, 1, total_bytes, fp);

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end1, 0);
        double region_read_time1 = PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
        if (is_client_debug_g) {
            printf("==PDC_CLIENT[%d]: fseek + fread %" PRIu64 " bytes, %.2fs\n", pdc_client_mpi_rank_g,
                   read_bytes, region_read_time1);
            fflush(stdout);
        }
#endif

        n_contig_MB += read_bytes / 1048576.0;
        n_contig_read++;
        if (read_bytes != total_bytes)
            PGOTO_ERROR(FAIL,
                        "==PDC_CLIENT[%d]: fread failed actual read bytes %" PRIu64 ", should be %" PRIu64 "",
                        pdc_client_mpi_rank_g, read_bytes, total_bytes);

        *total_read_bytes += read_bytes;

        if (is_client_debug_g == 1) {
            printf("==PDC_CLIENT[%d]: Read entire storage region, size=%" PRIu64 "", pdc_client_mpi_rank_g,
                   read_bytes);
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
                    PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: fread failed!", pdc_client_mpi_rank_g);

                *total_read_bytes += read_bytes;
            } // for each row
        }     // ndim=2
        else if (ndim == 3) {

            if (is_client_debug_g == 1) {
                printf("read count: %" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n", overlap_count[0],
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
                        printf("Read to buf offset: %" PRIu64 "\n", buf_serialize_offset);
                    }

                    read_bytes = fread(buf + buf_serialize_offset, 1, overlap_count[0], fp);
                    n_contig_MB += read_bytes / 1048576.0;
                    n_contig_read++;
                    if (read_bytes != overlap_count[0])
                        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: fread failed!", pdc_client_mpi_rank_g);

                    *total_read_bytes += read_bytes;
                    if (is_client_debug_g == 1) {
                        printf("z: %" PRIu64 ", j: %" PRIu64 ", Read data size=%" PRIu64 ": [%.*s]\n", j, i,
                               overlap_count[0], (int)overlap_count[0], (char *)buf + buf_serialize_offset);
                    }
                } // for each row
            }
        }
    } // end else (ndim != 1 && !is_all_selected);

    if (total_bytes != *total_read_bytes)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: read size error!", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_query_name_read_entire_obj_client_agg_cache_iter(int my_nobj, char **my_obj_names, void ***out_buf,
                                                            size_t *out_buf_sizes, int cache_percentage)
{
    perr_t ret_value   = SUCCEED;
    cache_percentage_g = cache_percentage;

    FUNC_ENTER(NULL);

    ret_value =
        PDC_Client_query_name_read_entire_obj_client_agg(my_nobj, my_obj_names, out_buf, out_buf_sizes);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_add_kvtag(pdcid_t obj_id, pdc_kvtag_t *kvtag, int is_cont)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    uint64_t                       meta_id;
    uint32_t                       server_id;
    hg_handle_t                    metadata_add_kvtag_handle;
    metadata_add_kvtag_in_t        in;
    struct _pdc_obj_info *         obj_prop;
    struct _pdc_cont_info *        cont_prop;
    struct _pdc_client_lookup_args lookup_args;

    FUNC_ENTER(NULL);

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

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_add_kvtag_register_id_g,
              &metadata_add_kvtag_handle);

    // Fill input structure

    if (kvtag != NULL && kvtag != NULL && kvtag->size != 0) {
        in.kvtag.name  = kvtag->name;
        in.kvtag.value = kvtag->value;
        in.kvtag.size  = kvtag->size;
    }
    else
        PGOTO_ERROR(FAIL, "==PDC_Client_add_kvtag(): invalid tag content!");

    hg_ret = HG_Forward(metadata_add_kvtag_handle, metadata_add_tag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_add_kvtag_metadata_with_name(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        printf("PDC_CLIENT: add kvtag NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    fflush(stdout);
    HG_Destroy(metadata_add_kvtag_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
metadata_get_kvtag_rpc_cb(const struct hg_cb_info *callback_info)
{

    hg_return_t                 ret_value          = HG_SUCCESS;
    struct _pdc_get_kvtag_args *client_lookup_args = (struct _pdc_get_kvtag_args *)callback_info->arg;
    hg_handle_t                 handle             = callback_info->info.forward.handle;
    metadata_get_kvtag_out_t    output;

    FUNC_ENTER(NULL);

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        client_lookup_args->ret = -1;
        PGOTO_ERROR(ret_value, "==PDC_CLIENT[%d]: metadata_add_tag_rpc_cb error with HG_Get_output",
                    pdc_client_mpi_rank_g);
    }
    client_lookup_args->ret          = output.ret;
    client_lookup_args->kvtag->name  = strdup(output.kvtag.name);
    client_lookup_args->kvtag->size  = output.kvtag.size;
    client_lookup_args->kvtag->value = malloc(output.kvtag.size);
    memcpy(client_lookup_args->kvtag->value, output.kvtag.value, output.kvtag.size);
    /* PDC_kvtag_dup(&(output.kvtag), &client_lookup_args->kvtag); */

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_get_kvtag(pdcid_t obj_id, char *tag_name, pdc_kvtag_t **kvtag, int is_cont)
{
    perr_t                     ret_value = SUCCEED;
    hg_return_t                hg_ret    = 0;
    uint64_t                   meta_id;
    uint32_t                   server_id;
    hg_handle_t                metadata_get_kvtag_handle;
    metadata_get_kvtag_in_t    in;
    struct _pdc_get_kvtag_args lookup_args;
    struct _pdc_obj_info *     obj_prop;
    struct _pdc_cont_info *    cont_prop;

    FUNC_ENTER(NULL);

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

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_get_kvtag_register_id_g,
              &metadata_get_kvtag_handle);

    if (tag_name != NULL && kvtag != NULL) {
        in.key = tag_name;
    }
    else
        PGOTO_ERROR(FAIL, "==PDC_Client_get_kvtag(): invalid tag content!");

    *kvtag            = (pdc_kvtag_t *)malloc(sizeof(pdc_kvtag_t));
    lookup_args.kvtag = *kvtag;
    hg_ret            = HG_Forward(metadata_get_kvtag_handle, metadata_get_kvtag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_get_kvtag_metadata_with_name(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        printf("PDC_CLIENT: get kvtag NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    fflush(stdout);
    HG_Destroy(metadata_get_kvtag_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCtag_delete(pdcid_t obj_id, char *tag_name)
{
    perr_t                         ret_value = SUCCEED;
    hg_return_t                    hg_ret    = 0;
    uint64_t                       meta_id;
    uint32_t                       server_id;
    hg_handle_t                    metadata_del_kvtag_handle;
    metadata_get_kvtag_in_t        in;
    struct _pdc_obj_info *         obj_prop;
    struct _pdc_client_lookup_args lookup_args;

    FUNC_ENTER(NULL);

    obj_prop  = PDC_obj_get_info(obj_id);
    meta_id   = obj_prop->obj_info_pub->meta_id;
    server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);

    debug_server_id_count[server_id]++;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_del_kvtag_register_id_g,
              &metadata_del_kvtag_handle);

    // Fill input structure
    in.obj_id     = meta_id;
    in.hash_value = PDC_get_hash_by_name(obj_prop->obj_info_pub->name);
    in.key        = tag_name;

    hg_ret = HG_Forward(metadata_del_kvtag_handle, metadata_add_tag_rpc_cb /*reuse*/, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_Client_del_kvtag_metadata_with_name(): Could not start HG_Forward()");

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1)
        printf("PDC_CLIENT: del kvtag NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    fflush(stdout);
    HG_Destroy(metadata_del_kvtag_handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
kvtag_query_bulk_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t         ret_value = HG_SUCCESS;
    struct bulk_args_t *bulk_args;
    hg_bulk_t           origin_bulk_handle = hg_cb_info->info.bulk.origin_handle;
    hg_bulk_t           local_bulk_handle  = hg_cb_info->info.bulk.local_handle;
    void *              buf                = NULL;
    uint32_t            n_meta, actual_cnt;
    uint64_t            buf_sizes[1];

    FUNC_ENTER(NULL);

    bulk_args = (struct bulk_args_t *)hg_cb_info->arg;

    n_meta = bulk_args->n_meta;

    if (hg_cb_info->ret == HG_SUCCESS) {
        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, buf_sizes,
                       &actual_cnt);

        bulk_args->obj_ids = (uint64_t *)calloc(sizeof(uint64_t), n_meta);
        memcpy(bulk_args->obj_ids, buf, sizeof(uint64_t) * n_meta);
    }
    else
        PGOTO_ERROR(HG_PROTOCOL_ERROR, "==PDC_CLIENT[%d]: Error with bulk handle", pdc_client_mpi_rank_g);

    bulk_todo_g--;
    hg_atomic_set32(&bulk_transfer_done_g, 1);

    // Free local bulk handle
    ret_value = HG_Bulk_free(local_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

    ret_value = HG_Bulk_free(origin_bulk_handle);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not free HG bulk handle");

done:
    fflush(stdout);
    HG_Destroy(bulk_args->handle);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
kvtag_query_forward_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                   ret_value;
    struct bulk_args_t *          bulk_arg;
    hg_handle_t                   handle;
    metadata_query_transfer_out_t output;
    uint32_t                      n_meta;
    hg_op_id_t                    hg_bulk_op_id;
    hg_bulk_t                     local_bulk_handle  = HG_BULK_NULL;
    hg_bulk_t                     origin_bulk_handle = HG_BULK_NULL;
    const struct hg_info *        hg_info            = NULL;

    FUNC_ENTER(NULL);

    bulk_arg = (struct bulk_args_t *)callback_info->arg;
    handle   = callback_info->info.forward.handle;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error HG_Get_output", pdc_client_mpi_rank_g);

    if (output.bulk_handle == HG_BULK_NULL || output.ret == 0) {
        bulk_todo_g = 0;
        work_todo_g--;
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
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Client_query_kvtag_server(uint32_t server_id, const pdc_kvtag_t *kvtag, int *n_res, uint64_t **out)
{
    perr_t              ret_value = SUCCEED;
    hg_return_t         hg_ret;
    hg_handle_t         query_kvtag_server_handle;
    pdc_kvtag_t         in;
    struct bulk_args_t *bulk_arg;

    FUNC_ENTER(NULL);

    if (kvtag == NULL || n_res == NULL || out == NULL)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: input is NULL!", pdc_client_mpi_rank_g);

    if (kvtag->name == NULL)
        in.name = " ";
    else
        in.name = kvtag->name;

    if (kvtag->value == NULL) {
        in.value = " ";
        in.size  = 1;
    }
    else {
        in.value = kvtag->value;
        in.size  = kvtag->size;
    }

    *out   = NULL;
    *n_res = 0;

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_kvtag_register_id_g,
                       &query_kvtag_server_handle);

    bulk_arg = (struct bulk_args_t *)calloc(1, sizeof(struct bulk_args_t));
    if (query_kvtag_server_handle == NULL)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: Error with query_kvtag_server_handle", pdc_client_mpi_rank_g);

    hg_ret = HG_Forward(query_kvtag_server_handle, kvtag_query_forward_cb, bulk_arg, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "PDC_client_list_all(): Could not start HG_Forward()");

    hg_atomic_set32(&bulk_transfer_done_g, 0);

    // Wait for response from server
    bulk_todo_g = 1;
    PDC_Client_check_bulk(send_context_g);

    *n_res = bulk_arg->n_meta;
    *out   = bulk_arg->obj_ids;
    free(bulk_arg);
    // TODO: need to be careful when freeing the lookup_args, as it include the results returned to user

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Single client query all servers
perr_t
PDC_Client_query_kvtag(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids)
{
    perr_t  ret_value = SUCCEED;
    int32_t i;
    int     nmeta = 0;

    FUNC_ENTER(NULL);

    *n_res = 0;
    for (i = 0; i < pdc_server_num_g; i++) {
        ret_value = PDC_Client_query_kvtag_server((uint32_t)i, kvtag, &nmeta, pdc_ids);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with PDC_Client_query_kvtag_server to server %d",
                        pdc_client_mpi_rank_g, i);
    }

    *n_res = nmeta;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void
PDC_assign_server(uint32_t *my_server_start, uint32_t *my_server_end, uint32_t *my_server_count)
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

    FUNC_LEAVE_VOID;
}

// All clients collectively query all servers
perr_t
PDC_Client_query_kvtag_col(const pdc_kvtag_t *kvtag, int *n_res, uint64_t **pdc_ids)
{
    perr_t  ret_value = SUCCEED;
    int32_t my_server_start, my_server_end, my_server_count;
    int32_t i;
    int     nmeta = 0;

    FUNC_ENTER(NULL);

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
            my_server_end = 0;
        }
    }

    *n_res = 0;
    for (i = my_server_start; i < my_server_end; i++) {
        if (i >= pdc_server_num_g) {
            break;
        }
        ret_value = PDC_Client_query_kvtag_server((uint32_t)i, kvtag, &nmeta, pdc_ids);
        if (ret_value != SUCCEED)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with PDC_Client_query_kvtag_server to server %u",
                        pdc_client_mpi_rank_g, i);
    }

    *n_res = nmeta;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/* - -------------------------------- */
/* New Simple Object Access Interface */
/* - -------------------------------- */
// Create a container with specified name
pdcid_t
PDCcont_put(const char *cont_name, pdcid_t pdc)
{
    perr_t  ret_value;
    pdcid_t cont_id = 0, cont_prop;

    FUNC_ENTER(NULL);

    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);

#ifdef ENABLE_MPI
    ret_value = PDC_Client_create_cont_id_mpi(cont_name, cont_prop, &cont_id);
#else
    ret_value = PDC_Client_create_cont_id(cont_name, cont_prop, &cont_id);
#endif
    if (ret_value != SUCCEED)
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: error with PDC_Client_create_cont_id", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(cont_id);
}

pdcid_t
PDCcont_get_id(const char *cont_name, pdcid_t pdc_id)
{
    pdcid_t  cont_id;
    uint64_t cont_meta_id;

    FUNC_ENTER(NULL);

    PDC_Client_query_container_name(cont_name, &cont_meta_id);
    cont_id = PDC_cont_create_local(pdc_id, cont_name, cont_meta_id);

    FUNC_LEAVE(cont_id);
}

// Get container name
perr_t
PDCcont_get(pdcid_t cont_id ATTRIBUTE(unused), char **cont_name ATTRIBUTE(unused))
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    // TODO
    /* ret_value = PDC_Client_query_container_name(char *cont_name, pdc_metadata_t **out); */
    /* if (ret_value != SUCCEED) { */
    /*     PGOTO_ERROR(0, "==PDC_CLIENT[%d]: %s - error with PDC_Client_create_cont_id", */
    /*             pdc_client_mpi_rank_g, __func__); */
    /* } */

    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_del(pdcid_t cont_id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_del_metadata(cont_id, 1);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with PDC_Client_del_objects_to_container",
                    pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Put a number of objects to a container
perr_t
PDCcont_put_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_add_objects_to_container(nobj, obj_ids, cont_id);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with PDC_Client_add_objects_to_container",
                    pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_del_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_del_objects_to_container(nobj, obj_ids, cont_id);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with PDC_Client_del_objects_to_container",
                    pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_get_objids(pdcid_t cont_id ATTRIBUTE(unused), int *nobj ATTRIBUTE(unused),
                   pdcid_t **obj_ids ATTRIBUTE(unused))
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    // TODO

    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_put_tag(pdcid_t cont_id, char *tag_name, void *tag_value, psize_t value_size)
{
    perr_t      ret_value = SUCCEED;
    pdc_kvtag_t kvtag;

    FUNC_ENTER(NULL);

    kvtag.name  = tag_name;
    kvtag.value = (void *)tag_value;
    kvtag.size  = (uint64_t)value_size;

    ret_value = PDC_add_kvtag(cont_id, &kvtag, 1);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Error with PDCcont_put_tag", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_get_tag(pdcid_t cont_id, char *tag_name, void **tag_value, psize_t *value_size)
{
    perr_t       ret_value = SUCCEED;
    pdc_kvtag_t *kvtag     = NULL;

    FUNC_ENTER(NULL);

    ret_value = PDC_get_kvtag(cont_id, tag_name, &kvtag, 1);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Error with PDC_get_kvtag", pdc_client_mpi_rank_g);

    *tag_value  = kvtag->value;
    *value_size = kvtag->size;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_del_tag(pdcid_t cont_id, char *tag_name)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDCobj_del_tag(cont_id, tag_name);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: error with PDCobj_del_tag", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCobj_put_data(const char *obj_name, void *data, uint64_t size, pdcid_t cont_id)
{
    pdcid_t ret_value = 0;
    pdcid_t obj_id, obj_prop, obj_region;
    perr_t  ret;
    // pdc_metadata_t *meta;
    struct _pdc_cont_info *info    = NULL;
    struct _pdc_id_info *  id_info = NULL;

    FUNC_ENTER(NULL);

    id_info = PDC_find_id(cont_id);
    info    = (struct _pdc_cont_info *)(id_info->obj_ptr);

    obj_prop = PDCprop_create(PDC_OBJ_CREATE, info->cont_pt->pdc->local_id);
    PDCprop_set_obj_type(obj_prop, PDC_CHAR);
    PDCprop_set_obj_dims(obj_prop, 1, &size);
    PDCprop_set_obj_user_id(obj_prop, getuid());
    PDCprop_set_obj_time_step(obj_prop, 0);

    obj_id = PDC_obj_create(cont_id, obj_name, obj_prop, PDC_OBJ_GLOBAL);
    if (obj_id <= 0)
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: Error creating object [%s]", pdc_client_mpi_rank_g, obj_name);

    int      ndim   = 1;
    uint64_t offset = 0;
    // size = ceil(size/sizeof(int));
    obj_region = PDCregion_create(ndim, &offset, &size);

    ret = PDCbuf_obj_map(data, PDC_CHAR, obj_region, obj_id, obj_region);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: Error with PDCbuf_obj_map for obj [%s]", pdc_client_mpi_rank_g,
                    obj_name);
    }

    ret = PDCreg_obtain_lock(obj_id, obj_region, PDC_WRITE, PDC_BLOCK);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: Error with PDCreg_obtain_lock for obj [%s]", pdc_client_mpi_rank_g,
                    obj_name);
    }
    ret = PDCreg_release_lock(obj_id, obj_region, PDC_WRITE);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: Error with PDCreg_release_lock for obj [%s]", pdc_client_mpi_rank_g,
                    obj_name);
    }

    ret = PDCbuf_obj_unmap(obj_id, obj_region);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: Error with PDCbuf_obj_unmap for obj [%s]", pdc_client_mpi_rank_g,
                    obj_name);
    }

    ret = PDCregion_close(obj_region);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: Error with PDCregion_close for obj [%s]", pdc_client_mpi_rank_g,
                    obj_name);
    }

    ret = PDCprop_close(obj_prop);
    if (ret != SUCCEED) {
        PGOTO_ERROR(0, "==PDC_CLIENT[%d]: Error with PDCprop_close for obj [%s]", pdc_client_mpi_rank_g,
                    obj_name);
    }

    ret_value = obj_id;
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_get_data(pdcid_t obj_id, void *data, uint64_t size)
{
    perr_t   ret_value = SUCCEED;
    uint64_t offset    = 0;
    pdcid_t  reg, reg_global;
    reg        = PDCregion_create(1, &offset, &size);
    reg_global = PDCregion_create(1, &offset, &size);

    ret_value = PDCbuf_obj_map(data, PDC_CHAR, reg, obj_id, reg_global);
    if (ret_value != SUCCEED) {
        goto done;
    }

    ret_value = PDCreg_obtain_lock(obj_id, reg_global, PDC_READ, PDC_BLOCK);
    if (ret_value != SUCCEED) {
        goto done;
    }

    ret_value = PDCreg_release_lock(obj_id, reg_global, PDC_READ);
    if (ret_value != SUCCEED) {
        goto done;
    }

    ret_value = PDCbuf_obj_unmap(obj_id, reg_global);
    if (ret_value != SUCCEED) {
        goto done;
    }

    ret_value = PDCregion_close(reg);
    if (ret_value != SUCCEED) {
        goto done;
    }
    ret_value = PDCregion_close(reg_global);
    if (ret_value != SUCCEED) {
        goto done;
    }
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Client_del_metadata(pdcid_t obj_id, int is_cont)
{
    perr_t                 ret_value = SUCCEED;
    uint64_t               meta_id;
    struct _pdc_obj_info * obj_prop;
    struct _pdc_cont_info *cont_prop;

    FUNC_ENTER(NULL);

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
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Error with PDC_Client_delete_metadata_by_id",
                    pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_put_tag(pdcid_t obj_id, char *tag_name, void *tag_value, psize_t value_size)
{
    perr_t      ret_value = SUCCEED;
    pdc_kvtag_t kvtag;

    FUNC_ENTER(NULL);

    kvtag.name  = tag_name;
    kvtag.value = (void *)tag_value;
    kvtag.size  = (uint64_t)value_size;

    ret_value = PDC_add_kvtag(obj_id, &kvtag, 0);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Error with PDC_add_kvtag", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_get_tag(pdcid_t obj_id, char *tag_name, void **tag_value, psize_t *value_size)
{
    perr_t       ret_value = SUCCEED;
    pdc_kvtag_t *kvtag     = NULL;

    FUNC_ENTER(NULL);

    ret_value = PDC_get_kvtag(obj_id, tag_name, &kvtag, 0);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Error with PDC_get_kvtag", pdc_client_mpi_rank_g);

    *tag_value  = kvtag->value;
    *value_size = kvtag->size;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_del_tag(pdcid_t obj_id, char *tag_name)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDCtag_delete(obj_id, tag_name);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: Error with PDC_del_kvtag", pdc_client_mpi_rank_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void
PDC_get_server_from_query(pdc_query_t *query, uint32_t *servers, int32_t *n)
{
    uint32_t id;
    int32_t  i, exist;

    FUNC_ENTER(NULL);

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
    FUNC_LEAVE_VOID;
}

int
gen_query_id()
{
    int ret_value = 0;

    FUNC_ENTER(NULL);

    ret_value = rand();

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_recv_nhits(const struct hg_cb_info *callback_info)
{
    hg_return_t                    ret_value = HG_SUCCESS;
    send_nhits_t *                 in        = (send_nhits_t *)callback_info->arg;
    struct _pdc_query_result_list *result_elt;

    FUNC_ENTER(NULL);

    printf("==PDC_CLIENT[%d]: %s - received %" PRIu64 " hits from server\n", pdc_client_mpi_rank_g, __func__,
           in->nhits);

    DL_FOREACH(pdcquery_result_list_head_g, result_elt)
    {
        if (result_elt->query_id == in->query_id) {
            result_elt->nhits = in->nhits;
            break;
        }
    }

    work_todo_g--;
    free(in);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_send_data_query(pdc_query_t *query, pdc_query_get_op_t get_op, uint64_t *nhits, pdc_selection_t *sel,
                    void *data ATTRIBUTE(unused))
{
    perr_t                         ret_value      = SUCCEED;
    hg_return_t                    hg_ret         = 0;
    uint32_t *                     target_servers = NULL;
    int                            i, server_id, next_server = 0, prev_server = 0, ntarget = 0;
    hg_handle_t                    handle;
    pdc_query_xfer_t *             query_xfer;
    struct _pdc_client_lookup_args lookup_args;
    struct _pdc_query_result_list *result;

    FUNC_ENTER(NULL);

    query_xfer = PDC_serialize_query(query);
    if (query_xfer == NULL)
        PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_serialize_query", pdc_client_mpi_rank_g);

    // Find unique server IDs that has metadata of the queried objects
    target_servers = (uint32_t *)calloc(pdc_server_num_g, sizeof(uint32_t));
    PDC_get_server_from_query(query, target_servers, &ntarget);
    query_xfer->n_unique_obj = ntarget;
    query_xfer->query_id     = gen_query_id();
    query_xfer->client_id    = pdc_client_mpi_rank_g;
    query_xfer->manager      = target_servers[0];
    query_xfer->get_op       = (int)get_op;

    result           = (struct _pdc_query_result_list *)calloc(1, sizeof(struct _pdc_query_result_list));
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

        if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
            PGOTO_ERROR(FAIL, "==CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, send_data_query_register_id_g, &handle);

        hg_ret = HG_Forward(handle, pdc_client_check_int_ret_cb, &lookup_args, query_xfer);
        if (hg_ret != HG_SUCCESS)
            PGOTO_ERROR(FAIL, "PDC_Client_del_kvtag_metadata_with_name(): Could not start HG_Forward()");

        // Wait for response from server
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);

        if (lookup_args.ret != 1)
            PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: send data query to server %u failed ... ret_value = %d",
                        pdc_client_mpi_rank_g, server_id, lookup_args.ret);

        HG_Destroy(handle);
    }

    // Wait for server to send query result
    work_todo_g = 1;
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
    fflush(stdout);
    if (target_servers)
        free(target_servers);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_recv_coords(const struct hg_cb_info *callback_info)
{
    hg_return_t                    ret_value         = HG_SUCCESS;
    hg_bulk_t                      local_bulk_handle = callback_info->info.bulk.local_handle;
    struct bulk_args_t *           bulk_args         = (struct bulk_args_t *)callback_info->arg;
    struct _pdc_query_result_list *result_elt;
    uint64_t                       nhits = 0;
    uint32_t                       ndim;
    int                            query_id, origin;
    void *                         buf;
    pdc_int_ret_t                  out;

    FUNC_ENTER(NULL);

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

        printf("==PDC_CLIENT[%d]: %s - received %" PRIu64 " coords from server %d\n", pdc_client_mpi_rank_g,
               __func__, nhits, origin);

        if (nhits > 0) {
            ret_value = HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1,
                                       (void **)&buf, NULL, NULL);
        }

        DL_FOREACH(pdcquery_result_list_head_g, result_elt)
        {
            if (result_elt->query_id == query_id) {
                result_elt->ndim   = ndim;
                result_elt->nhits  = nhits;
                result_elt->coords = (uint64_t *)malloc(nhits * ndim * sizeof(uint64_t));
                memcpy(result_elt->coords, buf, nhits * ndim * sizeof(uint64_t));
                break;
            }
        }

        if (result_elt == NULL)
            PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_CLIENT[%d]: Invalid task ID!", pdc_client_mpi_rank_g);
    } // End else

done:
    work_todo_g--;
    if (nhits > 0) {
        ret_value = HG_Bulk_free(local_bulk_handle);
        if (ret_value != HG_SUCCESS)
            PGOTO_ERROR(ret_value, "Could not free HG bulk handle");
    }

    ret_value = HG_Respond(bulk_args->handle, NULL, NULL, &out);
    if (ret_value != HG_SUCCESS)
        PGOTO_ERROR(ret_value, "Could not respond");

    fflush(stdout);
    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    FUNC_LEAVE(ret_value);
}

void
PDCselection_free(pdc_selection_t *sel)
{
    FUNC_ENTER(NULL);

    if (sel->coords_alloc > 0 && sel->coords)
        free(sel->coords);

    FUNC_LEAVE_VOID;
}

perr_t
PDC_Client_get_sel_data(pdcid_t obj_id, pdc_selection_t *sel, void *data)
{
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

    FUNC_ENTER(NULL);

    if (sel == NULL)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: NULL input!", pdc_client_mpi_rank_g);

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

    if (PDC_Client_try_lookup_server(server_id) != SUCCEED)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with PDC_Client_try_lookup_server", pdc_client_mpi_rank_g);

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, get_sel_data_register_id_g, &handle);

    hg_ret = HG_Forward(handle, pdc_client_check_int_ret_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS)
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: ERROR with HG_Forward", pdc_client_mpi_rank_g);

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1) {
        PGOTO_ERROR(FAIL, "==PDC_CLIENT[%d]: send data selection to server %u failed ... ret_value = %d",
                    pdc_client_mpi_rank_g, server_id, lookup_args.ret);
    }

    // Wait for server to send data
    work_todo_g = 1;
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
                    free(result_elt->data_arr[i]);
                    result_elt->data_arr[i] = NULL;
                }
            }
            result_elt->recv_data_nhits = 0;
            break;
        }
    }

done:
    fflush(stdout);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_recv_read_coords_data(const struct hg_cb_info *callback_info)
{
    hg_return_t                    ret_value         = HG_SUCCESS;
    hg_bulk_t                      local_bulk_handle = callback_info->info.bulk.local_handle;
    struct bulk_args_t *           bulk_args         = (struct bulk_args_t *)callback_info->arg;
    struct _pdc_query_result_list *result_elt;
    uint64_t                       nhits = 0;
    int                            query_id, seq_id;
    void *                         buf;
    pdc_int_ret_t                  out;

    FUNC_ENTER(NULL);

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
                    result_elt->data_arr      = calloc(sizeof(void *), pdc_server_num_g);
                    result_elt->data_arr_size = calloc(sizeof(uint64_t *), pdc_server_num_g);
                }

                result_elt->data_arr[seq_id] = malloc(bulk_args->nbytes);
                memcpy(result_elt->data_arr[seq_id], buf, bulk_args->nbytes);
                result_elt->data_arr_size[seq_id] = bulk_args->nbytes;
                result_elt->recv_data_nhits += nhits;
                break;
            }
        }
        if (result_elt == NULL)
            PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_CLIENT[%d]: Invalid task ID!", pdc_client_mpi_rank_g);

        if (result_elt->recv_data_nhits == result_elt->nhits) {
            work_todo_g--;
        }
        else if (result_elt->recv_data_nhits > result_elt->nhits) {
            PGOTO_ERROR(HG_OTHER_ERROR, "==PDC_CLIENT[%d]: received more results data than expected!",
                        pdc_client_mpi_rank_g);
            work_todo_g--;
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

    fflush(stdout);
    HG_Destroy(bulk_args->handle);
    free(bulk_args);

    FUNC_LEAVE(ret_value);
}
