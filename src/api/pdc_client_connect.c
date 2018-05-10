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

#include "config.h"
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

/* #define ENABLE_MPI 1 */
#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

#ifdef PDC_HAS_CRAY_DRC
    # include <rdmacred.h>
#endif

#include "../server/utlist.h"

#include "mercury.h"
#include "mercury_request.h"
#include "mercury_macros.h"
#include "mercury_hl.h"


#include "pdc_interface.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"
#include "pdc_obj_pkg.h"

int is_client_debug_g = 0;

int                    pdc_client_mpi_rank_g = 0;
int                    pdc_client_mpi_size_g = 1;

#ifdef ENABLE_MPI
MPI_Comm               PDC_SAME_NODE_COMM_g;
#endif
int                    pdc_client_same_node_rank_g = 0;
int                    pdc_client_same_node_size_g = 1;

int                    pdc_server_num_g;
int                    pdc_use_local_server_only_g = 0;
int                    pdc_nclient_per_server_g = 0;

char                   pdc_client_tmp_dir_g[ADDR_MAX];
pdc_server_info_t     *pdc_server_info_g = NULL;
pdc_client_t          *pdc_client_direct_channels = NULL;
pdc_client_t          *thisClient = NULL;
static int            *debug_server_id_count = NULL;

int                    pdc_io_request_seq_id = PDC_SEQ_ID_INIT_VALUE;
PDC_Request_t         *pdc_io_request_list_g = NULL;

double                 memcpy_time_g = 0.0;

static int             mercury_has_init_g = 0;
static hg_class_t     *send_class_g = NULL;
static hg_context_t   *send_context_g = NULL;
static int             work_todo_g = 0;
hg_request_class_t    *request_class_g = NULL;
static int             request_progressed = 0;
static int             request_triggered = 0;


static hg_id_t         client_test_connect_register_id_g;
static hg_id_t         gen_obj_register_id_g;
static hg_id_t         gen_cont_register_id_g;
static hg_id_t         close_server_register_id_g;
static hg_id_t         metadata_query_register_id_g;
static hg_id_t         metadata_delete_register_id_g;
static hg_id_t         metadata_delete_by_id_register_id_g;
static hg_id_t         metadata_update_register_id_g;
static hg_id_t         metadata_add_tag_register_id_g;
static hg_id_t         region_lock_register_id_g;
static hg_id_t         region_release_register_id_g;
static hg_id_t         data_server_read_register_id_g;
static hg_id_t         data_server_read_check_register_id_g;
static hg_id_t         data_server_write_check_register_id_g;
static hg_id_t         data_server_write_register_id_g;
static hg_id_t         aggregate_write_register_id_g;
static hg_id_t         server_checkpoint_rpc_register_id_g;

// bulk
static hg_id_t         query_partial_register_id_g;
static int             bulk_todo_g = 0;
hg_atomic_int32_t      bulk_transfer_done_g;

static hg_id_t         buf_map_register_id_g;
static hg_id_t	       reg_map_register_id_g;
static hg_id_t         buf_unmap_register_id_g;
static hg_id_t         reg_unmap_register_id_g;
static hg_id_t         obj_unmap_register_id_g;

// client direct
static hg_id_t         client_direct_addr_register_id_g;

static hg_id_t         cont_add_del_objs_rpc_register_id_g;
static hg_id_t         query_read_obj_name_register_id_g;


/* 
 *
 * Client Functions
 *
 */

static int
mercury_request_progress(unsigned int timeout, void *arg)
{
    /*
    printf("Doing progress\n");
    */
    hg_context_t *context = (hg_context_t *) arg;
    int ret = HG_UTIL_SUCCESS;

    if (HG_Progress(context, timeout) != HG_SUCCESS)
        ret = HG_UTIL_FAIL;

    return ret;
}

static int
mercury_request_trigger(unsigned int timeout, unsigned int *flag, void *arg)
{
    /*
    printf("Calling trigger\n");
    */
    hg_context_t *context = (hg_context_t *) arg;
    unsigned int actual_count = 0;
    int ret = HG_UTIL_SUCCESS;

    if (HG_Trigger(context, timeout, 1, &actual_count)
            != HG_SUCCESS) ret = HG_UTIL_FAIL;
    *flag = (actual_count) ? HG_UTIL_TRUE : HG_UTIL_FALSE;

    return ret;
}

static inline uint32_t get_server_id_by_hash_name(const char *name) 
{
    return (uint32_t)(PDC_get_hash_by_name(name) % pdc_server_num_g);
}

static inline uint32_t get_server_id_by_obj_id(uint64_t obj_id) 
{
    return (uint32_t)((obj_id / PDC_SERVER_ID_INTERVEL - 1) % pdc_server_num_g);
}

// Check if all work has been processed
// Using global variable $mercury_work_todo_g
perr_t PDC_Client_check_response(hg_context_t **hg_context)
{
    perr_t ret_value= SUCCEED;
    hg_return_t hg_ret;
    unsigned int actual_count;
    
    FUNC_ENTER(NULL);

    do {
        do {
            /* hg_ret = HG_Trigger(*hg_context, 100/1* timeout *1/, 1/1* max count *1/, &actual_count); */
            hg_ret = HG_Trigger(*hg_context, 0/* timeout */, 1/* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* printf("actual_count=%d\n",actual_count); */
        /* Do not try to make progress anymore if we're done */
        if (work_todo_g <= 0)  break;

        /* hg_ret = HG_Progress(*hg_context, 1000); */
        hg_ret = HG_Progress(*hg_context, HG_MAX_IDLE_TIME);
    } while (hg_ret == HG_SUCCESS || hg_ret == HG_TIMEOUT);

    ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}


perr_t PDC_Client_read_server_addr_from_file()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int max_tries = 9, sleeptime = 1;
    int i = 0, is_server_ready = 0;
    char  *p;
    FILE *na_config = NULL;
    char config_fname[ADDR_MAX];

    if (pdc_client_mpi_rank_g == 0) {
        sprintf(config_fname, "%s/%s", pdc_client_tmp_dir_g, pdc_server_cfg_name_g);
        /* printf("config file:%s\n",config_fname); */

        for (i = 0; i < max_tries; i++) {
            if( access( config_fname, F_OK ) != -1 ) {
                is_server_ready = 1;
                break;
            }
            printf("==PDC_CLIENT[%d]: Config file from default location [%s] not available, "
                   "waiting %d seconds\n", pdc_client_mpi_rank_g, config_fname, sleeptime);
            fflush(stdout);
            sleep(sleeptime);
            sleeptime *= 2;
        }
        if (is_server_ready != 1) {
            ret_value = FAIL;
            goto done;
        }

        na_config = fopen(config_fname, "r");
        if (!na_config) {
            fprintf(stderr, "Could not open config file from default location: %s\n", config_fname);
            ret_value = FAIL;
            goto done;
        }
        char n_server_string[ADDR_MAX];
        // Get the first line as $pdc_server_num_g
        fgets(n_server_string, ADDR_MAX, na_config);
        pdc_server_num_g = atoi(n_server_string);
    }

#ifdef ENABLE_MPI
     MPI_Bcast(&pdc_server_num_g, 1, MPI_INT, 0, MPI_COMM_WORLD);
     /* printf("[%d]: received server number %d\n", pdc_client_mpi_rank_g, pdc_server_num_g); */
     /* fflush(stdout); */
#endif

     if (pdc_server_num_g == 0) {
         printf("==PDC_CLIENT[%d]: server number error %d\n", pdc_client_mpi_rank_g, pdc_server_num_g);
         return -1;
     }

    // Allocate $pdc_server_info_g
    pdc_server_info_g = (pdc_server_info_t*)calloc(sizeof(pdc_server_info_t), pdc_server_num_g);

    i = 0;
    while (i < pdc_server_num_g) {
        if (pdc_client_mpi_rank_g == 0) {
            fgets(pdc_server_info_g[i].addr_string, ADDR_MAX, na_config);
            p = strrchr(pdc_server_info_g[i].addr_string, '\n');
            if (p != NULL) *p = '\0';
            /* printf("Read from config file [%s]\n", pdc_server_info_g[i].addr_string); */
            /* fflush(stdout); */
        }

        #ifdef ENABLE_MPI
        MPI_Bcast(pdc_server_info_g[i].addr_string, ADDR_MAX, MPI_CHAR, 0, MPI_COMM_WORLD);
        /* printf("[%d]: received server addr [%s]\n", pdc_client_mpi_rank_g, &pdc_server_info_g[i].addr_string);      */
        /* fflush(stdout); */
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
client_send_object_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct object_unmap_args *object_unmap_args;
    obj_unmap_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_send_object_unmap_rpc_cb()\n"); */
    object_unmap_args = (struct object_unmap_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: client_send_object_unmap_rpc_cb error with HG_Get_output\n", 
                pdc_client_mpi_rank_g);
        object_unmap_args->ret = -1;
        goto done;
    }

    /* printf("Return value=%d\n", output.ret); */

    object_unmap_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_buf_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct region_unmap_args *region_unmap_args;
    reg_unmap_out_t output;

    FUNC_ENTER(NULL);

    /* printf("Entered client_send_buf_unmap_rpc_cb()\n"); */
    region_unmap_args = (struct region_unmap_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: client_send_buf_unmap_rpc_cb error with HG_Get_output\n",
                pdc_client_mpi_rank_g);
        region_unmap_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%d\n", output.ret); */

    region_unmap_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
//    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_region_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct region_unmap_args *region_unmap_args;
    reg_unmap_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_send_region_unmap_rpc_cb()\n"); */
    region_unmap_args = (struct region_unmap_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: client_send_region_unmap_rpc_cb error with HG_Get_output\n", 
                pdc_client_mpi_rank_g);
        region_unmap_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%d\n", output.ret); */

    region_unmap_args->ret = output.ret;


done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_buf_map_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct buf_map_args *buf_map_args;
    buf_map_out_t output;

    FUNC_ENTER(NULL);

    /* printf("Entered client_send_buf_map_rpc_cb()\n"); */
    buf_map_args = (struct region_map_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: client_send_buf_map_rpc_cb error with HG_Get_output\n",
                pdc_client_mpi_rank_g);
        buf_map_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%d\n", output.ret); */

    buf_map_args->ret = output.ret;

done:
    work_todo_g = 0;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_send_region_map_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct region_map_args *region_map_args;
    reg_map_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_send_region_map_rpc_cb()\n"); */
    region_map_args = (struct region_map_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: client_send_region_map_rpc_cb error with HG_Get_output\n", 
                pdc_client_mpi_rank_g);
        region_map_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%d\n", output.ret); */

    region_map_args->ret = output.ret;

done:
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

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    client_test_connect_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: %s - error with HG_Get_output\n", pdc_client_mpi_rank_g, __func__);
        client_lookup_args->ret = -1;
        goto done;
    }

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT[%d]: %s - return from server %d\n", pdc_client_mpi_rank_g, __func__, output.ret);
        fflush(stdout);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g = 0;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
} // End of client_test_connect_rpc_cb 

// Callback function for HG_Addr_lookup()
// Start RPC connection
static hg_return_t
client_test_connect_lookup_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    uint32_t server_id;
    struct client_lookup_args *client_lookup_args;
    client_test_connect_in_t in;
    hg_handle_t client_test_handle;
    
    FUNC_ENTER(NULL);

    client_lookup_args = (struct client_lookup_args *) callback_info->arg;
    server_id = client_lookup_args->server_id;
    /* if (is_client_debug_g == 1) { */
    /*     printf("%s - server ID=%d\n", __func__, server_id); */
    /*     fflush(stdout); */
    /* } */
    pdc_server_info_g[server_id].addr = callback_info->info.lookup.addr;
    pdc_server_info_g[server_id].addr_valid = 1;

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, client_test_connect_register_id_g, 
              &client_test_handle);

    // Fill input structure
    in.client_id   = pdc_client_mpi_rank_g;
    in.nclient     = pdc_client_mpi_size_g;
    in.client_addr = client_lookup_args->client_addr;

    ret_value = HG_Forward(client_test_handle, client_test_connect_rpc_cb, client_lookup_args, &in);
    if (ret_value != HG_SUCCESS) {
        fprintf(stderr, "%s - Could not start HG_Forward\n", __func__);
        goto done;
    }

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);
    /* printf("==PDC_CLIENT[%d]: forwarded lookup rpc to server %d\n", pdc_client_mpi_rank_g, server_id); */
    /* fflush(stdout); */

done:
    ret_value = HG_Destroy(client_test_handle); 
    if (ret_value != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_close_all_server(): Could not destroy handle\n");
    }
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_lookup_server(int server_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    struct client_lookup_args lookup_args;
    char self_addr[ADDR_MAX];
    char *target_addr_string;

    FUNC_ENTER(NULL);

    if (server_id <0 || server_id > pdc_server_num_g) {
        printf("==PDC_CLIENT[%d]: ERROR with server id input %d\n", pdc_client_mpi_rank_g, server_id); 
        ret_value = FAIL;
        goto done;
    }

    ret_value = PDC_get_self_addr(send_class_g, self_addr);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT[%d]: ERROR geting self addr\n", pdc_client_mpi_rank_g); 
        goto done;
    }

    lookup_args.client_id = pdc_client_mpi_rank_g;
    lookup_args.server_id = server_id;
    lookup_args.client_addr = self_addr;
    target_addr_string = pdc_server_info_g[lookup_args.server_id].addr_string;

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT[%d]: - Testing connection to server %d [%s]\n", 
                pdc_client_mpi_rank_g, lookup_args.server_id, target_addr_string);
        fflush(stdout);
    }

    hg_ret = HG_Addr_lookup(send_context_g, client_test_connect_lookup_cb, &lookup_args, 
                            target_addr_string, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS ) {
        printf("==PDC_CLIENT: Connection to server FAILED!\n");
        ret_value = FAIL;
        goto done;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* printf("==PDC_CLIENT[%d]: - connected to server %d\n", pdc_client_mpi_rank_g, lookup_args.server_id); */
    /* fflush(stdout); */

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_try_lookup_server(int server_id)
{
    perr_t ret_value = SUCCEED;
    int    n_retry = 1;
    if (server_id < 0 || server_id > pdc_server_num_g) {
        printf("==CLIENT[%d]: %s - invalid server ID %d\n", pdc_client_mpi_rank_g, __func__, server_id);
        ret_value = FAIL;
        goto done;
    }

    while (pdc_server_info_g[server_id].addr_valid != 1) {
        if (n_retry > PDC_MAX_TRIAL_NUM) 
            break;
        ret_value = PDC_Client_lookup_server(server_id);
        if(ret_value != SUCCEED) {
            printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }
        n_retry++;
    }

done:
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
close_server_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    close_server_out_t output;
    /* struct client_lookup_args *client_lookup_args; */
    
    FUNC_ENTER(NULL);

    /* printf("Entered close_server_cb()\n"); */
    /* fflush(stdout); */
    /* client_lookup_args = (struct client_lookup_args*) callback_info->arg; */
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: close_server_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        goto done;
    }
    /* printf("Return value from server is %" PRIu64 "\n", output.ret); */
    /* fflush(stdout); */

done:
    work_todo_g = 0;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
close_server_cb_from_dataserver(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    close_server_out_t output;
    struct client_lookup_args *args;
    
    FUNC_ENTER(NULL);

    args   = (struct client_lookup_args *)callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: %s - error with HG_Get_output\n", pdc_client_mpi_rank_g, __func__);
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */

    ret_value = HG_Free_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: %s - error with free HG_Get_output\n", pdc_client_mpi_rank_g, __func__);
        goto done;
    }

done:
    fflush(stdout);
    /* work_todo_g = 0; */
    hg_request_complete(args->request);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
client_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct client_lookup_args *client_lookup_args;
    gen_obj_id_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_rpc_cb()\n"); */
    client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: client_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        client_lookup_args->obj_id = 0;
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */
    client_lookup_args->obj_id = output.obj_id;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}


static hg_return_t
client_region_lock_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct region_lock_args *client_lookup_args;
    region_lock_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_region_lock_rpc_cb()\n"); */
    client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("PDC_CLIENT[%d]: %s - error with HG_Get_output\n", pdc_client_mpi_rank_g, __func__);
        client_lookup_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */

    client_lookup_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

static hg_return_t
client_region_release_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct client_lookup_args *client_lookup_args;
    region_lock_out_t output;

    FUNC_ENTER(NULL);

    /* printf("Entered client_region_release_rpc_cb()\n"); */
    client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT: client_region_release_rpc_cb - HG_Get_output error!\n");
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */

    client_lookup_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Bulk
// Callback after bulk transfer is received by client
static hg_return_t
hg_test_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t ret = HG_SUCCESS;
    struct bulk_args_t *bulk_args;
    hg_bulk_t local_bulk_handle;
    /* bulk_write_out_t out_struct; */
    uint32_t i;
    void *buf = NULL;
    uint32_t n_meta;
    size_t buf_sizes[2] = {0,0};
    uint32_t actual_cnt;
    pdc_metadata_t *meta_ptr;
    
    FUNC_ENTER(NULL);
    
    bulk_args = (struct bulk_args_t *)hg_cb_info->arg;
    local_bulk_handle = hg_cb_info->info.bulk.local_handle;

    if (hg_cb_info->ret == HG_CANCELED) {
        printf("HG_Bulk_transfer() was successfully canceled\n");
        /* Fill output structure */
        /* out.ret = 0; */
    } else if (hg_cb_info->ret != HG_SUCCESS) {
        HG_LOG_ERROR("Error in callback");
        ret = HG_PROTOCOL_ERROR;
        goto done;
    }

    /* printf("hg_test_bulk_transfer_cb: n_meta=%u\n", *bulk_args->n_meta); */
    /* fflush(stdout); */
    n_meta = *bulk_args->n_meta;

    if (hg_cb_info->ret == HG_SUCCESS) {
        HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, &buf, buf_sizes, &actual_cnt);
        /* printf("received bulk transfer, buf_sizes: %d %d, actual_cnt=%d\n", buf_sizes[0], buf_sizes[1], actual_cnt); */
        /* fflush(stdout); */

        meta_ptr = (pdc_metadata_t*)(buf);
        bulk_args->meta_arr = (pdc_metadata_t**)malloc(sizeof(pdc_metadata_t*) * n_meta);
        for (i = 0; i < n_meta; i++) {
            bulk_args->meta_arr[i] = meta_ptr;
            /* PDC_print_metadata(meta_ptr); */
            meta_ptr++;
        }

        /* Fill output structure */
        /* out_struct.ret = 1; */
    }

    bulk_todo_g--;
    hg_atomic_set32(&bulk_transfer_done_g, 1);

    // Free block handle
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

    //Send response back
    /* printf("Pulled data, send back confirm message\n"); */
    /* fflush(stdout); */
    /* ret = HG_Respond(bulk_args->handle, NULL, NULL, &out_struct); */
    /* if (ret != HG_SUCCESS) { */
    /*     fprintf(stderr, "Could not respond\n"); */
    /*     return ret; */
    /* } */

done:
    // Don't destroy this handle, will be used for later transfer
    /* HG_Destroy(bulk_args->handle); */
    free(bulk_args);

    return ret;
}

// Bulk
// No need to have multi-threading
int PDC_Client_check_bulk(hg_context_t *hg_context)
{
    int ret_value;
    hg_return_t hg_ret;
    unsigned int actual_count;
    
    FUNC_ENTER(NULL);

    /* Poke progress engine and check for events */
    do {
        actual_count = 0;
        do {
            hg_ret = HG_Trigger(hg_context, 0/* timeout */, 1 /* max count */, &actual_count);
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (bulk_todo_g <= 0)  break;
        hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME);

    } while (hg_ret == HG_SUCCESS);

    if (hg_ret == HG_SUCCESS)
        ret_value = SUCCEED;
    else
        ret_value = FAIL;

    FUNC_LEAVE(ret_value);
}

#ifdef PDC_HAS_CRAY_DRC
/* Convert value to string */
#define DRC_ERROR_STRING_MACRO(def, value, string) \
  if (value == def) string = #def

static const char *
drc_strerror(int errnum)
{
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

    return errstring;
}
#endif

// Init Mercury class and context
// Register gen_obj_id rpc
perr_t PDC_Client_mercury_init(hg_class_t **hg_class, hg_context_t **hg_context, int port)
{
    perr_t ret_value = SUCCEED;
    char na_info_string[ADDR_MAX];
    char hostname[1024];
    int local_server_id;
    /* Set the default mercury transport 
     * but enable overriding that to any of:
     *   "ofi+gni"
     *   "ofi+tcp"
     *   "cci+tcp"
     */
    struct hg_init_info init_info = { 0 };
    char *default_hg_transport = "ofi+tcp";
    char *hg_transport;
#ifdef PDC_HAS_CRAY_DRC
    uint32_t credential, cookie;
    drc_info_handle_t credential_info;
    char pdc_auth_key[256] = { '\0' };
    char *auth_key;
    int rc;
#endif
    
    FUNC_ENTER(NULL);

    if ((hg_transport = getenv("HG_TRANSPORT")) == NULL) {
        hg_transport = default_hg_transport;
    }
    memset(hostname, 0, 1024);
    gethostname(hostname, 1023);
    sprintf(na_info_string, "%s://%s:%d", hg_transport, hostname, port);
    /* sprintf(na_info_string, "ofi+gni://%s:%d", hostname, port); */
    /* sprintf(na_info_string, "ofi+tcp://%s:%d", hostname, port); */
    /* sprintf(na_info_string, "cci+tcp://%d", port); */
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: using %.7s\n", na_info_string);
        fflush(stdout);
    }
    
    /* if (!na_info_string) { */
    /*     fprintf(stderr, HG_PORT_NAME " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"cci+tcp://22222\"\n"); */
    /*     exit(0); */
    /* } */

// gni starts here
#ifdef PDC_HAS_CRAY_DRC
    /* Acquire credential */
    if (pdc_client_mpi_rank_g == 0) {
        credential = atoi(getenv("PDC_DRC_KEY")); 
    }
    MPI_Bcast(&credential, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);
    
    printf("# Credential is %u\n", credential);
    fflush(stdout);
    rc = drc_access(credential, 0, &credential_info);
    if (rc != DRC_SUCCESS) { /* failed to access credential */
        printf("drc_access() failed (%d, %s)", rc,
            drc_strerror(-rc));
        fflush(stdout);
        ret_value = FAIL;
        goto done;
    }
    cookie = drc_get_first_cookie(credential_info);

    printf("# Cookie is %u\n", cookie);
    fflush(stdout);
    sprintf(pdc_auth_key, "%u", cookie);
    init_info.na_init_info.auth_key = strdup(pdc_auth_key);
#endif
// gni ends

    /* Initialize Mercury with the desired network abstraction class */
    /* printf("Using %s\n", na_info_string); */
//    *hg_class = HG_Init(na_info_string, HG_TRUE);
    init_info.na_init_info.progress_mode = NA_NO_BLOCK;
    init_info.auto_sm = HG_TRUE;
    *hg_class = HG_Init_opt(na_info_string, HG_TRUE, &init_info);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        goto done;
    }

    /* Create HG context */
    *hg_context = HG_Context_create(*hg_class);

    // Init mercury request class
    request_class_g = hg_request_init(mercury_request_progress, mercury_request_trigger, send_context_g);
    /* request_class_g = hg_request_init(hg_hl_request_progress, hg_hl_request_trigger, NULL); */


    // Register RPC
    client_test_connect_register_id_g         = client_test_connect_register(*hg_class);
    gen_obj_register_id_g                     = gen_obj_id_register(*hg_class);
    gen_cont_register_id_g                    = gen_cont_id_register(*hg_class);
    close_server_register_id_g                = close_server_register(*hg_class);
    metadata_query_register_id_g              = metadata_query_register(*hg_class);
    metadata_delete_register_id_g             = metadata_delete_register(*hg_class);
    metadata_delete_by_id_register_id_g       = metadata_delete_by_id_register(*hg_class);
    metadata_update_register_id_g             = metadata_update_register(*hg_class);
    metadata_add_tag_register_id_g            = metadata_add_tag_register(*hg_class);
    region_lock_register_id_g                 = region_lock_register(*hg_class);
    region_release_register_id_g              = region_release_register(*hg_class);
    data_server_read_register_id_g            = data_server_read_register(*hg_class);
    data_server_read_check_register_id_g      = data_server_read_check_register(*hg_class);
    data_server_write_check_register_id_g     = data_server_write_check_register(*hg_class);
    data_server_write_register_id_g           = data_server_write_register(*hg_class);
    server_checkpoint_rpc_register_id_g       = server_checkpoing_rpc_register(*hg_class);

    // bulk
    query_partial_register_id_g               = query_partial_register(*hg_class);

    cont_add_del_objs_rpc_register_id_g       = cont_add_del_objs_rpc_register(*hg_class);
    query_read_obj_name_register_id_g         = query_read_obj_name_rpc_register(*hg_class);

    // 
    buf_map_register_id_g                     = buf_map_register(*hg_class);
    reg_map_register_id_g                     = reg_map_register(*hg_class);
    buf_unmap_register_id_g                   = buf_unmap_register(*hg_class);
    reg_unmap_register_id_g                   = reg_unmap_register(*hg_class);
    obj_unmap_register_id_g                   = obj_unmap_register(*hg_class);

    server_lookup_client_register(*hg_class);
    notify_io_complete_register(*hg_class);
    notify_region_update_register(*hg_class);
    notify_client_multi_io_complete_rpc_register(*hg_class);
   
    // Client 0 looks up all servers, others only lookup their node local server
    char *client_lookup_env = getenv("PDC_CLIENT_LOOKUP");
    if (client_lookup_env != NULL && strcmp(client_lookup_env, "ALL") == 0) {
        if (pdc_client_mpi_rank_g == 0) 
            printf("==PDC_CLIENT[%d]: Client lookup server at start time disabled!\n", pdc_client_mpi_rank_g);
    }
    else {
        // Each client connect to its node local server only at start time
        local_server_id = pdc_client_mpi_rank_g/pdc_nclient_per_server_g;
        if (PDC_Client_try_lookup_server(local_server_id) != SUCCEED) {
            printf("==PDC_CLIENT[%d]: ERROR lookup server %d\n", pdc_client_mpi_rank_g, local_server_id);
            ret_value = FAIL;
            goto done;
        }
    }

/*
#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
*/
    /* if (is_client_debug_g == 1) { */
    if (is_client_debug_g == 1 && pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT[%d]: Successfully established connection to %d PDC metadata server%s\n\n", 
                pdc_client_mpi_rank_g, pdc_server_num_g, pdc_client_mpi_size_g == 1 ? "": "s");
        fflush(stdout);
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_init()
{
    perr_t ret_value = SUCCEED;
    pdc_server_info_g = NULL;
    char *tmp_dir;
    uint32_t port;
    int same_node_color, is_mpi_init = 0;

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

    pdc_client_mpi_rank_g       = 0;
    pdc_client_mpi_size_g       = 1;

    pdc_client_same_node_rank_g = 0;
    pdc_client_same_node_size_g = 1;

#ifdef ENABLE_MPI
    MPI_Initialized(&is_mpi_init);
    if (is_mpi_init != 1)
        MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_client_mpi_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_client_mpi_size_g);
#endif

    if (pdc_client_mpi_rank_g == 0)
        printf("==PDC_CLIENT: PDC_DEBUG set to %d!\n", is_client_debug_g);


    // get server address and fill in $pdc_server_info_g
    if (PDC_Client_read_server_addr_from_file() != SUCCEED) {
        printf("==PDC_CLIENT[%d]: Error getting PDC Metadata servers info, exiting ...", pdc_server_num_g);
        exit(0);
    }
    // Get the number of clients per server(node) through environment variable
    tmp_dir = getenv("PDC_NCLIENT_PER_SERVER");
    if (tmp_dir == NULL)
        pdc_nclient_per_server_g = pdc_client_mpi_size_g / pdc_server_num_g;
    else
        pdc_nclient_per_server_g = atoi(tmp_dir);

    if (pdc_nclient_per_server_g <= 0)
        pdc_nclient_per_server_g = 1;


#ifdef ENABLE_MPI
    // TODO: split based on hostname, use code from TAPIOCA
    // Split the MPI_COMM_WORLD communicator
    same_node_color = pdc_client_mpi_rank_g / pdc_nclient_per_server_g;
    MPI_Comm_split(MPI_COMM_WORLD, same_node_color, pdc_client_mpi_rank_g, &PDC_SAME_NODE_COMM_g);

    MPI_Comm_rank(PDC_SAME_NODE_COMM_g, &pdc_client_same_node_rank_g );
    MPI_Comm_size(PDC_SAME_NODE_COMM_g, &pdc_client_same_node_size_g );

    /* printf("==PDC_CLIENT[%d]: color %d, key %d, node rank %d!\n", */
    /*         pdc_client_mpi_rank_g, same_node_color, pdc_client_mpi_rank_g, pdc_client_same_node_rank_g); */

#endif

    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: Found %d PDC Metadata servers, running with %d PDC clients\n",
                pdc_server_num_g ,pdc_client_mpi_size_g);
    }

    // Init debug info
    if (pdc_server_num_g > 0) {
        debug_server_id_count = (int*)malloc(sizeof(int) * pdc_server_num_g);
        memset(debug_server_id_count, 0, sizeof(int) * pdc_server_num_g);
    }
    else
        printf("==PDC_CLIENT: Server number not properly initialized!\n");

    // Do we want local server mode?
    /* int   tmp_env; */
    /* char *tmp= NULL; */
    /* tmp = getenv("PDC_USE_LOCAL_SERVER"); */
    /* if (tmp != NULL) { */
    /*     tmp_env = atoi(tmp); */
    /*     if (tmp_env == 1) { */
    /*         pdc_use_local_server_only_g = 1; */
    /*         if(pdc_client_mpi_rank_g == 0) */
    /*             printf("==PDC_CLIENT: Contact local server only!\n"); */
    /*     } */
    /* } */

    // Cori KNL has 68 cores per node, Haswell 32
    port = pdc_client_mpi_rank_g % PDC_MAX_CORE_PER_NODE + 8000;
    if (mercury_has_init_g == 0) {
        // Init Mercury network connection
        ret_value = PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
        if (ret_value != SUCCEED || send_class_g == NULL || send_context_g == NULL) {
            printf("Error with Mercury Init, exiting...\n");
            ret_value = FAIL;
            goto done;
        }
        mercury_has_init_g = 1;
    }

    /* if (pdc_client_mpi_size_g / pdc_server_num_g > pdc_nclient_per_server_g) */
    /*     pdc_nclient_per_server_g = pdc_client_mpi_size_g / pdc_server_num_g; */
    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT[%d]: using [%s] as tmp dir, %d clients per server\n",
                pdc_client_mpi_rank_g, pdc_client_tmp_dir_g, pdc_nclient_per_server_g);
    }

    srand(time(NULL));

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_destroy_all_handles(pdc_server_info_t *server_info)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_finalize()
{
    hg_return_t hg_ret;
    perr_t ret_value = SUCCEED;
    int i;
    
    FUNC_ENTER(NULL);

    // Send close server request to all servers
    /* if (pdc_client_mpi_rank_g == 0) */ 
    /*      PDC_Client_close_all_server(); */ 

    hg_request_finalize(request_class_g, NULL);


    // Finalize Mercury
    for (i = 0; i < pdc_server_num_g; i++) {
        if (pdc_server_info_g[i].addr_valid) {
            HG_Addr_free(send_class_g, pdc_server_info_g[i].addr);
            pdc_server_info_g[i].addr_valid = 0;
        }
    }

    hg_ret = HG_Context_destroy(send_context_g);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: PDC_Client_finalize - error with HG_Context_destroy\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }
    hg_ret = HG_Finalize(send_class_g);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: PDC_Client_finalize - error with HG_Finalize\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_server_info_g != NULL)
        free(pdc_server_info_g);

    // Free client hash table
    /* if (obj_names_cache_hash_table_g != NULL) */ 
    /*     hg_hash_table_free(obj_names_cache_hash_table_g); */

#ifdef ENABLE_MPI

/*     // Print local server connection count */
/*     for (i = 0; i < pdc_server_num_g; i++) */ 
/*         printf("%d, %d, %d\n", pdc_client_mpi_rank_g, i, debug_server_id_count[i]); */
/*     fflush(stdout); */

    /* int *all_server_count = (int*)malloc(sizeof(int)*pdc_server_num_g); */
    /* MPI_Reduce(debug_server_id_count, all_server_count, pdc_server_num_g, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD); */
    /* if (pdc_client_mpi_rank_g == 0 && all_server_count[0] != 0) { */
    /*     printf("==PDC_CLIENT: server connection count:\n"); */
    /*     for (i = 0; i < pdc_server_num_g; i++) */ 
    /*         printf("  Server[%3d], %d\n", i, all_server_count[i]); */
    /* } */
    /* free(all_server_count); */
#else
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


#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
done:
    if (is_client_debug_g == 1 && pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: finalized\n");
    }
    FUNC_LEAVE(ret_value);
}

// Bulk
static hg_return_t
metadata_query_bulk_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    struct bulk_args_t *client_lookup_args;
    hg_handle_t handle;
    metadata_query_transfer_out_t output;
    uint32_t n_meta;
    hg_op_id_t hg_bulk_op_id;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t origin_bulk_handle = HG_BULK_NULL;
    const struct hg_info *hg_info = NULL;
    struct bulk_args_t *bulk_args;
    void *recv_meta;
    
    FUNC_ENTER(NULL);

    /* printf("Entered metadata_query_bulk_cb()\n"); */
    client_lookup_args = (struct bulk_args_t*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    // Get output from server
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: metadata_query_bulk_cb - error HG_Get_output\n", pdc_client_mpi_rank_g);
        goto done;
    }

    /* printf("==PDC_CLIENT: Received response from server with bulk handle, n_buf=%d\n", output.ret); */
    n_meta = output.ret;
    client_lookup_args->n_meta = (uint32_t*)malloc(sizeof(uint32_t));
    *(client_lookup_args->n_meta) = n_meta;
    if (n_meta == 0) {
        client_lookup_args->meta_arr = NULL;
        goto done;
    }

    // We have received the bulk handle from server (server uses hg_respond)
    // Use this to initiate a bulk transfer

    origin_bulk_handle = output.bulk_handle;
    hg_info = HG_Get_info(handle);

    bulk_args = (struct bulk_args_t *) malloc(sizeof(struct bulk_args_t));

    bulk_args->handle = handle;
    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->n_meta = client_lookup_args->n_meta;

    /* printf("nbytes=%u\n", bulk_args->nbytes); */
    recv_meta = (void *)malloc(sizeof(pdc_metadata_t) * n_meta);

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, (void**)&recv_meta, (hg_size_t *) &bulk_args->nbytes, HG_BULK_READWRITE, &local_bulk_handle);

    /* Pull bulk data */
    ret_value = HG_Bulk_transfer(hg_info->context, hg_test_bulk_transfer_cb,
        bulk_args, HG_BULK_PULL, hg_info->addr, origin_bulk_handle, 0,
        local_bulk_handle, 0, bulk_args->nbytes, &hg_bulk_op_id);
    if (ret_value!= HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        goto done;
    }

    // loop
    bulk_todo_g = 1;
    PDC_Client_check_bulk(send_context_g);

    /* printf("Print metadata after PDC_Client_check_bulk()\n"); */
    /* PDC_print_metadata(bulk_args->meta_arr[0]); */
    client_lookup_args->meta_arr = bulk_args->meta_arr;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// bulk test 
perr_t PDC_Client_list_all(int *n_res, pdc_metadata_t ***out)
{
    perr_t ret_value;

    FUNC_ENTER(NULL);
    
    ret_value = PDC_partial_query(1, -1, NULL, NULL, -1, -1, -1, NULL, n_res, out);

    FUNC_LEAVE(ret_value);
}

perr_t PDC_partial_query(int is_list_all, int user_id, const char* app_name, const char* obj_name, 
                         int time_step_from, int time_step_to, int ndim, const char* tags, int *n_res, 
                         pdc_metadata_t ***out)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    int n_recv = 0;
    uint32_t i, server_id = 0, my_server_start, my_server_end, my_server_count;
    size_t out_size = 0;
    hg_handle_t query_partial_handle;

    // Fill input structure
    metadata_query_transfer_in_t in;
    in.is_list_all = is_list_all;
    in.user_id = -1;
    in.app_name = " ";
    in.obj_name = " ";
    in.time_step_from = -1;
    in.time_step_to = -1;
    in.ndim = -1;
    in.tags = " ";

    if (is_list_all != 1) {
        in.user_id = user_id;
        in.ndim = ndim;
        in.time_step_from = time_step_from;
        in.time_step_to = time_step_to;
        if (app_name != NULL) 
            in.app_name = app_name;
        if (obj_name != NULL) 
            in.obj_name = obj_name;
        if (tags != NULL) 
            in.tags = tags;
    }

    *out = NULL;
    *n_res = 0;

    if (pdc_server_num_g > pdc_client_mpi_size_g) {
        /* printf("less clients than server\n"); */
        my_server_count = pdc_server_num_g / pdc_client_mpi_size_g;
        my_server_start = pdc_client_mpi_rank_g * my_server_count;
        my_server_end = my_server_start + my_server_count;
        if (pdc_client_mpi_rank_g == pdc_client_mpi_size_g - 1) {
            my_server_end += pdc_server_num_g % pdc_client_mpi_size_g;
        }
    }
    else {
        /* printf("more clients than server\n"); */
        my_server_start = pdc_client_mpi_rank_g;
        my_server_end   = my_server_start + 1;
        if (pdc_client_mpi_rank_g >= pdc_server_num_g) {
            my_server_end = -1;
        }
    }

    /* printf("%d: start: %d, end: %d\n", pdc_client_mpi_rank_g, my_server_start, my_server_end); */
    /* fflush(stdout); */

    for (server_id = my_server_start; server_id < my_server_end; server_id++) {
        if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
            printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_partial_register_id_g, 
                            &query_partial_handle);

        /* printf("Sending input to target\n"); */
        struct bulk_args_t lookup_args;
        if (query_partial_handle == NULL) {
            printf("==CLIENT[%d]: Error with query_partial_handle\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Forward(query_partial_handle, metadata_query_bulk_cb, &lookup_args, &in);
        if (hg_ret!= HG_SUCCESS) {
            fprintf(stderr, "PDC_client_list_all(): Could not start HG_Forward()\n");
            ret_value = FAIL;
            goto done;
        }

        hg_atomic_set32(&bulk_transfer_done_g, 0);

        // Wait for response from server
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);

        if ( *(lookup_args.n_meta) == 0) 
            continue;
        
        // We do not have the results ready yet, need to wait.
        while (1) {
            if (hg_atomic_get32(&bulk_transfer_done_g)) break;
            /* printf("waiting for bulk transfer done\n"); */
            /* fflush(stdout); */
        }

        if (*out == NULL) {
            out_size = sizeof(pdc_metadata_t*) * (*(lookup_args.n_meta));
            *out = (pdc_metadata_t**)malloc( out_size );
        }
        else {
            out_size += sizeof(pdc_metadata_t*) * (*(lookup_args.n_meta));
            *out = (pdc_metadata_t**)realloc( *out, out_size );
        }

        *n_res += (*lookup_args.n_meta);
        for (i = 0; i < *lookup_args.n_meta; i++) {
            (*out)[n_recv] = lookup_args.meta_arr[i];
            n_recv++;
        }
        /* printf("Received %u metadata from server %d\n", *lookup_args.n_meta, server_id); */

        HG_Destroy(query_partial_handle);
    } // for server_id

    
    /* printf("Received %u metadata.\n", *(lookup_args.n_meta)); */
    /* for (i = 0; i < *n_res; i++) { */
    /*     PDC_print_metadata(lookup_args.meta_arr[i]); */
    /* } */

    // TODO: need to be careful when freeing the lookup_args, as it include the results returned to user
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_query_tag(const char* tags, int *n_res, pdc_metadata_t ***out)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    int n_recv = 0;
    uint32_t i, server_id = 0;
    size_t out_size = 0;
    hg_handle_t query_partial_handle;

    FUNC_ENTER(NULL);

    if (tags == NULL) {
        printf("==CLIENT[%d]: input tag is NULL!\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    // Fill input structure
    metadata_query_transfer_in_t in;
    in.is_list_all = 0;
    in.user_id = -1;
    in.app_name = " ";
    in.obj_name = " ";
    in.time_step_from = -1;
    in.time_step_to = -1;
    in.ndim = 0;
    in.tags = tags;

    *out = NULL;
    *n_res = 0;

    /* printf("%d: start: %d, end: %d\n", pdc_client_mpi_rank_g, my_server_start, my_server_end); */
    /* fflush(stdout); */

    for (server_id = 0; server_id < pdc_server_num_g; server_id++) {
        if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
            printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_partial_register_id_g, 
                            &query_partial_handle);

        /* printf("Sending input to target\n"); */
        struct bulk_args_t lookup_args;
        if (query_partial_handle == NULL) {
            printf("==CLIENT[%d]: Error with query_partial_handle\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Forward(query_partial_handle, metadata_query_bulk_cb, &lookup_args, &in);
        if (hg_ret!= HG_SUCCESS) {
            fprintf(stderr, "PDC_client_list_all(): Could not start HG_Forward()\n");
            ret_value = FAIL;
            goto done;
        }

        hg_atomic_set32(&bulk_transfer_done_g, 0);

        // Wait for response from server
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);

        if ( *(lookup_args.n_meta) == 0) 
            continue;
        
        // We do not have the results ready yet, need to wait.
        while (1) {
            if (hg_atomic_get32(&bulk_transfer_done_g)) break;
            /* printf("waiting for bulk transfer done\n"); */
            /* fflush(stdout); */
        }

        if (*out == NULL) {
            out_size = sizeof(pdc_metadata_t*) * (*(lookup_args.n_meta));
            *out = (pdc_metadata_t**)malloc( out_size );
        }
        else {
            out_size += sizeof(pdc_metadata_t*) * (*(lookup_args.n_meta));
            *out = (pdc_metadata_t**)realloc( *out, out_size );
        }

        *n_res += (*lookup_args.n_meta);
        for (i = 0; i < *lookup_args.n_meta; i++) {
            (*out)[n_recv] = lookup_args.meta_arr[i];
            n_recv++;
        }
        /* printf("Received %u metadata from server %d\n", *lookup_args.n_meta, server_id); */

        HG_Destroy(query_partial_handle);
    } // for server_id

    
    /* printf("Received %u metadata.\n", *(lookup_args.n_meta)); */
    /* for (i = 0; i < *n_res; i++) { */
    /*     PDC_print_metadata(lookup_args.meta_arr[i]); */
    /* } */

    // TODO: need to be careful when freeing the lookup_args, as it include the results returned to user
done:
    FUNC_LEAVE(ret_value);
} // end PDC_Client_query_tag

// Gets executed after a receving queried metadata from server
static hg_return_t
metadata_query_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    metadata_query_args_t *client_lookup_args;
    hg_handle_t handle;
    metadata_query_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered metadata_query_rpc_cb()\n"); */
    client_lookup_args = (struct metadata_query_args_t*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: metadata_query_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        goto done;
    }

    if (output.ret.user_id == -1 && output.ret.obj_id == 0 && output.ret.time_step == -1) {
        /* printf("===PDC_CLIENT[%d]: got empty search response!\n", pdc_client_mpi_rank_g); */
        /* fflush(stdout); */
        client_lookup_args->data = NULL;
    }
    else {
        /* printf("===PDC_CLIENT[%d]: got valid search response for %s!\n", pdc_client_mpi_rank_g, output.ret.obj_name); */
        /* fflush(stdout); */
        client_lookup_args->data = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
        if (client_lookup_args->data == NULL) {
            printf("==PDC_CLIENT[%d]: %s - cannnot allocate space for client_lookup_args->data\n", 
                    pdc_client_mpi_rank_g, __func__);
        }

        // Now copy the received metadata info
        ret_value = PDC_metadata_init(client_lookup_args->data);
        ret_value = pdc_transfer_t_to_metadata_t(&output.ret, client_lookup_args->data);
    }

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}


// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
metadata_delete_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    struct client_lookup_args *client_lookup_args;
    hg_handle_t handle;
    metadata_delete_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered metadata_delete_rpc_cb()\n"); */
    client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: metadata_delete_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */
    client_lookup_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

static hg_return_t
metadata_delete_by_id_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    struct client_lookup_args *client_lookup_args;
    hg_handle_t handle;
    metadata_delete_by_id_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered metadata_delete_by_id_rpc_cb()\n"); */
    client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: metadata_delete_by_id_rpc_cb error with HG_Get_output\n", 
                pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */
    client_lookup_args->ret = output.ret;

done:
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

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    metadata_add_tag_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: metadata_add_tag_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */
    client_lookup_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_add_tag(pdc_metadata_t *old, const char *tag)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    hg_handle_t  metadata_add_tag_handle;

    uint32_t hash_name_value = PDC_get_hash_by_name(old->obj_name);
    uint32_t server_id = (hash_name_value + old->time_step);
    server_id %= pdc_server_num_g;

    /* printf("==PDC_CLIENT: PDC_Client_add_tag_metadata() - hash(%s)=%u\n", old->obj_name, hash_name_value); */

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_add_tag_register_id_g, 
                &metadata_add_tag_handle);

    // Fill input structure
    metadata_add_tag_in_t in;
    in.obj_id     = old->obj_id;
    in.hash_value = hash_name_value;

    if (tag != NULL && tag[0] != 0) {
        in.new_tag            = tag;
    }
    else {
        printf("PDC_Client_add_tag(): invalid tag content!\n");
        fflush(stdout);
        goto done;
    }


    /* printf("Sending input to target\n"); */
    struct client_lookup_args lookup_args;
    hg_ret = HG_Forward(metadata_add_tag_handle, metadata_add_tag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_add_tag_metadata_with_name(): Could not start HG_Forward()\n");
        return FAIL;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1) 
        printf("PDC_CLIENT: add tag NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_add_tag_handle);
    FUNC_LEAVE(ret_value);
}


// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
static hg_return_t
metadata_update_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    struct client_lookup_args *client_lookup_args;
    hg_handle_t handle;
    metadata_update_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered metadata_update_rpc_cb()\n"); */
    client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: metadata_update_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */
    client_lookup_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_update_metadata(pdc_metadata_t *old, pdc_metadata_t *new)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    int hash_name_value;
    uint32_t server_id = 0;
    metadata_update_in_t in;
    struct client_lookup_args lookup_args;
    hg_handle_t metadata_update_handle;
    
    FUNC_ENTER(NULL);

    if (old == NULL || new == NULL) {
        ret_value = FAIL;
        printf("==PDC_CLIENT: PDC_Client_update_metadata() - NULL inputs!\n");
        goto done;
    }

    hash_name_value = PDC_get_hash_by_name(old->obj_name);
    server_id = (hash_name_value + old->time_step);
    server_id %= pdc_server_num_g;

    /* printf("==PDC_CLIENT: PDC_Client_update_metadata() - hash(%s)=%u\n", old->obj_name, hash_name_value); */

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_update_register_id_g, 
                &metadata_update_handle);

    // Fill input structure
    in.obj_id     = old->obj_id;
    in.hash_value = hash_name_value;

    in.new_metadata.user_id = 0;
    in.new_metadata.time_step= -1;
    in.new_metadata.obj_id  = 0;
    in.new_metadata.obj_name= old->obj_name;

    if (new->data_location == NULL ||new->data_location[0] == 0) 
        in.new_metadata.data_location   = " ";
    else 
        in.new_metadata.data_location   = new->data_location;

    if (new->app_name == NULL ||new->app_name[0] == 0)
        in.new_metadata.app_name        = " ";
    else 
        in.new_metadata.app_name        = new->app_name;

    if (new->tags == NULL || new->tags[0] == 0 || old->tags == new->tags) 
        in.new_metadata.tags            = " ";
    else 
        in.new_metadata.tags            = new->tags;

    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(metadata_update_handle, metadata_update_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_update_metadata_with_name(): Could not start HG_Forward()\n");
        return FAIL;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1) 
        printf("PDC_CLIENT: update NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_update_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_delete_metadata_by_id(uint64_t obj_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    // Fill input structure
    metadata_delete_by_id_in_t in;
    uint32_t server_id;
    struct client_lookup_args lookup_args;
    hg_handle_t metadata_delete_by_id_handle;
    
    FUNC_ENTER(NULL);
    
    in.obj_id = obj_id;
    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_num_g);

    /* if (pdc_client_mpi_rank_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: PDC_Client_delete_by_id_metadata() server_id %d\n", pdc_client_mpi_rank_g, server_id); */
        fflush(stdout);
    /* } */

    // Debug statistics for counting number of messages sent to each server.
    if (server_id >= (uint32_t)pdc_server_num_g) {
        printf("Error with server id: %u\n", server_id);
        fflush(stdout);
        goto done;
    }
    else 
        debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_by_id_register_id_g, 
                       &metadata_delete_by_id_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_delete_by_id_metadata_with_name(): Could not create handle\n");
        return FAIL;
    }

    hg_ret = HG_Forward(metadata_delete_by_id_handle, metadata_delete_by_id_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_delete_by_id_metadata_with_name(): Could not start HG_Forward()\n");
        return FAIL;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* if (pdc_client_mpi_rank_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: PDC_Client_delete_by_id_metadata() got response from server %d\n", pdc_client_mpi_rank_g, lookup_args.ret); */
    /*     fflush(stdout); */
    /* } */

    if (lookup_args.ret != 1) 
        printf("PDC_CLIENT: delete_by_id NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_delete_by_id_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_delete_metadata(char *delete_name, pdcid_t obj_delete_prop)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    struct PDC_obj_prop *delete_prop;
    metadata_delete_in_t in;
    int hash_name_value;
    uint32_t server_id;
    struct client_lookup_args lookup_args;
    hg_handle_t metadata_delete_handle;
    
    FUNC_ENTER(NULL);

    delete_prop = PDCobj_prop_get_info(obj_delete_prop);
    // Fill input structure
    in.obj_name = delete_name;
    in.time_step = delete_prop->time_step;

    hash_name_value = PDC_get_hash_by_name(delete_name);
    server_id = (hash_name_value + in.time_step);
    server_id %= pdc_server_num_g;

    in.hash_value = hash_name_value;

    /* if (pdc_client_mpi_rank_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: PDC_Client_delete_metadata() - hash(%s)=%u, server_id %d\n", pdc_client_mpi_rank_g, target->obj_name, hash_name_value, server_id); */
    /*     fflush(stdout); */
    /* } */

    // Debug statistics for counting number of messages sent to each server.
    if (server_id >= (uint32_t)pdc_server_num_g) {
        printf("Error with server id: %u\n", server_id);
        fflush(stdout);
        goto done;
    }
    else 
        debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_register_id_g, 
                    &metadata_delete_handle);
    hg_ret = HG_Forward(metadata_delete_handle, metadata_delete_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_delete_metadata_with_name(): Could not start HG_Forward()\n");
        return FAIL;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* if (pdc_client_mpi_rank_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: PDC_Client_delete_metadata() got response from server %d\n", pdc_client_mpi_rank_g, lookup_args.ret); */
    /*     fflush(stdout); */
    /* } */

    if (lookup_args.ret != 1) 
        printf("PDC_CLIENT: delete NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(metadata_delete_handle);
    FUNC_LEAVE(ret_value);
}

// Search metadata using incomplete ID attributes 
// Currently it's only using obj_name, and search from all servers
perr_t PDC_Client_query_metadata_name_only(const char *obj_name, pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    metadata_query_in_t in;
    metadata_query_args_t **lookup_args;
    uint32_t server_id;
    uint32_t i, count = 0;
    hg_handle_t *metadata_query_handle;
    
    FUNC_ENTER(NULL);

    metadata_query_handle = (hg_handle_t*)malloc(sizeof(hg_handle_t)*pdc_server_num_g);

    // Fill input structure
    in.obj_name   = obj_name;
    in.hash_value = PDC_get_hash_by_name(obj_name);

    /* printf("==PDC_CLIENT: partial search obj_name [%s], hash value %u\n", in.obj_name, in.hash_value); */

    /* printf("Sending input to target\n"); */
    lookup_args = (metadata_query_args_t**)malloc(sizeof(metadata_query_args_t*) * pdc_server_num_g);

    // client_lookup_args->data is a pdc_metadata_t

    for (server_id = 0; server_id < (uint32_t)pdc_server_num_g; server_id++) {
        lookup_args[server_id] = (metadata_query_args_t*)malloc(sizeof(metadata_query_args_t));

        // Debug statistics for counting number of messages sent to each server.
        debug_server_id_count[server_id]++;

        if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
            printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }

        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g, 
                    &metadata_query_handle[server_id]);

        hg_ret = HG_Forward(metadata_query_handle[server_id], metadata_query_rpc_cb, lookup_args[server_id], &in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "PDC_Client_query_metadata_name_only(): Could not start HG_Forward()\n");
            return FAIL;
        }

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
    /* printf("==PDC_CLIENT: Found %d metadata with search\n", count); */


done:
    free(metadata_query_handle);
    FUNC_LEAVE(ret_value);
} // end PDC_Client_query_metadata_name_only

perr_t PDC_Client_query_metadata_name_timestep(const char *obj_name, int time_step, pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    uint32_t hash_name_value;
    uint32_t  server_id;
    metadata_query_in_t in;
    metadata_query_args_t lookup_args;
    hg_handle_t metadata_query_handle;
    
    FUNC_ENTER(NULL);

    /* printf("==PDC_CLIENT[%d]: search request name [%s]\n", pdc_client_mpi_rank_g, obj_name); */
    /* fflush(stdout); */

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(obj_name);
    /* uint32_t hash_name_value = PDC_get_hash_by_name(obj_name); */
    server_id = (hash_name_value + time_step);
    server_id %= pdc_server_num_g;

    /* printf("==PDC_CLIENT[%d]: processed request server id %u\n", pdc_client_mpi_rank_g, server_id); */
    /* fflush(stdout); */

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g, 
              &metadata_query_handle);

    // Fill input structure
    in.obj_name   = obj_name;
    in.hash_value = PDC_get_hash_by_name(obj_name);
    in.time_step  = time_step;

/* // Debug */                                                                                                         
/* volatile int dbg_sleep_g = 1; */
/* if (pdc_client_mpi_rank_g == 1) { */
/*     char hostname[128]; */
/*     gethostname(hostname, 127); */
/*     dbg_sleep_g = 1; */
/*     printf("== %s attach %d\n", hostname, getpid()); */
/*     fflush(stdout); */
/*     while(dbg_sleep_g ==1) { */
/*         dbg_sleep_g = 1; */
/*         sleep(1); */
/*     } */
/* } */

    /* printf("==PDC_CLIENT[%d]: search request obj_name [%s], hash value %u, server id %u\n", pdc_client_mpi_rank_g, in.obj_name, in.hash_value, server_id); */
    /* fflush(stdout); */
    /* printf("Sending input to target\n"); */
    // client_lookup_args->data is a pdc_metadata_t
    lookup_args.data = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    if (lookup_args.data == NULL) {
        printf("==PDC_CLIENT: ERROR - PDC_Client_query_metadata_with_name() "
                "cannnot allocate space for client_lookup_args->data \n");
    }
    hg_ret = HG_Forward(metadata_query_handle, metadata_query_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d] - PDC_Client_query_metadata_with_name(): Could not start HG_Forward()\n",
                pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* printf("==PDC_CLIENT[%d]: received query result name [%s], hash value %u\n", pdc_client_mpi_rank_g, in.obj_name, in.hash_value); */
    *out = lookup_args.data;


done:
    HG_Destroy(metadata_query_handle);
    FUNC_LEAVE(ret_value);
}

// Only let one process per node to do the actual query, then broadcast to all others
perr_t PDC_Client_query_metadata_name_timestep_agg(const char *obj_name, int time_step, pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;

#ifdef ENABLE_MPI
    if (pdc_client_same_node_rank_g == 0) {

        /* printf("==PDC_CLIENT[%d] - agg query, local rank: %d\n", */ 
        /*             pdc_client_mpi_rank_g, pdc_client_same_node_rank_g); */

        ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out);
        if (ret_value != SUCCEED || NULL == *out) {
            printf("==PDC_CLIENT[%d]: %s - ERROR with query [%s]\n", pdc_client_mpi_rank_g, __func__, obj_name);
            fflush(stdout);
            *out = (pdc_metadata_t*)calloc(1, sizeof(pdc_metadata_t));
            ret_value = FAIL;
        }
        /* PDC_print_metadata(*out); */
    }
    else    
        *out = (pdc_metadata_t*)calloc(1, sizeof(pdc_metadata_t));

    MPI_Bcast(*out, sizeof(pdc_metadata_t), MPI_CHAR, 0, PDC_SAME_NODE_COMM_g);

#else

    ret_value = PDC_Client_query_metadata_name_timestep(obj_name, time_step, out);

#endif

done:
    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an container (object) id
perr_t PDC_Client_create_cont_id(const char *cont_name, pdcid_t cont_create_prop, pdcid_t *cont_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    uint32_t server_id = 0;
    struct PDC_cont_prop *create_prop;
    gen_cont_id_in_t in;
    uint32_t hash_name_value;
    struct client_lookup_args lookup_args;
    hg_handle_t rpc_handle;
    
    FUNC_ENTER(NULL);
    
    if (cont_name == NULL) {
        printf("Cannot create container with empty name\n");
        goto done;
    }

    // Fill input structure
    memset(&in, 0, sizeof(in));

    hash_name_value = PDC_get_hash_by_name(cont_name);
    in.hash_value      = hash_name_value;
    in.cont_name       = cont_name;
    /* printf("Hash(%s) = %d\n", cont_name, in.hash_value); */

    // Calculate server id
    server_id  = hash_name_value;
    server_id %= pdc_server_num_g; 

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_cont_register_id_g, &rpc_handle);

    hg_ret = HG_Forward(rpc_handle, client_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_send_name_to_server(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == 0) {
        ret_value = FAIL;
        *cont_id = 0;
        goto done;
    }

    /* printf("Received obj_id=%" PRIu64 "\n", lookup_args.obj_id); */

    *cont_id = lookup_args.obj_id;
    ret_value = SUCCEED;


done:
    HG_Destroy(rpc_handle);
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


// Send a name to server and receive an obj id
perr_t PDC_Client_send_name_recv_id(const char *obj_name, uint64_t cont_id, pdcid_t obj_create_prop, pdcid_t *meta_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    uint32_t server_id = 0;
    PDC_lifetime obj_life;
    struct PDC_obj_prop *create_prop;
    gen_obj_id_in_t in;
    uint32_t hash_name_value;
    struct client_lookup_args lookup_args;
    hg_handle_t rpc_handle;
    
    FUNC_ENTER(NULL);
    
    create_prop = PDCobj_prop_get_info(obj_create_prop);
    obj_life  = create_prop->obj_life;

    if (obj_name == NULL) {
        printf("Cannot create object with empty object name\n");
        goto done;
    }

    // Fill input structure
    memset(&in,0,sizeof(in));

    in.data.obj_name  = obj_name;
    in.data.cont_id   = cont_id;
    in.data.time_step = create_prop->time_step;
    in.data.user_id   = create_prop->user_id;
    in.data_type      = create_prop->type;
//printf("data_type = %d\n", in.data_type);
//fflush(stdout);

    if ((in.data.ndim = create_prop->ndim) > 0) {
      if(in.data.ndim >= 1)
          in.data.dims0     = create_prop->dims[0];
      if(in.data.ndim >= 2)
          in.data.dims1     = create_prop->dims[1];
      if(in.data.ndim >= 3)
          in.data.dims2     = create_prop->dims[2];
      if(in.data.ndim >= 4)
          in.data.dims3     = create_prop->dims[3];
    }
   
    if (create_prop->tags == NULL) 
        in.data.tags      = " ";
    else
        in.data.tags      = create_prop->tags;
    if (create_prop->app_name == NULL) 
        in.data.app_name  = "Noname";
    else
        in.data.app_name  = create_prop->app_name;
    if (create_prop->data_loc == NULL) 
        in.data.data_location = " ";
    else 
        in.data.data_location = create_prop->data_loc;

    hash_name_value = PDC_get_hash_by_name(obj_name);
    in.hash_value      = hash_name_value;
    /* printf("Hash(%s) = %lu\n", obj_name, in.hash_value); */

    // Compute server id
    server_id       = (hash_name_value + in.data.time_step);
    server_id      %= pdc_server_num_g; 

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    /* printf("==PDC_CLIENT[%d]: obj_name=%s, user_id=%u, time_step=%u\n", pdc_client_mpi_rank_g, lookup_args.obj_name, lookup_args.user_id, lookup_args.time_step); */

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_register_id_g, &rpc_handle);

    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(rpc_handle, client_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_send_name_to_server(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }
    /* printf("After sending input to target\n"); */

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Now we have obj id stored in lookup_args.obj_id
    if (lookup_args.obj_id == 0) {
        ret_value = FAIL;
        *meta_id = 0;
        goto done;
    }

    /* printf("Received obj_id=%" PRIu64 "\n", lookup_args.obj_id); */

    *meta_id = lookup_args.obj_id;
    ret_value = SUCCEED;


done:
    HG_Destroy(rpc_handle);
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_close_all_server()
{
    uint64_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    uint32_t server_id = 0;
    uint32_t port, i;
    close_server_in_t in;
    struct client_lookup_args lookup_args;
    hg_handle_t close_server_handle;
    
    FUNC_ENTER(NULL);

    port = pdc_client_mpi_rank_g % 32 + 8000;

    if (pdc_client_mpi_rank_g == 0) {
        if (mercury_has_init_g == 0) {
            // Init Mercury network connection
            PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
            if (send_class_g == NULL || send_context_g == NULL) {
                printf("Error with Mercury Init, exiting...\n");
                exit(0);
            }
            mercury_has_init_g = 1;
        }
        for (i = 0; i < (uint32_t)pdc_server_num_g; i++) {
            server_id = i;
            /* printf("Closing server %d\n", server_id); */
            /* fflush(stdout); */

            int n_retry = 0;
            while (pdc_server_info_g[server_id].addr_valid != 1) {
                if (n_retry > 0) 
                    break;
                if( PDC_Client_lookup_server(server_id) != SUCCEED) {
                    printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
                    ret_value = FAIL;
                    goto done;
                }
                n_retry++;
            }

            /* printf("Addr: %s\n", pdc_server_info_g[server_id].addr); */
            /* fflush(stdout); */
            HG_Create(send_context_g, pdc_server_info_g[server_id].addr, close_server_register_id_g, 
                      &close_server_handle);

            // Fill input structure
            in.client_id = 0;

            /* printf("Sending input to target\n"); */
            hg_ret = HG_Forward(close_server_handle, NULL, NULL, &in);
            /* hg_ret = HG_Forward(close_server_handle, close_server_cb, &lookup_args, &in); */
            if (hg_ret != HG_SUCCESS) {
                fprintf(stderr, "PDC_Client_close_all_server(): Could not start HG_Forward()\n");
            }

            // Wait for response from server
            /* work_todo_g = 1; */
            /* PDC_Client_check_response(&send_context_g); */
            hg_ret = HG_Destroy(close_server_handle);
            if (hg_ret != HG_SUCCESS) {
                fprintf(stderr, "PDC_Client_close_all_server(): Could not destroy handle\n");
            }
        }


        if (is_client_debug_g == 1) {
            printf("\n\n\n==PDC_CLIENT: sent finalize request to all servers\n");
            fflush(stdout);
        }
    }// end if mpi_rank == 0

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_close_all_server_dataserver()
{
    uint64_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    uint32_t server_id = 0;
    uint32_t port, i;
    close_server_in_t in;
    struct client_lookup_args forward_args;
    hg_handle_t close_server_handle;
    
    FUNC_ENTER(NULL);


    if (pdc_client_mpi_rank_g == 0) {
        if (mercury_has_init_g == 0) {
            // Init Mercury network connection
            port = pdc_client_mpi_rank_g % 32 + 8000;
            PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
            if (send_class_g == NULL || send_context_g == NULL) {
                printf("Error with Mercury Init, exiting...\n");
                exit(0);
            }
            mercury_has_init_g = 1;
        }
        /* for (i = pdc_server_num_g - 1; i > 0; i--) { */
        for (i = 0; i < (uint32_t)pdc_server_num_g; i++) {
            server_id = i;
            printf("Closing server %d\n", server_id);
            fflush(stdout);

            if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
                printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
                ret_value = FAIL;
                goto done;
            }

            /* printf("Addr: %s\n", pdc_server_info_g[server_id].addr); */
            /* fflush(stdout); */
            HG_Create(send_context_g, pdc_server_info_g[server_id].addr, close_server_register_id_g, 
                      &close_server_handle);

            // Fill input structure
            in.client_id = 0;
            forward_args.request = hg_request_create(request_class_g);

            /* printf("Sending input to target\n"); */
            hg_ret = HG_Forward(close_server_handle, close_server_cb, &forward_args, &in);
            if (hg_ret != HG_SUCCESS) {
                fprintf(stderr, "PDC_Client_close_all_server(): Could not start HG_Forward()\n");
                ret_value = FAIL;
                goto done;
            }

            hg_request_wait(forward_args.request, HG_MAX_IDLE_TIME, NULL);


            // Wait for response from server
            /* work_todo_g = 1; */
            /* PDC_Client_check_response(&send_context_g); */
            if (HG_Destroy(close_server_handle) != HG_SUCCESS) {
                fprintf(stderr, "==PDC_CLIENT[0]: %s - Could not destroy handle\n", __func__);
                hg_request_destroy(forward_args.request);
                ret_value = FAIL;
                goto done;
            }

            hg_request_destroy(forward_args.request);

        }


        if (is_client_debug_g == 1) {
            printf("\n\n\n==PDC_CLIENT: sent finalize request to all servers\n");
            fflush(stdout);
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_object_unmap(pdcid_t local_obj_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    obj_unmap_in_t in;
    /* hg_bulk_t bulk_handle; */
    int n_retry;
    uint32_t server_id;
    struct object_unmap_args unmap_args;
    hg_handle_t client_send_object_unmap_handle;
    
    FUNC_ENTER(NULL);
    
    // Fill input structure
    in.local_obj_id = local_obj_id;

    // Create a bulk descriptor
    /* bulk_handle = HG_BULK_NULL; */

    server_id = PDC_get_server_by_obj_id(local_obj_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, obj_unmap_register_id_g, &client_send_object_unmap_handle);

    hg_ret = HG_Forward(client_send_object_unmap_handle, client_send_object_unmap_rpc_cb, &unmap_args, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "PDC_Client_send_object_unmap(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (unmap_args.ret != 1)
        PGOTO_ERROR(FAIL,"PDC_CLIENT: object unmapping failed...");
    
done:
    HG_Destroy(client_send_object_unmap_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_buf_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id, struct PDC_region_info *reginfo, PDC_var_type_t data_type)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    buf_unmap_in_t in;
    int n_retry;
    size_t unit;
    uint32_t data_server_id, meta_server_id;
    struct region_unmap_args unmap_args;
    hg_handle_t client_send_buf_unmap_handle;

    FUNC_ENTER(NULL); 
  
    // Fill input structure
    in.remote_obj_id = remote_obj_id;
    in.remote_reg_id = remote_reg_id;

    if(data_type == PDC_DOUBLE)
        unit = sizeof(double);
    else if(data_type == PDC_FLOAT)
        unit = sizeof(float);
    else if(data_type == PDC_INT)
        unit = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "data type is not supported yet");
    pdc_region_info_t_to_transfer_unit(reginfo, &(in.remote_region), unit);

    // Compute metadata server id
    meta_server_id = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    // Compute local data server id
    data_server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[data_server_id]++;

    if( PDC_Client_try_lookup_server(data_server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr, buf_unmap_register_id_g, &client_send_buf_unmap_handle);

    hg_ret = HG_Forward(client_send_buf_unmap_handle, client_send_buf_unmap_rpc_cb, &unmap_args, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "PDC_Client_send_buf_unmap(): Could not start HG_Forward()");
    } 

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g); 
    
    if (unmap_args.ret != 1) 
        PGOTO_ERROR(FAIL,"PDC_CLIENT: buf unmap failed...");

done:
    HG_Destroy(client_send_buf_unmap_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_region_unmap(pdcid_t local_obj_id, pdcid_t local_reg_id, struct PDC_region_info *reginfo, PDC_var_type_t data_type)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    reg_unmap_in_t in;
    /* hg_bulk_t bulk_handle; */
    uint32_t server_id;
    size_t unit;
    int n_retry;
    struct region_unmap_args unmap_args;
    hg_handle_t client_send_region_unmap_handle;
    
    FUNC_ENTER(NULL);
    
    // Fill input structure
    in.local_obj_id = local_obj_id;
    in.local_reg_id = local_reg_id;
   
    if(data_type == PDC_DOUBLE)
        unit = sizeof(double);
    else if(data_type == PDC_FLOAT)
        unit = sizeof(float);
    else if(data_type == PDC_INT)
        unit = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "data type is not supported yet");
    pdc_region_info_t_to_transfer_unit(reginfo, &(in.local_region), unit);

    // Create a bulk descriptor
    /* bulk_handle = HG_BULK_NULL; */

    server_id = PDC_get_server_by_obj_id(local_obj_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, reg_unmap_register_id_g, &client_send_region_unmap_handle);

    hg_ret = HG_Forward(client_send_region_unmap_handle, client_send_region_unmap_rpc_cb, &unmap_args, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "PDC_Client_send_region_unmap(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* if (unmap_args.ret != 1) */
    /*     PGOTO_ERROR(FAIL,"PDC_CLIENT: region unmapping failed..."); */
    
done:
    HG_Destroy(client_send_region_unmap_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_buf_map(pdcid_t local_region_id, pdcid_t remote_obj_id, pdcid_t remote_region_id, size_t ndim, uint64_t *local_dims, uint64_t *local_offset, uint64_t *local_size, PDC_var_type_t local_type, void *local_data, uint64_t *remote_dims, uint64_t *remote_offset, uint64_t *remote_size, PDC_var_type_t remote_type, int32_t remote_client_id, void *remote_data, struct PDC_region_info *local_region, struct PDC_region_info *remote_region)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    buf_map_in_t in;
    uint32_t data_server_id, meta_server_id;
    hg_class_t *hg_class;
    int n_retry;
    hg_uint32_t i, j;
    hg_uint32_t local_count;
    void    **data_ptrs = NULL;
    size_t  *data_size = NULL;
    size_t  unit, unit_to; 
    struct  buf_map_args map_args;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    hg_handle_t client_send_buf_map_handle;

    FUNC_ENTER(NULL);

    in.local_reg_id = local_region_id;
    in.remote_obj_id = remote_obj_id;
    in.remote_reg_id = remote_region_id;
    in.remote_client_id = remote_client_id;
    in.local_type = local_type;
    in.remote_type = remote_type;
    in.ndim = ndim;

    // Compute metadata server id
    meta_server_id = PDC_get_server_by_obj_id(remote_obj_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    // Compute local data server id
    data_server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[data_server_id]++;
    
    hg_class = HG_Context_get_class(send_context_g);
 
    if(local_type == PDC_DOUBLE)
        unit = sizeof(double);
    else if(local_type == PDC_FLOAT) 
        unit = sizeof(float);
    else if(local_type == PDC_INT)
        unit = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "local data type is not supported yet");
    pdc_region_info_t_to_transfer_unit(local_region, &(in.local_region), unit);

    if(remote_type == PDC_DOUBLE)
        unit_to = sizeof(double);
    else if(remote_type == PDC_FLOAT)
        unit_to = sizeof(float);
    else if(remote_type == PDC_INT)
        unit_to = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "local data type is not supported yet");
    pdc_region_info_t_to_transfer_unit(remote_region, &(in.remote_region_unit), unit_to);
    pdc_region_info_t_to_transfer(remote_region, &(in.remote_region_nounit));
    in.remote_unit = unit_to;

    if(ndim == 1) {
        local_count = 1;
        data_ptrs = (void **)malloc( sizeof(void *) );
        data_size = (size_t *)malloc( sizeof(size_t) );
        *data_ptrs = local_data + unit*local_offset[0];
        *data_size = unit*local_size[0];
    }
    else if(ndim == 2) {
        local_count = local_size[0];
        data_ptrs = (void **)malloc( local_count * sizeof(void *) );
        data_size = (size_t *)malloc( local_count * sizeof(size_t) );
        data_ptrs[0] = local_data + unit*(local_dims[1]*local_offset[0] + local_offset[1]);
        data_size[0] = unit*local_size[1];
        for(i=1; i<local_size[0]; i++) {
            data_ptrs[i] = data_ptrs[i-1] + unit*local_dims[1]; 
            data_size[i] = data_size[0];
        }
    }
    else if(ndim == 3) {
        local_count = local_size[0]*local_size[1];
        data_ptrs = (void **)malloc( local_count * sizeof(void *) );
        data_size = (size_t *)malloc( local_count * sizeof(size_t) );
        data_ptrs[0] = local_data + unit*(local_dims[2]*local_dims[1]*local_offset[0] + local_dims[2]*local_offset[1] + local_offset[2]);
        data_size[0] = unit*local_size[2];
        for(i=0; i<local_size[0]-1; i++) {
            for(j=0; j<local_size[1]-1; j++) {
                data_ptrs[i*local_size[1]+j+1] = data_ptrs[i*local_size[1]+j]+unit*local_dims[2];
                data_size[i*local_size[1]+j+1] = unit*local_size[2];
            }
            data_ptrs[i*local_size[1]+local_size[1]] = data_ptrs[i*local_size[1]]+unit*local_dims[2]*local_dims[1];
            data_size[i*local_size[1]+local_size[1]] = data_size[0];
        }
        i = local_size[0]-1;
        for(j=0; j<local_size[1]-1; j++) {
             data_ptrs[i*local_size[1]+j+1] = data_ptrs[i*local_size[1]+j]+unit*local_dims[2];
             data_size[i*local_size[1]+j+1] = data_size[0];
        }
    }
    else
        PGOTO_ERROR(FAIL, "mapping for array of dimension greater than 4 is not supproted");

    if( PDC_Client_try_lookup_server(data_server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[data_server_id].addr, buf_map_register_id_g, &client_send_buf_map_handle);

    // Create bulk handle
    hg_ret = HG_Bulk_create(hg_class, local_count, (void**)data_ptrs, (hg_size_t *)data_size, HG_BULK_READWRITE, &local_bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_buf_map(): Could not create local bulk data handle\n");
        return EXIT_FAILURE;
    }
    in.local_bulk_handle = local_bulk_handle;

    hg_ret = HG_Forward(client_send_buf_map_handle, client_send_buf_map_rpc_cb, &map_args, &in);	
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "PDC_Client_send_buf_map(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (map_args.ret != 1) 
        PGOTO_ERROR(FAIL,"PDC_CLIENT: buf map failed...");

done:
    HG_Destroy(client_send_buf_map_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_region_map(pdcid_t local_obj_id, pdcid_t local_region_id, pdcid_t remote_obj_id, pdcid_t remote_region_id, size_t ndim, uint64_t *local_dims, uint64_t *local_offset, uint64_t *local_size, PDC_var_type_t local_type, void *local_data, uint64_t *remote_dims, uint64_t *remote_offset, uint64_t *remote_size, PDC_var_type_t remote_type, int32_t remote_client_id, void *remote_data, struct PDC_region_info *local_region, struct PDC_region_info *remote_region)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    reg_map_in_t in;
    uint32_t server_id;
    hg_class_t *hg_class;
    int n_retry = 0;
    hg_uint32_t i, j;
    hg_uint32_t local_count, remote_count;
    void    **data_ptrs, **data_ptrs_to;
    size_t  *data_size, *data_size_to;
    size_t  unit, unit_to; 
    struct  region_map_args map_args;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t remote_bulk_handle = HG_BULK_NULL;
    hg_handle_t client_send_region_map_handle;
 
    FUNC_ENTER(NULL);
    
    // Fill input structure
    in.local_obj_id = local_obj_id;
    in.local_reg_id = local_region_id;
    in.remote_obj_id = remote_obj_id;
    in.remote_reg_id = remote_region_id;
    in.remote_client_id = remote_client_id;
    in.local_type = local_type;
    in.remote_type = remote_type;
    in.ndim = ndim;

    // Compute server id
    server_id = PDC_get_server_by_obj_id(local_obj_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;
    
    hg_class = HG_Context_get_class(send_context_g);

    if(local_type == PDC_DOUBLE)
        unit = sizeof(double);
    else if(local_type == PDC_FLOAT) 
        unit = sizeof(float);
    else if(local_type == PDC_INT)
        unit = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "local data type is not supported yet");
    pdc_region_info_t_to_transfer_unit(local_region, &(in.local_region), unit);

    if(remote_type == PDC_DOUBLE)
        unit_to = sizeof(double);
    else if(remote_type == PDC_FLOAT)
        unit_to = sizeof(float);
    else if(remote_type == PDC_INT)
        unit_to = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "local data type is not supported yet");
    pdc_region_info_t_to_transfer_unit(remote_region, &(in.remote_region), unit_to);

    if(ndim == 1) {
        local_count = 1;
        data_ptrs = (void **)malloc( sizeof(void *) );
        data_size = (size_t *)malloc( sizeof(size_t) );
        *data_ptrs = local_data + unit*local_offset[0];
        *data_size = unit*local_size[0];
   
        remote_count = 1;
        data_ptrs_to = (void **)malloc( sizeof(void *) );
        data_size_to = (size_t *)malloc( sizeof(size_t) );
        *data_ptrs_to = remote_data + unit_to*remote_offset[0];
        *data_size_to = unit_to*remote_size[0];
    }
    else if(ndim == 2) {
        local_count = local_size[0];
        data_ptrs = (void **)malloc( local_size[0] * sizeof(void *) );
        data_size = (size_t *)malloc( local_size[0] * sizeof(size_t) );
        data_ptrs[0] = local_data + unit*(local_dims[1]*local_offset[0] + local_offset[1]);
        data_size[0] = unit*local_size[1];
        for(i=1; i<local_size[0]; i++) {
            data_ptrs[i] = data_ptrs[i-1] + unit*local_dims[1]; 
            data_size[i] = unit*local_size[1];
        }

        remote_count = remote_size[0];
        data_ptrs_to = (void **)malloc( remote_size[0] * sizeof(void *) );
        data_size_to = (size_t *)malloc( remote_size[0] * sizeof(size_t) );
        data_ptrs_to[0] = remote_data + unit_to*(remote_dims[1]*remote_offset[0] + remote_offset[1]);
        data_size_to[0] = unit_to*remote_size[1];
        for(i=1; i<remote_size[0]; i++) {
            data_ptrs_to[i] = data_ptrs_to[i-1] + unit_to*remote_dims[1];
            data_size_to[i] = unit_to*remote_size[1];
         }
    }
    else if(ndim == 3) {
       local_count = local_size[0]*local_size[1];
        data_ptrs = (void **)malloc( local_size[0] * local_size[1] * sizeof(void *) );
        data_size = (size_t *)malloc( local_size[0] * local_size[1] * sizeof(size_t) );
        data_ptrs[0] = local_data + unit*(local_dims[2]*local_dims[1]*local_offset[0] + local_dims[2]*local_offset[1] + local_offset[2]);
        data_size[0] = unit*local_size[2];
        for(i=0; i<local_size[0]-1; i++) {
            for(j=0; j<local_size[1]-1; j++) {
                data_ptrs[i*local_size[1]+j+1] = data_ptrs[i*local_size[1]+j]+unit*local_dims[2];
                data_size[i*local_size[1]+j+1] = unit*local_size[2];
            }
            data_ptrs[i*local_size[1]+local_size[1]] = data_ptrs[i*local_size[1]]+unit*local_dims[2]*local_dims[1];
            data_size[i*local_size[1]+local_size[1]] = unit*local_size[2];
        }
        i = local_size[0]-1;
        for(j=0; j<local_size[1]-1; j++) {
             data_ptrs[i*local_size[1]+j+1] = data_ptrs[i*local_size[1]+j]+unit*local_dims[2];
             data_size[i*local_size[1]+j+1] = unit*local_size[2];
        }

        remote_count = remote_size[0] * remote_size[1];
        data_ptrs_to = (void **)malloc( remote_size[0] * remote_size[1] * sizeof(void *) );
        data_size_to = (size_t *)malloc( remote_size[0] * remote_size[1] * sizeof(size_t) );
        data_ptrs_to[0] = remote_data + unit_to*(remote_dims[2]*remote_dims[1]*remote_offset[0] + remote_dims[2]*remote_offset[1] + remote_offset[2]);
        data_size_to[0] = unit_to*remote_size[2];
        for(i=0; i<remote_size[0]-1; i++) {
            for(j=0; j<remote_size[1]-1; j++) {
                data_ptrs_to[i*remote_size[1]+j+1] = data_ptrs_to[i*remote_size[1]+j]+unit_to*remote_dims[2];
                data_size_to[i*remote_size[1]+j+1] = unit_to*remote_size[2];
            }
            data_ptrs_to[i*remote_size[1]+remote_size[1]] = data_ptrs_to[i*remote_size[1]]+unit_to*remote_dims[2]*remote_dims[1];
            data_size_to[i*remote_size[1]+remote_size[1]] = unit_to*remote_size[2];
        }
        i = remote_size[0]-1;
        for(j=0; j<remote_size[1]-1; j++) {
             data_ptrs_to[i*remote_size[1]+j+1] = data_ptrs_to[i*remote_size[1]+j]+unit_to*remote_dims[2];
             data_size_to[i*remote_size[1]+j+1] = unit_to*remote_size[2];
        } 
    }
    else
        PGOTO_ERROR(FAIL, "mapping for array of dimension greater than 4 is not supproted");

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, reg_map_register_id_g, &client_send_region_map_handle);
    
    // Create bulk handle
    hg_ret = HG_Bulk_create(hg_class, local_count, data_ptrs, (hg_size_t *)data_size, HG_BULK_READWRITE, &local_bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create local bulk data handle\n");
        return EXIT_FAILURE;
    }
    hg_ret = HG_Bulk_create(hg_class, remote_count, data_ptrs_to, (hg_size_t *)data_size_to, HG_BULK_READWRITE, &remote_bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create remote bulk data handle\n");
        return EXIT_FAILURE;
    }
    in.local_bulk_handle = local_bulk_handle;
    in.remote_bulk_handle = remote_bulk_handle;
//TODO: free
//    free(data_ptrs);
//    free(data_size);

    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(client_send_region_map_handle, client_send_region_map_rpc_cb, &map_args, &in);	
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "PDC_Client_send_region_map(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

	/* if (pdc_client_mpi_rank_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: PDC_Client_send_region() got response from server %d\n", pdc_client_mpi_rank_g, lookup_args.ret); */
    /*     fflush(stdout); */
    /* } */

    if (map_args.ret != 1) 
//        printf("PDC_CLIENT: object mapping NOT successful ... ret_value = %d\n", map_args.ret);
        PGOTO_ERROR(FAIL,"PDC_CLIENT: object mapping failed...");
//    else
//        printf("PDC_CLIENT: object mapping successful\n");
done:
    HG_Destroy(client_send_region_map_handle);
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Client_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_var_type_t data_type, pbool_t *status)
{
    perr_t ret_value;
    hg_return_t hg_ret;
    uint32_t server_id, meta_server_id;
    region_lock_in_t in;
    size_t  unit;
    int n_retry = 0;
    struct region_lock_args lookup_args;
    hg_handle_t region_lock_handle;
    
    FUNC_ENTER(NULL);
    
    // Compute local data server id
    server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    meta_server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    // Delay test
    srand(pdc_client_mpi_rank_g);
    int delay = rand() % 500;
    usleep(delay);


//    printf("==PDC_CLIENT[%d]: lock going to server %u\n", pdc_client_mpi_rank_g, server_id); 
//    fflush(stdout); 

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // Fill input structure
    in.obj_id = meta_id;
//    in.lock_op = lock_op;
    in.access_type = access_type;
    in.mapping = region_info->mapping;
    in.local_reg_id = region_info->local_id;
//    in.region.ndim   = region_info->ndim;
    size_t ndim = region_info->ndim;
    in.data_type = data_type;
    /* printf("==PDC_CLINET: lock dim=%u\n", ndim); */

    if (ndim >= 4 || ndim <=0) {
        printf("Dimension %lu is not supported\n", ndim);
        ret_value = FAIL;
        goto done;
    }

    if(data_type == PDC_DOUBLE)
        unit = sizeof(double);
    else if(data_type == PDC_FLOAT)
        unit = sizeof(float);
    else if(data_type == PDC_INT)
        unit = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "data type is not supported yet");

    pdc_region_info_t_to_transfer_unit(region_info, &(in.region), unit);
//    if (ndim >=1) {
//        in.region.start_0  = unit * region_info->offset[0];
//        in.region.count_0  = unit * region_info->size[0];
        /* in.region.stride_0 = 0; */
        // TODO current stride is not included in pdc.
//    }
//    if (ndim >=2) {
//        in.region.start_1  = unit * region_info->offset[1];
//        in.region.count_1  = unit * region_info->size[1];
        /* in.region.stride_1 = 0; */
//    }
//    if (ndim >=3) {
//        in.region.start_2  = unit * region_info->offset[2];
//        in.region.count_2  = unit * region_info->size[2];
        /* in.region.stride_2 = 0; */
//    }
    /* if (ndim >=4) { */
    /*     in.region.start_3  = region_info->offset[3]; */
    /*     in.region.count_3  = region_info->size[3]; */
    /*     /1* in.region.stride_3 = 0; *1/ */
    /* } */

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_lock_register_id_g, 
                &region_lock_handle);

    hg_ret = HG_Forward(region_lock_handle, client_region_lock_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_send_name_to_server(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }
/*
    if (lock_op == PDC_LOCK_OP_OBTAIN) {
    }
    else if (lock_op == PDC_LOCK_OP_RELEASE) {

    }
    else {
        printf("==PDC_CLIENT: unsupport lock operation!\n");
        ret_value = FAIL;
        goto done;
    }
*/
    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);


    // Now the return value is stored in lookup_args.ret
    if (lookup_args.ret == 1) {
        *status = TRUE;
        ret_value = SUCCEED;
    }
    else {
        *status = FALSE;
        ret_value = FAIL;
    }

done:
    HG_Destroy(region_lock_handle);
    FUNC_LEAVE(ret_value);
}

/* , uint64_t *block */
perr_t PDC_Client_obtain_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_lock_mode_t lock_mode, PDC_var_type_t data_type, pbool_t *obtained)
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    /* printf("meta_id=%" PRIu64 "\n", meta_id); */
/*
    if (access_type == READ ) {
        // TODO: currently does not perform local lock
        ret_value = SUCCEED;
        *obtained  = TRUE;
        goto done;
    }
*/
//    else if (access_type == WRITE) {
      if (access_type == WRITE || access_type == READ) {      
        if (lock_mode == BLOCK) {
            // TODO: currently the client would keep trying to send lock request
            while (1) {
                // ret_value = PDC_Client_region_lock(meta_id, region_info, WRITE, PDC_LOCK_OP_OBTAIN, obtained);
                ret_value = PDC_Client_region_lock(meta_id, region_info, access_type, data_type, obtained);
                if (*obtained == TRUE) {
                    ret_value = SUCCEED;
                    goto done;
                }
                printf("==PDC_CLIENT: cannot obtain lock an this moment, keep trying...\n");
                fflush(stdout);
                sleep(1);
            }
        }
        else if (lock_mode == NOBLOCK) {
            // ret_value = PDC_Client_region_lock(meta_id, region_info, WRITE, PDC_LOCK_OP_OBTAIN, obtained);
            ret_value = PDC_Client_region_lock(meta_id, region_info, access_type, data_type, obtained);
            goto done;
        }
        else {
            printf("==PDC_CLIENT: PDC_Client_obtain_region_lock - unknown lock mode (can only be BLOCK or NOBLOCK)\n");
            ret_value = FAIL;
            goto done;
        }
    }
    else {
        printf("==PDC_CLIENT: PDC_Client_obtain_region_lock - unknown access type (can only be READ or WRITE)\n");
        ret_value = FAIL;
        goto done;
    }

    ret_value = SUCCEED;
    
done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Client_region_release(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_var_type_t data_type, pbool_t *status)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    uint32_t server_id, meta_server_id;
    region_lock_in_t in;
    size_t unit;
    struct client_lookup_args lookup_args;
    hg_handle_t region_release_handle;

    FUNC_ENTER(NULL);

    // Compute local data server id
    server_id = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    meta_server_id = PDC_get_server_by_obj_id(meta_id, pdc_server_num_g);
    in.meta_server_id = meta_server_id;

    /* // Delay test */
    /* srand(pdc_client_mpi_rank_g); */
    /* int delay = rand() % 500; */
    /* usleep(delay); */


//    printf("==PDC_CLIENT[%d]: release going to server %u\n", pdc_client_mpi_rank_g, server_id);
//    fflush(stdout);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // Fill input structure
    in.obj_id = meta_id;
//    in.lock_op = lock_op;
    in.access_type = access_type;
    in.mapping = region_info->mapping;
    in.local_reg_id = region_info->local_id;
//    in.region.ndim   = region_info->ndim;
    size_t ndim = region_info->ndim;
    /* printf("==PDC_CLINET: lock dim=%u\n", ndim); */
 
    if (ndim >= 4 || ndim <=0) {
        printf("Dimension %lu is not supported\n", ndim);
        ret_value = FAIL;
        goto done;
    }

    if(data_type == PDC_DOUBLE)
        unit = sizeof(double);
    else if(data_type == PDC_FLOAT)
        unit = sizeof(float);
    else if(data_type == PDC_INT)
        unit = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "data type is not supported yet");

    pdc_region_info_t_to_transfer_unit(region_info, &(in.region), unit);
//    if (ndim >=1) {
//        in.region.start_0  = unit * region_info->offset[0];
//        in.region.count_0  = unit * region_info->size[0];
//        in.region.stride_0 = 0;
        // TODO current stride is not included in pdc.
//    }
//    if (ndim >=2) {
//        in.region.start_1  = unit * region_info->offset[1];
//        in.region.count_1  = unit * region_info->size[1];
//        in.region.stride_1 = 0;
//    }
//    if (ndim >=3) {
//        in.region.start_2  = unit * region_info->offset[2];
//        in.region.count_2  = unit * region_info->size[2];
//        in.region.stride_2 = 0;
//    }
/*
    if (ndim >=4) {
        in.region.start_3  = region_info->offset[3];
        in.region.count_3  = region_info->size[3];
        in.region.stride_3 = 0;
    }
*/
    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_release_register_id_g, 
              &region_release_handle);

    hg_ret = HG_Forward(region_release_handle, client_region_release_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Client_send_name_to_server(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);


    // Now the return value is stored in lookup_args.ret
    if (lookup_args.ret == 1) {
        *status = TRUE;
        ret_value = SUCCEED;
    }
    else {
        *status = FALSE;
        ret_value = FAIL;
    }

done:
    HG_Destroy(region_release_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_release_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, PDC_var_type_t data_type, pbool_t *released)
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    /* uint64_t meta_id; */
    /* PDC_obj_info *obj_prop = PDCobj_get_info(obj_id, pdc); */
    /* meta_id = obj_prop->meta_id; */
    ret_value = PDC_Client_region_release(meta_id, region_info, access_type, data_type, released);

    FUNC_LEAVE(ret_value);
}

/*
 * Data server related 
 */

static hg_return_t
data_server_read_check_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;

    FUNC_ENTER(NULL);

    /* printf("Entered data_server_read_check_rpc_cb()"); */
    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_read_check_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: data_server_read_check_rpc_cb error with HG_Get_output\n",
                pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        client_lookup_args->ret_string = " ";
        goto done;
    }
    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT: data_server_read_check ret=%d, addr=%s\n", output.ret, output.shm_addr);
    }
    client_lookup_args->ret = output.ret;
    client_lookup_args->ret_string = (char*)malloc(strlen(output.shm_addr)+1);
    strcpy(client_lookup_args->ret_string, output.shm_addr);

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}


// This function is used with server push notification
hg_return_t PDC_Client_get_data_from_server_shm_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    /* hg_return_t hg_ret; */
    /* struct client_lookup_args lookup_args; */
    /* data_server_read_check_in_t in; */

    int shm_fd = -1;        // file descriptor, from shm_open()
    uint32_t i = 0, j = 0;
    char *shm_base = NULL;    // base address, from mmap()
    char *shm_addr = NULL;
    uint64_t data_size = 1;
    client_read_info_t *read_info = NULL;
    PDC_Request_t *elt = NULL;
    struct PDC_region_info *target_region = NULL;

    FUNC_ENTER(NULL);

    read_info = (client_read_info_t*)callback_info->arg;

    shm_addr = read_info->shm_addr;

    // TODO: Need to find the correct request
    DL_FOREACH(pdc_io_request_list_g, elt) {
        if (elt->metadata->obj_id == read_info->obj_id && elt->access_type == READ) {
            target_region = elt->region;
            break;
        }
    }

    if (target_region == NULL) {
        printf("==PDC_CLIENT: PDC_Client_get_data_from_server_shm_cb() - request region not found!\n");
        goto done;
    }

    // Calculate data_size, TODO: this should be done in other places?
    data_size = 1;
    for (i = 0; i < target_region->ndim; i++) {
        data_size *= target_region->size[i];
    }

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT: PDC_Client_get_data_from_server_shm - shm_addr=[%s]\n", shm_addr);
    }

    /* open the shared memory segment as if it was a file */
    shm_fd = shm_open(shm_addr, O_RDONLY, 0666);
    if (shm_fd == -1) {
        printf("==PDC_CLIENT: Shared memory open failed [%s]!\n", shm_addr);
        ret_value = FAIL;
        goto done;
    }

    /* map the shared memory segment to the address space of the process */
    shm_base = mmap(0, data_size, PROT_READ, MAP_SHARED, shm_fd, 0);
    if (shm_base == MAP_FAILED) {
        printf("==PDC_CLIENT: Map failed: %s\n", strerror(errno));
        ret_value = FAIL;
        goto close;
    }

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
    #endif

    // Copy data
    /* printf("==PDC_SERVER: memcpy size = %" PRIu64 "\n", data_size); */
    memcpy(elt->buf, shm_base, data_size);

    #ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    #endif

    /* remove the mapped shared memory segment from the address space of the process */
    if (munmap(shm_base, data_size) == -1) {
        printf("==PDC_CLIENT: Unmap failed: %s\n", strerror(errno));
        ret_value = FAIL;
        goto done;
    }

close:
    /* close the shared memory segment as if it was a file */
    if (close(shm_fd) == -1) {
        printf("==PDC_CLIENT: Close failed!\n");
        ret_value = FAIL;
        goto done;
    }

    /* remove the shared memory segment from the file system */
    if (shm_unlink(shm_addr) == -1) {
        ret_value = FAIL;
        goto done;
        printf("==PDC_CLIENT: Error removing %s\n", shm_addr);
    }

done:
    work_todo_g--;
    FUNC_LEAVE(ret_value);
}


// This is used with polling approach to get data from server read
perr_t PDC_Client_data_server_read_check(int server_id, uint32_t client_id, pdc_metadata_t *meta, 
                                         struct PDC_region_info *region, int *status, void* buf)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    struct client_lookup_args lookup_args;
    data_server_read_check_in_t in;
    uint32_t i;
    hg_handle_t data_server_read_check_handle;

    int shm_fd;        // file descriptor, from shm_open()
    char *shm_base;    // base address, from mmap()
    char *shm_addr;


    FUNC_ENTER(NULL);

    if (meta == NULL || region == NULL || status == NULL || buf == NULL) {
        printf("==PDC_CLIENT[%d]: %s - NULL input\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("==PDC_CLIENT[%d]: %s- invalid server id %d/%d\n",
                pdc_client_mpi_rank_g, __func__, server_id, pdc_server_num_g);
        ret_value = FAIL;
        goto done;
    }

    // Dummy value fill
    in.client_id         = client_id;
    pdc_metadata_t_to_transfer_t(meta, &in.meta);
    pdc_region_info_t_to_transfer(region, &in.region);

    uint64_t read_size = 1;
    for (i = 0; i < region->ndim; i++) {
        read_size *= region->size[i];
    }

    if (is_client_debug_g) {
        printf("==PDC_CLIENT[%d]: checking io status (%" PRIu64 ", %" PRIu64 ") with data server %d\n",
                pdc_client_mpi_rank_g, region->offset[0], region->size[0], server_id);
        fflush(stdout);
    }

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_check_register_id_g,
                &data_server_read_check_handle);

    hg_ret = HG_Forward(data_server_read_check_handle, data_server_read_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==%s(): Could not start HG_Forward()\n", __func__);
        return EXIT_FAILURE;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    *status = lookup_args.ret;
    if (lookup_args.ret != 1) {
        ret_value = SUCCEED;
        if (is_client_debug_g) {
            printf("==PDC_CLIENT[%d]: %s- IO request has not been fulfilled by server\n", pdc_client_mpi_rank_g, __func__);
        }
        HG_Destroy(data_server_read_check_handle);
        if (lookup_args.ret == -1)
            ret_value = -1;
        goto done;
    }
    else {
        /* if (is_client_debug_g) { */
        /*     printf("==PDC_CLIENT: %s - IO request done by server: shm_addr=[%s]\n", */
        /*             lookup_args.ret_string, __func__); */
        /* } */
        shm_addr = lookup_args.ret_string;

        /* open the shared memory segment as if it was a file */
        shm_fd = shm_open(shm_addr, O_RDONLY, 0666);
        if (shm_fd == -1) {
            printf("==PDC_CLIENT: Shared memory open failed [%s]!\n", shm_addr);
            ret_value = FAIL;
            goto done;
        }

        /* map the shared memory segment to the address space of the process */
        shm_base = mmap(0, read_size, PROT_READ, MAP_SHARED, shm_fd, 0);
        if (shm_base == MAP_FAILED) {
            printf("==PDC_CLIENT: Map failed: %s\n", strerror(errno));
            // close and unlink?
            ret_value = FAIL;
            goto close;
        }

        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start;
        struct timeval  pdc_timer_end;
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
            printf("==PDC_CLIENT: Unmap failed: %s\n", strerror(errno));
            ret_value = FAIL;
            goto done;
        }

        HG_Destroy(data_server_read_check_handle);
    } // end of check io


close:
    /* close the shared memory segment as if it was a file */
    if (close(shm_fd) == -1) {
        printf("==PDC_CLIENT: Close failed!\n");
        ret_value = FAIL;
        goto done;
    }

    /* remove the shared memory segment from the file system */
    /* if (shm_unlink(shm_addr) == -1) { */
    /*     printf("==PDC_CLIENT: Error removing %s\n", shm_addr); */
    /*     ret_value = FAIL; */
    /*     goto done; */
    /* } */

    free(lookup_args.ret_string);

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_read_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    
    FUNC_ENTER(NULL);

    /* printf("Entered data_server_read_rpc_cb()"); */
    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_read_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: data_server_read_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */
    client_lookup_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}


perr_t PDC_Client_data_server_read(PDC_Request_t *request)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    struct client_lookup_args lookup_args;
    data_server_read_in_t in;
    hg_handle_t data_server_read_handle;
    int server_id, n_client, n_update;
    pdc_metadata_t *meta;
    struct PDC_region_info *region;

    FUNC_ENTER(NULL);

    server_id = request->server_id;
    n_client  = request->n_client;
    n_update  = request->n_update;
    meta      = request->metadata;
    region    = request->region;

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("PDC_CLIENT[%d]: %s - invalid server id %d/%d\n", 
                pdc_client_mpi_rank_g, __func__, server_id, pdc_server_num_g);
        ret_value = FAIL;
        goto done;
    }

    if (meta == NULL || region == NULL) {
        printf("PDC_CLIENT[%d]: %s - invalid metadata or region \n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // Dummy value fill
    in.client_id         = pdc_client_mpi_rank_g;
    in.nclient           = n_client;
    in.nupdate           = n_update;
    if (request->n_update == 0) 
        request->n_update    = 1;       // Only set to default value if it is not set prior
    pdc_metadata_t_to_transfer_t(meta, &in.meta);
    pdc_region_info_t_to_transfer(region, &in.region);

    /* printf("PDC_CLIENT: sending data server read request to server %d\n", server_id); */

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_register_id_g, 
                &data_server_read_handle);

    hg_ret = HG_Forward(data_server_read_handle, data_server_read_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "%s - Could not start HG_Forward()\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret == 1) {
        ret_value = SUCCEED;
        /* printf("PDC_CLIENT: %s - received confirmation from server\n", __func__); */
    }
    else {
        ret_value = FAIL;
        printf("PDC_CLIENT: %s - ERROR from server\n", __func__);
    }


done:
    HG_Destroy(data_server_read_handle);
    FUNC_LEAVE(ret_value);
} // End of PDC_Request_t *request

/*
 * Close the shared memory
 */
perr_t PDC_Client_close_shm(PDC_Request_t *req)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (req == NULL || req->shm_fd == 0 || req->shm_base == NULL) {
        printf("==PDC_CLIENT[%d]: %s - invalid input\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }  

    /* remove the mapped memory segment from the address space of the process */
    if (munmap(req->shm_base, req->shm_size) == -1) {
        printf("==PDC_CLIENT[%d]: %s - Unmap failed\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    /* close the shared memory segment as if it was a file */
    if (close(req->shm_fd) == -1) {
        printf("==PDC_CLIENT[%d]: %s - close shm failed\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    /* remove the shared memory segment from the file system */
    if (shm_unlink(req->shm_addr) == -1) {
        ret_value = FAIL;
        goto done;
        printf("==PDC_CLIENT: Error removing %s\n", req->shm_addr);
    }

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_write_check_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    
    FUNC_ENTER(NULL);

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_write_check_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: data_server_write_check_rpc_cb error with HG_Get_output\n", 
                pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }
    /* printf("==PDC_CLIENT: data_server_write_check ret=%d, addr=%s\n", output.ret, output.shm_addr); */
    client_lookup_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_data_server_write_check(PDC_Request_t *request, int *status)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    struct client_lookup_args lookup_args;
    data_server_write_check_in_t in;
    hg_handle_t data_server_write_check_handle;
    int server_id;
    pdc_metadata_t *meta;
    struct PDC_region_info *region;

    FUNC_ENTER(NULL);

    server_id = request->server_id;
    meta      = request->metadata;
    region    = request->region;

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("PDC_CLIENT[%d]: %s - invalid server id %d/%d\n"
                , pdc_client_mpi_rank_g, __func__, server_id, pdc_server_num_g);
        ret_value = FAIL;
        goto done;
    }

    in.client_id         = pdc_client_mpi_rank_g;
    pdc_metadata_t_to_transfer_t(meta, &in.meta);
    pdc_region_info_t_to_transfer(region, &in.region);

    uint64_t write_size = 1;
    uint32_t i;
    for (i = 0; i < region->ndim; i++) {
        write_size *= region->size[i];
    }

    /* printf("PDC_CLIENT: checking io status with data server %d\n", server_id); */

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_write_check_register_id_g, 
                &data_server_write_check_handle);

    hg_ret = HG_Forward(data_server_write_check_handle, data_server_write_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "%s(): Could not start HG_Forward()\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* if (is_client_debug_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write_check() - ret=%d \n", */
    /*                 pdc_client_mpi_rank_g, lookup_args.ret); */
    /* } */

    *status = lookup_args.ret;
    if (lookup_args.ret != 1) {
        ret_value = SUCCEED;
        if (is_client_debug_g == 1) {
            printf("==PDC_CLIENT[%d]: %s IO request not done by server yet\n", pdc_client_mpi_rank_g,__func__);
        }

        if (lookup_args.ret == -1)
            ret_value = FAIL;
        goto done;
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
    hg_return_t ret_value = HG_SUCCESS;

    FUNC_ENTER(NULL);

    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    data_server_write_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value !=  HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: data_server_write_rpc_cb error with HG_Get_output\n",
                pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }

    /* if (is_client_debug_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: data_server_write_rpc_cb(): Return value from server: %d\n", */
    /*             pdc_client_mpi_rank_g, output.ret); */
    /* } */

    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_data_server_write(PDC_Request_t *request)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    uint32_t i, j;
    uint64_t region_size = 1;
    struct client_lookup_args lookup_args;
    data_server_write_in_t in;
    int server_ret;
    hg_handle_t data_server_write_handle;
    hg_handle_t aggregate_write_handle;
    region_list_t* tmp_regions = NULL;
    int server_id, n_client, n_update; 
    pdc_metadata_t *meta; 
    struct PDC_region_info *region;
    void *buf;

    FUNC_ENTER(NULL);

    server_id = request->server_id;
    n_client  = request->n_client;
    n_update  = request->n_update;
    meta      = request->metadata;
    region    = request->region;
    buf       = request->buf;

    if (NULL == meta || NULL == region || NULL == buf) {
        printf("==PDC_CLIENT[%d]: %s - input NULL\n", pdc_client_mpi_rank_g, __func__);
        fflush(stdout);
        ret_value = FAIL;
        goto done;
    }

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("==PDC_CLIENT[%d]: %s - invalid server id %d/%d\n",
                pdc_client_mpi_rank_g, __func__, server_id, pdc_server_num_g);
        ret_value = FAIL;
        goto done;
    }

    if (region->ndim > 4) {
        printf("==PDC_CLIENT[%d]: %s - invalid dim %lu\n", pdc_client_mpi_rank_g, __func__, region->ndim);
        ret_value = FAIL;
        goto done;
    }

    // Calculate region size
    for (i = 0; i < region->ndim; i++) {
        region_size *= region->size[i];
        if (region_size == 0) {
            printf("==PDC_CLIENT[%d]: %s - size[%d]=0\n", pdc_client_mpi_rank_g, __func__, i);
            ret_value = FAIL;
            goto done;
        }
    }

    // Create shared memory
    // Shared memory address is /objID_ServerID_ClientID_rand
    int rnd = rand();
    sprintf(request->shm_addr, "/%" PRIu64 "_c%d_s%d_%d", meta->obj_id, pdc_client_mpi_rank_g, server_id, rnd);

    /* create the shared memory segment as if it was a file */
    /* printf("==PDC_CLIENT: creating share memory segment with address %s\n", shm_addr); */
    request->shm_fd = shm_open(request->shm_addr, O_CREAT | O_RDWR, 0666);
    if (request->shm_fd == -1) {
        printf("==PDC_CLIENT: Shared memory creation with shm_open failed\n");
        ret_value = FAIL;
        goto done;
    }

    /* configure the size of the shared memory segment */
    ftruncate(request->shm_fd, region_size);
    request->shm_size = region_size;

    /* map the shared memory segment to the address space of the process */
    request->shm_base = mmap(0, region_size, PROT_READ | PROT_WRITE, MAP_SHARED, request->shm_fd, 0);
    if (request->shm_base == MAP_FAILED) {
        printf("==PDC_CLIENT: Shared memory mmap failed, region size = %" PRIu64 "\n", region_size);
        // close and shm_unlink?
        ret_value = FAIL;
        goto done;
    }

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
    #endif

    // Copy the user's buffer to shm that can be accessed by data server
    memcpy(request->shm_base, buf, region_size);

    #ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    #endif

    int can_aggregate_send = 0;
    /* uint64_t same_node_ids[64]; */
    // Aggregate the send request when clients of same node are sending requests of same object
    // First check the obj ID are the same among the node local ranks

#ifdef ENABLE_MPI
    // TODO test
    /* if (pdc_client_same_node_size_g <= 1) { */

    /*     MPI_Gather(meta->obj_id, 1, MPI_UNSIGNED_LONG_LONG, */
    /*                same_node_ids, 1, MPI_UNSIGNED_LONG_LONG, 0, PDC_SAME_NODE_COMM_g); */

    /*     if (pdc_client_same_node_rank_g == 0) { */
    /*         for (i = 0; i < pdc_client_same_node_size_g; i++) { */
    /*             if (meta->obj_id != same_node_ids[i]) { */
    /*                 can_aggregate_send = -1; */
    /*                 break; */
    /*             } */
    /*         } */
    /*     } */

    /*     MPI_Bcast(&can_aggregate_send, 1, MPI_INT, 0, PDC_SAME_NODE_COMM_g); */
    /* } */
    /* // No need to aggregate if there is only one client per node */
    /* else */
    /*     can_aggregate_send = -1; */

    /* if (can_aggregate_send == 0) */
    /*     can_aggregate_send = 1; */
#endif


    /* printf("==PDC_CLIENT: sending data server write with %" PRIu64 " bytes to server %d\ndata: [%.*s]\n", */
    /*         region_size, server_id, region_size, buf); */

    // Generate a location for data storage for data server to write
    /* char *data_path = NULL; */
    /* char *user_specified_data_path = getenv("PDC_DATA_LOC"); */
    /* if (user_specified_data_path != NULL) { */
    /*     data_path = user_specified_data_path; */
    /* } */
    /* else { */
    /*     data_path = getenv("SCRATCH"); */
    /*     if (data_path == NULL) { */
    /*         data_path = "."; */

    /*     } */
    /* } */

    /* // Data path prefix will be $SCRATCH/pdc_data/obj_id/ */
    /* sprintf(meta->data_location, "%s/pdc_data/%" PRIu64 "", data_path, meta->obj_id); */

    /* if (pdc_client_mpi_rank_g == 0) */
    /*     printf("==PDC_CLIENT: data will be written to %s\n", meta->data_location); */

    // Update the data location of metadata object
    /* if (pdc_client_mpi_rank_g == 0) { */
    /*     PDC_Client_update_metadata(meta, meta); */
    /* } */

    /* if (can_aggregate_send != 1) { */
        // Normal send to server by each process
        meta->data_location[0] = ' ';
        meta->data_location[1] = 0;

        in.client_id         = pdc_client_mpi_rank_g;
        in.nclient           = n_client;
        in.nupdate           = n_update;
        in.shm_addr          = request->shm_addr;
        pdc_metadata_t_to_transfer_t(meta, &in.meta);
        pdc_region_info_t_to_transfer(region, &in.region);

        if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
            printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr,
                           data_server_write_register_id_g, &data_server_write_handle);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==%s: Could not HG_Create()\n", __func__);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Forward(data_server_write_handle, data_server_write_rpc_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==%s: Could not start HG_Forward()\n", __func__);
            HG_Destroy(data_server_write_handle);
            ret_value = FAIL;
            goto done;
        }

        // Wait for response from server
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);

        HG_Destroy(data_server_write_handle);

        server_ret = lookup_args.ret;
    /* } */
    /* else { */
    /*     // First client rank as the delegate and aggregate the requests from other clients */
    /*     // of same node to it (only needs region offsets/counts and shm_addr, metadata is shared) */

    /*     char my_serialized_buf[PDC_SERIALIZE_MAX_SIZE]; */
    /*     char *all_serialized_buf = NULL; */
    /*     /1* uint64_t *uint64_ptr = NULL; *1/ */
    /*     region_list_t tmp_region; */
    /*     region_list_t *regions[2]; */
    /*     pdc_aggregated_io_to_server_t agg_write_in; */

    /*     regions[0] = &tmp_region; */

    /*     memset(my_serialized_buf, 0, PDC_SERIALIZE_MAX_SIZE); */
    /*     memset(&tmp_region, 0, sizeof(region_list_t)); */

    /*     pdc_region_info_to_list_t(region, &tmp_region); */

    /*     // Store the shm addr in storage_location */
    /*     strcpy(tmp_region.storage_location, request->shm_addr); */

    /*     ret_value = PDC_serialize_regions_lists(regions, 1, my_serialized_buf, PDC_SERIALIZE_MAX_SIZE); */
    /*     if (ret_value != SUCCEED) { */
    /*         printf("==PDC_CLIENT[%d]: PDC_serialize_regions_lists ERROR!\n", pdc_client_mpi_rank_g); */
    /*         goto done; */
    /*     } */

    /*     PDC_replace_zero_chars(my_serialized_buf, PDC_SERIALIZE_MAX_SIZE); */
    /*     // Now my_serialized_buf has the serialized region data, with 0 substituted */
    /*     // n_region(1)|ndim|start_0|count_0|...|start_ndim|count_ndim|loc_len|shm_addr|offset|... */


    /*     if (pdc_client_same_node_rank_g == 0) { */
    /*         all_serialized_buf = (char*)calloc(PDC_SERIALIZE_MAX_SIZE, pdc_client_same_node_size_g); */
    /*     } */

    /*     // Gather serialized region info to first rank of each node */
/* #ifdef ENABLE_MPI */
    /*     MPI_Gather(my_serialized_buf,  PDC_SERIALIZE_MAX_SIZE, MPI_CHAR, */
    /*                all_serialized_buf, PDC_SERIALIZE_MAX_SIZE, MPI_CHAR, 0, PDC_SAME_NODE_COMM_g); */
/* #endif */

    /*     if (pdc_client_same_node_rank_g == 0) { */

    /*         tmp_regions = (region_list_t*)calloc(sizeof(region_list_t), pdc_client_same_node_size_g); */

    /*         // Now send all_serialized_buf to server and let server unserialize the regions */
    /*         meta->data_location[0] = ' '; */
    /*         meta->data_location[1] = 0; */
    /*         pdc_metadata_t_to_transfer_t(meta, &agg_write_in.meta); */
    /*         agg_write_in.buf = all_serialized_buf; */

    /*         int n_retry = 0; */
    /*         while (pdc_server_info_g[server_id].addr_valid != 1) { */
    /*             if (n_retry > 0) */
    /*                 break; */
    /*             if( PDC_Client_lookup_server(server_id) != SUCCEED) { */
    /*                 printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g); */
    /*                 ret_value = FAIL; */
    /*                 goto done; */
    /*             } */
    /*             n_retry++; */
    /*         } */

    /*         hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, */
    /*                            aggregate_write_register_id_g, &aggregate_write_handle); */
    /*         if (hg_ret != HG_SUCCESS) { */
    /*             fprintf(stderr, "==%s: Could not HG_Create()\n", __func__); */
    /*             ret_value = FAIL; */
    /*             goto done; */
    /*         } */

    /*         hg_ret = HG_Forward(aggregate_write_handle, data_server_write_rpc_cb, &lookup_args, &in); */
    /*         if (hg_ret != HG_SUCCESS) { */
    /*             fprintf(stderr, "==%s: Could not start HG_Forward()\n", __func__); */
    /*             HG_Destroy(aggregate_write_handle); */
    /*             ret_value = FAIL; */
    /*             goto done; */
    /*         } */

    /*         // Wait for response from server */
    /*         work_todo_g = 1; */
    /*         PDC_Client_check_response(&send_context_g); */

    /*         HG_Destroy(aggregate_write_handle); */
    /*         server_ret = lookup_args.ret; */

    /*         free(tmp_regions); */
    /*         free(all_serialized_buf); */
    /*     } // End of if client is first rank of the node */

/* #ifdef ENABLE_MPI */
    /*     // Now broadcast the result from server */
    /*     MPI_Bcast(&server_ret, 1, MPI_INT, 0, PDC_SAME_NODE_COMM_g); */
/* #endif */
    /* } */

    if (server_ret == 1) {
        ret_value = SUCCEED;
        /* printf("==PDC_CLIENT: %s- received confirmation from server\n", __func__); */
    }
    else {
        printf("==PDC_CLIENT[%d]: %s - ERROR from server\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // End of PDC_Client_data_server_write

perr_t PDC_Client_test(PDC_Request_t *request, int *completed)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (request == NULL || completed == NULL) {
        printf("==PDC_CLIENT: %s - request and/or completed is NULL!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    if (request->access_type == READ) {
        ret_value = PDC_Client_data_server_read_check(request->server_id, pdc_client_mpi_rank_g,
                     request->metadata, request->region, completed, request->buf);
        if (ret_value != SUCCEED) {
            printf("==PDC_CLIENT: PDC Client read check ERROR!\n");
            goto done;
        }
    }
    else if (request->access_type == WRITE) {

        ret_value = PDC_Client_data_server_write_check(request, completed);
        if (ret_value != SUCCEED) {
            printf("==PDC_CLIENT: PDC Client write check ERROR!\n");
            goto done;
        }
    }
    else {
        printf("==PDC_CLIENT: %s - error with request access type!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // End of PDC_Client_test

perr_t PDC_Client_wait(PDC_Request_t *request, unsigned long max_wait_ms, unsigned long check_interval_ms)
{
    int completed = 0;
    perr_t ret_value = SUCCEED;
    struct timeval  start_time;
    struct timeval  end_time;
    unsigned long elapsed_ms;
    uint64_t total_size = 1;
    long est_wait_time;
    long est_write_rate = 500;
    size_t i;
    int cnt = 0;

    FUNC_ENTER(NULL);

    gettimeofday(&start_time, 0);
    // TODO: Calculate region size and estimate the wait time
    // Write is 4-5x faster
    /* if (request->access_type == WRITE ) */ 
    /*     est_write_rate *= 4; */
    
    /* for (i = 0; i < request->region->ndim; i++) */ 
    /*     total_size *= request->region->size[i]; */ 
    /* total_size /= 1048576; */
    /* est_wait_time = total_size * request->n_client * 1000 / est_write_rate; */
    /* if (pdc_client_mpi_rank_g == 0) { */
    /*     printf("==PDC_CLIENT[%d]: estimate wait time is %ld\n", pdc_client_mpi_rank_g, est_wait_time); */
    /*     fflush(stdout); */
    /* } */
    /* pdc_msleep(est_wait_time); */

    while (completed != 1 && cnt < PDC_MAX_TRIAL_NUM) {

        ret_value = PDC_Client_test(request, &completed);
        if (ret_value != SUCCEED) {
            printf("==PDC_CLIENT[%d]: PDC_Client_test ERROR!\n", pdc_client_mpi_rank_g);
            goto done;
        }

        /* printf("completed ... %d\n", completed); */
        if (is_client_debug_g ==1 && completed == 1) {
            printf("==PDC_CLIENT[%d]: IO has completed.\n", pdc_client_mpi_rank_g);
            fflush(stdout);
            break;
        }
        /* else if (completed == 0){ */
        /*     printf("==PDC_CLIENT[%d]: IO has not completed yet, will wait and ping server again ...\n", */
        /*             pdc_client_mpi_rank_g); */
        /*     fflush(stdout); */
        /* } */

        gettimeofday(&end_time, 0);
        elapsed_ms = ( (end_time.tv_sec-start_time.tv_sec)*1000000LL + end_time.tv_usec -
                        start_time.tv_usec ) / 1000;
        if (elapsed_ms > max_wait_ms) {
            printf("==PDC_CLIENT[%d]: exceeded max IO request waiting time...\n", pdc_client_mpi_rank_g);
            break;
        }

        /* printf("==PDC_CLIENT[%d]: waiting for server to finish IO request\n", pdc_client_mpi_rank_g); */
        /* if (pdc_client_mpi_rank_g == 0) { */
        /*     printf("==PDC_CLIENT[ALL]: waiting for server to finish IO request...\n"); */
        /*     fflush(stdout); */
        /* } */
        pdc_msleep(check_interval_ms);
    }

    /* printf("==PDC_CLIENT[%d]: IO request completed by server\n", pdc_client_mpi_rank_g); */
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_iwrite(pdc_metadata_t *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    request->server_id   = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    if (request->n_client == 0) 
        request->n_client = pdc_nclient_per_server_g;    // Set by env var PDC_NCLIENT_PER_SERVER, default 1
    if (request->n_update == 0) 
        request->n_update = 1;       // Only set to default value if it is not set prior
    request->access_type = WRITE;
    request->metadata    = meta;
    request->region      = region;
    request->buf         = buf;
/* printf("==PDC_CLIENT[%d], sending write request to server %d\n", pdc_client_mpi_rank_g, request->server_id); */

    /* printf("==PDC_CLIENT: PDC_Client_iwrite - sending data server write with data: [%s]\n", buf); */
    ret_value = PDC_Client_data_server_write(request);

    FUNC_LEAVE(ret_value);
}

hg_return_t PDC_Client_work_done_cb(const struct hg_cb_info *callback_info)
{
    work_todo_g--;

    /* printf("==PDC_CLIENT[%d]: received confirmation of server write finish\n", pdc_client_mpi_rank_g); */
    /* fflush(stdout); */

    return HG_SUCCESS;
}

// PDC_Client_write is done using PDC_Client_iwrite and PDC_Client_wait
perr_t PDC_Client_write(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf)
{
    PDC_Request_t request;
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    request.n_update = 1;
    request.n_client = 1;
    ret_value = PDC_Client_iwrite(meta, region, &request, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_write - PDC_Client_iwrite error\n");
        goto done;
    }
    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_write - PDC_Client_wait error\n");
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

// PDC_Client_write is done using PDC_Client_iwrite and PDC_Client_wait
perr_t PDC_Client_write_id(pdcid_t local_obj_id, struct PDC_region_info *region, void *buf)
{
    PDC_Request_t request;
    struct PDC_id_info *info;
    struct PDC_obj_info *object;
    pdc_metadata_t *meta;
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);
 
    info = pdc_find_id(local_obj_id);
    if(info == NULL) {
        printf("==PDC_CLIENT[%d]: %s - obj_id %" PRIu64 " invalid!\n", 
                pdc_client_mpi_rank_g, __func__, local_obj_id);
        ret_value = FAIL;
        goto done;
    }
    object = (struct PDC_obj_info *)(info->obj_ptr);
    meta = object->obj_pt->metadata;
    if (meta == NULL) {
        printf("==PDC_CLIENT[%d]: %s - metadata is NULL!\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    request.n_update = 1;
    request.n_client = 1;
    ret_value = PDC_Client_iwrite(meta, region, &request, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_write - PDC_Client_iwrite error\n");
        goto done;
    }
    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_write - PDC_Client_wait error\n");
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_iread(pdc_metadata_t *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    request->server_id   = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    if (request->n_client == 0) 
        request->n_client    = pdc_nclient_per_server_g;    // Set by env var PDC_NCLIENT_PER_SERVER, default 1
    if (request->n_update == 0) 
        request->n_update    = 1;       // Only set to default value if it is not set prior
    request->access_type = READ;
    request->metadata    = meta;
    request->region      = region;
    request->buf         = buf;

/* printf("==PDC_CLIENT[%d], sending read request to server %d\n", pdc_client_mpi_rank_g, request->server_id); */
    ret_value = PDC_Client_data_server_read(request);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_iread- PDC_Client_data_server_read error\n");
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_read(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf)
{
    PDC_Request_t request;
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    request.n_update = 1;
    ret_value = PDC_Client_iread(meta, region, &request, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT[%d]: %s - PDC_Client_iread error\n", pdc_client_mpi_rank_g, __func__);
        goto done;
    }

    pdc_msleep(500);

    ret_value = PDC_Client_wait(&request, 60000, 500);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT[%d]: %s - PDC_Client_wait error\n", pdc_client_mpi_rank_g, __func__);
        goto done;
    }

    /* printf("==PDC_CLIENT: PDC_Client_read - Done\n"); */
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_write_wait_notify(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf)
{
    PDC_Request_t *request = (PDC_Request_t*)malloc(sizeof(PDC_Request_t));
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_iwrite(meta, region, request, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT[%d]: Failed to send write request to server\n", pdc_client_mpi_rank_g);
        goto done;
    }

    DL_PREPEND(pdc_io_request_list_g, request);
    printf("==PDC_CLIENT[%d]: Finished sending write request to server, waiting for notification\n", 
            pdc_client_mpi_rank_g);
    fflush(stdout);

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    printf("==PDC_CLIENT[%d]: received write finish notification\n", pdc_client_mpi_rank_g);
    fflush(stdout);

done:
    FUNC_LEAVE(ret_value);
}


perr_t PDC_Client_read_wait_notify(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf)
{
    PDC_Request_t *request = (PDC_Request_t*)malloc(sizeof(PDC_Request_t));
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_Client_iread(meta, region, request, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT[%d]: Faile to send read request to server\n", pdc_client_mpi_rank_g);
        goto done;
    }

    DL_PREPEND(pdc_io_request_list_g, request);
    /* printf("==PDC_CLIENT: Finished sending read request to server, waiting for notification\n"); */
    /* fflush(stdout); */

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
PDC_Client_add_del_objects_to_container_cb(const struct hg_cb_info *callback_info)
{
    hg_handle_t handle = callback_info->info.forward.handle;
    pdc_int_ret_t bulk_rpc_ret;
    hg_return_t ret = HG_SUCCESS;
    update_region_storage_meta_bulk_args_t *cb_args = (update_region_storage_meta_bulk_args_t*)callback_info->arg;

    // Sent the bulk handle with rpc and get a response
    ret = HG_Get_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get output\n");
        goto done;
    }

    /* printf("==PDC_SERVER[%d]: received rpc response from %d!\n", pdc_server_rank_g, cb_args->server_id); */
    /* fflush(stdout); */

    ret = HG_Free_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free output\n");
        goto done;
    }

    /* cb_args->cb(); */

    /* Free memory handle */
    ret = HG_Bulk_free(cb_args->bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free bulk data handle\n");
        goto done;
    }

    HG_Destroy(cb_args->rpc_handle);
done:
    work_todo_g--;
    return ret;
} // end of PDC_Client_add_del_objects_to_container_cb

// Add/delete a number of objects to one container
perr_t PDC_Client_add_del_objects_to_container(int nobj, uint64_t *obj_ids, uint64_t cont_meta_id, int op)
{
    perr_t      ret_value = SUCCEED;
    hg_return_t hg_ret = HG_SUCCESS;
    hg_handle_t rpc_handle;
    hg_bulk_t   bulk_handle;
    uint32_t    server_id;
    hg_size_t   buf_sizes[3];
    cont_add_del_objs_rpc_in_t bulk_rpc_in;
    // Reuse the existing args structure
    update_region_storage_meta_bulk_args_t cb_args;

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_obj_id(cont_meta_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==PDC_CLIENT[%d]: %s - ERROR with PDC_Client_lookup_server\n", 
                pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // Send the bulk handle to the target with RPC
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, 
                       cont_add_del_objs_rpc_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create handle\n");
        ret_value = FAIL;
        goto done;
    }

    buf_sizes[0] = sizeof(uint64_t)*nobj;

    /* Register memory */
    hg_ret = HG_Bulk_create(send_class_g, 1, (void**)&obj_ids, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        ret_value = FAIL;
        goto done;
    }

    /* Fill input structure */
    bulk_rpc_in.op          = op;
    bulk_rpc_in.cnt         = nobj;
    bulk_rpc_in.origin      = pdc_client_mpi_rank_g;
    bulk_rpc_in.cont_id     = cont_meta_id;
    bulk_rpc_in.bulk_handle = bulk_handle;

    /* cb_args.cb   = cb; */
    /* cb_args.meta_list_target = meta_list_target; */
    /* cb_args.n_updated = n_updated; */
    /* cb_args.server_id = server_id; */
    cb_args.bulk_handle = bulk_handle;
    cb_args.rpc_handle  = rpc_handle;

    /* Forward call to remote addr */
    hg_ret = HG_Forward(rpc_handle, PDC_Client_add_del_objects_to_container_cb, &cb_args, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not forward call\n");
        ret_value = FAIL;
        goto done;
    }

    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    ret_value = SUCCEED;
done:
    FUNC_LEAVE(ret_value);
}

// Add a number of objects to one container
perr_t PDC_Client_add_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id)
{
    int i;
    perr_t      ret_value = SUCCEED;
    uint64_t    *obj_ids;
    uint64_t    cont_meta_id;
    struct PDC_id_info *id_info = NULL;

    FUNC_ENTER(NULL);

    obj_ids = (uint64_t*)malloc(sizeof(uint64_t)*nobj);
    for (i = 0; i < nobj; i++) {
        id_info = pdc_find_id(local_obj_ids[i]);
        obj_ids[i] = ((struct PDC_obj_info*)(id_info->obj_ptr))->meta_id;
    }

    id_info = pdc_find_id(local_cont_id);
    cont_meta_id = ((struct PDC_cont_info *)(id_info->obj_ptr))->meta_id;
 
    ret_value = PDC_Client_add_del_objects_to_container(nobj, obj_ids, cont_meta_id, ADD_OBJ);

done:
    /* free(obj_ids); */
    FUNC_LEAVE(ret_value);
}

// Delete a number of objects to one container
perr_t PDC_Client_del_objects_to_container(int nobj, pdcid_t *local_obj_ids, pdcid_t local_cont_id)
{
    int i;
    perr_t      ret_value = SUCCEED;
    uint64_t    *obj_ids;
    uint64_t    cont_meta_id;
    struct PDC_id_info *id_info = NULL;

    FUNC_ENTER(NULL);

    obj_ids = (uint64_t*)malloc(sizeof(uint64_t)*nobj);
    for (i = 0; i < nobj; i++) {
        id_info = pdc_find_id(local_obj_ids[i]);
        obj_ids[i] = ((struct PDC_obj_info*)(id_info->obj_ptr))->meta_id;
    }

    id_info = pdc_find_id(local_cont_id);
    cont_meta_id = ((struct PDC_cont_info *)(id_info->obj_ptr))->meta_id;
 
    ret_value = PDC_Client_add_del_objects_to_container(nobj, obj_ids, cont_meta_id, DEL_OBJ);

done:
    /* free(obj_ids); */
    FUNC_LEAVE(ret_value);
}

// Query container with name, retrieve all metadata within that container
perr_t PDC_Client_query_container_name(const char *cont_name, pdc_metadata_t **out)
{
    perr_t                ret_value = SUCCEED;
    hg_return_t           hg_ret    = 0;
    uint32_t               hash_name_value, server_id;
    container_query_in_t  in;
    metadata_query_args_t lookup_args;
    hg_handle_t           metadata_query_handle;
    
    FUNC_ENTER(NULL);

    /* printf("==PDC_CLIENT[%d]: search container name [%s]\n", pdc_client_mpi_rank_g, cont_name); */
    /* fflush(stdout); */

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(cont_name);
    server_id = hash_name_value % pdc_server_num_g;

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g, 
              &metadata_query_handle);

    // Fill input structure
    in.cont_name  = cont_name;
    in.hash_value = hash_name_value;

    hg_ret = HG_Forward(metadata_query_handle, metadata_query_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d] - PDC_Client_query_metadata_with_name(): Could not start HG_Forward()\n",
                pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* printf("==PDC_CLIENT[%d]: received container [%s] query result\n", pdc_client_mpi_rank_g, in.cont_name; */
    *out = lookup_args.data;


done:
    HG_Destroy(metadata_query_handle);
    FUNC_LEAVE(ret_value);
}


static hg_return_t
PDC_Client_query_read_obj_name_cb(const struct hg_cb_info *callback_info)
{
    hg_handle_t handle = callback_info->info.forward.handle;
    pdc_int_ret_t bulk_rpc_ret;
    hg_return_t ret = HG_SUCCESS;
    update_region_storage_meta_bulk_args_t *cb_args = (update_region_storage_meta_bulk_args_t*)callback_info->arg;

    // Sent the bulk handle with rpc and get a response
    ret = HG_Get_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get output\n");
        goto done;
    }

    /* printf("==PDC_SERVER[%d]: received rpc response from %d!\n", pdc_server_rank_g, cb_args->server_id); */
    /* fflush(stdout); */

    ret = HG_Free_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free output\n");
        goto done;
    }

    /* Free memory handle */
    ret = HG_Bulk_free(cb_args->bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free bulk data handle\n");
        goto done;
    }

    /* Free other malloced resources*/

    HG_Destroy(cb_args->rpc_handle);
done:
    work_todo_g--;
    return ret;
} // end PDC_Client_query_read_obj_name_cb 

perr_t PDC_add_request_to_list(PDC_Request_t **list_head, PDC_Request_t *request)
{
    perr_t      ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (list_head == NULL || request == NULL) {
        printf("==PDC_CLIENT[%d]: %s - invalid input\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    request->seq_id = pdc_io_request_seq_id++;
    DL_PREPEND(*list_head, request);

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_del_request_from_list(PDC_Request_t **list_head, PDC_Request_t *request)
{
    perr_t      ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (list_head == NULL || request == NULL) {
        printf("==PDC_CLIENT[%d]: %s - invalid input\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    request->seq_id = pdc_io_request_seq_id++;
    DL_DELETE(*list_head, request);

done:
    FUNC_LEAVE(ret_value);
}

PDC_Request_t *PDC_find_request_from_list_by_seq_id(PDC_Request_t **list_head, int seq_id)
{
    PDC_Request_t *ret_value = NULL;
    PDC_Request_t *elt;

    FUNC_ENTER(NULL);

    if (list_head == NULL || seq_id < PDC_SEQ_ID_INIT_VALUE || seq_id > 99999) {
        printf("==PDC_CLIENT[%d]: %s - invalid input\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    DL_FOREACH(*list_head, elt)
        if (elt->seq_id == seq_id) 
            return elt;

done:
    FUNC_LEAVE(ret_value);
}

// Query and read objects with obj name, read data is stored in user provided buf
perr_t PDC_Client_query_name_read_entire_obj(int nobj, char **obj_names, void ***out_buf, 
                                             uint64_t **out_buf_sizes)
{
    perr_t      ret_value = SUCCEED;
    hg_return_t hg_ret = HG_SUCCESS;
    hg_handle_t rpc_handle;
    hg_bulk_t   bulk_handle;
    uint32_t    server_id;
    size_t   *buf_sizes, total_size;
    int i;
    query_read_obj_name_in_t bulk_rpc_in;
    // Reuse the existing args structure
    update_region_storage_meta_bulk_args_t cb_args;
    PDC_Request_t *request;

    FUNC_ENTER(NULL);

    if (nobj == 0 || obj_names == NULL || out_buf == NULL || out_buf_sizes == NULL) {
        printf("==PDC_CLIENT[%d]: %s - invalid input\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    server_id = PDC_get_local_server_id(pdc_client_mpi_rank_g, pdc_nclient_per_server_g, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==PDC_CLIENT[%d]: %s - ERROR with PDC_Client_lookup_server\n", 
                pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // Send the bulk handle to the target with RPC
    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, 
                       query_read_obj_name_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create handle\n");
        ret_value = FAIL;
        goto done;
    }

    total_size = 0;
    buf_sizes = (size_t*)calloc(sizeof(size_t), nobj);
    for (i = 0; i < nobj; i++) {
        /* printf("==PDC_CLIENT[%d]: %s - obj_name: %s\n", pdc_client_mpi_rank_g, __func__, obj_names[i]); */
        /* fflush(stdout); */
        buf_sizes[i] = strlen(obj_names[i]) + 1;
        total_size += buf_sizes[i];
    }

    /* Register memory */
    hg_ret = HG_Bulk_create(send_class_g, nobj, (void**)obj_names, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        ret_value = FAIL;
        goto done;
    }

    request = (PDC_Request_t*)calloc(1, sizeof(PDC_Request_t));
    request->server_id   = server_id;
    request->access_type = READ;
    request->n_buf_arr   = nobj;
    request->buf_arr     = out_buf;
    request->shm_size_arr= out_buf_sizes;
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
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not forward call\n");
        ret_value = FAIL;
        goto done;
    }

    // Wait for RPC response
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    // Wait for server to complete all reads
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    FUNC_LEAVE(ret_value);
} // end PDC_Client_query_name_read_entire_obj

// Copies the data from server's shm to user buffer
// Assumes the shm_addrs are avialable
perr_t PDC_Client_complete_read_request(int nbuf, PDC_Request_t *req)
{
    perr_t      ret_value = SUCCEED;
    int         i, cnt;
    uint64_t      *sizet_ptr;
    PDC_Request_t *request;

    FUNC_ENTER(NULL);

    *req->buf_arr = (void**)calloc(nbuf, sizeof(void*));
    req->shm_fd_arr = (int*)calloc(nbuf, sizeof(int));
    req->shm_base_arr = (char**)calloc(nbuf, sizeof(char*));

    for (i = 0; i < nbuf; i++) {

        /* open the shared memory segment as if it was a file */
        req->shm_fd_arr[i] = shm_open(req->shm_addr_arr[i], O_RDONLY, 0666);
        if (req->shm_fd_arr[i] == -1) {
            printf("==PDC_CLIENT: Shared memory open failed [%s]!\n", req->shm_addr_arr[i]);
            ret_value = FAIL;
            goto done;
        }

        /* map the shared memory segment to the address space of the process */
        req->shm_base_arr[i] = mmap(0, (*req->shm_size_arr)[i], PROT_READ, MAP_SHARED, req->shm_fd_arr[i], 0);
        if (req->shm_base_arr[i] == MAP_FAILED) {
            printf("==PDC_CLIENT: Map failed: %s\n", strerror(errno));
            ret_value = FAIL;
            goto done;
        }

        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start;
        struct timeval  pdc_timer_end;
        gettimeofday(&pdc_timer_start, 0);
        #endif

        // Copy data
        /* printf("==PDC_SERVER: memcpy size = %" PRIu64 "\n", data_size); */
        (*req->buf_arr)[i] = (void*)malloc((*req->shm_size_arr)[i]);
        memcpy((*req->buf_arr)[i], req->shm_base_arr[i], (*req->shm_size_arr)[i]);

        #ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        memcpy_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        #endif

        /* remove the mapped shared memory segment from the address space of the process */
        if (munmap(req->shm_base_arr[i], (*req->shm_size_arr)[i]) == -1) {
            printf("==PDC_CLIENT: Unmap failed: %s\n", strerror(errno));
            ret_value = FAIL;
            goto done;
        }

        /* close the shared memory segment as if it was a file */
        if (close(req->shm_fd_arr[i]) == -1) {
            printf("==PDC_CLIENT: Close failed!\n");
            ret_value = FAIL;
            goto done;
        }

        /* remove the shared memory segment from the file system */
        if (shm_unlink(req->shm_addr_arr[i]) == -1) {
            ret_value = FAIL;
            goto done;
            printf("==PDC_CLIENT: Error removing %s\n", req->shm_addr_arr[i]);
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_query_read_complete(char *shm_addrs, int size, int n_shm, int seq_id)
{
    perr_t      ret_value = SUCCEED;
    int         i, cnt;
    uint64_t      *sizet_ptr;
    PDC_Request_t *request;

    FUNC_ENTER(NULL);

    request = PDC_find_request_from_list_by_seq_id(&pdc_io_request_list_g, seq_id);
    if (request == NULL) {
        printf("==PDC_CLIENT[%d]: mismatch in number of shm received expect %d, actual %d\n", n_shm, i);
        ret_value = FAIL;
        goto done;
    }

    request->shm_addr_arr = (char**)calloc(n_shm, sizeof(char*));
    /* printf("shm_addrs: \n"); */
    cnt = 0;
    for (i = 0; i < size - 1; i++) {
        if (i == 0 || (i > 1 && shm_addrs[i-1] == 0)) {
            request->shm_addr_arr[cnt] = &shm_addrs[i];
            i+= strlen(&shm_addrs[i]);
            cnt++;
            if (cnt >= n_shm) 
                break;
        }
    }
    (*request->shm_size_arr) = (uint64_t*)&shm_addrs[i+1];
    /* for (i = 0; i < n_shm; i++) */ 
    /*     printf("[%s] size %" PRIu64 "\n", request->shm_addr_arr[i], (*request->shm_size_arr)[i]); */

    PDC_Client_complete_read_request(n_shm, request);

done:
    work_todo_g = 0;
    FUNC_LEAVE(ret_value);
} // End PDC_Client_query_read_complete

// Generic function to check the return value (RPC receipt) is 1
hg_return_t pdc_client_check_int_ret_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    pdc_int_ret_t output;

    FUNC_ENTER(NULL);

    hg_handle_t handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==%s() - Error with HG_Get_output\n", __func__);
        goto done;
    }

    if (output.ret != 1) {
        printf("==%s() - Return value [%d] is NOT expected\n", __func__, output.ret);
    }
done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
perr_t PDC_Client_server_checkpoint(uint32_t server_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    PDC_lifetime obj_life;
    pdc_int_send_t in;
    struct client_lookup_args lookup_args;
    hg_handle_t rpc_handle;
    
    FUNC_ENTER(NULL);
    
    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
        printf("==CLIENT[%d]: ERROR with PDC_Client_lookup_server\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, server_checkpoint_rpc_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_CLIENT[%d]: %s - Could not create handle\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    in.origin = pdc_client_mpi_rank_g;
    hg_ret = HG_Forward(rpc_handle, pdc_client_check_int_ret_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_CLIENT[%d]: %s - Could not start forward to server\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    HG_Destroy(rpc_handle);
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


perr_t PDC_Client_all_server_checkpoint()
{
    perr_t      ret_value = SUCCEED;
    uint32_t    i;

    FUNC_ENTER(NULL);

    if (pdc_server_num_g == 0) {
        printf("==PDC_CLIENT[%d]: %s - server number not initialized!\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // only let client rank 0 send all requests
    if (pdc_client_mpi_rank_g != 0) {
        goto done;
    }

    for (i = 0; i < pdc_server_num_g; i++) 
        ret_value = PDC_Client_server_checkpoint(i);

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_attach_metadata_to_local_obj(char *obj_name, uint64_t obj_id, uint64_t cont_id, 
                                               struct PDC_obj_prop *obj_prop)
{
    perr_t      ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    obj_prop->metadata = (pdc_metadata_t*)calloc(1, sizeof(pdc_metadata_t));
    obj_prop->metadata->user_id = obj_prop->user_id;
    if (NULL != obj_prop->app_name) 
        strcpy(obj_prop->metadata->app_name, obj_prop->app_name);
    if (NULL != obj_name) 
        strcpy(obj_prop->metadata->obj_name, obj_name);
    obj_prop->metadata->time_step = obj_prop->time_step;
    obj_prop->metadata->obj_id  = obj_id;
    obj_prop->metadata->cont_id = cont_id;
    if (NULL != obj_prop->tags) 
        strcpy(obj_prop->metadata->tags, obj_prop->tags);
    if (NULL != obj_prop->data_loc) 
        strcpy(obj_prop->metadata->data_location, obj_prop->data_loc);
    obj_prop->metadata->ndim    = obj_prop->ndim;
        if (NULL != obj_prop->dims) 
    memcpy(obj_prop->metadata->dims, obj_prop->dims, sizeof(uint64_t)*obj_prop->ndim);

done:
    FUNC_LEAVE(ret_value);
}

// Query and read objects with obj name, read data is stored in user provided buf
// Each request is sent to the server with its storage metadata, data is transferred 
// back with bulk or shm
perr_t PDC_Client_query_name_read_entire_obj_by_mserver(int nobj, char **obj_names, void ***out_buf, 
                                             uint64_t **out_buf_sizes)
{
    perr_t      ret_value = SUCCEED;
    hg_return_t hg_ret = HG_SUCCESS;
    hg_handle_t rpc_handle;
    hg_bulk_t   bulk_handle;
    uint32_t    server_id;
    size_t     *buf_sizes = NULL, total_size;
    int         i, *n_obj_name_by_server = NULL;
    int **obj_names_server_seq_mapping = NULL, *obj_names_server_seq_mapping_1d;
    int         send_n_request = 0;
    char        ***obj_names_by_server = NULL;
    query_read_obj_name_in_t bulk_rpc_in;
    update_region_storage_meta_bulk_args_t cb_args;
    PDC_Request_t *request = NULL;

    FUNC_ENTER(NULL);

    if (nobj == 0 || obj_names == NULL || out_buf == NULL || out_buf_sizes == NULL) {
        printf("==PDC_CLIENT[%d]: %s - invalid input\n", pdc_client_mpi_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // Sort obj_names based on their metadata server id
    char **obj_names_by_server_1d = (char**)calloc(sizeof(char*), nobj);
    for (i = 0; i < nobj; i++) 
        obj_names_by_server[i] = obj_names_by_server_1d[i];

    obj_names_by_server             = (char***)calloc(sizeof(char**), pdc_server_num_g);
    n_obj_name_by_server            = (int*)calloc(sizeof(int), pdc_server_num_g);
    obj_names_server_seq_mapping    = (int**)calloc(sizeof(int*), pdc_server_num_g);
    obj_names_server_seq_mapping_1d = (int*)calloc(sizeof(int), nobj*pdc_server_num_g);
    for (i = 0; i < pdc_server_num_g; i++) {
        obj_names_server_seq_mapping[i] = obj_names_server_seq_mapping_1d + nobj;
    }

    
    for (i = 0; i < nobj; i++)  {
        server_id = PDC_get_server_by_name(obj_names[i], pdc_server_num_g);
        obj_names_by_server[server_id][n_obj_name_by_server[server_id]] = obj_names[i];
        obj_names_server_seq_mapping[server_id][n_obj_name_by_server[server_id]] = i;
        n_obj_name_by_server[server_id]++;
    }

    // TODO: need to match back the original order of buf to obj_names

    // Now send the corresponding names to each server that has its metadata
    for (server_id = 0; server_id < pdc_server_num_g; server_id++) {

        if (n_obj_name_by_server[server_id] == 0) {
            send_n_request++;
            continue;
        }
        debug_server_id_count[server_id]++;

        if( PDC_Client_try_lookup_server(server_id) != SUCCEED) {
            printf("==PDC_CLIENT[%d]: %s - ERROR with PDC_Client_lookup_server\n", 
                    pdc_client_mpi_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        // Send the bulk handle to the target with RPC
        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, 
                           query_read_obj_name_register_id_g, &rpc_handle);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not create handle\n");
            ret_value = FAIL;
            goto done;
        }

        total_size = 0;
        buf_sizes = (size_t*)calloc(sizeof(size_t), n_obj_name_by_server[server_id]);
        for (i = 0; i < n_obj_name_by_server[server_id]; i++) {
            buf_sizes[i] = strlen(obj_names_by_server[server_id][i]) + 1;
            total_size += buf_sizes[i];
        }

        hg_ret = HG_Bulk_create(send_class_g, n_obj_name_by_server[server_id], 
                    (void**)obj_names_by_server[server_id], buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not create bulk data handle\n");
            ret_value = FAIL;
            goto done;
        }

        request = (PDC_Request_t*)calloc(1, sizeof(PDC_Request_t));
        request->server_id    = server_id;
        request->access_type  = READ;
        request->n_buf_arr    = obj_names_by_server[server_id];
        request->buf_arr      = out_buf;
        request->shm_size_arr = out_buf_sizes;
        request->buf_arr_idx  = obj_names_server_seq_mapping[server_id];

        PDC_add_request_to_list(&pdc_io_request_list_g, request);

        /* Fill input structure */
        bulk_rpc_in.client_seq_id = request->seq_id;
        bulk_rpc_in.cnt           = n_obj_name_by_server[server_id];
        bulk_rpc_in.total_size    = total_size;
        bulk_rpc_in.origin        = pdc_client_mpi_rank_g;
        bulk_rpc_in.bulk_handle   = bulk_handle;

        cb_args.bulk_handle = bulk_handle;
        cb_args.rpc_handle  = rpc_handle;

        /* Forward call to remote addr */
        hg_ret = HG_Forward(rpc_handle, PDC_Client_query_read_obj_name_cb, &cb_args, &bulk_rpc_in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not forward call\n");
            ret_value = FAIL;
            goto done;
        }

        // Wait for RPC response
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);
    }

    // Wait for server to complete all reads
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

done:
    if (NULL != obj_names_by_server) 
        free(obj_names_by_server);
    if (NULL != n_obj_name_by_server)
        free(n_obj_name_by_server);
    if (NULL != obj_names_by_server_1d)
        free(obj_names_by_server_1d);
    
    FUNC_LEAVE(ret_value);
} // end PDC_Client_query_name_read_entire_obj


