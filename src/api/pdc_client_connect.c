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
#include <sys/time.h>

/* #define ENABLE_MPI 1 */
#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

#include "../server/utlist.h"

#include "mercury.h"
#include "mercury_request.h"
#include "mercury_macros.h"
#include "mercury_hash_table.h"

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

int                    pdc_server_num_g = 0;
int                    pdc_use_local_server_only_g = 0;
int                    pdc_nclient_per_server_g = 0;

char                   pdc_client_tmp_dir_g[ADDR_MAX];
pdc_server_info_t     *pdc_server_info_g = NULL;
static int            *debug_server_id_count = NULL;

PDC_Request_t           *pdc_io_request_list_g = NULL;

static int             mercury_has_init_g = 0;
static hg_class_t     *send_class_g = NULL;
static hg_context_t   *send_context_g = NULL;
static int             work_todo_g = 0;

static hg_id_t         client_test_connect_register_id_g;
static hg_id_t         gen_obj_register_id_g;
static hg_id_t         close_server_register_id_g;
/* static hg_id_t         send_obj_name_marker_register_id_g; */
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

// bulk
static hg_id_t         query_partial_register_id_g;
static int             bulk_todo_g = 0;
hg_atomic_int32_t      bulk_transfer_done_g;

static hg_id_t	       gen_reg_map_notification_register_id_g;
static hg_id_t         gen_reg_unmap_notification_register_id_g;
static hg_id_t         gen_obj_unmap_notification_register_id_g;
/* hg_hash_table_t       *obj_names_cache_hash_table_g = NULL; */

/* static inline uint32_t get_server_id_by_hash_all() */ 
/* { */

/* } */

static inline uint32_t get_server_id_by_hash_name(const char *name) 
{
    return (uint32_t)(PDC_get_hash_by_name(name) % pdc_server_num_g);
}

static inline uint32_t get_server_id_by_obj_id(uint64_t obj_id) 
{
    return (uint32_t)((obj_id / PDC_SERVER_ID_INTERVEL - 1) % pdc_server_num_g);
}

/* static int */
/* PDC_Client_int_equal(hg_hash_table_key_t vlocation1, hg_hash_table_key_t vlocation2) */
/* { */
/*     return *((int *) vlocation1) == *((int *) vlocation2); */
/* } */

/* static unsigned int */
/* PDC_Client_int_hash(hg_hash_table_key_t vlocation) */
/* { */
/*     return *((unsigned int *) vlocation); */
/* } */

/* static void */
/* PDC_Client_int_hash_key_free(hg_hash_table_key_t key) */
/* { */
/*     free((int *) key); */
/* } */

/* static void */
/* PDC_Client_int_hash_value_free(hg_hash_table_value_t value) */
/* { */
/*     free((int *) value); */
/* } */

/* typedef struct hash_value_client_obj_name_t { */
/*     char obj_name[ADDR_MAX]; */
/* } hash_value_client_obj_name_t; */

/* static void */
/* metadata_hash_value_free(hg_hash_table_value_t value) */
/* { */
/*     free((hash_value_client_obj_name_t*) value); */
/* } */

// ^ Client hash table related functions

// Check if all work has been processed
// Using global variable $mercury_work_todo_g
perr_t PDC_Client_check_response(hg_context_t **hg_context)
{
    perr_t ret_value = SUCCEED;
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
    } while (hg_ret == HG_SUCCESS);

    ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}


perr_t PDC_Client_read_server_addr_from_file()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int max_tries = 7, sleeptime = 1;
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
    pdc_server_info_g = (pdc_server_info_t*)malloc(sizeof(pdc_server_info_t) * pdc_server_num_g);
    // Fill in default values
    for (i = 0; i < pdc_server_num_g; i++) {
        pdc_server_info_g[i].client_send_region_map_handle_valid = 0;
        pdc_server_info_g[i].client_send_region_unmap_handle_valid= 0;
        pdc_server_info_g[i].client_send_object_unmap_handle_valid = 0;
        pdc_server_info_g[i].addr_valid                          = 0;
        pdc_server_info_g[i].rpc_handle_valid                    = 0;
        pdc_server_info_g[i].client_test_handle_valid            = 0;
        pdc_server_info_g[i].close_server_handle_valid           = 0;
        pdc_server_info_g[i].metadata_query_handle_valid         = 0;
        pdc_server_info_g[i].query_partial_handle_valid = 0;
        pdc_server_info_g[i].metadata_delete_handle_valid        = 0;
        pdc_server_info_g[i].metadata_delete_by_id_handle_valid= 0;
        pdc_server_info_g[i].metadata_update_handle_valid        = 0;
        pdc_server_info_g[i].metadata_add_tag_handle_valid       = 0;
        pdc_server_info_g[i].region_lock_handle_valid            = 0;
        pdc_server_info_g[i].region_release_handle_valid          = 0;
        pdc_server_info_g[i].data_server_read_handle_valid       = 0;
        pdc_server_info_g[i].data_server_write_handle_valid      = 0;
        pdc_server_info_g[i].data_server_read_check_handle_valid   = 0;
        pdc_server_info_g[i].data_server_write_check_handle_valid   = 0;
        pdc_server_info_g[i].data_server_write_handle_valid      = 0;
    }

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
        /* printf("[%d]: received server addr [%s]\n", pdc_client_mpi_rank_g, &pdc_server_info_g[i].addr_string); */
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
client_test_connect_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_test_connect_rpc_cb()\n"); */
    struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    client_test_connect_out_t output;
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: client_test_connect_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        client_lookup_args->ret = -1;
        goto done;
    }

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT[%d]: client_test_connect_rpc_cb return from server %d\n", 
                pdc_client_mpi_rank_g, output.ret);
    }
    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g = 0;
    HG_Free_output(handle, &output);
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
    gen_obj_unmap_notification_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_send_object_unmap_rpc_cb()\n"); */
    object_unmap_args = (struct object_unmap_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: client_send_object_unmap_rpc_cb error with HG_Get_output\n", 
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
client_send_region_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct region_unmap_args *region_unmap_args;
    gen_reg_unmap_notification_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_send_region_unmap_rpc_cb()\n"); */
    region_unmap_args = (struct region_unmap_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: client_send_region_unmap_rpc_cb error with HG_Get_output\n", 
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
client_send_region_map_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    struct region_map_args *region_map_args;
    gen_reg_map_notification_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_send_region_map_rpc_cb()\n"); */
    region_map_args = (struct region_map_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: client_send_region_unmap_rpc_cb error with HG_Get_output\n", 
                pdc_client_mpi_rank_g);
        region_map_args->ret = -1;
        goto done;
    }
    /* printf("Return value=%d\n", output.ret); */

    region_map_args->ret = output.ret;

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Callback function for HG_Addr_lookup()
// Start RPC connection
static hg_return_t
client_test_connect_lookup_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    uint32_t server_id;
    struct client_lookup_args *client_lookup_args;
    client_test_connect_in_t in;
    
    FUNC_ENTER(NULL);

    client_lookup_args = (struct client_lookup_args *) callback_info->arg;
    server_id = client_lookup_args->server_id;
    /* if (is_client_debug_g == 1) { */
    /*     printf("client_test_connect_lookup_cb(): server ID=%d\n", server_id); */
    /*     fflush(stdout); */
    /* } */
    pdc_server_info_g[server_id].addr = callback_info->info.lookup.addr;
    pdc_server_info_g[server_id].addr_valid = 1;

    // Create HG handle if needed
    /* if (pdc_server_info_g[server_id].client_test_handle_valid!= 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, client_test_connect_register_id_g, 
                  &pdc_server_info_g[server_id].client_test_handle);
        /* pdc_server_info_g[server_id].client_test_handle_valid= 1; */
    /* } */

    // Fill input structure
    in.client_id   = pdc_client_mpi_rank_g;
    in.nclient     = pdc_client_mpi_size_g;
    in.client_addr = client_lookup_args->client_addr;

    /* printf("Sending input to target\n"); */
    ret_value = HG_Forward(pdc_server_info_g[server_id].client_test_handle, 
                           client_test_connect_rpc_cb, client_lookup_args, &in);
    if (ret_value != HG_SUCCESS) {
        fprintf(stderr, "client_test_connect_lookup_cb(): Could not start HG_Forward()\n");
        goto done;
    }

    /* work_todo_g = 1; */
    /* PDC_Client_check_response(&send_context_g); */


done:
    HG_Destroy(pdc_server_info_g[server_id].client_test_handle);
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
    /* client_lookup_args = (struct client_lookup_args*) callback_info->arg; */
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: close_server_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
        goto done;
    }
    /* printf("Return value=%" PRIu64 "\n", output.ret); */

done:
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

// Callback function for  HG_Forward()
// Gets executed after a call to HG_Trigger and the RPC has completed
/* static hg_return_t */
/* send_name_marker_rpc_cb(const struct hg_cb_info *callback_info) */
/* { */
/*     FUNC_ENTER(NULL); */

/*     hg_return_t ret_value; */

/*     struct client_lookup_args *client_lookup_args = (struct client_lookup_args*) callback_info->arg; */
/*     hg_handle_t handle = callback_info->info.forward.handle; */

    /* // Get output from server */
/*     send_obj_name_marker_out_t output; */
/*     ret_value = HG_Get_output(handle, &output); */
/*     /1* printf("Return value=%" PRIu64 "\n", output.ret); *1/ */

/*     work_todo_g--; */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

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
        printf("==PDC_CLIENT[%d]: client_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
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
    struct client_lookup_args *client_lookup_args;
    region_lock_out_t output;
    
    FUNC_ENTER(NULL);

    /* printf("Entered client_region_lock_rpc_cb()\n"); */
    client_lookup_args = (struct client_lookup_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_CLIENT[%d]: client_rpc_cb error with HG_Get_output\n", pdc_client_mpi_rank_g);
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

    /* printf("Entered client_region_lock_rpc_cb()\n"); */
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

    work_todo_g--;

done:
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

// Bulk
// Callback after bulk transfer is received by client
static hg_return_t
hg_test_bulk_transfer_cb(const struct hg_cb_info *hg_cb_info)
{
    hg_return_t ret = HG_SUCCESS;
    struct hg_test_bulk_args *bulk_args;
    hg_bulk_t local_bulk_handle;
    /* bulk_write_out_t out_struct; */
    uint32_t i;
    void *buf = NULL;
    uint32_t n_meta;
    size_t buf_sizes[2] = {0,0};
    uint32_t actual_cnt;
    pdc_metadata_t *meta_ptr;
    
    FUNC_ENTER(NULL);
    
    bulk_args = (struct hg_test_bulk_args *)hg_cb_info->arg;
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
        /* if (hg_atomic_cas32(&close_server_g, 1, 1)) break; */
        hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME);

    } while (hg_ret == HG_SUCCESS);

    if (hg_ret == HG_SUCCESS)
        ret_value = SUCCEED;
    else
        ret_value = FAIL;

    FUNC_LEAVE(ret_value);
}

// Init Mercury class and context
// Register gen_obj_id rpc
perr_t PDC_Client_mercury_init(hg_class_t **hg_class, hg_context_t **hg_context, int port)
{
    perr_t ret_value = SUCCEED;
    char na_info_string[ADDR_MAX];
    char hostname[1024];
    int i;
    hg_return_t hg_ret;
    struct client_lookup_args lookup_args;
    char *target_addr_string;
    char self_addr[ADDR_MAX];
    
    FUNC_ENTER(NULL);

    memset(hostname, 0, 1024);
    gethostname(hostname, 1023);

    char *na_env = getenv("PDC_NA_PLUGIN");
    if (na_env != NULL) {
        if (strcmp(na_env, "BMI") == 0)
            sprintf(na_info_string, "bmi+tcp://%s:%d", hostname, port);
        else if (strcmp(na_env, "OFI") == 0)
            sprintf(na_info_string, "ofi+tcp://%s:%d", hostname, port);
        else if (strcmp(na_env, "OFIGNI") == 0)
            sprintf(na_info_string, "ofi+gni://%s:%d", hostname, port);
        else if (strcmp(na_env, "CCI") == 0)
            sprintf(na_info_string, "cci+tcp://%s:%d", hostname, port);
    }
    else
        sprintf(na_info_string, "bmi+tcp://%s:%d", hostname, port);

    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: using %.7s\n", na_info_string);
        fflush(stdout);
    }
    
    /* if (!na_info_string) { */
    /*     fprintf(stderr, HG_PORT_NAME " environment variable must be set, e.g.:\nMERCURY_PORT_NAME=\"cci+tcp://22222\"\n"); */
    /*     exit(0); */
    /* } */

    /* Initialize Mercury with the desired network abstraction class */
    /* printf("Using %s\n", na_info_string); */
    *hg_class = HG_Init(na_info_string, HG_TRUE);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        goto done;
    }

    /* Create HG context */
    *hg_context = HG_Context_create(*hg_class);

    PDC_get_self_addr(*hg_class, self_addr);

    // Register RPC
    client_test_connect_register_id_g         = client_test_connect_register(*hg_class);
    gen_obj_register_id_g                     = gen_obj_id_register(*hg_class);
    close_server_register_id_g                = close_server_register(*hg_class);
    /* send_obj_name_marker_register_id_g       = send_obj_name_marker_register(*hg_class); */
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

    // bulk
    query_partial_register_id_g               = query_partial_register(*hg_class);

    // 
    gen_reg_map_notification_register_id_g    = gen_reg_map_notification_register(*hg_class);
    gen_reg_unmap_notification_register_id_g  = gen_reg_unmap_notification_register(*hg_class);
    gen_obj_unmap_notification_register_id_g  = gen_obj_unmap_notification_register(*hg_class);

    server_lookup_client_register(*hg_class);
    notify_io_complete_register(*hg_class);
    /* server_lookup_remote_server_register(*hg_class); */
    notify_region_update_register(*hg_class);

/* #ifdef ENABLE_MPI */
/*     MPI_Barrier(MPI_COMM_WORLD); */
/* #endif */
   
    // Lookup and fill the server info
    for (i = 0; i < pdc_server_num_g; i++) {
        lookup_args.client_id = pdc_client_mpi_rank_g;
        // Avoid making all clients connect to 1 server at the same time
        lookup_args.server_id = (i + pdc_client_mpi_rank_g) % pdc_server_num_g;
        lookup_args.client_addr = self_addr;
        lookup_args.client_id   = pdc_client_mpi_rank_g;
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
    }

    /* if (is_client_debug_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: finished lookup all servers, now waiting for %d server to connect back\n", */
    /*             pdc_client_mpi_rank_g, pdc_server_num_g); */
    /*     fflush(stdout); */
    /* } */

    // Wait for server to connect back
    /* work_todo_g = pdc_server_num_g; */
    /* PDC_Client_check_response(&send_context_g); */

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    if (is_client_debug_g == 1) {
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

    // Get the number of clients per server(node) through environment variable
    tmp_dir = getenv("PDC_NCLIENT_PER_SERVER");
    if (tmp_dir == NULL)
        pdc_nclient_per_server_g = 1;
    else
        pdc_nclient_per_server_g = atoi(tmp_dir);

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

    // Split the MPI_COMM_WORLD communicator
    same_node_color = pdc_client_mpi_rank_g / pdc_nclient_per_server_g;
    MPI_Comm_split(MPI_COMM_WORLD, same_node_color, pdc_client_mpi_rank_g, &PDC_SAME_NODE_COMM_g);

    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_client_same_node_rank_g );
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_client_same_node_size_g );
#endif
 
    if (pdc_client_mpi_rank_g == 0) 
        printf("==PDC_CLIENT: PDC_DEBUG set to %d!\n", is_client_debug_g);

    // get server address and fill in $pdc_server_info_g
    if (PDC_Client_read_server_addr_from_file() != SUCCEED) {
        printf("==PDC_CLIENT[%d]: Error getting PDC Metadata servers info, exiting ...", pdc_server_num_g);
        exit(0);
    }

    if (pdc_client_mpi_rank_g == 0) {
        printf("==PDC_CLIENT: Found %d PDC Metadata servers, running with %d PDC clients\n", 
                pdc_server_num_g ,pdc_client_mpi_size_g);
    }
    /* printf("pdc_rank:%d\n", pdc_client_mpi_rank_g); */
    /* fflush(stdout); */

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
        PDC_Client_mercury_init(&send_class_g, &send_context_g, port);
        if (send_class_g == NULL || send_context_g == NULL) {
            printf("Error with Mercury Init, exiting...\n");
            exit(0);
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

    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_destroy_all_handles(pdc_server_info_t *server_info)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (server_info->addr_valid == 1) 
        HG_Addr_free(send_class_g, server_info->addr);
/*     if (server_info->rpc_handle_valid == 1) */
/*         HG_Destroy(server_info->rpc_handle); */

/*     /1* if (server_info->client_test_handle_valid == 1) *1/ */
/*     /1*     HG_Destroy(server_info->client_test_handle); *1/ */

/*     if (server_info->close_server_handle_valid == 1) */
/*         HG_Destroy(server_info->close_server_handle); */
    
/*     if (server_info->metadata_query_handle_valid == 1) */
/*         HG_Destroy(server_info->metadata_query_handle); */

/*     if (server_info->metadata_delete_handle_valid == 1) */
/*         HG_Destroy(server_info->metadata_delete_handle); */

/*     if (server_info->metadata_delete_by_id_handle_valid == 1) */
/*         HG_Destroy(server_info->metadata_delete_by_id_handle); */

/*     if (server_info->metadata_add_tag_handle_valid == 1) */
/*         HG_Destroy(server_info->metadata_add_tag_handle); */

/*     if (server_info->metadata_update_handle_valid == 1) */
/*         HG_Destroy(server_info->metadata_update_handle); */

/*    if (server_info->client_send_region_map_handle_valid == 1) */
/*        HG_Destroy(server_info->client_send_region_map_handle); */

/*    if (server_info->client_send_region_unmap_handle_valid == 1) */
/*        HG_Destroy(server_info->client_send_region_unmap_handle); */
    
/*    if (server_info->client_send_object_unmap_handle_valid == 1) */
/*        HG_Destroy(server_info->client_send_object_unmap_handle); */

/*    if (server_info->region_lock_handle_valid == 1) */
/*        HG_Destroy(server_info->region_lock_handle); */

/*    if(server_info->region_release_handle_valid == 1) */
/*        HG_Destroy(server_info->region_release_handle); */

/*     if (server_info->query_partial_handle_valid == 1) */
/*         HG_Destroy(server_info->query_partial_handle); */

/*     if (server_info->data_server_read_handle_valid == 1) */
/*         HG_Destroy(server_info->data_server_read_handle); */

/*     if (server_info->data_server_write_handle_valid == 1) */
/*         HG_Destroy(server_info->data_server_write_handle); */

/*     if (server_info->data_server_read_check_handle_valid == 1) */
/*         HG_Destroy(server_info->data_server_read_check_handle); */

/*     if (server_info->data_server_write_check_handle_valid == 1) */
/*         HG_Destroy(server_info->data_server_write_check_handle); */

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
    /*     PDC_Client_close_all_server(); */

    // Finalize Mercury
    for (i = 0; i < pdc_server_num_g; i++) {
        if (pdc_server_info_g[i].addr_valid) {
            HG_Addr_free(send_class_g, pdc_server_info_g[i].addr);
        }
        ret_value = PDC_Client_destroy_all_handles(&pdc_server_info_g[i]);
        if (ret_value != SUCCEED) {
            printf("==PDC_CLIENT[%d]: PDC_Client_finalize - error with PDC_Client_destroy_all_handles\n", 
                    pdc_client_mpi_rank_g);
            goto done;
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
        printf("==PDC_CLIENT[%d]: PDC_Client_finalize - error with HG_Context_destroy\n", pdc_client_mpi_rank_g);
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

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
done:
    if (is_client_debug_g == 1 && pdc_client_mpi_rank_g == 0) {
        printf("PDC_CLIENT: finalized\n");
    }
    FUNC_LEAVE(ret_value);
}

/* perr_t PDC_Client_send_obj_name_mark(const char *obj_name, uint32_t server_id) */
/* { */
/*     FUNC_ENTER(NULL); */

/*     perr_t ret_value = SUCCEED; */
/*     hg_return_t  hg_ret = 0; */

/*     // Debug statistics for counting number of messages sent to each server. */
/*     debug_server_id_count[server_id]++; */

/*     // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb */ 
/*     if (pdc_server_info_g[server_id].name_marker_handle_valid != 1) { */
/*         HG_Create(send_context_g, pdc_server_info_g[server_id].addr, send_obj_name_marker_register_id_g, &pdc_server_info_g[server_id].name_marker_handle); */
/*         pdc_server_info_g[server_id].name_marker_handle_valid  = 1; */
/*     } */

/*     // Fill input structure */
/*     send_obj_name_marker_in_t in; */
/*     in.obj_name   = obj_name; */
/*     in.hash_value = PDC_get_hash_by_name(obj_name); */

/*     /1* printf("Sending input to target\n"); *1/ */
/*     struct client_lookup_args lookup_args; */
/*     hg_ret = HG_Forward(pdc_server_info_g[server_id].name_marker_handle, send_name_marker_rpc_cb, &lookup_args, &in); */
/*     if (ret_value != HG_SUCCESS) { */
/*         fprintf(stderr, "PDC_Client_send_name_to_server(): Could not start HG_Forward()\n"); */
/*         return EXIT_FAILURE; */
/*     } */

/*     // Wait for response from server */
/*     work_todo_g = 1; */
/*     PDC_Client_check_response(&send_context_g); */

/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

// Bulk
static hg_return_t
metadata_query_bulk_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    struct hg_test_bulk_args *client_lookup_args;
    hg_handle_t handle;
    metadata_query_transfer_out_t output;
    uint32_t n_meta;
    hg_op_id_t hg_bulk_op_id;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t origin_bulk_handle = HG_BULK_NULL;
    const struct hg_info *hg_info = NULL;
    struct hg_test_bulk_args *bulk_args;
    void *recv_meta;
    
    FUNC_ENTER(NULL);

    /* printf("Entered metadata_query_bulk_cb()\n"); */
    client_lookup_args = (struct hg_test_bulk_args*) callback_info->arg;
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

    bulk_args = (struct hg_test_bulk_args *) malloc(sizeof(struct hg_test_bulk_args));

    bulk_args->handle = handle;
    bulk_args->nbytes = HG_Bulk_get_size(origin_bulk_handle);
    bulk_args->n_meta = client_lookup_args->n_meta;

    /* printf("nbytes=%u\n", bulk_args->nbytes); */
    recv_meta = (void *)malloc(sizeof(pdc_metadata_t) * n_meta);

    /* Create a new bulk handle to read the data */
    HG_Bulk_create(hg_info->hg_class, 1, &recv_meta, (hg_size_t *) &bulk_args->nbytes, HG_BULK_READWRITE, &local_bulk_handle);

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

perr_t PDC_partial_query(int is_list_all, int user_id, const char* app_name, const char* obj_name, int time_step_from, 
                         int time_step_to, int ndim, const char* tags, int *n_res, pdc_metadata_t ***out)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    int n_recv = 0;
    uint32_t i, server_id = 0, my_server_start, my_server_end, my_server_count;
    size_t out_size = 0;

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

        // We may have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb
        // TODO: the handle becomes invalid after one pratial query 
        /* if (pdc_server_info_g[server_id].query_partial_handle_valid != 1) { */
            hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, query_partial_register_id_g, 
                                &pdc_server_info_g[server_id].query_partial_handle);
            /* pdc_server_info_g[server_id].query_partial_handle_valid= 1; */
        /* } */

        /* printf("Sending input to target\n"); */
        struct hg_test_bulk_args lookup_args;
        if (pdc_server_info_g[server_id].query_partial_handle == NULL) {
            printf("==CLIENT[%d]: Error with query_partial_handle\n", pdc_client_mpi_rank_g);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Forward(pdc_server_info_g[server_id].query_partial_handle, metadata_query_bulk_cb, &lookup_args, &in);
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
    } // for server_id

    
    /* printf("Received %u metadata.\n", *(lookup_args.n_meta)); */
    /* for (i = 0; i < *n_res; i++) { */
    /*     PDC_print_metadata(lookup_args.meta_arr[i]); */
    /* } */

    // TODO: need to be careful when freeing the lookup_args, as it include the results returned to user
done:
    HG_Destroy(pdc_server_info_g[server_id].query_partial_handle);
    FUNC_LEAVE(ret_value);
}

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
            printf("==PDC_CLIENT: ERROR - PDC_Client_query_metadata_name_only() cannnot allocate space for client_lookup_args->data \n");
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

    uint32_t hash_name_value = PDC_get_hash_by_name(old->obj_name);
    uint32_t server_id = (hash_name_value + old->time_step);
    server_id %= pdc_server_num_g;

    /* printf("==PDC_CLIENT: PDC_Client_add_tag_metadata() - hash(%s)=%u\n", old->obj_name, hash_name_value); */

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].metadata_add_tag_handle_valid != 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_add_tag_register_id_g, 
                    &pdc_server_info_g[server_id].metadata_add_tag_handle);
        /* pdc_server_info_g[server_id].metadata_add_tag_handle_valid  = 1; */
    /* } */

    // Fill input structure
    metadata_add_tag_in_t in;
    in.obj_id     = old->obj_id;
    in.hash_value = hash_name_value;

    if (tag != NULL && tag[0] != 0) {
        in.new_tag            = tag;
    }
    else {
        printf("==PDC_Client_add_tag(): invalid tag content!\n");
        fflush(stdout);
        goto done;
    }


    /* printf("Sending input to target\n"); */
    struct client_lookup_args lookup_args;
    hg_ret = HG_Forward(pdc_server_info_g[server_id].metadata_add_tag_handle, metadata_add_tag_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_add_tag_metadata_with_name(): Could not start HG_Forward()\n");
        return FAIL;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1) 
        printf("==PDC_CLIENT: add tag NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(pdc_server_info_g[server_id].metadata_add_tag_handle);
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

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].metadata_update_handle_valid != 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_update_register_id_g, 
                    &pdc_server_info_g[server_id].metadata_update_handle);
        /* pdc_server_info_g[server_id].metadata_update_handle_valid  = 1; */
    /* } */

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
    hg_ret = HG_Forward(pdc_server_info_g[server_id].metadata_update_handle, metadata_update_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_update_metadata_with_name(): Could not start HG_Forward()\n");
        return FAIL;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret != 1) 
        printf("==PDC_CLIENT: update NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(pdc_server_info_g[server_id].metadata_update_handle);
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

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].metadata_delete_by_id_handle_valid != 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_by_id_register_id_g, 
                  &pdc_server_info_g[server_id].metadata_delete_by_id_handle);
        /* pdc_server_info_g[server_id].metadata_delete_by_id_handle_valid  = 1; */
    /* } */
    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(pdc_server_info_g[server_id].metadata_delete_by_id_handle, metadata_delete_by_id_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_delete_by_id_metadata_with_name(): Could not start HG_Forward()\n");
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
        printf("==PDC_CLIENT: delete_by_id NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(pdc_server_info_g[server_id].metadata_delete_by_id_handle);
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

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].metadata_delete_handle_valid != 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_delete_register_id_g, 
                    &pdc_server_info_g[server_id].metadata_delete_handle);
        /* pdc_server_info_g[server_id].metadata_delete_handle_valid  = 1; */
    /* } */
    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(pdc_server_info_g[server_id].metadata_delete_handle, metadata_delete_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_delete_metadata_with_name(): Could not start HG_Forward()\n");
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
        printf("==PDC_CLIENT: delete NOT successful ... ret_value = %d\n", lookup_args.ret);

done:
    HG_Destroy(pdc_server_info_g[server_id].metadata_delete_handle);
    FUNC_LEAVE(ret_value);
}

// Search metadata using incomplete ID attributes 
// Currently it's only using obj_name, and search from all servers
perr_t PDC_Client_query_metadata_name_only(const char *obj_name, pdc_metadata_t **out)
/* perr_t PDC_Client_query_metadata(const char *obj_name, pdc_metadata_t **out) */
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    metadata_query_in_t in;
    metadata_query_args_t **lookup_args;
    uint32_t server_id;
    uint32_t i, count = 0;
    
    FUNC_ENTER(NULL);

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

        // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
        /* if (pdc_server_info_g[server_id].metadata_query_handle_valid != 1) { */
            HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g, 
                        &pdc_server_info_g[server_id].metadata_query_handle);
            /* pdc_server_info_g[server_id].metadata_query_handle_valid  = 1; */
        /* } */

        hg_ret = HG_Forward(pdc_server_info_g[server_id].metadata_query_handle, metadata_query_rpc_cb, lookup_args[server_id], &in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==PDC_Client_query_metadata_name_only(): Could not start HG_Forward()\n");
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
    }
    /* printf("==PDC_CLIENT: Found %d metadata with search\n", count); */

    // TODO lookup_args[i] are not freed
    //
    HG_Destroy(pdc_server_info_g[server_id].metadata_query_handle);

    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_query_metadata_name_timestep(const char *obj_name, uint32_t time_step, pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = 0;
    uint32_t hash_name_value;
    uint32_t  server_id;
    metadata_query_in_t in;
    metadata_query_args_t lookup_args;
    
    FUNC_ENTER(NULL);

    /* printf("==PDC_CLIENT[%d]: search request name [%s]\n", pdc_client_mpi_rank_g, obj_name); */
    /* fflush(stdout); */

    // Compute server id
    hash_name_value = PDC_get_hash_by_name(obj_name);
    server_id = (hash_name_value + time_step);
    server_id %= pdc_server_num_g;

    /* printf("==PDC_CLIENT[%d]: query will be sent to server id %u\n", pdc_client_mpi_rank_g, server_id); */
    /* fflush(stdout); */

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].metadata_query_handle_valid != 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, metadata_query_register_id_g, 
                  &pdc_server_info_g[server_id].metadata_query_handle);
        /* pdc_server_info_g[server_id].metadata_query_handle_valid  = 1; */
    /* } */

    // Fill input structure
    in.obj_name   = obj_name;
    in.hash_value = PDC_get_hash_by_name(obj_name);
    in.time_step  = time_step;

    /* printf("==PDC_CLIENT[%d]: search request obj_name [%s], hash value %u, server id %u\n", pdc_client_mpi_rank_g, in.obj_name, in.hash_value, server_id); */
    /* fflush(stdout); */
    /* printf("Sending input to target\n"); */
    // client_lookup_args->data is a pdc_metadata_t
    lookup_args.data = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    if (lookup_args.data == NULL) {
        printf("==PDC_CLIENT: ERROR - PDC_Client_query_metadata_with_name() cannnot allocate space for client_lookup_args->data \n");
    }
    hg_ret = HG_Forward(pdc_server_info_g[server_id].metadata_query_handle, metadata_query_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_query_metadata_with_name(): Could not start HG_Forward()\n");
        return FAIL;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    /* printf("==PDC_CLIENT[%d]: received query result name [%s], hash value %u\n", pdc_client_mpi_rank_g, in.obj_name, in.hash_value); */
    *out = lookup_args.data;

    HG_Destroy(pdc_server_info_g[server_id].metadata_query_handle);
    FUNC_LEAVE(ret_value);
}

// Send a name to server and receive an obj id
perr_t PDC_Client_send_name_recv_id(const char *obj_name, pdcid_t obj_create_prop, pdcid_t *meta_id, int32_t *client_id)
{
    perr_t ret_value = FAIL;
    hg_return_t hg_ret;
    uint32_t server_id = 0;
    /* PDC_lifetime obj_life; */
    struct PDC_obj_prop *create_prop;
    gen_obj_id_in_t in;
    uint32_t hash_name_value;
    struct client_lookup_args lookup_args;
    
    FUNC_ENTER(NULL);
    
    create_prop = PDCobj_prop_get_info(obj_create_prop);
    // TODO: obj_life not used for now
    /* obj_life  = create_prop->obj_life; */
    // Fill input structure
    
    if (obj_name == NULL) {
        printf("Cannot create object with empty object name\n");
        goto done;
    }
    in.data.obj_name  = obj_name;
    in.data.time_step = create_prop->time_step;
    in.data.user_id   = create_prop->user_id;
    in.data.ndim      = create_prop->ndim;
    in.data.dims0     = create_prop->dims[0];
    in.data.dims1     = create_prop->dims[1];
    in.data.dims2     = create_prop->dims[2];
    in.data.dims3     = create_prop->dims[3];
   
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
    /* printf("Hash(%s) = %d\n", obj_name, in.hash_value); */

    // Compute server id
    server_id       = (hash_name_value + in.data.time_step);
    server_id      %= pdc_server_num_g; 

    /* uint32_t base_server_id  = get_server_id_by_hash_name(obj_name); */

    // Use local server only?
    /* if (pdc_use_local_server_only_g == 1) { */
    /*     server_id = pdc_client_mpi_rank_g % pdc_server_num_g; */
    /* } */

    /* printf("==PDC_CLIENT: Create obj_name: %s, hash_value: %d, server_id:%d\n", */ 
    /*         obj_name, hash_name_value, server_id); */

/*     uint32_t *hash_key = (uint32_t *)malloc(sizeof(uint32_t)); */
/*     *hash_key = hash_name_value; */

/*     struct client_name_cache_t *lookup_value; */
/*     client_name_cache_t *elt; */
/*     client_name_cache_t *insert_value; */

/*     // Is this the current object name's first appearance? */
/*     if (obj_names_cache_hash_table_g != NULL) { */
/*         /1* printf("checking hash table with key=%d\n", *hash_key); *1/ */
/*         lookup_value = hg_hash_table_lookup(obj_names_cache_hash_table_g, hash_key); */
/*         if (lookup_value != NULL) { */
/*             /1* printf("==PDC_CLIENT: Current name exist in the name cache hash table\n"); *1/ */
/*             // TODO: double check in case of hash collision */
/*         } */
/*         else { */
/*             // insert */
/*             /1* printf("==PDC_CLIENT: Current name does NOT exist in the name cache hash table\n"); *1/ */
/*             insert_value = (client_name_cache_t *)malloc(sizeof(client_name_cache_t)); */
/*             strcpy(insert_value->name, name); */
/*             hg_hash_table_insert(obj_names_cache_hash_table_g, hash_key, insert_value); */
/*             // Send marker to base server */
/*             /1* PDC_Client_send_obj_name_mark(name, base_server_id); *1/ */
/*         } */
/*     } */
/*     else { */
/*         printf("==PDC_CLIENT: Error with obj_names_cache_hash_table_g, exiting...\n"); */
/*         goto done; */
/*     } */

    /* printf("==PDC_CLIENT: [%d]: target server %d, name [%s], hash_value %u\n", pdc_client_mpi_rank_g, server_id, name, hash_name_value); */
    /* fflush(stdout); */
    /* ret_value = PDC_Client_send_name_to_server(name, hash_name_value, property, time_step, server_id); */



    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    /* printf("==PDC_CLIENT[%d]: obj_name=%s, user_id=%u, time_step=%u\n", pdc_client_mpi_rank_g, lookup_args.obj_name, lookup_args.user_id, lookup_args.time_step); */

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].rpc_handle_valid != 1) { */
        /* printf("Addr: %s\n", pdc_server_info_g[server_id].addr); */
        /* fflush(stdout); */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_register_id_g, 
                  &pdc_server_info_g[server_id].rpc_handle);
        /* pdc_server_info_g[server_id].rpc_handle_valid = 1; */
    /* } */

    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(pdc_server_info_g[server_id].rpc_handle, client_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_send_name_to_server(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

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
    *client_id = pdc_client_mpi_rank_g; 
    ret_value = SUCCEED;


done:
    HG_Destroy(pdc_server_info_g[server_id].rpc_handle);
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

            // We have already filled in the pdc_server_info_g[server_id].addr in 
            // previous client_test_connect_lookup_cb 
            /* if (pdc_server_info_g[server_id].close_server_handle_valid != 1) { */
                /* printf("Addr: %s\n", pdc_server_info_g[server_id].addr); */
                /* fflush(stdout); */
                HG_Create(send_context_g, pdc_server_info_g[server_id].addr, close_server_register_id_g, 
                          &pdc_server_info_g[server_id].close_server_handle);
                /* pdc_server_info_g[server_id].close_server_handle_valid= 1; */
            /* } */

            // Fill input structure
            in.client_id = 0;

            /* printf("Sending input to target\n"); */
            hg_ret = HG_Forward(pdc_server_info_g[server_id].close_server_handle, 
                                   close_server_cb, &lookup_args, &in);
            if (hg_ret != HG_SUCCESS) {
                fprintf(stderr, "==PDC_Client_close_all_server(): Could not start HG_Forward()\n");
                ret_value = FAIL;
                goto done;
            }
        }

        // Wait for response from server
        work_todo_g = pdc_server_num_g;
        PDC_Client_check_response(&send_context_g);

        if (is_client_debug_g == 1) {
            printf("\n\n\n==PDC_CLIENT: sent finalize request to all servers\n");
            fflush(stdout);
        }
    }

done:
    for (i = 0; i < (uint32_t)pdc_server_num_g; i++) 
        HG_Destroy(pdc_server_info_g[i].close_server_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_send_object_unmap(pdcid_t local_obj_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    gen_obj_unmap_notification_in_t in;
    /* hg_bulk_t bulk_handle; */
    uint32_t server_id;
    struct object_unmap_args unmap_args;
    
    FUNC_ENTER(NULL);
    
    // Fill input structure
    in.local_obj_id = local_obj_id;

    // Create a bulk descriptor
    /* bulk_handle = HG_BULK_NULL; */

    server_id = PDC_get_server_by_obj_id(local_obj_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].client_send_object_unmap_handle_valid!= 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_obj_unmap_notification_register_id_g, 
                  &pdc_server_info_g[server_id].client_send_object_unmap_handle);
        /* pdc_server_info_g[server_id].client_send_object_unmap_handle_valid  = 1; */
    /* } */

    hg_ret = HG_Forward(pdc_server_info_g[server_id].client_send_object_unmap_handle, client_send_object_unmap_rpc_cb, &unmap_args, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "==PDC_Client_send_object_unmap(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (unmap_args.ret != 1)
        PGOTO_ERROR(FAIL,"==PDC_CLIENT: object unmapping failed...");
    
done:
    HG_Destroy(pdc_server_info_g[server_id].client_send_object_unmap_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_send_region_unmap(pdcid_t local_obj_id, pdcid_t local_reg_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    gen_reg_unmap_notification_in_t in;
    /* hg_bulk_t bulk_handle; */
    uint32_t server_id;
    struct region_unmap_args unmap_args;
    
    FUNC_ENTER(NULL);
    
    // Fill input structure
    in.local_obj_id = local_obj_id;
    in.local_reg_id = local_reg_id;

    // Create a bulk descriptor
    /* bulk_handle = HG_BULK_NULL; */

    server_id = PDC_get_server_by_obj_id(local_obj_id, pdc_server_num_g);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].client_send_region_unmap_handle_valid!= 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_reg_unmap_notification_register_id_g, 
                    &pdc_server_info_g[server_id].client_send_region_unmap_handle);
        /* pdc_server_info_g[server_id].client_send_region_unmap_handle_valid  = 1; */
    /* } */

    hg_ret = HG_Forward(pdc_server_info_g[server_id].client_send_region_unmap_handle, client_send_region_unmap_rpc_cb, &unmap_args, &in);
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "==PDC_Client_send_region_unmap(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (unmap_args.ret != 1)
        PGOTO_ERROR(FAIL,"==PDC_CLIENT: region unmapping failed...");
    
done:
    HG_Destroy(pdc_server_info_g[server_id].client_send_region_unmap_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_send_region_map(pdcid_t local_obj_id, pdcid_t local_region_id, pdcid_t remote_obj_id, pdcid_t remote_region_id, size_t ndim, uint64_t *local_dims, uint64_t *local_offset, uint64_t *local_size, PDC_var_type_t local_type, void *local_data, uint64_t *remote_dims, uint64_t *remote_offset, uint64_t *remote_size, PDC_var_type_t remote_type, int32_t remote_client_id, void *remote_data, struct PDC_region_info *remote_region)
{
    perr_t ret_value = SUCCEED;
    hg_return_t  hg_ret = HG_SUCCESS;
    gen_reg_map_notification_in_t in;
    uint32_t server_id;
    hg_class_t *hg_class;
    hg_uint32_t i, j;
    hg_uint32_t local_count, remote_count;
    void    **data_ptrs, **data_ptrs_to;
    size_t  *data_size, *data_size_to;
    size_t  unit, unit_to; 
    struct  region_map_args map_args;
    hg_bulk_t local_bulk_handle = HG_BULK_NULL;
    hg_bulk_t remote_bulk_handle = HG_BULK_NULL;
 
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
    pdc_region_info_t_to_transfer(remote_region, &(in.region));

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

    if(remote_type == PDC_DOUBLE)
        unit_to = sizeof(double);
    else if(remote_type == PDC_FLOAT)
        unit_to = sizeof(float);
    else if(remote_type == PDC_INT)
        unit_to = sizeof(int);
    else
        PGOTO_ERROR(FAIL, "local data type is not supported yet");

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

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].client_send_region_map_handle_valid!= 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, gen_reg_map_notification_register_id_g, 
                  &pdc_server_info_g[server_id].client_send_region_map_handle);
        /* pdc_server_info_g[server_id].client_send_region_map_handle_valid  = 1; */
    /* } */
    
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
    hg_ret = HG_Forward(pdc_server_info_g[server_id].client_send_region_map_handle, client_send_region_map_rpc_cb, &map_args, &in);	
    if (hg_ret != HG_SUCCESS) {
        PGOTO_ERROR(FAIL, "==PDC_Client_send_region_map(): Could not start HG_Forward()");
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

	/* if (pdc_client_mpi_rank_g == 1) { */
    /*     printf("==PDC_CLIENT[%d]: PDC_Client_send_region() got response from server %d\n", pdc_client_mpi_rank_g, lookup_args.ret); */
    /*     fflush(stdout); */
    /* } */

    if (map_args.ret != 1) 
//        printf("==PDC_CLIENT: object mapping NOT successful ... ret_value = %d\n", map_args.ret);
        PGOTO_ERROR(FAIL,"==PDC_CLIENT: object mapping failed...");
//    else
//        printf("==PDC_CLIENT: object mapping successful\n");
done:
    HG_Destroy(pdc_server_info_g[server_id].client_send_region_map_handle);
    FUNC_LEAVE(ret_value);
}

static perr_t PDC_Client_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, pbool_t *status)
{
    perr_t ret_value;
    hg_return_t hg_ret;
    uint32_t server_id;
    region_lock_in_t in;
    struct client_lookup_args lookup_args;
    
    FUNC_ENTER(NULL);
    
    // Compute server id
    server_id  = get_server_id_by_obj_id(meta_id);

    // Delay test
    srand(pdc_client_mpi_rank_g);
    int delay = rand() % 500;
    usleep(delay);


    printf("==PDC_CLIENT: lock going to server %u\n", server_id); 
    fflush(stdout); 

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // Fill input structure
    in.obj_id = meta_id;
//    in.lock_op = lock_op;
    in.access_type = access_type;
    in.mapping = region_info->mapping;
    in.local_reg_id = region_info->local_id;
    in.region.ndim   = region_info->ndim;
    size_t ndim = region_info->ndim;
    /* printf("==PDC_CLINET: lock dim=%u\n", ndim); */

    if (ndim >= 4 || ndim <=0) {
        printf("Dimension %lu is not supported\n", ndim);
        ret_value = FAIL;
        goto done;
    }

    if (ndim >=1) {
        in.region.start_0  = region_info->offset[0];
        in.region.count_0  = region_info->size[0];
        /* in.region.stride_0 = 0; */
        // TODO current stride is not included in pdc.
    }
    if (ndim >=2) {
        in.region.start_1  = region_info->offset[1];
        in.region.count_1  = region_info->size[1];
        /* in.region.stride_1 = 0; */
    }
    if (ndim >=3) {
        in.region.start_2  = region_info->offset[2];
        in.region.count_2  = region_info->size[2];
        /* in.region.stride_2 = 0; */
    }
    /* if (ndim >=4) { */
    /*     in.region.start_3  = region_info->offset[3]; */
    /*     in.region.count_3  = region_info->size[3]; */
    /*     /1* in.region.stride_3 = 0; *1/ */
    /* } */

    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
    /* if (pdc_server_info_g[server_id].region_lock_handle_valid != 1) { */
        /* printf("Addr: %s\n", pdc_server_info_g[server_id].addr); */
        /* fflush(stdout); */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_lock_register_id_g, 
                    &pdc_server_info_g[server_id].region_lock_handle);
        /* pdc_server_info_g[server_id].region_lock_handle_valid = 1; */
    /* } */
    /* printf("Sending input to target\n"); */

    hg_ret = HG_Forward(pdc_server_info_g[server_id].region_lock_handle, client_region_lock_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_send_name_to_server(): Could not start HG_Forward()\n");
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
    HG_Destroy(pdc_server_info_g[server_id].region_lock_handle);
    FUNC_LEAVE(ret_value);
}

/* , uint64_t *block */
perr_t PDC_Client_obtain_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info,
                                    PDC_access_t access_type, PDC_lock_mode_t lock_mode, pbool_t *obtained)
{
    perr_t ret_value = FAIL;
    
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
                ret_value = PDC_Client_region_lock(meta_id, region_info, access_type, obtained);
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
            ret_value = PDC_Client_region_lock(meta_id, region_info, access_type, obtained);
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

static perr_t PDC_Client_region_release(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, pbool_t *status)
{
    perr_t ret_value;
    hg_return_t hg_ret;
    uint32_t server_id;
    region_lock_in_t in;
    struct client_lookup_args lookup_args;

    FUNC_ENTER(NULL);

    // Compute server id
    server_id  = get_server_id_by_obj_id(meta_id);

    // Delay test
    srand(pdc_client_mpi_rank_g);
    int delay = rand() % 500;
    usleep(delay);


    printf("==PDC_CLIENT: release going to server %u\n", server_id);
    fflush(stdout);

    // Debug statistics for counting number of messages sent to each server.
    debug_server_id_count[server_id]++;

    // Fill input structure
    in.obj_id = meta_id;
//    in.lock_op = lock_op;
    in.access_type = access_type;
    in.mapping = region_info->mapping;
    in.local_reg_id = region_info->local_id;
    in.region.ndim   = region_info->ndim;
    size_t ndim = region_info->ndim;
    /* printf("==PDC_CLINET: lock dim=%u\n", ndim); */
 
    if (ndim >= 4 || ndim <=0) {
        printf("Dimension %lu is not supported\n", ndim);
        ret_value = FAIL;
        goto done;
    }

    if (ndim >=1) {
        in.region.start_0  = region_info->offset[0];
        in.region.count_0  = region_info->size[0];
        /* in.region.stride_0 = 0; */
        // TODO current stride is not included in pdc.
    }
    if (ndim >=2) {
        in.region.start_1  = region_info->offset[1];
        in.region.count_1  = region_info->size[1];
        /* in.region.stride_1 = 0; */
    }
    if (ndim >=3) {
        in.region.start_2  = region_info->offset[2];
        in.region.count_2  = region_info->size[2];
        /* in.region.stride_2 = 0; */
    }
/*
    if (ndim >=4) {
        in.region.start_3  = region_info->offset[3];
        in.region.count_3  = region_info->size[3];
        in.region.stride_3 = 0;
    }
*/
    // We have already filled in the pdc_server_info_g[server_id].addr in previous client_test_connect_lookup_cb 
//    if (pdc_server_info_g[server_id].region_release_handle_valid != 1) {
        /* printf("Addr: %s\n", pdc_server_info_g[server_id].addr); */
        /* fflush(stdout); */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, region_release_register_id_g, &pdc_server_info_g[server_id].region_release_handle);
//        pdc_server_info_g[server_id].region_release_handle_valid = 1;
//    }
    /* printf("Sending input to target\n"); */

    hg_ret = HG_Forward(pdc_server_info_g[server_id].region_release_handle, client_region_release_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_send_name_to_server(): Could not start HG_Forward()\n");
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
    HG_Destroy(pdc_server_info_g[server_id].region_release_handle);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_release_region_lock(pdcid_t meta_id, struct PDC_region_info *region_info, PDC_access_t access_type, pbool_t *released)
{
    perr_t ret_value = FAIL;
    
    FUNC_ENTER(NULL);

    /* uint64_t meta_id; */
    /* PDC_obj_info *obj_prop = PDCobj_get_info(obj_id, pdc); */
    /* meta_id = obj_prop->meta_id; */
    ret_value = PDC_Client_region_release(meta_id, region_info, access_type, released);

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
        printf("==PDC_CLIENT[%d]: Shared memory open failed [%s]!\n", pdc_client_mpi_rank_g, shm_addr);
        ret_value = FAIL;
        goto done;
    }

    /* map the shared memory segment to the address space of the process */
    shm_base = mmap(0, data_size, PROT_READ, MAP_SHARED, shm_fd, 0);
    if (shm_base == MAP_FAILED) {
        printf("==PDC_CLIENT: Map failed: %s\n", strerror(errno));
        // close and unlink?
        ret_value = FAIL;
        goto close;
    }

    // Copy data
    /* printf("==PDC_SERVER: memcpy size = %" PRIu64 "\n", data_size); */
    if (target_region->ndim == 1) {
        memcpy(elt->buf, shm_base, data_size);
    }
    else if (target_region->ndim == 2) {
        char **buf_2d = (char**)elt->buf;
        for (i = 0; i < target_region->size[1]; i++) {
            memcpy(buf_2d[i], shm_base + i*target_region->size[0], target_region->size[0]);
        }
    }
    else if (target_region->ndim == 3) {
        char ***buf_3d = (char***)elt->buf;
        for (j = 0; j < target_region->size[2]; j++) {
            for (i = 0; i < target_region->size[1]; i++) {
                memcpy(buf_3d[j][i], shm_base + i*target_region->size[0] + 
                                                j*target_region->size[0]*target_region->size[1], 
                                     target_region->size[0]);
            }
        }
    }

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


perr_t PDC_Client_data_server_read_check(int server_id, uint32_t client_id, pdc_metadata_t *meta, struct PDC_region_info *region, int *status, void* buf)
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
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_read_check - NULL input\n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_read_check - invalid server id %d/%d\n", 
                pdc_client_mpi_rank_g, server_id, pdc_server_num_g);
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

    // We may have already filled in the pdc_server_info_g[server_id].addr in previous calls
    /* if (pdc_server_info_g[server_id].data_server_read_check_handle_valid != 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_check_register_id_g, 
                    &data_server_read_check_handle);
        /* pdc_server_info_g[server_id].data_server_read_check_handle_valid = 1; */
    /* } */
    /* printf("Sending input to target\n"); */

    hg_ret = HG_Forward(data_server_read_check_handle, data_server_read_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_data_server_read_check(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    *status = lookup_args.ret;
    if (lookup_args.ret != 1) {
        ret_value = SUCCEED;
        if (is_client_debug_g) {
            printf("==PDC_CLIENT[%d]: PDC_Client_data_server_read_check - IO request has not been "
                    "fulfilled by server\n", pdc_client_mpi_rank_g);
        }
        HG_Destroy(data_server_read_check_handle);
        if (lookup_args.ret == -1) 
            ret_value = -1;
        goto done;
    }
    else {
        /* if (is_client_debug_g) { */
        /*     printf("==PDC_CLIENT: PDC_Client_data_server_read_check - IO request done by server: shm_addr=[%s]\n", */ 
        /*             lookup_args.ret_string); */
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

        // Copy data
        memcpy(buf, shm_base, read_size);

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
    if (shm_unlink(shm_addr) == -1) {
        ret_value = FAIL;
        goto done;
        printf("==PDC_CLIENT: Error removing %s\n", shm_addr);
    }

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


perr_t PDC_Client_data_server_read(int server_id, int n_client, pdc_metadata_t *meta, struct PDC_region_info *region)
{
    perr_t ret_value = FAIL;
    hg_return_t hg_ret;
    struct client_lookup_args lookup_args;
    data_server_read_in_t in;
    hg_handle_t data_server_read_handle;
    
    FUNC_ENTER(NULL);

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_read - invalid server id %d/%d\n", pdc_client_mpi_rank_g, server_id, pdc_server_num_g);
        ret_value = FAIL;
        goto done;
    }

    if (meta == NULL || region == NULL) {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_read - invalid metadata or region \n", pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

    // Dummy value fill
    in.client_id         = pdc_client_mpi_rank_g;
    in.nclient           = n_client;
    pdc_metadata_t_to_transfer_t(meta, &in.meta);
    pdc_region_info_t_to_transfer(region, &in.region);

    /* printf("==PDC_CLIENT: sending data server read request to server %d\n", server_id); */

    HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_read_register_id_g, 
                &data_server_read_handle);

    hg_ret = HG_Forward(data_server_read_handle, data_server_read_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_data_server_read(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
    }

    // Wait for response from server
    work_todo_g = 1;
    PDC_Client_check_response(&send_context_g);

    if (lookup_args.ret == 1) {
        ret_value = SUCCEED;
        /* printf("==PDC_CLIENT: PDC_Client_data_server_read - received confirmation from server\n"); */
    }
    else {
        ret_value = FAIL;
        printf("==PDC_CLIENT: PDC_Client_data_server_read - ERROR from server\n");
    }


    HG_Destroy(data_server_read_handle);
done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
data_server_write_check_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    
    FUNC_ENTER(NULL);

    /* printf("Entered data_server_write_check_rpc_cb()"); */
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

perr_t PDC_Client_data_server_write_check(int server_id, uint32_t client_id, pdc_metadata_t *meta, struct PDC_region_info *region, int *status)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    struct client_lookup_args lookup_args;
    data_server_write_check_in_t in;

    FUNC_ENTER(NULL);

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write_check - invalid server id %d/%d\n"
                , pdc_client_mpi_rank_g, server_id, pdc_server_num_g);
        ret_value = FAIL;
        goto done;
    }

    in.client_id         = client_id;
    pdc_metadata_t_to_transfer_t(meta, &in.meta);
    pdc_region_info_t_to_transfer(region, &in.region);

    uint64_t write_size = 1;
    uint32_t i;
    for (i = 0; i < region->ndim; i++) {
        write_size *= region->size[i];
    }

    /* printf("==PDC_CLIENT: checking io status with data server %d\n", server_id); */

    // We may have already filled in the pdc_server_info_g[server_id].addr in previous calls
    /* if (pdc_server_info_g[server_id].data_server_write_check_handle_valid != 1) { */
        HG_Create(send_context_g, pdc_server_info_g[server_id].addr, data_server_write_check_register_id_g, 
                    &pdc_server_info_g[server_id].data_server_write_check_handle);
        pdc_server_info_g[server_id].data_server_write_check_handle_valid = 1;
    /* } */
    /* printf("Sending input to target\n"); */

    hg_ret = HG_Forward(pdc_server_info_g[server_id].data_server_write_check_handle, data_server_write_check_rpc_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_Client_data_server_write_check(): Could not start HG_Forward()\n");
        return EXIT_FAILURE;
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
            printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write_check - "
                    "IO request has not been fulfilled by server\n", pdc_client_mpi_rank_g);
        }
        if (lookup_args.ret == -1) 
            ret_value = -1;
        goto done;
    }

done:
    HG_Destroy(pdc_server_info_g[server_id].data_server_write_check_handle);
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

    if (is_client_debug_g == 1) {
        printf("==PDC_CLIENT[%d]: data_server_write_rpc_cb(): Return value from server: %d\n", 
                pdc_client_mpi_rank_g, output.ret);
    }

    client_lookup_args->ret = output.ret;

done:
    fflush(stdout);
    work_todo_g--;
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_data_server_write(int server_id, int n_client, pdc_metadata_t *meta, struct PDC_region_info *region, void *buf)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    uint32_t i, j;
    uint64_t region_size = 1;
    struct client_lookup_args lookup_args;
    char shm_addr[ADDR_MAX];
    data_server_write_in_t in;
    int server_ret;
    hg_handle_t data_server_write_handle;
    hg_handle_t aggregate_write_handle;
    region_list_t* tmp_regions = NULL;
    
    FUNC_ENTER(NULL);

    if (NULL == meta || NULL == region || NULL == buf) {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write - input NULL\n", pdc_client_mpi_rank_g);
        fflush(stdout);
        ret_value = FAIL;
        goto done;
    }

    if (server_id < 0 || server_id >= pdc_server_num_g) {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write - invalid server id %d/%d\n", 
                pdc_client_mpi_rank_g, server_id, pdc_server_num_g);
        ret_value = FAIL;
        goto done;
    }

    if (region->ndim > 4) {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write - invalid dim %lu\n", 
                pdc_client_mpi_rank_g, region->ndim);
        ret_value = FAIL;
        goto done;
    }

    // Calculate region size
    for (i = 0; i < region->ndim; i++) {
        region_size *= region->size[i];
        if (region_size == 0) {
            printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write - size[%d]=0\n", 
                    pdc_client_mpi_rank_g, i);
            ret_value = FAIL;
            goto done;
        }
    }

    // Create shared memory 
    // Shared memory address is /objID_ServerID_ClientID_rand
    int rnd = rand();
    sprintf(shm_addr, "/%" PRIu64 "_c%d_s%d_%d", meta->obj_id, pdc_client_mpi_rank_g, server_id, rnd);

    /* create the shared memory segment as if it was a file */
    char     *shm_base;
    int      shm_fd;
    /* printf("==PDC_CLIENT: creating share memory segment with address %s\n", shm_addr); */
    shm_fd = shm_open(shm_addr, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        printf("==PDC_CLIENT: Shared memory creation with shm_open failed\n");
        ret_value = FAIL;
        goto done;
    }

    /* configure the size of the shared memory segment */
    ftruncate(shm_fd, region_size);

    /* map the shared memory segment to the address space of the process */
    shm_base = mmap(0, region_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_base == MAP_FAILED) {
        printf("==PDC_CLIENT: Shared memory mmap failed, region size = %" PRIu64 "\n", region_size);
        // close and shm_unlink?
        ret_value = FAIL;
        goto done;
    }

    // Copy the user's buffer to shm that can be accessed by data server
    // Linearize if more than 2D
    if (region->ndim == 1) {
        memcpy(shm_base, buf, region_size);
    }
    else if (region->ndim == 2) {
        char **buf_2d = buf;
        for (i = 0; i < region->size[1]; i++) {
            memcpy(shm_base + i*region->size[0], buf_2d[i], region->size[0]);
            /* printf("==PDC_CLIENT[%d]: write memcpy [%.*s]\n", */ 
            /*         pdc_client_mpi_rank_g, region->size[0], buf_2d[i]); */
        }
    }
    else if (region->ndim == 3) {
        char ***buf_3d = buf;
        for (j = 0; j < region->size[2]; j++) {
            for (i = 0; i < region->size[1]; i++) {
                memcpy(shm_base + i*region->size[0] + j*region->size[0]*region->size[1], 
                       buf_3d[j][i], region->size[0]);
            }
        }
    }
    else {
        printf("==PDC_CLIENT[%d]: %lu data write is not supported yet\n", 
                pdc_client_mpi_rank_g, region->ndim);
    }

    int can_aggregate_send = 0;
    uint64_t same_node_ids[64];
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

    if (can_aggregate_send != 1) {
        // Normal send to server by each process
        meta->data_location[0] = ' ';
        meta->data_location[1] = 0;

        in.client_id         = pdc_client_mpi_rank_g;
        in.nclient           = n_client;
        in.shm_addr          = shm_addr;
        pdc_metadata_t_to_transfer_t(meta, &in.meta);
        pdc_region_info_t_to_transfer(region, &in.region);


        hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, 
                           data_server_write_register_id_g, &data_server_write_handle);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==PDC_Client_data_server_write(): Could not HG_Create()\n");
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Forward(data_server_write_handle, data_server_write_rpc_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==PDC_Client_data_server_write(): Could not start HG_Forward()\n");
            HG_Destroy(data_server_write_handle);
            ret_value = FAIL;
            goto done;
        }

        // Wait for response from server
        work_todo_g = 1;
        PDC_Client_check_response(&send_context_g);

        HG_Destroy(data_server_write_handle);

        server_ret = lookup_args.ret;
    }
    else {
        // First client rank as the delegate and aggregate the requests from other clients 
        // of same node to it (only needs region offsets/counts and shm_addr, metadata is shared)
        
        char my_serialized_buf[PDC_SERIALIZE_MAX_SIZE];
        char *all_serialized_buf = NULL;
        uint64_t *uint64_ptr = NULL;
        region_list_t tmp_region;
        region_list_t *regions[2];
        pdc_aggregated_io_to_server_t agg_write_in;

        regions[0] = &tmp_region;

        memset(my_serialized_buf, 0, PDC_SERIALIZE_MAX_SIZE);
        memset(&tmp_region, 0, sizeof(region_list_t));

        pdc_region_info_to_list_t(region, &tmp_region);

        // Store the shm addr in storage_location
        strcpy(tmp_region.storage_location, shm_addr); 

        ret_value = PDC_serialize_regions_lists(regions, 1, my_serialized_buf, PDC_SERIALIZE_MAX_SIZE); 
        if (ret_value != SUCCEED) {
            printf("==PDC_CLIENT[%d]: PDC_serialize_regions_lists ERROR!\n", pdc_client_mpi_rank_g);
            goto done;
        }

        PDC_replace_zero_chars(my_serialized_buf, PDC_SERIALIZE_MAX_SIZE);
        // Now my_serialized_buf has the serialized region data, with 0 substituted
        // n_region(1)|ndim|start_0|count_0|...|start_ndim|count_ndim|loc_len|shm_addr|offset|...


        if (pdc_client_same_node_rank_g == 0) {
            all_serialized_buf = (char*)calloc(PDC_SERIALIZE_MAX_SIZE, pdc_client_same_node_size_g);
        }

        // Gather serialized region info to first rank of each node
#ifdef ENABLE_MPI
        MPI_Gather(my_serialized_buf,  PDC_SERIALIZE_MAX_SIZE, MPI_CHAR, 
                   all_serialized_buf, PDC_SERIALIZE_MAX_SIZE, MPI_CHAR, 0, PDC_SAME_NODE_COMM_g);
#endif

        if (pdc_client_same_node_rank_g == 0) {

            tmp_regions = (region_list_t*)calloc(sizeof(region_list_t), pdc_client_same_node_size_g);

            // Now send all_serialized_buf to server and let server unserialize the regions
            meta->data_location[0] = ' ';
            meta->data_location[1] = 0;
            pdc_metadata_t_to_transfer_t(meta, &agg_write_in.meta);
            agg_write_in.buf = all_serialized_buf;

            // TODO
            hg_ret = HG_Create(send_context_g, pdc_server_info_g[server_id].addr, 
                               aggregate_write_register_id_g, &aggregate_write_handle);
            if (hg_ret != HG_SUCCESS) {
                fprintf(stderr, "==PDC_Client_data_server_write(): Could not HG_Create()\n");
                ret_value = FAIL;
                goto done;
            }

            hg_ret = HG_Forward(aggregate_write_handle, data_server_write_rpc_cb, &lookup_args, &in);
            if (hg_ret != HG_SUCCESS) {
                fprintf(stderr, "==PDC_Client_data_server_write(): Could not start HG_Forward()\n");
                HG_Destroy(data_server_write_handle);
                ret_value = FAIL;
                goto done;
            }

            // Wait for response from server
            work_todo_g = 1;
            PDC_Client_check_response(&send_context_g);

            HG_Destroy(aggregate_write_handle);
            server_ret = lookup_args.ret;

            free(tmp_regions);
            free(all_serialized_buf);
        } // End of if client is first rank of the node

#ifdef ENABLE_MPI
        // Now broadcast the result from server
        MPI_Bcast(&server_ret, 1, MPI_INT, 0, PDC_SAME_NODE_COMM_g);
#endif
    }

    if (server_ret == 1) {
        ret_value = SUCCEED;
        /* printf("==PDC_CLIENT: PDC_Client_data_server_write - received confirmation from server\n"); */
    }
    else {
        printf("==PDC_CLIENT[%d]: PDC_Client_data_server_write - ERROR from server\n", 
                pdc_client_mpi_rank_g);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

int pdc_msleep(unsigned long milisec)
{
    struct timespec req={0};
    time_t sec = (int)(milisec/1000);
    milisec    = milisec-(sec*1000);
    req.tv_sec = sec;
    req.tv_nsec= milisec*1000000L;
    while(nanosleep(&req, &req) == -1)
         continue;
    return 1;
}

perr_t PDC_Client_test(PDC_Request_t *request, int *completed)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (request == NULL || completed == NULL) {
        printf("==PDC_CLIENT: PDC_Client_test() - request and/or completed is NULL!\n");
        ret_value = FAIL;
        goto done;
    }

    if (request->access_type == READ) {
        ret_value = PDC_Client_data_server_read_check(request->server_id, pdc_client_mpi_rank_g, 
                     request->metadata, request->region, completed, request->buf);
        if (ret_value != SUCCEED) {
            printf("==PDC_CLIENT: PDC_Client_write_check ERROR!\n");
            goto done;
        }
    }
    else if (request->access_type == WRITE) {

        ret_value = PDC_Client_data_server_write_check(request->server_id, pdc_client_mpi_rank_g, 
                     request->metadata, request->region, completed);
        if (ret_value != SUCCEED) {
            printf("==PDC_CLIENT: PDC_Client_write_check ERROR!\n");
            goto done;
        }
    }
    else {
        printf("==PDC_CLIENT: PDC_Client_test() - error with request access type!\n");
        ret_value = FAIL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_wait(PDC_Request_t *request, unsigned long max_wait_ms, unsigned long check_interval_ms)
{
    int completed = 0;
    perr_t ret_value = FAIL;
    struct timeval  start_time;
    struct timeval  end_time;
    unsigned long elapsed_ms;

    FUNC_ENTER(NULL);

    gettimeofday(&start_time, 0);

    while (completed != 1) {
        ret_value = PDC_Client_test(request, &completed);
        if (ret_value != SUCCEED) {
            goto done;
        }
        /* printf("completed ... %d\n", completed); */
        if (is_client_debug_g ==1 && completed == 1) {
            printf("==PDC_CLIENT[%d]: IO has completed.\n", pdc_client_mpi_rank_g);
            fflush(stdout);
            break;
        }
        else if (is_client_debug_g ==1 && completed == 0){
            printf("==PDC_CLIENT[%d]: IO has not completed yet, will wait and ping server again ...\n", 
                    pdc_client_mpi_rank_g);
            fflush(stdout);
        }
        
        gettimeofday(&end_time, 0);
        elapsed_ms = ( (end_time.tv_sec-start_time.tv_sec)*1000000LL + end_time.tv_usec - 
                        start_time.tv_usec ) / 1000;
        if (elapsed_ms > max_wait_ms) {
            printf("==PDC_CLIENT[%d]: exceeded max IO request waiting time...\n", pdc_client_mpi_rank_g);
            break;
        }

        pdc_msleep(check_interval_ms);

        if (pdc_client_mpi_rank_g == 0) {
            printf("==PDC_CLIENT[%d]: waiting for server to finish IO request...\n", pdc_client_mpi_rank_g);
            fflush(stdout);
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

/* perr_t PDC_Client_data_server_write(int server_id, int n_client, pdc_metadata_t *meta, struct PDC_region_info *region, void *buf) */
perr_t PDC_Client_iwrite(pdc_metadata_t *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf)
{
    perr_t ret_value = FAIL;

    FUNC_ENTER(NULL);

    request->server_id   = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    request->n_client    = pdc_nclient_per_server_g;    // Set by env var PDC_NCLIENT_PER_SERVER, default 1
    request->access_type = WRITE;
    request->metadata    = meta;
    request->region      = region;
    request->buf         = buf;
/* printf("==PDC_CLIENT[%d], sending write request to server %d\n", pdc_client_mpi_rank_g, request->server_id); */

    /* printf("==PDC_CLIENT: PDC_Client_iwrite - sending data server write with data: [%s]\n", buf); */
    ret_value = PDC_Client_data_server_write(request->server_id, request->n_client, meta, region, buf);

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

    ret_value = PDC_Client_iwrite(meta, region, &request, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_write - PDC_Client_iwrite error\n");
        goto done;
    }
    ret_value = PDC_Client_wait(&request, 120000, 400);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_write - PDC_Client_wait error\n");
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_iread(pdc_metadata_t *meta, struct PDC_region_info *region, PDC_Request_t *request, void *buf)
{
    perr_t ret_value = FAIL;

    FUNC_ENTER(NULL);

    request->server_id   = (pdc_client_mpi_rank_g / pdc_nclient_per_server_g) % pdc_server_num_g;
    request->n_client    = pdc_nclient_per_server_g;    // Set by env var PDC_NCLIENT_PER_SERVER, default 1
    request->access_type = READ;
    request->metadata    = meta;
    request->region      = region;
    request->buf         = buf;

/* printf("==PDC_CLIENT[%d], sending read request to server %d\n", pdc_client_mpi_rank_g, request->server_id); */
    ret_value = PDC_Client_data_server_read(request->server_id, request->n_client, meta, region);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_iwrite - PDC_Client_data_server_read error\n");
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Client_read(pdc_metadata_t *meta, struct PDC_region_info *region, void *buf)
{
    PDC_Request_t request;
    perr_t ret_value = FAIL;

    FUNC_ENTER(NULL);

    PDC_Client_iread(meta, region, &request, buf);
    ret_value = PDC_Client_wait(&request, 120000, 400);
    if (ret_value != SUCCEED) {
        printf("==PDC_CLIENT: PDC_Client_read - PDC_Client_wait error\n");
        goto done;
    }

done:
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

