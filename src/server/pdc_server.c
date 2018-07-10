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
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctype.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>

#include <sys/shm.h>
#include <sys/mman.h>

#include "config.h"

#include "utlist.h"
#include "hash-table.h"

#include "mercury.h"
#include "mercury_macros.h"

#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server.h"
#include "pdc_server_metadata.h"
#include "pdc_server_data.h"

#ifdef PDC_HAS_CRAY_DRC
# include <rdmacred.h>
#endif

// Global debug variable to control debug printfs
int is_debug_g              = 0;
int pdc_client_num_g        = 0;

hg_class_t   *hg_class_g    = NULL;
hg_context_t *hg_context_g  = NULL;

// Below three are guarded by pdc_server_task_mutex_g for multi-thread
pdc_task_list_t *pdc_server_agg_task_head_g = NULL;
pdc_task_list_t *pdc_server_s2s_task_head_g = NULL;
int              pdc_server_task_id_g       = PDC_SERVER_TASK_INIT_VALUE;

pdc_client_info_t        *pdc_client_info_g        = NULL;
pdc_remote_server_info_t *pdc_remote_server_info_g = NULL;
char                     *all_addr_strings_1d_g    = NULL;
char                    **all_addr_strings_g       = NULL;
int                       is_all_client_connected_g  = 0;
int                       is_hash_table_init_g = 0;

hg_id_t    get_remote_metadata_register_id_g;
hg_id_t    buf_map_server_register_id_g;
hg_id_t    buf_unmap_server_register_id_g;
hg_id_t    obj_map_server_register_id_g;
hg_id_t    obj_unmap_server_register_id_g;
hg_id_t    server_lookup_client_register_id_g;
hg_id_t    server_lookup_remote_server_register_id_g;
hg_id_t    notify_io_complete_register_id_g;
hg_id_t    update_region_loc_register_id_g;
hg_id_t    notify_region_update_register_id_g;
hg_id_t    get_metadata_by_id_register_id_g;
hg_id_t    get_reg_lock_register_id_g;
/* hg_id_t    get_storage_info_register_id_g; */
hg_id_t    bulk_rpc_register_id_g;
hg_id_t    storage_meta_name_query_register_id_g;
hg_id_t    get_storage_meta_name_query_bulk_result_rpc_register_id_g;
hg_id_t    notify_client_multi_io_complete_rpc_register_id_g;
hg_id_t    server_checkpoint_rpc_register_id_g;

// Global thread pool
extern hg_thread_pool_t *hg_test_thread_pool_g;
extern hg_thread_pool_t *hg_test_thread_pool_fs_g;

hg_atomic_int32_t close_server_g;
char   pdc_server_tmp_dir_g[ADDR_MAX];
int    is_restart_g                       = 0;
int    pdc_server_rank_g                  = 0;
int    pdc_server_size_g                  = 1;
int    write_to_bb_percentage_g           = 0;
int    pdc_nost_per_file_g                = 0;
int    nclient_per_node                   = 0;
int    n_get_remote_storage_meta_g        = 0;
int    update_remote_region_count_g       = 0;
int    update_local_region_count_g        = 0;
int    n_fwrite_g                         = 0;
int    n_fread_g                          = 0;
int    n_fopen_g                          = 0;
double fread_total_MB                     = 0;
double fwrite_total_MB                    = 0;

double server_write_time_g                  = 0.0;
double server_read_time_g                   = 0.0;
double server_get_storage_info_time_g       = 0.0;
double server_fopen_time_g                  = 0.0;
double server_fsync_time_g                  = 0.0;
double server_total_io_time_g               = 0.0;
double server_update_region_location_time_g = 0.0;
double server_io_elapsed_time_g             = 0.0;

// Debug var
volatile int dbg_sleep_g                    = 1;

// Stat var
double total_mem_usage_g                    = 0.0;

// Data server related
pdc_data_server_io_list_t  *pdc_data_server_read_list_head_g    = NULL;
pdc_data_server_io_list_t  *pdc_data_server_write_list_head_g   = NULL;
update_storage_meta_list_t *pdc_update_storage_meta_list_head_g = NULL;


/*
 * Init the remote server info structure
 *
 * \param  info [IN]        PDC remote server info
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_remote_server_info_init(pdc_remote_server_info_t *info)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (info == NULL) {
        ret_value = FAIL;
        printf("==PDC_SERVER: NULL info, unable to init pdc_remote_server_info_t!\n");
        goto done;
    }

    info->addr_string = NULL;
    info->addr_valid  = 0;
    info->addr        = 0;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Destroy the client info structures, free the allocated space
 *
 * \param  info[IN]        Pointer to the client info structures
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_destroy_client_info(pdc_client_info_t *info)
{
    int i;
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    
    FUNC_ENTER(NULL);

    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: Destryoing %d client info\n", pdc_server_rank_g, pdc_client_num_g);
    }

    // Destroy addr and handle
    for (i = 0; i < pdc_client_num_g ; i++) {

        if (info[i].addr_valid == 1) {
            hg_ret = HG_Addr_free(hg_class_g, info[i].addr);
            if (hg_ret != HG_SUCCESS) {
                printf("==PDC_SERVER: PDC_Server_destroy_client_info() error with HG_Addr_free\n");
                ret_value = FAIL;
                goto done;
            }
            info[i].addr_valid = 0;
        }

    } // end of for

    free(info);
done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_init

/*
 * Init the client info structure
 *
 * \param  a[IN]        PDC client info structure pointer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_client_info_init(pdc_client_info_t* a)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);
    if (a == NULL) {
        printf("==PDC_SERVER: PDC_client_info_init() NULL input!\n");
        ret_value = FAIL;
        goto done;
    }
    /* else if (pdc_client_num_g != 0) { */
    /*     printf("==PDC_SERVER: PDC_client_info_init() - pdc_client_num_g is not 0!\n"); */
    /*     ret_value = FAIL; */
    /*     goto done; */
    /* } */

    memset(a->addr_string, 0, ADDR_MAX);
    a->addr_valid = 0;
done:
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function, allocates the client info structure with the first connectiong from all clients,
 * copies the client's address, and when received all clients' test connection message, start the lookup
 * process to test connection to all clients.
 *
 * \param  callback_info[IN]        Mercury callback info pointer 
 *
 * \return Non-negative on success/Negative on failure
 */
hg_return_t PDC_Server_get_client_addr(const struct hg_cb_info *callback_info)
{
    int i;
    hg_return_t ret_value = HG_SUCCESS;

    FUNC_ENTER(NULL);

    /* printf("==PDC_SERVER_get_client_addr!\n"); */
    /* fflush(stdout); */

    client_test_connect_args *in= (client_test_connect_args*) callback_info->arg;
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_client_addr_mutex_g);
#endif

    if (is_all_client_connected_g  == 1) {
        /* if (is_debug_g == 1) { */ 
            printf("==PDC_SERVER[%d]: new application run detected, create new client info\n", pdc_server_rank_g);
            fflush(stdout);
        /* } */
        
        PDC_Server_destroy_client_info(pdc_client_info_g);
        pdc_client_info_g = NULL;
        is_all_client_connected_g = 0;
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&pdc_client_info_mutex_g);
#endif
    if (pdc_client_info_g == NULL) {
        pdc_client_info_g = (pdc_client_info_t*)calloc(sizeof(pdc_client_info_t), in->nclient);
        if (pdc_client_info_g == NULL) {
            printf("==PDC_SERVER: PDC_Server_get_client_addr - unable to allocate space\n");
            ret_value = FAIL;
            goto done;
        }
        pdc_client_num_g = in->nclient;
        for (i = 0; i < in->nclient; i++) 
            PDC_client_info_init(&pdc_client_info_g[i]);
        if (is_debug_g == 1) { 
            printf("==PDC_SERVER[%d]: finished init %d client info\n", pdc_server_rank_g, in->nclient);
            fflush(stdout);
        }
        
    }

    // Copy the client's address
    memcpy(pdc_client_info_g[in->client_id].addr_string, in->client_addr, sizeof(char)*ADDR_MAX);

    if (is_debug_g) {
        printf("==PDC_SERVER: got client addr: %s from client[%d], total: %d\n", 
                pdc_client_info_g[in->client_id].addr_string, in->client_id, pdc_client_num_g);
        fflush(stdout);
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&pdc_client_info_mutex_g);
#endif
    
    /* ret_value = PDC_Server_lookup_client(in->client_id); */

    /* if (pdc_client_num_g >= in->nclient) { */
    /*     printf("==PDC_SERVER[%d]: got the last connection request from client[%d]\n", */ 
    /*             pdc_server_rank_g, in->client_id); */
    /*     fflush(stdout); */
    /*     for (i = 0; i < in->nclient; i++) */ 
    /*         ret_value = PDC_Server_lookup_client(i); */

    /*     is_all_client_connected_g = 1; */

    /*     if (is_debug_g) { */
    /*         printf("==PDC_SERVER[%d]: PDC_Server_get_client_addr - Finished PDC_Server_lookup_client()\n", */ 
    /*                 pdc_server_rank_g); */
    /*         fflush(stdout); */
    /*     } */
    /* } */
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_client_addr_mutex_g);
#endif

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Print the Mercury version
 *
 * \return void
 */
void PDC_Server_print_version()
{
    unsigned major, minor, patch;
    
    FUNC_ENTER(NULL);
    
    HG_Version_get(&major, &minor, &patch);
    printf("Server running mercury version %u.%u-%u\n", major, minor, patch);
    return;
}

/*
 * Write the servers' addresses to file, so that client can read the file and 
 * get all the servers' addresses.
 *
 * \param  addr_strings[IN]     2D char array of all servers' network address
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_write_addr_to_file(char** addr_strings, int n)
{
    perr_t ret_value = SUCCEED;
    char config_fname[ADDR_MAX];
    int i;
    
    FUNC_ENTER(NULL);

    // write to file
    
    snprintf(config_fname, ADDR_MAX, "%s%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);
    FILE *na_config = fopen(config_fname, "w+");
    if (!na_config) {
        fprintf(stderr, "Could not open config file from: %s\n", config_fname);
        goto done;
    }
    
    fprintf(na_config, "%d\n", n);
    for (i = 0; i < n; i++) {
        fprintf(na_config, "%s\n", addr_strings[i]);
    }
    fclose(na_config);
    na_config = NULL;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Remove server config file
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_rm_config_file()
{
    perr_t ret_value = SUCCEED;
    char config_fname[ADDR_MAX];

    FUNC_ENTER(NULL);

    snprintf(config_fname, ADDR_MAX, "%s%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);

    if (remove(config_fname) != 0) {
        printf("==PDC_SERVER[%d]: Unable to delete the config file[%s]", pdc_server_rank_g, config_fname);
        ret_value = FAIL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function of the server to lookup other servers via Mercury RPC
 *
 * \param  callback_info[IN]        Mercury callback info pointer 
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
lookup_remote_server_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    uint32_t server_id;
    server_lookup_args_t *lookup_args;
//    server_lookup_remote_server_in_t in;

    FUNC_ENTER(NULL);

    lookup_args = (server_lookup_args_t*) callback_info->arg;
    server_id = lookup_args->server_id;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&update_remote_server_addr_mutex_g);
#endif
    pdc_remote_server_info_g[server_id].addr = callback_info->info.lookup.addr;
    pdc_remote_server_info_g[server_id].addr_valid = 1;
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&update_remote_server_addr_mutex_g);
#endif

    if (pdc_remote_server_info_g[server_id].addr == NULL) {
        printf("==PDC_SERVER[%d]: %s - remote server addr is NULL\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    free(lookup_args);
    /* HG_Destroy(handle); */
    FUNC_LEAVE(ret_value);
} // end of lookup_remote_server_cb

/*
 * Test connection to another server, and stores the remote server's addr into 
 * pdc_remote_server_info_g
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_lookup_server_id(int remote_server_id)
{
    perr_t ret_value      = SUCCEED;
    hg_return_t hg_ret    = HG_SUCCESS;
    server_lookup_args_t *lookup_args;

    FUNC_ENTER(NULL);

    if (remote_server_id == pdc_server_rank_g || pdc_remote_server_info_g[remote_server_id].addr_valid == 1)
        return SUCCEED;

    lookup_args = (server_lookup_args_t*)malloc(sizeof(server_lookup_args_t));
    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: Testing connection to remote server %d: %s\n", 
                pdc_server_rank_g, remote_server_id, pdc_remote_server_info_g[remote_server_id].addr_string);
        fflush(stdout);
    }

    lookup_args->server_id = remote_server_id;
    hg_ret = HG_Addr_lookup(hg_context_g, lookup_remote_server_cb, lookup_args, 
                            pdc_remote_server_info_g[remote_server_id].addr_string, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS ) {
        printf("==PDC_SERVER: Connection to remote server FAILED!\n");
        ret_value = FAIL;
        goto done;
    }
    /* printf("==PDC_SERVER[%d]: connected to remote server %d\n", pdc_server_rank_g, remote_server_id); */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_Server_lookup_server_id 

/*
 * Test connection to all other servers
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_lookup_all_servers()
{
    int i, j;
    perr_t ret_value      = SUCCEED;

    FUNC_ENTER(NULL);


    // Lookup and fill the remote server info
    for (j = 0; j < pdc_server_size_g; j++) {

        #ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        #endif
        /* if (is_debug_g == 1) { */
        /*     printf("\n==PDC_SERVER[%d]: ROUND %d START==\n\n", pdc_server_rank_g, j); */
        /*     fflush(stdout); */
        /* } */
       
        if (j == pdc_server_rank_g) {
            // It's my turn, I will connect to all other servers
            for (i = 0; i < pdc_server_size_g; i++) {
                if (i == pdc_server_rank_g) 
                    continue;

                if (PDC_Server_lookup_server_id(i) != SUCCEED) {
                    printf("==PDC_SERVER[%d]: ERROR when lookup remote server %d!\n", pdc_server_rank_g, i);
                    ret_value = FAIL;
                    goto done;
                }
            }
        }
        /* if (is_debug_g == 1) { */
        /*     printf("\n==PDC_SERVER[%d]: ROUND %d END==\n\n", pdc_server_rank_g, j); */
        /*     fflush(stdout); */
        /* } */
    }

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER[%d]: Successfully established connection to %d other PDC servers\n",
                pdc_server_rank_g, pdc_server_size_g- 1);
        fflush(stdout);
    }

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_lookup_all_servers

/*---------------------------------------------------------------------------*/
static hg_return_t
PDC_hg_handle_create_cb(hg_handle_t handle, void *arg)
{
    struct hg_thread_work *hg_thread_work =
        malloc(sizeof(struct hg_thread_work));
    hg_return_t ret = HG_SUCCESS;

    if (!hg_thread_work) {
        HG_LOG_ERROR("Could not allocate hg_thread_work");
        ret = HG_NOMEM_ERROR;
        goto done;
    }

    (void) arg;
    HG_Set_data(handle, hg_thread_work, free);

done:
    return ret;
}

/*---------------------------------------------------------------------------*/
perr_t PDC_Server_set_close(void)
{
    perr_t ret_value = SUCCEED;

    /* printf("==PDC_SERVER[%d]: Closing server ...\n", pdc_server_rank_g); */
    /* fflush(stdout); */

    while (hg_atomic_get32(&close_server_g) == 0 ) {
        hg_atomic_set32(&close_server_g, 1);
    }

    /* printf("==PDC_SERVER[%d]: Closing server set ...\n", pdc_server_rank_g); */
    /* fflush(stdout); */
    FUNC_LEAVE(ret_value);
}

#ifdef PDC_HAS_CRAY_DRC
/* Convert value to string */
#define DRC_ERROR_STRING_MACRO(def, value, string) \
  if (value == def) string = #def

static const char *drc_strerror(int errnum)
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

/*
 * Callback function of the server to lookup other servers via Mercury RPC
 *
 * \param  port[IN]        Port number for Mercury to use
 * \param  hg_class[IN]    Pointer to Mercury class
 * \param  hg_context[IN]  Pointer to Mercury context
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_init(int port, hg_class_t **hg_class, hg_context_t **hg_context)
{
    perr_t ret_value = SUCCEED;
    int i = 0;
    char self_addr_string[ADDR_MAX];
    char na_info_string[ADDR_MAX];
    char hostname[1024];
    struct hg_init_info init_info = { 0 };

    /* Set the default mercury transport 
     * but enable overriding that to any of:
     *   "ofi+gni"
     *   "ofi+tcp"
     *   "cci+tcp"
     */
    char *default_hg_transport = "ofi+tcp";
    char *hg_transport;
#ifdef PDC_HAS_CRAY_DRC
    uint32_t credential = 0, cookie;
    drc_info_handle_t credential_info;
    char pdc_auth_key[256] = { '\0' };
    char *auth_key;
    int rc;
#endif
    
    FUNC_ENTER(NULL);

    /* PDC_Server_print_version(); */

    // set server id start
    pdc_id_seq_g = pdc_id_seq_g * (pdc_server_rank_g+1);

    // Create server tmp dir
    pdc_mkdir(pdc_server_tmp_dir_g);

    all_addr_strings_1d_g = (char* )calloc(sizeof(char ), pdc_server_size_g * ADDR_MAX);
    all_addr_strings_g    = (char**)calloc(sizeof(char*), pdc_server_size_g );
    total_mem_usage_g += (sizeof(char) + sizeof(char*));

    if ((hg_transport = getenv("HG_TRANSPORT")) == NULL) {
        hg_transport = default_hg_transport;
    }
    memset(hostname, 0, 1024);
    gethostname(hostname, 1023);
    snprintf(na_info_string, ADDR_MAX, "%s://%s:%d", hg_transport, hostname, port);
    if (pdc_server_rank_g == 0) 
        printf("==PDC_SERVER[%d]: using %.7s\n", pdc_server_rank_g, na_info_string);

    // Clean up all the tmp files etc
    HG_Cleanup();

//gni starts here
#ifdef PDC_HAS_CRAY_DRC
    /* Acquire credential */
/*    if (pdc_server_rank_g == 0) {
        rc = drc_acquire(&credential, 0);
        if (rc != DRC_SUCCESS) { 
            printf("drc_acquire() failed (%d, %s)", rc, drc_strerror(-rc));
            goto done;
        }
    }*/
    if (pdc_server_rank_g == 0) {
        credential = atoi(getenv("PDC_DRC_KEY"));
    }
    MPI_Bcast(&credential, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);
    
//    printf("# Credential is %u\n", credential);
//    fflush(stdout);

    rc = drc_access(credential, 0, &credential_info);
    if (rc != DRC_SUCCESS) { /* failed to access credential */
        printf("drc_access() failed (%d, %s)", rc,
            drc_strerror(-rc));
        ret_value = FAIL; 
        goto done;
    }
    cookie = drc_get_first_cookie(credential_info);

    if (pdc_server_rank_g == 0) { 
        printf("# Credential is %u\n", credential);
        printf("# Cookie is %u\n", cookie);
        fflush(stdout);
    }
    sprintf(pdc_auth_key, "%u", cookie);
    init_info.na_init_info.auth_key = strdup(pdc_auth_key);
#endif
//end of gni

    // Init server
//    *hg_class = HG_Init(na_info_string, NA_TRUE);
#ifndef ENABLE_MULTITHREAD
    init_info.na_init_info.progress_mode = NA_NO_BLOCK;    // busy mode
#endif

#ifndef PDC_HAS_CRAY_DRC
    init_info.auto_sm = HG_TRUE;
#endif
    *hg_class = HG_Init_opt(na_info_string, NA_TRUE, &init_info);
    if (*hg_class == NULL) {
        printf("Error with HG_Init()\n");
        return FAIL;
    }

    /* Attach handle created for worker thread */
    HG_Class_set_handle_create_callback(*hg_class,
        PDC_hg_handle_create_cb, NULL);

    // Create HG context 
    *hg_context = HG_Context_create(*hg_class);
    if (*hg_context == NULL) {
        printf("Error with HG_Context_create()\n");
        return FAIL;
    }

    /* pdc_client_context_g = HG_Context_create(*hg_class); */
    /* if (pdc_client_context_g == NULL) { */
    /*     printf("Error with HG_Context_create()\n"); */
    /*     return FAIL; */
    /* } */


    // Get server address
    PDC_get_self_addr(*hg_class, self_addr_string);
    /* printf("Server address is: %s\n", self_addr_string); */
    /* fflush(stdout); */

    // Init server to server communication.
    pdc_remote_server_info_g = (pdc_remote_server_info_t*)calloc(sizeof(pdc_remote_server_info_t), 
                                pdc_server_size_g);

    for (i = 0; i < pdc_server_size_g; i++) {
        ret_value = PDC_Server_remote_server_info_init(&pdc_remote_server_info_g[i]);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: error with PDC_Server_remote_server_info_init\n", pdc_server_rank_g);
            goto done;
        }
    }

    // Gather addresses
#ifdef ENABLE_MPI
    MPI_Allgather(self_addr_string, ADDR_MAX, MPI_CHAR, all_addr_strings_1d_g, ADDR_MAX, MPI_CHAR, MPI_COMM_WORLD);
    for (i = 0; i < pdc_server_size_g; i++) {
        all_addr_strings_g[i] = &all_addr_strings_1d_g[i*ADDR_MAX];
        pdc_remote_server_info_g[i].addr_string = &all_addr_strings_1d_g[i*ADDR_MAX];
        /* printf("==PDC_SERVER[%d]: server %d addr [%s]\n", */
        /*         pdc_server_rank_g, i, pdc_remote_server_info_g[i].addr_string); */
    }
#else 
    strcpy(all_addr_strings_1d_g, self_addr_string);
    all_addr_strings_g[0] = all_addr_strings_1d_g;
#endif

#ifdef ENABLE_MULTITHREAD
    // Init threadpool
    char *nthread_env = getenv("PDC_SERVER_NTHREAD"); 
    int n_thread = 4;
    if (nthread_env != NULL) 
        n_thread = atoi(nthread_env);
    
    if (n_thread < 1) 
        n_thread = 2;
    hg_thread_pool_init(n_thread, &hg_test_thread_pool_g);
    hg_thread_pool_init(1, &hg_test_thread_pool_fs_g);
    if (pdc_server_rank_g == 0) {
        printf("\n==PDC_SERVER[%d]: Starting server with %d threads...\n", pdc_server_rank_g, n_thread);
        fflush(stdout);
    }
    hg_thread_mutex_init(&hash_table_new_mutex_g);
    hg_thread_mutex_init(&pdc_client_info_mutex_g);
    hg_thread_mutex_init(&pdc_metadata_hash_table_mutex_g);
    hg_thread_mutex_init(&pdc_container_hash_table_mutex_g);
    hg_thread_mutex_init(&pdc_client_addr_mutex_g);
    hg_thread_mutex_init(&pdc_time_mutex_g);
    hg_thread_mutex_init(&pdc_bloom_time_mutex_g);
    hg_thread_mutex_init(&n_metadata_mutex_g);
    hg_thread_mutex_init(&gen_obj_id_mutex_g);
    hg_thread_mutex_init(&total_mem_usage_mutex_g);
    hg_thread_mutex_init(&data_read_list_mutex_g);
    hg_thread_mutex_init(&data_write_list_mutex_g);
    hg_thread_mutex_init(&pdc_server_task_mutex_g);
    hg_thread_mutex_init(&region_struct_mutex_g);
    hg_thread_mutex_init(&data_buf_map_mutex_g);
    hg_thread_mutex_init(&meta_buf_map_mutex_g);
    hg_thread_mutex_init(&data_obj_map_mutex_g);
    hg_thread_mutex_init(&meta_obj_map_mutex_g);
    hg_thread_mutex_init(&lock_list_mutex_g);
    hg_thread_mutex_init(&insert_hash_table_mutex_g);
    hg_thread_mutex_init(&lock_request_mutex_g);
    hg_thread_mutex_init(&addr_valid_mutex_g);
    hg_thread_mutex_init(&update_remote_server_addr_mutex_g);
#else
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER[%d]: without multi-thread!\n", pdc_server_rank_g);
        fflush(stdout);
    }
#endif

    // TODO: support restart with different number of servers than previous run 
    char checkpoint_file[ADDR_MAX];
    if (is_restart_g == 1) {
        snprintf(checkpoint_file, ADDR_MAX, "%s%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);

#ifdef ENABLE_TIMING 
        // Timing
        struct timeval  pdc_timer_start;
        struct timeval  pdc_timer_end;
        double restart_time, all_restart_time;
        gettimeofday(&pdc_timer_start, 0);
#endif

        ret_value = PDC_Server_restart(checkpoint_file);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: error with PDC_Server_restart\n", pdc_server_rank_g);
            goto done;
        }
#ifdef ENABLE_TIMING 
        // Timing
        gettimeofday(&pdc_timer_end, 0);
        restart_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    #ifdef ENABLE_MPI
        MPI_Reduce(&restart_time, &all_restart_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    #else
        all_restart_time = restart_time;
    #endif
        if (pdc_server_rank_g == 0) 
            printf("==PDC_SERVER[0]: total restart time = %.6f\n", all_restart_time);
#endif
    }
    else {
        // We are starting a brand new server
        if (is_hash_table_init_g != 1) {
            // Hash table init
            ret_value = PDC_Server_init_hash_table();
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: error with PDC_Server_init_hash_table\n", pdc_server_rank_g);
                goto done;
            }
        }
    }

    // Data server related init
    pdc_data_server_read_list_head_g = NULL;
    pdc_data_server_write_list_head_g = NULL;
    pdc_update_storage_meta_list_head_g = NULL;
    pdc_buffered_bulk_update_total_g = 0;
    pdc_nbuffered_bulk_update_g = 0;

    // Initalize atomic variable to finalize server 
    hg_atomic_set32(&close_server_g, 0);

    n_metadata_g = 0;

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_init

/*
 * Destroy the remote server info structures, free the allocated space
 *
 * \param  info[IN]        Pointer to the remote server info structures
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_destroy_remote_server_info()
{
    int i;
    hg_return_t hg_ret;
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    // Destroy addr and handle
    for (i = 0; i <pdc_server_size_g; i++) {
        if (pdc_remote_server_info_g[i].addr_valid == 1) {
            /* printf("==PDC_SERVER[%d]: HG_Addr_free #%d\n", pdc_server_rank_g, i); */
            /* fflush(stdout); */
            hg_ret = HG_Addr_free(hg_class_g, pdc_remote_server_info_g[i].addr);
            if (hg_ret != HG_SUCCESS) {
                printf("==PDC_SERVER: PDC_Server_destroy_remote_server_info() error with HG_Addr_free\n");
                ret_value = FAIL;
                goto done;
            }
            pdc_remote_server_info_g[i].addr_valid = 0;
        }
    }

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_destroy_remote_server_info

/*
 * Finalize the server, free allocated spaces
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_finalize()
{
    pdc_data_server_io_list_t *io_elt = NULL;
    region_list_t *region_elt = NULL, *region_tmp = NULL;
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    
    FUNC_ENTER(NULL);

    // Debug: print all metadata
    if (is_debug_g == 1) {
        PDC_Server_print_all_metadata();
        PDC_Server_print_all_containers();
    }

    // Debug: check duplicates
    if (is_debug_g == 1) {
        PDC_Server_metadata_duplicate_check();
        fflush(stdout);
    }

    // Remove the opened shm
    DL_FOREACH(pdc_data_server_read_list_head_g, io_elt) {
        // remove IO request and its shm of perviously used obj
        DL_FOREACH_SAFE(io_elt->region_list_head, region_elt, region_tmp) {
            ret_value = PDC_Server_close_shm(region_elt);
            if (ret_value != SUCCEED) 
                printf("==PDC_SERVER: error closing shared memory\n");
            fflush(stdout);
            DL_DELETE(io_elt->region_list_head, region_elt);
            free(region_elt);
        }
        io_elt->region_list_head = NULL;
    }

    // Free hash table
    if(metadata_hash_table_g != NULL)
        hash_table_free(metadata_hash_table_g);

    if(container_hash_table_g != NULL)
        hash_table_free(container_hash_table_g);
/* printf("hash table freed!\n"); */
/* fflush(stdout); */

    ret_value = PDC_Server_destroy_client_info(pdc_client_info_g);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER: Error with PDC_Server_destroy_client_info\n");
        goto done;
    }

/* printf("client info destroyed!\n"); */
/* fflush(stdout); */

    ret_value = PDC_Server_destroy_remote_server_info();
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: Error with PDC_Server_destroy_client_info\n", pdc_server_rank_g);
        goto done;
    }

/* printf("server info destroyed!"\n); */
/* fflush(stdout); */

#ifdef ENABLE_TIMING 

    double all_bloom_check_time_max, all_bloom_check_time_min, all_insert_time_max, all_insert_time_min;
    double all_server_bloom_init_time_min,  all_server_bloom_init_time_max;
    /* double all_server_bloom_insert_time_min,  all_server_bloom_insert_time_max; */
    double all_server_hash_insert_time_min, all_server_hash_insert_time_max;

    #ifdef ENABLE_MPI
    MPI_Reduce(&server_bloom_check_time_g, &all_bloom_check_time_max,        1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_bloom_check_time_g, &all_bloom_check_time_min,        1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_insert_time_g,      &all_insert_time_max,             1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_insert_time_g,      &all_insert_time_min,             1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    MPI_Reduce(&server_bloom_init_time_g,  &all_server_bloom_init_time_max,  1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_bloom_init_time_g,  &all_server_bloom_init_time_min,  1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_hash_insert_time_g, &all_server_hash_insert_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_hash_insert_time_g, &all_server_hash_insert_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    #else
    all_bloom_check_time_min        = server_bloom_check_time_g;
    all_bloom_check_time_max        = server_bloom_check_time_g;
    all_insert_time_max             = server_insert_time_g;
    all_insert_time_min             = server_insert_time_g;
    all_server_bloom_init_time_min  = server_bloom_init_time_g;
    all_server_bloom_init_time_max  = server_bloom_init_time_g;
    all_server_hash_insert_time_max = server_hash_insert_time_g;
    all_server_hash_insert_time_min = server_hash_insert_time_g;
    #endif
    if (pdc_server_rank_g == 0) {
    /*     printf("==PDC_SERVER: total bloom check time = %.6f, %.6f\n", all_bloom_check_time_min, all_bloom_check_time_max); */
    /*     printf("==PDC_SERVER: total insert      time = %.6f, %.6f\n", all_insert_time_min, all_insert_time_max); */
    /*     printf("==PDC_SERVER: total hash insert time = %.6f, %.6f\n", all_server_hash_insert_time_min, all_server_hash_insert_time_max); */
    /*     printf("==PDC_SERVER: total bloom init  time = %.6f, %.6f\n", all_server_bloom_init_time_min, all_server_bloom_init_time_max); */
    /*     printf("==PDC_SERVER: total memory usage     = %.2f MB\n", total_mem_usage_g/1048576.0); */
    }
    /* printf("==PDC_SERVER[%d]: update local %d region, %d remote ones \n", */ 
    /*         pdc_server_rank_g, update_local_region_count_g, update_remote_region_count_g); */
    /* fflush(stdout); */

#endif

#ifdef ENABLE_MULTITHREAD
    // Destory pool
    hg_thread_pool_destroy(hg_test_thread_pool_fs_g);

    hg_thread_mutex_destroy(&hash_table_new_mutex_g);
    hg_thread_mutex_destroy(&pdc_client_info_mutex_g);
    hg_thread_mutex_destroy(&pdc_time_mutex_g);
    hg_thread_mutex_destroy(&pdc_metadata_hash_table_mutex_g);
    hg_thread_mutex_destroy(&pdc_container_hash_table_mutex_g);
    hg_thread_mutex_destroy(&pdc_client_addr_mutex_g);
    hg_thread_mutex_destroy(&pdc_bloom_time_mutex_g);
    hg_thread_mutex_destroy(&n_metadata_mutex_g);
    hg_thread_mutex_destroy(&gen_obj_id_mutex_g);
    hg_thread_mutex_destroy(&total_mem_usage_mutex_g);
    hg_thread_mutex_destroy(&data_read_list_mutex_g);
    hg_thread_mutex_destroy(&data_write_list_mutex_g);
    hg_thread_mutex_destroy(&pdc_server_task_mutex_g);
    hg_thread_mutex_destroy(&region_struct_mutex_g);
    hg_thread_mutex_destroy(&data_buf_map_mutex_g);
    hg_thread_mutex_destroy(&meta_buf_map_mutex_g);
    hg_thread_mutex_destroy(&data_obj_map_mutex_g);
    hg_thread_mutex_destroy(&meta_obj_map_mutex_g);
    hg_thread_mutex_destroy(&insert_hash_table_mutex_g);
    hg_thread_mutex_destroy(&lock_list_mutex_g);
    hg_thread_mutex_destroy(&lock_request_mutex_g);
    hg_thread_mutex_destroy(&addr_valid_mutex_g);
    hg_thread_mutex_destroy(&update_remote_server_addr_mutex_g);
#endif

    if (pdc_server_rank_g == 0)
        PDC_Server_rm_config_file();


    hg_ret = HG_Context_destroy(hg_context_g);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: error with HG_Context_destroy\n", pdc_server_rank_g);
        goto done;
    }

    hg_ret = HG_Finalize(hg_class_g);
    if (hg_ret != HG_SUCCESS) 
        printf("==PDC_SERVER[%d]: error with HG_Finalize\n", pdc_server_rank_g);

done:
    free(all_addr_strings_g);
    free(all_addr_strings_1d_g);
    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Server_checkpoint_cb(const struct hg_cb_info *callback_info)
{
    PDC_Server_checkpoint();

    return HG_SUCCESS;
}

/*
 * Checkpoint in-memory metadata to persistant storage, each server writes to one file
 *
 * \param  filename[IN]     Checkpoint file name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_checkpoint()
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *elt;
    region_list_t  *region_elt;
    pdc_hash_table_entry_head *head;
    int n_entry, metadata_size = 0, region_count = 0, n_region, n_write_region = 0;
    uint32_t hash_key;
    HashTablePair pair;
    char checkpoint_file[ADDR_MAX];
    
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double checkpoint_time;
    gettimeofday(&pdc_timer_start, 0);
#endif

    // TODO: instead of checkpoint at app finalize time, try checkpoint with a time countdown or # of objects
    snprintf(checkpoint_file, ADDR_MAX, "%s%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);
    if (pdc_server_rank_g == 0) {
        printf("\n\n==PDC_SERVER[%d]: Checkpoint file [%s]\n",pdc_server_rank_g, checkpoint_file);
        fflush(stdout);
    }

    FILE *file = fopen(checkpoint_file, "w+");
    if (file==NULL) {
        printf("==PDC_SERVER[%d]: %s - Checkpoint file open error", pdc_server_rank_g, __func__); 
        ret_value = FAIL;
        goto done;
    }

    // DHT
    n_entry = hash_table_num_entries(metadata_hash_table_g);
    /* printf("%d entries\n", n_entry); */
    fwrite(&n_entry, sizeof(int), 1, file);

    HashTableIterator hash_table_iter;
    hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

    while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
        head = pair.value;
        /* printf("count=%d\n", head->n_obj); */
        /* fflush(stdout); */

        fwrite(&head->n_obj, sizeof(int), 1, file);
        // TODO: find a way to get hash_key from hash table istead of calculating again.
        hash_key = PDC_get_hash_by_name(head->metadata->obj_name);
        fwrite(&hash_key, sizeof(uint32_t), 1, file);
        // Iterate every metadata structure in current entry
        DL_FOREACH(head->metadata, elt) {
            /* printf("==PDC_SERVER: Writing one metadata...\n"); */
            /* PDC_print_metadata(elt); */
            fwrite(elt, sizeof(pdc_metadata_t), 1, file);

            // Write region info
            DL_COUNT(elt->storage_region_list_head, region_elt, n_region);
            fwrite(&n_region, sizeof(int), 1, file);
            n_write_region = 0;
            DL_FOREACH(elt->storage_region_list_head, region_elt) {
                fwrite(region_elt, sizeof(region_list_t), 1, file);
                n_write_region++;
            }

            if (n_write_region != n_region) {
                printf("==PDC_SERVER[%d]: %s - ERROR with number of regions", pdc_server_rank_g, __func__); 
                ret_value = FAIL;
                goto done;
            }

            metadata_size++;
            region_count += n_region;
        }
    }

    fclose(file);
    file = NULL;

    int all_metadata_size, all_region_count;
#ifdef ENABLE_MPI
    MPI_Reduce(&metadata_size, &all_metadata_size, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&region_count,   &all_region_count,   1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_metadata_size = metadata_size;
    all_region_count   = region_count;
#endif
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: checkpointed %d objects, with %d regions \n", all_metadata_size, all_region_count);
    }

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    checkpoint_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: total checkpoint time = %.6f\n", checkpoint_time);
    }
#endif

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Load metadata from checkpoint file in persistant storage
 *
 * \param  filename[IN]     Checkpoint file name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_restart(char *filename)
{
    perr_t ret_value = SUCCEED;
    int n_entry, count, i, j, nobj = 0, all_nobj = 0, all_n_region, n_region, total_region = 0;
    pdc_metadata_t *metadata, *elt;
    region_list_t *region_list;
    pdc_hash_table_entry_head *entry;
    uint32_t *hash_key;
    
    FUNC_ENTER(NULL);

    // init hash table
    ret_value = PDC_Server_init_hash_table();
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - PDC_Server_init_hash_table FAILED!", pdc_server_rank_g, __func__); 
        ret_value = FAIL;
        goto done;
    }

    FILE *file = fopen(filename, "r");
    if (file==NULL) {
        printf("==PDC_SERVER[%d]: %s -  Checkpoint file open FAILED [%s]!", 
                pdc_server_rank_g, __func__, filename); 
        ret_value = FAIL;
        goto done;
    }

    // init hash table
    PDC_Server_init_hash_table();
    fread(&n_entry, sizeof(int), 1, file);
    /* printf("%d entries\n", n_entry); */

    char *slurm_jobid = getenv("SLURM_JOB_ID");
    if (slurm_jobid == NULL) {
        printf("Error getting slurm job id from SLURM_JOB_ID!\n");
    }

    while (n_entry>0) {
        fread(&count, sizeof(int), 1, file);
        /* printf("Count:%d\n", count); */

        hash_key = (uint32_t *)malloc(sizeof(uint32_t));
        fread(hash_key, sizeof(uint32_t), 1, file);
        /* printf("Hash key is %u\n", *hash_key); */
        total_mem_usage_g += sizeof(uint32_t);

        // Reconstruct hash table
        entry = (pdc_hash_table_entry_head*)malloc(sizeof(pdc_hash_table_entry_head));
        entry->n_obj = 0;
        entry->bloom = NULL;
        entry->metadata = NULL;
        // Init hash table metadata (w/ bloom) with first obj
        PDC_Server_hash_table_list_init(entry, hash_key);


        metadata = (pdc_metadata_t*)calloc(sizeof(pdc_metadata_t), count);

        for (i = 0; i < count; i++) {
            fread(metadata+i, sizeof(pdc_metadata_t), 1, file);

            (metadata+i)->storage_region_list_head = NULL;
            (metadata+i)->region_lock_head         = NULL;
            (metadata+i)->region_map_head          = NULL;
            (metadata+i)->region_buf_map_head      = NULL;
            (metadata+i)->region_obj_map_head      = NULL;
            (metadata+i)->bloom                    = NULL;
            (metadata+i)->prev                     = NULL;
            (metadata+i)->next                     = NULL;

            fread(&n_region, sizeof(int), 1, file);
            if (n_region < 0) {
                printf("==PDC_SERVER[%d]: %s -  Checkpoint file region number ERROR!", 
                        pdc_server_rank_g, __func__); 
                ret_value = FAIL;
                goto done;
            }

            total_region += n_region; 
            region_list = (region_list_t*)malloc(sizeof(region_list_t)* n_region);

            fread(region_list, sizeof(region_list_t), n_region, file);

            unsigned idx;
            for (j = 0; j < n_region; j++) {
                (region_list+j)->buf                        = NULL;
                (region_list+j)->data_size                  = 1;
                for (idx = 0; idx < (region_list+j)->ndim; idx++) 
                    (region_list+j)->data_size *= (region_list+j)->count[idx];
                (region_list+j)->is_data_ready              = 0;
                (region_list+j)->shm_fd                     = 0;
                (region_list+j)->meta                       = (metadata+i);
                (region_list+j)->prev                       = NULL;
                (region_list+j)->next                       = NULL;
                (region_list+j)->overlap_storage_regions    = NULL;
                (region_list+j)->n_overlap_storage_region   = 0;
		hg_atomic_init32(&((region_list+j)->buf_map_refcount), 0);
                (region_list+j)->reg_dirty_from_buf         = 0;
                (region_list+j)->reg_dirty_to_buf           = 0;
                (region_list+j)->access_type                = NA;
                (region_list+j)->bulk_handle                = NULL;
                (region_list+j)->addr                       = NULL;
                (region_list+j)->obj_id                     = (metadata+i)->obj_id;
                (region_list+j)->reg_id                     = 0;
                (region_list+j)->from_obj_id                = 0;
                (region_list+j)->client_id                  = 0;
                (region_list+j)->is_io_done                 = 0;
                (region_list+j)->is_shm_closed              = 0;
                (region_list+j)->seq_id                     = 0;

                memset((region_list+j)->shm_addr, 0, ADDR_MAX);
                memset((region_list+j)->client_ids, 0, PDC_SERVER_MAX_PROC_PER_NODE*sizeof(uint32_t));
                // Check if data is in persistent BB, change path if so
                if (strstr( (region_list+j)->storage_location, "/var/opt/cray/dws/mounts/batch") != NULL) {
                    (region_list+j)->data_loc_type = BB;
                    // find job id and replace it with current one
                    for (idx = 0; idx < strlen((region_list+j)->storage_location); idx++) 
                        if (isdigit((region_list+j)->storage_location[idx])) 
                            break;
                    if ( NULL != slurm_jobid) 
                        strncpy(&((region_list+j)->storage_location[idx]), slurm_jobid, strlen(slurm_jobid));
                }
                else if (strstr( (region_list+j)->storage_location, "/global/cscratch") != NULL) {
                    (region_list+j)->data_loc_type = LUSTRE;
                }

                DL_APPEND((metadata+i)->storage_region_list_head, region_list+j);
            }
        }

        nobj += count;
        total_mem_usage_g += sizeof(pdc_hash_table_entry_head);
        total_mem_usage_g += (sizeof(pdc_metadata_t)*count);

        entry->metadata = NULL;

        // Insert the previously read metadata to the linked list (hash table entry)
        for (i = 0; i < count; i++) {
            elt = metadata + i;
            // Add to hash list and bloom filter
            ret_value = PDC_Server_hash_table_list_insert(entry, elt);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER: error with hash table recovering from checkpoint file\n");
                goto done;
            }
        }
        n_entry--;
    }

    fclose(file);
    file = NULL;

#ifdef ENABLE_MPI
    MPI_Reduce(&nobj, &all_nobj, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&total_region, &all_n_region, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
#else
    all_nobj = nobj;
    all_n_region = total_region;
#endif 
    
    /* printf("==PDC_SERVER[%d]: Server restarted from saved session, " */
    /*        "successfully loaded %d objects, %d regions...\n", pdc_server_rank_g, nobj, total_region); */

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER[0]: Server restarted from saved session, "
               "successfully loaded %d objects, %d regions...\n", all_nobj, all_n_region);
    }

    // debug
    /* PDC_Server_print_all_metadata(); */

done:
    fflush(stdout);
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */

    FUNC_LEAVE(ret_value);
} // End of PDC_Server_restart

#ifdef ENABLE_MULTITHREAD
/*
 * Multi-thread Mercury progess
 *
 * \return Non-negative on success/Negative on failure
 */
static HG_THREAD_RETURN_TYPE
hg_progress_thread(void *arg)
{
    /* pthread_t tid = pthread_self(); */
    /* pid_t tid; */
    /* tid = syscall(SYS_gettid); */
    hg_context_t *context = (hg_context_t*)arg;
    HG_THREAD_RETURN_TYPE tret = (HG_THREAD_RETURN_TYPE) 0;
    hg_return_t ret = HG_SUCCESS;
    
    FUNC_ENTER(NULL);

    do {
        if (hg_atomic_cas32(&close_server_g, 1, 1)) break;

        /* printf("==PDC_SERVER[%d]: Before HG_Progress()\n", pdc_server_rank_g); */
        ret = HG_Progress(context, 100);
        /* printf("==PDC_SERVER[%d]: After HG_Progress()\n", pdc_server_rank_g); */
        /* printf("thread [%d]\n", tid); */
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    hg_thread_exit(tret);

    return tret;
}

/*
 * Multithread Mercury server to trigger and progress 
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_multithread_loop(hg_context_t *context)
{
    perr_t ret_value = SUCCEED;
    hg_thread_t progress_thread;
    hg_return_t ret = HG_SUCCESS;
    unsigned int actual_count;
    
    FUNC_ENTER(NULL);
    
    hg_thread_create(&progress_thread, hg_progress_thread, context);

    do {
        actual_count = 0;
        if (hg_atomic_get32(&close_server_g)) break;

        /* printf("==PDC_SERVER[%d]: Before HG_Trigger()\n", pdc_server_rank_g); */
        ret = HG_Trigger(context, HG_MAX_IDLE_TIME, 1, NULL);
        /* printf("==PDC_SERVER[%d]: After HG_Trigger()\n", pdc_server_rank_g); */
    } while (ret == HG_SUCCESS || ret == HG_TIMEOUT);

    hg_thread_join(progress_thread);

    // Destory pool
    hg_thread_pool_destroy(hg_test_thread_pool_g);

    FUNC_LEAVE(ret_value);
}
#endif

/*
 * Single-thread Mercury server to trigger and progress
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_loop(hg_context_t *hg_context)
{
    perr_t ret_value = SUCCEED;;
    hg_return_t hg_ret;
    unsigned int actual_count;
    
    FUNC_ENTER(NULL);
    
    /* Poke progress engine and check for events */
    do {
        actual_count = 0;
        do {
            /* printf("==%d: Before Trigger\n", pdc_server_rank_g); */
            /* fflush(stdout); */
            hg_ret = HG_Trigger(hg_context, 1024/* timeout */, 4096/* max count */, &actual_count);
            /* hg_ret = HG_Trigger(hg_context, 0/1* timeout *1/, 1 /1* max count *1/, &actual_count); */
            /* printf("==%d: After Trigger\n", pdc_server_rank_g); */
            /* fflush(stdout); */
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (hg_atomic_cas32(&close_server_g, 1, 1)) break;
        /* printf("==%d: Before Progress\n", pdc_server_rank_g); */
        /* fflush(stdout); */
        /* hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME); */
        /* printf("==%d: After Progress\n", pdc_server_rank_g); */
        /* fflush(stdout); */
        hg_ret = HG_Progress(hg_context, 50000);

    /* } while (hg_ret == HG_SUCCESS); */
    } while (hg_ret == HG_SUCCESS || hg_ret == HG_TIMEOUT);

    if (hg_ret == HG_SUCCESS) 
        ret_value = SUCCEED;
    else
        ret_value = FAIL;

    FUNC_LEAVE(ret_value);
}

static void PDC_print_IO_stats()
{
    // Debug print
#ifdef ENABLE_TIMING 
    double write_time_max, write_time_min, write_time_avg;
    double read_time_max,  read_time_min,  read_time_avg;
    double open_time_max,  open_time_min,  open_time_avg;
    double fsync_time_max, fsync_time_min, fsync_time_avg;
    double total_io_max,   total_io_min,   total_io_avg;
    double update_time_max, update_time_min, update_time_avg; 
    double get_info_time_max, get_info_time_min, get_info_time_avg;
    double io_elapsed_time_max, io_elapsed_time_min, io_elapsed_time_avg;

    #ifdef ENABLE_MPI
    MPI_Reduce(&server_write_time_g,  &write_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_write_time_g,  &write_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_write_time_g,  &write_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    write_time_avg /= pdc_server_size_g;

    MPI_Reduce(&server_read_time_g,    &read_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_read_time_g,    &read_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_read_time_g,    &read_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    read_time_avg /= pdc_server_size_g;

    MPI_Reduce(&server_fopen_time_g,   &open_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_fopen_time_g,   &open_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_fopen_time_g,   &open_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    open_time_avg /= pdc_server_size_g;

    MPI_Reduce(&server_fsync_time_g,  &fsync_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_fsync_time_g,  &fsync_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_fsync_time_g,  &fsync_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    fsync_time_avg /= pdc_server_size_g;

    MPI_Reduce(&server_total_io_time_g, &total_io_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_total_io_time_g, &total_io_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_total_io_time_g, &total_io_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    total_io_avg /= pdc_server_size_g;

    MPI_Reduce(&server_io_elapsed_time_g, &io_elapsed_time_max, 1, MPI_DOUBLE, MPI_MAX,0,MPI_COMM_WORLD);
    MPI_Reduce(&server_io_elapsed_time_g, &io_elapsed_time_min, 1, MPI_DOUBLE, MPI_MIN,0,MPI_COMM_WORLD);
    MPI_Reduce(&server_io_elapsed_time_g, &io_elapsed_time_avg, 1, MPI_DOUBLE, MPI_SUM,0,MPI_COMM_WORLD);
    io_elapsed_time_avg /= pdc_server_size_g;

    MPI_Reduce(&server_update_region_location_time_g, &update_time_max, 1, MPI_DOUBLE, MPI_MAX,0,MPI_COMM_WORLD);
    MPI_Reduce(&server_update_region_location_time_g, &update_time_min, 1, MPI_DOUBLE, MPI_MIN,0,MPI_COMM_WORLD);
    MPI_Reduce(&server_update_region_location_time_g, &update_time_avg, 1, MPI_DOUBLE, MPI_SUM,0,MPI_COMM_WORLD);
    update_time_avg /= pdc_server_size_g;

    MPI_Reduce(&server_get_storage_info_time_g, &get_info_time_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_get_storage_info_time_g, &get_info_time_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&server_get_storage_info_time_g, &get_info_time_avg, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    get_info_time_avg /= pdc_server_size_g;

    #else
    write_time_avg    = write_time_max    = write_time_min    = server_write_time_g;
    read_time_avg     = read_time_max     = read_time_min     = server_read_time_g;
    open_time_avg     = open_time_max     = open_time_min     = server_fopen_time_g;
    fsync_time_avg    = fsync_time_max    = fsync_time_min    = server_fsync_time_g;
    total_io_avg      = total_io_max      = total_io_min      = server_total_io_time_g;
    update_time_avg   = update_time_max   = update_time_min   = server_update_region_location_time_g;
    get_info_time_avg = get_info_time_max = get_info_time_min = server_get_storage_info_time_g;
    io_elapsed_time_avg = io_elapsed_time_max = io_elapsed_time_min = server_io_elapsed_time_g;
 
    #endif

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: IO STATS (MIN, AVG, MAX)\n"
               "              #fwrite %4d, Tfwrite (%6.2f, %6.2f, %6.2f), %.0f MB\n"
               "              #fread  %4d, Tfread  (%6.2f, %6.2f, %6.2f), %.0f MB\n"
               "              #fopen  %4d, Tfopen  (%6.2f, %6.2f, %6.2f)\n"
               "              Tfsync                (%6.2f, %6.2f, %6.2f)\n"
               "              Ttotal_IO             (%6.2f, %6.2f, %6.2f)\n"
               "              Ttotal_IO_elapsed     (%6.2f, %6.2f, %6.2f)\n"
               "              Tregion_update        (%6.2f, %6.2f, %6.2f)\n"
               "              Tget_region           (%6.2f, %6.2f, %6.2f)\n",  
                n_fwrite_g, write_time_min,    write_time_avg,    write_time_max, fwrite_total_MB, 
                n_fread_g ,  read_time_min,     read_time_avg,     read_time_max, fread_total_MB, 
                n_fopen_g ,  open_time_min,     open_time_avg,     open_time_max, 
                            fsync_time_min,    fsync_time_avg,    fsync_time_max,
                              total_io_min,      total_io_avg,      total_io_max,
                       io_elapsed_time_min, io_elapsed_time_avg, io_elapsed_time_max,
                           update_time_min,   update_time_avg,   update_time_max,
                         get_info_time_min, get_info_time_avg, get_info_time_max);
    }
#endif

}

static void PDC_Server_mercury_register()
{
    // Register RPC, metadata related
    client_test_connect_register(hg_class_g);
    gen_obj_id_register(hg_class_g);
    close_server_register(hg_class_g);
    metadata_query_register(hg_class_g);
    metadata_delete_register(hg_class_g);
    metadata_delete_by_id_register(hg_class_g);
    metadata_update_register(hg_class_g);
    metadata_add_tag_register(hg_class_g);
    region_lock_register(hg_class_g);
    region_release_register(hg_class_g);
    gen_cont_id_register(hg_class_g);

    // bulk
    query_partial_register(hg_class_g);
    cont_add_del_objs_rpc_register(hg_class_g);
    query_read_obj_name_rpc_register(hg_class_g);

    // Mapping
    buf_map_register(hg_class_g);
    reg_map_register(hg_class_g);
    buf_unmap_register(hg_class_g);
    reg_unmap_register(hg_class_g);
    obj_map_register(hg_class_g);
    obj_unmap_register(hg_class_g);

    // Data server
    data_server_read_register(hg_class_g);
    data_server_write_register(hg_class_g);
    data_server_read_check_register(hg_class_g);
    data_server_write_check_register(hg_class_g);

    // Analysis and Transforms
    set_execution_locus(SERVER_MEMORY);
    obj_data_iterator_register(hg_class_g);
    analysis_ftn_register(hg_class_g);
    transform_ftn_register(hg_class_g);

    // Server to client RPC
    server_lookup_client_register_id_g = server_lookup_client_register(hg_class_g);
    notify_io_complete_register_id_g   = notify_io_complete_register(hg_class_g);

    // Server to server RPC
    get_remote_metadata_register_id_g         = get_remote_metadata_register(hg_class_g);  
    buf_map_server_register_id_g              = buf_map_server_register(hg_class_g);
    buf_unmap_server_register_id_g            = buf_unmap_server_register(hg_class_g);
    obj_map_server_register_id_g              = obj_map_server_register(hg_class_g);
    obj_unmap_server_register_id_g            = obj_unmap_server_register(hg_class_g);
    server_lookup_remote_server_register_id_g = server_lookup_remote_server_register(hg_class_g);
    update_region_loc_register_id_g           = update_region_loc_register(hg_class_g);
    notify_region_update_register_id_g        = notify_region_update_register(hg_class_g);
    get_metadata_by_id_register_id_g          = get_metadata_by_id_register(hg_class_g);
    get_reg_lock_register_id_g                = get_reg_lock_register(hg_class_g);
    /* get_storage_info_register_id_g            = get_storage_info_register(hg_class_g); */
    bulk_rpc_register_id_g                    = bulk_rpc_register(hg_class_g);
    storage_meta_name_query_register_id_g     = storage_meta_name_query_rpc_register(hg_class_g);
    get_storage_meta_name_query_bulk_result_rpc_register_id_g = get_storage_meta_name_query_bulk_result_rpc_register(hg_class_g);
    notify_client_multi_io_complete_rpc_register_id_g = notify_client_multi_io_complete_rpc_register(hg_class_g);
    server_checkpoint_rpc_register_id_g       = server_checkpoing_rpc_register(hg_class_g);

}

static void PDC_Server_get_env()
{
    char *tmp_env_char;
    int default_nclient_per_node = 31;

    // Set up tmp dir
    tmp_env_char = getenv("PDC_TMPDIR");
    if (tmp_env_char == NULL) 
        tmp_env_char = "./pdc_tmp";

    snprintf(pdc_server_tmp_dir_g, ADDR_MAX,  "%s/", tmp_env_char);

    // Get number of OST per file
    tmp_env_char = getenv("PDC_NOST_PER_FILE");
    if (tmp_env_char!= NULL) {
        pdc_nost_per_file_g = atoi(tmp_env_char);
        // Make sure it is a sane value
        if (pdc_nost_per_file_g < 1 || pdc_nost_per_file_g > 248) {
            pdc_nost_per_file_g = 1;
        }
    }

    // Get number of clients per node
    tmp_env_char = (getenv("NCLIENT"));
    if (tmp_env_char == NULL)
        nclient_per_node = default_nclient_per_node;
    else
        nclient_per_node = atoi(tmp_env_char);

    // Get bb write percentage 
    tmp_env_char = getenv("PDC_BB_WRITE_PERCENT");
    if (tmp_env_char == NULL) 
        write_to_bb_percentage_g = 0;
    else
        write_to_bb_percentage_g = atoi(tmp_env_char);

    if (write_to_bb_percentage_g < 0 || write_to_bb_percentage_g > 100) 
        write_to_bb_percentage_g = 0;

    // Get debug environment var
    char *is_debug_env = getenv("PDC_DEBUG");
    if (is_debug_env != NULL) {
        is_debug_g = atoi(is_debug_env);
        if (pdc_server_rank_g == 0)
            printf("==PDC_SERVER[%d]: PDC_DEBUG set to %d!\n", pdc_server_rank_g, is_debug_g);
    }

    if (pdc_server_rank_g == 0) {
        printf("\n==PDC_SERVER[%d]: using [%s] as tmp dir. %d OSTs per data file, %d%% to BB\n", 
                pdc_server_rank_g, pdc_server_tmp_dir_g, pdc_nost_per_file_g, write_to_bb_percentage_g);
    }

    /* int pid = getpid(); */
    /* printf("==PDC_SERVER[%d]: my pid is %d\n", pdc_server_rank_g, pid); */
}

/*
 * Main function of PDC server
 *
 * \param  argc[IN]     Number of command line arguments
 * \param  argv[IN]     Command line arguments
 *
 * \return Non-negative on success/Negative on failure
 */
int main(int argc, char *argv[])
{
    int port;
    perr_t ret;
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_server_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_server_size_g);
#else
    pdc_server_rank_g = 0;
    pdc_server_size_g = 1;
#endif

#ifdef ENABLE_TIMING 
    struct timeval  start;
    struct timeval  end;
    double server_init_time;
    gettimeofday(&start, 0);
#endif

    if (argc > 1) 
        if (strcmp(argv[1], "restart") == 0) is_restart_g = 1;

    // Init rand seed
    srand(time(NULL));

    // Get environmental variables
    PDC_Server_get_env();

    port = pdc_server_rank_g % 32 + 7000 ;
    /* printf("rank=%d, port=%d\n", pdc_server_rank_g,port); */
    ret = PDC_Server_init(port, &hg_class_g, &hg_context_g);
    if (ret != SUCCEED || hg_class_g == NULL || hg_context_g == NULL) {
        printf("==PDC_SERVER[%d]: Error with Mercury init\n", pdc_server_rank_g);
        goto done;
    }

    // Register Mercury RPC/bulk
    PDC_Server_mercury_register();

#ifdef ENABLE_MPI
    // Need a barrier so that all servers finish init before the lookup process
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    
    // Lookup and get addresses of other servers
    char *lookup_on_demand = getenv("PDC_LOOKUP_ON_DEMAND");
    if (lookup_on_demand != NULL) {
        if (pdc_server_rank_g == 0) printf("==PDC_SERVER[0]: will lookup other PDC servers on demand\n");
    }
    else
        PDC_Server_lookup_all_servers();

    // Write server addrs to the config file for client to read from
    if (pdc_server_rank_g == 0) 
        if (PDC_Server_write_addr_to_file(all_addr_strings_g, pdc_server_size_g) != SUCCEED) 
            printf("==PDC_SERVER[%d]: Error with write config file\n", pdc_server_rank_g);

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&end, 0);
    server_init_time = PDC_get_elapsed_time_double(&start, &end);
#endif

    if (pdc_server_rank_g == 0) {
        #ifdef ENABLE_TIMING
        printf("==PDC_SERVER[%d]: total startup time = %.6f\n", pdc_server_rank_g, server_init_time);
        #endif
        printf("==PDC_SERVER[%d]: Server ready!\n\n\n", pdc_server_rank_g);
    }
    fflush(stdout);

    // Main loop to handle Mercury RPC/Bulk requests
#ifdef ENABLE_MULTITHREAD
    PDC_Server_multithread_loop(hg_context_g);
#else
    PDC_Server_loop(hg_context_g);
#endif

    // Exit from the loop, start finalize process
#ifdef ENABLE_CHECKPOINT
    char *tmp_env_char = getenv("PDC_DISABLE_CHECKPOINT");
    if (NULL != tmp_env_char) {
        if (pdc_server_rank_g == 0) printf("==PDC_SERVER[0]: checkpoint disabled!\n");
    }
    else  
        PDC_Server_checkpoint();
#endif

#ifdef ENABLE_TIMING 
    PDC_print_IO_stats();
#endif

done:
    PDC_Server_finalize();
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
} // End main

// END OF PDC_SERVER.C
