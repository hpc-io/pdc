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

#ifdef ENABLE_MPI
    #include "mpi.h"
#endif

#include "utlist.h"
#include "dablooms.h"
#include "hash-table.h"

#include "mercury.h"
#include "mercury_macros.h"

// Mercury hash table and list
/* #include "mercury_hash_table.h" */
/* #include "mercury_list.h" */

#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server.h"

#ifdef ENABLE_MULTITHREAD 
// Mercury multithread
#include "mercury_thread.h"
#include "mercury_thread_pool.h"
#include "mercury_thread_mutex.h"
hg_thread_mutex_t pdc_client_addr_metex_g;
hg_thread_mutex_t pdc_metadata_hash_table_mutex_g;
hg_thread_mutex_t pdc_container_hash_table_mutex_g;
hg_thread_mutex_t pdc_time_mutex_g;
hg_thread_mutex_t pdc_bloom_time_mutex_g;
hg_thread_mutex_t n_metadata_mutex_g;
hg_thread_mutex_t gen_obj_id_mutex_g;
hg_thread_mutex_t data_read_list_mutex_g;
hg_thread_mutex_t data_write_list_mutex_g;
hg_thread_mutex_t create_region_struct_metex_g;
hg_thread_mutex_t delete_buf_map_metex_g;
hg_thread_mutex_t remove_buf_map_metex_g;
hg_thread_mutex_t remove_lock_metex_g;
hg_thread_mutex_t pdc_server_task_mutex_g = NULL;
#endif

#ifdef PDC_HAS_CRAY_DRC
# include <rdmacred.h>
#endif

#define BLOOM_TYPE_T counting_bloom_t
#define BLOOM_NEW    new_counting_bloom
#define BLOOM_CHECK  counting_bloom_check
#define BLOOM_ADD    counting_bloom_add
#define BLOOM_REMOVE counting_bloom_remove
#define BLOOM_FREE   free_counting_bloom

// Global debug variable to control debug printfs
int is_debug_g = 0;
int pdc_client_num_g = 0;
int pdc_nost_per_file_g = 1;

hg_class_t   *hg_class_g   = NULL;
hg_context_t *hg_context_g = NULL;

// Below three are guarded by pdc_server_task_mutex_g for multi-thread
pdc_server_task_list_t *pdc_server_agg_task_head_g = NULL;
pdc_server_task_list_t *pdc_server_s2s_task_head_g = NULL;
int pdc_server_task_id_g = PDC_SERVER_TASK_INIT_VALUE;

/* int           work_todo_g = 0; */
/* int           s2s_send_work_todo_g = 0; */
/* int           s2s_recv_work_todo_g = 0; */
/* int           s2s_lookup_work_todo_g = 0; */

/* hg_context_t *pdc_client_context_g = NULL; */
/* int           pdc_to_client_work_todo_g = 0; */

pdc_client_info_t        *pdc_client_info_g        = NULL;
pdc_remote_server_info_t *pdc_remote_server_info_g = NULL;
char                     *all_addr_strings_1d_g    = NULL;
char                    **all_addr_strings_g       = NULL;
int                       is_all_client_connected_g  = 0;

static hg_id_t    get_remote_metadata_register_id_g;
static hg_id_t    buf_map_server_register_id_g;
static hg_id_t    buf_unmap_server_register_id_g;
static hg_id_t    region_lock_server_register_id_g;
static hg_id_t    region_release_server_register_id_g;
static hg_id_t    server_lookup_client_register_id_g;
static hg_id_t    server_lookup_remote_server_register_id_g;
static hg_id_t    notify_io_complete_register_id_g;
static hg_id_t    update_region_loc_register_id_g;
static hg_id_t    notify_region_update_register_id_g;
static hg_id_t    get_metadata_by_id_register_id_g;
static hg_id_t    get_reg_lock_register_id_g;
static hg_id_t    get_storage_info_register_id_g;
static hg_id_t    bulk_rpc_register_id_g;
static hg_id_t    storage_meta_name_query_register_id_g;
static hg_id_t    get_storage_meta_name_query_bulk_result_rpc_register_id_g;

// Global thread pool
extern hg_thread_pool_t *hg_test_thread_pool_g;
extern hg_thread_pool_t *hg_test_thread_pool_fs_g;

// Global hash table for storing metadata 
HashTable *metadata_hash_table_g  = NULL;
HashTable *container_hash_table_g = NULL;

// Global object region info list in local data server
data_server_region_t *dataserver_region_g = NULL;

hg_atomic_int32_t close_server_g;

int is_hash_table_init_g = 0;
int is_restart_g = 0;

int pdc_server_rank_g = 0;
int pdc_server_size_g = 1;

char pdc_server_tmp_dir_g[ADDR_MAX];

int write_to_bb_percentage_g          = 0;

// Debug statistics var
int n_bloom_total_g                   = 0;
int n_bloom_maybe_g                   = 0;
double server_bloom_check_time_g      = 0.0;
double server_bloom_insert_time_g     = 0.0;
double server_insert_time_g           = 0.0;
double server_delete_time_g           = 0.0;
double server_update_time_g           = 0.0;
double server_hash_insert_time_g      = 0.0;
double server_bloom_init_time_g       = 0.0;
double server_write_time_g            = 0.0;
double server_read_time_g             = 0.0;
double server_get_storage_info_time_g = 0.0;
double server_fopen_time_g            = 0.0;
double server_fsync_time_g            = 0.0;
int    n_fwrite_g                     = 0;
int    n_fread_g                      = 0;
int    n_fopen_g                      = 0;
double server_total_io_time_g         = 0.0;
double total_mem_usage_g              = 0.0;
uint32_t n_metadata_g                 = 0;
int    update_remote_region_count_g   = 0;
int    update_local_region_count_g    = 0;
double fread_total_MB                 = 0.0;
double fwrite_total_MB                = 0.0;

double server_update_region_location_time_g = 0.0;
double server_io_elapsed_time_g       = 0.0;

// Data server related
pdc_data_server_io_list_t *pdc_data_server_read_list_head_g = NULL;
pdc_data_server_io_list_t *pdc_data_server_write_list_head_g = NULL;

update_storage_meta_list_t *pdc_update_storage_meta_list_head_g = NULL;
int pdc_buffered_bulk_update_total_g = 0;
int pdc_nbuffered_bulk_update_g = 0;
int n_check_write_finish_returned_g = 0;
int buffer_read_request_total_g = 0;
int buffer_write_request_total_g = 0;
int buffer_write_request_num_g = 0;
int buffer_read_request_num_g = 0;

int is_server_direct_io_g = 0;

volatile int dbg_sleep_g = 1;

pbool_t region_is_identical(region_info_transfer_t reg1, region_info_transfer_t reg2)
{
    pbool_t ret_value = 0;

    FUNC_ENTER(NULL);

    if(reg1.ndim != reg2.ndim)
        PGOTO_DONE(ret_value);
    if (reg1.ndim >= 1) {
        if(reg1.count_0 != reg2.count_0 || reg1.start_0 != reg2.start_0)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 2) {
        if(reg1.count_1 != reg2.count_1 || reg1.start_1 != reg2.start_1)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 3) {
        if(reg1.count_2 != reg2.count_2 || reg1.start_2 != reg2.start_2)
            PGOTO_DONE(ret_value);
    }
    if (reg1.ndim >= 4) {
        if(reg1.count_3 != reg2.count_3 || reg1.start_3 != reg2.start_3)
            PGOTO_DONE(ret_value);
    }
    ret_value = 1;
done:
    FUNC_LEAVE(ret_value);
}

static int is_region_transfer_t_identical(region_info_transfer_t *a, region_info_transfer_t *b)
{
    int ret_value = -1;

    FUNC_ENTER(NULL);

    if (a == NULL || b == NULL) {
        PGOTO_DONE(ret_value);
    }

    if (a->ndim != b->ndim) {
        PGOTO_DONE(ret_value);
    }

    if(a->ndim >= 1) {
        if(a->start_0 != b->start_0 || a->count_0 != b->count_0)
            PGOTO_DONE(ret_value);
    }
    if(a->ndim >= 2) {
        if(a->start_1 != b->start_1 || a->count_1 != b->count_1)
            PGOTO_DONE(ret_value); 
    }
    if(a->ndim >= 3) {
        if(a->start_2 != b->start_2 || a->count_2 != b->count_2)
            PGOTO_DONE(ret_value);
    }
    if(a->ndim >= 4) {
        if(a->start_3 != b->start_3 || a->count_3 != b->count_3)
            PGOTO_DONE(ret_value);
    }
    ret_value = 1;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Check if two hash keys are equal 
 *
 * \param vlocation1 [IN]       Hash table key
 * \param vlocation2 [IN]       Hash table key
 *
 * \return 1 if two keys are equal, 0 otherwise
 */
static int 
PDC_Server_metadata_int_equal(void *vlocation1, void *vlocation2)
{
    return *((uint32_t *) vlocation1) == *((uint32_t *) vlocation2);
}

/*
 * Get hash key's location in hash table
 *
 * \param vlocation [IN]        Hash table key
 *
 * \return the location of hash key in the table
 */
static unsigned int
PDC_Server_metadata_int_hash(void *vlocation)
{
    return *((uint32_t *) vlocation);
}

/*
 * Free the hash key 
 *
 * \param  key [IN]        Hash table key
 *
 * \return void
 */
static void
PDC_Server_metadata_int_hash_key_free(void * key)
{
    free((uint32_t *) key);
}

/*
 * Free metadata hash value
 *
 * \param  value [IN]        Hash table value
 *
 * \return void
 */
static void
PDC_Server_metadata_hash_value_free(void * value)
{
    pdc_metadata_t *elt, *tmp;
    pdc_hash_table_entry_head *head;

    FUNC_ENTER(NULL);
    
    head = (pdc_hash_table_entry_head*) value;

    // Free bloom filter
    if (head->bloom != NULL) {
        BLOOM_FREE(head->bloom);
    }

    // Free metadata list
    if (is_restart_g == 0) {
        /* printf("freeing individual head->metadata\n"); */
        /* fflush(stdout); */
        DL_FOREACH_SAFE(head->metadata,elt,tmp) {
          /* DL_DELETE(head,elt); */
          free(elt);
        }
    }
    /* else { */
        /* printf("freeing head->metadata\n"); */
        /* fflush(stdout); */
        /* if (head->metadata != NULL) { */
        /*     free(head->metadata); */
        /* } */
    /* } */
}

/*
 * Free container hash value
 *
 * \param  value [IN]        Hash table value
 *
 * \return void
 */
static void
PDC_Server_container_hash_value_free(void * value)
{
    pdc_cont_hash_table_entry_t *head = (pdc_cont_hash_table_entry_t*) value;
    free(head->obj_ids);
}

/*
 * Set the Lustre stripe count/size of a given path
 *
 * \param  path[IN]             Directory to be set with Lustre stripe/count
 * \param  stripe_count[IN]     Stripe count
 * \param  stripe_size_MB[IN]   Stripe size in MB
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_set_lustre_stripe(const char *path, int stripe_count, int stripe_size_MB)
{
    perr_t ret_value = SUCCEED;
    size_t len;
    int i, index;
    char tmp[ADDR_MAX];
    char cmd[ADDR_MAX];

    FUNC_ENTER(NULL);

    // Make sure stripe count is sane
    if (stripe_count > 248 / pdc_server_size_g )
        stripe_count = 248 / pdc_server_size_g;

    if (stripe_count < 1)
        stripe_count = 1;

    if (stripe_size_MB <= 0)
        stripe_size_MB = 1;

    index = (pdc_server_rank_g * stripe_count) % 248;

    snprintf(tmp, sizeof(tmp),"%s",path);

    len = strlen(tmp);
    for (i = len-1; i >= 0; i--) {
        if (tmp[i] == '/') {
            tmp[i] = 0;
            break;
        }
    }
    /* sprintf(cmd, "lfs setstripe -S %dM -c %d %s", stripe_size_MB, stripe_count, tmp); */
    sprintf(cmd, "lfs setstripe -S %dM -c %d -i %d %s", stripe_size_MB, stripe_count, index, tmp);

    if (system(cmd) < 0) {
        printf("==PDC_SERVER: Fail to set Lustre stripe parameters [%s]\n", tmp);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

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
 * Init the PDC metadata structure
 *
 * \param  a [IN]        PDC metadata structure 
 *
 * \return void
 */
void PDC_Server_metadata_init(pdc_metadata_t* a)
{
    int i;
    
    FUNC_ENTER(NULL);
    
    a->user_id              = 0;
    a->time_step            = 0;
    a->app_name[0]          = 0;
    a->obj_name[0]          = 0;

    a->obj_id               = 0;
    a->ndim                 = 0;
    for (i = 0; i < DIM_MAX; i++) 
        a->dims[i] = 0;

    a->create_time          = 0;
    a->last_modified_time   = 0;
    a->tags[0]              = 0;
    a->data_location[0]     = 0;

    a->region_lock_head     = NULL;
    a->region_map_head      = NULL;
    a->region_buf_map_head  = NULL;
    a->prev                 = NULL;
    a->next                 = NULL;
}
// ^ hash table

/*
 * Concatenate the metadata's obj_name and time_step to one char array
 *
 * \param  metadata [IN]        PDC metadata structure pointer
 * \param  output   [OUT]       Concatenated char array pointer
 *
 * \return void
 */
static inline void combine_obj_info_to_string(pdc_metadata_t *metadata, char *output)
{
    /* sprintf(output, "%d%s%s%d", metadata->user_id, metadata->app_name, metadata->obj_name, metadata->time_step); */
    FUNC_ENTER(NULL);
    sprintf(output, "%s%d", metadata->obj_name, metadata->time_step);
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
 * Callback function of the server to lookup clients, also gets the confirm message from client.
 *
 * \param  callback_info[IN]        Mercury callback info pointer 
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
server_lookup_client_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    server_lookup_client_out_t output;
    server_lookup_args_t *server_lookup_args = (server_lookup_args_t*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    FUNC_ENTER(NULL);

    /* Get output from client */
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_lookup_client_rpc_cb - error HG_Get_output\n", pdc_server_rank_g);
        server_lookup_args->ret_int = -1;
        goto done;
    }
    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: server_lookup_client_rpc_cb,  got output from client=%d\n", 
                pdc_server_rank_g, output.ret);
    }
    server_lookup_args->ret_int = output.ret;

done:
    /* pdc_to_client_work_todo_g = 0; */
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function for HG_Addr_lookup(), creates a Mercury handle then forward the RPC message 
 * to the client
 *
 * \param  callback_info[IN]        Mercury callback info pointer 
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
PDC_Server_lookup_client_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    uint32_t client_id;
    server_lookup_args_t *server_lookup_args;

    FUNC_ENTER(NULL);

    server_lookup_args = (server_lookup_args_t*) callback_info->arg;
    client_id = server_lookup_args->client_id;

    pdc_client_info_g[client_id].addr = callback_info->info.lookup.addr;
    pdc_client_info_g[client_id].addr_valid = 1;

    /* // Create HG handle if needed */
    /* HG_Create(hg_context_g, pdc_client_info_g[client_id].addr, server_lookup_client_register_id_g, */ 
    /*           &server_lookup_client_handle); */

    /* // Fill input structure */
    /* in.server_id   = server_lookup_args->server_id; */
    /* in.server_addr = server_lookup_args->server_addr; */
    /* in.nserver     = pdc_server_rank_g; */

    /* ret_value = HG_Forward(server_lookup_client_handle, server_lookup_client_rpc_cb, server_lookup_args, &in); */
    /* if (ret_value != HG_SUCCESS) { */
    /*     fprintf(stderr, "server_lookup_client__cb(): Could not start HG_Forward()\n"); */
    /*     return EXIT_FAILURE; */
    /* } */

    /* if (is_debug_g == 1) { */
    /*     printf("==PDC_SERVER[%d]: PDC_Server_lookup_client_cb() - forwarded to client %d\n", */ 
    /*             pdc_server_rank_g, client_id); */
    /*     fflush(stdout); */
    /* } */

    /* ret_value = HG_Destroy(server_lookup_client_handle); */
    /* if (ret_value != HG_SUCCESS) { */
    /*     printf("==PDC_SERVER[%d]: PDC_Server_lookup_client_cb() - " */
    /*             "HG_Destroy(server_lookup_client_handle) error!\n", pdc_server_rank_g); */
    /*     fflush(stdout); */
    /* } */

    FUNC_LEAVE(ret_value);
}

/*
 * Lookup the available clients to obtain proper address of them for future communication
 * via Mercury.
 *
 * \param  client_id[IN]        Client's MPI rank
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_lookup_client(uint32_t client_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;

    FUNC_ENTER(NULL);

    if (pdc_client_num_g <= 0) {
        printf("==PDC_SERVER: PDC_Server_lookup_client() - number of client <= 0!\n");
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g[client_id].addr_valid == 1) 
        goto done;

    // Lookup and fill the client info
    server_lookup_args_t lookup_args;
    char *target_addr_string;

    lookup_args.server_id = pdc_server_rank_g;
    lookup_args.client_id = client_id;
    lookup_args.server_addr = pdc_client_info_g[client_id].addr_string;
    target_addr_string = pdc_client_info_g[client_id].addr_string;

    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: Testing connection to client %d: %s\n", 
                pdc_server_rank_g, client_id, target_addr_string);
        fflush(stdout);
    }

    hg_ret = HG_Addr_lookup(hg_context_g, PDC_Server_lookup_client_cb, 
                            &lookup_args, target_addr_string, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS ) {
        printf("==PDC_SERVER: Connection to client %d FAILED!\n", client_id);
        ret_value = FAIL;
        goto done;
    }

    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: waiting for client %d\n", pdc_server_rank_g, client_id);
        fflush(stdout);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

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

    /* printf("==PDC_Server_get_client_addr!\n"); */
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
 * Get the metadata with obj ID and hash key
 *
 * \param  obj_id[IN]        Object ID
 * \param  hash_key[IN]      Hash value of the ID attributes
 *
 * \return NULL if no match is found/pointer to the metadata structure otherwise
 */
/* static pdc_metadata_t * find_metadata_by_id_and_hash_key(uint64_t obj_id, uint32_t hash_key) */ 
/* { */
/*     pdc_metadata_t *ret_value = NULL; */
/*     pdc_metadata_t *elt; */
/*     pdc_hash_table_entry_head *lookup_value = NULL; */
    
/*     FUNC_ENTER(NULL); */

/*     if (metadata_hash_table_g != NULL) { */
/*         lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key); */

/*         if (lookup_value == NULL) { */
/*             ret_value = NULL; */
/*             goto done; */
/*         } */

/*         DL_FOREACH(lookup_value->metadata, elt) { */
/*             if (elt->obj_id == obj_id) { */
/*                 ret_value = elt; */
/*                 goto done; */
/*             } */
/*         } */

/*     }  // if (metadata_hash_table_g != NULL) */
/*     else { */
/*         printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n"); */
/*         ret_value = NULL; */
/*         goto done; */
/*     } */
/* done: */
/*     FUNC_LEAVE(ret_value); */
/* } */

/*
 * Get the metadata with obj ID from the metadata list
 *
 * \param  mlist[IN]         Metadata list head 
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t * find_metadata_by_id_from_list(pdc_metadata_t *mlist, uint64_t obj_id) 
{
    pdc_metadata_t *ret_value, *elt;
    
    FUNC_ENTER(NULL);

    ret_value = NULL;
    if (mlist == NULL) {
        ret_value = NULL;
        goto done;
    }

    DL_FOREACH(mlist, elt) {
        if (elt->obj_id == obj_id) {
            ret_value = elt;
            goto done;
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t get_remote_metadata_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    get_remote_metadata_out_t out;
    struct get_remote_metadata_arg *get_meta_args;

    FUNC_ENTER(NULL);

    get_meta_args = (struct get_remote_metadata_arg*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from client */
    ret_value = HG_Get_output(handle, &out);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: get_remote_metadata_rpc_cb - error with HG_Get_output\n",
                pdc_server_rank_g);
        get_meta_args->data = NULL;
        goto done;
    }

    if (out.ret.user_id == -1 && out.ret.obj_id == 0 && out.ret.time_step == -1) {
        get_meta_args->data = NULL;
    }
    else {
        get_meta_args->data = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
        if (get_meta_args->data == NULL) {
            printf("==PDC_SERVER: ERROR - get_remote_metadata_rpc_cb() cannnot allocate space for client_lookup_args->data \n");
        }

        // Now copy the received metadata info
        ret_value = PDC_metadata_init(get_meta_args->data);
        ret_value = pdc_transfer_t_to_metadata_t(&out.ret, get_meta_args->data);
    }

done:
    HG_Free_output(handle, &out);
    FUNC_LEAVE(ret_value);
}

/*
static pdc_metadata_t * find_remote_metadata_by_id(uint32_t meta_server_id, uint64_t obj_id)
{
    pdc_metadata_t *ret_value = NULL;
    hg_return_t  hg_ret = HG_SUCCESS;
    hg_handle_t get_remote_metadata_handle;
    get_remote_metadata_in_t in;
    struct get_remote_metadata_arg get_meta_args;

    FUNC_ENTER(NULL);

    in.obj_id = obj_id;

    HG_Create(hg_context_g, pdc_remote_server_info_g[meta_server_id].addr, get_remote_metadata_register_id_g, &get_remote_metadata_handle);

    hg_ret = HG_Forward(get_remote_metadata_handle, get_remote_metadata_rpc_cb, &get_meta_args, &in);
    if (hg_ret != HG_SUCCESS) {
        HG_Destroy(get_remote_metadata_handle);
        PGOTO_ERROR(FAIL, "find_remote_metadata_by_id(): Could not start HG_Forward()");
    }

printf("find_remote_metadata_by_id() calls PDC_Server_check_response()\n");
fflush(stdout);
    work_todo_g = 1;
    PDC_Server_check_response(&hg_context_g, &work_todo_g);
    if(get_meta_args.data == NULL) {
        PGOTO_ERROR(FAIL, "find_remote_metadata_by_id() got NULL metadata");
    }
    ret_value = get_meta_args.data;

done:
    HG_Destroy(get_remote_metadata_handle);
    FUNC_LEAVE(ret_value);
}
*/

/*
 * Get the metadata with the specified object ID by iteration of all metadata in the hash table
 *
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t * find_metadata_by_id(uint64_t obj_id) 
{
    pdc_metadata_t *ret_value;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    HashTablePair pair;
    int n_entry;
    
    FUNC_ENTER(NULL);

    if (metadata_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {

            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt) {
                if (elt->obj_id == obj_id) {
                    return elt;
                }
            }
        }
    }  // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = NULL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Wrapper function of find_metadata_by_id().
 *
 * \param  obj_id[IN]        Object ID
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
pdc_metadata_t *PDC_Server_get_obj_metadata(pdcid_t obj_id)
{
    pdc_metadata_t *ret_value = NULL;
    
    FUNC_ENTER(NULL);

    ret_value = find_metadata_by_id(obj_id);
    if (ret_value == NULL) {
        printf("==PDC_SERVER[%d]: PDC_Server_get_obj_metadata() - cannot find meta with id %" PRIu64 "\n",
                pdc_server_rank_g, obj_id);
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Find if there is identical metadata exist in hash table
 *
 * \param  entry[IN]        Hash table entry of metadata
 * \param  a[IN]            Pointer to metadata to be checked against
 *
 * \return NULL if no match is found/pointer to the found metadata otherwise
 */
static pdc_metadata_t * find_identical_metadata(pdc_hash_table_entry_head *entry, pdc_metadata_t *a)
{
    pdc_metadata_t *ret_value = NULL;
    BLOOM_TYPE_T *bloom;
    int bloom_check;
    
    FUNC_ENTER(NULL);

/*     printf("==PDC_SERVER: querying with:\n"); */
/*     PDC_print_metadata(a); */
/*     fflush(stdout); */

    // Use bloom filter to quick check if current metadata is in the list
    if (entry->bloom != NULL && a->user_id != 0 && a->app_name[0] != 0) {
        /* printf("==PDC_SERVER: using bloom filter\n"); */
        /* fflush(stdout); */

        bloom = entry->bloom;

        char combined_string[ADDR_MAX];
        combine_obj_info_to_string(a, combined_string);
        /* printf("bloom_check: Combined string: %s\n", combined_string); */
        /* fflush(stdout); */

        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start;
        struct timeval  pdc_timer_end;
        double ht_total_sec;
 
        gettimeofday(&pdc_timer_start, 0);
        #endif

        bloom_check = BLOOM_CHECK(bloom, combined_string, strlen(combined_string));

        #ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        #endif

        #ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_lock(&pdc_bloom_time_mutex_g);
        #endif

        #ifdef ENABLE_TIMING 
        server_bloom_check_time_g += ht_total_sec;
        #endif

        #ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&pdc_bloom_time_mutex_g);
        #endif

        n_bloom_total_g++;
        if (bloom_check == 0) {
            /* printf("==PDC_SERVER[%d]: Bloom filter says definitely not %s!\n", pdc_server_rank_g, combined_string); */
            /* fflush(stdout); */
            ret_value = NULL;
            goto done;
        }
        else {
            // bloom filter says maybe, so need to check entire list
            /* printf("==PDC_SERVER[%d]: Bloom filter says maybe for %s!\n", pdc_server_rank_g, combined_string); */
            /* fflush(stdout); */
            n_bloom_maybe_g++;
            pdc_metadata_t *elt;
            DL_FOREACH(entry->metadata, elt) {
                if (PDC_metadata_cmp(elt, a) == 0) {
                    /* printf("Identical metadata exist in Metadata store!\n"); */
                    /* PDC_print_metadata(a); */
                    ret_value = elt;
                    goto done;
                }
            }
        }

    }
    else {
        // Bloom has not been created
        /* printf("Bloom filter not created yet!\n"); */
        /* fflush(stdout); */
        pdc_metadata_t *elt;
        DL_FOREACH(entry->metadata, elt) {
            if (PDC_metadata_cmp(elt, a) == 0) {
                /* printf("Identical metadata exist in Metadata store!\n"); */
                /* PDC_print_metadata(a); */
                ret_value = elt;
                goto done;
            }
        }
    } // if bloom==NULL
    
done:
    /* int count; */
    /* DL_COUNT(lookup_value, elt, count); */
    /* printf("%d item(s) in list\n", count); */

    FUNC_LEAVE(ret_value);
} 

data_server_region_t *PDC_Server_get_obj_region(pdcid_t obj_id) 
{
    data_server_region_t *ret_value = NULL;
    data_server_region_t *elt = NULL;

    FUNC_ENTER(NULL);

    if(dataserver_region_g != NULL) {
       DL_FOREACH(dataserver_region_g, elt) {
           if (elt->obj_id == obj_id)
               ret_value = elt;
       }
    }

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
 * Allocate a new object ID
 *
 * \return 64-bit integer of object ID
 */
static uint64_t PDC_Server_gen_obj_id()
{
    uint64_t ret_value;
    
    FUNC_ENTER(NULL);
    
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&gen_obj_id_mutex_g);
#endif

    ret_value = pdc_id_seq_g++;
    
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&gen_obj_id_mutex_g);
#endif

    FUNC_LEAVE(ret_value);
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
    
    sprintf(config_fname, "%s%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);
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

    sprintf(config_fname, "%s%s", pdc_server_tmp_dir_g, pdc_server_cfg_name_g);

    if (remove(config_fname) != 0) {
        printf("==PDC_SERVER[%d]: Unable to delete the config file[%s]", pdc_server_rank_g, config_fname);
        ret_value = FAIL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Init the hash table for metadata storage
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_init_hash_table()
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    // Metadata hash table
    metadata_hash_table_g = hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (metadata_hash_table_g == NULL) {
        printf("==PDC_SERVER: metadata_hash_table_g init error! Exit...\n");
        goto done;
    }
    hash_table_register_free_functions(metadata_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_metadata_hash_value_free);

    // Container hash table
    container_hash_table_g = hash_table_new(PDC_Server_metadata_int_hash, PDC_Server_metadata_int_equal);
    if (container_hash_table_g == NULL) {
        printf("==PDC_SERVER: container_hash_table_g init error! Exit...\n");
        goto done;
    }
    hash_table_register_free_functions(container_hash_table_g, PDC_Server_metadata_int_hash_key_free, PDC_Server_container_hash_value_free);

    is_hash_table_init_g = 1;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Remove a metadata from bloom filter
 *
 * \param  metadata[IN]     Metadata pointer of the remove target
 * \param  bloom[IN]        Bloom filter's pointer
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_remove_from_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);

    if (bloom == NULL) {
        printf("==PDC_SERVER: PDC_Server_remove_from_bloom(): bloom pointer is NULL\n");
        ret_value = FAIL;
        goto done;
    }
 
    char combined_string[ADDR_MAX];
    combine_obj_info_to_string(metadata, combined_string);
    /* printf("==PDC_SERVER: PDC_Server_remove_from_bloom(): Combined string: %s\n", combined_string); */

    ret_value = BLOOM_REMOVE(bloom, combined_string, strlen(combined_string));
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_remove_from_bloom() - error\n",
                pdc_server_rank_g);
        goto done;
    }


done:
    FUNC_LEAVE(ret_value);
}

/*
 * Add a metadata to bloom filter
 *
 * \param  metadata[IN]     Metadata pointer of the target
 * \param  bloom[IN]        Bloom filter's pointer
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_add_to_bloom(pdc_metadata_t *metadata, BLOOM_TYPE_T *bloom)
{
    perr_t ret_value = SUCCEED;
    char combined_string[ADDR_MAX];
    
    FUNC_ENTER(NULL);

    if (bloom == NULL) {
        /* printf("==PDC_SERVER: PDC_Server_add_to_bloom(): bloom pointer is NULL\n"); */
        /* ret_value = FAIL; */
        goto done;
    }

    combine_obj_info_to_string(metadata, combined_string);
    /* printf("==PDC_SERVER[%d]: PDC_Server_add_to_bloom(): Combined string: %s\n", pdc_server_rank_g, combined_string); */
    /* fflush(stdout); */

    // Only add to bloom filter if it's definately not 
    /* int bloom_check; */
    /* bloom_check = BLOOM_CHECK(bloom, combined_string, strlen(combined_string)); */
    /* if (bloom_check == 0) { */
        ret_value = BLOOM_ADD(bloom, combined_string, strlen(combined_string));
    /* } */
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_add_to_bloom() - error \n",
                pdc_server_rank_g);
        goto done;
    }


done:
    FUNC_LEAVE(ret_value);
}

/*
 * Init a bloom filter
 *
 * \param  entry[IN]     Entry of the metadata hash table
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_bloom_init(pdc_hash_table_entry_head *entry)
{
    perr_t      ret_value = 0;
    int capacity = 500000;
    double error_rate = 0.05;
    
    FUNC_ENTER(NULL);

    // Init bloom filter
    /* int capacity = 100000; */
    n_bloom_maybe_g = 0;
    n_bloom_total_g = 0;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;
    
    gettimeofday(&pdc_timer_start, 0);
#endif

    entry->bloom = (BLOOM_TYPE_T*)BLOOM_NEW(capacity, error_rate);
    if (!entry->bloom) {
        fprintf(stderr, "ERROR: Could not create bloom filter\n");
        ret_value = -1;
        goto done;
    }

    /* PDC_Server_add_to_bloom(entry, bloom); */

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    server_bloom_init_time_g += ht_total_sec;
#endif

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Insert a metadata to the metadata hash table
 *
 * \param  head[IN]      Head of the hash table
 * \param  new[IN]       Metadata pointer to be inserted
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_hash_table_list_insert(pdc_hash_table_entry_head *head, pdc_metadata_t *new)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *elt;
    
    FUNC_ENTER(NULL);

    // add to bloom filter
    if (head->n_obj == CREATE_BLOOM_THRESHOLD) {
        /* printf("==PDC_SERVER:Init bloom\n"); */
        /* fflush(stdout); */
        PDC_Server_bloom_init(head);
        DL_FOREACH(head->metadata, elt) {
            PDC_Server_add_to_bloom(elt, head->bloom);
        }
        ret_value = PDC_Server_add_to_bloom(new, head->bloom);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_hash_table_list_insert() - error add to bloom\n",
                    pdc_server_rank_g);
            goto done;
        }
    }
    else if (head->n_obj >= CREATE_BLOOM_THRESHOLD || head->bloom != NULL) {
        /* printf("==PDC_SERVER: Adding to bloom filter, %d existed\n", head->n_obj); */
        /* fflush(stdout); */
        ret_value = PDC_Server_add_to_bloom(new, head->bloom);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_hash_table_list_insert() - error add to bloom\n",
                    pdc_server_rank_g);
            goto done;
        }
    
    }

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&insert_hash_table_mutex_g);
#endif
    /* printf("Adding to linked list\n"); */
    // Currently $metadata is unique, insert to linked list
    DL_APPEND(head->metadata, new);
    head->n_obj++;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&insert_hash_table_mutex_g);
#endif
    /* // Debug print */
    /* int count; */
    /* pdc_metadata_t *elt; */
    /* DL_COUNT(head, elt, count); */
    /* printf("Append one metadata, total=%d\n", count); */
done:
 
    FUNC_LEAVE(ret_value);
}

/*
 * Init a metadata list (doubly linked) under the given hash table entry
 *
 * \param  entry[IN]        An entry pointer of the hash table
 * \param  hash_key[IN]     Hash key of the entry
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_hash_table_list_init(pdc_hash_table_entry_head *entry, uint32_t *hash_key)
{
    
    perr_t      ret_value = SUCCEED;
    hg_return_t ret;
    
    FUNC_ENTER(NULL);

    /* printf("==PDC_SERVER[%d]: hash entry init for hash key [%u]\n", pdc_server_rank_g, *hash_key); */

/*     // Init head of linked list */
/*     metadata->prev = metadata;                                                                   \ */
/*     metadata->next = NULL; */   

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    // Insert to hash table
    ret = hash_table_insert(metadata_hash_table_g, hash_key, entry);
    if (ret != 1) {
        fprintf(stderr, "PDC_Server_hash_table_list_init(): Error with hash table insert!\n");
        ret_value = FAIL;
        goto done;
    }

    /* PDC_print_metadata(entry->metadata); */
    /* printf("entry n_obj=%d\n", entry->n_obj); */

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

    server_hash_insert_time_g += ht_total_sec;
#endif

    /* PDC_Server_bloom_init(new); */

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Add the tag received from one client to the corresponding metadata structure
 *
 * \param  in[IN]       Input structure received from client
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_add_tag_metadata(metadata_add_tag_in_t *in, metadata_add_tag_out_t *out)
{

    FUNC_ENTER(NULL);

    perr_t ret_value;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER: Got add_tag request: hash=%u, obj_id=%" PRIu64 "\n", in->hash_value, in->obj_id); */

    uint32_t *hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    uint64_t obj_id = in->obj_id;

    pdc_hash_table_entry_head *lookup_value;

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("==PDC_SERVER: checking hash table with key=%d\n", *hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            /* printf("==PDC_SERVER: lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            pdc_metadata_t *target;
            target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
            if (target != NULL) {
                /* printf("==PDC_SERVER: Found add_tag target!\n"); */
                /* printf("Received add_tag info:\n"); */
                /* PDC_print_metadata(&in->new_metadata); */

                // Check and find valid add_tag fields
                // Currently user_id, obj_name are not supported to be updated in this way
                // obj_name change is done through client with delete and add operation.
                if (in->new_tag != NULL && in->new_tag[0] != 0 &&
                        !(in->new_tag[0] == ' ' && in->new_tag[1] == 0)) {
                    // add a ',' to separate different tags
                    /* printf("Previous tags: %s\n", target->tags); */
                    /* printf("Adding tags: %s\n", in->new_metadata.tags); */
                    target->tags[strlen(target->tags)+1] = 0;
                    target->tags[strlen(target->tags)] = ',';
                    strcat(target->tags, in->new_tag);
                    /* printf("Final tags: %s\n", target->tags); */
                }

                out->ret  = 1;

            } // if (lookup_value != NULL) 
            else {
                // Object not found for deletion request
                /* printf("==PDC_SERVER: add tag target not found!\n"); */
                ret_value = FAIL;
                out->ret  = -1;
            }
       
        } // if lookup_value != NULL
        else {
            /* printf("==PDC_SERVER: add tag target not found!\n"); */
            ret_value = FAIL;
            out->ret = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
        ret_value = FAIL;
        out->ret = -1;
        goto done;
    }

    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_add_tag_metadata() - error \n",
                pdc_server_rank_g);
        goto done;
    }


#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif   

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING 
    server_update_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif
    

done:
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of add_tag_metadata_from_hash_table

/*
 * Update the metadata received from one client to the corresponding metadata structure
 *
 * \param  in[IN]       Input structure received from client
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_metadata(metadata_update_in_t *in, metadata_update_out_t *out)
{
    perr_t ret_value;
    uint64_t obj_id;
    pdc_hash_table_entry_head *lookup_value;
    /* pdc_metadata_t *elt; */
    uint32_t *hash_key;
    pdc_metadata_t *target;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;
    
    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER: Got update request: hash=%u, obj_id=%" PRIu64 "\n", in->hash_value, in->obj_id); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    obj_id = in->obj_id;

#ifdef ENABLE_MULTITHREAD 
    int unlocked = 0;
    // Obtain lock for hash table
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("==PDC_SERVER: checking hash table with key=%d\n", *hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            /* printf("==PDC_SERVER: lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            target = find_metadata_by_id_from_list(lookup_value->metadata, obj_id);
            if (target != NULL) {
                /* printf("==PDC_SERVER: Found update target!\n"); */
                /* printf("Received update info:\n"); */
                /* PDC_print_metadata(&in->new_metadata); */
                /* printf("==PDC_SERVER: new dataloc: [%s]\n", in->new_metadata.data_location); */

                // Check and find valid update fields
                // Currently user_id, obj_name are not supported to be updated in this way
                // obj_name change is done through client with delete and add operation.
                if (in->new_metadata.time_step != -1) 
                    target->time_step = in->new_metadata.time_step;
                if (in->new_metadata.app_name[0] != 0 && 
                        !(in->new_metadata.app_name[0] == ' ' && in->new_metadata.app_name[1] == 0)) 
                    strcpy(target->app_name,      in->new_metadata.app_name);
                if (in->new_metadata.data_location[0] != 0 && 
                        !(in->new_metadata.data_location[0] == ' ' && in->new_metadata.data_location[1] == 0)) 
                    strcpy(target->data_location, in->new_metadata.data_location);
                if (in->new_metadata.tags[0] != 0 &&
                        !(in->new_metadata.tags[0] == ' ' && in->new_metadata.tags[1] == 0)) {
                    // add a ',' to separate different tags
                    /* printf("Previous tags: %s\n", target->tags); */
                    /* printf("Adding tags: %s\n", in->new_metadata.tags); */
                    target->tags[strlen(target->tags)+1] = 0;
                    target->tags[strlen(target->tags)] = ',';
                    strcat(target->tags, in->new_metadata.tags);
                    /* printf("Final tags: %s\n", target->tags); */
                }

                out->ret  = 1;
            } // if (lookup_value != NULL) 
            else {
                // Object not found for deletion request
                /* printf("==PDC_SERVER: update target not found!\n"); */
                ret_value = -1;
                out->ret  = -1;
            }
       
        } // if lookup_value != NULL
        else {
            /* printf("==PDC_SERVER: update target not found!\n"); */
            ret_value = -1;
            out->ret = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        out->ret = -1;
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif   

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING 
    server_update_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif
    

done:
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of update_metadata_from_hash_table

/*
 * Delete metdata with the ID received from a client
 *
 * \param  in[IN]       Input structure received from client, conatins object ID
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_delete_metadata_by_id(metadata_delete_by_id_in_t *in, metadata_delete_by_id_out_t *out)
{
    perr_t ret_value = FAIL;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    HashTablePair pair;
    uint64_t target_obj_id;
    int n_entry;

    FUNC_ENTER(NULL);

    out->ret = -1;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif


    /* printf("==PDC_SERVER[%d]: Got delete by id request: obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->obj_id); */
    /* fflush(stdout); */

    target_obj_id = in->obj_id;

    /* printf("==PDC_SERVER: delete request name:%s ts=%d hash=%u\n", in->obj_name, in->time_step, in->hash_value); */

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {

        // Since we only have the obj id, need to iterate the entire hash table
        pdc_hash_table_entry_head *head; 

        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {

            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt) {

                if (elt->obj_id == target_obj_id) {
                    // We found the delete target
                    // Check if there are more objects in this list
                    if (head->n_obj > 1) {
                        // Remove from bloom filter
                        if (head->bloom != NULL) {
                            PDC_Server_remove_from_bloom(elt, head->bloom);
                        }

                        // Remove from linked list
                        DL_DELETE(head->metadata, elt);
                        head->n_obj--;
                        /* printf("==PDC_SERVER: delete from DL!\n"); */
                    }
                    else {
                        // This is the last item under the current entry, remove the hash entry 
                        /* printf("==PDC_SERVER: delete from hash table!\n"); */
                        uint32_t hash_key = PDC_get_hash_by_name(elt->obj_name);
                        hash_table_remove(metadata_hash_table_g, &hash_key);

                        // Free this item and delete hash table entry
                        /* if(is_restart_g != 1) { */
                        /*     free(elt); */
                        /* } */
                    }
                    out->ret  = 1;
                    ret_value = SUCCEED;
                }
            } // DL_FOREACH
        }  // while 
    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        out->ret = -1;
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING 
    server_delete_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif
    

    // Decrement total metadata count
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_lock(&n_metadata_mutex_g);
#endif
        n_metadata_g-- ;
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif


done:
    /* printf("==PDC_SERVER[%d]: Finished delete by id request: obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->obj_id); */
    /* fflush(stdout); */
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_delete_metadata_by_id


/*
 * Delete metdata from hash table with the ID received from a client
 *
 * \param  in[IN]       Input structure received from client, conatins object ID
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t delete_metadata_from_hash_table(metadata_delete_in_t *in, metadata_delete_out_t *out)
{
    perr_t ret_value;
    uint32_t *hash_key;
    pdc_metadata_t *target;
    
    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER[%d]: Got delete request: hash=%d, obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->hash_value, in->obj_id); */
    /* fflush(stdout); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("==PDC_SERVER: Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;
    /* uint64_t obj_id = in->obj_id; */

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t metadata;
    strcpy(metadata.obj_name, in->obj_name);
    metadata.time_step = in->time_step;
    metadata.app_name[0] = 0;
    metadata.user_id = -1;
    metadata.obj_id = 0;
        
    /* printf("==PDC_SERVER: delete request name:%s ts=%d hash=%u\n", in->obj_name, in->time_step, in->hash_value); */

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("==PDC_SERVER: checking hash table with key=%d\n", *hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {

            /* printf("==PDC_SERVER: lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            target = find_identical_metadata(lookup_value, &metadata);
            if (target != NULL) {
                /* printf("==PDC_SERVER: Found delete target!\n"); */
                
                // Check if target is the only item in this linked list
                /* int curr_list_size; */
                /* DL_COUNT(lookup_value, elt, curr_list_size); */

                /* printf("==PDC_SERVER: still %d objects in current list\n", curr_list_size); */

                /* if (curr_list_size > 1) { */
                if (lookup_value->n_obj > 1) {
                    // Remove from bloom filter
                    if (lookup_value->bloom != NULL) {
                        PDC_Server_remove_from_bloom(target, lookup_value->bloom);
                    }

                    // Remove from linked list
                    DL_DELETE(lookup_value->metadata, target);
                    lookup_value->n_obj--;
                    /* printf("==PDC_SERVER: delete from DL!\n"); */
                }
                else {
                    // Remove from hash
                    /* printf("==PDC_SERVER: delete from hash table!\n"); */
                    hash_table_remove(metadata_hash_table_g, hash_key);

                    // Free this item and delete hash table entry
                    /* if(is_restart_g != 1) { */
                    /*     free(target); */
                    /* } */
                }
                out->ret  = 1;

            } // if (lookup_value != NULL) 
            else {
                // Object not found for deletion request
                printf("==PDC_SERVER: delete target not found!\n");
                ret_value = -1;
                out->ret  = -1;
            }
       
        } // if lookup_value != NULL
        else {
            printf("==PDC_SERVER: delete target not found!\n");
            ret_value = -1;
            out->ret = -1;
        }

    } // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        out->ret = -1;
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING 
    server_delete_time_g += ht_total_sec;
#endif
    
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif
    

    // Decrement total metadata count
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_lock(&n_metadata_mutex_g);
#endif
        n_metadata_g-- ;
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif

done:
    /* printf("==PDC_SERVER[%d]: Finished delete request: hash=%u, obj_id=%" PRIu64 "\n", pdc_server_rank_g, in->hash_value, in->obj_id); */
    /* fflush(stdout); */
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
        
    FUNC_LEAVE(ret_value);
} // end of delete_metadata_from_hash_table

/*
 * Insert the metdata received from client to the hash table
 *
 * \param  in[IN]       Input structure received from client, conatins metadata 
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t insert_metadata_to_hash_table(gen_obj_id_in_t *in, gen_obj_id_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *metadata;
    uint32_t *hash_key, i;
    

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER[%d]: Got object creation request with name: %s\tHash=%u\n", */
    /*         pdc_server_rank_g, in->data.obj_name, in->hash_value); */
    /* printf("Full name check: %s\n", &in->obj_name[507]); */

    /* printf("%s\t%u\n", in->data.obj_name, in->hash_value); */
    metadata = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
    if (metadata == NULL) {
        printf("Cannot allocate pdc_metadata_t!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(pdc_metadata_t);

    PDC_metadata_init(metadata);

    metadata->cont_id   = in->data.cont_id;
    metadata->user_id   = in->data.user_id;
    metadata->time_step = in->data.time_step;
    metadata->ndim      = in->data.ndim;
    metadata->dims[0]   = in->data.dims0;
    metadata->dims[1]   = in->data.dims1;
    metadata->dims[2]   = in->data.dims2;
    metadata->dims[3]   = in->data.dims3;
    for (i = metadata->ndim; i < DIM_MAX; i++) 
        metadata->dims[i] = 0;
    
       
    strcpy(metadata->obj_name,      in->data.obj_name);
    strcpy(metadata->app_name,      in->data.app_name);
    strcpy(metadata->tags,          in->data.tags);
    strcpy(metadata->data_location, in->data.data_location);
       
    // DEBUG
    int debug_flag = 0;
    /* PDC_print_metadata(metadata); */

    /* create_time              =; */
    /* last_modified_time       =; */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;

    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t *found_identical;

#ifdef ENABLE_MULTITHREAD 
    // Obtain lock for hash table
    int unlocked = 0;
    hg_thread_mutex_lock(&pdc_metadata_hash_table_mutex_g);
#endif

    if (debug_flag == 1) 
        printf("checking hash table with key=%d\n", *hash_key);

    if (metadata_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(metadata_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            
            if (debug_flag == 1) 
                printf("lookup_value not NULL!\n");
            // Check if there exist metadata identical to current one
            /* found_identical = NULL; */
            found_identical = find_identical_metadata(lookup_value, metadata);
            if ( found_identical != NULL) {
                printf("==PDC_SERVER[%d]: Found identical metadata with name %s!\n", 
                        pdc_server_rank_g, metadata->obj_name);

                if (debug_flag == 1) {
                    /* PDC_print_metadata(metadata); */
                    /* PDC_print_metadata(found_identical); */
                }
                out->obj_id = 0;
                free(metadata);
                goto done;
            }
            else {
                PDC_Server_hash_table_list_insert(lookup_value, metadata);
            }
        
        }
        else {
            // First entry for current hasy_key, init linked list, and insert to hash table
            if (debug_flag == 1) {
                printf("lookup_value is NULL! Init linked list\n");
            }
            fflush(stdout);

            pdc_hash_table_entry_head *entry = (pdc_hash_table_entry_head*)malloc(sizeof(pdc_hash_table_entry_head));
            entry->bloom    = NULL;
            entry->metadata = NULL;
            entry->n_obj    = 0;
            total_mem_usage_g += sizeof(pdc_hash_table_entry_head);

            PDC_Server_hash_table_list_init(entry, hash_key);
            PDC_Server_hash_table_list_insert(entry, metadata);
        }

    }
    else {
        printf("metadata_hash_table_g not initialized!\n");
        goto done;
    }

    // Generate object id (uint64_t)
    metadata->obj_id = PDC_Server_gen_obj_id();

#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
    unlocked = 1;
#endif


#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_lock(&n_metadata_mutex_g);
#endif
        n_metadata_g++ ;
#ifdef ENABLE_MULTITHREAD 
        hg_thread_mutex_unlock(&n_metadata_mutex_g);
#endif


    // Fill $out structure for returning the generated obj_id to client
    out->obj_id = metadata->obj_id;

    // Debug print metadata info
    /* PDC_print_metadata(metadata); */

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING 
    server_insert_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif
    

done:
#ifdef ENABLE_MULTITHREAD 
    if (unlocked == 0)
        hg_thread_mutex_unlock(&pdc_metadata_hash_table_mutex_g);
#endif
    /* printf("==PDC_SERVER[%d]: inserted name %s hash key %u to hash table\n", pdc_server_rank_g, in->data.obj_name, *hash_key); */
    /* fflush(stdout); */
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
    FUNC_LEAVE(ret_value);
} // end of insert_metadata_to_hash_table

/*
 * Print all existing metadata in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_print_all_metadata()
{
    perr_t ret_value = SUCCEED;
    HashTableIterator hash_table_iter;
    pdc_metadata_t *elt;
    pdc_hash_table_entry_head *head;
    HashTablePair pair;
    
    FUNC_ENTER(NULL);

    hash_table_iterate(metadata_hash_table_g, &hash_table_iter);
    while (hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
        head = pair.value;
        DL_FOREACH(head->metadata, elt) {
            PDC_print_metadata(elt);
        }
    }
    FUNC_LEAVE(ret_value);
}

/*
 * Print all existing container in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_print_all_containers()
{
    int i;
    perr_t ret_value = SUCCEED;
    HashTableIterator hash_table_iter;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    HashTablePair pair;
    
    FUNC_ENTER(NULL);

    hash_table_iterate(container_hash_table_g, &hash_table_iter);
    while (hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
        cont_entry = pair.value;
        printf("Container [%s]:", cont_entry->cont_name);
        for (i = 0; i < cont_entry->n_obj; i++) {
            if (cont_entry->obj_ids[i] != 0) {
                printf("%" PRIu64 ", ", cont_entry->obj_ids[i]);
            }
        }
        printf("\n");
    }

    FUNC_LEAVE(ret_value);
}


/*
 * Check for duplicates in the hash table
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_metadata_duplicate_check()
{
    perr_t ret_value = SUCCEED;
    HashTableIterator hash_table_iter;
    HashTablePair pair;
    int n_entry, count = 0;
    int all_maybe, all_total, all_entry;
    int has_dup_obj = 0;
    int all_dup_obj = 0;
    pdc_metadata_t *elt, *elt_next;
    pdc_hash_table_entry_head *head;
    
    FUNC_ENTER(NULL);

    n_entry = hash_table_num_entries(metadata_hash_table_g);

    #ifdef ENABLE_MPI
        MPI_Reduce(&n_bloom_maybe_g, &all_maybe, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&n_bloom_total_g, &all_total, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&n_entry,         &all_entry, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    #else
        all_maybe = n_bloom_maybe_g;
        all_total = n_bloom_total_g;
        all_entry = n_entry;
    #endif

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: Bloom filter says maybe %d times out of %d\n", all_maybe, all_total);
        printf("==PDC_SERVER: Metadata duplicate check with %d hash entries ", all_entry);
    }

    fflush(stdout);

    hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

    while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
        pair = hash_table_iter_next(&hash_table_iter);
        head = pair.value;
        /* DL_COUNT(head, elt, dl_count); */
        /* if (pdc_server_rank_g == 0) { */
        /*     printf("  Hash entry[%d], with %d items\n", count, dl_count); */
        /* } */
        DL_SORT(head->metadata, PDC_metadata_cmp); 
        // With sorted list, just compare each one with its next
        DL_FOREACH(head->metadata, elt) {
            elt_next = elt->next;
            if (elt_next != NULL) {
                if (PDC_metadata_cmp(elt, elt_next) == 0) {
                    /* PDC_print_metadata(elt); */
                    has_dup_obj = 1;
                    ret_value = FAIL;
                    goto done;
                }
            }
        }
        count++;
    }

    fflush(stdout);

done:
    #ifdef ENABLE_MPI
        MPI_Reduce(&has_dup_obj, &all_dup_obj, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    #else
        all_dup_obj = has_dup_obj;
    #endif
    if (pdc_server_rank_g == 0) {
        if (all_dup_obj > 0) {
            printf("  ...Found duplicates!\n");
        }
        else {
            printf("  ...No duplicates found!\n");
        }
    }
 
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

    /* if (pdc_server_rank_g == 0) { */
    /*     printf("==PDC_SERVER[%d]: Successfully established connection to %d other PDC servers\n", */
    /*             pdc_server_rank_g, pdc_server_size_g- 1); */
    /*     fflush(stdout); */
    /* } */

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
    uint32_t credential, cookie;
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
    sprintf(na_info_string, "%s://%s:%d", hg_transport, hostname, port);
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
    
    printf("# Credential is %u\n", credential);
    fflush(stdout);

    rc = drc_access(credential, 0, &credential_info);
    if (rc != DRC_SUCCESS) { /* failed to access credential */
        printf("drc_access() failed (%d, %s)", rc,
            drc_strerror(-rc));
        ret_value = FAIL; 
        goto done;
    }
    cookie = drc_get_first_cookie(credential_info);

    printf("# Cookie is %u\n", cookie);
    fflush(stdout);
    sprintf(pdc_auth_key, "%u", cookie);
    init_info.na_init_info.auth_key = strdup(pdc_auth_key);
#endif
//end of gni

    // Init server
//    *hg_class = HG_Init(na_info_string, NA_TRUE);
    init_info.na_init_info.progress_mode = NA_NO_BLOCK;
    init_info.auto_sm = HG_TRUE;
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
    hg_thread_mutex_init(&pdc_client_addr_mutex_g);
    hg_thread_mutex_init(&pdc_metadata_hash_table_mutex_g);
    hg_thread_mutex_init(&pdc_container_hash_table_mutex_g);
    hg_thread_mutex_init(&pdc_client_addr_metex_g);
    hg_thread_mutex_init(&pdc_time_mutex_g);
    hg_thread_mutex_init(&pdc_bloom_time_mutex_g);
    hg_thread_mutex_init(&n_metadata_mutex_g);
    hg_thread_mutex_init(&gen_obj_id_mutex_g);
    hg_thread_mutex_init(&data_read_list_mutex_g);
    hg_thread_mutex_init(&data_write_list_mutex_g);
    hg_thread_mutex_init(&pdc_server_task_mutex_g);
    hg_thread_mutex_init(&create_region_struct_mutex_g);
    hg_thread_mutex_init(&delete_buf_map_mutex_g);
    hg_thread_mutex_init(&remove_buf_map_mutex_g);
    hg_thread_mutex_init(&access_lock_list_mutex_g);
    hg_thread_mutex_init(&append_lock_mutex_g);
    hg_thread_mutex_init(&append_buf_map_mutex_g);
    hg_thread_mutex_init(&append_region_struct_mutex_g);
    hg_thread_mutex_init(&insert_hash_table_mutex_g);
    hg_thread_mutex_init(&append_lock_request_mutex_g);
    hg_thread_mutex_init(&remove_lock_request_mutex_g);
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
        sprintf(checkpoint_file, "%s%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);

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
            printf("==PDC_SERVER: total restart time = %.6f\n", all_restart_time);
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

    hg_thread_mutex_destroy(&pdc_time_mutex_g);
    hg_thread_mutex_destroy(&pdc_metadata_hash_table_mutex_g);
    hg_thread_mutex_destroy(&pdc_client_addr_mutex_g);
    hg_thread_mutex_destroy(&pdc_bloom_time_mutex_g);
    hg_thread_mutex_destroy(&n_metadata_mutex_g);
    hg_thread_mutex_destroy(&gen_obj_id_mutex_g);
    hg_thread_mutex_destroy(&data_read_list_mutex_g);
    hg_thread_mutex_destroy(&data_write_list_mutex_g);
    hg_thread_mutex_destroy(&create_region_struct_mutex_g);
    hg_thread_mutex_destroy(&delete_buf_map_mutex_g);
    hg_thread_mutex_destroy(&remove_buf_map_mutex_g);
    hg_thread_mutex_destroy(&access_lock_list_mutex_g);
    hg_thread_mutex_destroy(&append_lock_mutex_g);
    hg_thread_mutex_destroy(&append_buf_map_mutex_g);
    hg_thread_mutex_destroy(&append_region_struct_mutex_g);
    hg_thread_mutex_destroy(&insert_hash_table_mutex_g);
    hg_thread_mutex_destroy(&append_lock_request_mutex_g);
    hg_thread_mutex_destroy(&remove_lock_request_mutex_g);
    hg_thread_mutex_destroy(&update_remote_server_addr_mutex_g);
#endif

    if (pdc_server_rank_g == 0)
        PDC_Server_rm_config_file();


done:
    free(all_addr_strings_g);
    free(all_addr_strings_1d_g);
    FUNC_LEAVE(ret_value);
}

/*
 * Checkpoint in-memory metadata to persistant storage, each server writes to one file
 *
 * \param  filename[IN]     Checkpoint file name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_checkpoint(char *filename)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *elt;
    region_list_t  *region_elt;
    pdc_hash_table_entry_head *head;
    int n_entry, metadata_size = 0, region_count = 0, n_region, n_write_region = 0;
    uint32_t hash_key;
    HashTablePair pair;

    
    FUNC_ENTER(NULL);

    /* if (pdc_server_rank_g == 0) { */
        printf("\n\n==PDC_SERVER[%d]: Checkpoint file [%s]\n",pdc_server_rank_g, filename);
        fflush(stdout);
    /* } */

    FILE *file = fopen(filename, "w+");
    if (file==NULL) {
        printf("==PDC_SERVER: PDC_Server_checkpoint() - Checkpoint file open error"); 
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
                printf("==PDC_SERVER: PDC_Server_checkpoint() - ERROR with number of regions"); 
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
    /* if (pdc_server_rank_g == 0) { */
        printf("==PDC_SERVER: checkpointed %d objects, with %d regions \n", all_metadata_size, all_region_count);
    /* } */

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
        printf("==PDC_SERVER[%d]: %s -  Checkpoint file open FAILED!", pdc_server_rank_g, __func__); 
        ret_value = FAIL;
        goto done;
    }

    // init hash table
    PDC_Server_init_hash_table();
    fread(&n_entry, sizeof(int), 1, file);
    /* printf("%d entries\n", n_entry); */

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
            (metadata+i)->bloom                    = NULL;
            (metadata+i)->prev                     = NULL;
            (metadata+i)->next                     = NULL;

            fread(&n_region, sizeof(int), 1, file);
            if (n_region < 0) {
                printf("==PDC_SERVER[%d]: %s -  Checkpoint file region number ERROR!", pdc_server_rank_g, __func__); 
                ret_value = FAIL;
                goto done;
            }

            total_region += n_region; 
            region_list = (region_list_t*)calloc(sizeof(region_list_t), n_region);

            fread(region_list, sizeof(region_list_t), n_region, file);

            for (j = 0; j < n_region; j++) {
                (region_list+j)->buf           = NULL;
                (region_list+j)->is_data_ready = 0;
                (region_list+j)->shm_fd        = 0;
                (region_list+j)->meta          = (metadata+i);
                (region_list+j)->prev          = NULL;
                (region_list+j)->next          = NULL;
                memset((region_list+j)->shm_addr, 0, ADDR_MAX);

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
        printf("==PDC_SERVER: Server restarted from saved session, "
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
            /* hg_ret = HG_Trigger(hg_context, 1024/1* timeout *1/, 4096/1* max count *1/, &actual_count); */
            hg_ret = HG_Trigger(hg_context, 0/* timeout */, 1 /* max count */, &actual_count);
            /* printf("==%d: After Trigger\n", pdc_server_rank_g); */
            /* fflush(stdout); */
        } while ((hg_ret == HG_SUCCESS) && actual_count);

        /* Do not try to make progress anymore if we're done */
        if (hg_atomic_cas32(&close_server_g, 1, 1)) break;
        /* printf("==%d: Before Progress\n", pdc_server_rank_g); */
        /* fflush(stdout); */
        hg_ret = HG_Progress(hg_context, HG_MAX_IDLE_TIME);
        /* printf("==%d: After Progress\n", pdc_server_rank_g); */
        /* fflush(stdout); */
        /* hg_ret = HG_Progress(hg_context, 50000); */

    /* } while (hg_ret == HG_SUCCESS); */
    } while (hg_ret == HG_SUCCESS || hg_ret == HG_TIMEOUT);

    if (hg_ret == HG_SUCCESS) 
        ret_value = SUCCEED;
    else
        ret_value = FAIL;

    FUNC_LEAVE(ret_value);
}

/* For 1D boxes (intervals) we have: */
/* box1 = (xmin1, xmax1) */
/* box2 = (xmin2, xmax2) */
/* overlapping1D(box1,box2) = xmax1 >= xmin2 and xmax2 >= xmin1 */

/* For 2D boxes (rectangles) we have: */
/* box1 = (x:(xmin1,xmax1),y:(ymin1,ymax1)) */
/* box2 = (x:(xmin2,xmax2),y:(ymin2,ymax2)) */
/* overlapping2D(box1,box2) = overlapping1D(box1.x, box2.x) and */ 
/*                            overlapping1D(box1.y, box2.y) */

/* For 3D boxes we have: */
/* box1 = (x:(xmin1,xmax1),y:(ymin1,ymax1),z:(zmin1,zmax1)) */
/* box2 = (x:(xmin2,xmax2),y:(ymin2,ymax2),z:(zmin2,zmax2)) */
/* overlapping3D(box1,box2) = overlapping1D(box1.x, box2.x) and */ 
/*                            overlapping1D(box1.y, box2.y) and */
/*                            overlapping1D(box1.z, box2.z) */
 
/*
 * Check if two 1D segments overlaps
 *
 * \param  xmin1[IN]        Start offset of first segment
 * \param  xmax1[IN]        End offset of first segment
 * \param  xmin2[IN]        Start offset of second segment
 * \param  xmax2[IN]        End offset of second segment
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_overlap_1D(uint64_t xmin1, uint64_t xmax1, uint64_t xmin2, uint64_t xmax2)
{
    int ret_value = -1;
    
    if (xmax1 >= xmin2 && xmax2 >= xmin1) {
        ret_value = 1;
    }

    return ret_value;
}

/*
 * Check if two 2D box overlaps
 *
 * \param  xmin1[IN]        Start offset (x-axis) of first  box
 * \param  xmax1[IN]        End   offset (x-axis) of first  box
 * \param  ymin1[IN]        Start offset (y-axis) of first  box
 * \param  ymax1[IN]        End   offset (y-axis) of first  box
 * \param  xmin2[IN]        Start offset (x-axis) of second box
 * \param  xmax2[IN]        End   offset (x-axis) of second box
 * \param  ymin2[IN]        Start offset (y-axis) of second box
 * \param  ymax2[IN]        End   offset (y-axis) of second box
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_overlap_2D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, 
                         uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2)
{
    int ret_value = -1;
    
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2 ) == 1 &&                              
        is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1) {
        ret_value = 1;
    }

    return ret_value;
}

/*
 * Check if two 3D box overlaps
 *
 * \param  xmin1[IN]        Start offset (x-axis) of first  box
 * \param  xmax1[IN]        End   offset (x-axis) of first  box
 * \param  ymin1[IN]        Start offset (y-axis) of first  box
 * \param  ymax1[IN]        End   offset (y-axis) of first  box
 * \param  zmin2[IN]        Start offset (z-axis) of first  box
 * \param  zmax2[IN]        End   offset (z-axis) of first  box
 * \param  xmin2[IN]        Start offset (x-axis) of second box
 * \param  xmax2[IN]        End   offset (x-axis) of second box
 * \param  ymin2[IN]        Start offset (y-axis) of second box
 * \param  ymax2[IN]        End   offset (y-axis) of second box
 * \param  zmin2[IN]        Start offset (z-axis) of second box
 * \param  zmax2[IN]        End   offset (z-axis) of second box
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_overlap_3D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t zmin1, uint64_t zmax1,
                         uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2, uint64_t zmin2, uint64_t zmax2)
{
    int ret_value = -1;
    
    /* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { */
    if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && 
        is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1 && 
        is_overlap_1D(zmin1, zmax1, zmin2, zmax2) == 1 ) 
    {
        ret_value = 1;
    }

    return ret_value;
}

/* static int is_overlap_4D(uint64_t xmin1, uint64_t xmax1, uint64_t ymin1, uint64_t ymax1, uint64_t zmin1, uint64_t zmax1, */
/*                          uint64_t mmin1, uint64_t mmax1, */
/*                          uint64_t xmin2, uint64_t xmax2, uint64_t ymin2, uint64_t ymax2, uint64_t zmin2, uint64_t zmax2, */
/*                          uint64_t mmin2, uint64_t mmax2 ) */
/* { */
/*     int ret_value = -1; */
    
/*     /1* if (is_overlap_1D(box1.x, box2.x) == 1 && is_overlap_1D(box1.y, box2.y) == 1) { *1/ */
/*     if (is_overlap_1D(xmin1, xmax1, xmin2, xmax2) == 1 && */ 
/*         is_overlap_1D(ymin1, ymax1, ymin2, ymax2) == 1 && */ 
/*         is_overlap_1D(zmin1, zmax1, zmin2, zmax2) == 1 && */ 
/*         is_overlap_1D(mmin1, mmax1, mmin2, mmax2) == 1 ) */ 
/*     { */
/*         ret_value = 1; */
/*     } */

/*     return ret_value; */
/* } */

/*
 * Check if two regions overlap
 *
 * \param  a[IN]     Pointer to first region
 * \param  b[IN]     Pointer to second region
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_contiguous_region_overlap(region_list_t *a, region_list_t *b)
{
    int ret_value = 1;
    
    if (a == NULL || b == NULL) {
        printf("==PDC_SERVER: is_region_identical() - passed NULL value!\n");
        ret_value = -1;
        goto done;
    }

    /* printf("==PDC_SERVER: is_contiguous_region_overlap adim=%d, bdim=%d\n", a->ndim, b->ndim); */
    if (a->ndim != b->ndim || a->ndim <= 0 || b->ndim <= 0) {
        ret_value = -1;
        goto done;
    }

    uint64_t xmin1 = 0, xmin2 = 0, xmax1 = 0, xmax2 = 0;
    uint64_t ymin1 = 0, ymin2 = 0, ymax1 = 0, ymax2 = 0;
    uint64_t zmin1 = 0, zmin2 = 0, zmax1 = 0, zmax2 = 0;
    /* uint64_t mmin1 = 0, mmin2 = 0, mmax1 = 0, mmax2 = 0; */

    if (a->ndim >= 1) {
        xmin1 = a->start[0];
        xmax1 = a->start[0] + a->count[0] - 1;
        xmin2 = b->start[0];
        xmax2 = b->start[0] + b->count[0] - 1;
        /* printf("xmin1, xmax1, xmin2, xmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", xmin1, xmax1, xmin2, xmax2); */
    }
    if (a->ndim >= 2) {
        ymin1 = a->start[1];
        ymax1 = a->start[1] + a->count[1] - 1;
        ymin2 = b->start[1];
        ymax2 = b->start[1] + b->count[1] - 1;
        /* printf("ymin1, ymax1, ymin2, ymax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", ymin1, ymax1, ymin2, ymax2); */
    }
    if (a->ndim >= 3) {
        zmin1 = a->start[2];
        zmax1 = a->start[2] + a->count[2] - 1;
        zmin2 = b->start[2];
        zmax2 = b->start[2] + b->count[2] - 1;
        /* printf("zmin1, zmax1, zmin2, zmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", zmin1, zmax1, zmin2, zmax2); */
    }
    /* if (a->ndim >= 4) { */
    /*     mmin1 = a->start[3]; */
    /*     mmax1 = a->start[3] + a->count[3] - 1; */
    /*     mmin2 = b->start[3]; */
    /*     mmax2 = b->start[3] + b->count[3] - 1; */
    /* } */
 
    if (a->ndim == 1) {
        ret_value = is_overlap_1D(xmin1, xmax1, xmin2, xmax2);
    }
    else if (a->ndim == 2) {
        ret_value = is_overlap_2D(xmin1, xmax1, ymin1, ymax1, xmin2, xmax2, ymin2, ymax2);
    }
    else if (a->ndim == 3) {
        ret_value = is_overlap_3D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2);
    }
    /* else if (a->ndim == 4) { */
    /*     ret_value = is_overlap_4D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, mmin1, mmax1, xmin2, xmax2, ymin2, ymax2, zmin2, zmax2, mmin2, mmax2); */
    /* } */

done:
    /* printf("is overlap: %d\n", ret_value); */
    FUNC_LEAVE(ret_value);
}

/*
 * Check if two regions overlap
 *
 * \param  ndim[IN]        Dimension of the two region
 * \param  a_start[IN]     Start offsets of the the first region
 * \param  a_count[IN]     Size of the the first region
 * \param  b_start[IN]     Start offsets of the the second region
 * \param  b_count[IN]     Size of the the second region
 *
 * \return 1 if they overlap/-1 otherwise
 */
static int is_contiguous_start_count_overlap(uint32_t ndim, uint64_t *a_start, uint64_t *a_count, uint64_t *b_start, uint64_t *b_count)
{
    int ret_value = 1;
    
    if (ndim > DIM_MAX || NULL == a_start || NULL == a_count ||NULL == b_start ||NULL == b_count) {
        printf("is_contiguous_start_count_overlap: invalid input !\n");
        ret_value = -1;
        goto done;
    }

    uint64_t xmin1 = 0, xmin2 = 0, xmax1 = 0, xmax2 = 0;
    uint64_t ymin1 = 0, ymin2 = 0, ymax1 = 0, ymax2 = 0;
    uint64_t zmin1 = 0, zmin2 = 0, zmax1 = 0, zmax2 = 0;
    /* uint64_t mmin1 = 0, mmin2 = 0, mmax1 = 0, mmax2 = 0; */

    if (ndim >= 1) {
        xmin1 = a_start[0];
        xmax1 = a_start[0] + a_count[0] - 1;
        xmin2 = b_start[0];
        xmax2 = b_start[0] + b_count[0] - 1;
        /* printf("xmin1, xmax1, xmin2, xmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", xmin1, xmax1, xmin2, xmax2); */
    }
    if (ndim >= 2) {
        ymin1 = a_start[1];
        ymax1 = a_start[1] + a_count[1] - 1;
        ymin2 = b_start[1];
        ymax2 = b_start[1] + b_count[1] - 1;
        /* printf("ymin1, ymax1, ymin2, ymax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", ymin1, ymax1, ymin2, ymax2); */
    }
    if (ndim >= 3) {
        zmin1 = a_start[2];
        zmax1 = a_start[2] + a_count[2] - 1;
        zmin2 = b_start[2];
        zmax2 = b_start[2] + b_count[2] - 1;
        /* printf("zmin1, zmax1, zmin2, zmax2: %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n", zmin1, zmax1, zmin2, zmax2); */
    }
    /* if (ndim >= 4) { */
    /*     mmin1 = a_start[3]; */
    /*     mmax1 = a_start[3] + a_count[3] - 1; */
    /*     mmin2 = b_start[3]; */
    /*     mmax2 = b_start[3] + b_count[3] - 1; */
    /* } */
 
    if (ndim == 1) 
        ret_value = is_overlap_1D(xmin1, xmax1, xmin2, xmax2);
    else if (ndim == 2) 
        ret_value = is_overlap_2D(xmin1, xmax1, ymin1, ymax1, 
                                  xmin2, xmax2, ymin2, ymax2);
    else if (ndim == 3) 
        ret_value = is_overlap_3D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, 
                                  xmin2, xmax2, ymin2, ymax2, zmin2, zmax2);
    /* else if (ndim == 4) */ 
    /*     ret_value = is_overlap_4D(xmin1, xmax1, ymin1, ymax1, zmin1, zmax1, mmin1, mmax1, */ 
    /*                               xmin2, xmax2, ymin2, ymax2, zmin2, zmax2, mmin2, mmax2); */

done:
    FUNC_LEAVE(ret_value);
}
 
/*
 * Get the overlapping region's information of two regions
 *
 * \param  ndim[IN]             Dimension of the two region
 * \param  start_a[IN]           Start offsets of the the first region
 * \param  count_a[IN]           Sizes of the the first region
 * \param  start_b[IN]           Start offsets of the the second region
 * \param  count_b[IN]           Sizes of the the second region
 * \param  overlap_start[IN]    Start offsets of the the overlapping region
 * \param  overlap_size[IN]     Sizes of the the overlapping region
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t get_overlap_start_count(uint32_t ndim, uint64_t *start_a, uint64_t *count_a, 
                                                     uint64_t *start_b, uint64_t *count_b, 
                                       uint64_t *overlap_start, uint64_t *overlap_count)
{
    perr_t ret_value = SUCCEED;
    uint64_t i;

    if (NULL == start_a || NULL == count_a || NULL == start_b || NULL == count_b || 
            NULL == overlap_start || NULL == overlap_count) {

        printf("get_overlap NULL input!\n");
        ret_value = FAIL;
        return ret_value;
    }
    
    // Check if they are truly overlapping regions
    if (is_contiguous_start_count_overlap(ndim, start_a, count_a, start_b, count_b) != 1) {
        printf("==PDC_SERVER[%d]: %s: non-overlap regions!\n", pdc_server_rank_g, __func__);
        for (i = 0; i < ndim; i++) {
            printf("\t\tdim%" PRIu64 " - start_a: %" PRIu64 " count_a: %" PRIu64 ", "
                   "\t\tstart_b:%" PRIu64 " count_b:%" PRIu64 "\n", 
                    i, start_a[i], count_a[i], start_b[i], count_b[i]);
        }
        ret_value = FAIL;
        goto done;
    }

    for (i = 0; i < ndim; i++) {
        overlap_start[i] = PDC_MAX(start_a[i], start_b[i]);
        /* end = max(xmax2, xmax1); */
        overlap_count[i] = PDC_MIN(start_a[i]+count_a[i], start_b[i]+count_b[i]) - overlap_start[i];
    }

done:
    if (ret_value == FAIL) {
        for (i = 0; i < ndim; i++) {
            overlap_start[i] = 0;
            overlap_count[i] = 0;
        }
    }
    return ret_value;
}

/*
 * Check if two regions are the same
 *
 * \param  a[IN]     Pointer to the first region
 * \param  b[IN]     Pointer to the second region
 *
 * \return 1 if they are the same/-1 otherwise
 */
static int is_region_identical(region_list_t *a, region_list_t *b)
{
    int ret_value = -1;
    uint32_t i;
    
    FUNC_ENTER(NULL);

    if (a == NULL || b == NULL) {
        printf("==PDC_SERVER: is_region_identical() - passed NULL value!\n");
        ret_value = -1;
        goto done;
    }

    if (a->ndim != b->ndim) {
        ret_value = -1;
        goto done;
    }

    for (i = 0; i < a->ndim; i++) {
        if (a->start[i] != b->start[i] || a->count[i] != b->count[i] ) {
        /* if (a->start[i] != b->start[i] || a->count[i] != b->count[i] || a->stride[i] != b->stride[i] ) { */
            ret_value = -1;
            goto done;
        }
    }

    ret_value = 1;

done:
    FUNC_LEAVE(ret_value);
}

/*
perr_t PDC_Server_get_reg_lock_status_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    pdc_metadata_t *meta = NULL;
    server_lookup_args_t *lookup_args;
    hg_handle_t handle;
    get_reg_lock_status_out_t output;
}
*/

perr_t PDC_Server_local_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *res_meta;
    region_list_t *elt, *request_region;

    FUNC_ENTER(NULL);

    // Check if the region lock info is on current server
    *lock_status = 0;
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    pdc_region_transfer_t_to_list_t(&(mapped_region->remote_region), request_region);
    res_meta = find_metadata_by_id(mapped_region->remote_obj_id);
    if (res_meta == NULL || res_meta->region_lock_head == NULL) {
        printf("==PDC_SERVER[%d]: PDC_Server_region_lock_status - metadata/region_lock is NULL!\n",
                pdc_server_rank_g);
        fflush(stdout);
        ret_value = FAIL;
        goto done;
    }
    /*
    printf("requested region: \n");
    printf("offset is %lld, %lld\n", (request_region->start)[0], (request_region->start)[1]);
    printf("size is %lld, %lld\n", (request_region->count)[0], (request_region->count)[1]);
    */
    // iterate the target metadata's region_lock_head (linked list) to search for queried region
    DL_FOREACH(res_meta->region_lock_head, elt) {
        /*
        printf("region in lock list: \n");
        printf("offset is %lld, %lld\n", (elt->start)[0], (elt->start)[1]);
        printf("size is %lld, %lld\n", (elt->count)[0], (elt->count)[1]);
        */
        if (is_region_identical(request_region, elt) == 1) {
            *lock_status = 1;
            elt->reg_dirty = 1;
            elt->bulk_handle = mapped_region->remote_bulk_handle;
            elt->addr = mapped_region->remote_addr;
            elt->from_obj_id = mapped_region->from_obj_id;
            elt->obj_id = mapped_region->remote_obj_id;
            elt->reg_id = mapped_region->remote_reg_id;
            elt->client_id = mapped_region->remote_client_id;
        }
    }
    free(request_region);

done:
    FUNC_LEAVE(ret_value);
}
//perr_t PDC_Server_region_lock_status(pdcid_t obj_id, region_info_transfer_t *region, int *lock_status)
perr_t PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status)
{
    perr_t ret_value = SUCCEED;
    region_list_t *request_region;
    uint32_t server_id = 0;

    *lock_status = 0;
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    pdc_region_transfer_t_to_list_t(&(mapped_region->remote_region), request_region);

    server_id = PDC_get_server_by_obj_id(mapped_region->remote_obj_id, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        PDC_Server_local_region_lock_status(mapped_region, lock_status);
    }
    else {
        printf("lock is located in a different server, work not finished yet\n");
        fflush(stdout);
    }
/*
    else {
        if (pdc_remote_server_info_g[server_id].addr_valid != 1) {
            if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
                printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                        pdc_server_rank_g, server_id);
                ret_value = FAIL;
                goto done;
            }
        }

        HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_reg_lock_register_id_g,
                &get_reg_lock_handle);

        in.remote_obj_id = mapped_region->remote_obj_id;
        in.remote_reg_id = mapped_region->remote_reg_id;
        in.remote_client_id = mapped_region->remote_client_id;
        size_t ndim = mapped_region->remote_ndim;
        if (ndim >= 4 || ndim <=0) {
            printf("Dimension %lu is not supported\n", ndim);
            ret_value = FAIL;
            goto done;
        }
        if (ndim >=1) {
            in.remote_region.start_0  = (mapped_region->remote_region).start_0;
            in.remote_region.count_0  = (mapped_region->remote_region).count_0;
        }
        if (ndim >=2) {
            in.remote_region.start_1  = (mapped_region->remote_region).start_1;
            in.remote_region.count_1  = (mapped_region->remote_region).count_1;
        }
        if (ndim >=3) {
            in.remote_region.start_2  = (mapped_region->remote_region).start_2;
            in.remote_region.count_2  = (mapped_region->remote_region).count_2;
        }
        in.remote_bulk_handle = mapped_region->remote_bulk_handle;
        in.remote_addr = mapped_region->remote_addr;
        in.from_obj_id = mapped_region->from_obj_id;
        hg_ret = HG_Forward(get_reg_lock_handle, PDC_Server_get_reg_lock_status_cb, &lookup_args, &in);

        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "PDC_Server_get_metadata_by_id(): Could not start HG_Forward()\n");
            return FAIL;
        }

        // Wait for response from server
        work_todo_g += 1;
        PDC_Server_check_response(&hg_context_g, &work_todo_g);

        // Retrieved metadata is stored in lookup_args
        *lock_status = lookup_args.lock;

        HG_Destroy(get_reg_lock_handle);
    }
*/
    FUNC_LEAVE(ret_value);
}

/*
static hg_return_t server_send_region_lock_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    region_lock_out_t output;
    struct transfer_lock_args *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_metadata_args *) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_region_lock_rpc_cb - error with HG_Get_output\n",
                pdc_server_rank_g);
        tranx_args->ret = -1;
        goto done;
    }

    tranx_args->ret = output.ret;

done:
    HG_Respond(tranx_args->handle, NULL, NULL, &output);
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &output);
    HG_Destroy(handle);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}
*/

/*
 * Lock a reigon.
 *
 * \param  in[IN]       Lock region information received from the client
 * \param  out[OUT]     Output stucture to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Data_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out, hg_handle_t *handle)
{
    perr_t ret_value = SUCCEED;
    uint64_t target_obj_id;
    int ndim;
//    int lock_op;
    region_list_t *request_region;
    data_server_region_t *new_obj_reg;
    region_list_t *elt1, *tmp;
    region_buf_map_t *eltt;
    int error = 0;
    int found_lock = 0;

    FUNC_ENTER(NULL);
    
    /* printf("==PDC_SERVER: received lock request,                                \ */
    /*         obj_id=%" PRIu64 ", op=%d, ndim=%d, start=%" PRIu64 " count=%" PRIu64 " stride=%d\n", */ 
    /*         in->obj_id, in->lock_op, in->region.ndim, */ 
    /*         in->region.start_0, in->region.count_0, in->region.stride_0); */

    target_obj_id = in->obj_id;
    ndim = in->region.ndim;
//    lock_op = in->lock_op;

    // Convert transferred lock region to structure
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    PDC_init_region_list(request_region);
    request_region->ndim = ndim;

    if (ndim >=1) {
        request_region->start[0]  = in->region.start_0;
        request_region->count[0]  = in->region.count_0;
        /* request_region->stride[0] = in->region.stride_0; */
    }
    if (ndim >=2) {
        request_region->start[1]  = in->region.start_1;
        request_region->count[1]  = in->region.count_1;
        /* request_region->stride[1] = in->region.stride_1; */
    }
    if (ndim >=3) {
        request_region->start[2]  = in->region.start_2;
        request_region->count[2]  = in->region.count_2;
        /* request_region->stride[2] = in->region.stride_2; */
    }
    if (ndim >=4) {
        request_region->start[3]  = in->region.start_3;
        request_region->count[3]  = in->region.count_3;
        /* request_region->stride[3] = in->region.stride_3; */
    }
    
/*
printf("enter PDC_Data_Server_region_lock()\n");
printf("request_region->start[0] = %lld\n", request_region->start[0]);
printf("request_region->count[0] = %lld\n", request_region->count[0]);
fflush(stdout);
*/
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&create_region_struct_mutex_g);
#endif
    new_obj_reg = PDC_Server_get_obj_region(in->obj_id);
    if(new_obj_reg == NULL) {
        new_obj_reg = (data_server_region_t *)malloc(sizeof(struct data_server_region_t));
        if(new_obj_reg == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "PDC_SERVER: PDC_Server_region_lock() allocates new object failed");
        }
        new_obj_reg->obj_id = in->obj_id;
        new_obj_reg->region_lock_head = NULL;
        new_obj_reg->region_buf_map_head = NULL;
        new_obj_reg->region_lock_request_head = NULL;
//        new_obj_reg->region_storage_head = NULL;
        DL_APPEND(dataserver_region_g, new_obj_reg);
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&create_region_struct_mutex_g);
#endif

    // Go through all existing locks to check for region lock
    DL_FOREACH(new_obj_reg->region_lock_head, elt1) {
        if (PDC_is_same_region_list(elt1, request_region) == 1) {
            found_lock = 1;
            if(in->lock_mode == BLOCK) {
                ret_value = FAIL;
//printf("region is currently lock\n");
//fflush(stdout);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&append_lock_request_mutex_g);
#endif
                request_region->lock_handle = *handle;
                DL_APPEND(new_obj_reg->region_lock_request_head, request_region);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&append_lock_request_mutex_g);
#endif
            }
            else
                error = 1; 
        }
    }

    if(found_lock == 0) {
    // check if the lock region is used in buf map function 
    tmp = (region_list_t *)malloc(sizeof(region_list_t));
    DL_FOREACH(new_obj_reg->region_buf_map_head, eltt) {
        pdc_region_transfer_t_to_list_t(&(eltt->remote_region_unit), tmp);
        if(PDC_is_same_region_list(tmp, request_region) == 1) {
            request_region->reg_dirty = 1;
            hg_atomic_incr32(&(request_region->buf_map_refcount));
        }
    }
    free(tmp);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&append_lock_mutex_g);
#endif
    // No overlaps found
    DL_APPEND(new_obj_reg->region_lock_head, request_region);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&append_lock_mutex_g);
#endif
    }

    out->ret = 1;

done:
    fflush(stdout);
    if(error == 1) {
        out->ret = 0;
//        HG_Respond(*handle, NULL, NULL, out);
//        HG_Free_input(*handle, in);
//        HG_Destroy(*handle);
    }

    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_release_lock_request(uint64_t obj_id, struct PDC_region_info *region) 
{
    perr_t ret_value = SUCCEED;
    region_list_t *request_region;
    region_list_t *elt, *tmp;
    data_server_region_t *new_obj_reg;
    region_lock_out_t out;

    FUNC_ENTER(NULL);

    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    PDC_init_region_list(request_region);
    pdc_region_info_to_list_t(region, request_region);

    new_obj_reg = PDC_Server_get_obj_region(obj_id);
    if(new_obj_reg == NULL) {
        PGOTO_ERROR(FAIL, "===PDC Server: cannot locate data_server_region_t strcut for object ID");
    }
    DL_FOREACH_SAFE(new_obj_reg->region_lock_request_head, elt, tmp) {
        if (is_region_identical(request_region, elt) == 1) {
            out.ret = 1;
            HG_Respond(elt->lock_handle, NULL, NULL, &out);
            HG_Destroy(elt->lock_handle);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&remove_lock_request_mutex_g);
#endif
            DL_DELETE(new_obj_reg->region_lock_request_head, elt);
            free(elt); 
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&remove_lock_request_mutex_g);
#endif
        }
    }
    free(request_region);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Lock a reigon.
 *
 * \param  in[IN]       Lock region information received from the client
 * \param  out[OUT]     Output stucture to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Meta_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out)
{
    perr_t ret_value = SUCCEED;
    uint64_t target_obj_id;
    int ndim;
//    int lock_op;
    region_list_t *request_region;
    pdc_metadata_t *target_obj;
    region_list_t *elt, *tmp;
    region_buf_map_t *eltt;
 
    FUNC_ENTER(NULL);
    
    /* printf("==PDC_SERVER: received lock request,                                \ */
    /*         obj_id=%" PRIu64 ", op=%d, ndim=%d, start=%" PRIu64 " count=%" PRIu64 " stride=%d\n", */ 
    /*         in->obj_id, in->lock_op, in->region.ndim, */ 
    /*         in->region.start_0, in->region.count_0, in->region.stride_0); */

    target_obj_id = in->obj_id;
    ndim = in->region.ndim;
//    lock_op = in->lock_op;

    // Convert transferred lock region to structure
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    PDC_init_region_list(request_region);
    request_region->ndim = ndim;

    if (ndim >=1) {
        request_region->start[0]  = in->region.start_0;
        request_region->count[0]  = in->region.count_0;
        /* request_region->stride[0] = in->region.stride_0; */
    }
    if (ndim >=2) {
        request_region->start[1]  = in->region.start_1;
        request_region->count[1]  = in->region.count_1;
        /* request_region->stride[1] = in->region.stride_1; */
    }
    if (ndim >=3) {
        request_region->start[2]  = in->region.start_2;
        request_region->count[2]  = in->region.count_2;
        /* request_region->stride[2] = in->region.stride_2; */
    }
    if (ndim >=4) {
        request_region->start[3]  = in->region.start_3;
        request_region->count[3]  = in->region.count_3;
        /* request_region->stride[3] = in->region.stride_3; */
    }
    

    // Locate target metadata structure
    target_obj = find_metadata_by_id(target_obj_id);
    if (target_obj == NULL) {
        out->ret = -1;
        PGOTO_ERROR(FAIL, "==PDC_SERVER: PDC_Server_region_lock - requested object does not exist\n");
    }

    request_region->meta = target_obj;

    /* printf("==PDC_SERVER: obtaining lock ... "); */
    // Go through all existing locks to check for overlapping
    // Note: currently only assumes contiguous region
    DL_FOREACH(target_obj->region_lock_head, elt) {
        if (is_contiguous_region_overlap(elt, request_region) == 1) {
            /* printf("rejected! (found overlapping regions)\n"); */
            out->ret = -1;
            goto done;
        }
    }

    // check if the lock region is used in buf map function 
    tmp = (region_list_t *)malloc(sizeof(region_list_t));
    DL_FOREACH(target_obj->region_buf_map_head, eltt) {
        pdc_region_transfer_t_to_list_t(&(eltt->remote_region_unit), tmp);
        if(PDC_is_same_region_list(tmp, request_region) == 1) {
            request_region->reg_dirty = 1;
            hg_atomic_incr32(&(request_region->buf_map_refcount));
        }
    }
    free(tmp);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&append_lock_mutex_g);
#endif
    // No overlaps found
    DL_APPEND(target_obj->region_lock_head, request_region);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&append_lock_mutex_g);
#endif

    out->ret = 1;
    /* printf("granted\n"); */
   
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Meta_Server_region_release(region_lock_in_t *in, region_lock_out_t *out)
{
    perr_t ret_value = SUCCEED;
    uint64_t target_obj_id;
    int ndim;
//    int lock_op;
    pdc_metadata_t *target_obj;
    region_list_t *elt, *tmp;
    region_list_t request_region;
    int found = 0;
 
    FUNC_ENTER(NULL);
    
    /* printf("==PDC_SERVER: received lock request,                                \ */
    /*         obj_id=%" PRIu64 ", op=%d, ndim=%d, start=%" PRIu64 " count=%" PRIu64 " stride=%d\n", */ 
    /*         in->obj_id, in->lock_op, in->region.ndim, */ 
    /*         in->region.start_0, in->region.count_0, in->region.stride_0); */

    target_obj_id = in->obj_id;
    ndim = in->region.ndim;
//    lock_op = in->lock_op;

    // Convert transferred lock region to structure
    PDC_init_region_list(&request_region);
    request_region.ndim = ndim;

    if (ndim >=1) {
        request_region.start[0]  = in->region.start_0;
        request_region.count[0]  = in->region.count_0;
        /* request_region->stride[0] = in->region.stride_0; */
    }
    if (ndim >=2) {
        request_region.start[1]  = in->region.start_1;
        request_region.count[1]  = in->region.count_1;
        /* request_region->stride[1] = in->region.stride_1; */
    }
    if (ndim >=3) {
        request_region.start[2]  = in->region.start_2;
        request_region.count[2]  = in->region.count_2;
        /* request_region->stride[2] = in->region.stride_2; */
    }
    if (ndim >=4) {
        request_region.start[3]  = in->region.start_3;
        request_region.count[3]  = in->region.count_3;
        /* request_region->stride[3] = in->region.stride_3; */
    }
    

    // Locate target metadata structure
    target_obj = find_metadata_by_id(target_obj_id);
    if (target_obj == NULL) {
        out->ret = -1;
        PGOTO_ERROR(FAIL, "==PDC_SERVER: PDC_Meta_Server_region_release() - requested object does not exist\n");
    }

    request_region.meta = target_obj;

    /* printf("==PDC_SERVER: releasing lock ... "); */
    // Find the lock region in the list and remove it
    DL_FOREACH_SAFE(target_obj->region_lock_head, elt, tmp) {
        if (is_region_identical(&request_region, elt) == 1) {
            // Found the requested region lock, remove from the linked list
            found = 1;
            DL_DELETE(target_obj->region_lock_head, elt);
            free(elt);
            out->ret = 1;
            /* printf("released!\n"); */
            goto done;
        }
    }

    // Request release lock region not found
    if (found == 0) {
        printf("==PDC_SERVER[%d]: PDC_Meta_Server_region_release() - requested release region/object does not exist\n", pdc_server_rank_g);
    }

    out->ret = 1;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
static hg_return_t server_send_region_release_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    region_lock_out_t output;
    struct transfer_unlock_args *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_metadata_args *) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_region_release_rpc_cb - error with HG_Get_output\n",
                pdc_server_rank_g);
        tranx_args->ret = -1;
        goto done;
    }

    tranx_args->ret = output.ret;

done:
    HG_Respond(tranx_args->handle, NULL, NULL, &output);
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &output);
    HG_Destroy(handle);
    free(tranx_args->bulk_args);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}
*/

perr_t PDC_Data_Server_region_release(struct buf_map_release_bulk_args *bulk_args, region_lock_out_t *out)
{
    perr_t ret_value = SUCCEED;
    uint64_t target_obj_id;
    int ndim;
//    int lock_op;
    region_list_t *tmp1, *tmp2;
    region_list_t request_region;
    int found = 0;
    data_server_region_t *obj_reg = NULL;
    hg_handle_t *handle;
    region_lock_in_t *in;
     
    FUNC_ENTER(NULL);
    
    /* printf("==PDC_SERVER: received lock request,                                \ */
    /*         obj_id=%" PRIu64 ", op=%d, ndim=%d, start=%" PRIu64 " count=%" PRIu64 " stride=%d\n", */ 
    /*         in->obj_id, in->lock_op, in->region.ndim, */ 
    /*         in->region.start_0, in->region.count_0, in->region.stride_0); */

    handle = &(bulk_args->handle);
    in = &(bulk_args->in);

    target_obj_id = in->obj_id;
    ndim = in->region.ndim;
//    lock_op = in->lock_op;

    // Convert transferred lock region to structure
    PDC_init_region_list(&request_region);
    request_region.ndim = ndim;

    if (ndim >=1) {
        request_region.start[0]  = in->region.start_0;
        request_region.count[0]  = in->region.count_0;
        /* request_region->stride[0] = in->region.stride_0; */
    }
    if (ndim >=2) {
        request_region.start[1]  = in->region.start_1;
        request_region.count[1]  = in->region.count_1;
        /* request_region->stride[1] = in->region.stride_1; */
    }
    if (ndim >=3) {
        request_region.start[2]  = in->region.start_2;
        request_region.count[2]  = in->region.count_2;
        /* request_region->stride[2] = in->region.stride_2; */
    }
    if (ndim >=4) {
        request_region.start[3]  = in->region.start_3;
        request_region.count[3]  = in->region.count_3;
        /* request_region->stride[3] = in->region.stride_3; */
    }

    obj_reg = PDC_Server_get_obj_region(in->obj_id);
    /* printf("==PDC_SERVER: releasing lock ... "); */
    // Find the lock region in the list and remove it
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&access_lock_list_mutex_g);
#endif
    DL_FOREACH_SAFE(obj_reg->region_lock_head, tmp1, tmp2) {
        if (is_region_identical(&request_region, tmp1) == 1) {
            // Found the requested region lock, remove from the linked list
            found = 1;
            DL_DELETE(obj_reg->region_lock_head, tmp1);
            free(tmp1);
//            out->ret = 1;
            /* printf("released!\n"); */
//            goto done;
        }
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&access_lock_list_mutex_g);
#endif
    // Request release lock region not found
    if (found == 0) {
        ret_value = FAIL; 
        printf("==PDC_SERVER[%d]: requested release region/object does not exist\n", pdc_server_rank_g);
        goto done;
    }
    out->ret = 1;
/*
    // contacting metadata server
    if(pdc_server_rank_g == in->meta_server_id) {
        PDC_Meta_Server_region_release(in, out);

        HG_Respond(*handle, NULL, NULL, out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }
    else {
        if (pdc_remote_server_info_g[in->meta_server_id].addr_valid != 1) {
             if (PDC_Server_lookup_server_id(in->meta_server_id) != SUCCEED) {
                printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                        pdc_server_rank_g, in->meta_server_id);
                ret_value = FAIL;
                error = 1;
                goto done;
            }
        }
        HG_Create(hg_context_g, pdc_remote_server_info_g[in->meta_server_id].addr, region_release_server_register_id_g, &server_send_region_release_handle);

        tranx_args = (struct transfer_unlock_args *)malloc(sizeof(struct transfer_unlock_args)); 
        tranx_args->handle = *handle;
        tranx_args->in = *in;
        tranx_args->bulk_args = bulk_args;
 
        hg_ret = HG_Forward(server_send_region_release_handle, server_send_region_release_rpc_cb, tranx_args, in);
        if (hg_ret != HG_SUCCESS) {
            error = 1;
            HG_Destroy(server_send_region_release_handle);
            free(tranx_args);
            PGOTO_ERROR(FAIL, "===PDC SERVER: PDC_Data_Server_region_release() - Could not start HG_Forward()");
        }
    }
*/
done:
    fflush(stdout);
/*
    if(error == 1) 
        out->ret = 0;
        HG_Respond(*handle, NULL, NULL, out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
        free(bulk_args);
*/    
    FUNC_LEAVE(ret_value);
}

/*
 * Check if the metadata satisfies the constraint received from client
 *
 * \param  metadata[IN]       Metadata pointer
 * \param  constraints[IN]    Constraints received from client    
 *
 * \return 1 if the metadata satisfies the constratins/-1 otherwise
 */
static int is_metadata_satisfy_constraint(pdc_metadata_t *metadata, metadata_query_transfer_in_t *constraints)
{
    int ret_value = 1;
    
    FUNC_ENTER(NULL);

    /* int     user_id; */
    /* char    *app_name; */
    /* char    *obj_name; */
    /* int     time_step_from; */
    /* int     time_step_to; */
    /* int     ndim; */
    /* char    *tags; */
    if (constraints->user_id > 0 && constraints->user_id != metadata->user_id) {
        ret_value = -1;
        goto done;
    }
    if (strcmp(constraints->app_name, " ") != 0 && strcmp(metadata->app_name, constraints->app_name) != 0) {
        ret_value = -1;
        goto done;
    }
    if (strcmp(constraints->obj_name, " ") != 0 && strcmp(metadata->obj_name, constraints->obj_name) != 0) {
        ret_value = -1;
        goto done;
    }
    if (constraints->time_step_from > 0 && constraints->time_step_to > 0 && 
        (metadata->time_step < constraints->time_step_from || metadata->time_step > constraints->time_step_to)
       ) {
        ret_value = -1;
        goto done;
    }
    if (constraints->ndim > 0 && metadata->ndim != constraints->ndim ) {
        ret_value = -1;
        goto done;
    }
    // TODO: Currently only supports searching with one tag
    if (strcmp(constraints->tags, " ") != 0 && strstr(metadata->tags, constraints->tags) == NULL) {
        ret_value = -1;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Get the metadata that satisfies the query constraint
 *
 * \param  in[IN]           Input structure from client that contains the query constraint
 * \param  n_meta[OUT]      Number of metadata that satisfies the query constraint
 * \param  buf_ptrs[OUT]    Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_partial_query_result(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs)
{
    perr_t ret_value = FAIL;
    uint32_t i;
    uint32_t n_buf, iter = 0;
    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    int n_entry;
    HashTablePair pair;

    
    FUNC_ENTER(NULL);

    // n_buf = n_metadata_g + 1 for potential padding array
    n_buf = n_metadata_g + 1;
    *buf_ptrs = (void**)calloc(n_buf, sizeof(void*));
    for (i = 0; i < n_buf; i++) {
        (*buf_ptrs)[i] = (void*)calloc(1, sizeof(void*));
    }
    // TODO: free buf_ptrs
    if (metadata_hash_table_g != NULL) {

        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);


        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            DL_FOREACH(head->metadata, elt) {
                // List all objects, no need to check other constraints
                if (in->is_list_all == 1) {
                    (*buf_ptrs)[iter++] = elt;
                }
                // check if current metadata matches search constraint
                else if (is_metadata_satisfy_constraint(elt, in) == 1) {
                    (*buf_ptrs)[iter++] = elt;
                }
            }
        }
        *n_meta = iter;

        /* printf("PDC_Server_get_partial_query_result: Total matching results: %d\n", *n_meta); */

    }  // if (metadata_hash_table_g != NULL)
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        goto done;
    }

    ret_value = SUCCEED;

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Seach the hash table with object name and hash key
 *
 * \param  obj_name[IN]     Name of the object to be searched
 * \param  hash_key[IN]     Hash value of the name string
 * \param  ts[IN]           Timestep value
 * \param  out[OUT]         Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_search_with_name_timestep(const char *obj_name, uint32_t hash_key, uint32_t ts, 
                                            pdc_metadata_t **out)
{
    perr_t ret_value = SUCCEED;
    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t metadata;
    const char *name;

    FUNC_ENTER(NULL);

    *out = NULL;

    // Set up a metadata struct to query
    PDC_Server_metadata_init(&metadata);

    name = obj_name;

    strcpy(metadata.obj_name, name);
    metadata.time_step = ts;

    /* printf("==PDC_SERVER[%d]: search with name [%s], hash key %u\n", pdc_server_rank_g, name, hash_key); */

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            /* printf("==PDC_SERVER: %s - lookup_value not NULL!\n", __func__); */
            // Check if there exist metadata identical to current one
            /* PDC_print_metadata(lookup_value->metadata); */
            /* if (lookup_value->bloom == NULL) { */
            /*     printf("bloom is NULL\n"); */
            /* } */
            *out = find_identical_metadata(lookup_value, &metadata);

            if (*out == NULL) {
                 /* printf("==PDC_SERVER[%d]: Queried object with name [%s] has no full match!\n", */
                 /* pdc_server_rank_g, obj_name); */
                /* fflush(stdout); */
                ret_value = FAIL;
                goto done;
            }
            /* else { */
                /* printf("==PDC_SERVER[%d]: name %s found in hash table \n", pdc_server_rank_g, name); */
                /* fflush(stdout); */
                /* PDC_print_metadata(*out); */
            /* } */
        }
        else {
            *out = NULL;
        }

    }
    else {
        printf("metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        goto done;
    }

    if (*out == NULL)
        printf("==PDC_SERVER[%d]: Queried object with name [%s] not found! \n", pdc_server_rank_g, name);

    /* PDC_print_metadata(*out); */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_search_with_name_timestep

/*
 * Seach the hash table with object name and hash key
 *
 * \param  obj_name[IN]     Name of the object to be searched
 * \param  hash_key[IN]     Hash value of the name string
 * \param  out[OUT]         Pointers to the found metadata
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_search_with_name_hash(const char *obj_name, uint32_t hash_key, pdc_metadata_t** out)
{
    perr_t ret_value = SUCCEED;
    pdc_hash_table_entry_head *lookup_value;
    pdc_metadata_t metadata;
    const char *name;
    
    FUNC_ENTER(NULL);
    
    *out = NULL;

    // Set up a metadata struct to query
    PDC_Server_metadata_init(&metadata);
    
    name = obj_name;

    strcpy(metadata.obj_name, name);
    /* metadata.time_step = tmp_time_step; */
    // TODO: currently PDC_Client_query_metadata_name_timestep is not taking timestep for querying 
    metadata.time_step = 0;

    /* printf("==PDC_SERVER[%d]: search with name [%s], hash key %u\n", pdc_server_rank_g, name, hash_key); */

    if (metadata_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", hash_key); */
        lookup_value = hash_table_lookup(metadata_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            /* printf("==PDC_SERVER: PDC_Server_search_with_name_hash(): lookup_value not NULL!\n"); */
            // Check if there exist metadata identical to current one
            /* PDC_print_metadata(lookup_value->metadata); */
            /* if (lookup_value->bloom == NULL) { */
            /*     printf("bloom is NULL\n"); */
            /* } */
            *out = find_identical_metadata(lookup_value, &metadata);

            if (*out == NULL) {
                 /* printf("==PDC_SERVER[%d]: Queried object with name [%s] has no full match!\n", */ 
                 /* pdc_server_rank_g, obj_name); */ 
                /* fflush(stdout); */
                ret_value = FAIL;
                goto done;
            }
            /* else { */
                /* printf("==PDC_SERVER[%d]: name %s found in hash table \n", pdc_server_rank_g, name); */
                /* fflush(stdout); */
                /* PDC_print_metadata(*out); */
            /* } */
        }
        else {
            *out = NULL;
        }

    }
    else {
        printf("metadata_hash_table_g not initialized!\n");
        ret_value = -1;
        goto done;
    }

    if (*out == NULL) 
        printf("==PDC_SERVER[%d]: Queried object with name [%s] not found! \n", pdc_server_rank_g, name);
    
    /* PDC_print_metadata(*out); */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/* void dstnode ( MySKL_n n ) */
/* { */
/*     pdc_metadata_t *item = MySKLgetEntry ( n, pdc_metadata_t, node ); */
/*     free(item); */
/* } */

/* int test_skl(int maxlev) */
/* { */
/*     MySKL_e err; */
/*     MySKL_t l; */
/*     MySKL_i it1; */
/*     MySKL_n n; */

/*     pdc_metadata_t *item1 = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t)); */
/*     pdc_metadata_t *item2 = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t)); */
/*     pdc_metadata_t *item3 = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t)); */
/*     item1->obj_id = 1; */
/*     item2->obj_id = 2; */
/*     item3->obj_id = 3; */

/*     l = MySKLinit( maxlev, PDC_metadata_cmp, dstnode, &err ); */
/*     if ( err == MYSKL_STATUS_OK ) { */
/*         printf("Inserting data to skl\n"); */
/*         MySKLinsertAD( l, &(item1->node) ); */
/*         MySKLinsertAD( l, &(item2->node) ); */
/*         MySKLinsertAD( l, &(item3->node) ); */

/*         MySKLsetIterator ( l, &it1, NULL ); */
/*         while ( ( n = MySKLgetNextNode ( &it1, 1 ) ) ) { */
/*             printf("%" PRIu64 "", MySKLgetEntry( n, LN_s, node )->obj_id); */
/*         } */

/*         MySKLdestroyIterator( &it1 ); */

/*         /1* MySKLdeleteNF( l, &tofound.node, NULL ); *1/ */
/*     } */
/*     else printf ( "Error with skiplist init!"); */

/*     return 0; */
/* } */

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
    char *tmp_env_char;
    perr_t ret;
    hg_return_t hg_ret;
    
    FUNC_ENTER(NULL);
    
#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pdc_server_rank_g);
    MPI_Comm_size(MPI_COMM_WORLD, &pdc_server_size_g);
#else
    pdc_server_rank_g = 0;
    pdc_server_size_g = 1;
#endif

    // Init rand seed
    srand(time(NULL));

    is_restart_g = 0;
    port = pdc_server_rank_g % 32 + 7000 ;
    /* printf("rank=%d, port=%d\n", pdc_server_rank_g,port); */

    // Set up tmp dir
    tmp_env_char = getenv("PDC_TMPDIR");
    if (tmp_env_char == NULL) 
        tmp_env_char = "./pdc_tmp";

    sprintf(pdc_server_tmp_dir_g, "%s/", tmp_env_char);

    // Get number of OST per file
    tmp_env_char = getenv("PDC_NOST_PER_FILE");
    if (tmp_env_char!= NULL) {
        pdc_nost_per_file_g = atoi(tmp_env_char);
        // Make sure it is a sane value
        if (pdc_nost_per_file_g < 1 || pdc_nost_per_file_g > 248) {
            pdc_nost_per_file_g = 1;
        }
    }

    // Get bb write percentage 
    tmp_env_char = getenv("PDC_BB_WRITE_PERCENT");
    if (tmp_env_char == NULL) 
        write_to_bb_percentage_g = 0;
    else
        write_to_bb_percentage_g = atoi(tmp_env_char);

    if (write_to_bb_percentage_g < 0 || write_to_bb_percentage_g > 100) 
        write_to_bb_percentage_g = 0;

    /* int pid = getpid(); */
    /* printf("==PDC_SERVER[%d]: my pid is %d\n", pdc_server_rank_g, pid); */


    if (pdc_server_rank_g == 0) {
        printf("\n==PDC_SERVER[%d]: using [%s] as tmp dir. %d OSTs per data file, %d%% to BB\n", 
                pdc_server_rank_g, pdc_server_tmp_dir_g, pdc_nost_per_file_g, write_to_bb_percentage_g);
    }

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  start;
    struct timeval  end;
    long long elapsed;
    double server_init_time;
    gettimeofday(&start, 0);
#endif

    if (argc > 1) {
        if (strcmp(argv[1], "restart") == 0) 
            is_restart_g = 1;
    }

    ret = PDC_Server_init(port, &hg_class_g, &hg_context_g);
    if (ret != SUCCEED || hg_class_g == NULL || hg_context_g == NULL) {
        printf("Error with Mercury init, exit...\n");
        ret = FAIL;
        goto done;
    }

    // Get debug environment var
    char *is_debug_env = getenv("PDC_DEBUG");
    if (is_debug_env != NULL) {
        is_debug_g = atoi(is_debug_env);
        if (pdc_server_rank_g == 0)
            printf("==PDC_SERVER[%d]: PDC_DEBUG set to %d!\n", pdc_server_rank_g, is_debug_g);
    }

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
    obj_unmap_register(hg_class_g);

    // Data server
    data_server_read_register(hg_class_g);
    data_server_write_register(hg_class_g);
    data_server_read_check_register(hg_class_g);
    data_server_write_check_register(hg_class_g);

    // Server to client RPC
    server_lookup_client_register_id_g = server_lookup_client_register(hg_class_g);
    notify_io_complete_register_id_g   = notify_io_complete_register(hg_class_g);

    // Server to server RPC
    get_remote_metadata_register_id_g         = get_remote_metadata_register(hg_class_g);  
    buf_map_server_register_id_g              = buf_map_server_register(hg_class_g);
    buf_unmap_server_register_id_g            = buf_unmap_server_register(hg_class_g);
    region_lock_server_register_id_g          = region_lock_server_register(hg_class_g);
    region_release_server_register_id_g       = region_release_server_register(hg_class_g);
    server_lookup_remote_server_register_id_g = server_lookup_remote_server_register(hg_class_g);
    update_region_loc_register_id_g           = update_region_loc_register(hg_class_g);
    notify_region_update_register_id_g        = notify_region_update_register(hg_class_g);
    get_metadata_by_id_register_id_g          = get_metadata_by_id_register(hg_class_g);
    get_reg_lock_register_id_g                = get_reg_lock_register(hg_class_g);
    get_storage_info_register_id_g            = get_storage_info_register(hg_class_g);
    bulk_rpc_register_id_g                    = bulk_rpc_register(hg_class_g);
    storage_meta_name_query_register_id_g     = storage_meta_name_query_rpc_register(hg_class_g);
    get_storage_meta_name_query_bulk_result_rpc_register_id_g = get_storage_meta_name_query_bulk_result_rpc_register(hg_class_g);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    
    /* char *is_lookupall = getenv("PDC_LOOKUP_ALL"); */
    /* if (is_lookupall == NULL || strcmp(is_lookupall, "0") == 0) { */
    /*     if (pdc_server_rank_g == 0) */ 
    /*         printf("==PDC_SERVER[%d]: will lookup other PDC servers on demand\n", pdc_server_rank_g); */
    /* } */
    /* else { */
        // Lookup all remote servers addr
        if (PDC_Server_lookup_all_servers() != SUCCEED) {
            printf("==PDC_SERVER[%d]: unable to lookup_remote_server, exiting...\n", pdc_server_rank_g);
            fflush(stdout);
            goto done;
        }
        else {
            if (pdc_server_rank_g == 0) {
                printf("==PDC_SERVER[%d]: Successfully established connection to %d other PDC servers\n",
                        pdc_server_rank_g, pdc_server_size_g- 1);
                fflush(stdout);
            }
        }
    /* } */


    if (pdc_server_rank_g == 0) 
        if (PDC_Server_write_addr_to_file(all_addr_strings_g, pdc_server_size_g) != SUCCEED) 
            printf("==PDC_SERVER[%d]: Error with write config file\n", pdc_server_rank_g);

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&end, 0);
    elapsed = (end.tv_sec-start.tv_sec)*1000000LL + end.tv_usec-start.tv_usec;
    server_init_time = elapsed / 1000000.0;
#endif

    if (pdc_server_rank_g == 0) {
        #ifdef ENABLE_TIMING
        printf("==PDC_SERVER[%d]: total startup time = %.6f\n", pdc_server_rank_g, server_init_time);
        #endif

        printf("==PDC_SERVER[%d]: Server ready!\n\n\n", pdc_server_rank_g);
    }
    fflush(stdout);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif

    /* // Debug */
    /* test_serialize(); */


#ifdef ENABLE_MULTITHREAD
    PDC_Server_multithread_loop(hg_context_g);
#else
    PDC_Server_loop(hg_context_g);
#endif

    /* printf("==PDC_SERVER[%d]: shutdown in progress...\n", pdc_server_rank_g); */
    /* fflush(stdout); */

    /* if (pdc_server_rank_g == 0) { */
    /*     printf("==PDC_SERVER: All work done, finalizing\n"); */
    /*     fflush(stdout); */
    /* } */
#ifdef ENABLE_CHECKPOINT
    // TODO: instead of checkpoint at app finalize time, try checkpoint with a time countdown or # of objects
    char checkpoint_file[ADDR_MAX];
    sprintf(checkpoint_file, "%s%s%d", pdc_server_tmp_dir_g, "metadata_checkpoint.", pdc_server_rank_g);

    #ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double checkpoint_time, all_checkpoint_time;
    gettimeofday(&pdc_timer_start, 0);
    #endif

    PDC_Server_checkpoint(checkpoint_file);

    #ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    checkpoint_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);

        #ifdef ENABLE_MPI
    MPI_Reduce(&checkpoint_time, &all_checkpoint_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        #else
    all_checkpoint_time = checkpoint_time;
        #endif

    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER: total checkpoint  time = %.6f\n", all_checkpoint_time);
    }

    #endif
#endif

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
               "              #fwrite %3d, Tfwrite (%6.2f, %6.2f, %6.2f), %.0f MB\n"
               "              #fread  %3d, Tfread  (%6.2f, %6.2f, %6.2f), %.0f MB\n"
               "              #fopen  %3d, Tfopen  (%6.2f, %6.2f, %6.2f)\n"
               "              Tfsync               (%6.2f, %6.2f, %6.2f)\n"
               "              Ttotal_IO            (%6.2f, %6.2f, %6.2f)\n"
               "              Ttotal_IO_elapsed    (%6.2f, %6.2f, %6.2f)\n"
               "              Tregion_update       (%6.2f, %6.2f, %6.2f)\n"
               "              Tget_region          (%6.2f, %6.2f, %6.2f)\n",  
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

done:
    if (pdc_server_rank_g == 0) {
        printf("==PDC_SERVER[0]: start finalizing ...\n");
        /* printf("==PDC_SERVER: [%d] exiting...\n", pdc_server_rank_g); */
        fflush(stdout);
    }
    PDC_Server_finalize();

    // Finalize 
    /* hg_ret = HG_Context_destroy(pdc_client_context_g); */
    /* if (hg_ret != HG_SUCCESS) */ 
    /*     printf("error with HG_Context_destroy(pdc_client_context_g)\n"); */

    hg_ret = HG_Context_destroy(hg_context_g);
    if (hg_ret != HG_SUCCESS) 
        printf("==PDC_SERVER[%d]: error with HG_Context_destroy\n", pdc_server_rank_g);

    hg_ret = HG_Finalize(hg_class_g);
    if (hg_ret != HG_SUCCESS) 
        printf("==PDC_SERVER[%d]: error with HG_Finalize\n", pdc_server_rank_g);
    else {
        if (pdc_server_rank_g == 0) {
            printf("==PDC_SERVER[0]: Finalized\n");
            /* printf("==PDC_SERVER: [%d] exiting...\n", pdc_server_rank_g); */
            fflush(stdout);
        }
    }

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}


/*
 * Data Server related
 */

/*
 * Check if two region are the same
 *
 * \param  a[IN]     Pointer of the first region 
 * \param  b[IN]     Pointer of the second region 
 *
 * \return 1 if same/0 otherwise
 */
int region_list_cmp(region_list_t *a, region_list_t *b) 
{
    if (a->ndim != b->ndim) {
        printf("  region_list_cmp(): not equal ndim! \n");
        return -1;
    }

    uint32_t i;
    uint64_t tmp;
    for (i = 0; i < a->ndim; i++) {
        tmp = a->start[i] - b->start[i];
        if (tmp != 0) 
            return tmp;
    }
    return 0;
}

/*
 * Check if two region are from the same client
 *
 * \param  a[IN]     Pointer of the first region 
 * \param  b[IN]     Pointer of the second region 
 *
 * \return 1 if same/0 otherwise
 */
int region_list_cmp_by_client_id(region_list_t *a, region_list_t *b) 
{
    if (a->ndim != b->ndim) {
        printf("  region_list_cmp_by_client_id(): not equal ndim! \n");
        return -1;
    }

    return (a->client_ids[0] - b->client_ids[0]);
}

perr_t PDC_Data_Server_buf_unmap(const struct hg_info *info, buf_unmap_in_t *in)
{
    perr_t ret_value = SUCCEED;
    region_buf_map_t *tmp, *elt;
    data_server_region_t *target_obj;

    FUNC_ENTER(NULL);
   
    target_obj = PDC_Server_get_obj_region(in->remote_obj_id);
    if (target_obj == NULL) {
        PGOTO_ERROR(FAIL, "===PDC_DATA_SERVER: PDC_Data_Server_buf_unmap() - requested object does not exist");
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&delete_buf_map_mutex_g);
#endif
    DL_FOREACH_SAFE(target_obj->region_buf_map_head, elt, tmp) {
        if(in->remote_obj_id==elt->remote_obj_id && region_is_identical(in->remote_region, elt->remote_region_unit)) {
            // wait for work to be done, then free
            hg_thread_mutex_lock(&(elt->bulk_args->work_mutex));
            while (!elt->bulk_args->work_completed)
                hg_thread_cond_wait(&(elt->bulk_args->work_cond), &(elt->bulk_args->work_mutex));
            elt->bulk_args->work_completed = 0;
            hg_thread_mutex_unlock(&(elt->bulk_args->work_mutex));  //per bulk_args

            free(elt->remote_data_ptr);  
            HG_Addr_free(info->hg_class, elt->local_addr);
            HG_Bulk_free(elt->local_bulk_handle);
            DL_DELETE(target_obj->region_buf_map_head, elt);
            hg_thread_mutex_destroy(&(elt->bulk_args->work_mutex));
            hg_thread_cond_destroy(&(elt->bulk_args->work_cond)); 
            free(elt->bulk_args); 
            free(elt);
        }
    }
    if(target_obj->region_buf_map_head == NULL && pdc_server_rank_g == 0) {
        close(target_obj->fd);
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&delete_buf_map_mutex_g);
#endif

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t server_send_buf_unmap_addr_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    buf_map_out_t out;
    struct transfer_buf_unmap *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_unmap *) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &out);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_buf_unmap_addr_rpc_cb - error with HG_Get_output\n",
                pdc_server_rank_g);
        goto done;
    }

done:
    HG_Respond(tranx_args->handle, NULL, NULL, &out);
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
buf_unmap_lookup_remote_server_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    uint32_t server_id;
    struct buf_unmap_server_lookup_args_t *lookup_args;
    hg_handle_t server_send_buf_unmap_handle;
    hg_handle_t handle;
    struct transfer_buf_unmap *tranx_args;
    buf_unmap_out_t out;
    int error = 0;

    FUNC_ENTER(NULL);

    lookup_args = (struct buf_unmap_server_lookup_args_t*) callback_info->arg;
    server_id = lookup_args->server_id;
    tranx_args = lookup_args->buf_unmap_args;
    handle = tranx_args->handle;

    pdc_remote_server_info_g[server_id].addr = callback_info->info.lookup.addr;
    pdc_remote_server_info_g[server_id].addr_valid = 1;

    if (pdc_remote_server_info_g[server_id].addr == NULL) {
        printf("==PDC_SERVER[%d]: %s - remote server addr is NULL\n", pdc_server_rank_g, __func__);
        error = 1;
        goto done;
    }
    HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, buf_unmap_server_register_id_g, &server_send_buf_unmap_handle);

    ret_value = HG_Forward(server_send_buf_unmap_handle, server_send_buf_unmap_addr_rpc_cb, tranx_args, &(tranx_args->in)); 
    if (ret_value != HG_SUCCESS) {
        error = 1;
        HG_Destroy(server_send_buf_unmap_handle);
        PGOTO_ERROR(ret_value, "===PDC SERVER: buf_unmap_lookup_remote_server_cb() - Could not start HG_Forward()");
    }
done:
    if(error == 1) {
        out.ret = 0;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &(lookup_args->buf_unmap_args->in));
        HG_Destroy(handle);
        free(tranx_args);
    }
    free(lookup_args);
    FUNC_LEAVE(ret_value);
}

// reference from PDC_Server_lookup_server_id
perr_t PDC_Server_buf_unmap_lookup_server_id(int remote_server_id, struct transfer_buf_unmap *transfer_args)
{
    perr_t ret_value      = SUCCEED;
    hg_return_t hg_ret    = HG_SUCCESS;
    struct buf_unmap_server_lookup_args_t *lookup_args;
    hg_handle_t handle;
    buf_unmap_out_t out;
    int error = 0;

    FUNC_ENTER(NULL);

    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: Testing connection to remote server %d: %s\n",
                pdc_server_rank_g, remote_server_id, pdc_remote_server_info_g[remote_server_id].addr_string);
        fflush(stdout);
    }

    handle = transfer_args->handle;
    lookup_args = (struct buf_unmap_server_lookup_args_t *)malloc(sizeof(struct buf_unmap_server_lookup_args_t));
    lookup_args->server_id = remote_server_id;
    lookup_args->buf_unmap_args = transfer_args;
    hg_ret = HG_Addr_lookup(hg_context_g, buf_unmap_lookup_remote_server_cb, lookup_args,
                            pdc_remote_server_info_g[remote_server_id].addr_string, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS) {
        error = 1;
        PGOTO_ERROR(FAIL, "==PDC_SERVER: PDC_Server_buf_unmap_lookup_server_id() Connection to remote server FAILED!");
    }

    /* printf("==PDC_SERVER[%d]: connected to remote server %d\n", pdc_server_rank_g, remote_server_id); */

done:
    fflush(stdout);
    if(error == 1) {
        out.ret = 0;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &(transfer_args->in));
        HG_Destroy(handle);
        free(transfer_args);
    }

    FUNC_LEAVE(ret_value);
} //  PDC_Server_buf_unmap_lookup_server_id 

static hg_return_t server_send_buf_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    buf_unmap_out_t output;
    struct transfer_buf_unmap_args *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_unmap_args *) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_buf_unmap_rpc_cb() - error with HG_Get_output\n",
                pdc_server_rank_g);
        tranx_args->ret = -1;
        goto done;
    }

    tranx_args->ret = output.ret;

done:
    HG_Respond(tranx_args->handle, NULL, NULL, &output);
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &output);
    HG_Destroy(handle);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}

perr_t PDC_Meta_Server_buf_unmap(buf_unmap_in_t *in, hg_handle_t *handle)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret = HG_SUCCESS;
    hg_handle_t server_send_buf_unmap_handle;
    struct transfer_buf_unmap_args *buf_unmap_args;
    struct transfer_buf_unmap *addr_args;
    pdc_metadata_t *target_meta = NULL;
    region_buf_map_t *tmp, *elt;
    buf_unmap_out_t out;
    const struct hg_info *info;
    int error = 0;
 
    FUNC_ENTER(NULL);

    info = HG_Get_info(*handle);

    if((uint32_t)pdc_server_rank_g == in->meta_server_id) {
        target_meta = find_metadata_by_id(in->remote_obj_id);
        if(target_meta == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "===PDC META SERVER: cannot retrieve object metadata");
        }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&remove_buf_map_mutex_g);
#endif
        DL_FOREACH_SAFE(target_meta->region_buf_map_head, elt, tmp) {

            if(in->remote_obj_id==elt->remote_obj_id && region_is_identical(in->remote_region, elt->remote_region_unit)) {
                HG_Bulk_free(elt->local_bulk_handle);
                HG_Addr_free(info->hg_class, elt->local_addr);
                DL_DELETE(target_meta->region_buf_map_head, elt);
                free(elt);
            }
        }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&remove_buf_map_mutex_g);
#endif
        out.ret = 1;
        HG_Respond(*handle, NULL, NULL, &out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }
    else {
        if (pdc_remote_server_info_g[in->meta_server_id].addr_valid != 1) {
            addr_args = (struct transfer_buf_unmap *)malloc(sizeof(struct transfer_buf_unmap));
            addr_args->handle = *handle;
            addr_args->in = *in;

            PDC_Server_buf_unmap_lookup_server_id(in->meta_server_id, addr_args);
        }
        else {
            HG_Create(hg_context_g, pdc_remote_server_info_g[in->meta_server_id].addr, buf_unmap_server_register_id_g, &server_send_buf_unmap_handle);

            buf_unmap_args = (struct transfer_buf_unmap_args *)malloc(sizeof(struct transfer_buf_unmap_args));
            buf_unmap_args->handle = *handle;
            buf_unmap_args->in = *in;
            hg_ret = HG_Forward(server_send_buf_unmap_handle, server_send_buf_unmap_rpc_cb, buf_unmap_args, in);
            if (hg_ret != HG_SUCCESS) {
                HG_Destroy(server_send_buf_unmap_handle);
                free(buf_unmap_args);
                error = 1;
                PGOTO_ERROR(FAIL, "PDC_Meta_Server_buf_unmap(): Could not start HG_Forward()");
            }
        }
    }

done:
    if(error == 1) {
        out.ret = 0;
        HG_Respond(*handle, NULL, NULL, &out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }

    FUNC_LEAVE(ret_value);
}

region_buf_map_t *PDC_Data_Server_buf_map(const struct hg_info *info, buf_map_in_t *in, region_list_t *request_region, void *data_ptr)
{
    region_buf_map_t *ret_value = NULL;
    data_server_region_t *new_obj_reg = NULL;
    region_list_t *elt_reg;
    region_buf_map_t *buf_map_ptr = NULL;
    char *data_path = NULL;
    char *user_specified_data_path = NULL;
    char storage_location[ADDR_MAX];
    int stripe_count, stripe_size;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&create_region_struct_mutex_g);
#endif

    new_obj_reg = PDC_Server_get_obj_region(in->remote_obj_id);
    if(new_obj_reg == NULL) {
        new_obj_reg = (data_server_region_t *)malloc(sizeof(struct data_server_region_t));
        if(new_obj_reg == NULL)
            PGOTO_ERROR(NULL, "PDC_SERVER: PDC_Server_insert_buf_map_region() allocates new object failed");
        new_obj_reg->obj_id = in->remote_obj_id;
        new_obj_reg->region_lock_head = NULL;
        new_obj_reg->region_buf_map_head = NULL;
//        new_obj_reg->region_storage_head = NULL;

        // Generate a location for data storage for data server to write
        user_specified_data_path = getenv("PDC_DATA_LOC");
        if (user_specified_data_path != NULL)
            data_path = user_specified_data_path;
        else {
            data_path = getenv("SCRATCH");
            if (data_path == NULL)
                data_path = ".";
        }
        // Data path prefix will be $SCRATCH/pdc_data/$obj_id/
        sprintf(storage_location, "%s/pdc_data/%" PRIu64 "/server%d/s%04d.bin",
            data_path, in->remote_obj_id, pdc_server_rank_g, pdc_server_rank_g);
        pdc_mkdir(storage_location);

#ifdef ENABLE_LUSTRE
        if (pdc_nost_per_file_g != 1)
            stripe_count = 248 / pdc_server_size_g;
        else
            stripe_count = pdc_nost_per_file_g;
        stripe_size  = 16;           //MB
        PDC_Server_set_lustre_stripe(storage_location, stripe_count, stripe_size);

        if (is_debug_g == 1 && pdc_server_rank_g == 0) {
            printf("storage_location is %s\n", storage_location);
        }
#endif
        new_obj_reg->fd = open(storage_location, O_WRONLY|O_CREAT, 0666);
        if(new_obj_reg->fd == -1){
            printf("==PDC_SERVER[%d]: open %s failed\n", pdc_server_rank_g, storage_location);
            goto done;
        }

        DL_APPEND(dataserver_region_g, new_obj_reg);
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&create_region_struct_mutex_g);
#endif

    buf_map_ptr = (region_buf_map_t *)malloc(sizeof(region_buf_map_t));
    if(buf_map_ptr == NULL)
        PGOTO_ERROR(NULL, "PDC_SERVER: PDC_Server_insert_buf_map_region() allocates region pointer failed");

    buf_map_ptr->local_reg_id = in->local_reg_id;
    buf_map_ptr->local_region = in->local_region;
    buf_map_ptr->local_ndim = in->ndim;
    buf_map_ptr->local_data_type = in->local_type;
    HG_Addr_dup(info->hg_class, info->addr, &(buf_map_ptr->local_addr));
    HG_Bulk_ref_incr(in->local_bulk_handle);
    buf_map_ptr->local_bulk_handle = in->local_bulk_handle;

    buf_map_ptr->remote_obj_id = in->remote_obj_id;
    buf_map_ptr->remote_reg_id = in->remote_reg_id;
    buf_map_ptr->remote_client_id = in->remote_client_id;
    buf_map_ptr->remote_ndim = in->ndim;
    buf_map_ptr->remote_unit = in->remote_unit;
    buf_map_ptr->remote_region_unit = in->remote_region_unit;
    buf_map_ptr->remote_region_nounit = in->remote_region_nounit;
    buf_map_ptr->remote_data_ptr = data_ptr;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&append_region_struct_mutex_g);
#endif
    DL_APPEND(new_obj_reg->region_buf_map_head, buf_map_ptr);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&append_region_struct_mutex_g);
#endif

    DL_FOREACH(new_obj_reg->region_lock_head, elt_reg) {
        if (PDC_is_same_region_list(elt_reg, request_region) == 1) {
            hg_atomic_incr32(&(elt_reg->buf_map_refcount));
//            printf("mapped region is locked \n");
//            fflush(stdout); 
        }
    }
    ret_value = buf_map_ptr;

done:
    FUNC_LEAVE(ret_value);
}

void *PDC_Server_get_region_ptr(pdcid_t obj_id, region_info_transfer_t region)
{
    void *ret_value = NULL;
    data_server_region_t *target_obj = NULL, *elt = NULL;
    region_buf_map_t *tmp;

    FUNC_ENTER(NULL);

    if(dataserver_region_g == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_ptr() - object list is NULL");
    DL_FOREACH(dataserver_region_g, elt) {
        if(obj_id == elt->obj_id)
            target_obj = elt;
    }
    if(target_obj == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_ptr() - cannot locate object");

    DL_FOREACH(target_obj->region_buf_map_head, tmp) {
        if (is_region_transfer_t_identical(&region, &(tmp->remote_region_unit)) == 1) {
            ret_value = tmp->remote_data_ptr;
            break;
        }
    }
    if(ret_value == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_ptr() - region data pointer is NULL");

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t server_send_buf_map_addr_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    buf_map_out_t out;
    struct transfer_buf_map *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_map *) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &out);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_buf_map_addr_rpc_cb - error with HG_Get_output\n",
                pdc_server_rank_g);
        goto done;
    }

done:
    HG_Respond(tranx_args->handle, NULL, NULL, &out);
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}

static hg_return_t
buf_map_lookup_remote_server_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    uint32_t server_id;
    struct buf_map_server_lookup_args_t *lookup_args;
    hg_handle_t server_send_buf_map_handle;
    hg_handle_t handle;
    struct transfer_buf_map *tranx_args;
    buf_map_out_t out;
    int error = 0;

    FUNC_ENTER(NULL);

    lookup_args = (struct buf_map_server_lookup_args_t*) callback_info->arg;
    server_id = lookup_args->server_id;
    tranx_args = lookup_args->buf_map_args;
    handle = tranx_args->handle;

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
        error = 1;
        goto done;
    }
    HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, buf_map_server_register_id_g, &server_send_buf_map_handle);

    ret_value = HG_Forward(server_send_buf_map_handle, server_send_buf_map_addr_rpc_cb, tranx_args, &(tranx_args->in));
    if (ret_value != HG_SUCCESS) {
        error = 1;
        HG_Destroy(server_send_buf_map_handle);
        PGOTO_ERROR(ret_value, "===PDC SERVER: buf_map_lookup_remote_server_cb() - Could not start HG_Forward()");
    }

done:
    if(error == 1) {
        out.ret = 0;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &(lookup_args->buf_map_args->in));
        HG_Destroy(handle);
        free(tranx_args);
    }
    free(lookup_args);
    FUNC_LEAVE(ret_value);
}

// reference from PDC_Server_lookup_server_id
perr_t PDC_Server_buf_map_lookup_server_id(int remote_server_id, struct transfer_buf_map *transfer_args)
{
    perr_t ret_value      = SUCCEED;
    hg_return_t hg_ret    = HG_SUCCESS;
    struct buf_map_server_lookup_args_t *lookup_args;
    hg_handle_t handle;
    buf_map_out_t out;
    int error = 0;

    FUNC_ENTER(NULL);

    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: Testing connection to remote server %d: %s\n",
                pdc_server_rank_g, remote_server_id, pdc_remote_server_info_g[remote_server_id].addr_string);
        fflush(stdout);
    }

    handle = transfer_args->handle;
    lookup_args = (struct buf_map_server_lookup_args_t *)malloc(sizeof(struct buf_map_server_lookup_args_t));
    lookup_args->server_id = remote_server_id;
    lookup_args->buf_map_args = transfer_args;
    hg_ret = HG_Addr_lookup(hg_context_g, buf_map_lookup_remote_server_cb, lookup_args,
                            pdc_remote_server_info_g[remote_server_id].addr_string, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS) {
        error = 1;
        PGOTO_ERROR(FAIL, "==PDC_SERVER: PDC_Server_buf_map_lookup_server_id() Connection to remote server FAILED!");
    }

    /* printf("==PDC_SERVER[%d]: connected to remote server %d\n", pdc_server_rank_g, remote_server_id); */

done:
    fflush(stdout);
    if(error == 1) {
        out.ret = 0;
        HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &(transfer_args->in));
        HG_Destroy(handle); 
        free(transfer_args);
    }

    FUNC_LEAVE(ret_value);
} //  PDC_Server_buf_map_lookup_server_id 

static hg_return_t server_send_buf_map_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    buf_map_out_t out;
    struct transfer_buf_map_args *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_map_args *) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &out);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_buf_map_rpc_cb - error with HG_Get_output\n",
                pdc_server_rank_g);
        tranx_args->ret = -1;
        goto done;
    }
    tranx_args->ret = out.ret;

done:
    HG_Respond(tranx_args->handle, NULL, NULL, &out);
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}

perr_t PDC_Meta_Server_buf_map(buf_map_in_t *in, region_buf_map_t *new_buf_map_ptr, hg_handle_t *handle)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret = HG_SUCCESS;
    hg_handle_t server_send_buf_map_handle;
    struct transfer_buf_map_args *tranx_args = NULL;
    struct transfer_buf_map *addr_args;
    pdc_metadata_t *target_meta = NULL;
    region_buf_map_t *buf_map_ptr;
    buf_map_out_t out;
    int error = 0;

    FUNC_ENTER(NULL);

    // dataserver and metadata server is on the same node
    if((uint32_t)pdc_server_rank_g == in->meta_server_id) {
        target_meta = find_metadata_by_id(in->remote_obj_id);
        if (target_meta == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "PDC_DATA_SERVER: PDC_Meta_Server_buf_map() find_metadata_by_id FAILED!");
        }

        buf_map_ptr = (region_buf_map_t *)malloc(sizeof(region_buf_map_t));
        if(buf_map_ptr == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "PDC_SERVER: PDC_Server_insert_buf_map_region() allocates region pointer failed");
        }
        buf_map_ptr->local_reg_id = new_buf_map_ptr->local_reg_id;
        buf_map_ptr->local_region = new_buf_map_ptr->local_region;
        buf_map_ptr->local_ndim = new_buf_map_ptr->local_ndim;
        buf_map_ptr->local_data_type = new_buf_map_ptr->local_data_type;
        buf_map_ptr->local_addr = new_buf_map_ptr->local_addr;
        buf_map_ptr->local_bulk_handle = new_buf_map_ptr->local_bulk_handle;

        buf_map_ptr->remote_obj_id = new_buf_map_ptr->remote_obj_id;
        buf_map_ptr->remote_reg_id = new_buf_map_ptr->remote_reg_id;
        buf_map_ptr->remote_client_id = new_buf_map_ptr->remote_client_id;
        buf_map_ptr->remote_ndim = new_buf_map_ptr->remote_ndim;
        buf_map_ptr->remote_unit = new_buf_map_ptr->remote_unit;
        buf_map_ptr->remote_region_unit = new_buf_map_ptr->remote_region_unit;
        buf_map_ptr->remote_region_nounit = new_buf_map_ptr->remote_region_nounit;
        buf_map_ptr->remote_data_ptr = new_buf_map_ptr->remote_data_ptr;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&append_buf_map_mutex_g);
#endif
        DL_APPEND(target_meta->region_buf_map_head, buf_map_ptr);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&append_buf_map_mutex_g);
#endif

        out.ret = 1;
        HG_Respond(*handle, NULL, NULL, &out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }
    else {
        if (pdc_remote_server_info_g[in->meta_server_id].addr_valid != 1) {
             addr_args = (struct transfer_buf_map *)malloc(sizeof(struct transfer_buf_map));
             addr_args->handle = *handle;
             addr_args->in = *in;

             PDC_Server_buf_map_lookup_server_id(in->meta_server_id, addr_args);
        }
        else {
            HG_Create(hg_context_g, pdc_remote_server_info_g[in->meta_server_id].addr, buf_map_server_register_id_g, &server_send_buf_map_handle);

            tranx_args = (struct transfer_buf_map_args *)malloc(sizeof(struct transfer_buf_map_args));
            tranx_args->handle = *handle;
            tranx_args->in = *in;
            hg_ret = HG_Forward(server_send_buf_map_handle, server_send_buf_map_rpc_cb, tranx_args, in);
            if (hg_ret != HG_SUCCESS) {
                error = 1;
                HG_Destroy(server_send_buf_map_handle);
                free(tranx_args);
                PGOTO_ERROR(FAIL, "PDC_Server_transfer_region_info(): Could not start HG_Forward()");
            }
        }
    }

done:
    if(error == 1) {
        out.ret = 0;
        HG_Respond(*handle, NULL, NULL, &out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
        if((uint32_t)pdc_server_rank_g != in->meta_server_id && tranx_args != NULL)
            free(tranx_args);
    }
    FUNC_LEAVE(ret_value);
}


// TODO: currently only support merging regions that are cut in one dimension
/*
 * Merge multiple region to contiguous ones
 *
 * \param  list[IN]         Pointer of the regions in a list
 * \param  merged[OUT]      Merged list (new)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_merge_region_list_naive(region_list_t *list, region_list_t **merged)
{
    perr_t ret_value = FAIL;


    // print all regions
    region_list_t *elt, *elt_elt;
    region_list_t *tmp_merge;
    uint32_t i;
    int count, pos, pos_pos, tmp_pos;
    int *is_merged;

    DL_SORT(list, region_list_cmp);
    DL_COUNT(list, elt, count);

    is_merged = (int*)calloc(sizeof(int), count);

    DL_FOREACH(list, elt) {
        PDC_print_region_list(elt);
    }

    // Init merged head
    pos = 0;
    DL_FOREACH(list, elt) {
        if (is_merged[pos] != 0) {
            pos++;
            continue;
        }

        // First region that has not been merged
        tmp_merge = (region_list_t*)malloc(sizeof(region_list_t));
        if (NULL == tmp_merge) {
            printf("==PDC_SERVER: ERROR allocating for region_list_t!\n");
            ret_value = FAIL;
            goto done;
        }
        PDC_init_region_list(tmp_merge);

        // Add the client id to the client_ids[] arrary
        tmp_pos = 0;
        tmp_merge->client_ids[tmp_pos] = elt->client_ids[0];
        tmp_pos++;

        tmp_merge->ndim = list->ndim;
        for (i = 0; i < list->ndim; i++) {
            tmp_merge->start[i]  = elt->start[i];
            /* tmp_merge->stride[i] = elt->stride[i]; */
            tmp_merge->count[i]  = elt->count[i];
        }
        is_merged[pos] = 1;

        DL_APPEND(*merged, tmp_merge);

        // Check for all other regions in the list and see it any can be merged
        pos_pos = 0;
        DL_FOREACH(list, elt_elt) {
            if (is_merged[pos_pos] != 0) {
                pos_pos++;
                continue;
            }

            // check if current elt_elt can be merged to elt
            for (i = 0; i < list->ndim; i++) {
                if (elt_elt->start[i] == tmp_merge->start[i] + tmp_merge->count[i]) {
                    tmp_merge->count[i] += elt_elt->count[i];
                    is_merged[pos_pos] = 1;
                    tmp_merge->client_ids[tmp_pos] = elt_elt->client_ids[0];
                    tmp_pos++;
                    break;
                }
            }
            pos_pos++;
        }

        pos++;
    }

    ret_value = SUCCEED;

done:
    fflush(stdout);
    free(is_merged);
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function for the region update, gets output from client
 *
 * \param  callback_info[OUT]      Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t PDC_Server_notify_region_update_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    notify_region_update_out_t output;
    struct server_region_update_args *update_args;

    FUNC_ENTER(NULL);

    update_args = (struct server_region_update_args*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from client */
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_notify_region_update_cb - error with HG_Get_output\n", 
                pdc_server_rank_g);
        update_args->ret = -1;
        goto done;
    }
    //printf("==PDC_SERVER[%d]: PDC_Server_notify_region_update_cb - received from client with %d\n", pdc_server_rank_g, output.ret);
    update_args->ret = output.ret;

done:
    /* work_todo_g--; */
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function for the notify region update
 *
 * \param  meta_id[OUT]      Metadata ID
 * \param  reg_id[OUT]       Object ID
 * \param  client_id[OUT]    Client's MPI rank
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_SERVER_notify_region_update_to_client(uint64_t obj_id, uint64_t reg_id, int32_t client_id)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    struct server_region_update_args update_args;
    hg_handle_t notify_region_update_handle;

    FUNC_ENTER(NULL);

    if (client_id >= (uint32_t)pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: PDC_SERVER_notify_region_update_to_client() - "
                "client_id %d invalid)\n", pdc_server_rank_g, client_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g || pdc_client_info_g[client_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_client(client_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER: PDC_Server_notify_region_update_to_client() - \
                    PDC_Server_lookup_client failed)\n");
            return FAIL;
        }
    }

    if (pdc_client_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER: pdc_client_info_g is NULL\n");
        return FAIL;
    }


    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr, notify_region_update_register_id_g,
                        &notify_region_update_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Server_notify_region_update_to_client(): Could not HG_Create()\n");
        ret_value = FAIL;
        goto done;
    }

    // Fill input structure
    notify_region_update_in_t in;
    in.obj_id    = obj_id;
    in.reg_id    = reg_id;

    /* printf("Sending input to target\n"); */
    hg_ret = HG_Forward(notify_region_update_handle, PDC_Server_notify_region_update_cb, &update_args, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Server_notify_region_update_to_client(): Could not start HG_Forward()\n");
        ret_value = FAIL;
        goto done;
    }

done:
    HG_Destroy(notify_region_update_handle);
    FUNC_LEAVE(ret_value);
}

/*
 * Close the shared memory
 *
 * \param  region[OUT]    Pointer to region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_close_shm(region_list_t *region)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (region->is_shm_closed == 1) {
        goto done;
    }

    if (region == NULL || region->buf == NULL) {
        printf("==PDC_SERVER[%d]: %s - NULL input\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    /* remove the mapped memory segment from the address space of the process */
    if (munmap(region->buf, region->data_size) == -1) {
        printf("==PDC_SERVER[%d]: %s - unmap failed\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    /* close the shared memory segment as if it was a file */
    if (close(region->shm_fd) == -1) {
        printf("==PDC_SERVER[%d]: close shm_fd failed\n", pdc_server_rank_g);
        ret_value = FAIL;
        goto done;
    }

    /* remove the shared memory segment from the file system */
    if (shm_unlink(region->shm_addr) == -1) {
        ret_value = FAIL;
        goto done;
        printf("==PDC_SERVER[%d]: Error removing %s\n", pdc_server_rank_g, region->shm_addr);
    }

    region->is_shm_closed = 1;

    /* printf("==PDC_SERVER[%d]: shm %s closed\n", pdc_server_rank_g, region->shm_addr); */
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function for IO complete notification send to client, gets output from client
 *
 * \param  callback_info[IN]    Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t PDC_Server_notify_io_complete_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;

    FUNC_ENTER(NULL);

    server_lookup_args_t *lookup_args = (server_lookup_args_t*) callback_info->arg;
    hg_handle_t handle = callback_info->info.forward.handle;

    /* Get output from server*/
    notify_io_complete_out_t output;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_cb to client %"PRIu32 " "
                "- error with HG_Get_output\n", pdc_server_rank_g, lookup_args->client_id);
        lookup_args->ret_int = -1;
        goto done;
    }

    if (is_debug_g ) {
        printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_cb - received from client %d with %d\n", 
                pdc_server_rank_g, lookup_args->client_id, output.ret);
    }
    lookup_args->ret_int = output.ret;

done:
    /* pdc_to_client_work_todo_g = 0; */
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function for IO complete notification send to client
 *
 * \param  client_id[IN]    Target client's MPI rank
 * \param  obj_id[IN]       Object ID 
 * \param  shm_addr[IN]     Server's shared memory address  
 * \param  io_typ[IN]       IO type (read/write)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_notify_io_complete_to_client(uint32_t client_id, uint64_t obj_id, 
        char* shm_addr, PDC_access_t io_type)
{
    char tmp_shm[50];
    perr_t ret_value   = SUCCEED;
    hg_return_t hg_ret = HG_SUCCESS;
    server_lookup_args_t lookup_args;
    hg_handle_t notify_io_complete_handle;

    FUNC_ENTER(NULL);

    if (client_id >= (uint32_t)pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_to_client() - "
                "client_id %d invalid)\n", pdc_server_rank_g, client_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g[client_id].addr_valid != 1) {
        ret_value = PDC_Server_lookup_client(client_id);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_to_client() - "
                    "PDC_Server_lookup_client failed)\n", pdc_server_rank_g);
            goto done;
        }
    }

    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr, 
                notify_io_complete_register_id_g, &notify_io_complete_handle);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_to_client() - "
                "HG_Create failed)\n", pdc_server_rank_g);
        ret_value = FAIL;
        goto done;
    }

    // Fill input structure
    notify_io_complete_in_t in;
    in.obj_id     = obj_id;
    in.io_type    = io_type;
    if (shm_addr[0] == 0) {
        sprintf(tmp_shm, "%d", client_id * 10);
        in.shm_addr   = tmp_shm;
        /* in.shm_addr   = " "; */
    }
    else 
        in.shm_addr   = shm_addr;

    if (is_debug_g ) {
        /* printf("==PDC_SERVER: PDC_Server_notify_io_complete_to_client shm_addr = [%s]\n", in.shm_addr); */
        printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_to_client %d \n", pdc_server_rank_g, client_id);
        fflush(stdout);
    }
    
    /* printf("Sending input to target\n"); */
    lookup_args.client_id = client_id;
    hg_ret = HG_Forward(notify_io_complete_handle, PDC_Server_notify_io_complete_cb, &lookup_args, &in);

    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "PDC_Server_notify_io_complete_to_client(): Could not start HG_Forward()\n");
        ret_value = FAIL;
        goto done;
    }

    /* if (is_debug_g) { */
    /*     printf("==PDC_SERVER[%d]: forwarded to client %d!\n", pdc_server_rank_g, client_id); */
    /* } */

done:
    fflush(stdout);
    hg_ret = HG_Destroy(notify_io_complete_handle);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_to_client() - "
                "HG_Destroy(notify_io_complete_handle) error!\n", pdc_server_rank_g);
    }
    FUNC_LEAVE(ret_value);
}

/*
 * Check if a previous read request has been completed
 *
 * \param  in[IN]       Input structure received from client containing the IO request info
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_read_check(data_server_read_check_in_t *in, data_server_read_check_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_data_server_io_list_t *io_elt = NULL, *io_target = NULL;
    region_list_t *region_elt = NULL, *region_tmp = NULL;
    region_list_t r_target;
    /* uint32_t i; */

    FUNC_ENTER(NULL);

    pdc_metadata_t meta;
    PDC_metadata_init(&meta);
    pdc_transfer_t_to_metadata_t(&in->meta, &meta);

    PDC_init_region_list(&r_target);
    pdc_region_transfer_t_to_list_t(&(in->region), &r_target);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_read_list_mutex_g);
#endif
    // FIXME:  need to find a better place to remove shm
    // Iterate io list, find current request
    DL_FOREACH(pdc_data_server_read_list_head_g, io_elt) {
        if (meta.obj_id == io_elt->obj_id) {
            io_target = io_elt;
            break;
        }
        else {
            // Only close the shm that is in the list before current reqd check request
            // as we know for sure that the clinet has copied the data in shm already
            // this way we can correctly release the shm that has been used
            if (io_elt->is_shm_closed != 1) {
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
                io_elt->is_shm_closed = 1;
            }
        }
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_read_list_mutex_g);
#endif

    if (NULL == io_target) {
        printf("==PDC_SERVER[%d]: %s - No existing io request with same obj_id %" PRIu64 " found!\n", 
                    pdc_server_rank_g, __func__, meta.obj_id);
        out->ret = -1;
        out->shm_addr = " ";
        goto done;
    }


    if (is_debug_g) {
        printf("==PDC_SERVER[%d]: Read check Obj [%s] id=%" PRIu64 "  region: start(%" PRIu64 ", %" PRIu64 ") "
                "size(%" PRIu64 ", %" PRIu64 ") \n", pdc_server_rank_g, meta.obj_name,
                meta.obj_id, r_target.start[0], r_target.start[1], r_target.count[0], r_target.count[1]);
    }

    /* int count = 0; */
    /* if (is_debug_g == 1) { */
    /*     DL_COUNT(io_target->region_list_head, region_elt, count); */
    /*     printf("==PDC_SERVER[%d]: current region list count: %d\n", pdc_server_rank_g, count); */
    /*     PDC_print_region_list(io_target->region_list_head); */
    /* } */

    int found_region = 0;
    DL_FOREACH_SAFE(io_target->region_list_head, region_elt, region_tmp) {
        if (region_list_cmp(region_elt, &r_target) == 0) {
            // Found previous io request
            found_region = 1;
            out->ret = region_elt->is_data_ready;
            out->shm_addr = " ";
            /* printf("==PDC_SERVER: found IO request region\n"); */
            // NOTE: Similar to write check, we don't want to remove the region info, 
            //       as it will be used to release the shm at a later time.
            if (region_elt->is_data_ready == 1) {
                out->shm_addr = calloc(sizeof(char), ADDR_MAX);
                strcpy(out->shm_addr, region_elt->shm_addr);
                /* printf("==PDC_SERVER[%d]: remove a read request region, shm_addr=[%s]\n", */
                /*         pdc_server_rank_g, region_elt->shm_addr); */
                /* DL_DELETE(io_target->region_list_head, region_elt); */
                /* free(region_elt); */
            }

            ret_value = SUCCEED;
            goto done;

            // TODO: may also want to free the io_target if there is no
            //       region in its list
        }
    }


    if (found_region == 0) {
        printf("==PDC_SERVER[%d]: %s -  No io request with same region found!\n", pdc_server_rank_g, __func__);
        PDC_print_region_list(&r_target);
        out->ret = -1;
        out->shm_addr = " ";
        goto done;
    }

done:
    // TODO remove the item in pdc_data_server_read_list_head_g after the request is fulfilled
    //      at object close time?
    // TODO server needs to remove the shm after client finished reading from it
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // End of PDC_Server_read_check

/*
 * Check if a previous write request has been completed
 *
 * \param  in[IN]       Input structure received from client containing the IO request info
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out)
{
    perr_t ret_value = FAIL;
    pdc_data_server_io_list_t *io_elt = NULL, *io_target = NULL;
    region_list_t *region_elt = NULL, *region_tmp = NULL;
    /* int i; */

    FUNC_ENTER(NULL);

    pdc_metadata_t meta;
    PDC_metadata_init(&meta);
    pdc_transfer_t_to_metadata_t(&in->meta, &meta);

    region_list_t r_target;
    PDC_init_region_list(&r_target);
    pdc_region_transfer_t_to_list_t(&(in->region), &r_target);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_write_list_mutex_g);
#endif
    // Iterate io list, find current request
    DL_FOREACH(pdc_data_server_write_list_head_g, io_elt) {
        if (meta.obj_id == io_elt->obj_id) {
            io_target = io_elt;
            break;
        }
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_write_list_mutex_g);
#endif

    // If not found, create and insert one to the list
    if (NULL == io_target) {
        printf("==PDC_SERVER: No existing io request with same obj_id found!\n");
        out->ret = -1;
        ret_value = SUCCEED;
        goto done;
    }

    /* printf("%d region: start(%" PRIu64 ", %" PRIu64 ") size(%" PRIu64 ", %" PRIu64 ") \n", r_target.start[0], r     _target.start[1], r_target.count[0], r_target.count[1]); */
    int found_region = 0;
    DL_FOREACH_SAFE(io_target->region_list_head, region_elt, region_tmp) {
        if (region_list_cmp(region_elt, &r_target) == 0) {
            // Found io list
            found_region = 1;
            out->ret = region_elt->is_data_ready;
            /* printf("==PDC_SERVER: found IO request region\n"); */
            // NOTE: We don't want to remove the region here as this check request may come before
            //       the region is used to update the corresponding region metadata in remote server.
            // TODO: Instead, the region should be deleted some time after, when we know the region
            //       is no longer needed. But this could be tricky, as we don't know if the client 
            //       want to read the data multliple times, so the best time is when the server recycles
            //       the shm associated with this region.
            // TODO: Need to add shm and region free functions to release memory
            /* if (region_elt->is_data_ready == 1) { */
            /*     DL_DELETE(io_target->region_list_head, region_elt); */
            /*     free(region_elt); */
            /* } */
            ret_value = SUCCEED;
            goto done;
        }
    }

    if (found_region == 0) {
        printf("==PDC_SERVER: No existing IO request of requested region found!\n");
        out->ret = -1;
        ret_value = SUCCEED;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} //PDC_Server_write_check

/*
 * Read the requested data to shared memory address
 *
 * \param  region_list_head[IN]       List of IO request to be performed
 * \param  obj_id[IN]                 Object ID of the IO request
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_read_to_shm(region_list_t *region_list_head, uint64_t obj_id)
{
    perr_t ret_value = FAIL;
    region_list_t *elt;
    /* region_list_t *tmp; */
    /* region_list_t *merged_list = NULL; */

    FUNC_ENTER(NULL);

    is_server_direct_io_g = 0;
    // TODO: merge regions for aggregated read
    // Merge regions
    /* PDC_Server_merge_region_list_naive(region_list_head, &merged_list); */

    // Replace region_list with merged list
    // FIXME: seems there is something wrong with sort, list gets corrupted!
    /* DL_SORT(region_list_head, region_list_cmp_by_client_id); */
    /* DL_FOREACH_SAFE(region_list_head, elt, tmp) { */
    /*     DL_DELETE(region_list_head, elt); */
    /*     free(elt); */
    /* } */
    /* region_list_head = merged_list; */

    /* printf("==PDC_SERVER: after merge\n"); */
    /* DL_FOREACH(merged_list, elt) { */
    /*     PDC_print_region_list(elt); */
    /* } */
    /* fflush(stdout); */

    // Now we have a -merged- list of regions to be read,
    // so just read one by one

    // Prepare the shared memory for transfer back to client
    DL_FOREACH(region_list_head, elt) {

        elt->data_size = elt->count[0];
        size_t i = 0;
        for (i = 1; i < elt->ndim; i++)
            elt->data_size *= elt->count[i];

        // Get min max client ID
        uint32_t client_id_min = elt->client_ids[0];
        uint32_t client_id_max = elt->client_ids[0];
        for (i = 1; i < PDC_SERVER_MAX_PROC_PER_NODE; i++) {
            if (elt->client_ids[i] == 0)
                break;
            client_id_min = elt->client_ids[i] < client_id_min ? elt->client_ids[i] : client_id_min;
            client_id_max = elt->client_ids[i] > client_id_max ? elt->client_ids[i] : client_id_max;
        }

        int rnd = rand();
        // Shared memory address is /objID_ServerID_ClientIDmin_to_ClientIDmax_rand
        sprintf(elt->shm_addr, "/%" PRIu64 "_s%d_c%dto%d_%d", obj_id, pdc_server_rank_g,
                                             client_id_min, client_id_max, rnd);

        /* create the shared memory segment as if it was a file */
        /* printf("==PDC_SERVER: creating share memory segment with address [%s]\n", elt->shm_addr); */
        elt->shm_fd = shm_open(elt->shm_addr, O_CREAT | O_RDWR, 0666);
        if (elt->shm_fd == -1) {
            printf("==PDC_SERVER: Shared memory shm_open failed\n");
            ret_value = FAIL;
            goto done;
        }

        /* configure the size of the shared memory segment */
        ftruncate(elt->shm_fd, elt->data_size);

        /* map the shared memory segment to the address space of the process */
        elt->buf = mmap(0, elt->data_size, PROT_READ | PROT_WRITE, MAP_SHARED, elt->shm_fd, 0);
        if (elt->buf == MAP_FAILED) {
            printf("==PDC_SERVER: Shared memory mmap failed\n");
            // close and shm_unlink?
            ret_value = FAIL;
            goto done;
        }
    } // DL_FOREACH

    // POSIX read for now
    ret_value = PDC_Server_regions_io(region_list_head, POSIX);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: error reading data from storage and create shared memory\n", pdc_server_rank_g);
        goto done;
    }

    ret_value = SUCCEED;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} //PDC_Server_data_read_to_shm

/*
 * Get the storage location of a region from local metadata hash table
 *
 * \param  obj_id[IN]               Object ID of the request
 * \param  region[IN]               Request region
 * \param  n_loc[OUT]               Number of storage locations of the target region
 * \param  overlap_region_loc[OUT]  List of region locations
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_local_storage_location_of_region(uint64_t obj_id, region_list_t *region,
        uint32_t *n_loc, region_list_t **overlap_region_loc)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *target_meta = NULL;
    region_list_t  *region_elt = NULL;

    FUNC_ENTER(NULL);

    // Find object metadata
    *n_loc = 0;
    target_meta = find_metadata_by_id(obj_id);
    if (target_meta == NULL) {
        printf("==PDC_SERVER[%d]: find_metadata_by_id FAILED!\n", pdc_server_rank_g);
        ret_value = FAIL;
        goto done;
    }
    DL_FOREACH(target_meta->storage_region_list_head, region_elt) {
        if (is_contiguous_region_overlap(region_elt, region) == 1) {
            // No need to make a copy here, but need to make sure we won't change it
            /* PDC_init_region_list(overlap_region_loc[*n_loc]); */
            /* pdc_region_list_t_deep_cp(region_elt, overlap_region_loc[*n_loc]); */
            overlap_region_loc[*n_loc] = region_elt;
            *n_loc += 1;
        }
        /* PDC_print_storage_region_list(region_elt); */
        if (*n_loc > PDC_MAX_OVERLAP_REGION_NUM) {
            printf("==PDC_SERVER[%d]: %s- exceeding PDC_MAX_OVERLAP_REGION_NUM regions!\n", 
                    pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }
    } // DL_FOREACH

    if (*n_loc == 0) {
        printf("==PDC_SERVER[%d]: %s - no overlapping storage region found\n", 
                pdc_server_rank_g, __func__);
        PDC_print_region_list(region);
        ret_value = FAIL;
        goto done;
    }


done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_get_local_storage_location_of_region

/*
 * Callback function to cleanup after storage meta bulk update
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t update_region_storage_meta_bulk_cleanup_cb(update_storage_meta_list_t *meta_list_target, int *n_updated)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    (*n_updated)++;

    DL_DELETE(pdc_update_storage_meta_list_head_g, meta_list_target);
    free(meta_list_target->storage_meta_bulk_xfer_data->buf_ptrs[0]);
    free(meta_list_target->storage_meta_bulk_xfer_data->buf_sizes);
    free(meta_list_target->storage_meta_bulk_xfer_data);
    free(meta_list_target);

    if (NULL == pdc_update_storage_meta_list_head_g) {
        pdc_nbuffered_bulk_update_g = 0;
        pdc_buffered_bulk_update_total_g = 0;
        /* pdc_update_storage_meta_list_head_g = NULL; */
        n_check_write_finish_returned_g = 0;
    }

    FUNC_LEAVE(ret_value);
}

/*
 * Update storage meta.
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t PDC_Server_update_storage_meta(int *n_updated)
{
    perr_t ret_value;
    update_storage_meta_list_t *meta_list_elt;

    FUNC_ENTER(NULL);
    /* int update_cnt; */
    /* DL_COUNT(pdc_update_storage_meta_list_head_g, meta_list_elt, update_cnt); */
    /* printf("==PDC_SERVER[%d]: finish %d write check confirms, start %d bulk update storage meta!\n", */
    /*         pdc_server_rank_g, n_check_write_finish_returned_g, update_cnt); */
    /* fflush(stdout); */

    // FIXME: There is a problem with the following update via mercury  
    //        It would hang at iteration 5, as one or a couple of servers does not
    //        proceed after server respond its bulk rpc
    //        So switch to MPI approach 
    /* DL_FOREACH(pdc_update_storage_meta_list_head_g, meta_list_elt) { */

    /*     // NOTE: barrier is used for faster client response, all server respond the client */ 
    /*     //       write check request first before starting the update process */
    /*     // Do the actual update when reaches client set threshold */
    /*     printf("==PDC_SERVER[%d]: Before Barrier iter #%d!\n", pdc_server_rank_g, n_updated); */
    /*     fflush(stdout); */

    /*     #ifdef ENABLE_MPI */
    /*     MPI_Barrier(MPI_COMM_WORLD); */
    /*     #endif */

    /*     printf("==PDC_SERVER[%d]: start #%d bulk updates!\n", pdc_server_rank_g, n_updated); */
    /*     fflush(stdout); */

    /*     ret_value=PDC_Server_update_region_storage_meta_bulk(meta_list_elt->storage_meta_bulk_xfer_data); */
    /*     if (ret_value != SUCCEED) { */
    /*         printf("==PDC_SERVER[%d]: %s - update storage info FAILED!", pdc_server_rank_g, __func__); */
    /*         goto done; */
    /*     } */
    /*     n_updated++; */
    /* } */
    // MPI Reduce implementation of metadata update
    *n_updated = 0;
    DL_FOREACH(pdc_update_storage_meta_list_head_g, meta_list_elt) {

        ret_value = PDC_Server_update_region_storage_meta_bulk_with_cb(meta_list_elt->storage_meta_bulk_xfer_data,
                    update_region_storage_meta_bulk_cleanup_cb, meta_list_elt, n_updated);
        /* ret_value=PDC_Server_update_region_storage_meta_bulk_mpi(meta_list_elt->storage_meta_bulk_xfer_data); */
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - update storage info FAILED!", pdc_server_rank_g, __func__);
            goto done;
        }
    }

    /* // Free allocated space */
    /* DL_FOREACH_SAFE(pdc_update_storage_meta_list_head_g, meta_list_elt, meta_list_tmp) { */
    /*     DL_DELETE(pdc_update_storage_meta_list_head_g, meta_list_elt); */
    /*     free(meta_list_elt->storage_meta_bulk_xfer_data->buf_ptrs[0]); */
    /*     free(meta_list_elt->storage_meta_bulk_xfer_data->buf_sizes); */
    /*     free(meta_list_elt->storage_meta_bulk_xfer_data); */
    /*     free(meta_list_elt); */
    /* } */

    /* // Reset the values */
    /* pdc_nbuffered_bulk_update_g = 0; */
    /* pdc_buffered_bulk_update_total_g = 0; */
    /* pdc_update_storage_meta_list_head_g = NULL; */
    /* n_check_write_finish_returned_g = 0; */

done:
    FUNC_LEAVE(ret_value);
} // end PDC_Server_update_storage_meta


/*
 * Callback function for get storage info.
 *
 * \param  callback_info[IN]         Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
hg_return_t PDC_Server_count_write_check_update_storage_meta_cb (const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    data_server_write_check_out_t *write_ret;
    int n_updated = 0;

    FUNC_ENTER(NULL);

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
    #endif


    write_ret = (data_server_write_check_out_t*) callback_info->arg;

    if (write_ret->ret == 1) {
        n_check_write_finish_returned_g++;
    
        if (n_check_write_finish_returned_g >= pdc_buffered_bulk_update_total_g) {

            /* printf("==PDC_SERVER[%d]: returned %d write checks!\n", pdc_server_rank_g, pdc_buffered_bulk_update_total_g); */
            /* fflush(stdout); */
            ret_value = PDC_Server_update_storage_meta(&n_updated);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - FAILED to update storage meta\n", pdc_server_rank_g, __func__);
                goto done;
            }

        } // end of if
    } // end of if (write_ret->ret == 1)


    /* printf("==PDC_SERVER[%d]: returned write finish to client, %d total!\n", */ 
    /*         pdc_server_rank_g, n_check_write_finish_returned_g); */
done:

    #ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    double update_region_location_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    server_update_region_location_time_g += update_region_location_time;
    /* if (n_updated > 0) { */
    /*     printf("==PDC_SERVER[%d]: updated %d bulk meta time %.6f!\n", */
    /*             pdc_server_rank_g, n_updated, update_region_location_time); */
    /* } */
    #endif

    if (write_ret) 
        free(write_ret);
    fflush(stdout);
    
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function for get storage info.
 *
 * \param  callback_info[IN]         Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
PDC_Server_get_storage_info_cb (const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    hg_handle_t handle;
    pdc_serialized_data_t serialized_output;
    get_storage_info_single_out_t single_output;
    uint32_t n_loc;
    int is_single_region = 1;
    get_storage_loc_args_t *cb_args;

    FUNC_ENTER(NULL);

    cb_args = (get_storage_loc_args_t*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &single_output);
    if (ret_value != HG_SUCCESS) {
        // Returned multiple regions
        // TODO: test this
        is_single_region = 0;
        ret_value = HG_Get_output(handle, &serialized_output);
        if (ret_value != HG_SUCCESS) {
            printf("==PDC_SERVER[%d]: %s - ERROR HG_Get_output\n", pdc_server_rank_g, __func__);
            cb_args->void_buf = NULL;
            ret_value = FAIL;
            goto done;
        } // end if 
        else {
            if(PDC_unserialize_region_lists(serialized_output.buf,cb_args->region_lists,&n_loc)!= SUCCEED) {
                printf("==PDC_SERVER[%d]: ERROR with PDC_unserialize_region_lists, buf size %lu\n",
                                      pdc_server_rank_g, strlen((char*)serialized_output.buf));
                *(cb_args->n_loc) = 0;
                ret_value = FAIL;
                goto done;
            }

            *(cb_args->n_loc) = n_loc;
            cb_args->void_buf = serialized_output.buf;
            if (is_debug_g == 1) {
                printf(  "==PDC_SERVER[%d]: %s - : received %ls storage info\n",
                        pdc_server_rank_g, __func__, cb_args->n_loc);
            }
        } // end of else (with multiple storage regions)
    }
    else {
        // with only one matching storage region
        *(cb_args->n_loc) = 1;
        pdc_region_transfer_t_to_list_t(&single_output.region_transfer, cb_args->region_lists[0]);
        strcpy(cb_args->region_lists[0]->storage_location, single_output.storage_loc);
        cb_args->region_lists[0]->offset = single_output.file_offset;
    }

    /* if (is_debug_g == 1) { */
    /*     printf("==PDC_SERVER[%d]: got %u locations of region\n", */
    /*             pdc_server_rank_g, *cb_args->n_loc); */
    /*     fflush(stdout); */
    /* } */


done:
    if (cb_args->cb != NULL) 
        cb_args->cb(cb_args->cb_args);
    
    fflush(stdout);
    /* s2s_send_work_todo_g--; */
    if (is_single_region == 1) 
        HG_Free_output(handle, &single_output);
    else
        HG_Free_output(handle, &serialized_output);
    
    FUNC_LEAVE(ret_value);
}
/* Use this callback to decrease the s2s_recv_work_todo_g */
hg_return_t PDC_Server_s2s_recv_work_done_cb(const struct hg_cb_info *callback_info)
{
    // TODO: add mutex when multi threading
    /* s2s_recv_work_todo_g--; */

    /* printf("==PDC_SERVER[%d]: s2s_recv_work_done!\n", pdc_server_rank_g); */
    /* fflush(stdout); */
    return HG_SUCCESS;
}

/*
 * Get the storage location of a region from (possiblly remote) metadata hash table
 *
 * \param  region[IN/OUT]               Request region
 *
 * \return Non-negative on success/Negative on failure
 */
// Note: one request region can spread across multiple regions in storage
// Need to allocate **overlap_region_loc with PDC_MAX_OVERLAP_REGION_NUM before calling this 
perr_t PDC_Server_get_storage_location_of_region_mpi(region_list_t *regions_head)
{
    perr_t ret_value = SUCCEED;
    uint32_t server_id = 0;
    uint32_t i, j;
    pdc_metadata_t *region_meta = NULL, *region_meta_prev = NULL;
    region_list_t *region_elt, req_region, **overlap_regions_2d = NULL;
    region_info_transfer_t local_region_transfer[PDC_SERVER_MAX_PROC_PER_NODE];
    region_info_transfer_t *all_requests = NULL;
    update_region_storage_meta_bulk_t *send_buf = NULL;
    update_region_storage_meta_bulk_t *result_storage_meta = NULL;
    uint32_t total_request_cnt;
    int data_size    = sizeof(region_info_transfer_t);
    /* int all_meta_cnt = meta_cnt * pdc_server_size_g; */
    int *send_bytes = NULL;
    int *displs = NULL;
    int *request_overlap_cnt = NULL;
    int nrequest_per_server = 0;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    if (regions_head == NULL) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    nrequest_per_server = 0;
    region_meta_prev = regions_head->meta;
    DL_FOREACH(regions_head, region_elt) {
        if (region_elt->access_type == READ) {
        
            region_meta = region_elt->meta;
            // All requests should point to the same metadata
            if (region_meta == NULL) {
                printf("==PDC_SERVER[%d]: %s - request region has NULL metadata!\n", 
                        pdc_server_rank_g, __func__);
                ret_value = FAIL;
                goto done;
            }
            else if (region_meta->obj_id != region_meta_prev->obj_id) {
                printf("==PDC_SERVER[%d]: %s - request regions are of different object!\n", 
                        pdc_server_rank_g, __func__);
                ret_value = FAIL;
                goto done;
            }
            region_meta_prev = region_meta;

            // nrequest_per_server should be less than PDC_SERVER_MAX_PROC_PER_NODE
            // and should be the same across all servers.
            if (nrequest_per_server > PDC_SERVER_MAX_PROC_PER_NODE) {
                printf("==PDC_SERVER[%d]: %s - more requests than expected! "
                       "Increase PDC_SERVER_MAX_PROC_PER_NODE!\n", pdc_server_rank_g, __func__);
                fflush(stdout);
            }
            else {
                // After saninty check, add the current request to gather send buffer
                pdc_region_list_t_to_transfer(region_elt, &local_region_transfer[nrequest_per_server]);
                nrequest_per_server++;
            }
        }
    } // end of DL_FOREACH

    // NOTE: Assume nrequest_per_server are the same across all servers
    /* printf("==PDC_SERVER[%d]: %s - %d requests!\n", pdc_server_rank_g, __func__, nrequest_per_server); */

    server_id = PDC_get_server_by_obj_id(region_meta->obj_id, pdc_server_size_g);

    // Only recv server needs allocation
    if (server_id == (uint32_t)pdc_server_rank_g) {
        all_requests = (region_info_transfer_t*)calloc(pdc_server_size_g, 
                                                   data_size*nrequest_per_server);
    }

    /* printf("==PDC_SERVER[%d]: will MPI_Reduce update region to server %d, with data size %d\n", */
    /*         pdc_server_rank_g, server_id, data_size); */
    /* fflush(stdout); */

    // Gather the requests from all data servers to metadata server
    // equivalent to all data server send requests to metadata server
    MPI_Gather(&local_region_transfer, nrequest_per_server*data_size, MPI_CHAR, 
               all_requests, nrequest_per_server*data_size, MPI_CHAR, server_id, MPI_COMM_WORLD);

    // NOTE: Assumes all data server send equal number of requests
    total_request_cnt   = nrequest_per_server * pdc_server_size_g;
    send_bytes          = (int*)calloc(sizeof(int), pdc_server_size_g);
    displs              = (int*)calloc(sizeof(int), pdc_server_size_g);
    request_overlap_cnt = (int*)calloc(total_request_cnt, sizeof(int));
    // ^storage meta results in all_requests to be returned to all data servers

    // Now server_id has all the data in all_requests, find all storage regions that overlaps with it
    // equivalent to storage metadadata searching
    if (server_id == (uint32_t)pdc_server_rank_g) {

        /* char                        storage_location[ADDR_MAX]; */
        /* uint64_t                    offset; */
        /* } update_region_storage_meta_bulk_t; */
        // send_buf will have the all results from all node-local client requests, need enough space
        send_buf = (update_region_storage_meta_bulk_t*)calloc(sizeof(update_region_storage_meta_bulk_t), 
                                 pdc_server_size_g*nrequest_per_server*PDC_MAX_OVERLAP_REGION_NUM);

        // All participants are querying the same object, so obj_ids are the same
        // Search one by one
        int found_cnt = 0;
        uint32_t overlap_cnt = 0;
        int server_idx;
        // overlap_regions_2d has the ptrs to overlap storage regions to current request
        overlap_regions_2d = (region_list_t**)calloc(sizeof(region_list_t*), PDC_MAX_OVERLAP_REGION_NUM);
        for (i = 0; i < total_request_cnt; i++) {
            server_idx = i/nrequest_per_server;
            // server_idx should be [0, pdc_server_size_g)
            if (server_idx < 0 || server_idx >= pdc_server_size_g) {
                printf("==PDC_SERVER[%d]: %s - ERROR with server idx count!\n", pdc_server_rank_g, __func__);
                ret_value = FAIL;
                goto done;
            }
            memset(&req_region, 0, sizeof(region_list_t));
            pdc_region_transfer_t_to_list_t(&all_requests[i], &req_region);
            ret_value = PDC_Server_get_local_storage_location_of_region(region_meta->obj_id, &req_region, 
                                                                        &overlap_cnt, overlap_regions_2d);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - unable to get local storage location!\n", 
                        pdc_server_rank_g, __func__);
                goto done;
            }

            if (overlap_cnt > PDC_MAX_OVERLAP_REGION_NUM) {
                printf("==PDC_SERVER[%d]: %s - found %d storage locations than PDC_MAX_OVERLAP_REGION_NUM!\n", 
                        pdc_server_rank_g, __func__, overlap_cnt);
                overlap_cnt = PDC_MAX_OVERLAP_REGION_NUM;
                fflush(stdout);
            }

            // Record how many overlap regions for each request
            request_overlap_cnt[i] = overlap_cnt;

            // Record the total size to send to each data server
            send_bytes[server_idx] += overlap_cnt * sizeof(update_region_storage_meta_bulk_t);

            // Record the displacement in the overall result array
            if (server_idx  > 0) 
                displs[server_idx] = displs[server_idx-1] + send_bytes[server_idx-1];
            
            /* printf("==PDC_SERVER[%d]: %s - found %d overlap storage meta!\n", */ 
            /*         pdc_server_rank_g, __func__, overlap_cnt); */
            /* fflush(stdout); */
            // for each request, copy all overlap storage meta to send buf 
            for (j = 0; j < overlap_cnt; j++) {
                pdc_region_list_t_to_transfer(overlap_regions_2d[j], &send_buf[found_cnt].region_transfer);
                strcpy(send_buf[found_cnt].storage_location, overlap_regions_2d[j]->storage_location);
                send_buf[found_cnt].offset = overlap_regions_2d[j]->offset;
                found_cnt++;
            }

        } // end for

    } // end of if (current server is metadata server)

    // send_bytes[] / sizeof(update_region_storage_meta_bulk_t) is the number of regions for each data server
    // Now we have all the overlapping storage meta in the send buf, indexed by send_bytes
    // for each requested server, just scatter

    // result_storage_meta is the recv result storage metadata of the requests sent
    result_storage_meta = (update_region_storage_meta_bulk_t*)calloc(sizeof(update_region_storage_meta_bulk_t), 
                                                             nrequest_per_server*PDC_MAX_OVERLAP_REGION_NUM);

    MPI_Bcast(request_overlap_cnt, total_request_cnt, MPI_INT, server_id, MPI_COMM_WORLD);
    MPI_Bcast(send_bytes, pdc_server_size_g, MPI_INT, server_id, MPI_COMM_WORLD);
    MPI_Bcast(displs, pdc_server_size_g, MPI_INT, server_id, MPI_COMM_WORLD);

    MPI_Scatterv(send_buf, send_bytes, displs, MPI_CHAR, 
                 result_storage_meta, send_bytes[pdc_server_rank_g], MPI_CHAR, 
                 server_id, MPI_COMM_WORLD);

    // result_storage_meta has all the request storage region location for requests
    // Put results to the region lists
    int overlap_idx = pdc_server_rank_g * nrequest_per_server;
    int region_idx = 0;
    int result_idx = 0;
    // We will have the same linked list traversal order as before, so no need to check region match
    DL_FOREACH(regions_head, region_elt) {
        if (region_elt->access_type == READ) {
            region_elt->n_overlap_storage_region = request_overlap_cnt[overlap_idx + region_idx];
            region_elt->overlap_storage_regions = (region_list_t*)calloc(sizeof(region_list_t), 
                                                          region_elt->n_overlap_storage_region);
            for (i = 0; i < region_elt->n_overlap_storage_region; i++) {
                pdc_region_transfer_t_to_list_t(&result_storage_meta[result_idx].region_transfer, 
                                                &region_elt->overlap_storage_regions[i]);
                strcpy(region_elt->overlap_storage_regions[i].storage_location, 
                        result_storage_meta[result_idx].storage_location);
                region_elt->overlap_storage_regions[i].offset = result_storage_meta[result_idx].offset;
                result_idx++;
            }

            region_idx++;
        }
    }

    // NOTE: overlap_storage_regions will be freed after read in the caller function

done:
    /* MPI_Barrier(MPI_COMM_WORLD); */

    if (server_id == (uint32_t)pdc_server_rank_g) {
        if (all_requests) free(all_requests);
        if (overlap_regions_2d) free(overlap_regions_2d);
        if (send_buf) free(send_buf);
    }
    if (result_storage_meta) free(result_storage_meta);
    if (send_bytes) free(send_bytes);
    if (displs) free(displs);
    if (request_overlap_cnt) free(request_overlap_cnt);
#else
    printf("%s - is not supposed to be called without MPI enabled!\n", __func__);
#endif

    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_Server_get_storage_location_of_region_mpi


/*
 * Get the storage location of a region from (possiblly remote) metadata hash table
 *
 * \param  request_region[IN]       Request region
 * \param  n_loc[OUT]               Number of storage locations of the target region
 * \param  overlap_region_loc[OUT]  List of region locations
 * \param  cb[IN]                   Callback function pointer to be performed after getting the storage loc
 * \param  cb_args[IN]              Callback function parameters
 *
 * \return Non-negative on success/Negative on failure
 */
// Note: one request region can spread across multiple regions in storage
// Need to allocate **overlap_region_loc with PDC_MAX_OVERLAP_REGION_NUM before calling this 
perr_t PDC_Server_get_storage_location_of_region_with_cb(region_list_t *request_region, uint32_t *n_loc, 
                                                         region_list_t **overlap_region_loc, 
                                                         perr_t (*cb)(), void *args)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    uint32_t server_id = 0;
    pdc_metadata_t *region_meta = NULL;
    get_storage_info_in_t in;
    hg_handle_t get_storage_info_handle;
    get_storage_loc_args_t cb_args;

    FUNC_ENTER(NULL);

    if (request_region == NULL || overlap_region_loc == NULL || overlap_region_loc[0] == NULL || n_loc == NULL) {
        printf("==PDC_SERVER[%d]: %s () input has NULL value!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    region_meta = request_region->meta;
    if (region_meta == NULL) {
        printf("==PDC_SERVER[%d]: %s - request region has NULL metadata\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }
    server_id = PDC_get_server_by_obj_id(region_meta->obj_id, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: get storage region info from local server\n",pdc_server_rank_g);
        }
        // Metadata object is local, no need to send update RPC
        ret_value = PDC_Server_get_local_storage_location_of_region(region_meta->obj_id,
                        request_region, n_loc, overlap_region_loc);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - get local storage location ERROR!\n", pdc_server_rank_g, __func__);
            goto done;
        }

        // Execute callback function immediately
        if (cb != NULL) {
            cb(args);
        }
    } // end if
    else {
        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: will get storage region info from remote server %d\n",
                    pdc_server_rank_g, server_id);
            fflush(stdout);
        }

        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                    pdc_server_rank_g, server_id);
            ret_value = FAIL;
            goto done;
        }

        HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_storage_info_register_id_g,
                  &get_storage_info_handle);


        if (request_region->meta == NULL) {
            printf("==PDC_SERVER: NULL request_region->meta");
            ret_value = FAIL;
            goto done;
        }
        in.obj_id = request_region->meta->obj_id;
        pdc_region_list_t_to_transfer(request_region, &in.req_region);

        cb_args.cb   = cb;
        cb_args.cb_args = args;
        cb_args.region_lists = overlap_region_loc;
        cb_args.n_loc = n_loc;

        hg_ret = HG_Forward(get_storage_info_handle, PDC_Server_get_storage_info_cb, &cb_args, &in);
        if (hg_ret != HG_SUCCESS) {
            printf("PDC_Client_update_metadata_with_name(): Could not start HG_Forward() to server %u\n",
                        server_id);
            HG_Destroy(get_storage_info_handle);
            ret_value = FAIL;
            goto done;
        }
        HG_Destroy(get_storage_info_handle);
    } // end else

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_Server_get_storage_location_of_region_with_cb

/*
 * Perform the IO request with different IO system
 * after the server has accumulated requests from all node local clients
 *
 * \param  region_list_head[IN]     List of IO requests
 * \param  plugin[IN]               IO system to be used
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_regions_io(region_list_t *region_list_head, PDC_io_plugin_t plugin)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
    #endif


    // If read, need to get locations from metadata server
    if (plugin == POSIX) {
        ret_value = PDC_Server_posix_one_file_io(region_list_head);
        if (ret_value !=  SUCCEED) {
            printf("==PDC_SERVER[%d]: %s-error with PDC_Server_posix_one_file_io\n", pdc_server_rank_g, __func__);
            goto done;
        }
    }
    else if (plugin == DAOS) {
        printf("DAOS plugin in under development, switch to POSIX instead.\n");
        ret_value = PDC_Server_posix_one_file_io(region_list_head);
    }
    else {
        printf("==PDC_SERVER: unsupported IO plugin!\n");
        ret_value = FAIL;
        goto done;
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    server_total_io_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    #endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Write the data from clients' shared memory to persistant storage,
 * after the server has accumulated requests from all node local clients
 *
 * \param  region_list_head[IN]     List of IO requests
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_write_from_shm(region_list_t *region_list_head)
{
    perr_t ret_value = SUCCEED;
    /* region_list_t *merged_list = NULL; */
    /* region_list_t *elt; */
    region_list_t *region_elt;
    uint32_t i;

    FUNC_ENTER(NULL);

    is_server_direct_io_g = 0;
    // TODO: Merge regions
    /* PDC_Server_merge_region_list_naive(io_list->region_list_head, &merged_list); */
    /* printf("==PDC_SERVER: write regions after merge\n"); */
    /* DL_FOREACH(io_list->region_list_head, elt) { */
    /*     PDC_print_region_list(elt); */
    /* } */

    // Now we have a merged list of regions to be read,
    // so just write one by one


    // Open the clients' shared memory for data to be written to storage
    DL_FOREACH(region_list_head, region_elt) {
        if (region_elt->is_io_done == 1) 
            continue;
        
        // Calculate io size
        if (region_elt->data_size == 0) {
            region_elt->data_size = region_elt->count[0];
            for (i = 1; i < region_elt->ndim; i++)
                region_elt->data_size *= region_elt->count[i];
        }

        // Open shared memory and map to data buf
        region_elt->shm_fd = shm_open(region_elt->shm_addr, O_RDONLY, 0666);
        if (region_elt->shm_fd == -1) {
            printf("==PDC_SERVER[%d]: Shared memory open failed [%s]!\n", 
                    pdc_server_rank_g, region_elt->shm_addr);
            ret_value = FAIL;
            goto done;
        }
        else {
            if (is_debug_g == 1) 
                printf("==PDC_SERVER[%d]: Shared memory open [%s]!\n", pdc_server_rank_g, region_elt->shm_addr);
        }

        region_elt->buf= mmap(0, region_elt->data_size, PROT_READ, MAP_SHARED, region_elt->shm_fd, 0);
        if (region_elt->buf== MAP_FAILED) {
            printf("==PDC_SERVER[%d]: Map failed: %s\n", pdc_server_rank_g, strerror(errno));
            // close and unlink?
            ret_value = FAIL;
            goto done;
        }
        /* printf("==PDC_SERVER[%d]: %s buf [%.*s]\n", */
        /*         pdc_server_rank_g, __func__, region_elt->data_size, region_elt->buf); */

    }

    // POSIX write
    ret_value = PDC_Server_regions_io(region_list_head, POSIX);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER: PDC_Server_regions_io ERROR!\n");
        goto done;
    }

    /* printf("==PDC_SERVER[%d]: Finished PDC_Server_regions_io\n", pdc_server_rank_g); */
done:
    fflush(stdout);

    // TODO: keep the shared memory for now and close them later?
    /* printf("==PDC_SERVER[%d]: closing shared mem\n", pdc_server_rank_g); */
    /* PDC_print_region_list(region_elt); */
    DL_FOREACH(region_list_head, region_elt) {
        if (region_elt->is_io_done == 1) {
            ret_value = PDC_Server_close_shm(region_elt);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER: error closing shared memory\n");
                goto done;
            }
        }
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_Server_data_write_from_shm

/*
 * Perform the IO request via shared memory
 *
 * \param  callback_info[IN]     Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
hg_return_t PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info)
{
    perr_t ret_value = SUCCEED;
    perr_t status    = SUCCEED;

    pdc_data_server_io_list_t *io_list_elt, *io_list = NULL, *io_list_target = NULL;
    region_list_t *region_elt = NULL, *region_tmp;
    /* region_list_t *region_tmp = NULL; */
    int real_bb_cnt = 0, real_lustre_cnt = 0;
    int write_to_bb_cnt = 0;
    int count;
    /* uint32_t client_id; */
    size_t i;

    FUNC_ENTER(NULL);

    // Debug
    /* is_debug_g = 1; */

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
    #endif

    data_server_io_info_t *io_info = (data_server_io_info_t*) callback_info->arg;

    if (io_info->io_type == WRITE) {
        // Set the number of storage metadta update to be buffered, number is relative to 
        // write requests from all node local clients for one object
        if (pdc_buffered_bulk_update_total_g == 0) {
            pdc_buffered_bulk_update_total_g = io_info->nbuffer_request * io_info->nclient;
            /* printf("==PDC_SERVER[%d]: set buffer size %d\n", pdc_server_rank_g, pdc_buffered_bulk_update_total_g); */
            /* fflush(stdout); */
        }

        // Set the buffer io request
        if (buffer_write_request_total_g == 0) 
            buffer_write_request_total_g = io_info->nbuffer_request * io_info->nclient;
        buffer_write_request_num_g++;
        
        /* printf("==PDC_SERVER[%d]: received %d/%d write requests\n", */ 
        /*         pdc_server_rank_g, buffer_write_request_num_g, buffer_write_request_total_g); */
    }
    else if (io_info->io_type == READ) {
        if (buffer_read_request_total_g == 0) 
            buffer_read_request_total_g = io_info->nbuffer_request * io_info->nclient;
        buffer_read_request_num_g++;
        /* printf("==PDC_SERVER[%d]: received %d/%d read requests\n", */ 
        /*         pdc_server_rank_g, buffer_read_request_num_g, buffer_read_request_total_g); */
    }
    else {
        printf("==PDC_SERVER: PDC_Server_data_io_via_shm - invalid IO type received from client!\n");
        ret_value = FAIL;
        goto done;
    }
    fflush(stdout);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_write_list_mutex_g);
#endif
    // Iterate io list, find the IO list of this obj
    if (io_info->io_type == WRITE) 
        io_list = pdc_data_server_write_list_head_g;
    else if (io_info->io_type == READ)
        io_list = pdc_data_server_read_list_head_g;

    DL_FOREACH(io_list, io_list_elt) {
        /* printf("io_list_elt obj id: %" PRIu64 "\n", io_list_elt->obj_id); */
        /* fflush(stdout); */
        if (io_info->meta.obj_id == io_list_elt->obj_id) {
            io_list_target = io_list_elt;
            break;
        }
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_write_list_mutex_g);
#endif

    // If not found, create and insert one to the list
    if (NULL == io_list_target) {

        /* if (is_debug_g == 1) { */
        /*     printf("==PDC_SERVER: No existing io request with same obj_id %" PRIu64 " found!\n", */
        /*                                                                 io_info->meta.obj_id); */
        /* } */
        // pdc_data_server_io_list_t maintains the request list for one object id,
        // write and read are separate lists
        io_list_target = (pdc_data_server_io_list_t*)calloc(1, sizeof(pdc_data_server_io_list_t));
        if (NULL == io_list_target) {
            printf("==PDC_SERVER: ERROR allocating pdc_data_server_io_list_t!\n");
            ret_value = FAIL;
            goto done;
        }
        io_list_target->obj_id = io_info->meta.obj_id;
        io_list_target->total  = io_info->nclient;
        io_list_target->count  = 0;
        io_list_target->ndim   = io_info->meta.ndim;
        for (i = 0; i < io_info->meta.ndim; i++)
            io_list_target->dims[i] = io_info->meta.dims[i];

        /* io_list_target->total_size  = 0; */

        char *data_path = NULL;
        char *bb_data_path = NULL;
        char *user_specified_data_path = getenv("PDC_DATA_LOC");
        if (user_specified_data_path != NULL) {
            data_path = user_specified_data_path;
        }
        else {
            data_path = getenv("SCRATCH");
            if (data_path == NULL)
                data_path = ".";
        }

        bb_data_path = getenv("PDC_BB_LOC");
        if (bb_data_path != NULL)
            sprintf(io_list_target->bb_path, "%s/pdc_data/cont_%" PRIu64 "", 
                    bb_data_path, io_info->meta.cont_id);

        // Auto generate a data location path for storing the data
        sprintf(io_list_target->path, "%s/pdc_data/cont_%" PRIu64 "", data_path, io_info->meta.cont_id);
        io_list_target->region_list_head = NULL;

        // Add to the io list
        if (io_info->io_type == WRITE)
            DL_APPEND(pdc_data_server_write_list_head_g, io_list_target);
        else if (io_info->io_type == READ)
            DL_APPEND(pdc_data_server_read_list_head_g, io_list_target);
    }

    io_list_target->count++;
    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: received %d/%d data %s requests of [%s]\n",
                pdc_server_rank_g, io_list_target->count, io_list_target->total,
                io_info->io_type == READ? "read": "write", io_info->meta.obj_name);
        fflush(stdout);
    }

    // TODO: instead of having a single list that stores IO requests, we can have an array of
    //       lists each corresponds to one client, so the lookup can be faster and more error
    //       prune.

    // Need to check if there is already one region in the list, and update accordingly
    DL_FOREACH_SAFE(io_list_target->region_list_head, region_elt, region_tmp) {
        // Only compares the start and count to see if two are identical
        if (PDC_is_same_region_list(&(io_info->region), region_elt) == 1) {
            // free the shm if read
            if (io_info->io_type == READ) {
                if (PDC_Server_close_shm(region_elt) != SUCCEED) {
                    printf("==PDC_SERVER[%d]: PDC_Server_data_io_via_shm() - ERROR close shm!\n", 
                            pdc_server_rank_g);
                }
            }
            // Remove that region
            DL_DELETE(io_list_target->region_list_head, region_elt);
            free(region_elt);
        }
    }

    // append current request region to the io list
    // Init current request region
    region_list_t *new_region = (region_list_t*)calloc(1, sizeof(region_list_t));
    if (new_region == NULL) {
        printf("==PDC_SERVER: ERROR allocating new_region!\n");
        ret_value = FAIL;
        goto done;
    }
    pdc_region_list_t_deep_cp(&(io_info->region), new_region);

    // Calculate size
    /* uint64_t tmp_total_size = 1; */
    /* for (i = 0; i < io_info->meta.ndim; i++) */
    /*     tmp_total_size *= new_region->count[i]; */
    /* io_list_target->total_size += tmp_total_size; */

    DL_APPEND(io_list_target->region_list_head, new_region);

    if (is_debug_g == 1) {
        DL_COUNT(io_list_target->region_list_head, region_elt, count);
        printf("==PDC_SERVER[%d]: Added 1 to IO request list, obj_id=%" PRIu64 ", %d total\n",
                pdc_server_rank_g, new_region->meta->obj_id, count);
        PDC_print_region_list(new_region);
    }

    // NOTE: new buffer io code
    // Write
    if (io_info->io_type == WRITE && buffer_write_request_num_g >= buffer_write_request_total_g &&
        buffer_write_request_total_g != 0) {

        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: received all %d requests, starts writing.\n",
                    pdc_server_rank_g, buffer_write_request_total_g);
            fflush(stdout);
        }

        // Perfor IO for all requests in each io_list (of unique obj_id)
        DL_FOREACH(pdc_data_server_write_list_head_g, io_list_elt) {

            // Sort the list so it is ordered by client id
            DL_SORT(io_list_elt->region_list_head, region_list_cmp_by_client_id);

            // received all requests, start writing
            // Some server write to BB when specified
            int curr_cnt = 0;
            write_to_bb_cnt = io_list_elt->total * write_to_bb_percentage_g / 100;
            real_bb_cnt = 0;
            real_lustre_cnt = 0;
            // Specify the location of data to be written to
            DL_FOREACH(io_list_elt->region_list_head, region_elt) {

                sprintf(region_elt->storage_location, "%s/server%d/s%04d.bin",
                        io_list_elt->path, pdc_server_rank_g, pdc_server_rank_g);
                real_lustre_cnt++;
                /* PDC_print_region_list(region_elt); */

                // If BB is enabled, then overwrite with BB path with the right number of servers
                if (write_to_bb_percentage_g > 0 ) {
                    if (io_list_elt->bb_path == NULL || io_list_elt->bb_path[0] == ' ') {
                        printf("==PDC_SERVER[%d]: Error with BB path [%s]!\n",
                                pdc_server_rank_g, io_list_elt->bb_path);
                    }
                    else {
                        if (pdc_server_rank_g % 2 == 0) {
                            // Half of the servers writes to BB first
                            if (curr_cnt < write_to_bb_cnt) {
                                sprintf(region_elt->storage_location, "%s/server%d/s%04d.bin",
                                        io_list_elt->bb_path, pdc_server_rank_g, pdc_server_rank_g);
                                real_bb_cnt++;
                                real_lustre_cnt--;
                            }
                        }
                        else {
                            // Others write to Lustre first
                            if (curr_cnt >= io_list_elt->total - write_to_bb_cnt) {
                                sprintf(region_elt->storage_location, "%s/server%d/s%04d.bin",
                                        io_list_elt->bb_path, pdc_server_rank_g, pdc_server_rank_g);
                                real_bb_cnt++;
                                real_lustre_cnt--;
                            }
                        }
                    }
                }
                curr_cnt++;
                /* printf("region to write obj_id: %" PRIu64 ", loc [%s], %d %%\n", */
                /*        region_elt->meta->obj_id,region_elt->storage_location,write_to_bb_percentage_g); */
            }

            /* if (write_to_bb_percentage_g > 0) { */
            /*     printf("==PDC_SERVER[%d]: write to BB %d, write to Lustre %d\n", */ 
            /*             pdc_server_rank_g, real_bb_cnt, real_lustre_cnt); */
            /*     fflush(stdout); */
            /* } */

            status = PDC_Server_data_write_from_shm(io_list_elt->region_list_head);
            if (status != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_Server_data_write_from_shm FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }
            else {
                // mark this list is done, to avoid future rework
                if (is_debug_g == 1) {
                    printf("==PDC_SERVER[%d]: finished data write for 1 request!\n", pdc_server_rank_g);
                }
            }

            // TODO: Need to remove the io_list after IO check is done!
        }
        buffer_write_request_num_g = 0;
        buffer_write_request_total_g = 0;
    }
    else if (io_info->io_type == READ && buffer_read_request_num_g == buffer_read_request_total_g &&
        buffer_read_request_total_g != 0) {

        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: received all %d requests, starts reading.\n",
                    pdc_server_rank_g, buffer_read_request_total_g);
        }
        DL_FOREACH(pdc_data_server_read_list_head_g, io_list_elt) {

            // Sort the list so it is ordered by client id
            DL_SORT(io_list_elt->region_list_head, region_list_cmp_by_client_id);

            if (is_debug_g == 1) {
                DL_COUNT(io_list_elt->region_list_head, region_elt, count);
                printf("==PDC_SERVER[%d]: read request list %d total, first is:\n",
                        pdc_server_rank_g, count);
                PDC_print_region_list(io_list_elt->region_list_head);
            }

            // Storage location is obtained later
            status = PDC_Server_data_read_to_shm(io_list_elt->region_list_head, io_list_elt->obj_id);
            if (status != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_Server_data_read_to_shm FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }
        }

        buffer_read_request_num_g = 0;
        buffer_read_request_total_g = 0;
    }

    // ^NOTE: new buffer io code
    

    // NOTE: old working code 
    // Check if we have received all requests
    // NOTE: this assumes the aggregation of client IO are all of the same object
    //       otherwise should set differently on the client
    /* if (io_list_target->count == io_list_target->total) { */

    /*     // Sort the list so it is ordered by client id */
    /*     DL_SORT(io_list_target->region_list_head, region_list_cmp_by_client_id); */

    /*     if (is_debug_g) { */
    /*         printf("==PDC_SERVER[%d]: received all %d requests, starts %s.\n", */
    /*                 pdc_server_rank_g, io_list_target->total, io_info->io_type == READ? "reading": "writing"); */
    /*     } */

    /*     if (io_info->io_type == READ) { */

    /*         if (is_debug_g == 1) { */
    /*             DL_COUNT(io_list_target->region_list_head, region_elt, count); */
    /*             printf("==PDC_SERVER[%d]: read request list %d total, first is:\n", */
    /*                     pdc_server_rank_g, count); */
    /*             PDC_print_region_list(io_list_target->region_list_head); */
    /*         } */

    /*         // Storage location is obtained later */
    /*         status = PDC_Server_data_read_to_shm(io_list_target->region_list_head, io_list_target->obj_id); */
    /*         if (status != SUCCEED) { */
    /*             printf("==PDC_SERVER[%d]: PDC_Server_data_read_to_shm FAILED!\n", pdc_server_rank_g); */
    /*             ret_value = FAIL; */
    /*             goto done; */
    /*         } */
    /*     } */
    /*     else if (io_info->io_type == WRITE) { */
    /*         // received all requests, start writing */
    /*         // Some server write to BB when specified */
    /*         int curr_cnt = 0; */
    /*         int write_to_bb_cnt = io_list_target->total * write_to_bb_percentage_g / 100; */

    /*         // Specify the location of data to be written to */
    /*         DL_FOREACH(io_list_target->region_list_head, region_elt) { */

    /*             sprintf(region_elt->storage_location, "%s/server%d/s%04d.bin", */
    /*                     io_list_target->path, pdc_server_rank_g, pdc_server_rank_g); */
    /*             /1* PDC_print_region_list(region_elt); *1/ */

    /*             // If BB is enabled, then overwrite with BB path with the right number of servers */
    /*             if (write_to_bb_percentage_g > 0 && curr_cnt < write_to_bb_cnt) { */
    /*                 sprintf(region_elt->storage_location, "%s/server%d/s%04d.bin", */
    /*                         io_list_target->bb_path, pdc_server_rank_g, pdc_server_rank_g); */
    /*             } */
    /*             curr_cnt++; */
    /*             /1* printf("region to write obj_id: %" PRIu64 ", loc [%s], %d %%\n", *1/ */
    /*             /1*        region_elt->meta->obj_id, region_elt->storage_location, write_to_bb_percentage_g); *1/ */
    /*         } */

    /*         status = PDC_Server_data_write_from_shm(io_list_target->region_list_head); */
    /*         if (status != SUCCEED) { */
    /*             printf("==PDC_SERVER[%d]: PDC_Server_data_write_from_shm FAILED!\n", pdc_server_rank_g); */
    /*             ret_value = FAIL; */
    /*             goto done; */
    /*         } */
    /*     } */
    /*     else { */
    /*         printf("==PDC_SERVER: PDC_Server_data_io_via_shm - invalid IO type received from client!\n"); */
    /*         ret_value = FAIL; */
    /*         goto done; */
    /*     } */

    /*     if (status != SUCCEED) { */
    /*         printf("==PDC_SERVER[%d]: ERROR %s [%s]!\n", pdc_server_rank_g, */
    /*                 io_info->io_type == WRITE? "writing to": "reading from", io_list_target->path); */
    /*         ret_value = FAIL; */
    /*         goto done; */
    /*     } */

    /*     // IO is done, notify all clients */

    /*     /1* DL_COUNT(io_list_target->region_list_head, region_elt, count); *1/ */
    /*     /1* printf("region_list_head: %d total\n", count); *1/ */
    /*     region_elt = NULL; */
    /*     /1* DL_FOREACH(io_list_target->region_list_head, region_elt) { *1/ */
    /*     /1*     /2* PDC_print_region_list(region_elt); *2/ *1/ */
    /*     /1*     for (i = 0; i < PDC_SERVER_MAX_PROC_PER_NODE; i++) { *1/ */
    /*     /1*         if (i != 0 && region_elt->client_ids[i] == 0) *1/ */
    /*     /1*             break; *1/ */

    /*     /1*         client_id = region_elt->client_ids[i]; *1/ */
    /*     /1*         if (client_id >= (uint32_t)pdc_client_num_g) { *1/ */
    /*     /1*             printf("==PDC_SERVER[%d]: PDC_Server_data_io_via_shm - error with client_id=%u/%d notify!\n     ", *1/ */
    /*     /1*                    pdc_server_rank_g, client_id, pdc_client_num_g); *1/ */
    /*     /1*             break; *1/ */
    /*     /1*         } *1/ */

    /*     /1*         if (is_debug_g ) { *1/ */
    /*     /1*             printf("==PDC_SERVER[%d]: Finished %s request, notify client %u\n", *1/ */
    /*     /1*                     pdc_server_rank_g, io_info->io_type == READ? "read": "write", client_id); *1/ */
    /*     /1*             fflush(stdout); *1/ */
    /*     /1*         } *1/ */

    /*     /1*         if (io_info->io_type == WRITE) *1/ */
    /*     /1*             region_elt->shm_addr[0] = 0; *1/ */

    /*     /1*         // TODO: currently assumes each region is for one client only! *1/ */
    /*     /1*         //       if one region is merged from the requests from multiple clients *1/ */
    /*     /1*         //       the shm_addr needs to be properly offseted when returning to client *1/ */
    /*     /1*         ret_value = PDC_Server_notify_io_complete_to_client(client_id, io_list_target->obj_id, *1/ */
    /*     /1*                                             region_elt->shm_addr, io_info->io_type); *1/ */
    /*     /1*         if (ret_value != SUCCEED) { *1/ */
    /*     /1*             printf("PDC_SERVER[%d]: PDC_Server_notify_io_complete_to_client FAILED!\n", *1/ */
    /*     /1*                     pdc_server_rank_g); *1/ */
    /*     /1*             /2* goto done; *2/ *1/ */
    /*     /1*         } *1/ */

    /*     /1*     } // End of for *1/ */
    /*     /1* } // End of DL_FOREACH *1/ */

    /*     /1* if (is_debug_g == 1) { *1/ */
    /*     /1*     printf("==PDC_SERVER[%d]: finished writing to storage, and notified all clients\n", *1/ */
    /*     /1*             pdc_server_rank_g); *1/ */
    /*     /1* } *1/ */
    /*     /1* // Remove all regions of the finished request *1/ */
    /*     /1* region_elt = NULL; *1/ */
    /*     /1* DL_FOREACH_SAFE(io_list_target->region_list_head, region_elt, region_tmp) { *1/ */
    /*     /1*     /2* PDC_print_region_list(region_elt); *2/ *1/ */
    /*     /1*     DL_DELETE(io_list_target->region_list_head, region_elt); *1/ */
    /*     /1*     free(region_elt); *1/ */
    /*     /1*     /2* printf("Deleted one region\n"); *2/ *1/ */
    /*     /1*     /2* fflush(stdout); *2/ *1/ */
    /*     /1* } *1/ */

    /*     /1* // Check if this is the last one from the list *1/ */
    /*     /1* DL_COUNT(io_list, io_list_elt, count); *1/ */
    /*     /1* if (count == 1) { *1/ */
    /*     /1*     free(io_list); *1/ */
    /*     /1*     if (io_info->io_type == WRITE) *1/ */
    /*     /1*         pdc_data_server_write_list_head_g = NULL; *1/ */
    /*     /1*     else if (io_info->io_type == READ) *1/ */
    /*     /1*         pdc_data_server_read_list_head_g = NULL; *1/ */
    /*     /1* } *1/ */
    /*     /1* else { *1/ */
    /*     /1*     DL_DELETE(io_list, io_list_target); *1/ */
    /*     /1*     free(io_list_target); *1/ */
    /*     /1* } *1/ */

    /*     io_list_target->count = 0; */
    /* } // end of if (io_list_target->count == io_list_target->total) */
    /* else if (io_list_target->count > io_list_target->total) { */
    /*     printf("==PDC_SERVER[%d]: received more requested than requested, %d/%d!\n", */
    /*             pdc_server_rank_g, io_list_target->count, io_list_target->total); */
    /*     ret_value = FAIL; */
    /*     goto done; */
    /* } */


    ret_value = SUCCEED;

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    server_io_elapsed_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

    // Debug
    /* is_debug_g = 0; */

done:
    /* free(io_info); */
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_data_io_via_shm

/*
 * Update the storage location information of the corresponding metadata that is stored locally
 *
 * \param  region[IN]     Region info of the data that's been written by server 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *target_meta = NULL;
    /* pdc_metadata_t *region_meta = NULL; */
    region_list_t  *region_elt = NULL, *region_tmp, *new_region = NULL;
    int update_success = -1;

    FUNC_ENTER(NULL);


    if (region == NULL) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // Find object metadata
    target_meta = find_metadata_by_id(obj_id);
    if (target_meta == NULL) {
        printf("==PDC_SERVER[%d]: %s - FAIL to get storage metadata\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // Find if there is the same region already stored in the metadata and update it
    DL_FOREACH_SAFE(target_meta->storage_region_list_head, region_elt, region_tmp) {
        if (PDC_is_same_region_list(region_elt, region) == 1) {
            // Update location and offset
            memcpy(region_elt->storage_location, region->storage_location, sizeof(char)*ADDR_MAX);
            region_elt->offset = region->offset;
            region_elt->data_size = pdc_get_region_size(region_elt);
            update_success = 1;
            /* if (is_debug_g == 1) { */
                printf("==PDC_SERVER[%d]: overwrite existing region location/offset\n", pdc_server_rank_g);
                PDC_print_storage_region_list(region_elt);
            /* } */
            break;
        }
    } // DL_FOREACH

    // Insert the storage region if not found
    if (update_success == -1) {

        // Create the region list
        new_region = (region_list_t*)calloc(1, sizeof(region_list_t));
        /* PDC_init_region_list(new_region); */
        if (pdc_region_list_t_deep_cp(region, new_region) != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - deep copy FAILED!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        DL_APPEND(target_meta->storage_region_list_head, new_region);
        /* if (is_debug_g == 1) { */
        /*     printf("==PDC_SERVER[%d]: created new region location/offset\n", pdc_server_rank_g); */
        /*     PDC_print_storage_region_list(new_region); */
        /* } */

        // Debug print
        /* printf("==PDC_SERVER[%d]: created new region with offset %" PRIu64 "\n", */
        /*         pdc_server_rank_g, new_region->start[0]); */
    }

done:
    /* printf("==PDC_SERVER[%d]: updated local region storage location, start %" PRIu64 "\n", */ 
    /*         pdc_server_rank_g, region->start[0]); */
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end PDC_Server_update_local_region_storage_loc

/*
 * Callback function for the region location info update
 *
 * \param  callback_info[IN]     Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
PDC_Server_update_region_loc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    server_lookup_args_t *lookup_args;
    hg_handle_t handle;
    metadata_update_out_t output;

    FUNC_ENTER(NULL);

    lookup_args = (server_lookup_args_t*) callback_info->arg;
    handle = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS || output.ret != 20171031) {
        printf("==PDC_SERVER[%d]: %s - error HG_Get_output\n", pdc_server_rank_g, __func__);
        lookup_args->ret_int = -1;
        goto done;
    }

    if (is_debug_g) {
        printf("==PDC_SERVER[%d]: %s: ret=%d\n", pdc_server_rank_g, __func__, output.ret);
        fflush(stdout);
    }
    lookup_args->ret_int = output.ret;

done:
    /* s2s_send_work_todo_g--; */
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}



/*
 * Update the storage location information of the corresponding metadata that may be stored in a
 * remote server.
 *
 * \param  callback_info[IN]     Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storagelocation_offset(region_list_t *region)
{
    hg_return_t hg_ret;
    perr_t ret_value = SUCCEED;
    uint32_t server_id = 0;
    pdc_metadata_t *region_meta = NULL;
    hg_handle_t update_region_loc_handle;
    update_region_loc_in_t in;
    server_lookup_args_t lookup_args;

    FUNC_ENTER(NULL);

    if (region == NULL) {
        printf("==PDC_SERVER[%d] %s - NULL region!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (region->storage_location[0] == 0) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    region_meta = region->meta;
    if (region_meta == NULL) {
        printf("==PDC_SERVER[%d]: %s - region meta is NULL!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    server_id = PDC_get_server_by_obj_id(region_meta->obj_id, pdc_server_size_g);
    /* printf("==PDC_SERVER[%d]: will update storage region to server %d, %" PRIu64 "\n", */
    /*         pdc_server_rank_g, server_id, region->start[0]); */

    if (server_id == (uint32_t)pdc_server_rank_g) {
        // Metadata object is local, no need to send update RPC
        ret_value = PDC_Server_update_local_region_storage_loc(region, region_meta->obj_id);
        update_local_region_count_g++;
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - update_local_region_storage FAILED!\n",pdc_server_rank_g,__func__);
            goto done;
        }
    }
    else {

        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                    pdc_server_rank_g, server_id);
            ret_value = FAIL;
            goto done;
        }
       
        hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr,
                           update_region_loc_register_id_g, &update_region_loc_handle);
        if (hg_ret != HG_SUCCESS) {
            printf("==PDC_SERVER[%d]: %s - HG_Create FAILED!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: Sending updated region loc to server %d\n",
                    pdc_server_rank_g, server_id);
            /* PDC_print_region_list(region); */
            fflush(stdout);
        }


        in.obj_id = region->meta->obj_id;
        in.offset = region->offset;
        in.storage_location = calloc(sizeof(char), ADDR_MAX);
        memcpy(in.storage_location, region->storage_location, sizeof(char)*ADDR_MAX);

        pdc_region_list_t_to_transfer(region, &(in.region));

        hg_ret = HG_Forward(update_region_loc_handle, PDC_Server_update_region_loc_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS) {
            printf("==PDC_SERVER[%d]: %s - HG_Forward() to server %d FAILED\n",
                    pdc_server_rank_g, __func__, server_id);
            HG_Destroy(update_region_loc_handle);
            ret_value = FAIL;
            goto done;
        }

        /* if (s2s_send_work_todo_g != 0) { */
        /*     printf("==PDC_SERVER[%d]: %s s2s_send_work_todo_g is %d", */ 
        /*             pdc_server_rank_g, __func__, s2s_send_work_todo_g); */
        /*     fflush(stdout); */
        /* } */

/* printf("PDC_Server_update_region_storagelocation_offset() calls PDC_Server_check_response()\n"); */
/* fflush(stdout); */
        // Wait for response from server
        /* s2s_send_work_todo_g = 1; */
        /* PDC_Server_check_response(&hg_context_g, &s2s_send_work_todo_g); */

        // TODO: move to callback
        update_remote_region_count_g++;

        HG_Destroy(update_region_loc_handle);
        free(in.storage_location);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_update_region_storagelocation_offset

/*
 * Update the storage location information of the corresponding metadata that may be stored in a
 * remote server, using Mercury bulk transfer.
 *
 * \param  region_ptrs[IN]     Pointers to the regions that need to be updated
 * \param  cnt        [IN]     Number of pointers in region_ptrs
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_add_region_storage_meta_to_bulk_buf(region_list_t *region, bulk_xfer_data_t *bulk_data)
{
    perr_t ret_value = SUCCEED;
    int i;
    uint64_t obj_id = 0;
    update_region_storage_meta_bulk_t *curr_buf_ptr;
    uint64_t *obj_id_ptr;

    FUNC_ENTER(NULL);

    // Sanity check 
    if (NULL == region || region->storage_location[0] == 0 || NULL == region->meta) {
        printf("==PDC_SERVER[%d]: %s - invalid region data!\n", pdc_server_rank_g, __func__);
        PDC_print_region_list(region);
        ret_value = FAIL;
        goto done;
    }

    // Alloc space and init if it's empty
    if (0 == bulk_data->n_alloc) {

        bulk_data->buf_ptrs = (void**)calloc(sizeof(void*), PDC_BULK_XFER_INIT_NALLOC+1);
        update_region_storage_meta_bulk_t *buf_ptrs_1d = (update_region_storage_meta_bulk_t*)calloc(sizeof(update_region_storage_meta_bulk_t), PDC_BULK_XFER_INIT_NALLOC);

        bulk_data->buf_sizes = (hg_size_t*)calloc(sizeof(hg_size_t), PDC_BULK_XFER_INIT_NALLOC);
        if (NULL == buf_ptrs_1d || NULL == bulk_data->buf_sizes) {
            printf("==PDC_SERVER[%d]: %s - calloc FAILED!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        // first element of bulk_buf is the obj_id
        bulk_data->buf_ptrs[0] = (void*)calloc(sizeof(uint64_t), 1);
        bulk_data->buf_sizes[0] = sizeof(uint64_t);

        for (i = 1; i < PDC_BULK_XFER_INIT_NALLOC+1; i++) {
            bulk_data->buf_ptrs[i] = &buf_ptrs_1d[i];
            bulk_data->buf_sizes[i] = sizeof(update_region_storage_meta_bulk_t);
        }
        bulk_data->n_alloc   = PDC_BULK_XFER_INIT_NALLOC;
        bulk_data->idx       = 1;
        bulk_data->obj_id    = 0;
        bulk_data->origin_id = pdc_server_rank_g;
        bulk_data->target_id = 0;
    }

    // TODO: Need to expand the space when more than initial allocated
    int idx = bulk_data->idx;
    if (idx >= bulk_data->n_alloc) {
        printf("==PDC_SERVER[%d]: %s- need to alloc larger!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // get obj_id
    obj_id = region->meta->obj_id;
    if (obj_id == 0) {
        printf("==PDC_SERVER[%d]: %s - invalid metadata from region!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    obj_id_ptr = (uint64_t*)bulk_data->buf_ptrs[0];

    // Check if current region has the same obj_id
    if (0 != *obj_id_ptr) {
        if (bulk_data->obj_id != obj_id) {
            printf("==PDC_SERVER[%d]: %s - region has a different obj id!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }
    }
    else {
        // obj_id and target_id only need to be init when the first data is added (when obj_id==0)
        *obj_id_ptr = obj_id;
        bulk_data->target_id = PDC_get_server_by_obj_id(obj_id, pdc_server_size_g);
        bulk_data->obj_id = obj_id;
    }

    // Copy data from region to corresponding buf ptr in bulk_data
    curr_buf_ptr = (update_region_storage_meta_bulk_t*)(bulk_data->buf_ptrs[idx]);
    /* curr_buf_ptr->obj_id = region->meta->obj_id; */
    pdc_region_list_t_to_transfer(region, &curr_buf_ptr->region_transfer);
    strcpy(curr_buf_ptr->storage_location, region->storage_location);
    curr_buf_ptr->offset = region->offset;

    bulk_data->idx++;
done:
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_add_region_storage_meta_to_bulk_buf

/*
 * Callback function for server to update the metadata stored locally, 
 * can be called by the server itself, or through bulk transfer callback.
 *
 * \param  update_region_storage_meta_bulk_t
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storage_meta_bulk_local(update_region_storage_meta_bulk_t **bulk_ptrs, int cnt)
{
    perr_t ret_value = SUCCEED;
    int i;
    pdc_metadata_t *target_meta = NULL;
    region_list_t  *region_elt = NULL, *new_region = NULL;
    update_region_storage_meta_bulk_t *bulk_ptr;
    int update_success = 0, express_insert = 0;
    uint64_t obj_id;

    FUNC_ENTER(NULL);

    if (NULL == bulk_ptrs || cnt == 0 || NULL == bulk_ptrs[0]) {
        printf("==PDC_SERVER[%d]: %s invalid input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }


    obj_id = *(uint64_t*)bulk_ptrs[0];

    /* printf("==PDC_SERVER[%d]: start to update local storage meta for obj id %" PRIu64 "\n", */ 
    /*         pdc_server_rank_g, obj_id); */
    /* fflush(stdout); */

    // First ptr in buf_ptrs is the obj_id
    for (i = 1; i < cnt; i++) {

        bulk_ptr = (update_region_storage_meta_bulk_t*)(bulk_ptrs[i]);

        // Create a new region for each and copy the data from bulk data
        new_region = (region_list_t*)calloc(1, sizeof(region_list_t));
        pdc_region_transfer_t_to_list_t(&bulk_ptr->region_transfer, new_region);
        new_region->data_size = pdc_get_region_size(new_region);
        strcpy(new_region->storage_location, bulk_ptr->storage_location);
        new_region->offset = bulk_ptr->offset;
        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: created new region from bulk_ptr\n", pdc_server_rank_g);
            PDC_print_storage_region_list(new_region);
        }


        // The bulk data are regions of same obj_id, and the corresponding metadata must be local
        target_meta = find_metadata_by_id(obj_id);
        if (target_meta == NULL) {
            printf("==PDC_SERVER[%d]: %s - FAIL to get storage metadata\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        // Check if we can insert without duplicate check
        if (i == 1 && target_meta->storage_region_list_head == NULL) 
            express_insert = 1;
        
        if (express_insert != 1) {
            // Find if there is the same region already stored in the metadata and update it
            DL_FOREACH(target_meta->storage_region_list_head, region_elt) {
                if (PDC_is_same_region_list(region_elt, new_region) == 1) {
                    // Update location and offset
                    strcpy(region_elt->storage_location, new_region->storage_location);
                    region_elt->offset = new_region->offset;
                    update_success = 1;

                    printf("==PDC_SERVER[%d]: overwrite existing region location/offset\n", pdc_server_rank_g);
                    PDC_print_storage_region_list(region_elt);
                    free(new_region);
                    break;
                }
            } // DL_FOREACH
        }

        // Insert the storage region if not found
        if (update_success != 1) 
            DL_APPEND(target_meta->storage_region_list_head, new_region);
        
    }

    /* printf("==PDC_SERVER[%d]: finished update %d local storage meta for obj id %" PRIu64 "\n", */ 
    /*         pdc_server_rank_g, cnt-1, obj_id); */
    /* fflush(stdout); */
done:
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_update_region_storage_meta_bulk_local

static hg_return_t
update_storage_meta_bulk_rpc_cb(const struct hg_cb_info *callback_info)
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

    /* Get output parameters, 9999 corresponds to the one set in update_storage_meta_bulk_cb */
    if (bulk_rpc_ret.ret != 9999)
        printf("==PDC_SERVER[%d]: update storage meta bulk rpc returned value error!\n", pdc_server_rank_g);

    fflush(stdout);

    ret = HG_Free_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free output\n");
        goto done;
    }

    if (cb_args->cb != NULL) 
        cb_args->cb((update_storage_meta_list_t*)cb_args->meta_list_target, cb_args->n_updated);

    /* Free memory handle */
    ret = HG_Bulk_free(cb_args->bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free bulk data handle\n");
        goto done;
    } 

    /* HG_Destroy(cb_args->rpc_handle); */
done:
    /* s2s_send_work_todo_g--; */
    return ret;
} // end of update_storage_meta_bulk_rpc_cb

/*
 * MPI VERSION. Update the storage location information of the corresponding metadata that may be stored in a
 * remote server, using Mercury bulk transfer.
 *
 * \param  bulk_data[IN]     Bulk data ptr, obtained with PDC_Server_add_region_storage_meta_to_bulk_buf
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storage_meta_bulk_mpi(bulk_xfer_data_t *bulk_data)
{
    perr_t ret_value = SUCCEED;

    int i;
    uint32_t server_id = 0;

    update_region_storage_meta_bulk_t *recv_buf = NULL;
    void **all_meta = NULL;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    server_id        = bulk_data->target_id;
    int meta_cnt     = bulk_data->idx-1;        // idx includes the first element of buf_ptrs, which is count
    int data_size    = sizeof(update_region_storage_meta_bulk_t)*meta_cnt;
    int all_meta_cnt = meta_cnt * pdc_server_size_g;

    // Only recv server needs allocation
    if (server_id == (uint32_t)pdc_server_rank_g) {
        recv_buf = (update_region_storage_meta_bulk_t*)calloc(pdc_server_size_g, data_size);
    }

    /* printf("==PDC_SERVER[%d]: will MPI_Reduce update region to server %d, with data size %d\n", */
    /*         pdc_server_rank_g, server_id, data_size); */
    /* fflush(stdout); */

    // bulk_data->buf_ptrs[0] is number of metadata to be updated
    // Note: during add to buf_ptr process, we actually have the big 1D array allocated to buf_ptrs[1]
    //       so all metadata can be transferred with one single reduce 
    // TODO: here we assume each server has exactly the same number of metadata
    MPI_Gather(bulk_data->buf_ptrs[1], data_size, MPI_CHAR, recv_buf, data_size, MPI_CHAR, 
               server_id, MPI_COMM_WORLD);

    // Now server_id has all the data in recv_buf, start update all 
    if (server_id == (uint32_t)pdc_server_rank_g) {
        all_meta = (void**)calloc(sizeof(void*), all_meta_cnt+1);
        all_meta[0] = bulk_data->buf_ptrs[0];
        for (i = 1; i < all_meta_cnt+1; i++) {
            all_meta[i] = &recv_buf[i-1];
        }

        ret_value = PDC_Server_update_region_storage_meta_bulk_local((update_region_storage_meta_bulk_t **)
                                                                     all_meta, all_meta_cnt+1);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - update_region_storage_meta_bulk_local FAILED!\n", 
                    pdc_server_rank_g, __func__);
            goto done;
        }
        update_local_region_count_g += all_meta_cnt;

        // Debug print
        /* if (server_id == (uint32_t)pdc_server_rank_g) { */
        /*     printf("==PDC_SERVER[%d]: Received obj ids %d\n", pdc_server_rank_g, pdc_server_size_g); */
        /*     for (i = 0; i < pdc_server_size_g; i++) { */
        /*         printf("%" PRIu64 " ", obj_ids[i]); */
        /*     } */
        /* } */

    } // end of if

    /* printf("==PDC_SERVER[%d]: region storage meta bulk update done\n", pdc_server_rank_g); */
    /* fflush(stdout); */

done:
    if (server_id == (uint32_t)pdc_server_rank_g) {
        free(recv_buf);
        free(all_meta);
    }
#else
    printf("%s - is not supposed to be called without MPI enabled!\n", __func__);
#endif
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_update_region_storage_meta_bulk_mpi


/*
 * Update the storage location information of the corresponding metadata that may be stored in a
 * remote server, using Mercury bulk transfer.
 *
 * \param  bulk_data[IN]     Bulk data ptr, obtained with PDC_Server_add_region_storage_meta_to_bulk_buf
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_update_region_storage_meta_bulk_with_cb(bulk_xfer_data_t *bulk_data, perr_t (*cb)(), update_storage_meta_list_t *meta_list_target, int *n_updated)
{
    hg_return_t hg_ret;
    perr_t ret_value = SUCCEED;

    int i;
    uint32_t server_id = 0;
    uint64_t obj_id = 0;
    hg_handle_t rpc_handle;
    hg_bulk_t bulk_handle;
    server_lookup_args_t lookup_args;
    bulk_rpc_in_t bulk_rpc_in;
    update_region_storage_meta_bulk_args_t *cb_args;

    FUNC_ENTER(NULL);

    cb_args = (update_region_storage_meta_bulk_args_t*)calloc(1, sizeof(update_region_storage_meta_bulk_args_t));
    server_id = bulk_data->target_id;

    /* printf("==PDC_SERVER[%d]: will bulk update storage region to server %d, obj id is %" PRIu64 "\n", */
    /*         pdc_server_rank_g, server_id, bulk_data->obj_id); */
    /* fflush(stdout); */

    if (server_id == (uint32_t)pdc_server_rank_g) {

        ret_value = PDC_Server_update_region_storage_meta_bulk_local((update_region_storage_meta_bulk_t**)bulk_data->buf_ptrs, bulk_data->idx);
        update_local_region_count_g++;
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - update_region_storage_meta_bulk_local FAILED!\n", 
                    pdc_server_rank_g, __func__);
            goto done;
        }
        meta_list_target->is_updated = 1;

        // Run callback function immediately
        cb(meta_list_target, n_updated);
    } // end of if
    else {
        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                    pdc_server_rank_g, server_id);
            ret_value = FAIL;
            goto done;
        }

        // Send the bulk handle to the target with RPC
        hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, 
                           bulk_rpc_register_id_g, &rpc_handle);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not create handle\n");
            ret_value = FAIL;
            goto done;
        }

        /* Register memory */
        hg_ret = HG_Bulk_create(hg_class_g, bulk_data->idx, bulk_data->buf_ptrs, bulk_data->buf_sizes, 
                                HG_BULK_READ_ONLY, &bulk_handle);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not create bulk data handle\n");
            ret_value = FAIL;
            goto done;
        }

        /* Fill input structure */
        bulk_rpc_in.origin = pdc_server_rank_g;
        bulk_rpc_in.cnt = bulk_data->idx;
        bulk_rpc_in.bulk_handle = bulk_handle;

        /* pdc_msleep(pdc_server_rank_g*10%400); */

        cb_args->cb   = cb;
        cb_args->meta_list_target = meta_list_target;
        cb_args->n_updated = n_updated;
        cb_args->server_id = server_id;
        cb_args->bulk_handle = bulk_handle;
        cb_args->rpc_handle  = rpc_handle;

        /* Forward call to remote addr */
        hg_ret = HG_Forward(rpc_handle, update_storage_meta_bulk_rpc_cb, cb_args, &bulk_rpc_in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not forward call\n");
            ret_value = FAIL;
            goto done;
        }

        meta_list_target->is_updated = 1;
        /* if (s2s_send_work_todo_g != 0) { */
        /*     printf("==PDC_SERVER[%d]: %s s2s_send_work_todo_g is %d", */ 
        /*             pdc_server_rank_g, __func__, s2s_send_work_todo_g); */
        /*     fflush(stdout); */
        /* } */
        /* s2s_send_work_todo_g += 1; */

        /* printf("==PDC_SERVER[%d]: sent %d bulk update of obj %" PRIu64 " to server %d, send work todo is %d\n", */ 
        /*         pdc_server_rank_g, bulk_data->idx-1, bulk_data->obj_id, server_id, s2s_send_work_todo_g); */
        /* fflush(stdout); */

/*         /1* Free memory handle *1/ */
/*         hg_ret = HG_Bulk_free(bulk_handle); */
/*         if (hg_ret != HG_SUCCESS) { */
/*             fprintf(stderr, "Could not free bulk data handle\n"); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */ 

        /* HG_Destroy(rpc_handle); */
    } // end of else

    /* printf("==PDC_SERVER[%d]: region storage meta bulk update done\n", pdc_server_rank_g); */
    /* fflush(stdout); */

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_update_region_storage_meta_bulk_with_cb

/*
 * Get metadata of the object ID received from client from local metadata hash table
 *
 * \param  obj_id[IN]           Object ID
 * \param  res_meta_ptrdata[IN]     Pointer of metadata of the specified object ID
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_local_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta_ptr)
{
    perr_t ret_value = SUCCEED;

    pdc_hash_table_entry_head *head;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    int n_entry;
    HashTablePair pair;

    
    FUNC_ENTER(NULL);

    *res_meta_ptr = NULL;

    if (metadata_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(metadata_hash_table_g);
        hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair = hash_table_iter_next(&hash_table_iter);
            head = pair.value;
            // Now iterate the list under this entry
            DL_FOREACH(head->metadata, elt) {
                if (elt->obj_id == obj_id) {
                    *res_meta_ptr = elt;
                    goto done;
                }
            }
        }
    }  
    else {
        printf("==PDC_SERVER: metadata_hash_table_g not initialized!\n");
        ret_value = FAIL;
        *res_meta_ptr = NULL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
} // PDC_Server_get_local_metadata_by_id

/*
 * Callback function for get the metadata by ID
 *
 * \param  callback_info[IN]     Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
PDC_Server_get_metadata_by_id_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                  ret_value;
    hg_handle_t                  handle;
    pdc_metadata_t              *meta = NULL;
    get_metadata_by_id_args_t   *cb_args;
    get_metadata_by_id_out_t     output;

    FUNC_ENTER(NULL);

    cb_args = (get_metadata_by_id_args_t*)callback_info->arg;
    handle = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_get_metadata_by_id_cb - error HG_Get_output\n", pdc_server_rank_g);
        goto done;
    }

    if (output.res_meta.obj_id != 0) {
        // TODO free metdata
        meta = (pdc_metadata_t*)malloc(sizeof(pdc_metadata_t));
        pdc_transfer_t_to_metadata_t(&output.res_meta, meta);
    }
    else {
        printf("==PDC_SERVER[%d]: %s - no valid metadata is retrieved\n", pdc_server_rank_g, __func__);
        goto done;
    }

    // Execute the callback function 
    if (NULL != cb_args->cb) {
        ((region_list_t*)(cb_args->args))->meta = meta;
        cb_args->cb(cb_args->args, POSIX);
    }
    else {
        printf("==PDC_SERVER[%d]: %s NULL callback ptr\n", pdc_server_rank_g, __func__);
        goto done;
    }

done:
    HG_Free_output(handle, &output);
    free(cb_args);
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
} // PDC_Server_get_metadata_by_id_cb

/*
 * Get metadata of the object ID received from client from (possibly remtoe) metadata hash table
 *
 * \param  obj_id[IN]           Object ID
 * \param  res_meta_ptr[IN/OUT]     Pointer of metadata of the specified object ID
 * \param  cb[IN]               Callback function needs to be executed after getting the metadata
 * \param  args[IN]             Callback function input parameters
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_metadata_by_id_with_cb(uint64_t obj_id, perr_t (*cb)(), void *args)
{
    pdc_metadata_t             *res_meta_ptr = NULL;
    hg_return_t                 hg_ret;
    perr_t                      ret_value = SUCCEED;
    uint32_t                    server_id = 0;
    hg_handle_t                 get_metadata_by_id_handle;
    get_metadata_by_id_args_t   *cb_args;
    get_metadata_by_id_in_t     in;

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        // Metadata object is local, no need to send update RPC
        ret_value = PDC_Server_get_local_metadata_by_id(obj_id, &res_meta_ptr);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_get_local_metadata_by_id FAILED!\n", pdc_server_rank_g);
            goto done;
        }

        ((region_list_t*)args)->meta = res_meta_ptr;
        // Call the callback function directly and pass in the result metadata ptr
        cb(args, POSIX);
    }
    else {
        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                    pdc_server_rank_g, server_id);
            ret_value = FAIL;
            goto done;
        }

        HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_metadata_by_id_register_id_g,
                &get_metadata_by_id_handle);

        /* printf("Sending updated region loc to target\n"); */

        cb_args = (get_metadata_by_id_args_t *)malloc(sizeof(get_metadata_by_id_args_t));
        in.obj_id    = obj_id;
        cb_args->cb   = cb;
        cb_args->args = args;

        // TODO: Include the callback function ptr and args in the cb_args
        hg_ret = HG_Forward(get_metadata_by_id_handle, PDC_Server_get_metadata_by_id_cb, &cb_args, &in);

        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - Could not forward\n", 
                    pdc_server_rank_g, __func__);
            res_meta_ptr = NULL;
            HG_Destroy(get_metadata_by_id_handle);
            return FAIL;
        }

        // Wait for response from server
        /* work_todo_g = 1; */
        /* PDC_Server_check_response(&hg_context_g, &work_todo_g); */

//        HG_Destroy(get_metadata_by_id_handle);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_get_metadata_by_id_with_cb


/*
 * Get metadata of the object ID received from client from (possibly remtoe) metadata hash table
 *
 * \param  obj_id[IN]           Object ID
 * \param  res_meta_ptrdata[IN]     Pointer of metadata of the specified object ID
 *
 * \return Non-negative on success/Negative on failure
 */
/* perr_t PDC_Server_get_metadata_by_id(uint64_t obj_id, pdc_metadata_t **res_meta_ptr) */
/* { */
/*     hg_return_t hg_ret; */
/*     perr_t ret_value = SUCCEED; */
/*     uint32_t server_id = 0; */
/*     hg_handle_t get_metadata_by_id_handle; */

/*     FUNC_ENTER(NULL); */

/*     server_id = PDC_get_server_by_obj_id(obj_id, pdc_server_size_g); */
/*     if (server_id == (uint32_t)pdc_server_rank_g) { */
/*         // Metadata object is local, no need to send update RPC */
/*         ret_value = PDC_Server_get_local_metadata_by_id(obj_id, res_meta_ptr); */
/*         if (ret_value != SUCCEED) { */
/*             printf("==PDC_SERVER[%d]: PDC_Server_get_local_metadata_by_id FAILED!\n", pdc_server_rank_g); */
/*             goto done; */
/*         } */
/*     } */
/*     else { */

/*         if (pdc_remote_server_info_g[server_id].addr_valid != 1) { */
/*             if (PDC_Server_lookup_server_id(server_id) != SUCCEED) { */
/*                 printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", */
/*                         pdc_server_rank_g, server_id); */
/*                 ret_value = FAIL; */
/*                 goto done; */
/*             } */
/*         } */

/*         HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_metadata_by_id_register_id_g, */
/*                 &get_metadata_by_id_handle); */

/*         /1* printf("Sending updated region loc to target\n"); *1/ */
/*         server_lookup_args_t lookup_args; */

/*         get_metadata_by_id_in_t in; */
/*         in.obj_id = obj_id; */

/*         hg_ret = HG_Forward(get_metadata_by_id_handle, PDC_Server_get_metadata_by_id_cb, &lookup_args, &in); */

/*         if (hg_ret != HG_SUCCESS) { */
/*             fprintf(stderr, "==PDC_SERVER[%d]: %s - Could not forward\n", */ 
/*                     pdc_server_rank_g, __func__); */
/*             *res_meta_ptr = NULL; */
/*             HG_Destroy(get_metadata_by_id_handle); */
/*             return FAIL; */
/*         } */

/*         // Wait for response from server */
/*         /1* work_todo_g = 1; *1/ */
/*         /1* PDC_Server_check_response(&hg_context_g, &work_todo_g); *1/ */

/*         // TODO: move to callback */
/*         // Retrieved metadata is stored in lookup_args */
/*         *res_meta_ptr = lookup_args.meta; */

/*         HG_Destroy(get_metadata_by_id_handle); */
/*     } */

/* done: */
/*     fflush(stdout); */
/*     FUNC_LEAVE(ret_value); */
/* } // end of PDC_Server_get_metadata_by_id */

/*
 * Test serialize/un-serialized code
 *
 * \return void
 */
void test_serialize()
{
    region_list_t **head = NULL, *a, *b, *c, *d;
    head = (region_list_t**)malloc(sizeof(region_list_t*) * 4);
    a = (region_list_t*)malloc(sizeof(region_list_t));
    b = (region_list_t*)malloc(sizeof(region_list_t));
    c = (region_list_t*)malloc(sizeof(region_list_t));
    d = (region_list_t*)malloc(sizeof(region_list_t));

    head[0] = a;
    head[1] = b;
    head[2] = c;
    head[3] = d;

    PDC_init_region_list(a);
    PDC_init_region_list(b);
    PDC_init_region_list(c);
    PDC_init_region_list(d);

    a->ndim = 2;
    a->start[0] = 0;
    a->start[1] = 4;
    a->count[0] = 10;
    a->count[1] = 14;
    a->offset   = 1234;
    sprintf(a->storage_location, "%s", "/path/to/a/a/a/a/a");

    b->ndim = 2;
    b->start[0] = 10;
    b->start[1] = 14;
    b->count[0] = 100;
    b->count[1] = 104;
    b->offset   = 12345;
    sprintf(b->storage_location, "%s", "/path/to/b/b");


    c->ndim = 2;
    c->start[0] = 20;
    c->start[1] = 21;
    c->count[0] = 23;
    c->count[1] = 24;
    c->offset   = 123456;
    sprintf(c->storage_location, "%s", "/path/to/c/c/c/c");


    d->ndim = 2;
    d->start[0] = 110;
    d->start[1] = 111;
    d->count[0] = 70;
    d->count[1] = 71;
    d->offset   = 1234567;
    sprintf(d->storage_location, "%s", "/path/to/d");

    uint32_t total_str_len = 0;
    uint32_t n_region = 4;
    PDC_get_serialized_size(head, n_region, &total_str_len);

    void *buf = (void*)malloc(total_str_len);

    PDC_serialize_regions_lists(head, n_region, buf, total_str_len);

    region_list_t **regions = (region_list_t**)malloc(sizeof(region_list_t*) * PDC_MAX_OVERLAP_REGION_NUM);
    uint32_t i;
    for (i = 0; i < n_region; i++) {
        regions[i] = (region_list_t*)malloc(sizeof(region_list_t));
        PDC_init_region_list(regions[i]);
    }

    PDC_unserialize_region_lists(buf, regions, &n_region);

}

/*
 * Perform the POSIX read of multiple storage regions that overlap with the read request
 *
 * \param  ndim[IN]                 Number of dimension
 * \param  req_start[IN]            Start offsets of the request
 * \param  req_count[IN]            Counts of the request
 * \param  storage_start[IN]        Start offsets of the storage region
 * \param  storage_count[IN]        Counts of the storage region
 * \param  fp[IN]                   File pointer
 * \param  storage region[IN]       File offset of the first storage region
 * \param  buf[OUT]                 Data buffer to be read to
 * \param  total_read_bytes[OUT]    Total number of bytes read
 *
 * \return Non-negative on success/Negative on failure
 */
// For each intersecteed region in storage, calculate the actual overlapping regions'
// start[] and count[], then read into the buffer with correct offset
perr_t PDC_Server_read_overlap_regions(uint32_t ndim, uint64_t *req_start, uint64_t *req_count,
                                       uint64_t *storage_start, uint64_t *storage_count,
                                       FILE *fp, uint64_t file_offset, void *buf,  size_t *total_read_bytes)
{
    perr_t ret_value = SUCCEED;
    uint64_t overlap_start[DIM_MAX], overlap_count[DIM_MAX];
    uint64_t buf_start[DIM_MAX];
    uint64_t storage_start_physical[DIM_MAX];
    uint64_t buf_offset = 0, storage_offset = file_offset, total_bytes = 0, read_bytes = 0, row_offset = 0;
    uint64_t i = 0, j = 0;
    int is_all_selected = 0;
    int n_contig_read = 0;
    double n_contig_MB = 0.0;


    FUNC_ENTER(NULL);

    *total_read_bytes = 0;
    if (ndim > 3 || ndim <= 0) {
        printf("==PDC_SERVER[%d]: dim=%" PRIu32 " unsupported yet!", pdc_server_rank_g, ndim);
        ret_value = FAIL;
        goto done;
    }

    // Get the actual start and count of region in storage
    if (get_overlap_start_count(ndim, req_start, req_count, storage_start, storage_count,
                                overlap_start, overlap_count) != SUCCEED ) {
        printf("==PDC_SERVER[%d]: get_overlap_start_count FAILED!\n", pdc_server_rank_g);
        ret_value = FAIL;
        goto done;
    }

/*     printf("==PDC_SERVER[%d]: get_overlap_start_count done!\n", pdc_server_rank_g); */
/*     fflush(stdout); */

    total_bytes = 1;
    for (i = 0; i < ndim; i++) {
        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: overlap_start[%" PRIu64 "]=%" PRIu64 ", "
                    "req_start[%" PRIu64 "]=%" PRIu64 "  \n", pdc_server_rank_g, i, overlap_start[i],
                    i, req_start[i]);
        }

        total_bytes              *= overlap_count[i];
        buf_start[i]              = overlap_start[i] - req_start[i];
        storage_start_physical[i] = overlap_start[i] - storage_start[i];
        if (i == 0) {
            buf_offset = buf_start[0];
            storage_offset += storage_start_physical[0];
        }
        else if (i == 1)  {
            buf_offset += buf_start[1] * req_count[0];
            storage_offset += storage_start_physical[1] * storage_count[0];
        }
        else if (i == 2)  {
            buf_offset += buf_start[2] * req_count[0] * req_count[1];
            storage_offset += storage_start_physical[2] * storage_count[0] * storage_count[1];
        }

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: buf_offset=%" PRIu64 ", req_count[%" PRIu64 "]=%" PRIu64 "  "
                    "buf_start[%" PRIu64 "]=%" PRIu64 " \n",
                    pdc_server_rank_g, buf_offset, i, req_count[i], i, buf_start[i]);
        }
    }

    if (is_debug_g == 1) {
        for (i = 0; i < ndim; i++) {
            printf("==PDC_SERVER[%d]: overlap start %" PRIu64 ", "
                   "storage_start  %" PRIu64 ", req_start %" PRIu64 " \n",
                   pdc_server_rank_g, overlap_start[i], storage_start[i], req_start[i]);
        }

        for (i = 0; i < ndim; i++) {
            printf("==PDC_SERVER[%d]: dim=%" PRIu32 ", read with storage start %" PRIu64 ","
                    " to buffer offset %" PRIu64 ", of size %" PRIu64 " \n",
                    pdc_server_rank_g, ndim, storage_start_physical[i], buf_start[i], overlap_count[i]);
        }
        fflush(stdout);
    }

    // Check if the entire storage region is selected
    is_all_selected = 1;
    for (i = 0; i < ndim; i++) {
        if (overlap_start[i] != storage_start[i] || overlap_count[i] != storage_count[i]) {
            is_all_selected = -1;
            break;
        }
    }

    /* printf("ndim = %" PRIu64 ", is_all_selected=%d\n", ndim, is_all_selected); */

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
    #endif


    // TODO: additional optimization to check if any dimension is entirely selected
    if (ndim == 1 || is_all_selected == 1) {
        // Can read the entire storage region at once

        // Check if current file ptr is at correct pos
        uint64_t cur_off = (uint64_t)ftell(fp);
        if (cur_off != storage_offset) {
            fseek (fp, storage_offset, SEEK_SET);
            /* printf("==PDC_SERVER[%d]: curr %" PRIu64 ", fseek to %" PRIu64 "!\n", */ 
                    /* pdc_server_rank_g, cur_off, storage_offset); */
        }

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: read storage offset %" PRIu64 ", buf_offset  %" PRIu64 "\n",
                    pdc_server_rank_g,storage_offset, buf_offset);
        }

        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start1;
        struct timeval  pdc_timer_end1;
        gettimeofday(&pdc_timer_start1, 0);
        #endif

        read_bytes = fread(buf+buf_offset, 1, total_bytes, fp);

        #ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end1, 0);
        double region_read_time1 = PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
        /* printf("==PDC_SERVER[%d]: fread %" PRIu64 " bytes, %.2fs\n", */ 
        /*         pdc_server_rank_g, read_bytes, region_read_time1); */
        #endif

        n_contig_MB += read_bytes/1048576.0;
        n_contig_read++;
        if (read_bytes != total_bytes) {
            printf("==PDC_SERVER[%d]: %s - fread failed actual read bytes %" PRIu64 ", should be %" PRIu64 "\n",
                    pdc_server_rank_g, __func__, read_bytes, total_bytes);
            ret_value= FAIL;
            goto done;
        }
        *total_read_bytes += read_bytes;

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: Read entire storage region, size=%" PRIu64 "\n",
                        pdc_server_rank_g, read_bytes);
        }
    } // end if
    else {
        // NOTE: assuming row major, read overlapping region row by row
        if (ndim == 2) {
            row_offset = 0;
            fseek (fp, storage_offset, SEEK_SET);
            for (i = 0; i < overlap_count[1]; i++) {
                // Move to next row's begining position
                if (i != 0) {
                    fseek (fp, storage_count[0] - overlap_count[0], SEEK_CUR);
                    row_offset = i * req_count[0];
                }
                read_bytes = fread(buf+buf_offset+row_offset, 1, overlap_count[0], fp);
                n_contig_MB += read_bytes/1048576.0;
                n_contig_read++;
                if (read_bytes != overlap_count[0]) {
                    printf("==PDC_SERVER[%d]: %s - fread failed!\n", pdc_server_rank_g, __func__);
                    ret_value= FAIL;
                    goto done;
                }
                *total_read_bytes += read_bytes;
                /* printf("Row %" PRIu64 ", Read data size=%" PRIu64 ": [%.*s]\n", i, overlap_count[0], */
                /*                                 overlap_count[0], buf+buf_offset+row_offset); */
            } // for each row
        } // ndim=2
        else if (ndim == 3) {

            if (is_debug_g == 1) {
                printf("read count: %" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n",
                        overlap_count[0], overlap_count[1], overlap_count[2]);
            }

            uint64_t buf_serialize_offset;
            /* fseek (fp, storage_offset, SEEK_SET); */
            for (j = 0; j < overlap_count[2]; j++) {

                fseek (fp, storage_offset + j*storage_count[0]*storage_count[1], SEEK_SET);
                /* printf("seek offset: %" PRIu64 "\n", storage_offset + j*storage_count[0]*storage_count[1]); */

                for (i = 0; i < overlap_count[1]; i++) {
                    // Move to next row's begining position
                    if (i != 0)
                        fseek (fp, storage_count[0] - overlap_count[0], SEEK_CUR);

                    buf_serialize_offset = buf_offset + i*req_count[0] + j*req_count[0]*req_count[1];
                    if (is_debug_g == 1) {
                        printf("Read to buf offset: %" PRIu64 "\n", buf_serialize_offset);
                    }

                    read_bytes = fread(buf+buf_serialize_offset, 1, overlap_count[0], fp);
                    n_contig_MB += read_bytes/1048576.0;
                    n_contig_read++;
                    if (read_bytes != overlap_count[0]) {
                        printf("==PDC_SERVER[%d]: %s - fread failed!\n", pdc_server_rank_g, __func__);
                        ret_value= FAIL;
                        goto done;
                    }
                    *total_read_bytes += read_bytes;
                    if (is_debug_g == 1) {
                        printf("z: %" PRIu64 ", j: %" PRIu64 ", Read data size=%" PRIu64 ": [%.*s]\n",
                                j, i, overlap_count[0], (int)overlap_count[0], (char*)buf+buf_serialize_offset);
                    }
                } // for each row
            }

        }

    } // end else (ndim != 1 && !is_all_selected);

    n_fread_g += n_contig_read;
    fread_total_MB += n_contig_MB;

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    double region_read_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    server_read_time_g += region_read_time;
    /* printf("==PDC_SERVER[%d]: %d fread total %" PRIu64 " bytes, %.2fs\n", */ 
    /*         pdc_server_rank_g, n_contig_read, read_bytes, region_read_time); */
#endif


    if (total_bytes != *total_read_bytes) {
        printf("==PDC_SERVER[%d]: %s - read size error!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }


done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void PDC_init_bulk_xfer_data_t(bulk_xfer_data_t* a) 
{
    if (NULL == a) {
        printf("==%s: NULL input!\n", __func__);
        return;
    }
    a->buf_ptrs  = NULL;
    a->buf_sizes = NULL;
    a->n_alloc   = 0;
    a->idx       = 0;
    a->obj_id    = 0;
    a->target_id = 0;
    a->origin_id = 0;
}
/*
 * Read with POSIX within one file, based on the region list
 * after the server has accumulated requests from all node local clients
 *
 * \param  region_list_head[IN]       Region info of IO request
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_posix_one_file_io(region_list_t* region_list_head)
{
    perr_t ret_value = SUCCEED;
    size_t read_bytes = 0, write_bytes = 0;
    size_t total_read_bytes = 0;
    uint64_t offset = 0;
    uint32_t n_storage_regions = 0, i = 0;
    region_list_t *region_elt = NULL, *previous_region = NULL;
    FILE *fp_read = NULL, *fp_write = NULL;
    char *prev_path = NULL;
    int stripe_count, stripe_size;
    int io_iter = 0;
    bulk_xfer_data_t *bulk_data = NULL;
    int nregion_in_bulk_update = 0;
    int has_read  = 0;
    /* int has_write = 0; */

    FUNC_ENTER(NULL);

    if (NULL == region_list_head) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    io_iter = 0;

    // Check if there are any reads or writes
    DL_FOREACH(region_list_head, region_elt) {
        if (region_elt->access_type == READ) {
            has_read = 1;
            break;
        }
        /* else if (region_elt->access_type == WRITE) */ 
        /*     has_write = 1; */
    }

    // For read requests, it's better to aggregate read requests from all node-local clients
    // and query once, rather than query one by one, so we aggregate at the beginning
    if (has_read == 1) {
        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start1;
        struct timeval  pdc_timer_end1;
        gettimeofday(&pdc_timer_start1, 0);
        #endif

        /* ret_value = PDC_Server_get_storage_location_of_region_with_cb(region_elt); */
        ret_value = PDC_Server_get_storage_location_of_region_mpi(region_elt);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_get_storage_location_of_region failed!\n",
                                                                        pdc_server_rank_g);
            goto done;
        }

        #ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end1, 0);
        server_get_storage_info_time_g += PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
        #endif
    }

    // Iterate over all region IO requests and perform actual IO
    // We have all requests from node local clients, now read them from storage system
    DL_FOREACH(region_list_head, region_elt) {
        if (region_elt->is_io_done == 1) 
            continue;

        if (region_elt->access_type == READ) {

            // Now for each storage region that overlaps with request region,
            // read with the corresponding offset and size
            for (i = 0; i < region_elt->n_overlap_storage_region; i++) {

                if (is_debug_g == 1) {
                    printf("==PDC_SERVER[%d]: Found overlapping storage regions %d\n",
                            pdc_server_rank_g, n_storage_regions);
                    /* printf("=========================================\n"); */
                    /* PDC_print_storage_region_list(overlap_regions[i]); */
                    /* printf("=========================================\n"); */
                }

                // If a new file needs to be opened
                if (prev_path == NULL || 
                    strcmp(region_elt->overlap_storage_regions[i].storage_location, prev_path) != 0) {

                    if (fp_read != NULL)  {
                        fclose(fp_read);
                        fp_read = NULL;
                    }

                    #ifdef ENABLE_TIMING
                    struct timeval  pdc_timer_start2;
                    struct timeval  pdc_timer_end2;
                    gettimeofday(&pdc_timer_start2, 0);
                    #endif

                    /* printf("==PDC_SERVER[%d]: opening [%s]\n", pdc_server_rank_g, */ 
                    /*                         overlap_regions[i]->storage_location); */
                    /* fflush(stdout); */

                    fp_read = fopen(region_elt->overlap_storage_regions[i].storage_location, "rb");
                    n_fopen_g++;

                    #ifdef ENABLE_TIMING
                    gettimeofday(&pdc_timer_end2, 0);
                    server_fopen_time_g += PDC_get_elapsed_time_double(&pdc_timer_start2, &pdc_timer_end2);
                    #endif

                    if (fp_read == NULL) {
                        printf("==PDC_SERVER[%d]: fopen failed [%s]\n",
                                pdc_server_rank_g, region_elt->storage_location);
                        ret_value = FAIL;
                        goto done;
                    }
                }

                // Request: elt->start/count
                // Storage: region_elt->overlap_storage_regions[i]->start/count
                ret_value = PDC_Server_read_overlap_regions(region_elt->ndim, region_elt->start, 
                            region_elt->count, region_elt->overlap_storage_regions[i].start, 
                            region_elt->overlap_storage_regions[i].count, fp_read, 
                            region_elt->overlap_storage_regions[i].offset, region_elt->buf, &read_bytes);

                if (ret_value != SUCCEED) {
                    printf("==PDC_SERVER[%d]: error with PDC_Server_read_overlap_regions\n",
                            pdc_server_rank_g);
                    fclose(fp_read);
                    fp_read = NULL;

                    goto done;
                }
                total_read_bytes += read_bytes;

                prev_path = region_elt->overlap_storage_regions[i].storage_location;
            } // end of for all overlapping storage regions for one request region

            if (is_debug_g == 1) {
                printf("==PDC_SERVER[%d]: Read data total size %" PRIu64 "\n",
                        pdc_server_rank_g, total_read_bytes);
                fflush(stdout);
            }
            /* printf("Read iteration: size %" PRIu64 "\n", region_elt->data_size); */

            region_elt->is_data_ready = 1;
            region_elt->offset = offset;
            offset += total_read_bytes;

            region_elt->is_io_done = 1;

        } // end of READ
        else if (region_elt->access_type == WRITE) {

            // Assumes all regions are written to one file
            if (region_elt->storage_location == NULL) {
//                printf("==PDC_SERVER[%d]: %s - storage_location is NULL!\n", pdc_server_rank_g, __func__);
                ret_value = FAIL;
                region_elt->is_data_ready = -1;
                goto done;
            }

            // Open file if needed:
            //   if this is first time to write data, or 
            //   writing to a different location from last write.
            if (previous_region == NULL
                    || strcmp(region_elt->storage_location, previous_region->storage_location) != 0) {

                // Only need to mkdir once
                pdc_mkdir(region_elt->storage_location);

                #ifdef ENABLE_LUSTRE
                // Set Lustre stripe only if this is Lustre
                // NOTE: this only applies to NERSC Lustre on Cori and Edison
                if (strstr(region_elt->storage_location, "/global/cscratch") != NULL      ||
                    strstr(region_elt->storage_location, "/scratch1/scratchdirs") != NULL ||
                    strstr(region_elt->storage_location, "/scratch2/scratchdirs") != NULL ) {

                    // When env var PDC_NOST_PER_FILE is not set
                    if (pdc_nost_per_file_g != 1)
                        stripe_count = 248 / pdc_server_size_g;
                    else
                        stripe_count = pdc_nost_per_file_g;
                    stripe_size  = 16;                      // MB
                    PDC_Server_set_lustre_stripe(region_elt->storage_location, stripe_count, stripe_size);
                }
                #endif

                // Close previous file
                if (fp_write != NULL) {
                    fclose(fp_write);
                    fp_write = NULL;

                    if (is_debug_g == 1) {
                        printf("==PDC_SERVER[%d]: close and open new file [%s]\n",
                                  pdc_server_rank_g, region_elt->storage_location);
                    }
                }

                // TODO: need to recycle file space in cases of data update and delete

                #ifdef ENABLE_TIMING
                struct timeval  pdc_timer_start4;
                struct timeval  pdc_timer_end4;
                gettimeofday(&pdc_timer_start4, 0);
                #endif

                // Open current file as binary and append only, it is guarenteed that only current
                // server process access this file, so no lock is needed.
                fp_write = fopen(region_elt->storage_location, "ab");
                n_fopen_g++;

                #ifdef ENABLE_TIMING
                gettimeofday(&pdc_timer_end4, 0);
                server_fopen_time_g += PDC_get_elapsed_time_double(&pdc_timer_start4, &pdc_timer_end4);
                #endif

                if (NULL == fp_write) {
                    printf("==PDC_SERVER[%d]: fopen failed [%s]\n",
                                pdc_server_rank_g, region_elt->storage_location);
                    ret_value = FAIL;
                    goto done;
                }
                /* printf("write location is %s\n", region_elt->storage_location); */
            } // End open file

            // Get the current write offset
            offset = ftell(fp_write);

            #ifdef ENABLE_TIMING
            struct timeval  pdc_timer_start5;
            struct timeval  pdc_timer_end5;
            gettimeofday(&pdc_timer_start5, 0);
            #endif

            // Actual write (append)
            write_bytes = fwrite(region_elt->buf, 1, region_elt->data_size, fp_write);
            if (write_bytes != region_elt->data_size) {
                printf("==PDC_SERVER[%d]: fwrite to [%s] FAILED, region off %" PRIu64 ", size %" PRIu64 ", "
                       "actual writeen %" PRIu64 "!\n",
                            pdc_server_rank_g, region_elt->storage_location, offset, region_elt->data_size,
                            write_bytes);
                ret_value= FAIL;
                goto done;
            }
            n_fwrite_g++;
            fwrite_total_MB += write_bytes/1048576.0;

            #ifdef ENABLE_TIMING
            gettimeofday(&pdc_timer_end5, 0);
            double region_write_time = PDC_get_elapsed_time_double(&pdc_timer_start5, &pdc_timer_end5);
            server_write_time_g += region_write_time;
            /* printf("==PDC_SERVER[%d]: fwrite %" PRIu64 " bytes, %.2fs\n", */ 
            /*         pdc_server_rank_g, write_bytes, region_write_time); */
            #endif

            /* fclose(fp_write); */

            if (is_debug_g == 1) {
                printf("Write data offset: %" PRIu64 ", size %" PRIu64 ", to [%s]\n",
                                              offset, region_elt->data_size, region_elt->storage_location);
                /* printf("Write data buf: [%.*s]\n", region_elt->data_size, (char*)region_elt->buf); */
            }
            region_elt->is_data_ready = 1;
            region_elt->offset = offset;

            // add current region to the update bulk buf
            if (NULL == bulk_data) {
                bulk_data = (bulk_xfer_data_t*)calloc(1, sizeof(bulk_xfer_data_t));
                PDC_init_bulk_xfer_data_t(bulk_data);
            }

            ret_value = PDC_Server_add_region_storage_meta_to_bulk_buf(region_elt, bulk_data);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - add_region_storage_meta_to_bulk_buf FAILED!", 
                        pdc_server_rank_g, __func__);
                goto done;
            }
            nregion_in_bulk_update++;

            previous_region = region_elt;

            region_elt->is_io_done = 1;

        } // end of WRITE
        else {
            printf("==PDC_SERVER[%d]: %s- unsupported access type\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        /* PDC_print_region_list(region_elt); */
        /* fflush(stdout); */

        io_iter++;

        if (region_elt->overlap_storage_regions!= NULL) {
            free(region_elt->overlap_storage_regions);
            region_elt->overlap_storage_regions = NULL;
        }
        
    } // end DL_FOREACH region IO request (region)

    /* printf("==PDC_SERVER[%d]: %s- IO finished\n", pdc_server_rank_g, __func__); */
    /* fflush(stdout); */

    // Buffer the region storage metadata update 
    update_storage_meta_list_t *tmp_alloc;
    if (nregion_in_bulk_update != 0) {
        tmp_alloc = (update_storage_meta_list_t*)calloc(sizeof(update_storage_meta_list_t), 1);
        tmp_alloc->storage_meta_bulk_xfer_data = bulk_data;
        DL_APPEND(pdc_update_storage_meta_list_head_g, tmp_alloc);
        pdc_nbuffered_bulk_update_g++;
    }

    // FIXME: tmp fix for server direct IO
    int n_updated;
    if (is_server_direct_io_g) {
        ret_value = PDC_Server_update_storage_meta(&n_updated);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - FAILED to update storage meta\n", pdc_server_rank_g, __func__);
        }
    }
    /* int n_region = 0; */
    /* DL_COUNT(region_list_head, region_elt, n_region); */
    /* printf("==PDC_SERVER[%d]: after writes done, will update %d regions \n", pdc_server_rank_g, n_region); */
    /* fflush(stdout); */

    /* int iter = 0, n_write_region = 0; */
    /* if (is_read_only != 1) { */

    /*     #ifdef ENABLE_TIMING */
    /*     gettimeofday(&pdc_timer_start, 0); */
    /*     #endif */

    /*     // update all regions that are just written through bulk transfer */
    /*     ret_value = PDC_Server_update_region_storage_meta_bulk_with_cb(&bulk_data); */
    /*     if (ret_value != SUCCEED) { */
    /*         printf("==PDC_SERVER[%d]: %s - update storage info FAILED!", pdc_server_rank_g, __func__); */
    /*         goto done; */
    /*     } */

    /* /1*     // Update all regions location info to the metadata server *1/ */
    /* /1*     DL_FOREACH_SAFE(region_list_head, region_elt, region_tmp) { *1/ */
    /* /1*         // Debug print *1/ */
    /* /1*         /2* PDC_print_region_list(region_elt); *2/ *1/ */
    /* /1*         /2* fflush(stdout); *2/ *1/ */
    /* /1*         /2* printf("==PDC_SERVER[%d]: goint to update region no. %d\n", pdc_server_rank_g, iter); *2/ *1/ */

    /* /1*         if (region_elt->access_type == WRITE) { *1/ */
    /* /1*             n_write_region++; *1/ */
    /* /1*             // Update metadata with the location and offset *1/ */
    /* /1*             /2* printf("==PDC_SERVER[%d]: update storage info obj_id: %" PRIu64 " " *2/ *1/ */
    /* /1*             /2*        "ndim: %" PRIu64 ", offset %" PRIu64 " \n", pdc_server_rank_g, *2/ *1/ */ 
    /* /1*             /2*        region_elt->meta->obj_id, region_elt->ndim,  region_elt->start[0]); *2/ *1/ */     

    /* /1*             ret_value = PDC_Server_update_region_storagelocation_offset(region_elt); *1/ */
    /* /1*             if (ret_value != SUCCEED) { *1/ */
    /* /1*                 printf("==PDC_SERVER[%d]: failed to update region storage info!\n", pdc_server_rank_g); *1/ */
    /* /1*                 goto done; *1/ */
    /* /1*             } *1/ */
    /* /1*         } *1/ */

    /* /1*         // Debug print *1/ */
    /* /1*         /2* PDC_print_region_list(region_elt); *2/ *1/ */
    /* /1*         /2* fflush(stdout); *2/ *1/ */

    /* /1*         iter++; *1/ */
    /* /1*     } // end DL_FOREACH *1/ */

    /*     #ifdef ENABLE_TIMING */
    /*     gettimeofday(&pdc_timer_end, 0); */
    /*     double update_region_location_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end); */
    /*     /1* printf("==PDC_SERVER[%d]: update region location [%s] time %.6f!\n", *1/ */
    /*     /1*         pdc_server_rank_g, region_elt->storage_location, update_region_location_time); *1/ */
    /*     server_update_region_location_time_g += update_region_location_time; */
    /*     #endif */

    /* } // end if is_read_only */


done:
    if (fp_write != NULL) {
        /* #ifdef ENABLE_TIMING */
        /* gettimeofday(&pdc_timer_start, 0); */
        /* #endif */

        /* fsync(fileno(fp_write)); */

        /* #ifdef ENABLE_TIMING */
        /* double fsync_time; */
        /* gettimeofday(&pdc_timer_end, 0); */
        /* fsync_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end); */
        /* server_fsync_time_g += fsync_time; */
        /* /1* printf("==PDC_SERVER[%d]: fsync %.2fs\n", pdc_server_rank_g, fsync_time); *1/ */
        /* #endif */

        fclose(fp_write);
        fp_write = NULL;
    }

    if (fp_read != NULL) {
        fclose(fp_read);
        fp_read = NULL;
    }

    if (ret_value != SUCCEED)
        region_elt->is_data_ready = -1;

    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_Server_posix_one_file_io

// Insert the write request to a queue(list) for aggregation
/* perr_t PDC_Server_add_io_request(PDC_access_t io_type, pdc_metadata_t *meta, struct PDC_region_info *region_info, void *buf, uint32_t client_id) */
/* { */
/*     perr_t ret_value = SUCCEED; */
/*     pdc_data_server_io_list_t *elt = NULL, *io_list = NULL, *io_list_target = NULL; */
/*     region_list_t *region_elt = NULL; */

/*     FUNC_ENTER(NULL); */

/*     if (io_type == WRITE) */ 
/*         io_list = pdc_data_server_write_list_head_g; */
/*     else if (io_type == READ) */ 
/*         io_list = pdc_data_server_read_list_head_g; */
/*     else { */
/*         printf("==PDC_SERVER: PDC_Server_add_io_request_to_queue - invalid IO type!\n"); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/* #ifdef ENABLE_MULTITHREAD */ 
/*     hg_thread_mutex_lock(&data_write_list_mutex_g); */
/* #endif */
/*     // Iterate io list, find the IO list and region of current request */
/*     DL_FOREACH(io_list, elt) { */
/*         if (meta->obj_id == elt->obj_id) { */
/*             io_list_target = elt; */
/*             break; */
/*         } */
/*     } */
/* #ifdef ENABLE_MULTITHREAD */ 
/*     hg_thread_mutex_unlock(&data_write_list_mutex_g); */
/* #endif */

/*     // If there is no IO list created for current obj_id, create one and insert it to the global list */
/*     if (NULL == io_list_target) { */

/*         /1* printf("==PDC_SERVER: No existing io request with same obj_id found!\n"); *1/ */
/*         io_list_target = (pdc_data_server_io_list_t*)malloc(sizeof(pdc_data_server_io_list_t)); */
/*         if (NULL == io_list_target) { */
/*             printf("==PDC_SERVER: ERROR allocating pdc_data_server_io_list_t!\n"); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         io_list_target->obj_id = meta->obj_id; */
/*         io_list_target->total  = -1; */
/*         io_list_target->count  = 0; */
/*         io_list_target->ndim   = meta->ndim; */
/*         for (i = 0; i < meta->ndim; i++) */ 
/*             io_list_target->dims[i] = meta->dims[i]; */
        
/*         io_list_target->total_size  = 0; */

/*         // Auto generate a data location path for storing the data */
/*         strcpy(io_list_target->path, meta->data_location); */
/*         io_list_target->region_list_head = NULL; */

/*         DL_APPEND(io_list, io_list_target); */
/*     } */

/*     /1* printf("==PDC_SERVER[%d]: received %d/%d data %s requests of [%s]\n", pdc_server_rank_g, io_list_target->count, io_list_target->total, io_type == READ? "read": "write", meta->obj_name); *1/ */
/*     region_list_t *new_region = (region_list_t*)malloc(sizeof(region_list_t)); */
/*     if (new_region == NULL) { */
/*         printf("==PDC_SERVER: ERROR allocating new_region!\n"); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     PDC_init_region_list(new_region); */
/*     perr_t pdc_region_info_to_list_t(region_info, new_region); */

/*     new_region->client_ids[0] = client_id; */

/*     // Calculate size */
/*     new_region->data_size = 1; */
/*     for (i = 0; i < new_region->ndim; i++) */ 
/*         new_region->data_size *= new_region->count[i]; */

/*     io_list_target->total_size += new_region->data_size; */
/*     io_list_targeta->count++; */

/*     // Insert current request to the IO list's region list head */
/*     DL_APPEND(io_list_target->region_list_head, new_region); */

/* done: */
/*     fflush(stdout); */
/*     FUNC_LEAVE(ret_value); */
/* } // end of PDC_Server_add_io_request */

/*
 * Directly server read/write buffer from/to storage of one region
 * Read with POSIX within one file
 *
 * \param  io_type[IN]           IO type (read/write)
 * \param  obj_id[IN]            Object ID
 * \param  region_info[IN]       Region info of IO request
 * \param  buf[IN/OUT]           Data buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_io_direct(PDC_access_t io_type, uint64_t obj_id, struct PDC_region_info *region_info, void *buf)
{
    perr_t ret_value = SUCCEED;
    region_list_t *io_region = NULL;
    int stripe_count, stripe_size;
    size_t i;

    FUNC_ENTER(NULL);

    is_server_direct_io_g = 1;
    io_region = (region_list_t*)calloc(1, sizeof(region_list_t));
    PDC_init_region_list(io_region);
    pdc_region_info_to_list_t(region_info, io_region);

    /* printf("==PDC_SERVER[%d]: PDC_Server_data_io_direct read region:\n", pdc_server_rank_g); */
    /* PDC_print_region_list(io_region); */
    /* fflush(stdout); */

    // Generate a location for data storage for data server to write
    char *data_path = NULL;
    char *user_specified_data_path = getenv("PDC_DATA_LOC");
    if (user_specified_data_path != NULL)
        data_path = user_specified_data_path;
    else {
        data_path = getenv("SCRATCH");
        if (data_path == NULL)
            data_path = ".";
    }

    // Data path prefix will be $SCRATCH/pdc_data/$obj_id/
    sprintf(io_region->storage_location, "%s/pdc_data/%" PRIu64 "/server%d/s%04d.bin",
            data_path, obj_id, pdc_server_rank_g, pdc_server_rank_g);
    pdc_mkdir(io_region->storage_location);


#ifdef ENABLE_LUSTRE
    stripe_count = 248 / pdc_server_size_g;
    stripe_size  = 16;                      // MB
    PDC_Server_set_lustre_stripe(io_region->storage_location, stripe_count, stripe_size);

    if (is_debug_g == 1 && pdc_server_rank_g == 0) {
        printf("storage_location is %s\n", io_region->storage_location);
        /* printf("lustre is enabled\n"); */
    }
#endif

    io_region->access_type = io_type;

    io_region->data_size = io_region->count[0];
    for (i = 1; i < io_region->ndim; i++)
        io_region->data_size *= io_region->count[i];

    io_region->buf = buf;

    // Need to get the metadata
    /* meta = (pdc_metadata_t*)calloc(1, sizeof(pdc_metadata_t)); */
    /* ret_value = PDC_Server_get_metadata_by_id(obj_id, &meta); */
    /* if (ret_value != SUCCEED) { */
    /*     printf("PDC_SERVER: PDC_Server_data_io_direct - unable to get metadata of object\n"); */
    /*     goto done; */
    /* } */
    /* io_region->meta = meta; */

    /* if (is_debug_g == 1) { */
    /*     printf("PDC_SERVER[%d]: PDC_Server_data_io_direct %s - start %" PRIu64 ", size %" PRIu64 "\n", */ 
    /*             pdc_server_rank_g, io_type==READ?"READ":"WRITE", io_region->start[0], io_region->count[0]); */

    /* } */
    /* // Call the actual IO routine */
    /* ret_value = PDC_Server_regions_io(io_region, POSIX); */
    /* if (ret_value != SUCCEED) { */
    /*     printf("PDC_SERVER: PDC_Server_data_io_direct - PDC_Server_regions_io FAILED!\n"); */
    /*     goto done; */
    /* } */

    ret_value = PDC_Server_get_metadata_by_id_with_cb(obj_id, PDC_Server_regions_io, io_region);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_data_write_out(uint64_t obj_id, struct PDC_region_info *region_info, void *buf)
{
    perr_t ret_value = SUCCEED;
    ssize_t write_bytes; 
    char *nclient_per_node_str;
    int nclient_per_node, default_nclient_per_node = 31;
    data_server_region_t *region = NULL;

    FUNC_ENTER(NULL);

    region = PDC_Server_get_obj_region(obj_id);
    if(region == NULL) {
        printf("cannot locate file handle\n");
        goto done;
    }
    nclient_per_node_str = (getenv("NCLIENT"));
    if (nclient_per_node_str == NULL) 
        nclient_per_node = default_nclient_per_node;
    else
      nclient_per_node = atoi(nclient_per_node_str);

    write_bytes = pwrite(region->fd, buf, region_info->size[0], region_info->offset[0]-pdc_server_rank_g*nclient_per_node*region_info->size[0]);
    // printf("server %d calls pwrite, offset = %lld, size = %lld\n", pdc_server_rank_g, region_info->offset[0]-pdc_server_rank_g*nclient_per_node*region_info->size[0], region_info->size[0]);
    if(write_bytes == -1){
        printf("==PDC_SERVER[%d]: pwrite %d failed\n", pdc_server_rank_g, region->fd);
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
/*
 * Server writes buffer to storage of one region without client involvement
 * Read with POSIX within one file
 *
 * \param  obj_id[IN]            Object ID
 * \param  region_info[IN]       Region info of IO request
 * \param  buf[IN]               Data buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_write_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    ret_value = PDC_Server_data_io_direct(WRITE, obj_id, region_info, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_data_write_direct() "
                "error with PDC_Server_data_io_direct()\n", pdc_server_rank_g);
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Server reads buffer from storage of one region without client involvement
 *
 * \param  obj_id[IN]            Object ID
 * \param  region_info[IN]       Region info of IO request
 * \param  buf[OUT]              Data buffer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_data_read_direct(uint64_t obj_id, struct PDC_region_info *region_info, void *buf)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    ret_value = PDC_Server_data_io_direct(READ, obj_id, region_info, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_data_read_direct() "
                "error with PDC_Server_data_io_direct()\n", pdc_server_rank_g);
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Create a container
 *
 * \param  in[IN]       Input structure received from client, conatins metadata 
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_create_container(gen_cont_id_in_t *in, gen_cont_id_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *metadata;
    uint32_t *hash_key, i;

    FUNC_ENTER(NULL);

    out->cont_id = 0;

#ifdef ENABLE_TIMING 
    // Timing
    struct timeval  pdc_timer_start;
    struct timeval  pdc_timer_end;
    double ht_total_sec;

    gettimeofday(&pdc_timer_start, 0);
#endif

    /* printf("==PDC_SERVER[%d]: Got container creation request with name: %s\n", */
    /*         pdc_server_rank_g, in->cont_name); */
    /* fflush(stdout); */

    hash_key = (uint32_t*)malloc(sizeof(uint32_t));
    if (hash_key == NULL) {
        printf("Cannot allocate hash_key!\n");
        goto done;
    }
    total_mem_usage_g += sizeof(uint32_t);
    *hash_key = in->hash_value;

    pdc_cont_hash_table_entry_t *lookup_value;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_container_hash_table_mutex_g);
#endif

    if (container_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(container_hash_table_g, hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            
            // Check if there exist container identical to current one
            printf("==PDC_SERVER[%d]: Found existing container with same name [%s]!\n", 
                    pdc_server_rank_g, lookup_value->cont_name);
            out->cont_id = lookup_value->cont_id;
        
        }
        else {
            pdc_cont_hash_table_entry_t *entry = (pdc_cont_hash_table_entry_t*)calloc(1, sizeof(pdc_cont_hash_table_entry_t));
            strcpy(entry->cont_name, in->cont_name);
            entry->n_obj = 0;
            entry->n_allocated = 128;
            entry->obj_ids = (uint64_t*)calloc(entry->n_allocated, sizeof(uint64_t));
            entry->cont_id = PDC_Server_gen_obj_id();

            total_mem_usage_g += sizeof(pdc_cont_hash_table_entry_t);
            total_mem_usage_g += sizeof(uint64_t)*entry->n_allocated;

            // Insert to hash table
            if (hash_table_insert(container_hash_table_g, hash_key, entry) != 1) {
                printf("==PDC_SERVER[%d]: %s - hash table insert failed\n", pdc_server_rank_g, __func__);
                ret_value = FAIL;
            }
            else
                out->cont_id = entry->cont_id;
        }

    }
    else {
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g, __func__);
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_container_hash_table_mutex_g);
    hg_thread_mutex_lock(&pdc_time_mutex_g);
#endif

#ifdef ENABLE_TIMING 
    // Timing
    gettimeofday(&pdc_timer_end, 0);
    ht_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    server_insert_time_g += ht_total_sec;
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_time_mutex_g);
#endif
    

done:
    /* if (hash_key != NULL) */ 
    /*     free(hash_key); */
    FUNC_LEAVE(ret_value);
} // end PDC_Server_create_container



/*
 * Delete a container by name
 *
 * \param  in[IN]       Input structure received from client, conatins metadata 
 * \param  out[OUT]     Output structure to be sent back to the client
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_delete_container_by_name(gen_cont_id_in_t *in, gen_cont_id_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *metadata;
    uint32_t hash_key, i;

    FUNC_ENTER(NULL);

    out->cont_id = 0;

    /* printf("==PDC_SERVER[%d]: Got container creation request with name: %s\n", */
    /*         pdc_server_rank_g, in->data.cont_name); */
    hash_key = in->hash_value;

    pdc_cont_hash_table_entry_t *lookup_value;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_container_hash_table_mutex_g);
#endif

    if (container_hash_table_g != NULL) {
        // lookup
        lookup_value = hash_table_lookup(container_hash_table_g, &hash_key);

        // Is this hash value exist in the Hash table?
        if (lookup_value != NULL) {
            
            // Check if there exist metadata identical to current one
            printf("==PDC_SERVER[%d]: Found existing container with same hash value, name=%s!\n", 
                    pdc_server_rank_g, lookup_value->cont_name);
            out->cont_id = 0;
            goto done;
        
        }
        else {
            // Check if there exist metadata identical to current one
            printf("==PDC_SERVER[%d]: Did not found existing container with same hash value, name=%s!\n", 
                    pdc_server_rank_g, lookup_value->cont_name);
            ret_value = FAIL;
        }

    }
    else {
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g, __func__);
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    // ^ Release hash table lock
    hg_thread_mutex_unlock(&pdc_container_hash_table_mutex_g);
#endif

done:
    FUNC_LEAVE(ret_value);
} // end PDC_Server_delete_container

/*
 * Search a container by name
 *
 * \param  cont_name[IN]       Container name
 * \param  hash_key[IN]       Hash value of container name
 * \param  out[OUT]           Pointer to the result container
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_find_container_by_name(const char *cont_name, pdc_cont_hash_table_entry_t **out)
{
    perr_t ret_value = SUCCEED;
    uint32_t hash_key;

    FUNC_ENTER(NULL);
    if (NULL == cont_name || NULL == out) {
        printf("==PDC_SERVER[%d]: %s - input is NULL! \n", pdc_server_rank_g, __func__);
        goto done;
    }

    if (container_hash_table_g != NULL) {
        // lookup
        /* printf("checking hash table with key=%d\n", hash_key); */
        hash_key = PDC_get_hash_by_name(cont_name);
        *out = hash_table_lookup(container_hash_table_g, &hash_key);
        if (*out != NULL) {
            // Double check with name match
            if (strcmp(cont_name, (*out)->cont_name) != 0) {
                *out = NULL;
            }
        }
    }
    else {
        printf("container_hash_table_g not initialized!\n");
        ret_value = -1;
        goto done;
    }

    if (*out == NULL)
        printf("==PDC_SERVER[%d]: container [%s] not found! \n", pdc_server_rank_g, cont_name);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_find_container_by_name

/*
 * Search a container by obj id 
 *
 * \param  cont_id[IN]        Container  ID
 * \param  out[OUT]           Pointer to the result container
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_find_container_by_id(uint64_t cont_id, pdc_cont_hash_table_entry_t **out)
{
    perr_t ret_value = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry;
    pdc_metadata_t *elt;
    HashTableIterator hash_table_iter;
    int n_entry;
    HashTablePair pair;

    FUNC_ENTER(NULL);

    if (NULL == out) {
        printf("==PDC_SERVER[%d]: %s - input is NULL! \n", pdc_server_rank_g, __func__);
        goto done;
    }

    if (container_hash_table_g != NULL) {
        // Since we only have the obj id, need to iterate the entire hash table
        n_entry = hash_table_num_entries(container_hash_table_g);
        hash_table_iterate(container_hash_table_g, &hash_table_iter);

        while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
            pair = hash_table_iter_next(&hash_table_iter);
            cont_entry = pair.value;
            if (cont_entry->cont_id == cont_id) {
                *out = cont_entry;
                goto done;
            }
        }
    }  
    else {
        printf("==PDC_SERVER[%d]: %s - container_hash_table_g not initialized!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        out = NULL;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_find_container_by_id

/*
 * Add objects to a container 
 *
 * \param  n_obj[IN]           Number of objects to be added/deleted
 * \param  obj_ids[IN]        Pointer to object array with nobj objects
 * \param  cont_id[IN]        Container ID
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_container_add_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id)
{
    perr_t ret_value = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    int realloc_size = 0;

    FUNC_ENTER(NULL);
    ret_value = PDC_Server_find_container_by_id(cont_id, &cont_entry);

    if (cont_entry != NULL) {
        // Check if need to allocate space
        if (cont_entry->n_allocated == 0) {
            cont_entry->n_allocated = PDC_ALLOC_BASE_NUM;
            cont_entry->obj_ids = (uint64_t*)calloc(sizeof(uint64_t), PDC_ALLOC_BASE_NUM);
        }
        else if (cont_entry->n_allocated < cont_entry->n_obj + n_obj) {
            // Extend the allocated space by twice its original size 
            // or twice the n_obj size, whichever greater
            realloc_size = cont_entry->n_allocated > n_obj? cont_entry->n_allocated: n_obj;
            realloc_size *= (sizeof(uint64_t)*2);
            cont_entry->obj_ids = (uint64_t*)realloc(cont_entry->obj_ids, realloc_size);
            cont_entry->n_allocated = realloc_size / sizeof(uint64_t);
        }

        // Append the new ids
        memcpy(cont_entry->obj_ids+cont_entry->n_obj, obj_ids, n_obj*sizeof(uint64_t));
        cont_entry->n_obj += n_obj;
            
        // Debug prints
        /* printf("==PDC_SERVER[%d]: add %d objects to container %" PRIu64 ", total %d !\n", */ 
                /* pdc_server_rank_g, n_obj, cont_id, cont_entry->n_obj - cont_entry->n_deleted); */
        
        /* int i; */
        /* for (i = 0; i < cont_entry->n_obj; i++) */ 
        /*     printf(" %" PRIu64 ",", cont_entry->obj_ids[i]); */
        /* printf("\n"); */
 
        // TODO: find duplicates
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container %" PRIu64 " not found!\n", 
                pdc_server_rank_g, __func__, cont_id);
        ret_value = FAIL;
        goto done;
    }
   

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_container_add_objs

/*
 * Delete objects to a container 
 *
 * \param  n_obj[IN]           Number of objects to be added/deleted
 * \param  obj_ids[IN]        Pointer to object array with n_obj objects
 * \param  cont_id[IN]        Container ID
 *
 * \return Non-negative on success/Negative on failure
 */

perr_t PDC_Server_container_del_objs(int n_obj, uint64_t *obj_ids, uint64_t cont_id)
{
    perr_t ret_value = SUCCEED;
    pdc_cont_hash_table_entry_t *cont_entry = NULL;
    int realloc_size = 0, i, j;
    int n_deletes = 0;

    FUNC_ENTER(NULL);
    ret_value = PDC_Server_find_container_by_id(cont_id, &cont_entry);

    if (cont_entry != NULL) {
        for (i = 0; i < n_obj; i++) {
            for (j = 0; j < cont_entry->n_obj; j++) {
                if (cont_entry->obj_ids[j] == obj_ids[i]) {
                    cont_entry->obj_ids[j] = 0;
                    cont_entry->n_deleted++;
                    n_deletes++;
                    break;
                }
            }
        }
        // Debug print
        printf("==PDC_SERVER[%d]: successfully deleted %d objects!\n", 
                pdc_server_rank_g, n_deletes);

        if (n_deletes != n_obj) {
            printf("==PDC_SERVER[%d]: %s - %d objects are not found to be deleted!\n", 
                    pdc_server_rank_g, __func__, n_obj - n_deletes);
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - container %" PRIu64 " not found!\n", 
                pdc_server_rank_g, __func__, cont_id);
        ret_value = FAIL;
        goto done;
    }

    // Debug prints
    printf("==PDC_SERVER[%d]: After deletion, container %" PRIu64 " has %d objects:\n", 
            pdc_server_rank_g, cont_id, cont_entry->n_obj - cont_entry->n_deleted);
    for (i = 0; i < cont_entry->n_obj; i++) {
        if (cont_entry->obj_ids[i] != 0) {
            printf(" %" PRIu64 ",", cont_entry->obj_ids[i]);
        }
    }
    printf("\n");
 
 
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_container_del_objs

perr_t PDC_Server_get_local_storage_meta_with_one_name(storage_meta_query_one_name_args_t *args)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *meta = NULL;
    region_list_t *region_elt = NULL, *region_head = NULL;
    int region_count = 0, i = 0, j;

    FUNC_ENTER(NULL);

    PDC_Server_search_with_name_timestep(args->name, PDC_get_hash_by_name(args->name), 0, &meta);
    if (meta == NULL) {
        printf("==PDC_SERVER[%d]: No metadata with name [%s] found!\n", pdc_server_rank_g, args->name);
        goto done;
    }

    region_head = meta->storage_region_list_head;

    // Go through all regions and copy to result
    DL_COUNT(region_head, region_elt, region_count);
    args->n_res = region_count;
    args->regions = (region_list_t**)calloc(region_count, sizeof(region_list_t*));

    i = 0;
    // Copy location and offset
    DL_FOREACH(region_head, region_elt) {
        args->regions[i++] = region_elt;
    } 

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_get_local_storage_meta_with_one_name

/*
 * Get the storage metadata of *1* object
 *
 * \param  in[IN/OUT]            inpupt (obj_name)        
 *                               output (n_storage_region, file_path, offset, size)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_get_all_storage_meta_with_one_name(storage_meta_query_one_name_args_t *args)
{
    hg_return_t hg_ret;
    perr_t ret_value = SUCCEED;
    uint32_t server_id = 0;
    hg_handle_t rpc_handle;
    storage_meta_name_query_in_t in;

    accumulate_storage_meta_t *accumulate_meta;

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_name(args->name, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: get storage region info from local server\n",pdc_server_rank_g);
        }

        // Metadata object is local, no need to send update RPC
        // Fill in with storage meta (region_list_t **regions, int n_res)
        ret_value = PDC_Server_get_local_storage_meta_with_one_name(args);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - get local storage location ERROR!\n", pdc_server_rank_g, __func__);
            goto done;
        }

        // Execute callback function immediately
        // cb:      PDC_Server_accumulate_storage_meta
        // cb_args: args
        if (args->cb != NULL) {
            args->cb(args);
        }
    } 
    else {
        // send the name to target server
        
        server_id = PDC_get_server_by_name(args->name, pdc_server_size_g);
        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: %s - will get storage meta from remote server %d\n",
                    pdc_server_rank_g, __func__, server_id);
            fflush(stdout);
        }

        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                    pdc_server_rank_g, server_id);
            ret_value = FAIL;
            goto done;
        }

        // Add current task and relevant data ptrs to s2s task queue, so later when receiving bulk transfer
        // request we know which task that bulk data is needed 
        in.obj_name  = args->name;
        in.origin_id = pdc_server_rank_g;
        in.task_id   = PDC_Server_add_task_to_list(pdc_server_s2s_task_head_g, args->cb, args);
        
        hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, 
                           storage_meta_name_query_register_id_g, &rpc_handle);

        hg_ret = HG_Forward(rpc_handle, pdc_check_int_ret_cb, NULL, &in);
        if (hg_ret != HG_SUCCESS) {
            printf("==PDC_SERVER[%d]: %s - Could not start HG_Forward to server %u\n",
                    pdc_server_rank_g, __func__, server_id);
            HG_Destroy(rpc_handle);
            ret_value = FAIL;
            goto done;
        }
        HG_Destroy(rpc_handle);
    } // end else

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_Server_get_all_storage_meta_with_one_name

/*
 * Accumulate storage metadata until reached n_total, then perform the corresponding callback (Read data)
 *
 * \param  in[IN]               input with type accumulate_storage_meta_t 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_accumulate_storage_meta(storage_meta_query_one_name_args_t *in)
{
    perr_t ret_value = SUCCEED;
    accumulate_storage_meta_t *accu_meta = in->accu_meta;
    int i, j;

    // Add current input to accumulate_storage_meta structure
    accu_meta->storage_meta[accu_meta->n_accumulated++] = in;

    // Start the read when accumulated all storage meta
    if (accu_meta->n_accumulated >= accu_meta->n_total) {
        printf("==PDC_Server[%d]: Retrieved all storage meta: \n", pdc_server_rank_g);

        for (i = 0; i < accu_meta->n_accumulated; i++) {
            for (j = 0; j < accu_meta->storage_meta[i]->n_res; j++) {
                PDC_print_region_list(accu_meta->storage_meta[i]->regions[j]);
                // TODO read data to shm, then send shm info to client 
            }
        }
    }

done:
    // TODO free many things 
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_accumulate_storage_meta


/*
 * Query and read entire objects with a list of object names, received from a client
 * Callback function used when respond to client request
 *
 * \param  callback_info[IN]              Callback info pointer
 *
 * \return Non-negative on success/Negative on failure
 */
hg_return_t PDC_Server_query_read_names(const struct hg_cb_info *callback_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    int i;
    query_read_names_args_t *cb_args= (query_read_names_args_t*) callback_info->arg;
    storage_meta_query_one_name_args_t *query_args = NULL; 

    FUNC_ENTER(NULL);

    printf("==PDC_Server[%d]: Query read obj names:\n", pdc_server_rank_g);
    for (i = 0; i < cb_args->cnt; i++) {
        printf("%s\n", cb_args->obj_names[i]);
    }

    // Temp storage to accumulate all storage meta of the requested objects
    // Each task should have one such structure
    accumulate_storage_meta_t *accmulate_meta = (accumulate_storage_meta_t*)
                                                calloc(1, sizeof(accumulate_storage_meta_t));
    accmulate_meta->n_total = cb_args->cnt;
    accmulate_meta->storage_meta = (storage_meta_query_one_name_args_t**)calloc(cb_args->cnt, 
                                                sizeof(storage_meta_query_one_name_args_t*));

    // Now we need to retrieve their storage metadata, some can be found in local metadata server, 
    // others are stored on remote metadata servers
    for (i = 0; i < cb_args->cnt; i++) {
        query_args = (storage_meta_query_one_name_args_t*)calloc(1, sizeof(storage_meta_query_one_name_args_t));
        query_args->name      = cb_args->obj_names[i];
        query_args->cb        = PDC_Server_accumulate_storage_meta;
        /* query_args->cb_args   = query_args; */
        query_args->accu_meta = accmulate_meta;
        PDC_Server_get_all_storage_meta_with_one_name(query_args);
    }
 
done:
    fflush(stdout);
    FUNC_LEAVE(hg_ret);
} // end of PDC_Server_query_read_names

int PDC_Server_add_task_to_list(pdc_server_task_list_t *target_list, perr_t (*cb)(), void *cb_args)
{
    pdc_server_task_list_t *new_task;
    
    FUNC_ENTER(NULL);

    if (target_list == NULL || cb == NULL) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        return -1;
    }

    new_task = (pdc_server_task_list_t*)calloc(1, sizeof(pdc_server_task_list_t));
    new_task->cb = cb;
    new_task->cb_args = cb_args;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_server_task_mutex_g);
#endif

    new_task->task_id = pdc_server_task_id_g++;
    DL_APPEND(target_list, new_task);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_server_task_mutex_g);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(new_task->task_id);
} // end PDC_Server_add_task_to_list

perr_t PDC_Server_del_task_from_list(pdc_server_task_list_t *target_list, pdc_server_task_list_t *del)
{
    perr_t ret_value;
    pdc_server_task_list_t *tmp;
    FUNC_ENTER(NULL);

    if (target_list == NULL || del == NULL) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_server_task_mutex_g);
#endif

    tmp = del;
    DL_DELETE(target_list, del);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_server_task_mutex_g);
#endif
    free(tmp);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end PDC_Server_del_task_from_list

inline int PDC_Server_is_valid_task_id(int id)
{
    if (id < PDC_SERVER_TASK_INIT_VALUE || id > pdc_server_task_id_g + 100 ) {
        printf("==PDC_SERVER[%d]: id %d is invalid!\n", pdc_server_rank_g, id);
        return -1;
    }
    return 1;
}

pdc_server_task_list_t *PDC_Server_find_task_from_list(pdc_server_task_list_t* list_head, int id)
{
    pdc_server_task_list_t *tmp;
    FUNC_ENTER(NULL);

    if (PDC_Server_is_valid_task_id(id) != 1) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_server_task_mutex_g);
#endif

    DL_FOREACH(list_head, tmp) {
        if (tmp->task_id == id) {
            return tmp;
        }
    }

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_server_task_mutex_g);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(NULL);
} // end PDC_Server_find_task_from_s2s_list

perr_t PDC_Server_del_task_from_list_id(pdc_server_task_list_t *target_list, int id)
{
    perr_t ret_value;
    pdc_server_task_list_t *tmp;
    FUNC_ENTER(NULL);

    if (target_list == NULL || PDC_Server_is_valid_task_id(id) != 1) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&pdc_server_task_mutex_g);
#endif

    tmp = PDC_Server_find_task_from_list(target_list, id);
    DL_DELETE(target_list, tmp);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&pdc_server_task_mutex_g);
#endif
    free(tmp);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end PDC_Server_del_task_from_list


hg_return_t PDC_Server_storage_meta_name_query_bulk_cleanup_cb(const struct hg_cb_info *callback_info)
{
    // TODO: free previously allocated resources

    return HG_SUCCESS;
} // end PDC_Server_storage_meta_name_query_bulk_cleanup_cb 

// Get all storage meta of the object with name and bulk xfer to original requested server  
hg_cb_t PDC_Server_storage_meta_name_query_bulk_respond(storage_meta_name_query_in_t *args)
{
    hg_return_t hg_ret = HG_SUCCESS;
    hg_cb_t hg_cb_ret = HG_SUCCESS;
    perr_t ret_value;
    storage_meta_query_one_name_args_t *query_args;
    hg_handle_t rpc_handle;
    hg_bulk_t bulk_handle;
    bulk_rpc_in_t bulk_rpc_in;
    void **buf_ptrs;
    hg_size_t *buf_sizes;
    uint32_t server_id;
    region_info_transfer_t **region_infos;
    int i, j;
    FUNC_ENTER(NULL);

    // Now metadata object is local
    query_args = (storage_meta_query_one_name_args_t*)calloc(1, sizeof(storage_meta_query_one_name_args_t));
    query_args->name = args->obj_name;

    ret_value = PDC_Server_get_local_storage_meta_with_one_name(query_args);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - get local storage location ERROR!\n", pdc_server_rank_g, __func__);
        goto done;
    }

    // Now the storage meta is stored in query_args->regions;
    server_id = args->origin_id;
    if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
        printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n",
                pdc_server_rank_g, server_id);
        ret_value = FAIL;
        goto done;
    }

    // TODO: init bulk transfer to args->origin_id
    hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr,
                       get_storage_meta_name_query_bulk_result_rpc_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create handle\n");
        ret_value = FAIL;
        goto done;
    }

    int nbuf = 3*query_args->n_res + 1;
    buf_sizes = (size_t*)calloc(sizeof(size_t), nbuf);
    buf_ptrs  = (void**)calloc(sizeof(void*),  nbuf);
    region_infos = (region_info_transfer_t**)calloc(sizeof(region_info_transfer_t*), query_args->n_res);

    // buf_ptrs[0]: task_id
    buf_ptrs[0] = &(args->task_id);

    buf_sizes[0] = sizeof(int);

    // We need path, offset, region info (region_info_transfer_t)
    i = 1;
    for (j = 0; j < query_args->n_res; j++) {
        region_infos[j] = (region_info_transfer_t*)calloc(sizeof(region_info_transfer_t), 1);
        pdc_region_list_t_to_transfer(query_args->regions[j], region_infos[j]);
        buf_ptrs[i  ]  = query_args->regions[j]->storage_location;
        buf_ptrs[i+1]  = &(query_args->regions[j]->offset);
        buf_ptrs[i+2]  = region_infos[j];
        buf_sizes[i  ] = strlen(buf_ptrs[i]) + 1;
        buf_sizes[i+1] = sizeof(uint64_t);
        buf_sizes[i+2] = sizeof(region_info_transfer_t);
        i+=3;
    }

    /* Register memory */
    hg_ret = HG_Bulk_create(hg_class_g, nbuf, buf_ptrs, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        ret_value = FAIL;
        goto done;
    }

    /* Fill input structure */
    bulk_rpc_in.cnt         = query_args->n_res;
    bulk_rpc_in.origin      = pdc_server_rank_g;
    bulk_rpc_in.bulk_handle = bulk_handle;

    // TODO: put ptrs that need to be freed into cb_args
    /* cb_args.bulk_handle = bulk_handle; */
    /* cb_args.rpc_handle  = rpc_handle; */

    /* Forward call to remote addr */
    hg_ret = HG_Forward(rpc_handle, PDC_Server_storage_meta_name_query_bulk_cleanup_cb, NULL, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not forward call\n");
        ret_value = FAIL;
        goto done;
    } 

done:
    fflush(stdout);
    FUNC_LEAVE(hg_cb_ret);
} // end PDC_Server_storage_meta_name_query_bulk_respond

// We have received the storage metadata from remote meta server, which is stored in region_list_head
// Now we want to find the corresponding task the previously requested the meta retrival and attach
// the received storage meta to it.
// Each task was previously created with 
//      cb   = PDC_Server_accumulate_storage_meta
//      args = (storage_meta_query_one_name_args_t *args)
perr_t PDC_Server_proc_storage_meta_bulk(int task_id, int n_regions, region_list_t *region_list_head)
{
    perr_t ret_value = SUCCEED;
    storage_meta_query_one_name_args_t *query_args;
    int i;
    region_list_t *region_elt;
    FUNC_ENTER(NULL);

    pdc_server_task_list_t *task = PDC_Server_find_task_from_list(pdc_server_s2s_task_head_g, task_id);
    if (task == NULL) {
        printf("==PDC_SERVER[%d]: %s - Error getting task %d\n", pdc_server_rank_g, __func__, task_id);
        ret_value = FAIL;
        goto done;
    }

    // Add the result storage regions to accumulate_storage_meta
    query_args = (storage_meta_query_one_name_args_t*)task->cb_args;

    // query_args->regions is not allocated?
    query_args->regions = (region_list_t**)calloc(sizeof(region_list_t), n_regions);
    i = 0;
    query_args->n_res = n_regions;
    DL_FOREACH(region_list_head, region_elt) {
        query_args->regions[i++] = region_elt;
    }

    ret_value = PDC_Server_accumulate_storage_meta(query_args);
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}


// END OF PDC_SERVER.C
