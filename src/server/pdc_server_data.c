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
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/shm.h>
#include <sys/mman.h>

#include "config.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "utlist.h"
#include "pdc_public.h"
#include "pdc_interface.h"
#include "pdc_client_server_common.h"
#include "pdc_server_data.h"
#include "pdc_server_metadata.h"
#include "pdc_server.h"
#include "pdc_hist.h"

// Global object region info list in local data server
data_server_region_t *dataserver_region_g = NULL;

int pdc_buffered_bulk_update_total_g = 0;
int pdc_nbuffered_bulk_update_g      = 0;
int n_check_write_finish_returned_g  = 0;
int buffer_read_request_total_g      = 0;
int buffer_write_request_total_g     = 0;
int buffer_write_request_num_g       = 0;
int buffer_read_request_num_g        = 0;
int is_server_direct_io_g            = 0;

uint64_t total_mem_cache_size_mb_g   = 0;
uint64_t max_mem_cache_size_mb_g     = 0;
int cache_percentage_g               = 0;
int current_read_from_cache_cnt_g    = 0;
int total_read_from_cache_cnt_g      = 0;
FILE *pdc_cache_file_ptr_g           = NULL;
char pdc_cache_file_path_g[ADDR_MAX];

query_task_t *query_task_list_head_g = NULL;

/*
 * Callback function of the server to lookup clients, also gets the confirm message from client.
 *
 * \param  callback_info[IN]        Mercury callback info pointer 
 *
 * \return Non-negative on success/Negative on failure
 */
/* static hg_return_t */
/* server_lookup_client_rpc_cb(const struct hg_cb_info *callback_info) */
/* { */
/*     hg_return_t ret_value = HG_SUCCESS; */
/*     server_lookup_client_out_t output; */
/*     server_lookup_args_t *server_lookup_args = (server_lookup_args_t*) callback_info->arg; */
/*     hg_handle_t handle = callback_info->info.forward.handle; */

/*     FUNC_ENTER(NULL); */

/*     ret_value = HG_Get_output(handle, &output); */
/*     if (ret_value != HG_SUCCESS) { */
/*         printf("==PDC_SERVER[%d]: server_lookup_client_rpc_cb - error HG_Get_output\n", pdc_server_rank_g); */
/*         server_lookup_args->ret_int = -1; */
/*         goto done; */
/*     } */
/*     /1* if (is_debug_g == 1) { *1/ */
/*     /1*     printf("==PDC_SERVER[%d]: server_lookup_client_rpc_cb,  got output from client=%d\n", *1/ */ 
/*     /1*             pdc_server_rank_g, output.ret); *1/ */
/*     /1* } *1/ */
/*     server_lookup_args->ret_int = output.ret; */

/* done: */
/*     HG_Free_output(handle, &output); */
/*     FUNC_LEAVE(ret_value); */
/* } */

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

    if (stripe_count > 16) 
        stripe_count = 16;

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
    snprintf(cmd, ADDR_MAX,  "lfs setstripe -S %dM -c %d -i %d %s", stripe_size_MB, stripe_count, index, tmp);

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
perr_t PDC_Server_get_reg_lock_status_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;
    pdc_metadata_t *meta = NULL;
    server_lookup_args_t *lookup_args;
    hg_handle_t handle;
    get_reg_lock_status_out_t output;
}
*/
/*
 * Check if two regions are the same
 *
 * \param  a[IN]     Pointer to the first region
 * \param  b[IN]     Pointer to the second region
 *
 * \return 1 if they are the same/-1 otherwise
 */
int is_region_identical(region_list_t *a, region_list_t *b)
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
            elt->reg_dirty_from_buf= 1;
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
    int ndim;
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

    ndim = in->region.ndim;

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
    hg_thread_mutex_lock(&region_struct_mutex_g);
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
        DL_APPEND(dataserver_region_g, new_obj_reg);
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&region_struct_mutex_g);
#endif

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
    // Go through all existing locks to check for region lock
    DL_FOREACH(new_obj_reg->region_lock_head, elt1) {
        if (PDC_is_same_region_list(elt1, request_region) == 1) {
            found_lock = 1;
            if(in->lock_mode == BLOCK) {
                ret_value = FAIL;
//              printf("region is currently lock\n");
//              fflush(stdout);
                request_region->lock_handle = *handle;
                DL_APPEND(new_obj_reg->region_lock_request_head, request_region);
            }
            else
                error = 1; 
        }
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif

    if(found_lock == 0) {
    // check if the lock region is used in buf map function 
    tmp = (region_list_t *)malloc(sizeof(region_list_t));
    DL_FOREACH(new_obj_reg->region_buf_map_head, eltt) {
        pdc_region_transfer_t_to_list_t(&(eltt->remote_region_unit), tmp);
        if(PDC_is_same_region_list(tmp, request_region) == 1) {
            request_region->reg_dirty_from_buf = 1;
            hg_atomic_incr32(&(request_region->buf_map_refcount));
        }
    }
    free(tmp);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
    // No overlaps found
    DL_APPEND(new_obj_reg->region_lock_head, request_region);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&lock_list_mutex_g);
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
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&lock_request_mutex_g);
#endif
    DL_FOREACH_SAFE(new_obj_reg->region_lock_request_head, elt, tmp) {
        if (is_region_identical(request_region, elt) == 1) {
            out.ret = 1;
            HG_Respond(elt->lock_handle, NULL, NULL, &out);
            HG_Destroy(elt->lock_handle);
            DL_DELETE(new_obj_reg->region_lock_request_head, elt);
            free(elt); 
        }
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&lock_request_mutex_g);
#endif
    free(request_region);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Data_Server_region_release(region_lock_in_t *in, region_lock_out_t *out)
{
    perr_t ret_value = SUCCEED;
    int ndim;
    region_list_t *tmp1, *tmp2;
    region_list_t request_region;
    int found = 0;
    data_server_region_t *obj_reg = NULL;
     
    FUNC_ENTER(NULL);
    
    /* printf("==PDC_SERVER: received lock request,                                \ */
    /*         obj_id=%" PRIu64 ", op=%d, ndim=%d, start=%" PRIu64 " count=%" PRIu64 " stride=%d\n", */ 
    /*         in->obj_id, in->lock_op, in->region.ndim, */ 
    /*         in->region.start_0, in->region.count_0, in->region.stride_0); */

    ndim = in->region.ndim;

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
    if(obj_reg == NULL) {
        ret_value = FAIL;
        printf("==PDC_SERVER[%d]: requested release object does not exist\n", pdc_server_rank_g);
        goto done;
    }
    /* printf("==PDC_SERVER: releasing lock ... "); */
    // Find the lock region in the list and remove it
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
    DL_FOREACH_SAFE(obj_reg->region_lock_head, tmp1, tmp2) {
        if (is_region_identical(&request_region, tmp1) == 1) {
            // Found the requested region lock, remove from the linked list
            found = 1;
            DL_DELETE(obj_reg->region_lock_head, tmp1);
            free(tmp1);
            tmp1 = NULL;
        }
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&lock_list_mutex_g);
#endif
    // Request release lock region not found
    if (found == 0) {
        ret_value = FAIL; 
        printf("==PDC_SERVER[%d]: requested release region/object does not exist\n", pdc_server_rank_g);
        goto done;
    }
    out->ret = 1;

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
 * Check if two region are the same
 *
 * \param  a[IN]     Pointer of the first region 
 * \param  b[IN]     Pointer of the second region 
 *
 * \return 1 if same/0 otherwise
 */
int region_list_path_offset_cmp(region_list_t *a, region_list_t *b) 
{
    int ret_value;
    if (NULL == a || NULL == b) {
        printf("  %s - NULL input!\n", __func__);
        return -1;
    }

    ret_value = strcmp(a->storage_location, b->storage_location);
    if (0 == ret_value) 
        ret_value = a->offset > b->offset ? 1: -1;

    return ret_value;
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
    hg_thread_mutex_lock(&data_buf_map_mutex_g);
#endif
    DL_FOREACH_SAFE(target_obj->region_buf_map_head, elt, tmp) {
        if(in->remote_obj_id==elt->remote_obj_id && region_is_identical(in->remote_region, elt->remote_region_unit)) {
#ifdef ENABLE_MULTITHREAD
            // wait for work to be done, then free
            hg_thread_mutex_lock(&(elt->bulk_args->work_mutex));
            while (!elt->bulk_args->work_completed)
                hg_thread_cond_wait(&(elt->bulk_args->work_cond), &(elt->bulk_args->work_mutex));
            elt->bulk_args->work_completed = 0;
            hg_thread_mutex_unlock(&(elt->bulk_args->work_mutex));  //per bulk_args
#endif
            free(elt->remote_data_ptr);  
            HG_Addr_free(info->hg_class, elt->local_addr);
            HG_Bulk_free(elt->local_bulk_handle);
#ifdef ENABLE_MULTITHREAD
            hg_thread_mutex_destroy(&(elt->bulk_args->work_mutex));
            hg_thread_cond_destroy(&(elt->bulk_args->work_cond)); 
            free(elt->bulk_args); 
#endif
            DL_DELETE(target_obj->region_buf_map_head, elt);
            free(elt);
            
        }
    }
    if(target_obj->region_buf_map_head == NULL && pdc_server_rank_g == 0) {
        close(target_obj->fd);
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&data_buf_map_mutex_g);
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

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
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
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&addr_valid_mutex_g);
#endif

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

    /* if (is_debug_g == 1) { */
    /*     printf("==PDC_SERVER[%d]: Testing connection to remote server %d: %s\n", */
    /*             pdc_server_rank_g, remote_server_id, pdc_remote_server_info_g[remote_server_id].addr_string); */
    /*     fflush(stdout); */
    /* } */

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
    int error = 0;
 
    FUNC_ENTER(NULL);

    if((uint32_t)pdc_server_rank_g == in->meta_server_id) {
        target_meta = find_metadata_by_id(in->remote_obj_id);
        if(target_meta == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "===PDC META SERVER: cannot retrieve object metadata");
        }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
        DL_FOREACH_SAFE(target_meta->region_buf_map_head, elt, tmp) {

            if(in->remote_obj_id==elt->remote_obj_id && region_is_identical(in->remote_region, elt->remote_region_unit)) {
//                HG_Bulk_free(elt->local_bulk_handle);
//                HG_Addr_free(info->hg_class, elt->local_addr);
                DL_DELETE(target_meta->region_buf_map_head, elt);
                free(elt);
            }
        }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&meta_buf_map_mutex_g);
#endif
        out.ret = 1;
        HG_Respond(*handle, NULL, NULL, &out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }
    else {
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
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
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&addr_valid_mutex_g);
#endif
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
#ifdef ENABLE_LUSTRE
    int stripe_count, stripe_size;
#endif

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&region_struct_mutex_g);
#endif
    new_obj_reg = PDC_Server_get_obj_region(in->remote_obj_id);
    if(new_obj_reg == NULL) {
        new_obj_reg = (data_server_region_t *)malloc(sizeof(struct data_server_region_t));
        if(new_obj_reg == NULL)
            PGOTO_ERROR(NULL, "PDC_SERVER: PDC_Server_insert_buf_map_region() allocates new object failed");
        new_obj_reg->obj_id = in->remote_obj_id;
        new_obj_reg->region_lock_head = NULL;
        new_obj_reg->region_buf_map_head = NULL;
        new_obj_reg->region_lock_request_head = NULL;
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
        snprintf(storage_location, ADDR_MAX, "%.200s/pdc_data/%" PRIu64 "/server%d/s%04d.bin",
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
        new_obj_reg->fd = open(storage_location, O_RDWR|O_CREAT, 0666);
        if(new_obj_reg->fd == -1){
            printf("==PDC_SERVER[%d]: open %s failed\n", pdc_server_rank_g, storage_location);
            goto done;
        }
        DL_APPEND(dataserver_region_g, new_obj_reg);
    }
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&region_struct_mutex_g);
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
    buf_map_ptr->remote_ndim = in->ndim;
    buf_map_ptr->remote_unit = in->remote_unit;
    buf_map_ptr->remote_region_unit = in->remote_region_unit;
    buf_map_ptr->remote_region_nounit = in->remote_region_nounit;
    buf_map_ptr->remote_data_ptr = data_ptr;

#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_lock(&data_buf_map_mutex_g);
#endif
    DL_APPEND(new_obj_reg->region_buf_map_head, buf_map_ptr);
#ifdef ENABLE_MULTITHREAD 
    hg_thread_mutex_unlock(&data_buf_map_mutex_g);
#endif

    DL_FOREACH(new_obj_reg->region_lock_head, elt_reg) {
        if (PDC_is_same_region_list(elt_reg, request_region) == 1) {
            hg_atomic_incr32(&(elt_reg->buf_map_refcount));
//            printf("mapped region is locked \n");
//            fflush(stdout); 
        }
    }
    ret_value = buf_map_ptr;

    free(request_region);

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

void *PDC_Server_get_region_buf_ptr(pdcid_t obj_id, region_info_transfer_t region)
{
    void *ret_value = NULL;
    data_server_region_t *target_obj = NULL, *elt = NULL;
    region_buf_map_t *tmp;

    FUNC_ENTER(NULL);

    if(dataserver_region_g == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - object list is NULL");
    DL_FOREACH(dataserver_region_g, elt) {
        if(obj_id == elt->obj_id)
            target_obj = elt;
    }
    if(target_obj == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - cannot locate object");

    DL_FOREACH(target_obj->region_buf_map_head, tmp) {
        if (is_region_transfer_t_identical(&region, &(tmp->remote_region_unit)) == 1) {
            ret_value = tmp->remote_data_ptr;
            break;
        }
    }
    if(ret_value == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - region data pointer is NULL");

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

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
    lookup_args = (struct buf_map_server_lookup_args_t*) callback_info->arg;
    server_id = lookup_args->server_id;
    tranx_args = lookup_args->buf_map_args;
    handle = tranx_args->handle;

    pdc_remote_server_info_g[server_id].addr = callback_info->info.lookup.addr;
    pdc_remote_server_info_g[server_id].addr_valid = 1;

    if (pdc_remote_server_info_g[server_id].addr == NULL) {
        printf("==PDC_SERVER[%d]: %s - remote server addr is NULL\n", pdc_server_rank_g, __func__);
        error = 1;
        goto done;
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&addr_valid_mutex_g);
#endif

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

    /* if (is_debug_g == 1) { */
    /*     printf("==PDC_SERVER[%d]: Testing connection to remote server %d: %s\n", pdc_server_rank_g, remote_server_id, pdc_remote_server_info_g[remote_server_id].addr_string); */
    /*     fflush(stdout); */
    /* } */

    handle = transfer_args->handle;
    lookup_args = (struct buf_map_server_lookup_args_t *)malloc(sizeof(struct buf_map_server_lookup_args_t));
    lookup_args->server_id = remote_server_id;
    lookup_args->buf_map_args = transfer_args;
    hg_ret = HG_Addr_lookup(hg_context_g, buf_map_lookup_remote_server_cb, lookup_args, pdc_remote_server_info_g[remote_server_id].addr_string, HG_OP_ID_IGNORE);
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
            PGOTO_ERROR(FAIL, "PDC_SERVER: PDC_Meta_Server_buf_map() find_metadata_by_id FAILED!");
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
//        buf_map_ptr->local_addr = new_buf_map_ptr->local_addr;
//        buf_map_ptr->local_bulk_handle = new_buf_map_ptr->local_bulk_handle;

        buf_map_ptr->remote_obj_id = new_buf_map_ptr->remote_obj_id;
        buf_map_ptr->remote_reg_id = new_buf_map_ptr->remote_reg_id;
        buf_map_ptr->remote_client_id = new_buf_map_ptr->remote_client_id;
        buf_map_ptr->remote_ndim = new_buf_map_ptr->remote_ndim;
        buf_map_ptr->remote_unit = new_buf_map_ptr->remote_unit;
        buf_map_ptr->remote_region_unit = new_buf_map_ptr->remote_region_unit;
        buf_map_ptr->remote_region_nounit = new_buf_map_ptr->remote_region_nounit;
        buf_map_ptr->remote_data_ptr = new_buf_map_ptr->remote_data_ptr;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
        DL_APPEND(target_meta->region_buf_map_head, buf_map_ptr);
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&meta_buf_map_mutex_g);
#endif

        out.ret = 1;
        HG_Respond(*handle, NULL, NULL, &out);
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }
    else {
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
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
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&addr_valid_mutex_g);
#endif
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

    if (client_id >= (int32_t)pdc_client_num_g) {
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
perr_t PDC_Server_close_shm(region_list_t *region, int is_remove)
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

    if (is_remove == 1) {
        /* remove the shared memory segment from the file system */
        if (shm_unlink(region->shm_addr) == -1) {
            ret_value = FAIL;
            goto done;
            printf("==PDC_SERVER[%d]: Error removing %s\n", pdc_server_rank_g, region->shm_addr);
        }
    }

    region->buf           = NULL;
    region->shm_fd        = -1;
    region->is_shm_closed = 1;
    memset(region->shm_addr, 0, ADDR_MAX);
    total_mem_cache_size_mb_g -= (region->data_size / 1048576);

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

    /* if (is_debug_g ) { */
    /*     printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_cb - received from client %d with %d\n", */ 
    /*             pdc_server_rank_g, lookup_args->client_id, output.ret); */
    /* } */
    lookup_args->ret_int = output.ret;

done:
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
    char tmp_shm[ADDR_MAX];
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
        snprintf(tmp_shm, ADDR_MAX, "%d", client_id * 10);
        in.shm_addr   = tmp_shm;
        /* in.shm_addr   = " "; */
    }
    else 
        in.shm_addr   = shm_addr;

    /* if (is_debug_g ) { */
        /* printf("==PDC_SERVER: PDC_Server_notify_io_complete_to_client shm_addr = [%s]\n", in.shm_addr); */
        /* printf("==PDC_SERVER[%d]: PDC_Server_notify_io_complete_to_client %d \n", pdc_server_rank_g, client_id); */
        /* fflush(stdout); */
    /* } */
    
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

// Generic function to check the return value (RPC receipt) is 1
hg_return_t PDC_Server_notify_client_multi_io_complete_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    pdc_int_ret_t output;
    notify_multi_io_args_t *args = NULL;

    FUNC_ENTER(NULL);
    hg_handle_t handle = callback_info->info.forward.handle;
    args = (notify_multi_io_args_t*) callback_info->arg;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - Error with HG_Get_output\n", pdc_server_rank_g, __func__);
        goto done;
    }

    if (output.ret != 1) {
        printf("==PDC_SERVER[%d]: %s - Return value [%d] is NOT expected\n", 
                pdc_server_rank_g, __func__, output.ret);
    }               

    // TODO: Cache to BB if needed

done:
    if (args) {
        free(args->buf_sizes);
        free(args->buf_ptrs);
        free(args);
    }

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
perr_t PDC_Server_notify_client_multi_io_complete(uint32_t client_id, int client_seq_id, int n_completed, 
                                                  region_list_t *completed_region_list)
{
    perr_t                ret_value     = SUCCEED;
    hg_return_t           hg_ret        = HG_SUCCESS;
    hg_handle_t           rpc_handle;
    hg_bulk_t             bulk_handle;
    void                **buf_ptrs;
    hg_size_t            *buf_sizes;
    bulk_rpc_in_t         bulk_rpc_in;
    int                   i;
    region_list_t        *region_elt;
    notify_multi_io_args_t *bulk_args;

    FUNC_ENTER(NULL);

    /* printf("==PDC_SERVER[%d]: %s - notify client_id %d io complete\n", pdc_server_rank_g, __func__, client_id); */
    /* fflush(stdout); */

    if (client_id >= (uint32_t)pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: %s - client_id %d invalid\n", pdc_server_rank_g, __func__, client_id);
        ret_value = FAIL;
        goto done;
    }

    while (pdc_client_info_g[client_id].addr_valid != 1) {
        ret_value = PDC_Server_lookup_client(client_id);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - lookup client failed!\n", pdc_server_rank_g, __func__);
            goto done;
        }
    }

    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr,
                       notify_client_multi_io_complete_rpc_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create handle\n");
        ret_value = FAIL;
        goto done;
    }

    // Send the shm_addr + total data size to client
    buf_sizes = (hg_size_t*)calloc(sizeof(hg_size_t), n_completed*2);
    buf_ptrs  = (void**)calloc(sizeof(void*),  n_completed*2);

    i = 0;
    DL_FOREACH(completed_region_list, region_elt) {
        buf_ptrs[i]              = region_elt->shm_addr;
        buf_sizes[i]             = strlen(buf_ptrs[i]) + 1;
        buf_ptrs[n_completed+i]  = (void*)&(region_elt->data_size);
        buf_sizes[n_completed+i] = sizeof(uint64_t);
        i++;
    }

    /* Register memory */
    hg_ret = HG_Bulk_create(hg_class_g, n_completed*2, buf_ptrs, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        ret_value = FAIL;
        goto done;
    }

    /* Fill input structure */
    bulk_rpc_in.cnt         = n_completed;
    bulk_rpc_in.origin      = pdc_server_rank_g;
    bulk_rpc_in.seq_id      = client_seq_id;
    bulk_rpc_in.bulk_handle = bulk_handle;

    bulk_args = (notify_multi_io_args_t*)calloc(1, sizeof(notify_multi_io_args_t));
    bulk_args->bulk_handle = bulk_handle;
    bulk_args->buf_sizes   = buf_sizes;
    bulk_args->buf_ptrs    = buf_ptrs;
    bulk_args->region_list = completed_region_list;

    /* Forward call to remote addr */
    hg_ret = HG_Forward(rpc_handle, PDC_Server_notify_client_multi_io_complete_cb, bulk_args, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not forward call\n");
        ret_value = FAIL;
        goto done;
    } 

    /* printf("==PDC_SERVER[%d]: %s forwarded bulk handle!\n", pdc_server_rank_g, __func__); */

done:
    fflush(stdout);
    HG_Destroy(rpc_handle);
    FUNC_LEAVE(ret_value);
} // End PDC_Server_notify_client_multi_io_complete

perr_t PDC_Close_cache_file()
{
    if (pdc_cache_file_ptr_g != NULL) 
        fclose(pdc_cache_file_ptr_g);
    return SUCCEED;
}
/*
 * Cache a region to BB, write and update
 *
 * \param  in[IN]       Region pointer, must have valid buf (data pointer)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_cache_region_to_BB(region_list_t *region)
{
    perr_t ret_value = SUCCEED;
    uint64_t write_bytes, offset;

    FUNC_ENTER(NULL);
    // Write to BB

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start, pdc_timer_end;
    double cache_total_sec;
    gettimeofday(&pdc_timer_start, 0);
    #endif

    if (pdc_cache_file_ptr_g == NULL) {
        char *bb_data_path = getenv("PDC_BB_LOC");
        if (bb_data_path != NULL) {
            sprintf(pdc_cache_file_path_g, "%s/PDCcacheBB.%d", bb_data_path, pdc_server_rank_g);
        }
        else {
            char *user_specified_data_path = getenv("PDC_DATA_LOC");
            if (user_specified_data_path != NULL)
                bb_data_path = user_specified_data_path;
            else {
                bb_data_path = getenv("SCRATCH");
                if (bb_data_path == NULL)
                    bb_data_path = ".";
            }
            sprintf(pdc_cache_file_path_g, "%s/PDCcacheBB.%d", bb_data_path, pdc_server_rank_g);
            printf("==PDC_SERVER[%d]: %s - No PDC_BB_LOC specified, use [%s]!\n", 
                    pdc_server_rank_g, __func__, bb_data_path);
        }

        // Debug print
        /* printf("==PDC_SERVER[%d]: %s - opening file in BB [%s]!\n", */ 
        /*         pdc_server_rank_g, __func__, pdc_cache_file_path_g); */

        pdc_cache_file_ptr_g = fopen(pdc_cache_file_path_g, "ab");
        if (NULL == pdc_cache_file_ptr_g) {
            printf("==PDC_SERVER[%d]: fopen failed [%s]\n", pdc_server_rank_g, pdc_cache_file_path_g);
            ret_value = FAIL;
            goto done;
        }
        n_fopen_g++;
    } // End open cache file

    // Get the current write offset
    offset = ftell(pdc_cache_file_ptr_g);

    // Actual write (append)
    printf("==PDC_SERVER[%d]: %s - appending %" PRIu64 " bytes to BB\n", 
            pdc_server_rank_g, __func__, region->data_size);
    write_bytes = fwrite(region->buf, 1, region->data_size, pdc_cache_file_ptr_g);
    if (write_bytes != region->data_size) {
        printf("==PDC_SERVER[%d]: fwrite to [%s] FAILED, size %" PRIu64", actual writeen %" PRIu64 "!\n",
                    pdc_server_rank_g, region->storage_location, region->data_size, write_bytes);
        ret_value= FAIL;
        goto done;
    }
    n_fwrite_g++;
    /* fwrite_total_MB += write_bytes/1048576.0; */

    #ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    double region_write_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    server_write_time_g += region_write_time;
    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: fwrite %" PRIu64 " bytes, %.2fs\n", 
                pdc_server_rank_g, write_bytes, region_write_time);
    }
    #endif

    // Prepare update
    strcpy(region->storage_location, pdc_cache_file_path_g);
    region->offset = offset;
    strcpy(region->cache_location, pdc_cache_file_path_g);
    region->cache_offset = offset;

    // Update storage meta
    ret_value = PDC_Server_update_region_storagelocation_offset(region, PDC_UPDATE_CACHE);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: failed to update region storage info!\n", pdc_server_rank_g);
        goto done;
    }

    // Close shm after writing to BB
    ret_value = PDC_Server_close_shm(region, 0);
    region->is_data_ready = 0;
    region->is_io_done    = 0;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // PDC_Server_cache_region_to_BB

hg_return_t PDC_cache_region_to_bb_cb (const struct hg_cb_info *callback_info) 
{
    perr_t ret;
    hg_return_t ret_value = HG_SUCCESS;
    server_read_check_out_t *out;

    FUNC_ENTER(NULL);

    out = (server_read_check_out_t*) callback_info->arg;
    ret = PDC_Server_cache_region_to_BB(out->region);
    if (ret != SUCCEED) 
        printf("==PDC_SERVER[%d]: %s - Error with PDC_Server_cache_region_to_BB\n", pdc_server_rank_g, __func__);

    if (out != NULL) {
        free(out);
        /* if (out->shm_addr != NULL) */ 
        /*     free(out->shm_addr); */
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
perr_t PDC_Server_read_check(data_server_read_check_in_t *in, server_read_check_out_t *out)
{
    perr_t ret_value = SUCCEED;
    pdc_data_server_io_list_t *io_elt = NULL, *io_target = NULL;
    region_list_t *region_elt = NULL;
    region_list_t r_target;
    /* uint32_t i; */

    FUNC_ENTER(NULL);

/* volatile int dbg_sleep_g = 1; */
/* printf("== attach %d\n", getpid()); */
/* fflush(stdout); */
/* while(dbg_sleep_g ==1) { */
/*     dbg_sleep_g = 1; */
/*     sleep(1); */
/* } */

    pdc_metadata_t meta;
    PDC_metadata_init(&meta);
    pdc_transfer_t_to_metadata_t(&in->meta, &meta);

    PDC_init_region_list(&r_target);
    pdc_region_transfer_t_to_list_t(&(in->region), &r_target);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_read_list_mutex_g);
#endif
    // Iterate io list, find current request
    DL_FOREACH(pdc_data_server_read_list_head_g, io_elt) {
        if (meta.obj_id == io_elt->obj_id) {
            io_target = io_elt;
            break;
        }
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_read_list_mutex_g);
#endif

    if (NULL == io_target) {
        printf("==PDC_SERVER[%d]: %s - No existing io request with same obj_id %" PRIu64 " found!\n", 
                    pdc_server_rank_g, __func__, meta.obj_id);
        out->ret = -1;
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
    DL_FOREACH(io_target->region_list_head, region_elt) {
        if (region_list_cmp(region_elt, &r_target) == 0) {
            // Found previous io request
            found_region = 1;
            out->ret = region_elt->is_data_ready;
            out->region = NULL;
            out->is_cache_to_bb = 0;
            if (region_elt->is_data_ready == 1) {
                out->shm_addr = calloc(sizeof(char), ADDR_MAX);
                if (strlen(region_elt->shm_addr) == 0)
                    printf("==PDC_SERVER[%d]: %s - found shm_addr is NULL!\n", pdc_server_rank_g, __func__);
		else strcpy(out->shm_addr, region_elt->shm_addr);

                /* printf("==PDC_SERVER[%d]: %s - found shm_addr [%s]\n", */ 
                /*         pdc_server_rank_g, __func__,region_elt->shm_addr); */
                out->region = region_elt;
                // If total cache in memory exceeds threshold, cache the data in BB when available
                if (total_mem_cache_size_mb_g >= max_mem_cache_size_mb_g && 
                        strstr(region_elt->cache_location, "PDCcacheBB") == NULL) 
                    out->is_cache_to_bb = 1;
            }
            ret_value = SUCCEED;
            goto done;
        }
    }

    if (found_region == 0) {
        printf("==PDC_SERVER[%d]: %s -  No io request with same region found!\n", pdc_server_rank_g, __func__);
        PDC_print_region_list(&r_target);
        out->ret = -1;
        goto done;
    }

done:
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
perr_t PDC_Server_data_read_to_shm(region_list_t *region_list_head)
{
    perr_t ret_value = FAIL;
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
    struct timeval  pdc_timer_start, pdc_timer_end;
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
/* static hg_return_t */
/* PDC_Server_get_storage_info_cb (const struct hg_cb_info *callback_info) */
/* { */
/*     hg_return_t ret_value; */
/*     hg_handle_t handle; */
/*     pdc_serialized_data_t serialized_output; */
/*     get_storage_info_single_out_t single_output; */
/*     uint32_t n_loc; */
/*     int is_single_region = 1; */
/*     get_storage_loc_args_t *cb_args; */

/*     FUNC_ENTER(NULL); */

/*     cb_args = (get_storage_loc_args_t*) callback_info->arg; */
/*     handle = callback_info->info.forward.handle; */

/*     ret_value = HG_Get_output(handle, &single_output); */
/*     if (ret_value != HG_SUCCESS) { */
/*         // Returned multiple regions */
/*         // TODO: test this */
/*         is_single_region = 0; */
/*         ret_value = HG_Get_output(handle, &serialized_output); */
/*         if (ret_value != HG_SUCCESS) { */
/*             printf("==PDC_SERVER[%d]: %s - ERROR HG_Get_output\n", pdc_server_rank_g, __func__); */
/*             cb_args->void_buf = NULL; */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } // end if */ 
/*         else { */
/*             if(PDC_unserialize_region_lists(serialized_output.buf,cb_args->region_lists,&n_loc)!= SUCCEED) { */
/*                 printf("==PDC_SERVER[%d]: ERROR with PDC_unserialize_region_lists, buf size %lu\n", */
/*                                       pdc_server_rank_g, strlen((char*)serialized_output.buf)); */
/*                 *(cb_args->n_loc) = 0; */
/*                 ret_value = FAIL; */
/*                 goto done; */
/*             } */

/*             *(cb_args->n_loc) = n_loc; */
/*             cb_args->void_buf = serialized_output.buf; */
/*             if (is_debug_g == 1) { */
/*                 printf(  "==PDC_SERVER[%d]: %s - : received %ls storage info\n", */
/*                         pdc_server_rank_g, __func__, cb_args->n_loc); */
/*             } */
/*         } // end of else (with multiple storage regions) */
/*     } */
/*     else { */
/*         // with only one matching storage region */
/*         *(cb_args->n_loc) = 1; */
/*         pdc_region_transfer_t_to_list_t(&single_output.region_transfer, cb_args->region_lists[0]); */
/*         strcpy(cb_args->region_lists[0]->storage_location, single_output.storage_loc); */
/*         cb_args->region_lists[0]->offset = single_output.file_offset; */
/*     } */

/*     /1* if (is_debug_g == 1) { *1/ */
/*     /1*     printf("==PDC_SERVER[%d]: got %u locations of region\n", *1/ */
/*     /1*             pdc_server_rank_g, *cb_args->n_loc); *1/ */
/*     /1*     fflush(stdout); *1/ */
/*     /1* } *1/ */


/* done: */
/*     if (cb_args->cb != NULL) */ 
/*         cb_args->cb(cb_args->cb_args); */
    
/*     fflush(stdout); */
/*     /1* s2s_send_work_todo_g--; *1/ */
/*     if (is_single_region == 1) */ 
/*         HG_Free_output(handle, &single_output); */
/*     else */
/*         HG_Free_output(handle, &serialized_output); */
    
/*     FUNC_LEAVE(ret_value); */
/* } */

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
    else all_requests = local_region_transfer;

#ifdef ENABLE_MPI
    /* printf("==PDC_SERVER[%d]: will MPI_Reduce update region to server %d, with data size %d\n", */
    /*         pdc_server_rank_g, server_id, data_size); */
    /* fflush(stdout); */

    // Gather the requests from all data servers to metadata server
    // equivalent to all data server send requests to metadata server
    MPI_Gather(&local_region_transfer, nrequest_per_server*data_size, MPI_CHAR, 
               all_requests, nrequest_per_server*data_size, MPI_CHAR, server_id, MPI_COMM_WORLD);
#endif

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

#ifdef ENABLE_MPI
    MPI_Bcast(request_overlap_cnt, total_request_cnt, MPI_INT, server_id, MPI_COMM_WORLD);
    MPI_Bcast(send_bytes, pdc_server_size_g, MPI_INT, server_id, MPI_COMM_WORLD);
    MPI_Bcast(displs, pdc_server_size_g, MPI_INT, server_id, MPI_COMM_WORLD);

    MPI_Scatterv(send_buf, send_bytes, displs, MPI_CHAR, 
                 result_storage_meta, send_bytes[pdc_server_rank_g], MPI_CHAR, 
                 server_id, MPI_COMM_WORLD);
#else
    result_storage_meta = send_buf;
#endif

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

#ifdef ENABLE_MPI
    if (server_id == (uint32_t)pdc_server_rank_g) {
        if (all_requests) free(all_requests);
        if (overlap_regions_2d) free(overlap_regions_2d);
        if (send_buf) free(send_buf);
    }
    if (result_storage_meta) free(result_storage_meta);
#else
        if (overlap_regions_2d) free(overlap_regions_2d);
        if (send_buf) free(send_buf);
#endif
    if (send_bytes) free(send_bytes);
    if (displs) free(displs);
    if (request_overlap_cnt) free(request_overlap_cnt);

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
/* perr_t PDC_Server_get_storage_location_of_region_with_cb(region_list_t *request_region, uint32_t *n_loc, */ 
/*                                                          region_list_t **overlap_region_loc, */ 
/*                                                          perr_t (*cb)(), void *args) */
/* { */
/*     perr_t ret_value = SUCCEED; */
/*     hg_return_t hg_ret; */
/*     uint32_t server_id = 0; */
/*     pdc_metadata_t *region_meta = NULL; */
/*     get_storage_info_in_t in; */
/*     hg_handle_t get_storage_info_handle; */
/*     get_storage_loc_args_t cb_args; */

/*     FUNC_ENTER(NULL); */

/*     if (request_region == NULL || overlap_region_loc == NULL || overlap_region_loc[0] == NULL || n_loc == NULL) { */
/*         printf("==PDC_SERVER[%d]: %s () input has NULL value!\n", pdc_server_rank_g, __func__); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */

/*     region_meta = request_region->meta; */
/*     if (region_meta == NULL) { */
/*         printf("==PDC_SERVER[%d]: %s - request region has NULL metadata\n", pdc_server_rank_g, __func__); */
/*         ret_value = FAIL; */
/*         goto done; */
/*     } */
/*     server_id = PDC_get_server_by_obj_id(region_meta->obj_id, pdc_server_size_g); */
/*     if (server_id == (uint32_t)pdc_server_rank_g) { */
/*         if (is_debug_g == 1) { */
/*             printf("==PDC_SERVER[%d]: get storage region info from local server\n",pdc_server_rank_g); */
/*         } */
/*         // Metadata object is local, no need to send update RPC */
/*         ret_value = PDC_Server_get_local_storage_location_of_region(region_meta->obj_id, */
/*                         request_region, n_loc, overlap_region_loc); */
/*         if (ret_value != SUCCEED) { */
/*             printf("==PDC_SERVER[%d]: %s - get local storage location ERROR!\n", pdc_server_rank_g, __func__); */
/*             goto done; */
/*         } */

/*         // Execute callback function immediately */
/*         if (cb != NULL) { */
/*             cb(args); */
/*         } */
/*     } // end if */
/*     else { */
/*         if (is_debug_g == 1) { */
/*             printf("==PDC_SERVER[%d]: will get storage region info from remote server %d\n", */
/*                     pdc_server_rank_g, server_id); */
/*             fflush(stdout); */
/*         } */

/*         if (PDC_Server_lookup_server_id(server_id) != SUCCEED) { */
/*             printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", */
/*                     pdc_server_rank_g, server_id); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */

/*         HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, get_storage_info_register_id_g, */
/*                   &get_storage_info_handle); */


/*         if (request_region->meta == NULL) { */
/*             printf("==PDC_SERVER: NULL request_region->meta"); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         in.obj_id = request_region->meta->obj_id; */
/*         pdc_region_list_t_to_transfer(request_region, &in.req_region); */

/*         cb_args.cb   = cb; */
/*         cb_args.cb_args = args; */
/*         cb_args.region_lists = overlap_region_loc; */
/*         cb_args.n_loc = n_loc; */

/*         hg_ret = HG_Forward(get_storage_info_handle, PDC_Server_get_storage_info_cb, &cb_args, &in); */
/*         if (hg_ret != HG_SUCCESS) { */
/*             printf("PDC_Client_update_metadata_with_name(): Could not start HG_Forward() to server %u\n", */
/*                         server_id); */
/*             HG_Destroy(get_storage_info_handle); */
/*             ret_value = FAIL; */
/*             goto done; */
/*         } */
/*         HG_Destroy(get_storage_info_handle); */
/*     } // end else */

/* done: */
/*     fflush(stdout); */
/*     FUNC_LEAVE(ret_value); */
/* } // PDC_Server_get_storage_location_of_region_with_cb */

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
    struct timeval  pdc_timer_start, pdc_timer_end;
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

        if (region_elt->shm_fd > 0 && region_elt->buf!= NULL) 
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
            printf("==PDC_SERVER[%d]: %s - Shared memory open failed [%s]!\n", 
                    pdc_server_rank_g, __func__, region_elt->shm_addr);
            ret_value = FAIL;
            goto done;
        }
        /* else { */
        /*     if (is_debug_g == 1) */ 
        /*         printf("==PDC_SERVER[%d]: Shared memory open [%s]!\n", pdc_server_rank_g, region_elt->shm_addr); */
        /* } */

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
        if (region_elt->is_io_done == 1) 
            ret_value = PDC_Server_close_shm(region_elt, 1);
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

    pdc_data_server_io_list_t *io_list_elt, *io_list = NULL, *io_list_target = NULL;
    region_list_t *region_elt = NULL, *region_tmp;
    int real_bb_cnt = 0, real_lustre_cnt = 0;
    int write_to_bb_cnt = 0;
    int count;
    size_t i;

    FUNC_ENTER(NULL);

    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start, pdc_timer_end;
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
        
        /* print("==PDC_SERVER[%d]: received %d/%d write requests\n", */ 
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
    if (io_info->io_type == WRITE) 
        hg_thread_mutex_lock(&data_write_list_mutex_g);
    else if (io_info->io_type == READ)
        hg_thread_mutex_lock(&data_read_list_mutex_g);
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
    if (io_info->io_type == WRITE) 
        hg_thread_mutex_unlock(&data_write_list_mutex_g);
    else if (io_info->io_type == READ)
        hg_thread_mutex_unlock(&data_read_list_mutex_g);
#endif

    // If not found, create and insert one to the list
    if (NULL == io_list_target) {
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

        if (io_info->io_type == WRITE) {
            char *data_path = NULL;
            char *bb_data_path = NULL;
            char *user_specified_data_path = getenv("PDC_DATA_LOC");
            if (user_specified_data_path != NULL) 
                data_path = user_specified_data_path;
            else {
                data_path = getenv("SCRATCH");
                if (data_path == NULL)
                    data_path = ".";
            }

            bb_data_path = getenv("PDC_BB_LOC");
            if (bb_data_path != NULL)
                snprintf(io_list_target->bb_path, ADDR_MAX, "%.200s/pdc_data/cont_%" PRIu64 "", 
                        bb_data_path, io_info->meta.cont_id);

            // Auto generate a data location path for storing the data
            snprintf(io_list_target->path, ADDR_MAX, "%.200s/pdc_data/cont_%" PRIu64 "", data_path, io_info->meta.cont_id);
        }

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

    int has_read_cache = 0;
    // Need to check if there is already one region in the list, and update accordingly
    DL_FOREACH_SAFE(io_list_target->region_list_head, region_elt, region_tmp) {
        // Only compares the start and count to see if two are identical
        if (PDC_is_same_region_list(&(io_info->region), region_elt) == 1) {
            // free the shm if read
            if (io_info->io_type == READ) {
                has_read_cache = 1;
                /* if (PDC_Server_close_shm(region_elt, 1) != SUCCEED) { */
                /*     printf("==PDC_SERVER[%d]: PDC_Server_data_io_via_shm() - ERROR close shm!\n", */ 
                /*             pdc_server_rank_g); */
                /* } */
            }
            else {
                // Remove that region
                DL_DELETE(io_list_target->region_list_head, region_elt);
                free(region_elt);
            }
        }
    }

    if (has_read_cache != 1) {
        // append current request region to the io list
        region_list_t *new_region = (region_list_t*)calloc(1, sizeof(region_list_t));
        if (new_region == NULL) {
            printf("==PDC_SERVER: ERROR allocating new_region!\n");
            ret_value = FAIL;
            goto done;
        }
        pdc_region_list_t_deep_cp(&(io_info->region), new_region);
        DL_APPEND(io_list_target->region_list_head, new_region);
        if (is_debug_g == 1) {
            DL_COUNT(io_list_target->region_list_head, region_elt, count);
            printf("==PDC_SERVER[%d]: Added 1 to IO request list, obj_id=%" PRIu64 ", %d total\n",
                    pdc_server_rank_g, new_region->meta->obj_id, count);
            PDC_print_region_list(new_region);
        }
    }

    // Write
    if (io_info->io_type == WRITE && buffer_write_request_num_g >= buffer_write_request_total_g &&
        buffer_write_request_total_g != 0) {

        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: received all %d requests, starts writing.\n",
                    pdc_server_rank_g, buffer_write_request_total_g);
            fflush(stdout);
        }

        // Perform IO for all requests in each io_list (of unique obj_id)
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

                snprintf(region_elt->storage_location, ADDR_MAX, "%.200s/server%d/s%04d.bin",
                        io_list_elt->path, pdc_server_rank_g, pdc_server_rank_g);
                real_lustre_cnt++;
                /* PDC_print_region_list(region_elt); */

                // If BB is enabled, then overwrite with BB path with the right number of servers
                if (write_to_bb_percentage_g > 0 ) {
                    if (strcmp(io_list_elt->bb_path, "") == 0 ||io_list_elt->bb_path[0] == 0) {
                        printf("==PDC_SERVER[%d]: Error with BB path [%s]!\n",
                                pdc_server_rank_g, io_list_elt->bb_path);
                    }
                    else {
                        if (pdc_server_rank_g % 2 == 0) {
                            // Half of the servers writes to BB first
                            if (curr_cnt < write_to_bb_cnt) {
                                snprintf(region_elt->storage_location, ADDR_MAX, "%.200s/server%d/s%04d.bin",
                                        io_list_elt->bb_path, pdc_server_rank_g, pdc_server_rank_g);
                                real_bb_cnt++;
                                real_lustre_cnt--;
                            }
                        }
                        else {
                            // Others write to Lustre first
                            if (curr_cnt >= io_list_elt->total - write_to_bb_cnt) {
                                snprintf(region_elt->storage_location, ADDR_MAX, "%.200s/server%d/s%04d.bin",
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

            ret_value = PDC_Server_data_write_from_shm(io_list_elt->region_list_head);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_Server_data_write_from_shm FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }
            /* else { */
            /*     // mark this list is done, to avoid future rework */
            /*     if (is_debug_g == 1) { */
            /*         printf("==PDC_SERVER[%d]: finished data write for 1 request!\n", pdc_server_rank_g); */
            /*     } */
            /* } */

        } // end for io_list
        buffer_write_request_num_g = 0;
        buffer_write_request_total_g = 0;
    } // End write
    else if (io_info->io_type == READ && buffer_read_request_num_g == buffer_read_request_total_g &&
        buffer_read_request_total_g != 0) {
        // Read
        // TODO TEMPWORK
        cache_percentage_g            = io_info->cache_percentage;
        current_read_from_cache_cnt_g = 0;
        total_read_from_cache_cnt_g   = buffer_read_request_total_g * cache_percentage_g / 100;
        if (pdc_server_rank_g == 0) {
            printf("==PDC_SERVER[%d]: cache percentage %d%% read_from_cache %d/%d\n", 
               pdc_server_rank_g, cache_percentage_g, current_read_from_cache_cnt_g, total_read_from_cache_cnt_g);
        }

        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: received all %d requests, starts reading.\n",
                    pdc_server_rank_g, buffer_read_request_total_g);
        }
        DL_FOREACH(pdc_data_server_read_list_head_g, io_list_elt) {
            if (cache_percentage_g == 100) {
                // check if everything in this list has been cached
                int all_cached = 1;
                DL_FOREACH(io_list_elt->region_list_head, region_elt) {
                    if (region_elt->is_io_done != 1) {
                        all_cached = 0;
                        break;
                    }
                }

                if (all_cached == 1) {
                    /* printf("==PDC_SERVER[%d]: has cache!\n", pdc_server_rank_g); */
                    continue;
                }
            }
            
            // Sort the list so it is ordered by client id
            /* DL_SORT(io_list_elt->region_list_head, region_list_cmp_by_client_id); */

            /* if (is_debug_g == 1) { */
            /*     DL_COUNT(io_list_elt->region_list_head, region_elt, count); */
            /*     printf("==PDC_SERVER[%d]: read request list %d total, first is:\n", */
            /*             pdc_server_rank_g, count); */
            /*     PDC_print_region_list(io_list_elt->region_list_head); */
            /* } */

            // Storage location is obtained later
            /* status = PDC_Server_data_read_to_shm(io_list_elt->region_list_head, io_list_elt->obj_id); */
            ret_value = PDC_Server_data_read_to_shm(io_list_elt->region_list_head);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_Server_data_read_to_shm FAILED!\n", pdc_server_rank_g);
                goto done;
            }
        }
        buffer_read_request_num_g = 0;
        buffer_read_request_total_g = 0;
    } // End read

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    server_io_elapsed_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

done:
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
perr_t PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id, int type)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *target_meta = NULL;
    /* pdc_metadata_t *region_meta = NULL; */
    region_list_t  *region_elt = NULL, *new_region = NULL;
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
    DL_FOREACH(target_meta->storage_region_list_head, region_elt) {
        if (PDC_is_same_region_list(region_elt, region) == 1) {
            /* printf("==PDC_SERVER[%d]: overwrite existing region location/offset\n", pdc_server_rank_g); */
            /* printf("==PDC_SERVER[%d]: original:\n", pdc_server_rank_g); */
            /* PDC_print_storage_region_list(region_elt); */
            /* printf("==PDC_SERVER[%d]: new:\n", pdc_server_rank_g); */
            /* PDC_print_storage_region_list(region); */
            /* fflush(stdout); */

            // Update location and offset
            if (type == PDC_UPDATE_CACHE) {
                memcpy(region_elt->cache_location, region->storage_location, sizeof(char)*ADDR_MAX);
                region_elt->cache_offset = region->offset;
            }
            else if (type == PDC_UPDATE_STORAGE) {
                memcpy(region_elt->storage_location, region->storage_location, sizeof(char)*ADDR_MAX);
                region_elt->offset = region->offset;
            }
            else {
                printf("==PDC_SERVER[%d]: %s - error with update type %d!\n", pdc_server_rank_g, __func__, type);
                break;
            }

            region_elt->data_size = pdc_get_region_size(region_elt);
            update_success = 1;

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

        new_region->data_size = pdc_get_region_size(new_region);
        new_region->meta      = target_meta;
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

    /* if (is_debug_g) { */
    /*     printf("==PDC_SERVER[%d]: %s: ret=%d\n", pdc_server_rank_g, __func__, output.ret); */
    /*     fflush(stdout); */
    /* } */
    lookup_args->ret_int = output.ret;

    update_remote_region_count_g++;
done:
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
perr_t PDC_Server_update_region_storagelocation_offset(region_list_t *region, int type)
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
        ret_value = PDC_Server_update_local_region_storage_loc(region, region_meta->obj_id, type);
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

        in.obj_id           = region->meta->obj_id;
        in.offset           = region->offset;
        in.storage_location = region->storage_location;
        in.type             = type;
        pdc_region_list_t_to_transfer(region, &(in.region));

        lookup_args.rpc_handle = update_region_loc_handle;
        hg_ret = HG_Forward(update_region_loc_handle, PDC_Server_update_region_loc_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS) {
            printf("==PDC_SERVER[%d]: %s - HG_Forward() to server %d FAILED\n",
                    pdc_server_rank_g, __func__, server_id);
            HG_Destroy(update_region_loc_handle);
            ret_value = FAIL;
            goto done;
        }
        HG_Destroy(update_region_loc_handle);
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
        /* if (is_debug_g == 1) { */
        /*     printf("==PDC_SERVER[%d]: created new region from bulk_ptr\n", pdc_server_rank_g); */
        /*     PDC_print_storage_region_list(new_region); */
        /* } */


        // The bulk data are regions of same obj_id, and the corresponding metadata must be local
        target_meta = find_metadata_by_id(obj_id);
        if (target_meta == NULL) {
            printf("==PDC_SERVER[%d]: %s - FAIL to get storage metadata\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        new_region->meta   = target_meta;
        new_region->obj_id = target_meta->obj_id;

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
                    /* PDC_print_metadata(target_meta); */
                    /* printf("==PDC_SERVER[%d]: original:\n", pdc_server_rank_g); */
                    /* PDC_print_storage_region_list(region_elt); */
                    /* printf("==PDC_SERVER[%d]: new:\n", pdc_server_rank_g); */
                    /* PDC_print_storage_region_list(new_region); */
                    fflush(stdout);
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

    uint32_t server_id = 0;
    hg_handle_t rpc_handle;
    hg_bulk_t bulk_handle;
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
    uint64_t overlap_start[DIM_MAX] = {0}, overlap_count[DIM_MAX] = {0};
    uint64_t buf_start[DIM_MAX] = {0};
    uint64_t storage_start_physical[DIM_MAX] = {0};
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

    if (req_count && req_count[0] == 0) {
        is_all_selected = 1;
        req_start[0] = 0;
        req_count[0] = storage_count[0];
        total_bytes  = storage_count[0];
        goto all_select;
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
        /* if (is_debug_g == 1) { */
        /*     printf("==PDC_SERVER[%d]: overlap_start[%" PRIu64 "]=%" PRIu64 ", " */
        /*             "req_start[%" PRIu64 "]=%" PRIu64 "  \n", pdc_server_rank_g, i, overlap_start[i], */
        /*             i, req_start[i]); */
        /* } */

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

        /* if (is_debug_g == 1) { */
        /*     printf("==PDC_SERVER[%d]: buf_offset=%" PRIu64 ", req_count[%" PRIu64 "]=%" PRIu64 "  " */
        /*             "buf_start[%" PRIu64 "]=%" PRIu64 " \n", */
        /*             pdc_server_rank_g, buf_offset, i, req_count[i], i, buf_start[i]); */
        /* } */
    }

    /* if (is_debug_g == 1) { */
    /*     for (i = 0; i < ndim; i++) { */
    /*         printf("==PDC_SERVER[%d]: overlap start %" PRIu64 ", " */
    /*                "storage_start  %" PRIu64 ", req_start %" PRIu64 " \n", */
    /*                pdc_server_rank_g, overlap_start[i], storage_start[i], req_start[i]); */
    /*     } */

    /*     for (i = 0; i < ndim; i++) { */
    /*         printf("==PDC_SERVER[%d]: dim=%" PRIu32 ", read with storage start %" PRIu64 "," */
    /*                 " to buffer offset %" PRIu64 ", of size %" PRIu64 " \n", */
    /*                 pdc_server_rank_g, ndim, storage_start_physical[i], buf_start[i], overlap_count[i]); */
    /*     } */
    /*     fflush(stdout); */
    /* } */

    // Check if the entire storage region is selected
    is_all_selected = 1;
    for (i = 0; i < ndim; i++) {
        if (overlap_start[i] != storage_start[i] || overlap_count[i] != storage_count[i]) {
            is_all_selected = -1;
            break;
        }
    }

    /* printf("ndim = %" PRIu64 ", is_all_selected=%d\n", ndim, is_all_selected); */

    /* #ifdef ENABLE_TIMING */
    /* struct timeval  pdc_timer_start; */
    /* struct timeval  pdc_timer_end; */
    /* gettimeofday(&pdc_timer_start, 0); */
    /* #endif */


all_select:
    // TODO: additional optimization to check if any dimension is entirely selected
    if (ndim == 1 || is_all_selected == 1) {
        // Can read the entire storage region at once

        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start1, pdc_timer_end1;
        gettimeofday(&pdc_timer_start1, 0);
        #endif

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

        read_bytes = fread(buf+buf_offset, 1, total_bytes, fp);

        #ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end1, 0);
        double region_read_time1 = PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: fseek + fread %" PRIu64 " bytes, %.2fs\n", 
                    pdc_server_rank_g, read_bytes, region_read_time1);
            fflush(stdout);
        }
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

            /* if (is_debug_g == 1) { */
            /*     printf("read count: %" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n", */
            /*             overlap_count[0], overlap_count[1], overlap_count[2]); */
            /* } */

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

/* #ifdef ENABLE_TIMING */
    /* gettimeofday(&pdc_timer_end, 0); */
    /* double region_read_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end); */
    /* server_read_time_g += region_read_time; */
    /* printf("==PDC_SERVER[%d]: %d fseek + fread total %" PRIu64 " bytes, %.4fs\n", */ 
    /*         pdc_server_rank_g, n_contig_read, read_bytes, region_read_time); */
/* #endif */


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
 * Read data based on the region structure, which should already have its overlapping storage metadata
 * included.
 *
 * \param  read_region[IN/OUT]       Region pointer
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_read_one_region(region_list_t *read_region)
{
    perr_t ret_value = SUCCEED;
    size_t read_bytes = 0;
    size_t total_read_bytes = 0;
    uint32_t n_storage_regions = 0;
    region_list_t  *region_elt;
    FILE *fp_read = NULL;
    char *prev_path = NULL;
    int is_shm_created = 0, is_read_succeed = 0;
    #ifdef ENABLE_TIMING
    double fopen_time;
    #endif

    FUNC_ENTER(NULL);

    if (read_region->access_type != READ || read_region->n_overlap_storage_region == 0 ||
        read_region->overlap_storage_regions == NULL ) {

        printf("==PDC_SERVER[%d]: %s - Error with input\n", pdc_server_rank_g, __func__);
        PDC_print_region_list(read_region);
        goto done;
    }

    // Create the shm segment to read data into
    snprintf(read_region->shm_addr, ADDR_MAX, "/PDC%d_%d", pdc_server_rank_g, rand());
    ret_value = PDC_create_shm_segment(read_region);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - Error with shared memory creation\n", pdc_server_rank_g, __func__);
        goto done;
    }
    is_shm_created = 1;
    total_mem_cache_size_mb_g += (read_region->data_size/1048576);

    // Now for each storage region that overlaps with request region,
    // read with the corresponding offset and size
    DL_FOREACH(read_region->overlap_storage_regions, region_elt) {

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: Found overlapping storage regions %d\n",
                    pdc_server_rank_g, n_storage_regions);
            /* printf("=========================================\n"); */
            /* PDC_print_storage_region_list(overlap_regions[i]); */
            /* printf("=========================================\n"); */
        }

        if (region_elt->storage_location[0] == 0) {
            printf("==PDC_SERVER[%d]: empty overlapping storage location \n", pdc_server_rank_g);
            PDC_print_storage_region_list(region_elt);
            fflush(stdout);
            continue;
        }

        // Debug print
        /* printf("==PDC_SERVER[%d]: going to read following overlap storage region(s)\n", pdc_server_rank_g); */
        /* PDC_print_region_list(region_elt); */

        // If a new file needs to be opened
        if (prev_path == NULL || 
            strcmp(region_elt->storage_location, prev_path) != 0) {

            if (fp_read != NULL)  {
                fclose(fp_read);
                fp_read = NULL;
            }

            #ifdef ENABLE_TIMING
            struct timeval  pdc_timer_start2, pdc_timer_end2;
            gettimeofday(&pdc_timer_start2, 0);
            #endif

            /* printf("==PDC_SERVER[%d]: opening [%s]\n", pdc_server_rank_g, */ 
            /*                         overlap_regions[i]->storage_location); */
            /* fflush(stdout); */

            fp_read = fopen(region_elt->storage_location, "rb");
            if (fp_read == NULL) {
                printf("==PDC_SERVER[%d]: fopen failed [%s]\n",
                        pdc_server_rank_g, read_region->storage_location);
                continue;
            }
            n_fopen_g++;

            #ifdef ENABLE_TIMING
            gettimeofday(&pdc_timer_end2, 0);
            fopen_time = PDC_get_elapsed_time_double(&pdc_timer_start2, &pdc_timer_end2);
            server_fopen_time_g += fopen_time;
            #endif
        }

        // Request: elt->start/count
        // Storage: region_elt->start/count
        ret_value = PDC_Server_read_overlap_regions(read_region->ndim, read_region->start, 
                    read_region->count, region_elt->start, 
                    region_elt->count, fp_read, 
                    region_elt->offset, read_region->buf, &read_bytes);

        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: error with PDC_Server_read_overlap_regions\n",
                    pdc_server_rank_g);
            fclose(fp_read);
            fp_read = NULL;
            continue;
        }
        total_read_bytes += read_bytes;
        is_read_succeed = 1;

        prev_path = region_elt->storage_location;
    } // end of for all overlapping storage regions for one request region

    #ifdef ENABLE_TIMING
    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: Read data total size %" PRIu64 ", fopen time: %.3f\n",
                pdc_server_rank_g, total_read_bytes, fopen_time);
        fflush(stdout);
    }
    #endif

    read_region->is_data_ready = 1;
    read_region->is_io_done = 1;

done:
    // Close shm if read failed
    if (is_shm_created == 1 && is_read_succeed == 0) {
        PDC_Server_close_shm(read_region, 1);
        read_region->is_data_ready = 0;
        read_region->is_io_done = 0;
    }
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // End PDC_Server_read_one_region


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
    bulk_xfer_data_t *bulk_data = NULL;
    int nregion_in_bulk_update = 0;
    /* int is_read_only = 1; */
    /* int has_write = 0; */

    FUNC_ENTER(NULL);

    if (NULL == region_list_head) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // For read requests, it's better to aggregate read requests from all node-local clients
    // and query once, rather than query one by one, so we aggregate at the beginning
    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start1, pdc_timer_end1;
    gettimeofday(&pdc_timer_start1, 0);
    #endif

    /* ret_value = PDC_Server_get_storage_location_of_region_with_cb(region_elt); */
    DL_FOREACH(region_list_head, region_elt) {
        if (region_elt->access_type == READ) {

            /* #ifdef ENABLE_CACHE */
            if (region_elt->is_io_done == 1) 
                continue;
            /* #endif */
            ret_value = PDC_Server_get_storage_location_of_region_mpi(region_elt);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_Server_get_storage_location_of_region failed!\n",
                                                                            pdc_server_rank_g);
                goto done;
            }
        }
    }

    #ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end1, 0);
    server_get_storage_info_time_g += PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
    #endif

    // Iterate over all region IO requests and perform actual IO
    // We have all requests from node local clients, now read them from storage system
    DL_FOREACH(region_list_head, region_elt) {
        if (region_elt->access_type == READ) {
            if (region_elt->is_io_done == 1 && region_elt->is_shm_closed != 1) {
                printf("==PDC_SERVER[%d]: found cached data!\n", pdc_server_rank_g);
                if(region_elt->access_type == READ && current_read_from_cache_cnt_g < total_read_from_cache_cnt_g)
                    current_read_from_cache_cnt_g++;
                else
                    continue;
            }


            // Prepare the shared memory for transfer back to client
            snprintf(region_elt->shm_addr, ADDR_MAX, "/PDC%d_%d", pdc_server_rank_g, rand());
            ret_value = PDC_create_shm_segment(region_elt);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - Error with shared memory creation\n", pdc_server_rank_g, __func__);
                continue;
            }
            /* printf("==PDC_SERVER[%d]: %s - shared memory created [%s]\n", pdc_server_rank_g, __func__, region_elt->shm_addr); */
            /* printf("==PDC_SERVER[%d]: %s - cache_location [%s]\n", pdc_server_rank_g, __func__, region_elt->cache_location); */

            // Check if the region is cached to BB
            if (strstr(region_elt->cache_location, "PDCcacheBB") != NULL) {

                #ifdef ENABLE_TIMING
                struct timeval  pdc_timer_start11, pdc_timer_end11;
                gettimeofday(&pdc_timer_start1, 0);
                #endif
                if (prev_path == NULL || strcmp(region_elt->cache_location, prev_path) != 0) {
                    if (fp_read != NULL)  
                        fclose(fp_read);
                    fp_read = fopen(region_elt->cache_location, "rb");
                    n_fopen_g++;
                }
                offset = ftell(fp_read);
                if (offset < region_elt->cache_offset) 
                    fseek(fp_read, region_elt->cache_offset-offset, SEEK_CUR);
                else
                    fseek(fp_read, region_elt->cache_offset, SEEK_SET);
                
                if (region_elt->data_size == 0) {
                    printf("==PDC_SERVER[%d]: %s - region data_size is 0\n", pdc_server_rank_g, __func__);
                    continue;
                }

                // Debug print
                /* printf("==PDC_SERVER[%d]: %s - read from [%s] offset %" PRIu64 "\n", */ 
                /*         pdc_server_rank_g, __func__, region_elt->cache_location, region_elt->cache_offset); */

                read_bytes = fread(region_elt->buf, 1, region_elt->data_size, fp_read);
                if (read_bytes != region_elt->data_size) {
                    printf("==PDC_SERVER[%d]: %s - read size %" PRIu64 " is not expected %" PRIu64 "\n", 
                            pdc_server_rank_g, __func__, read_bytes, region_elt->data_size);
                    continue;
                }
                #ifdef ENABLE_TIMING
                gettimeofday(&pdc_timer_end11, 0);
                server_read_time_g += PDC_get_elapsed_time_double(&pdc_timer_start11, &pdc_timer_end11);
                #endif

                n_read_from_bb_g++;
                read_from_bb_size_g += read_bytes;
            }
            else {
                // Now for each storage region that overlaps with request region,
                // read with the corresponding offset and size
                for (i = 0; i < region_elt->n_overlap_storage_region; i++) {

                    /* if (is_debug_g == 1) { */
                    /*     printf("==PDC_SERVER[%d]: Found overlapping storage regions %d\n", */
                    /*             pdc_server_rank_g, n_storage_regions); */
                    /*     /1* printf("=========================================\n"); *1/ */
                    /*     /1* PDC_print_storage_region_list(overlap_regions[i]); *1/ */
                    /*     /1* printf("=========================================\n"); *1/ */
                    /* } */

                    // If a new file needs to be opened
                    if (prev_path == NULL || 
                        strcmp(region_elt->overlap_storage_regions[i].storage_location, prev_path) != 0) {

                        if (fp_read != NULL)  {
                            fclose(fp_read);
                            fp_read = NULL;
                        }

                        #ifdef ENABLE_TIMING
                        struct timeval  pdc_timer_start2, pdc_timer_end2;
                        gettimeofday(&pdc_timer_start2, 0);
                        #endif

                        /* printf("==PDC_SERVER[%d]: opening [%s]\n", pdc_server_rank_g, */ 
                        /*                         overlap_regions[i]->storage_location); */
                        /* fflush(stdout); */

                        if (strlen(region_elt->overlap_storage_regions[i].storage_location) > 0) {
                            fp_read = fopen(region_elt->overlap_storage_regions[i].storage_location, "rb");
                            n_fopen_g++;
                        }
                        else {
                            printf("==PDC_SERVER[%d]: %s - NULL storage location\n",
                                    pdc_server_rank_g, __func__);
                            fp_read = NULL;
                        }

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
                        printf("==PDC_SERVER[%d]: error with PDC_Server_read_overlap_regions\n", pdc_server_rank_g);
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

                /* region_elt->offset = offset; */
                offset += total_read_bytes;

            } // end else read from storage
            if (is_debug_g == 1) {
                printf("==PDC_SERVER[%d]: Read data total size %zu\n",
                        pdc_server_rank_g, total_read_bytes);
                fflush(stdout);
            }
            /* printf("Read iteration: size %" PRIu64 "\n", region_elt->data_size); */

            region_elt->is_data_ready = 1;
            region_elt->is_io_done = 1;

        } // end of READ
        else if (region_elt->access_type == WRITE) {
            if (region_elt->is_io_done == 1) 
                continue;

            // Assumes all regions are written to one file
            if (region_elt->storage_location[0] == 0) {
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

                    /* if (is_debug_g == 1) { */
                    /*     printf("==PDC_SERVER[%d]: close and open new file [%s]\n", */
                    /*               pdc_server_rank_g, region_elt->storage_location); */
                    /* } */
                }

                // TODO: need to recycle file space in cases of data update and delete

                #ifdef ENABLE_TIMING
                struct timeval  pdc_timer_start4, pdc_timer_end4;
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
            struct timeval  pdc_timer_start5, pdc_timer_end5;
            gettimeofday(&pdc_timer_start5, 0);
            #endif

            // Actual write (append)
            write_bytes = fwrite(region_elt->buf, 1, region_elt->data_size, fp_write);
            if (write_bytes != region_elt->data_size) {
                printf("==PDC_SERVER[%d]: fwrite to [%s] FAILED, region off %" PRIu64 ", size %" PRIu64 ", "
                       "actual writeen %zu!\n",
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
            if (is_debug_g == 1) {
                printf("==PDC_SERVER[%d]: fwrite %" PRIu64 " bytes, %.2fs\n", 
                        pdc_server_rank_g, write_bytes, region_write_time);
            }
            #endif

            /* fclose(fp_write); */

            if (is_debug_g == 1) {
                printf("Write data offset: %" PRIu64 ", size %" PRIu64 ", to [%s]\n",
                                              offset, region_elt->data_size, region_elt->storage_location);
                /* printf("Write data buf: [%.*s]\n", region_elt->data_size, (char*)region_elt->buf); */
            }
            region_elt->is_data_ready = 1;
            region_elt->offset = offset;

            ret_value = PDC_Server_update_region_storagelocation_offset(region_elt, PDC_UPDATE_STORAGE);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: failed to update region storage info!\n", pdc_server_rank_g);
                goto done;
            }


            // add current region to the update bulk buf
            /* if (NULL == bulk_data) { */
            /*     bulk_data = (bulk_xfer_data_t*)calloc(1, sizeof(bulk_xfer_data_t)); */
            /*     PDC_init_bulk_xfer_data_t(bulk_data); */
            /* } */

            /* ret_value = PDC_Server_add_region_storage_meta_to_bulk_buf(region_elt, bulk_data); */
            /* if (ret_value != SUCCEED) { */
            /*     printf("==PDC_SERVER[%d]: %s - add_region_storage_meta_to_bulk_buf FAILED!", */ 
            /*             pdc_server_rank_g, __func__); */
            /*     goto done; */
            /* } */
            /* nregion_in_bulk_update++; */

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

        /* if (region_elt->overlap_storage_regions!= NULL) { */
        /*     free(region_elt->overlap_storage_regions); */
        /*     region_elt->overlap_storage_regions = NULL; */
        /* } */
        
    } // end DL_FOREACH region IO request (region)

    /* printf("==PDC_SERVER[%d]: %s- IO finished\n", pdc_server_rank_g, __func__); */
    /* fflush(stdout); */

    // Buffer the region storage metadata update 
    /* update_storage_meta_list_t *tmp_alloc; */
    /* if (nregion_in_bulk_update != 0) { */
    /*     tmp_alloc = (update_storage_meta_list_t*)calloc(sizeof(update_storage_meta_list_t), 1); */
    /*     tmp_alloc->storage_meta_bulk_xfer_data = bulk_data; */
    /*     DL_APPEND(pdc_update_storage_meta_list_head_g, tmp_alloc); */
    /*     pdc_nbuffered_bulk_update_g++; */
    /* } */
    /* int n_region = 0; */
    /* DL_COUNT(region_list_head, region_elt, n_region); */
    /* printf("==PDC_SERVER[%d]: after writes done, will update %d regions \n", pdc_server_rank_g, n_region); */
    /* fflush(stdout); */

    /* int iter = 0, n_write_region = 0; */
    /* if (is_read_only != 1) { */

    /*     #ifdef ENABLE_TIMING */
    /*     struct timeval  pdc_timer_end; */
    /*     struct timeval  pdc_timer_start; */
    /*     gettimeofday(&pdc_timer_start, 0); */
    /*     #endif */

        // update all regions that are just written through bulk transfer
        /* ret_value = PDC_Server_update_region_storage_meta_bulk_with_cb(&bulk_data); */
        /* if (ret_value != SUCCEED) { */
        /*     printf("==PDC_SERVER[%d]: %s - update storage info FAILED!", pdc_server_rank_g, __func__); */
        /*     goto done; */
        /* } */

        /* // Update all regions location info to the metadata server */
        /* DL_FOREACH(region_list_head, region_elt) { */

        /*     if (region_elt->access_type == WRITE) { */
        /*         n_write_region++; */
        /*         // Update metadata with the location and offset */
        /*         /1* printf("==PDC_SERVER[%d]: update storage info obj_id: %" PRIu64 " " *1/ */
        /*         /1*        "ndim: %" PRIu64 ", offset %" PRIu64 " \n", pdc_server_rank_g, *1/ */ 
        /*         /1*        region_elt->meta->obj_id, region_elt->ndim,  region_elt->start[0]); *1/ */     

        /*         ret_value = PDC_Server_update_region_storagelocation_offset(region_elt PDC_UPDATE_STORAGE); */
        /*         if (ret_value != SUCCEED) { */
        /*             printf("==PDC_SERVER[%d]: failed to update region storage info!\n", pdc_server_rank_g); */
        /*             goto done; */
        /*         } */
        /*     } */

        /*     // Debug print */
        /*     /1* PDC_print_region_list(region_elt); *1/ */
        /*     /1* fflush(stdout); *1/ */

        /*     iter++; */
        /* } // end DL_FOREACH */

        /* #ifdef ENABLE_TIMING */
        /* gettimeofday(&pdc_timer_end, 0); */
        /* double update_region_location_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end); */
        /* printf("==PDC_SERVER[%d]: update region location [%s] time %.6f!\n", */
        /*         pdc_server_rank_g, region_elt->storage_location, update_region_location_time); */
        /* server_update_region_location_time_g += update_region_location_time; */
        /* #endif */

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
    snprintf(io_region->storage_location, ADDR_MAX, "%.200s/pdc_data/%" PRIu64 "/server%d/s%04d.bin",
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
    data_server_region_t *region = NULL;

    FUNC_ENTER(NULL);

    region = PDC_Server_get_obj_region(obj_id);
    if(region == NULL) {
        printf("cannot locate file handle\n");
        goto done;
    }

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

perr_t PDC_Server_data_read_from(uint64_t obj_id, struct PDC_region_info *region_info, void *buf)
{
    perr_t ret_value = SUCCEED;
    ssize_t read_bytes;
    data_server_region_t *region = NULL;
    
    FUNC_ENTER(NULL);
    
    region = PDC_Server_get_obj_region(obj_id);
    if(region == NULL) {
        printf("cannot locate file handle\n");
        goto done;
    }
    
    read_bytes = pread(region->fd, buf, region_info->size[0], region_info->offset[0]-pdc_server_rank_g*nclient_per_node*region_info->size[0]);
    /* printf("server %d calls pread, offset = %lld, size = %lld\n", pdc_server_rank_g, region_info->offset[0]-pdc_server_rank_g*nclient_per_node*region_info->size[0], region_info->size[0]); */
    if(read_bytes == -1){
        printf("==PDC_SERVER[%d]: pread %d failed\n", pdc_server_rank_g, region->fd);
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

perr_t PDC_Server_get_local_storage_meta_with_one_name(storage_meta_query_one_name_args_t *args)
{
    perr_t ret_value = SUCCEED;
    pdc_metadata_t *meta = NULL;
    region_list_t *region_elt = NULL, *region_head = NULL, *res_region_list = NULL;
    int region_count = 0, i = 0;

    FUNC_ENTER(NULL);

    // FIXME: currently use timestep value of 0
    PDC_Server_search_with_name_timestep(args->name, PDC_get_hash_by_name(args->name), 0, &meta);
    if (meta == NULL) {
        /* printf("==PDC_SERVER[%d]: No metadata with name [%s] found!\n", pdc_server_rank_g, args->name); */
        goto done;
    }

    region_head = meta->storage_region_list_head;

    // Copy the matched regions with storage metadata to a result region list
    DL_COUNT(region_head, region_elt, region_count);
    args->n_res = region_count;
    res_region_list = (region_list_t*)calloc(region_count, sizeof(region_list_t));

    // Copy location and offset
    i = 0;
    /* args->overlap_storage_region_list = res_region_list; */
    DL_FOREACH(region_head, region_elt) {
        if (i >= region_count) {
            printf("==PDC_SERVER[%d] %s - More regions %d than allocated %d\n", 
                    pdc_server_rank_g, __func__, i, region_count);
            ret_value = FAIL;
            goto done;
        }
        pdc_region_list_t_deep_cp(region_elt, &res_region_list[i]);
        res_region_list[i].prev = NULL;
        res_region_list[i].next = NULL;
        DL_APPEND(args->overlap_storage_region_list, &res_region_list[i]);
        i++;
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

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_name(args->name, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        /* if (is_debug_g == 1) { */
        /*     printf("==PDC_SERVER[%d]: get storage region info from local server\n",pdc_server_rank_g); */
        /* } */

        // Metadata object is local, no need to send update RPC
        // Fill in with storage meta (region_list_t **regions, int n_res)
        ret_value = PDC_Server_get_local_storage_meta_with_one_name(args);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - get local storage location ERROR!\n", pdc_server_rank_g, __func__);
            goto done;
        }

        // Execute callback function immediately
        // cb:      PDC_Server_accumulate_storage_meta_then_read
        // cb_args: args
        if (args->cb != NULL) 
            args->cb(args->cb_args);
        
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
        n_get_remote_storage_meta_g++;

        // Add current task and relevant data ptrs to s2s task queue, so later when receiving bulk transfer
        // request we know which task that bulk data is needed 
        in.obj_name  = args->name;
        in.origin_id = pdc_server_rank_g;
        in.task_id   = PDC_add_task_to_list(&pdc_server_s2s_task_head_g, args->cb, args->cb_args, 
                                            &pdc_server_task_id_g, &pdc_server_task_mutex_g);
        
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
perr_t PDC_Server_accumulate_storage_meta_then_read(storage_meta_query_one_name_args_t *in)
{
    perr_t ret_value = SUCCEED;
    accumulate_storage_meta_t *accu_meta;
    region_list_t *req_region = NULL, *region_elt = NULL, *read_list_head = NULL;
    int i, is_sort_read;
    size_t j;

    FUNC_ENTER(NULL);

    accu_meta = in->accu_meta;

    // Add current input to accumulate_storage_meta structure
    accu_meta->storage_meta[accu_meta->n_accumulated++] = in;

    // Trigger the read procedure when we have accumulated all storage meta
    if (accu_meta->n_accumulated >= accu_meta->n_total) {
        /* printf("==PDC_SERVER[%d]: Retrieved all storage meta, %d from remote servers. \n", */ 
        /*         pdc_server_rank_g, n_get_remote_storage_meta_g); */
        /* n_get_remote_storage_meta_g = 0; */
        /* fflush(stdout); */
        is_sort_read = 0;
        char *sort_request = getenv("PDC_SORT_READ");
        if (NULL != sort_request) 
            is_sort_read = 1;
        

        #ifdef ENABLE_TIMING
        struct timeval  pdc_timer_start, pdc_timer_end, pdc_timer_start1, pdc_timer_end1;
        double read_total_sec, notify_total_sec;
        gettimeofday(&pdc_timer_start, 0);
        #endif

        // Attach the overlapping storage regions we got to the request region
        for (i = 0; i < accu_meta->n_accumulated; i++) {
            req_region = accu_meta->storage_meta[i]->req_region;
            req_region->n_overlap_storage_region = accu_meta->storage_meta[i]->n_res;
            req_region->overlap_storage_regions  = accu_meta->storage_meta[i]->overlap_storage_region_list;
            req_region->seq_id                   = accu_meta->storage_meta[i]->seq_id;

            // In case we are reading the entire region and the client does not provide the region info
            // we just set the region equal to the entire object dims
            if (req_region->ndim == 0) {
                req_region->ndim = req_region->overlap_storage_regions->ndim;
                for (j = 0; j < req_region->ndim; j++) {
                    req_region->start[j] = 0;
                    req_region->count[j] = 0;
                    // Count is the max value of bottom right corner of all storage regions
                    DL_FOREACH(req_region->overlap_storage_regions, region_elt) 
                        req_region->count[j] = PDC_MAX(req_region->count[j]+req_region->start[j], 
                                                       region_elt->count[j]+region_elt->start[j]);
                }
            }
            // Insert the request to a list based on file path and offset
            if (1 == is_sort_read) 
                DL_INSERT_INORDER(read_list_head, req_region, region_list_path_offset_cmp);
            else
                DL_APPEND(read_list_head, req_region);
        }

        DL_FOREACH(read_list_head, region_elt) {

            // Read data to shm
            /* printf("==PDC_SERVER[%d]: Start reading a region\n", pdc_server_rank_g); */
            ret_value = PDC_Server_read_one_region(region_elt);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - Error with PDC_Server_read_one_region\n", 
                        pdc_server_rank_g, __func__);
            }
            // Debug print
            /* printf("==PDC_SERVER[%d]: Finished reading a region [%c]...[%c]\n", */ 
            /*         pdc_server_rank_g, req_region->buf[0], req_region->buf[req_region->data_size-1]); */

        }

        // Sort (restore) the order of read_list_head according to the original order from request
        DL_SORT(read_list_head, PDC_region_list_seq_id_cmp);
        /* for (i = 0; i < accu_meta->n_accumulated; i++) { */
        /*     req_region = accu_meta->storage_meta[i]->req_region; */
        /*     req_region->prev = NULL; */
        /*     req_region->next = NULL; */
        /*     DL_INSERT_INORDER(result_list_head, req_region, PDC_region_list_seq_id_cmp); */
        /* } // End for */

        #ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        read_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        printf("==PDC_SERVER[%d]: read %d objects time: %.4f, n_fread: %d, n_fopen: %d, is_sort_read: %d\n", 
                pdc_server_rank_g, accu_meta->n_accumulated, read_total_sec, n_fread_g, n_fopen_g, is_sort_read);
        fflush(stdout);
        #endif

        // send all shm info to client 
        ret_value = PDC_Server_notify_client_multi_io_complete(accu_meta->client_id, accu_meta->client_seq_id,
                                                               accu_meta->n_total, read_list_head);

        /* #ifdef ENABLE_TIMING */
        /* gettimeofday(&pdc_timer_end1, 0); */
        /* notify_total_sec = PDC_get_elapsed_time_double(&pdc_timer_end, &pdc_timer_end1); */
        /* printf("==PDC_SERVER[%d]: read %d objects time: %.4f, notify time: %.4f\n", */ 
        /*         pdc_server_rank_g, accu_meta->n_accumulated, read_total_sec, notify_total_sec); */
        /* fflush(stdout); */
        /* #endif */
    } // End if

/* done: */
    // TODO free many things 
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_accumulate_storage_meta_then_read

/*
 * Query and read entire objects with a list of object names, received from a client
 *
 * \param  query_args[IN]              Query info
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_Server_query_read_names(query_read_names_args_t *query_read_args)
{
    perr_t ret_value = SUCCEED;
    int i;
    storage_meta_query_one_name_args_t *query_name_args = NULL; 

    FUNC_ENTER(NULL);
    /* printf("==PDC_SERVER[%d]: Query read obj names:\n", pdc_server_rank_g); */
    /* for (i = 0; i < query_read_args->cnt; i++) { */
    /*     printf("%s\n", query_read_args->obj_names[i]); */
    /* } */

    // Temp storage to accumulate all storage meta of the requested objects
    // Each task should have one such structure
    accumulate_storage_meta_t *accmulate_meta = (accumulate_storage_meta_t*)
                                                calloc(1, sizeof(accumulate_storage_meta_t));
    accmulate_meta->n_total = query_read_args->cnt;
    accmulate_meta->client_id = query_read_args->client_id;
    accmulate_meta->client_seq_id = query_read_args->client_seq_id;
    accmulate_meta->storage_meta = (storage_meta_query_one_name_args_t**)calloc(query_read_args->cnt, 
                                                sizeof(storage_meta_query_one_name_args_t*));
    

    // Now we need to retrieve their storage metadata, some can be found in local metadata server, 
    // others are stored on remote metadata servers
    for (i = 0; i < query_read_args->cnt; i++) {
        // query_name_args is the struct to store all storage metadata of each request obj (obj_name)
        query_name_args = (storage_meta_query_one_name_args_t*)calloc(1, sizeof(storage_meta_query_one_name_args_t));
        query_name_args->seq_id         = i; 
        query_name_args->req_region     = (region_list_t*)calloc(1, sizeof(region_list_t));
        PDC_init_region_list(query_name_args->req_region);
        query_name_args->req_region->access_type = READ;
        // TODO: if requesting partial data, adjust the actual region info accordingly
        query_name_args->name           = query_read_args->obj_names[i];
        query_name_args->cb             = PDC_Server_accumulate_storage_meta_then_read;
        query_name_args->cb_args        = query_name_args;
        query_name_args->accu_meta      = accmulate_meta;
        accmulate_meta->storage_meta[i] = query_name_args;

        /* printf("==PDC_SERVER[%d]: start to get storage meta for name [%s]\n", */ 
        /*         pdc_server_rank_g, query_name_args->name); */
        /* fflush(stdout); */
        PDC_Server_get_all_storage_meta_with_one_name(query_name_args);
    }

    // After query all the object names, wait for the bulk xfer from the remote servers
    // When all storage metadata have been collected, the read operation will be triggered
    // in PDC_Server_accumulate_storage_meta_then_read().
 
/* done: */
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // end of PDC_Server_query_read_names

/*
 * Wrapper function for callback
 *
 * \param  callback_info[IN]              Callback info pointer
 *
 * \return Non-negative on success/Negative on failure
 */
hg_return_t PDC_Server_query_read_names_cb(const struct hg_cb_info *callback_info)
{
    PDC_Server_query_read_names((query_read_names_args_t*) callback_info->arg);

    return HG_SUCCESS;
} // end of PDC_Server_query_read_names_cb

/* hg_return_t PDC_Server_bulk_cleanup_cb(const struct hg_cb_info *callback_info)
{
    // TODO: free previously allocated resources

    return HG_SUCCESS;
} */ // end PDC_Server_bulk_cleanup_cb 

hg_return_t PDC_Server_storage_meta_name_query_bulk_respond_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret = HG_SUCCESS;
    hg_handle_t handle = callback_info->info.forward.handle;
    pdc_int_ret_t bulk_rpc_ret;

    // Sent the bulk handle with rpc and get a response
    ret = HG_Get_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get output\n");
        goto done;
    }

    ret = HG_Free_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free output\n");
        goto done;
    }

    /* /1* Free memory handle *1/ */
    /* ret = HG_Bulk_free(cb_args->bulk_handle); */
    /* if (ret != HG_SUCCESS) { */
    /*     fprintf(stderr, "Could not free bulk data handle\n"); */
    /*     goto done; */
    /* } */ 

    /* HG_Destroy(cb_args->rpc_handle); */
done:
    return ret;
} // end PDC_Server_storage_meta_name_query_bulk_respond_cb

// Get all storage meta of the one requested object name and bulk xfer to original requested server  
hg_return_t PDC_Server_storage_meta_name_query_bulk_respond(const struct hg_cb_info *callback_info)
{
    hg_return_t hg_ret = HG_SUCCESS;
    perr_t ret_value;
    storage_meta_name_query_in_t *args;
    storage_meta_query_one_name_args_t *query_args;
    hg_handle_t rpc_handle;
    hg_bulk_t bulk_handle;
    bulk_rpc_in_t bulk_rpc_in;
    void **buf_ptrs;
    hg_size_t *buf_sizes;
    uint32_t server_id;
    region_info_transfer_t **region_infos;
    region_list_t *region_elt;
    int i, j;
    FUNC_ENTER(NULL);

    args = (storage_meta_name_query_in_t*)callback_info->arg;

    // Now metadata object is local
    query_args = (storage_meta_query_one_name_args_t*)calloc(1, sizeof(storage_meta_query_one_name_args_t));
    query_args->name = args->obj_name;

    /* printf("==PDC_SERVER[%d]: process storage meta query, name [%s], task_id %d, origin_id %d\n", */ 
    /*         pdc_server_rank_g, args->obj_name, args->task_id, args->origin_id); */

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

    // bulk transfer to args->origin_id
    hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr,
                       get_storage_meta_name_query_bulk_result_rpc_register_id_g, &rpc_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create handle\n");
        ret_value = FAIL;
        goto done;
    }

    int nbuf = 3*query_args->n_res + 1;
    buf_sizes = (hg_size_t *)calloc(sizeof(hg_size_t), nbuf);
    buf_ptrs  = (void**)calloc(sizeof(void*),  nbuf);
    region_infos = (region_info_transfer_t**)calloc(sizeof(region_info_transfer_t*), query_args->n_res);

    // buf_ptrs[0]: task_id
    buf_ptrs[0] = &(args->task_id);

    buf_sizes[0] = sizeof(int);

    // We need path, offset, region info (region_info_transfer_t)
    i = 1;
    j = 0;
    DL_FOREACH(query_args->overlap_storage_region_list, region_elt) {
        region_infos[j] = (region_info_transfer_t*)calloc(sizeof(region_info_transfer_t), 1);
        pdc_region_list_t_to_transfer(region_elt, region_infos[j]);
        // Check if cache is available
        /* #ifdef ENABLE_CACHE */
        if (region_elt->cache_location[0] != 0) {
            buf_ptrs[i  ]  = region_elt->cache_location;
            buf_ptrs[i+1]  = &(region_elt->cache_offset);
        }
        else {
        /* #endif */
            buf_ptrs[i  ]  = region_elt->storage_location;
            buf_ptrs[i+1]  = &(region_elt->offset);
        /* #ifdef ENABLE_CACHE */
        }
        /* #endif */
        buf_ptrs[i+2]  = region_infos[j];
        buf_sizes[i  ] = strlen(buf_ptrs[i]) + 1;
        buf_sizes[i+1] = sizeof(uint64_t);
        buf_sizes[i+2] = sizeof(region_info_transfer_t);
        i+=3;
        j++;
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
    hg_ret = HG_Forward(rpc_handle, PDC_Server_storage_meta_name_query_bulk_respond_cb, NULL, &bulk_rpc_in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not forward call\n");
        ret_value = FAIL;
        goto done;
    } 

done:
    /* HG_Destroy(rpc_handle); */
    fflush(stdout);
    FUNC_LEAVE(hg_ret);
} // end PDC_Server_storage_meta_name_query_bulk_respond


// We have received the storage metadata from remote meta server, which is stored in region_list_head
// Now we want to find the corresponding task the previously requested the meta retrival and attach
// the received storage meta to it.
// Each task was previously created with 
//      cb   = PDC_Server_accumulate_storage_meta_then_read
//      args = (storage_meta_query_one_name_args_t *args)
perr_t PDC_Server_proc_storage_meta_bulk(int task_id, int n_regions, region_list_t *region_list_head)
{
    perr_t ret_value = SUCCEED;
    storage_meta_query_one_name_args_t *query_args;

    FUNC_ENTER(NULL);

    pdc_task_list_t *task = PDC_find_task_from_list(&pdc_server_s2s_task_head_g, task_id, &pdc_server_task_mutex_g);
    if (task == NULL) {
        printf("==PDC_SERVER[%d]: %s - Error getting task %d\n", pdc_server_rank_g, __func__, task_id);
        ret_value = FAIL;
        goto done;
    }

    // Add the result storage regions to accumulate_storage_meta
    query_args = (storage_meta_query_one_name_args_t*)task->cb_args;
    query_args->overlap_storage_region_list = region_list_head;
    query_args->n_res = n_regions;

    // Execute callback function associated with this task
    if (task->cb != NULL)
        task->cb(task->cb_args);        // should equals to PDC_Server_accumulate_storage_meta_then_read(query_args)

    PDC_del_task_from_list(&pdc_server_s2s_task_head_g, task, &pdc_server_task_mutex_g);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t PDC_Server_add_client_shm_to_cache(int origin, int cnt, void *buf_cp)
{
    perr_t ret_value = SUCCEED;
    int i, j;
    region_storage_meta_t *storage_metas = (region_storage_meta_t*)buf_cp;
    pdc_data_server_io_list_t *io_list_elt, *io_list_target;
    region_list_t *new_region; 

    FUNC_ENTER(NULL);

    #ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_read_list_mutex_g);
    #endif
    // Have the server update the metadata or write to burst buffer
    for (i = 0; i < cnt; i++) {
        /* printf("objid=%" PRIu64 ", [%s], offset=%" PRIu64 ", size=%" PRIu64 "\n", */ 
        /*         storage_metas[i].obj_id, storage_metas[i].storage_location, storage_metas[i].offset, */
        /*         storage_metas[i].size); */

        // Find existing io_list target for this object
        io_list_target = NULL;
        DL_FOREACH(pdc_data_server_read_list_head_g, io_list_elt) 
            if (storage_metas[i].obj_id == io_list_elt->obj_id) {
                io_list_target = io_list_elt;
                break;
            }

        // If not found, create and insert one to the read list
        if (NULL == io_list_target) {
            io_list_target = (pdc_data_server_io_list_t*)calloc(1, sizeof(pdc_data_server_io_list_t));
            io_list_target->obj_id = storage_metas[i].obj_id;
            io_list_target->total  = 0;
            io_list_target->count  = 0;
            io_list_target->ndim   = storage_metas[i].region_transfer.ndim;
            // TODO
            for (j = 0; j < io_list_target->ndim; j++)
                io_list_target->dims[j] = 0;

            DL_APPEND(pdc_data_server_read_list_head_g, io_list_target);
        }
        io_list_target->total++;
        io_list_target->count++;

        new_region = (region_list_t*)calloc(1, sizeof(region_list_t));
        pdc_region_transfer_t_to_list_t(&storage_metas[i].region_transfer, new_region);
        strcpy(new_region->shm_addr, storage_metas[i].storage_location);
        new_region->offset = storage_metas[i].offset;
        new_region->data_size   = storage_metas[i].size;

        // Open shared memory and map to data buf
        new_region->shm_fd = shm_open(new_region->shm_addr, O_RDONLY, 0666);
        if (new_region->shm_fd == -1) {
            printf("==PDC_SERVER[%d]: %s - Shared memory open failed [%s]!\n", 
                    pdc_server_rank_g, __func__, new_region->shm_addr);
            ret_value = FAIL;
            goto done;
        }

        new_region->buf= mmap(0, new_region->data_size, PROT_READ, MAP_SHARED, 
                new_region->shm_fd, new_region->offset);
        if (new_region->buf== MAP_FAILED) {
            printf("==PDC_SERVER[%d]: Map failed: %s\n", pdc_server_rank_g, strerror(errno));
            ret_value = FAIL;
            goto done;
        }

        new_region->is_data_ready = 1;
        new_region->is_io_done    = 1;
        DL_PREPEND(io_list_target->region_list_head, new_region);

    } // End for each cache entry
    
done:
    #ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_read_list_mutex_g);
    #endif
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // End PDC_Server_add_client_shm_to_cache

// Data query
static hg_return_t 
send_query_storage_region_to_server_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value = HG_SUCCESS;
    hg_handle_t handle;
    pdc_int_ret_t out;

    FUNC_ENTER(NULL);

    handle = callback_info->info.forward.handle;
    ret_value = HG_Get_output(handle, &out);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - error with HG_Get_output\n", pdc_server_rank_g, __func__);
        goto done;
    }

done:
    HG_Free_output(handle, &out);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

void copy_hist(pdc_histogram_t *to, pdc_histogram_t *from)
{
    int nbin = from->nbin;

    to->incr  = from->incr;
    to->dtype = from->dtype;
    if (NULL == to->range) 
        to->range = (double*)calloc(sizeof(double), nbin*2);
    memcpy(to->range, from->range, sizeof(double)*nbin*2);
    if (NULL == to->bin) 
        to->bin   = (uint64_t*)calloc(sizeof(uint64_t), nbin);
    memcpy(to->bin, from->bin, sizeof(uint64_t)*nbin);
    to->nbin  = from->nbin;
}

/* For data query */
perr_t 
send_query_storage_region_to_server(query_task_t *task, region_list_t *region, uint64_t obj_id, int server_id, int is_done)
{
    perr_t ret_value = SUCCEED;
    hg_return_t hg_ret;
    hg_handle_t handle;
    query_storage_region_transfer_t in;

    in.query_id = task->query_id;
    in.obj_id   = obj_id;
    pdc_region_list_t_to_transfer(region, &in.region_transfer);
    in.storage_location = strdup(region->storage_location);
    in.offset   = region->offset;
    in.size     = region->data_size;
    in.is_done  = is_done;


    // TODO: for test purpose  generate histogram of this region
    {
        region->region_hist = PDC_create_hist(PDC_FLOAT, 10, 0.0, 100.0);
        region->region_hist->bin[0] = 1;
        region->region_hist->bin[5] = 11;
        region->region_hist->bin[9] = 111;

        printf("==PDC_SERVER[%d]: generated a test histogram!\n", pdc_server_rank_g);
        PDC_print_hist(region->region_hist);
    }
                

    if (NULL != region->region_hist) {
        in.has_hist = 1;
        in.hist.bin   = NULL;
        in.hist.range = NULL;
        copy_hist(&in.hist, region->region_hist);
    }
    else {
        in.has_hist = 0;
    }

    printf("==PDC_SERVER[%d]: sending storage region to server %d!\n", 
            pdc_server_rank_g, server_id);
    fflush(stdout);

    hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, 
                       send_data_query_region_register_id_g, &handle);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - HG_Create failed!\n", pdc_server_rank_g, __func__);
        goto done;
    }

    hg_ret = HG_Forward(handle, send_query_storage_region_to_server_rpc_cb, NULL, &in);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        HG_Destroy(handle);
        goto done;
    }


    HG_Destroy(handle);

done:
    return ret_value;
}

perr_t 
PDC_proces_query_storage_info(query_task_t *task, region_list_t *region)
{
    perr_t ret_value = SUCCEED;
    printf("==PDC_SERVER[%d]: processing storage region!\n", pdc_server_rank_g);

    return ret_value;
}

void
send_query_storage_info(query_task_t *task, uint64_t obj_id, int *obj_idx, uint64_t *obj_ids)
{
    pdc_metadata_t *meta;
    int i, found, server_id, count, is_last;
    region_list_t *elt;

    if (1 == PDC_Server_has_metadata(obj_id)) {
        // Check if a previous query condition has already send the storage regions
        found = 0;
        for (i = 0; i < *obj_idx; i++) {
            if (obj_id == obj_ids[i]) {
                found = 1;
                printf("==PDC_SERVER[%d]: storage region already sent for %" PRIu64 " !\n", 
                        pdc_server_rank_g, obj_id);
                break;
            }
        }

        if (0 == found) {
            // there can be multiple pass to the same object, we only need to send it once
            meta = PDC_Server_get_obj_metadata(obj_id);
            if (NULL != meta) {
                obj_ids[*obj_idx] = obj_id;
                (*obj_idx)++;
                printf("==PDC_SERVER[%d]: found metadata for %" PRIu64 "!\n", 
                        pdc_server_rank_g, obj_id);
                // Iterate over all storage regions and send one by on
                server_id = 0;
                DL_COUNT(meta->storage_region_list_head, elt, count);
                if (count > pdc_server_size_g - 1) 
                    task->n_sent = pdc_server_size_g - 1;
                else
                    task->n_sent = count;

                i = 0;
                DL_FOREACH(meta->storage_region_list_head, elt) {
                    /* if (server_id == pdc_server_rank_g) { */
                    /*     PDC_proces_query_storage_info(elt); */
                    /* } */
                    if (pdc_server_size_g == 1) {
                        PDC_proces_query_storage_info(task, elt);
                    }
                    // Skip myself if there are other servers
                    else if (server_id != pdc_server_rank_g) {
                        // mark is_last=1 when it's the last region to send to a server
                        is_last = 0;
                        if (i >= count - pdc_server_size_g) 
                            is_last = 1;
                        send_query_storage_region_to_server(task, elt, meta->obj_id, server_id, is_last);
                    }

                    server_id++; 
                    server_id %= pdc_server_size_g;
                    i++;
                }

            } // end if (NULL != meta)
            else {
                printf("==PDC_SERVER[%d]: %s - metadata not found for %" PRIu64 "!\n",
                        pdc_server_rank_g, __func__, obj_id);
            }
        } // end if (0 == found)
    }
    /* else */
    /*     printf("==PDC_SERVER[%d]: metadata not here for %" PRIu64 "!\n", pdc_server_rank_g, obj_id); */

}

void
distribute_query_workload(query_task_t *task, pdcquery_t *query, int *obj_idx, uint64_t *obj_ids)
{
    
    // Postorder tranversal
    if (NULL != query) {
        distribute_query_workload(task, query->left, obj_idx, obj_ids);
        distribute_query_workload(task, query->right, obj_idx, obj_ids);

        // Iterate over storage regions and prepare for their distribution to other servers
        if (NULL == query->left && NULL == query->right) 
            send_query_storage_info(task, query->constraint->obj_id, obj_idx, obj_ids);
    }
    return;
}

void attach_storage_region_to_query(pdcquery_t *query, region_list_t *region)
{
    region_list_t *head;
    if (NULL == query) 
        return;

    attach_storage_region_to_query(query->left, region);
    attach_storage_region_to_query(query->right, region);

    // Note: currently attaches the received region to all query constraints of the same object,
    //       and uses the same "prev" "next" pointers 
    if (NULL == query->left && NULL == query->right && NULL != query->constraint) {
        if (query->constraint->obj_id == region->obj_id) { 
            if (query->constraint->storage_region_list_head == NULL) 
                query->constraint->storage_region_list_head = region;
            else {
                head = (region_list_t*)query->constraint->storage_region_list_head;
                DL_APPEND(head, region);
            }
        }
    }
}

void
PDC_query_iter_with_cb(pdcquery_t *query, void (*func)(pdcquery_t *arg))
{
    if (NULL == query) 
        return;

    PDC_query_constraint_iter_with_cb(query->left, func);
    PDC_query_constraint_iter_with_cb(query->right, func);

    if (NULL == query->left && NULL == query->right) 
        func(query);
    return;
}

// TODO: testing
static perr_t
PDC_Server_load_query_data(pdcquery_t *query)
{
    perr_t ret_value;
    region_list_t *req_region, *region_tmp, *read_list_head = NULL, *storage_region_list_head;
    pdc_data_server_io_list_t *io_list_elt, *io_list_target;
    uint64_t obj_id;
    int count;

    pdcquery_constraint_t *constraint = query->constraint;
    storage_region_list_head = constraint->storage_region_list_head;

    if (NULL == constraint || NULL == storage_region_list_head) {
        printf("==PDC_SERVER[%d]: %s -  NULL query constraint/storage region!\n", 
                pdc_server_rank_g, __func__);
        goto done;
    }

    obj_id = constraint->obj_id;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_read_list_mutex_g);
#endif

    // Iterate io list, find the IO list of this obj
    DL_FOREACH(pdc_data_server_read_list_head_g, io_list_elt) {
        /* printf("io_list_elt obj id: %" PRIu64 "\n", io_list_elt->obj_id); */
        /* fflush(stdout); */
        if (obj_id == io_list_elt->obj_id) {
            io_list_target = io_list_elt;
            break;
        }
    }

    // If not found, create and insert one to the list
    if (NULL == io_list_target) {
        // pdc_data_server_io_list_t maintains the request list for one object id,
        // write and read are separate lists
        io_list_target = (pdc_data_server_io_list_t*)calloc(1, sizeof(pdc_data_server_io_list_t));
        if (NULL == io_list_target) {
            printf("==PDC_SERVER[%d]: %s -  ERROR allocating pdc_data_server_io_list_t!\n", 
                    pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }
        io_list_target->obj_id = obj_id;
        io_list_target->total  = 0;     // TODO
        io_list_target->count  = 0;
        io_list_target->ndim   = 0;
        /* for (i = 0; i < meta.ndim; i++) */
        /*     io_list_target->dims[i] = meta.dims[i]; */

        io_list_target->region_list_head = NULL;

        // Add to the io list
        DL_APPEND(pdc_data_server_read_list_head_g, io_list_target);
    }

    io_list_target->count++;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_read_list_mutex_g);
#endif

    // Iterate over current list and check for existing identical regions in the io list
    DL_FOREACH(storage_region_list_head, req_region) {

        DL_FOREACH(io_list_target->region_list_head, region_tmp) {
            if (PDC_is_same_region_list(region_tmp, req_region) != 1) {
                // append current request region to the io list
                region_list_t *new_region = (region_list_t*)calloc(1, sizeof(region_list_t));
                if (new_region == NULL) {
                    printf("==PDC_SERVER: ERROR allocating new_region!\n");
                    ret_value = FAIL;
                    goto done;
                }
                req_region->io_cache_region = new_region;
                pdc_region_list_t_deep_cp(&(io_info->region), new_region);
                DL_APPEND(io_list_target->region_list_head, new_region);
                if (is_debug_g == 1) {
                    DL_COUNT(io_list_target->region_list_head, req_region, count);
                    printf("==PDC_SERVER[%d]: Added 1 to IO request list, obj_id=%" PRIu64 ", %d total\n",
                            pdc_server_rank_g, new_region->meta->obj_id, count);
                    PDC_print_region_list(new_region);
                }
            }
        }
    }

    // Read
    #ifdef ENABLE_TIMING
    struct timeval  pdc_timer_start, pdc_timer_end;
    double cache_total_sec;
    gettimeofday(&pdc_timer_start, 0);
    #endif

    // Currently reads all regions of a query constraint together
    // TODO: potential optimization: aggregate all I/O requests
    ret_value = PDC_Server_data_read_to_shm(io_list_target->region_list_head);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - PDC_Server_data_read_to_shm FAILED!\n", pdc_server_rank_g, __func__);
        goto done;
    }

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    server_io_elapsed_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

done:
    fflush(stdout);
    return ret_value;
} // End PDC_Server_load_query_data

// TODO: optimization that support faster one pass processing with (x > a AND x < b) and (x < a OR x > b)
perr_t
PDC_query_evaluate(pdcquery_t *query)
{

}

hg_return_t 
PDC_Server_recv_data_query_region(const struct hg_cb_info *callback_info)
{
    hg_return_t ret = HG_SUCCESS;
    hg_handle_t handle = callback_info->info.forward.handle;
    storage_regions_args_t *args = (storage_regions_args_t*) callback_info->arg;
    query_task_t *query_task;
    region_list_t *region = args->storage_region;
    int query_id_exist;

    /* printf("==PDC_SERVER[%d]: received a query region!\n", pdc_server_rank_g); */
    /* fflush(stdout); */

    // Find query from query task list
    query_id_exist = 0;
    DL_FOREACH(query_task_list_head_g, query_task) {
        if (query_task->query_id == args->query_id) {
            query_id_exist = 1;
            break;
        }
    }

    /* printf("==PDC_SERVER[%d]: %s - found existing query task:\n", pdc_server_rank_g, __func__); */
    /* PDCquery_print(query_task->query); */

    printf("==PDC_SERVER[%d]: %s received region with hist:\n", pdc_server_rank_g, __func__);
    PDC_print_hist(region->region_hist);

    // TODO: deal with the case when query is not received from client (cannot attach region)
    if (NULL == query_task || 0 == query_id_exist) {
        printf("==PDC_SERVER[%d]: query %d has not been received from client!\n", 
                pdc_server_rank_g, args->query_id);
        goto done;
    }

    // Add this storage region to all query constraints that specifies the same object
    attach_storage_region_to_query(query_task->query, region); 
    printf("==PDC_SERVER[%d]: attached storage region to query\n", pdc_server_rank_g);

    if (args->is_done == 1) {
        query_task->n_recv_region++;
        if (query_task->n_recv_region == query_task->n_unique_obj) {
            printf("==PDC_SERVER[%d]: Received all storage info from meta server!\n", pdc_server_rank_g);
            // TODO: load data and start query evaluation
            PDC_query_iter_with_cb(query, PDC_Server_load_query_data, query_task->query);
            PDC_query_evaluate(query);
        }
    }
done:
    fflush(stdout);
    return ret;
} // end PDC_Server_recv_data_query_region

hg_return_t 
PDC_Server_recv_data_query(const struct hg_cb_info *callback_info)
{
    hg_return_t ret = HG_SUCCESS;
    hg_handle_t handle = callback_info->info.forward.handle;
    pdc_query_xfer_t *query_xfer = (pdc_query_xfer_t*) callback_info->arg;
    int found_region = 0, obj_idx = 0;
    uint64_t *obj_ids;
    query_task_t *new_task, *task_elt;
    int query_id_exist;

    pdcquery_t *query = PDC_deserialize_query(query_xfer);
    if (NULL == query) {
        printf("==PDC_SERVER[%d]: deserialize query FAILED!\n", pdc_server_rank_g);
        goto done;
    }

    // Add query to global task list
    DL_FOREACH(query_task_list_head_g, task_elt) {
        if (task_elt->query_id == query_xfer->query_id) {
            query_id_exist = 1;
            break;
        }
    }

    if (0 == query_id_exist) {
        new_task = (query_task_t*)calloc(1, sizeof(query_task_t));
        new_task->query_id     = query_xfer->query_id;
        new_task->query        = query;
        new_task->n_unique_obj = query_xfer->n_unique_obj;
        DL_APPEND(query_task_list_head_g, new_task);
        printf("==PDC_SERVER[%d]: %s - appended new query task %d to list head\n", 
                pdc_server_rank_g, __func__, new_task->query_id);
    }
    else {
        task_elt->query = query;
        new_task = task_elt;
    }


    printf("==PDC_SERVER[%d]: %s - deserialized query:\n", pdc_server_rank_g, __func__);
    PDCquery_print(query);
    fflush(stdout);

    // TODO: find metadata of all queried objects and distribute to other servers
    // Traverse the query tree and process the query constraints that has metadata stored in here 
    obj_ids = (uint64_t*)calloc(query_xfer->n_unique_obj, sizeof(uint64_t));
    distribute_query_workload(new_task, query, &obj_idx, obj_ids); 

    free(obj_ids);

done:
    if(query_xfer) PDC_query_xfer_free(query_xfer);
    fflush(stdout);
    return ret;
} // end PDC_Server_storage_meta_name_query_bulk_respond_cb

