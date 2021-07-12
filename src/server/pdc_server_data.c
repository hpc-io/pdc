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

#include "pdc_config.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#include "pdc_utlist.h"
#include "pdc_public.h"
#include "pdc_interface.h"
#include "pdc_region.h"
#include "pdc_client_server_common.h"
#include "pdc_server_data.h"
#include "pdc_server_metadata.h"
#include "pdc_server.h"
#include "pdc_hist_pkg.h"

// Global object region info list in local data server
data_server_region_t *      dataserver_region_g     = NULL;
data_server_region_unmap_t *dataserver_region_unmap = NULL;

int pdc_buffered_bulk_update_total_g = 0;
int pdc_nbuffered_bulk_update_g      = 0;
int n_check_write_finish_returned_g  = 0;
int buffer_read_request_total_g      = 0;
int buffer_write_request_total_g     = 0;
int buffer_write_request_num_g       = 0;
int buffer_read_request_num_g        = 0;
int is_server_direct_io_g            = 0;

uint64_t total_mem_cache_size_mb_g     = 0;
uint64_t max_mem_cache_size_mb_g       = 0;
int      cache_percentage_g            = 0;
int      current_read_from_cache_cnt_g = 0;
int      total_read_from_cache_cnt_g   = 0;
FILE *   pdc_cache_file_ptr_g          = NULL;
char     pdc_cache_file_path_g[ADDR_MAX];

query_task_t *          query_task_list_head_g      = NULL;
cache_storage_region_t *cache_storage_region_head_g = NULL;

perr_t
PDC_Server_set_lustre_stripe(const char *path, int stripe_count, int stripe_size_MB)
{
    perr_t ret_value = SUCCEED;
    size_t len;
    int    i, index;
    char   tmp[ADDR_MAX];
    char   cmd[TAG_LEN_MAX];

    FUNC_ENTER(NULL);

    // Make sure stripe count is sane
    if (stripe_count > 248 / pdc_server_size_g)
        stripe_count = 248 / pdc_server_size_g;

    if (stripe_count < 1)
        stripe_count = 1;

    if (stripe_count > 16)
        stripe_count = 16;

    if (stripe_size_MB <= 0)
        stripe_size_MB = 1;

    index = (pdc_server_rank_g * stripe_count) % 248;

    snprintf(tmp, sizeof(tmp), "%s", path);

    len = strlen(tmp);
    for (i = len - 1; i >= 0; i--) {
        if (tmp[i] == '/') {
            tmp[i] = 0;
            break;
        }
    }
    snprintf(cmd, TAG_LEN_MAX, "lfs setstripe -S %dM -c %d -i %d %s", stripe_size_MB, stripe_count, index,
             tmp);

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
 * Check if two regions are the same
 *
 * \param  a[IN]     Pointer to the first region
 * \param  b[IN]     Pointer to the second region
 *
 * \return 1 if they are the same/-1 otherwise
 */
static int
is_region_identical(region_list_t *a, region_list_t *b)
{
    int      ret_value = -1;
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
        if (a->start[i] != b->start[i] || a->count[i] != b->count[i]) {
            ret_value = -1;
            goto done;
        }
    }

    ret_value = 1;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_local_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status)
{
    perr_t          ret_value = SUCCEED;
    pdc_metadata_t *res_meta;
    region_list_t * elt, *request_region;

    FUNC_ENTER(NULL);

    // Check if the region lock info is on current server
    *lock_status   = 0;
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    PDC_region_transfer_t_to_list_t(&(mapped_region->remote_region), request_region);
    res_meta = find_metadata_by_id(mapped_region->remote_obj_id);
    if (res_meta == NULL || res_meta->region_lock_head == NULL) {
        printf("==PDC_SERVER[%d]: PDC_Server_region_lock_status - metadata/region_lock is NULL!\n",
               pdc_server_rank_g);
        fflush(stdout);
        ret_value = FAIL;
        goto done;
    }

    // iterate the target metadata's region_lock_head (linked list) to search for queried region
    DL_FOREACH(res_meta->region_lock_head, elt)
    {
        if (is_region_identical(request_region, elt) == 1) {
            *lock_status            = 1;
            elt->reg_dirty_from_buf = 1;
            /* printf("%s: set reg_dirty_from_buf \n", __func__); */
            elt->bulk_handle = mapped_region->remote_bulk_handle;
            elt->addr        = mapped_region->remote_addr;
            elt->from_obj_id = mapped_region->from_obj_id;
            elt->obj_id      = mapped_region->remote_obj_id;
            elt->reg_id      = mapped_region->remote_reg_id;
            elt->client_id   = mapped_region->remote_client_id;
        }
    }
    free(request_region);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_region_lock_status(PDC_mapping_info_t *mapped_region, int *lock_status)
{
    perr_t         ret_value = SUCCEED;
    region_list_t *request_region;
    uint32_t       server_id = 0;

    *lock_status   = 0;
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    PDC_region_transfer_t_to_list_t(&(mapped_region->remote_region), request_region);

    server_id = PDC_get_server_by_obj_id(mapped_region->remote_obj_id, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
        PDC_Server_local_region_lock_status(mapped_region, lock_status);
    }
    else {
        printf("lock is located in a different server, work not finished yet\n");
        fflush(stdout);
    }

    FUNC_LEAVE(ret_value);
}

data_server_region_t *
PDC_Server_get_obj_region(pdcid_t obj_id)
{
    data_server_region_t *ret_value = NULL;
    data_server_region_t *elt       = NULL;

    FUNC_ENTER(NULL);

    if (dataserver_region_g != NULL) {
        DL_FOREACH(dataserver_region_g, elt)
        {
            if (elt->obj_id == obj_id)
                ret_value = elt;
        }
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Data_Server_region_lock(region_lock_in_t *in, region_lock_out_t *out, hg_handle_t *handle)
{
    perr_t                ret_value = SUCCEED;
    int                   ndim;
    region_list_t *       request_region;
    data_server_region_t *new_obj_reg;
    region_list_t *       elt1, *tmp;
    region_buf_map_t *    eltt;
    int                   error      = 0;
    int                   found_lock = 0;
    // time_t                t;
    // struct tm             tm;

    FUNC_ENTER(NULL);

    ndim = in->region.ndim;

    // Convert transferred lock region to structure
    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    PDC_init_region_list(request_region);
    request_region->ndim = ndim;

    if (ndim >= 1) {
        request_region->start[0] = in->region.start_0;
        request_region->count[0] = in->region.count_0;
    }
    if (ndim >= 2) {
        request_region->start[1] = in->region.start_1;
        request_region->count[1] = in->region.count_1;
    }
    if (ndim >= 3) {
        request_region->start[2] = in->region.start_2;
        request_region->count[2] = in->region.count_2;

        /* t = time(NULL); */
        /* tm = *localtime(&t); */
        /* printf("%02d:%02d:%02d\n", tm.tm_hour, tm.tm_min, tm.tm_sec); */
        /* printf("==PDC_SERVER[%d]: locking region (%llu, %llu, %llu), (%llu, %llu, %llu)\n",
         * pdc_server_rank_g, */
        /*                                                                                     request_region->start[0],
         * request_region->start[1], request_region->start[2], */
        /* request_region->count[0], request_region->count[1], request_region->count[2]); */
    }
    if (ndim >= 4) {
        request_region->start[3] = in->region.start_3;
        request_region->count[3] = in->region.count_3;
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&region_struct_mutex_g);
#endif
    new_obj_reg = PDC_Server_get_obj_region(in->obj_id);
    if (new_obj_reg == NULL) {
        new_obj_reg = (data_server_region_t *)malloc(sizeof(struct data_server_region_t));
        if (new_obj_reg == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "PDC_SERVER: PDC_Server_region_lock() allocates new object failed");
        }
        new_obj_reg->obj_id                   = in->obj_id;
        new_obj_reg->region_lock_head         = NULL;
        new_obj_reg->region_buf_map_head      = NULL;
        new_obj_reg->region_lock_request_head = NULL;
        new_obj_reg->region_storage_head      = NULL;
        DL_APPEND(dataserver_region_g, new_obj_reg);
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&region_struct_mutex_g);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
    // Go through all existing locks to check for region lock
    DL_FOREACH(new_obj_reg->region_lock_head, elt1)
    {
        if (PDC_is_same_region_list(elt1, request_region) == 1) {
            found_lock = 1;
            if (in->lock_mode == PDC_BLOCK) {
                ret_value                   = FAIL;
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

    if (found_lock == 0) {
        // check if the lock region is used in buf map function
        tmp = (region_list_t *)malloc(sizeof(region_list_t));
        DL_FOREACH(new_obj_reg->region_buf_map_head, eltt)
        {
            PDC_region_transfer_t_to_list_t(&(eltt->remote_region_unit), tmp);
            if (PDC_is_same_region_list(tmp, request_region) == 1) {
                request_region->reg_dirty_from_buf = 1;
                hg_atomic_incr32(&(request_region->buf_map_refcount));
                /* printf("%s: set reg_dirty_from_buf and buf_map_refcount\n", __func__); */
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
    /* t = time(NULL); */
    /* tm = *localtime(&t); */
    /* printf("Done locking region %02d:%02d:%02d\n", tm.tm_hour, tm.tm_min, tm.tm_sec); */
    /* fflush(stdout); */
    if (error == 1) {
        out->ret = 0;
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_release_lock_request(uint64_t obj_id, struct pdc_region_info *region)
{
    perr_t                ret_value = SUCCEED;
    region_list_t *       request_region;
    region_list_t *       elt, *tmp;
    data_server_region_t *new_obj_reg;
    region_lock_out_t     out;

    FUNC_ENTER(NULL);

    request_region = (region_list_t *)malloc(sizeof(region_list_t));
    PDC_init_region_list(request_region);
    PDC_region_info_to_list_t(region, request_region);

    new_obj_reg = PDC_Server_get_obj_region(obj_id);
    if (new_obj_reg == NULL) {
        PGOTO_ERROR(FAIL, "===PDC Server: cannot locate data_server_region_t strcut for object ID");
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&lock_request_mutex_g);
#endif
    DL_FOREACH_SAFE(new_obj_reg->region_lock_request_head, elt, tmp)
    {
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

perr_t
PDC_Data_Server_region_release(region_lock_in_t *in, region_lock_out_t *out)
{
    perr_t                ret_value = SUCCEED;
    int                   ndim;
    region_list_t *       tmp1, *tmp2;
    region_list_t         request_region;
    int                   found   = 0;
    data_server_region_t *obj_reg = NULL;

    FUNC_ENTER(NULL);

    /* time_t t; */
    /* struct tm tm; */
    /* t = time(NULL); */
    /* tm = *localtime(&t); */
    /* printf("start PDC_Data_Server_region_release %02d:%02d:%02d\n", tm.tm_hour, tm.tm_min, tm.tm_sec); */

    ndim = in->region.ndim;

    // Convert transferred lock region to structure
    PDC_init_region_list(&request_region);
    request_region.ndim = ndim;

    if (ndim >= 1) {
        request_region.start[0] = in->region.start_0;
        request_region.count[0] = in->region.count_0;
    }
    if (ndim >= 2) {
        request_region.start[1] = in->region.start_1;
        request_region.count[1] = in->region.count_1;
    }
    if (ndim >= 3) {
        request_region.start[2] = in->region.start_2;
        request_region.count[2] = in->region.count_2;

        /* time_t func_time=time(NULL);    \ */
        /* struct tm tm_time;               \ */
        /* func_time = time(NULL);tm_time = *localtime(&func_time);\ */
        /* printf("%02d:%02d:%02d: %s\n", tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec, __func__);\ */
    }
    if (ndim >= 4) {
        request_region.start[3] = in->region.start_3;
        request_region.count[3] = in->region.count_3;
    }

    obj_reg = PDC_Server_get_obj_region(in->obj_id);
    if (obj_reg == NULL) {
        ret_value = FAIL;
        printf("==PDC_SERVER[%d]: requested release object does not exist\n", pdc_server_rank_g);
        goto done;
    }
    // Find the lock region in the list and remove it
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&lock_list_mutex_g);
#endif
    DL_FOREACH_SAFE(obj_reg->region_lock_head, tmp1, tmp2)
    {
        if (is_region_identical(&request_region, tmp1) == 1) {
            // Found the requested region lock, remove from the linked list
            found = 1;
            DL_DELETE(obj_reg->region_lock_head, tmp1);
            free(tmp1);
            tmp1 = NULL;
            break;
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
    /* t = time(NULL); */
    /* tm = *localtime(&t); */
    /* printf("done PDC_Data_Server_region_release %02d:%02d:%02d\n", tm.tm_hour, tm.tm_min, tm.tm_sec); */
    /* fflush(stdout); */

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
static int
region_list_cmp(region_list_t *a, region_list_t *b)
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
static int
region_list_path_offset_cmp(region_list_t *a, region_list_t *b)
{
    int ret_value;
    if (NULL == a || NULL == b) {
        printf("  %s - NULL input!\n", __func__);
        return -1;
    }

    ret_value = strcmp(a->storage_location, b->storage_location);
    if (0 == ret_value)
        ret_value = a->offset > b->offset ? 1 : -1;

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
static int
region_list_cmp_by_client_id(region_list_t *a, region_list_t *b)
{
    if (a->ndim != b->ndim) {
        printf("  region_list_cmp_by_client_id(): not equal ndim! \n");
        return -1;
    }

    return (a->client_ids[0] - b->client_ids[0]);
}

perr_t
PDC_Data_Server_buf_unmap(const struct hg_info *info, buf_unmap_in_t *in)
{
    perr_t                ret_value = SUCCEED;
    int                   ret       = HG_UTIL_SUCCESS;
    region_buf_map_t *    tmp, *elt;
    data_server_region_t *target_obj;

    FUNC_ENTER(NULL);

    target_obj = PDC_Server_get_obj_region(in->remote_obj_id);
    if (target_obj == NULL) {
        PGOTO_ERROR(FAIL,
                    "===PDC_DATA_SERVER: PDC_Data_Server_buf_unmap() - requested object does not exist");
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_buf_map_mutex_g);
#endif
    DL_FOREACH_SAFE(target_obj->region_buf_map_head, elt, tmp)
    {
        if (in->remote_obj_id == elt->remote_obj_id &&
            PDC_region_is_identical(in->remote_region, elt->remote_region_unit)) {
#ifdef ENABLE_MULTITHREAD
            // wait for work to be done, then free
            hg_thread_mutex_lock(&(elt->bulk_args->work_mutex));
#ifdef ENABLE_WAIT_DATA
            while (!elt->bulk_args->work_completed)
                hg_thread_cond_wait(&(elt->bulk_args->work_cond), &(elt->bulk_args->work_mutex));
            elt->bulk_args->work_completed = 0;
            ret                            = HG_UTIL_SUCCESS;
#else
            if (!elt->bulk_args->work_completed)
                ret = hg_thread_cond_timedwait(&(elt->bulk_args->work_cond), &(elt->bulk_args->work_mutex),
                                               100);
            // free resource if work is done
            if (ret == HG_UTIL_SUCCESS)
                elt->bulk_args->work_completed = 0;
#endif
            hg_thread_mutex_unlock(&(elt->bulk_args->work_mutex)); // per bulk_args
#endif
            if (ret == HG_UTIL_SUCCESS) {
                if (elt->remote_data_ptr) {
                    free(elt->remote_data_ptr);
                    elt->remote_data_ptr = NULL;
                }
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
#ifndef ENABLE_WAIT_DATA
            // timeout, append the global list for unmap
            else {
                data_server_region_unmap_t *region = NULL;
                region = (data_server_region_unmap_t *)malloc(sizeof(struct data_server_region_unmap_t));
                if (region == NULL)
                    PGOTO_ERROR(FAIL,
                                "===PDC_DATA_SERVER: PDC_Data_Server_buf_unmap() - cannot allocate region");
                region->obj_id       = in->remote_obj_id;
                region->unmap_region = in->remote_region;
                region->info         = info;
#ifdef ENABLE_MULTITHREAD
                hg_thread_mutex_lock(&data_buf_unmap_mutex_g);
#endif
                DL_APPEND(dataserver_region_unmap, region);
#ifdef ENABLE_MULTITHREAD
                hg_thread_mutex_unlock(&data_buf_unmap_mutex_g);
#endif
            }
#endif
        }
    }

    if (target_obj->region_buf_map_head == NULL && pdc_server_rank_g == 0) {
        close(target_obj->fd);
        target_obj->fd = -1;
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_buf_map_mutex_g);
#endif

done:
    FUNC_LEAVE(ret_value);
}

// This function is called when multhread is enabled
#ifdef ENABLE_MULTITHREAD
perr_t
PDC_Data_Server_check_unmap()
{
    perr_t                      ret_value = SUCCEED;
    int                         ret       = HG_UTIL_SUCCESS;
    pdcid_t                     remote_obj_id;
    region_buf_map_t *          tmp, *elt;
    data_server_region_unmap_t *tmp1, *elt1;
    data_server_region_t *      target_obj;
    int                         completed = 0;

    FUNC_ENTER(NULL);

    DL_FOREACH_SAFE(dataserver_region_unmap, elt1, tmp1)
    {
        remote_obj_id = elt1->obj_id;
        target_obj    = PDC_Server_get_obj_region(remote_obj_id);
        if (target_obj == NULL) {
            PGOTO_ERROR(
                FAIL, "===PDC_DATA_SERVER: PDC_Data_Server_check_unmap() - requested object does not exist");
        }
        completed = 0;
        hg_thread_mutex_lock(&data_buf_map_mutex_g);
        DL_FOREACH_SAFE(target_obj->region_buf_map_head, elt, tmp)
        {
            if (remote_obj_id == elt->remote_obj_id &&
                PDC_region_is_identical(elt1->unmap_region, elt->remote_region_unit)) {
                hg_thread_mutex_lock(&(elt->bulk_args->work_mutex));
                if (!elt->bulk_args->work_completed)
                    // wait for 100ms for work completed
                    ret = hg_thread_cond_timedwait(&(elt->bulk_args->work_cond),
                                                   &(elt->bulk_args->work_mutex), 100);
                hg_thread_mutex_unlock(&(elt->bulk_args->work_mutex)); // per bulk_args
                // free resource if work is completed
                if (ret == HG_UTIL_SUCCESS) {
                    completed                      = 1;
                    elt->bulk_args->work_completed = 0;
                    if (elt->remote_data_ptr) {
                        free(elt->remote_data_ptr);
                        elt->remote_data_ptr = NULL;
                    }
                    HG_Addr_free(elt1->info->hg_class, elt->local_addr);
                    HG_Bulk_free(elt->local_bulk_handle);
                    hg_thread_mutex_destroy(&(elt->bulk_args->work_mutex));
                    hg_thread_cond_destroy(&(elt->bulk_args->work_cond));
                    free(elt->bulk_args);

                    DL_DELETE(target_obj->region_buf_map_head, elt);
                    free(elt);
                }
            }
        }
        if (target_obj->region_buf_map_head == NULL && pdc_server_rank_g == 0) {
            close(target_obj->fd);
            target_obj->fd = -1;
        }
        hg_thread_mutex_unlock(&data_buf_map_mutex_g);
        if (completed == 1) {
            DL_DELETE(dataserver_region_unmap, elt1);
            free(elt1);
        }
    }

done:
    FUNC_LEAVE(ret_value);
}
#endif

static hg_return_t
server_send_buf_unmap_addr_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                ret_value = HG_SUCCESS;
    hg_handle_t                handle;
    buf_map_out_t              out;
    struct transfer_buf_unmap *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_unmap *)callback_info->arg;
    handle     = callback_info->info.forward.handle;

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
    hg_return_t                            ret_value = HG_SUCCESS;
    uint32_t                               server_id;
    struct buf_unmap_server_lookup_args_t *lookup_args;
    hg_handle_t                            server_send_buf_unmap_handle;
    hg_handle_t                            handle;
    struct transfer_buf_unmap *            tranx_args;
    int                                    error = 0;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
    lookup_args = (struct buf_unmap_server_lookup_args_t *)callback_info->arg;
    server_id   = lookup_args->server_id;
    tranx_args  = lookup_args->buf_unmap_args;
    handle      = tranx_args->handle;

    pdc_remote_server_info_g[server_id].addr       = callback_info->info.lookup.addr;
    pdc_remote_server_info_g[server_id].addr_valid = 1;

    if (pdc_remote_server_info_g[server_id].addr == NULL) {
        printf("==PDC_SERVER[%d]: %s - remote server addr is NULL\n", pdc_server_rank_g, __func__);
        error = 1;
        goto done;
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&addr_valid_mutex_g);
#endif

    HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, buf_unmap_server_register_id_g,
              &server_send_buf_unmap_handle);

    ret_value = HG_Forward(server_send_buf_unmap_handle, server_send_buf_unmap_addr_rpc_cb, tranx_args,
                           &(tranx_args->in));
    if (ret_value != HG_SUCCESS) {
        error = 1;
        HG_Destroy(server_send_buf_unmap_handle);
        PGOTO_ERROR(ret_value,
                    "===PDC SERVER: buf_unmap_lookup_remote_server_cb() - Could not start HG_Forward()");
    }
done:
    if (error == 1) {
        HG_Free_input(handle, &(lookup_args->buf_unmap_args->in));
        HG_Destroy(handle);
        free(tranx_args);
    }
    free(lookup_args);
    FUNC_LEAVE(ret_value);
}

// reference from PDC_Server_lookup_server_id
perr_t
PDC_Server_buf_unmap_lookup_server_id(int remote_server_id, struct transfer_buf_unmap *transfer_args)
{
    perr_t                                 ret_value = SUCCEED;
    hg_return_t                            hg_ret    = HG_SUCCESS;
    struct buf_unmap_server_lookup_args_t *lookup_args;
    hg_handle_t                            handle;
    int                                    error = 0;

    FUNC_ENTER(NULL);

    handle = transfer_args->handle;
    lookup_args =
        (struct buf_unmap_server_lookup_args_t *)malloc(sizeof(struct buf_unmap_server_lookup_args_t));
    lookup_args->server_id      = remote_server_id;
    lookup_args->buf_unmap_args = transfer_args;
    hg_ret                      = HG_Addr_lookup(hg_context_g, buf_unmap_lookup_remote_server_cb, lookup_args,
                            pdc_remote_server_info_g[remote_server_id].addr_string, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS) {
        error = 1;
        PGOTO_ERROR(
            FAIL,
            "==PDC_SERVER: PDC_Server_buf_unmap_lookup_server_id() Connection to remote server FAILED!");
    }

done:
    fflush(stdout);
    if (error == 1) {
        HG_Free_input(handle, &(transfer_args->in));
        HG_Destroy(handle);
        free(transfer_args);
    }

    FUNC_LEAVE(ret_value);
}

static hg_return_t
server_send_buf_unmap_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                     ret_value = HG_SUCCESS;
    hg_handle_t                     handle;
    buf_unmap_out_t                 output;
    struct transfer_buf_unmap_args *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_unmap_args *)callback_info->arg;
    handle     = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_buf_unmap_rpc_cb() - error with HG_Get_output\n",
               pdc_server_rank_g);
        tranx_args->ret = -1;
        goto done;
    }

    tranx_args->ret = output.ret;

done:
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &output);
    HG_Destroy(handle);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Meta_Server_buf_unmap(buf_unmap_in_t *in, hg_handle_t *handle)
{
    perr_t                          ret_value = SUCCEED;
    hg_return_t                     hg_ret    = HG_SUCCESS;
    hg_handle_t                     server_send_buf_unmap_handle;
    struct transfer_buf_unmap_args *buf_unmap_args;
    struct transfer_buf_unmap *     addr_args;
    pdc_metadata_t *                target_meta = NULL;
    region_buf_map_t *              tmp, *elt;
    int                             error = 0;

    FUNC_ENTER(NULL);

    if ((uint32_t)pdc_server_rank_g == in->meta_server_id) {
        target_meta = find_metadata_by_id(in->remote_obj_id);
        if (target_meta == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "===PDC META SERVER: cannot retrieve object metadata");
        }
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
        DL_FOREACH_SAFE(target_meta->region_buf_map_head, elt, tmp)
        {

            if (in->remote_obj_id == elt->remote_obj_id &&
                PDC_region_is_identical(in->remote_region, elt->remote_region_unit)) {
                DL_DELETE(target_meta->region_buf_map_head, elt);
                free(elt);
            }
        }
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_unlock(&meta_buf_map_mutex_g);
#endif
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }
    else {
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
        if (pdc_remote_server_info_g[in->meta_server_id].addr_valid != 1) {
            addr_args         = (struct transfer_buf_unmap *)malloc(sizeof(struct transfer_buf_unmap));
            addr_args->handle = *handle;
            addr_args->in     = *in;

            PDC_Server_buf_unmap_lookup_server_id(in->meta_server_id, addr_args);
        }
        else {
            HG_Create(hg_context_g, pdc_remote_server_info_g[in->meta_server_id].addr,
                      buf_unmap_server_register_id_g, &server_send_buf_unmap_handle);

            buf_unmap_args = (struct transfer_buf_unmap_args *)malloc(sizeof(struct transfer_buf_unmap_args));
            buf_unmap_args->handle = *handle;
            buf_unmap_args->in     = *in;
            hg_ret =
                HG_Forward(server_send_buf_unmap_handle, server_send_buf_unmap_rpc_cb, buf_unmap_args, in);
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
    if (error == 1) {
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }

    FUNC_LEAVE(ret_value);
}

static int
server_open_storage(char *storage_location, pdcid_t obj_id)
{

#ifdef ENABLE_LUSTRE
    int stripe_count, stripe_size;
#endif
    // Generate a location for data storage for data server to write
    char *data_path                = NULL;
    char *user_specified_data_path = getenv("PDC_DATA_LOC");

    if (user_specified_data_path != NULL)
        data_path = user_specified_data_path;
    else {
        data_path = getenv("SCRATCH");
        if (data_path == NULL)
            data_path = ".";
    }
    // Data path prefix will be $SCRATCH/pdc_data/$obj_id/
    snprintf(storage_location, ADDR_MAX, "%s/pdc_data/%" PRIu64 "/server%d/s%04d.bin", data_path, obj_id,
             pdc_server_rank_g, pdc_server_rank_g);
    PDC_mkdir(storage_location);

#ifdef ENABLE_LUSTRE
    if (pdc_nost_per_file_g != 1)
        stripe_count = 248 / pdc_server_size_g;
    else
        stripe_count = pdc_nost_per_file_g;
    stripe_size = 16; // MB
    PDC_Server_set_lustre_stripe(storage_location, stripe_count, stripe_size);

    if (is_debug_g == 1 && pdc_server_rank_g == 0) {
        printf("storage_location is %s\n", storage_location);
    }
#endif
    return open(storage_location, O_RDWR | O_CREAT, 0666);
}

region_buf_map_t *
PDC_Data_Server_buf_map(const struct hg_info *info, buf_map_in_t *in, region_list_t *request_region,
                        void *data_ptr)
{
    region_buf_map_t *    ret_value   = NULL;
    data_server_region_t *new_obj_reg = NULL;
    region_list_t *       elt_reg;
    region_buf_map_t *    buf_map_ptr = NULL;
    region_buf_map_t *    tmp;
    int                   dup                      = 0;
    char *                data_path                = NULL;
    char *                user_specified_data_path = NULL;
    char                  storage_location[ADDR_MAX];
#ifdef ENABLE_LUSTRE
    int stripe_count, stripe_size;
#endif

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&region_struct_mutex_g);
#endif
    new_obj_reg = PDC_Server_get_obj_region(in->remote_obj_id);
    if (new_obj_reg == NULL) {
        new_obj_reg = (data_server_region_t *)malloc(sizeof(struct data_server_region_t));
        if (new_obj_reg == NULL)
            PGOTO_ERROR(NULL, "PDC_SERVER: PDC_Server_insert_buf_map_region() allocates new object failed");
        new_obj_reg->obj_id                   = in->remote_obj_id;
        new_obj_reg->region_lock_head         = NULL;
        new_obj_reg->region_buf_map_head      = NULL;
        new_obj_reg->region_lock_request_head = NULL;
        new_obj_reg->region_storage_head      = NULL;

        new_obj_reg->fd = server_open_storage(storage_location, in->remote_obj_id);
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
        snprintf(storage_location, ADDR_MAX, "%.200s/pdc_data/%" PRIu64 "/server%d/s%04d.bin", data_path,
                 in->remote_obj_id, pdc_server_rank_g, pdc_server_rank_g);
        PDC_mkdir(storage_location);

#ifdef ENABLE_LUSTRE
        if (pdc_nost_per_file_g != 1)
            stripe_count = 248 / pdc_server_size_g;
        else
            stripe_count = pdc_nost_per_file_g;
        stripe_size = lustre_stripe_size_mb_g;
        PDC_Server_set_lustre_stripe(storage_location, stripe_count, stripe_size);

        if (is_debug_g == 1 && pdc_server_rank_g == 0) {
            printf("storage_location is %s\n", storage_location);
        }
#endif
        new_obj_reg->fd = open(storage_location, O_RDWR | O_CREAT, 0666);
        if (new_obj_reg->fd == -1) {
            printf("==PDC_SERVER[%d]: open %s failed\n", pdc_server_rank_g, storage_location);
            goto done;
        }
        new_obj_reg->storage_location = strdup(storage_location);
        DL_APPEND(dataserver_region_g, new_obj_reg);
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&region_struct_mutex_g);
#endif

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_buf_map_mutex_g);
#endif
    DL_FOREACH(new_obj_reg->region_buf_map_head, tmp)
    {
        if (in->ndim == 1) {
            if (tmp->remote_obj_id == in->remote_obj_id &&
                in->remote_region_unit.start_0 == tmp->remote_region_unit.start_0 &&
                in->remote_region_unit.count_0 == tmp->remote_region_unit.count_0 &&
                in->local_region.start_0 == tmp->local_region.start_0 &&
                in->local_region.count_0 == tmp->local_region.count_0)
                dup = 1;
        }
        else if (in->ndim == 2) {
            if (tmp->remote_obj_id == in->remote_obj_id &&
                in->remote_region_unit.start_0 == tmp->remote_region_unit.start_0 &&
                in->remote_region_unit.start_1 == tmp->remote_region_unit.start_1 &&
                in->remote_region_unit.count_0 == tmp->remote_region_unit.count_0 &&
                in->remote_region_unit.count_1 == tmp->remote_region_unit.count_1 &&
                in->local_region.start_0 == tmp->local_region.start_0 &&
                in->local_region.start_1 == tmp->local_region.start_1 &&
                in->local_region.count_0 == tmp->local_region.count_0 &&
                in->local_region.count_1 == tmp->local_region.count_1)
                dup = 1;
        }
        else if (in->ndim == 3) {
            if (tmp->remote_obj_id == in->remote_obj_id &&
                in->remote_region_unit.start_0 == tmp->remote_region_unit.start_0 &&
                in->remote_region_unit.start_1 == tmp->remote_region_unit.start_1 &&
                in->remote_region_unit.start_2 == tmp->remote_region_unit.start_2 &&
                in->remote_region_unit.count_0 == tmp->remote_region_unit.count_0 &&
                in->remote_region_unit.count_1 == tmp->remote_region_unit.count_1 &&
                in->remote_region_unit.count_2 == tmp->remote_region_unit.count_2 &&
                in->local_region.start_0 == tmp->local_region.start_0 &&
                in->local_region.start_1 == tmp->local_region.start_1 &&
                in->local_region.start_2 == tmp->local_region.start_2 &&
                in->local_region.count_0 == tmp->local_region.count_0 &&
                in->local_region.count_1 == tmp->local_region.count_1 &&
                in->local_region.count_2 == tmp->local_region.count_2)
                dup = 1;
        }
    }
    if (dup == 0) {
        buf_map_ptr = (region_buf_map_t *)malloc(sizeof(region_buf_map_t));
        if (buf_map_ptr == NULL)
            PGOTO_ERROR(NULL,
                        "PDC_SERVER: PDC_Server_insert_buf_map_region() allocates region pointer failed");

        buf_map_ptr->local_reg_id    = in->local_reg_id;
        buf_map_ptr->local_region    = in->local_region;
        buf_map_ptr->local_ndim      = in->ndim;
        buf_map_ptr->local_data_type = in->local_type;
        HG_Addr_dup(info->hg_class, info->addr, &(buf_map_ptr->local_addr));
        HG_Bulk_ref_incr(in->local_bulk_handle);
        buf_map_ptr->local_bulk_handle = in->local_bulk_handle;

        buf_map_ptr->remote_obj_id        = in->remote_obj_id;
        buf_map_ptr->remote_ndim          = in->ndim;
        buf_map_ptr->remote_unit          = in->remote_unit;
        buf_map_ptr->remote_region_unit   = in->remote_region_unit;
        buf_map_ptr->remote_region_nounit = in->remote_region_nounit;
        buf_map_ptr->remote_data_ptr      = data_ptr;

        DL_APPEND(new_obj_reg->region_buf_map_head, buf_map_ptr);
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_buf_map_mutex_g);
#endif

    DL_FOREACH(new_obj_reg->region_lock_head, elt_reg)
    {
        if (PDC_is_same_region_list(elt_reg, request_region) == 1) {
            hg_atomic_incr32(&(elt_reg->buf_map_refcount));
            /* printf("%s: set buf_map_refcount\n", __func__); */
        }
    }
    ret_value = buf_map_ptr;

    free(request_region);

done:
    FUNC_LEAVE(ret_value);
}

static int
is_region_transfer_t_identical(region_info_transfer_t *a, region_info_transfer_t *b)
{
    int ret_value = -1;

    FUNC_ENTER(NULL);

    if (a == NULL || b == NULL) {
        PGOTO_DONE(ret_value);
    }

    if (a->ndim != b->ndim) {
        PGOTO_DONE(ret_value);
    }

    if (a->ndim >= 1) {
        if (a->start_0 != b->start_0 || a->count_0 != b->count_0)
            PGOTO_DONE(ret_value);
    }
    if (a->ndim >= 2) {
        if (a->start_1 != b->start_1 || a->count_1 != b->count_1)
            PGOTO_DONE(ret_value);
    }
    if (a->ndim >= 3) {
        if (a->start_2 != b->start_2 || a->count_2 != b->count_2)
            PGOTO_DONE(ret_value);
    }
    if (a->ndim >= 4) {
        if (a->start_3 != b->start_3 || a->count_3 != b->count_3)
            PGOTO_DONE(ret_value);
    }
    ret_value = 1;

done:
    FUNC_LEAVE(ret_value);
}

void *
PDC_Server_maybe_allocate_region_buf_ptr(pdcid_t obj_id, region_info_transfer_t region, size_t type_size)
{
    void *                ret_value  = NULL;
    data_server_region_t *target_obj = NULL, *elt = NULL;
    region_buf_map_t *    tmp;

    FUNC_ENTER(NULL);

    if (dataserver_region_g == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - object list is NULL");
    DL_FOREACH(dataserver_region_g, elt)
    {
        if (obj_id == elt->obj_id)
            target_obj = elt;
    }
    if (target_obj == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - cannot locate object");

    DL_FOREACH(target_obj->region_buf_map_head, tmp)
    {
        if (is_region_transfer_t_identical(&region, &(tmp->remote_region_unit)) == 1) {
            ret_value = tmp->remote_data_ptr;
            break;
        }
    }
    /* We don't currently have a buffer to receive data */
    if (ret_value == NULL) {
        size_t            i;
        size_t            region_size = region.count_0;
        region_buf_map_t *buf_map_ptr = NULL;
        for (i = 1; i < region.ndim; i++) {
            if (i == 1)
                region_size *= (region.count_1 / type_size);
            else if (i == 2)
                region_size *= (region.count_2 / type_size);
            else if (i == 3)
                region_size *= (region.count_3 / type_size);
        }
        ret_value = malloc(region_size);

        buf_map_ptr                       = (region_buf_map_t *)malloc(sizeof(region_buf_map_t));
        buf_map_ptr->remote_obj_id        = obj_id;
        buf_map_ptr->remote_ndim          = region.ndim;
        buf_map_ptr->remote_data_ptr      = ret_value;
        buf_map_ptr->remote_region_unit   = region;
        buf_map_ptr->remote_region_nounit = region;
        buf_map_ptr->remote_region_nounit.count_0 /= type_size;
        buf_map_ptr->remote_region_nounit.count_1 /= type_size;
        buf_map_ptr->remote_region_nounit.count_2 /= type_size;
        buf_map_ptr->remote_region_nounit.count_3 /= type_size;
        DL_APPEND(target_obj->region_buf_map_head, buf_map_ptr);
    }
    if (ret_value == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - region data pointer is NULL");

done:
    FUNC_LEAVE(ret_value);
}

void *
PDC_Server_get_region_buf_ptr(pdcid_t obj_id, region_info_transfer_t region)
{
    void *                ret_value  = NULL;
    data_server_region_t *target_obj = NULL, *elt = NULL;
    region_buf_map_t *    tmp;

    FUNC_ENTER(NULL);

    if (dataserver_region_g == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - object list is NULL");
    DL_FOREACH(dataserver_region_g, elt)
    {
        if (obj_id == elt->obj_id)
            target_obj = elt;
    }
    if (target_obj == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - cannot locate object");

    DL_FOREACH(target_obj->region_buf_map_head, tmp)
    {
        if (is_region_transfer_t_identical(&region, &(tmp->remote_region_unit)) == 1) {
            ret_value = tmp->remote_data_ptr;
            break;
        }
    }
    if (ret_value == NULL)
        PGOTO_ERROR(NULL, "===PDC SERVER: PDC_Server_get_region_buf_ptr() - region data pointer is NULL");

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
server_send_buf_map_addr_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t              ret_value = HG_SUCCESS;
    hg_handle_t              handle;
    buf_map_out_t            out;
    struct transfer_buf_map *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_map *)callback_info->arg;
    handle     = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &out);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_buf_map_addr_rpc_cb - error with HG_Get_output\n",
               pdc_server_rank_g);
        goto done;
    }

done:
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
    hg_return_t                          ret_value = HG_SUCCESS;
    uint32_t                             server_id;
    struct buf_map_server_lookup_args_t *lookup_args;
    hg_handle_t                          server_send_buf_map_handle;
    hg_handle_t                          handle;
    struct transfer_buf_map *            tranx_args;
    int                                  error = 0;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
    lookup_args = (struct buf_map_server_lookup_args_t *)callback_info->arg;
    server_id   = lookup_args->server_id;
    tranx_args  = lookup_args->buf_map_args;
    handle      = tranx_args->handle;

    pdc_remote_server_info_g[server_id].addr       = callback_info->info.lookup.addr;
    pdc_remote_server_info_g[server_id].addr_valid = 1;

    if (pdc_remote_server_info_g[server_id].addr == NULL) {
        printf("==PDC_SERVER[%d]: %s - remote server addr is NULL\n", pdc_server_rank_g, __func__);
        error = 1;
        goto done;
    }
#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&addr_valid_mutex_g);
#endif

    HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, buf_map_server_register_id_g,
              &server_send_buf_map_handle);

    ret_value = HG_Forward(server_send_buf_map_handle, server_send_buf_map_addr_rpc_cb, tranx_args,
                           &(tranx_args->in));
    if (ret_value != HG_SUCCESS) {
        error = 1;
        HG_Destroy(server_send_buf_map_handle);
        PGOTO_ERROR(ret_value,
                    "===PDC SERVER: buf_map_lookup_remote_server_cb() - Could not start HG_Forward()");
    }

done:
    if (error == 1) {
        HG_Free_input(handle, &(lookup_args->buf_map_args->in));
        HG_Destroy(handle);
        free(tranx_args);
    }
    free(lookup_args);
    FUNC_LEAVE(ret_value);
}

// reference from PDC_Server_lookup_server_id
perr_t
PDC_Server_buf_map_lookup_server_id(int remote_server_id, struct transfer_buf_map *transfer_args)
{
    perr_t                               ret_value = SUCCEED;
    hg_return_t                          hg_ret    = HG_SUCCESS;
    struct buf_map_server_lookup_args_t *lookup_args;
    hg_handle_t                          handle;
    int                                  error = 0;

    FUNC_ENTER(NULL);

    handle      = transfer_args->handle;
    lookup_args = (struct buf_map_server_lookup_args_t *)malloc(sizeof(struct buf_map_server_lookup_args_t));
    lookup_args->server_id    = remote_server_id;
    lookup_args->buf_map_args = transfer_args;
    hg_ret                    = HG_Addr_lookup(hg_context_g, buf_map_lookup_remote_server_cb, lookup_args,
                            pdc_remote_server_info_g[remote_server_id].addr_string, HG_OP_ID_IGNORE);
    if (hg_ret != HG_SUCCESS) {
        error = 1;
        PGOTO_ERROR(
            FAIL, "==PDC_SERVER: PDC_Server_buf_map_lookup_server_id() Connection to remote server FAILED!");
    }

done:
    fflush(stdout);
    if (error == 1) {
        HG_Free_input(handle, &(transfer_args->in));
        HG_Destroy(handle);
        free(transfer_args);
    }

    FUNC_LEAVE(ret_value);
}

static hg_return_t
server_send_buf_map_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                   ret_value = HG_SUCCESS;
    hg_handle_t                   handle;
    buf_map_out_t                 out;
    struct transfer_buf_map_args *tranx_args;

    FUNC_ENTER(NULL);

    tranx_args = (struct transfer_buf_map_args *)callback_info->arg;
    handle     = callback_info->info.forward.handle;

    ret_value = HG_Get_output(handle, &out);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: server_send_buf_map_rpc_cb - error with HG_Get_output\n",
               pdc_server_rank_g);
        tranx_args->ret = -1;
        goto done;
    }

done:
    HG_Free_input(tranx_args->handle, &(tranx_args->in));
    HG_Destroy(tranx_args->handle);
    HG_Free_output(handle, &out);
    HG_Destroy(handle);
    free(tranx_args);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Meta_Server_buf_map(buf_map_in_t *in, region_buf_map_t *new_buf_map_ptr, hg_handle_t *handle)
{
    perr_t                        ret_value = SUCCEED;
    hg_return_t                   hg_ret    = HG_SUCCESS;
    hg_handle_t                   server_send_buf_map_handle;
    struct transfer_buf_map_args *tranx_args = NULL;
    struct transfer_buf_map *     addr_args;
    pdc_metadata_t *              target_meta = NULL;
    region_buf_map_t *            buf_map_ptr;
    int                           error = 0;

    FUNC_ENTER(NULL);

    /* time_t t; */
    /* struct tm tm; */
    /* t = time(NULL); */
    /* tm = *localtime(&t); */
    /* printf("start PDC_Meta_Server_buf_map %02d:%02d:%02d\n", tm.tm_hour, tm.tm_min, tm.tm_sec); */

    // dataserver and metadata server is on the same node
    if ((uint32_t)pdc_server_rank_g == in->meta_server_id) {
        target_meta = find_metadata_by_id(in->remote_obj_id);
        if (target_meta == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL, "PDC_SERVER: PDC_Meta_Server_buf_map() find_metadata_by_id FAILED!");
        }

        buf_map_ptr = (region_buf_map_t *)malloc(sizeof(region_buf_map_t));
        if (buf_map_ptr == NULL) {
            error = 1;
            PGOTO_ERROR(FAIL,
                        "PDC_SERVER: PDC_Server_insert_buf_map_region() allocates region pointer failed");
        }
        buf_map_ptr->local_reg_id    = new_buf_map_ptr->local_reg_id;
        buf_map_ptr->local_region    = new_buf_map_ptr->local_region;
        buf_map_ptr->local_ndim      = new_buf_map_ptr->local_ndim;
        buf_map_ptr->local_data_type = new_buf_map_ptr->local_data_type;

        buf_map_ptr->remote_obj_id        = new_buf_map_ptr->remote_obj_id;
        buf_map_ptr->remote_reg_id        = new_buf_map_ptr->remote_reg_id;
        buf_map_ptr->remote_client_id     = new_buf_map_ptr->remote_client_id;
        buf_map_ptr->remote_ndim          = new_buf_map_ptr->remote_ndim;
        buf_map_ptr->remote_unit          = new_buf_map_ptr->remote_unit;
        buf_map_ptr->remote_region_unit   = new_buf_map_ptr->remote_region_unit;
        buf_map_ptr->remote_region_nounit = new_buf_map_ptr->remote_region_nounit;
        buf_map_ptr->remote_data_ptr      = new_buf_map_ptr->remote_data_ptr;

#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&meta_buf_map_mutex_g);
#endif
        DL_APPEND(target_meta->region_buf_map_head, buf_map_ptr);
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_unlock(&meta_buf_map_mutex_g);
#endif

        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
    }
    else {
#ifdef ENABLE_MULTITHREAD
        hg_thread_mutex_lock(&addr_valid_mutex_g);
#endif
        if (pdc_remote_server_info_g[in->meta_server_id].addr_valid != 1) {
            addr_args         = (struct transfer_buf_map *)malloc(sizeof(struct transfer_buf_map));
            addr_args->handle = *handle;
            addr_args->in     = *in;

            PDC_Server_buf_map_lookup_server_id(in->meta_server_id, addr_args);
        }
        else {
            HG_Create(hg_context_g, pdc_remote_server_info_g[in->meta_server_id].addr,
                      buf_map_server_register_id_g, &server_send_buf_map_handle);

            tranx_args         = (struct transfer_buf_map_args *)malloc(sizeof(struct transfer_buf_map_args));
            tranx_args->handle = *handle;
            tranx_args->in     = *in;
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

    /* t = time(NULL); */
    /* tm = *localtime(&t); */
    /* printf("done PDC_Meta_Server_buf_map %02d:%02d:%02d\n", tm.tm_hour, tm.tm_min, tm.tm_sec); */

    if (error == 1) {
        HG_Free_input(*handle, in);
        HG_Destroy(*handle);
        if ((uint32_t)pdc_server_rank_g != in->meta_server_id && tranx_args != NULL)
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
/*
static perr_t PDC_Server_merge_region_list_naive(region_list_t *list, region_list_t **merged)
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
*/

/*
 * Callback function for the region update, gets output from client
 *
 * \param  callback_info[OUT]      Mercury callback info
 *
 * \return Non-negative on success/Negative on failure
 */
static hg_return_t
PDC_Server_notify_region_update_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                       ret_value = HG_SUCCESS;
    hg_handle_t                       handle;
    notify_region_update_out_t        output;
    struct server_region_update_args *update_args;

    FUNC_ENTER(NULL);

    update_args = (struct server_region_update_args *)callback_info->arg;
    handle      = callback_info->info.forward.handle;

    /* Get output from client */
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: PDC_Server_notify_region_update_cb - error with HG_Get_output\n",
               pdc_server_rank_g);
        update_args->ret = -1;
        goto done;
    }
    update_args->ret = output.ret;

done:
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_notify_region_update_to_client(uint64_t obj_id, uint64_t reg_id, int32_t client_id)
{
    perr_t                           ret_value = SUCCEED;
    hg_return_t                      hg_ret;
    struct server_region_update_args update_args;
    hg_handle_t                      notify_region_update_handle;

    FUNC_ENTER(NULL);

    if (client_id >= pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: PDC_SERVER_notify_region_update_to_client() - "
               "client_id %d invalid)\n",
               pdc_server_rank_g, client_id);
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
    in.obj_id = obj_id;
    in.reg_id = reg_id;

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

perr_t
PDC_Server_close_shm(region_list_t *region, int is_remove)
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
/*
static hg_return_t
PDC_Server_notify_io_complete_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t ret_value;

    FUNC_ENTER(NULL);

    server_lookup_args_t *lookup_args = (server_lookup_args_t *)callback_info->arg;
    hg_handle_t           handle      = callback_info->info.forward.handle;

    // Get output from server
    notify_io_complete_out_t output;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - to client %" PRIu32 " "
               "- error with HG_Get_output\n",
               pdc_server_rank_g, __func__, lookup_args->client_id);
        lookup_args->ret_int = -1;
        goto done;
    }

    lookup_args->ret_int = output.ret;

done:
    HG_Free_output(handle, &output);
    FUNC_LEAVE(ret_value);
}
*/
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
/*
static perr_t PDC_Server_notify_io_complete_to_client(uint32_t client_id, uint64_t obj_id,
        char* shm_addr, PDC_access_t io_type)
{
    char tmp_shm[ADDR_MAX];
    perr_t ret_value   = SUCCEED;
    hg_return_t hg_ret = HG_SUCCESS;
    server_lookup_args_t lookup_args;
    hg_handle_t notify_io_complete_handle;

    FUNC_ENTER(NULL);

    if (client_id >= (uint32_t)pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: %s - client_id %d invalid\n", pdc_server_rank_g, __func__, client_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g[client_id].addr_valid != 1) {
        ret_value = PDC_Server_lookup_client(client_id);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - PDC_Server_lookup_client failed\n", pdc_server_rank_g, __func__);
            goto done;
        }
    }

    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr,
                notify_io_complete_register_id_g, &notify_io_complete_handle);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - HG_Create failed\n", pdc_server_rank_g, __func__);
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
    }
    else
        in.shm_addr   = shm_addr;

    lookup_args.client_id = client_id;
    hg_ret = HG_Forward(notify_io_complete_handle, PDC_Server_notify_io_complete_cb, &lookup_args, &in);
    if (hg_ret != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - HG_Forward failed\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);
    hg_ret = HG_Destroy(notify_io_complete_handle);
    if (hg_ret != HG_SUCCESS)
        printf("==PDC_SERVER[%d]: %s - HG_Destroy failed\n", pdc_server_rank_g, __func__);

    FUNC_LEAVE(ret_value);
}
*/

// Generic function to check the return value (RPC receipt) is 1
hg_return_t
PDC_Server_notify_client_multi_io_complete_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t             ret_value = HG_SUCCESS;
    pdc_int_ret_t           output;
    notify_multi_io_args_t *args = NULL;

    FUNC_ENTER(NULL);
    hg_handle_t handle = callback_info->info.forward.handle;
    args               = (notify_multi_io_args_t *)callback_info->arg;

    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS) {
        printf("==PDC_SERVER[%d]: %s - Error with HG_Get_output\n", pdc_server_rank_g, __func__);
        goto done;
    }

    if (output.ret != 1) {
        printf("==PDC_SERVER[%d]: %s - Return value [%d] is NOT expected\n", pdc_server_rank_g, __func__,
               output.ret);
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

perr_t
PDC_Server_notify_client_multi_io_complete(uint32_t client_id, int client_seq_id, int n_completed,
                                           region_list_t *completed_region_list)
{
    perr_t                  ret_value = SUCCEED;
    hg_return_t             hg_ret    = HG_SUCCESS;
    hg_handle_t             rpc_handle;
    hg_bulk_t               bulk_handle;
    void **                 buf_ptrs;
    hg_size_t *             buf_sizes;
    bulk_rpc_in_t           bulk_rpc_in;
    int                     i;
    region_list_t *         region_elt;
    notify_multi_io_args_t *bulk_args;

    FUNC_ENTER(NULL);

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
    buf_sizes = (hg_size_t *)calloc(sizeof(hg_size_t), n_completed * 2);
    buf_ptrs  = (void **)calloc(sizeof(void *), n_completed * 2);

    i = 0;
    DL_FOREACH(completed_region_list, region_elt)
    {
        buf_ptrs[i]                = region_elt->shm_addr;
        buf_sizes[i]               = strlen(buf_ptrs[i]) + 1;
        buf_ptrs[n_completed + i]  = (void *)&(region_elt->data_size);
        buf_sizes[n_completed + i] = sizeof(uint64_t);
        i++;
    }

    /* Register memory */
    hg_ret =
        HG_Bulk_create(hg_class_g, n_completed * 2, buf_ptrs, buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
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

    bulk_args              = (notify_multi_io_args_t *)calloc(1, sizeof(notify_multi_io_args_t));
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

done:
    fflush(stdout);
    HG_Destroy(rpc_handle);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Close_cache_file()
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
static perr_t
PDC_Server_cache_region_to_BB(region_list_t *region)
{
    perr_t   ret_value = SUCCEED;
    uint64_t write_bytes, offset;

    FUNC_ENTER(NULL);
    // Write to BB

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    /* double cache_total_sec; */
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
            printf("==PDC_SERVER[%d]: %s - No PDC_BB_LOC specified, use [%s]!\n", pdc_server_rank_g, __func__,
                   bb_data_path);
        }

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
    printf("==PDC_SERVER[%d]: %s - appending %" PRIu64 " bytes to BB\n", pdc_server_rank_g, __func__,
           region->data_size);
    write_bytes = fwrite(region->buf, 1, region->data_size, pdc_cache_file_ptr_g);
    if (write_bytes != region->data_size) {
        printf("==PDC_SERVER[%d]: fwrite to [%s] FAILED, size %" PRIu64 ", actual writeen %" PRIu64 "!\n",
               pdc_server_rank_g, region->storage_location, region->data_size, write_bytes);
        ret_value = FAIL;
        goto done;
    }
    n_fwrite_g++;

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    double region_write_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    server_write_time_g += region_write_time;
    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: fwrite %" PRIu64 " bytes, %.2fs\n", pdc_server_rank_g, write_bytes,
               region_write_time);
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
    ret_value             = PDC_Server_close_shm(region, 0);
    region->is_data_ready = 0;
    region->is_io_done    = 0;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_cache_region_to_bb_cb(const struct hg_cb_info *callback_info)
{
    perr_t                   ret;
    hg_return_t              ret_value = HG_SUCCESS;
    server_read_check_out_t *out;

    FUNC_ENTER(NULL);

    out = (server_read_check_out_t *)callback_info->arg;
    ret = PDC_Server_cache_region_to_BB(out->region);
    if (ret != SUCCEED)
        printf("==PDC_SERVER[%d]: %s - Error with PDC_Server_cache_region_to_BB\n", pdc_server_rank_g,
               __func__);

    if (out != NULL) {
        free(out);
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_read_check(data_server_read_check_in_t *in, server_read_check_out_t *out)
{
    perr_t                     ret_value = SUCCEED;
    pdc_data_server_io_list_t *io_elt = NULL, *io_target = NULL;
    region_list_t *            region_elt = NULL;
    region_list_t              r_target;
    /* uint32_t i; */

    FUNC_ENTER(NULL);

    pdc_metadata_t meta;
    PDC_metadata_init(&meta);
    PDC_transfer_t_to_metadata_t(&in->meta, &meta);

    PDC_init_region_list(&r_target);
    PDC_region_transfer_t_to_list_t(&(in->region), &r_target);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_read_list_mutex_g);
#endif
    // Iterate io list, find current request
    DL_FOREACH(pdc_data_server_read_list_head_g, io_elt)
    {
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
        printf("==PDC_SERVER[%d]: Read check Obj [%s] id=%" PRIu64 "  region: start(%" PRIu64 ", %" PRIu64
               ") "
               "size(%" PRIu64 ", %" PRIu64 ") \n",
               pdc_server_rank_g, meta.obj_name, meta.obj_id, r_target.start[0], r_target.start[1],
               r_target.count[0], r_target.count[1]);
    }

    int found_region = 0;
    DL_FOREACH(io_target->region_list_head, region_elt)
    {
        if (region_list_cmp(region_elt, &r_target) == 0) {
            // Found previous io request
            found_region        = 1;
            out->ret            = region_elt->is_data_ready;
            out->region         = NULL;
            out->is_cache_to_bb = 0;
            if (region_elt->is_data_ready == 1) {
                out->shm_addr = calloc(sizeof(char), ADDR_MAX);
                if (strlen(region_elt->shm_addr) == 0)
                    printf("==PDC_SERVER[%d]: %s - found shm_addr is NULL!\n", pdc_server_rank_g, __func__);
                else
                    strcpy(out->shm_addr, region_elt->shm_addr);
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
        printf("==PDC_SERVER[%d]: %s -  No io request with same region found!\n", pdc_server_rank_g,
               __func__);
        PDC_print_region_list(&r_target);
        out->ret = -1;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_write_check(data_server_write_check_in_t *in, data_server_write_check_out_t *out)
{
    perr_t                     ret_value = FAIL;
    pdc_data_server_io_list_t *io_elt = NULL, *io_target = NULL;
    region_list_t *            region_elt = NULL, *region_tmp = NULL;
    int                        found_region = 0;

    FUNC_ENTER(NULL);

    pdc_metadata_t meta;
    PDC_metadata_init(&meta);
    PDC_transfer_t_to_metadata_t(&in->meta, &meta);

    region_list_t r_target;
    PDC_init_region_list(&r_target);
    PDC_region_transfer_t_to_list_t(&(in->region), &r_target);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_write_list_mutex_g);
#endif
    // Iterate io list, find current request
    DL_FOREACH(pdc_data_server_write_list_head_g, io_elt)
    {
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
        out->ret  = -1;
        ret_value = SUCCEED;
        goto done;
    }

    DL_FOREACH_SAFE(io_target->region_list_head, region_elt, region_tmp)
    {
        if (region_list_cmp(region_elt, &r_target) == 0) {
            // Found io list
            found_region = 1;
            out->ret     = region_elt->is_data_ready;
            // NOTE: We don't want to remove the region here as this check request may come before
            //       the region is used to update the corresponding region metadata in remote server.
            // TODO: Instead, the region should be deleted some time after, when we know the region
            //       is no longer needed. But this could be tricky, as we don't know if the client
            //       want to read the data multliple times, so the best time is when the server recycles
            //       the shm associated with this region.

            ret_value = SUCCEED;
            goto done;
        }
    }

    if (found_region == 0) {
        printf("==PDC_SERVER[%d]: No existing IO request of requested region found!\n", pdc_server_rank_g);
        out->ret  = -1;
        ret_value = SUCCEED;
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Read the requested data to shared memory address
 *
 * \param  region_list_head[IN]       List of IO request to be performed
 * \param  obj_id[IN]                 Object ID of the IO request
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t
PDC_Server_data_read_to_shm(region_list_t *region_list_head)
{
    perr_t ret_value = FAIL;

    FUNC_ENTER(NULL);

    is_server_direct_io_g = 0;
    // TODO: merge regions for aggregated read

    // Now we have a -merged- list of regions to be read,
    // so just read one by one

    // POSIX read for now
    ret_value = PDC_Server_regions_io(region_list_head, PDC_POSIX);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: error reading data from storage and create shared memory\n",
               pdc_server_rank_g);
        goto done;
    }

    ret_value = SUCCEED;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_get_local_storage_location_of_region(uint64_t obj_id, region_list_t *region, uint32_t *n_loc,
                                                region_list_t **overlap_region_loc)
{
    perr_t          ret_value   = SUCCEED;
    pdc_metadata_t *target_meta = NULL;
    region_list_t * region_elt  = NULL;

    FUNC_ENTER(NULL);

    // Find object metadata
    *n_loc      = 0;
    target_meta = find_metadata_by_id(obj_id);
    if (target_meta == NULL) {
        printf("==PDC_SERVER[%d]: find_metadata_by_id FAILED!\n", pdc_server_rank_g);
        ret_value = FAIL;
        goto done;
    }
    DL_FOREACH(target_meta->storage_region_list_head, region_elt)
    {
        if (PDC_is_contiguous_region_overlap(region_elt, region) == 1) {
            // No need to make a copy here, but need to make sure we won't change it
            overlap_region_loc[*n_loc] = region_elt;
            *n_loc += 1;
        }
        if (*n_loc > PDC_MAX_OVERLAP_REGION_NUM) {
            printf("==PDC_SERVER[%d]: %s- exceeding PDC_MAX_OVERLAP_REGION_NUM regions!\n", pdc_server_rank_g,
                   __func__);
            ret_value = FAIL;
            goto done;
        }
    } // DL_FOREACH

    if (*n_loc == 0) {
        printf("==PDC_SERVER[%d]: %s - no overlapping storage region found\n", pdc_server_rank_g, __func__);
        PDC_print_region_list(region);
        ret_value = FAIL;
        goto done;
    }

done:
    FUNC_LEAVE(ret_value);
}

/*
 * Callback function to cleanup after storage meta bulk update
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t
update_region_storage_meta_bulk_cleanup_cb(update_storage_meta_list_t *meta_list_target, int *n_updated)
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
        pdc_nbuffered_bulk_update_g      = 0;
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
static perr_t
PDC_Server_update_storage_meta(int *n_updated)
{
    perr_t                      ret_value;
    update_storage_meta_list_t *meta_list_elt;

    FUNC_ENTER(NULL);

    // MPI Reduce implementation of metadata update
    *n_updated = 0;
    DL_FOREACH(pdc_update_storage_meta_list_head_g, meta_list_elt)
    {

        ret_value = PDC_Server_update_region_storage_meta_bulk_with_cb(
            meta_list_elt->storage_meta_bulk_xfer_data, update_region_storage_meta_bulk_cleanup_cb,
            meta_list_elt, n_updated);

        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - update storage info FAILED!", pdc_server_rank_g, __func__);
            goto done;
        }
    }

done:
    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Server_count_write_check_update_storage_meta_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t                    ret_value = HG_SUCCESS;
    data_server_write_check_out_t *write_ret;
    int                            n_updated = 0;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
#endif

    write_ret = (data_server_write_check_out_t *)callback_info->arg;

    if (write_ret->ret == 1) {
        n_check_write_finish_returned_g++;

        if (n_check_write_finish_returned_g >= pdc_buffered_bulk_update_total_g) {
            ret_value = PDC_Server_update_storage_meta(&n_updated);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - FAILED to update storage meta\n", pdc_server_rank_g, __func__);
                goto done;
            }

        } // end of if
    }     // end of if (write_ret->ret == 1)

done:

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    double update_region_location_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    server_update_region_location_time_g += update_region_location_time;
#endif

    if (write_ret)
        free(write_ret);
    fflush(stdout);

    FUNC_LEAVE(ret_value);
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
static perr_t
PDC_Server_get_storage_location_of_region_mpi(region_list_t *regions_head)
{
    perr_t                             ret_value = SUCCEED;
    uint32_t                           server_id = 0;
    uint32_t                           i, j;
    pdc_metadata_t *                   region_meta = NULL, *region_meta_prev = NULL;
    region_list_t *                    region_elt, req_region, **overlap_regions_2d = NULL;
    region_info_transfer_t             local_region_transfer[PDC_SERVER_MAX_PROC_PER_NODE];
    region_info_transfer_t *           all_requests        = NULL;
    update_region_storage_meta_bulk_t *send_buf            = NULL;
    update_region_storage_meta_bulk_t *result_storage_meta = NULL;
    uint32_t                           total_request_cnt;
    int                                data_size           = sizeof(region_info_transfer_t);
    int *                              send_bytes          = NULL;
    int *                              displs              = NULL;
    int *                              request_overlap_cnt = NULL;
    int                                nrequest_per_server = 0;

    FUNC_ENTER(NULL);

    if (regions_head == NULL) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    nrequest_per_server = 0;
    region_meta_prev    = regions_head->meta;
    DL_FOREACH(regions_head, region_elt)
    {
        if (region_elt->access_type == PDC_READ) {

            region_meta = region_elt->meta;
            // All requests should point to the same metadata
            if (region_meta == NULL) {
                printf("==PDC_SERVER[%d]: %s - request region has NULL metadata!\n", pdc_server_rank_g,
                       __func__);
                ret_value = FAIL;
                goto done;
            }
            else if (region_meta->obj_id != region_meta_prev->obj_id) {
                printf("==PDC_SERVER[%d]: %s - request regions are of different object!\n", pdc_server_rank_g,
                       __func__);
                ret_value = FAIL;
                goto done;
            }
            region_meta_prev = region_meta;

            // nrequest_per_server should be less than PDC_SERVER_MAX_PROC_PER_NODE
            // and should be the same across all servers.
            if (nrequest_per_server > PDC_SERVER_MAX_PROC_PER_NODE) {
                printf("==PDC_SERVER[%d]: %s - more requests than expected! "
                       "Increase PDC_SERVER_MAX_PROC_PER_NODE!\n",
                       pdc_server_rank_g, __func__);
                fflush(stdout);
            }
            else {
                // After saninty check, add the current request to gather send buffer
                PDC_region_list_t_to_transfer(region_elt, &local_region_transfer[nrequest_per_server]);
                nrequest_per_server++;
            }
        }
    } // end of DL_FOREACH

    // NOTE: Assume nrequest_per_server are the same across all servers
    server_id = PDC_get_server_by_obj_id(region_meta->obj_id, pdc_server_size_g);

    // Only recv server needs allocation
    if (server_id == (uint32_t)pdc_server_rank_g) {
        all_requests = (region_info_transfer_t *)calloc(pdc_server_size_g, data_size * nrequest_per_server);
    }
    else
        all_requests = local_region_transfer;

#ifdef ENABLE_MPI
    // Gather the requests from all data servers to metadata server
    // equivalent to all data server send requests to metadata server
    MPI_Gather(&local_region_transfer, nrequest_per_server * data_size, MPI_CHAR, all_requests,
               nrequest_per_server * data_size, MPI_CHAR, server_id, MPI_COMM_WORLD);
#endif

    // NOTE: Assumes all data server send equal number of requests
    total_request_cnt   = nrequest_per_server * pdc_server_size_g;
    send_bytes          = (int *)calloc(sizeof(int), pdc_server_size_g);
    displs              = (int *)calloc(sizeof(int), pdc_server_size_g);
    request_overlap_cnt = (int *)calloc(total_request_cnt, sizeof(int));
    // ^storage meta results in all_requests to be returned to all data servers

    // Now server_id has all the data in all_requests, find all storage regions that overlaps with it
    // equivalent to storage metadadata searching
    if (server_id == (uint32_t)pdc_server_rank_g) {
        send_buf = (update_region_storage_meta_bulk_t *)calloc(sizeof(update_region_storage_meta_bulk_t),
                                                               pdc_server_size_g * nrequest_per_server *
                                                                   PDC_MAX_OVERLAP_REGION_NUM);

        // All participants are querying the same object, so obj_ids are the same
        // Search one by one
        int      found_cnt   = 0;
        uint32_t overlap_cnt = 0;
        int      server_idx;
        // overlap_regions_2d has the ptrs to overlap storage regions to current request
        overlap_regions_2d = (region_list_t **)calloc(sizeof(region_list_t *), PDC_MAX_OVERLAP_REGION_NUM);
        for (i = 0; i < total_request_cnt; i++) {
            server_idx = i / nrequest_per_server;
            // server_idx should be [0, pdc_server_size_g)
            if (server_idx < 0 || server_idx >= pdc_server_size_g) {
                printf("==PDC_SERVER[%d]: %s - ERROR with server idx count!\n", pdc_server_rank_g, __func__);
                ret_value = FAIL;
                goto done;
            }
            memset(&req_region, 0, sizeof(region_list_t));
            PDC_region_transfer_t_to_list_t(&all_requests[i], &req_region);
            ret_value = PDC_Server_get_local_storage_location_of_region(region_meta->obj_id, &req_region,
                                                                        &overlap_cnt, overlap_regions_2d);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - unable to get local storage location!\n", pdc_server_rank_g,
                       __func__);
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
            if (server_idx > 0)
                displs[server_idx] = displs[server_idx - 1] + send_bytes[server_idx - 1];
            // for each request, copy all overlap storage meta to send buf
            for (j = 0; j < overlap_cnt; j++) {
                PDC_region_list_t_to_transfer(overlap_regions_2d[j], &send_buf[found_cnt].region_transfer);
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
    result_storage_meta = (update_region_storage_meta_bulk_t *)calloc(
        sizeof(update_region_storage_meta_bulk_t), nrequest_per_server * PDC_MAX_OVERLAP_REGION_NUM);

#ifdef ENABLE_MPI
    MPI_Bcast(request_overlap_cnt, total_request_cnt, MPI_INT, server_id, MPI_COMM_WORLD);
    MPI_Bcast(send_bytes, pdc_server_size_g, MPI_INT, server_id, MPI_COMM_WORLD);
    MPI_Bcast(displs, pdc_server_size_g, MPI_INT, server_id, MPI_COMM_WORLD);

    MPI_Scatterv(send_buf, send_bytes, displs, MPI_CHAR, result_storage_meta, send_bytes[pdc_server_rank_g],
                 MPI_CHAR, server_id, MPI_COMM_WORLD);
#else
    result_storage_meta = send_buf;
#endif

    // result_storage_meta has all the request storage region location for requests
    // Put results to the region lists
    int overlap_idx = pdc_server_rank_g * nrequest_per_server;
    int region_idx  = 0;
    int result_idx  = 0;
    // We will have the same linked list traversal order as before, so no need to check region match
    DL_FOREACH(regions_head, region_elt)
    {
        if (region_elt->access_type == PDC_READ) {
            region_elt->n_overlap_storage_region = request_overlap_cnt[overlap_idx + region_idx];
            region_elt->overlap_storage_regions =
                (region_list_t *)calloc(sizeof(region_list_t), region_elt->n_overlap_storage_region);
            for (i = 0; i < region_elt->n_overlap_storage_region; i++) {
                PDC_region_transfer_t_to_list_t(&result_storage_meta[result_idx].region_transfer,
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

#ifdef ENABLE_MPI
    if (server_id == (uint32_t)pdc_server_rank_g) {
        if (all_requests)
            free(all_requests);
        if (overlap_regions_2d)
            free(overlap_regions_2d);
        if (send_buf)
            free(send_buf);
    }
    if (result_storage_meta)
        free(result_storage_meta);
#else
    if (overlap_regions_2d)
        free(overlap_regions_2d);
    if (send_buf)
        free(send_buf);
#endif
    if (send_bytes)
        free(send_bytes);
    if (displs)
        free(displs);
    if (request_overlap_cnt)
        free(request_overlap_cnt);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_regions_io(region_list_t *region_list_head, _pdc_io_plugin_t plugin)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
#endif

    // If read, need to get locations from metadata server
    if (plugin == PDC_POSIX) {
        ret_value = PDC_Server_posix_one_file_io(region_list_head);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s-error with PDC_Server_posix_one_file_io\n", pdc_server_rank_g,
                   __func__);
            goto done;
        }
    }
    else if (plugin == PDC_DAOS) {
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
static perr_t
PDC_Server_data_write_from_shm(region_list_t *region_list_head)
{
    perr_t         ret_value = SUCCEED;
    region_list_t *region_elt;

    FUNC_ENTER(NULL);

    is_server_direct_io_g = 0;
    // TODO: Merge regions

    // Now we have a merged list of regions to be read,
    // so just write one by one

    // Open the clients' shared memory for data to be written to storage
    DL_FOREACH(region_list_head, region_elt)
    {
        if (region_elt->is_io_done == 1)
            continue;

        if (region_elt->shm_fd > 0 && region_elt->buf != NULL)
            continue;

        // Calculate io size
        if (region_elt->data_size == 0)
            region_elt->data_size = PDC_get_region_size(region_elt);

        // Open shared memory and map to data buf
        region_elt->shm_fd = shm_open(region_elt->shm_addr, O_RDONLY, 0666);
        if (region_elt->shm_fd == -1) {
            printf("==PDC_SERVER[%d]: %s - Shared memory open failed [%s]!\n", pdc_server_rank_g, __func__,
                   region_elt->shm_addr);
            ret_value = FAIL;
            goto done;
        }

        region_elt->buf = mmap(0, region_elt->data_size, PROT_READ, MAP_SHARED, region_elt->shm_fd, 0);
        if (region_elt->buf == MAP_FAILED) {
            // printf("==PDC_SERVER[%d]: Map failed: %s\n", pdc_server_rank_g, strerror(errno));
            // close and unlink?
            ret_value = FAIL;
            goto done;
        }
    }

    // POSIX write
    ret_value = PDC_Server_regions_io(region_list_head, PDC_POSIX);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER: PDC_Server_regions_io ERROR!\n");
        goto done;
    }

done:
    fflush(stdout);

    // TODO: keep the shared memory for now and close them later?
    DL_FOREACH(region_list_head, region_elt)
    {
        if (region_elt->is_io_done == 1)
            ret_value = PDC_Server_close_shm(region_elt, 1);
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Server_data_io_via_shm(const struct hg_cb_info *callback_info)
{
    perr_t ret_value = SUCCEED;

    pdc_data_server_io_list_t *io_list_elt, *io_list = NULL, *io_list_target = NULL;
    region_list_t *            region_elt  = NULL, *region_tmp;
    int                        real_bb_cnt = 0, real_lustre_cnt = 0;
    int                        write_to_bb_cnt = 0;
    int                        count;
    size_t                     i;

    FUNC_ENTER(NULL);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
#endif

    data_server_io_info_t *io_info = (data_server_io_info_t *)callback_info->arg;

    if (io_info->io_type == PDC_WRITE) {
        // Set the number of storage metadta update to be buffered, number is relative to
        // write requests from all node local clients for one object
        if (pdc_buffered_bulk_update_total_g == 0) {
            pdc_buffered_bulk_update_total_g = io_info->nbuffer_request * io_info->nclient;
        }

        // Set the buffer io request
        if (buffer_write_request_total_g == 0)
            buffer_write_request_total_g = io_info->nbuffer_request * io_info->nclient;
        buffer_write_request_num_g++;
    }
    else if (io_info->io_type == PDC_READ) {
        if (buffer_read_request_total_g == 0)
            buffer_read_request_total_g = io_info->nbuffer_request * io_info->nclient;
        buffer_read_request_num_g++;
    }
    else {
        printf("==PDC_SERVER: PDC_Server_data_io_via_shm - invalid IO type received from client!\n");
        ret_value = FAIL;
        goto done;
    }
    fflush(stdout);

#ifdef ENABLE_MULTITHREAD
    if (io_info->io_type == PDC_WRITE)
        hg_thread_mutex_lock(&data_write_list_mutex_g);
    else if (io_info->io_type == PDC_READ)
        hg_thread_mutex_lock(&data_read_list_mutex_g);
#endif
    // Iterate io list, find the IO list of this obj
    if (io_info->io_type == PDC_WRITE)
        io_list = pdc_data_server_write_list_head_g;
    else if (io_info->io_type == PDC_READ)
        io_list = pdc_data_server_read_list_head_g;

    DL_FOREACH(io_list, io_list_elt)
    {
        if (io_info->meta.obj_id == io_list_elt->obj_id) {
            io_list_target = io_list_elt;
            break;
        }
    }
#ifdef ENABLE_MULTITHREAD
    if (io_info->io_type == PDC_WRITE)
        hg_thread_mutex_unlock(&data_write_list_mutex_g);
    else if (io_info->io_type == PDC_READ)
        hg_thread_mutex_unlock(&data_read_list_mutex_g);
#endif

    // If not found, create and insert one to the list
    if (NULL == io_list_target) {
        // pdc_data_server_io_list_t maintains the request list for one object id,
        // write and read are separate lists
        io_list_target = (pdc_data_server_io_list_t *)calloc(1, sizeof(pdc_data_server_io_list_t));
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

        if (io_info->io_type == PDC_WRITE) {
            char *data_path                = NULL;
            char *bb_data_path             = NULL;
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
                snprintf(io_list_target->bb_path, ADDR_MAX, "%.200s/pdc_data/cont_%" PRIu64 "", bb_data_path,
                         io_info->meta.cont_id);

            // Auto generate a data location path for storing the data
            snprintf(io_list_target->path, ADDR_MAX, "%.200s/pdc_data/cont_%" PRIu64 "", data_path,
                     io_info->meta.cont_id);
        }

        io_list_target->region_list_head = NULL;

        // Add to the io list
        if (io_info->io_type == PDC_WRITE)
            DL_APPEND(pdc_data_server_write_list_head_g, io_list_target);
        else if (io_info->io_type == PDC_READ)
            DL_APPEND(pdc_data_server_read_list_head_g, io_list_target);
    }

    io_list_target->count++;
    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: received %d/%d data %s requests of [%s]\n", pdc_server_rank_g,
               io_list_target->count, io_list_target->total, io_info->io_type == PDC_READ ? "read" : "write",
               io_info->meta.obj_name);
        fflush(stdout);
    }

    int has_read_cache = 0;
    // Need to check if there is already one region in the list, and update accordingly
    DL_FOREACH_SAFE(io_list_target->region_list_head, region_elt, region_tmp)
    {
        // Only compares the start and count to see if two are identical
        if (PDC_is_same_region_list(&(io_info->region), region_elt) == 1) {
            // free the shm if read
            if (io_info->io_type == PDC_READ) {
                has_read_cache = 1;
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
        region_list_t *new_region = (region_list_t *)calloc(1, sizeof(region_list_t));
        if (new_region == NULL) {
            printf("==PDC_SERVER: ERROR allocating new_region!\n");
            ret_value = FAIL;
            goto done;
        }
        PDC_region_list_t_deep_cp(&(io_info->region), new_region);

        DL_APPEND(io_list_target->region_list_head, new_region);
        if (is_debug_g == 1) {
            DL_COUNT(io_list_target->region_list_head, region_elt, count);
            printf("==PDC_SERVER[%d]: Added 1 to IO request list, obj_id=%" PRIu64 ", %d total\n",
                   pdc_server_rank_g, new_region->meta->obj_id, count);
            PDC_print_region_list(new_region);
        }
    }

    // Write
    if (io_info->io_type == PDC_WRITE && buffer_write_request_num_g >= buffer_write_request_total_g &&
        buffer_write_request_total_g != 0) {

        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: received all %d requests, starts writing.\n", pdc_server_rank_g,
                   buffer_write_request_total_g);
            fflush(stdout);
        }

        // Perform IO for all requests in each io_list (of unique obj_id)
        DL_FOREACH(pdc_data_server_write_list_head_g, io_list_elt)
        {

            // Sort the list so it is ordered by client id
            DL_SORT(io_list_elt->region_list_head, region_list_cmp_by_client_id);

            // received all requests, start writing
            // Some server write to BB when specified
            int curr_cnt    = 0;
            write_to_bb_cnt = io_list_elt->total * write_to_bb_percentage_g / 100;
            real_bb_cnt     = 0;
            real_lustre_cnt = 0;
            // Specify the location of data to be written to
            DL_FOREACH(io_list_elt->region_list_head, region_elt)
            {

                snprintf(region_elt->storage_location, ADDR_MAX, "%.200s/server%d/s%04d.bin",
                         io_list_elt->path, pdc_server_rank_g, pdc_server_rank_g);
                real_lustre_cnt++;

                // If BB is enabled, then overwrite with BB path with the right number of servers
                if (write_to_bb_percentage_g > 0) {
                    if (strcmp(io_list_elt->bb_path, "") == 0 || io_list_elt->bb_path[0] == 0) {
                        printf("==PDC_SERVER[%d]: Error with BB path [%s]!\n", pdc_server_rank_g,
                               io_list_elt->bb_path);
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
            }
            ret_value = PDC_Server_data_write_from_shm(io_list_elt->region_list_head);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_Server_data_write_from_shm FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }
        } // end for io_list
        buffer_write_request_num_g   = 0;
        buffer_write_request_total_g = 0;
    } // End write
    else if (io_info->io_type == PDC_READ && buffer_read_request_num_g == buffer_read_request_total_g &&
             buffer_read_request_total_g != 0) {
        // Read
        // TODO TEMPWORK
        cache_percentage_g            = io_info->cache_percentage;
        current_read_from_cache_cnt_g = 0;
        total_read_from_cache_cnt_g   = buffer_read_request_total_g * cache_percentage_g / 100;
        if (pdc_server_rank_g == 0) {
            printf("==PDC_SERVER[%d]: cache percentage %d%% read_from_cache %d/%d\n", pdc_server_rank_g,
                   cache_percentage_g, current_read_from_cache_cnt_g, total_read_from_cache_cnt_g);
        }

        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: received all %d requests, starts reading.\n", pdc_server_rank_g,
                   buffer_read_request_total_g);
        }
        DL_FOREACH(pdc_data_server_read_list_head_g, io_list_elt)
        {
            if (cache_percentage_g == 100) {
                // check if everything in this list has been cached
                int all_cached = 1;
                DL_FOREACH(io_list_elt->region_list_head, region_elt)
                {
                    if (region_elt->is_io_done != 1) {
                        all_cached = 0;
                        break;
                    }
                }

                if (all_cached == 1) {
                    continue;
                }
            }
            ret_value = PDC_Server_data_read_to_shm(io_list_elt->region_list_head);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_Server_data_read_to_shm FAILED!\n", pdc_server_rank_g);
                goto done;
            }
        }
        buffer_read_request_num_g   = 0;
        buffer_read_request_total_g = 0;
    } // End read

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    server_io_elapsed_time_g += PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_update_local_region_storage_loc(region_list_t *region, uint64_t obj_id, int type)
{
    perr_t          ret_value   = SUCCEED;
    pdc_metadata_t *target_meta = NULL;
    /* pdc_metadata_t *region_meta = NULL; */
    region_list_t *region_elt = NULL, *new_region = NULL;
    int            update_success = -1;

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
    DL_FOREACH(target_meta->storage_region_list_head, region_elt)
    {
        if (PDC_is_same_region_list(region_elt, region) == 1) {
            // Update location and offset
            if (type == PDC_UPDATE_CACHE) {
                memcpy(region_elt->cache_location, region->storage_location, sizeof(char) * ADDR_MAX);
                region_elt->cache_offset = region->offset;
            }
            else if (type == PDC_UPDATE_STORAGE) {
                memcpy(region_elt->storage_location, region->storage_location, sizeof(char) * ADDR_MAX);
                region_elt->offset = region->offset;
                if (region->region_hist != NULL)
                    region_elt->region_hist = region->region_hist;
            }
            else {
                printf("==PDC_SERVER[%d]: %s - error with update type %d!\n", pdc_server_rank_g, __func__,
                       type);
                break;
            }

            if (region_elt->data_size == 0)
                region_elt->data_size = PDC_get_region_size(region_elt);

            update_success = 1;

            break;
        }
    } // DL_FOREACH

    // Insert the storage region if not found
    if (update_success == -1) {

        // Create the region list
        new_region = (region_list_t *)calloc(1, sizeof(region_list_t));
        if (PDC_region_list_t_deep_cp(region, new_region) != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - deep copy FAILED!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        new_region->meta        = target_meta;
        new_region->region_hist = region->region_hist;
        if (new_region->data_size == 0)
            new_region->data_size = PDC_get_region_size(new_region);
        DL_APPEND(target_meta->storage_region_list_head, new_region);
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

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
    hg_return_t           ret_value;
    server_lookup_args_t *lookup_args;
    hg_handle_t           handle;
    metadata_update_out_t output;

    FUNC_ENTER(NULL);

    lookup_args = (server_lookup_args_t *)callback_info->arg;
    handle      = callback_info->info.forward.handle;

    /* Get output from server*/
    ret_value = HG_Get_output(handle, &output);
    if (ret_value != HG_SUCCESS || output.ret != 20171031) {
        printf("==PDC_SERVER[%d]: %s - error HG_Get_output\n", pdc_server_rank_g, __func__);
        lookup_args->ret_int = -1;
        goto done;
    }

    lookup_args->ret_int = output.ret;
    update_remote_region_count_g++;

done:
    HG_Free_output(handle, &output);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_update_region_storagelocation_offset(region_list_t *region, int type)
{
    hg_return_t            hg_ret;
    perr_t                 ret_value   = SUCCEED;
    uint32_t               server_id   = 0;
    pdc_metadata_t *       region_meta = NULL;
    hg_handle_t            update_region_loc_handle;
    update_region_loc_in_t in;
    server_lookup_args_t   lookup_args;

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

    if (server_id == (uint32_t)pdc_server_rank_g) {
        // Metadata object is local, no need to send update RPC
        ret_value = PDC_Server_update_local_region_storage_loc(region, region_meta->obj_id, type);
        update_local_region_count_g++;
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - update_local_region_storage FAILED!\n", pdc_server_rank_g,
                   __func__);
            goto done;
        }
    }
    else {

        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", pdc_server_rank_g,
                   server_id);
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
            printf("==PDC_SERVER[%d]: Sending updated region loc to server %d\n", pdc_server_rank_g,
                   server_id);
            fflush(stdout);
        }

        in.obj_id           = region->meta->obj_id;
        in.offset           = region->offset;
        in.storage_location = region->storage_location;
        in.type             = type;
        in.has_hist         = 0;
        PDC_region_list_t_to_transfer(region, &(in.region));

        if (region->region_hist != NULL) {
            in.has_hist = 1;
            memset(&in.hist, 0, sizeof(pdc_histogram_t));
            PDC_copy_hist(&in.hist, region->region_hist);
        }

        if (in.hist.nbin == 0) {
            printf("==PDC_SERVER[%d]: %s - ERROR sending hist to server %d with 0 bins\n", pdc_server_rank_g,
                   __func__, server_id);
        }

        lookup_args.rpc_handle = update_region_loc_handle;
        hg_ret = HG_Forward(update_region_loc_handle, PDC_Server_update_region_loc_cb, &lookup_args, &in);
        if (hg_ret != HG_SUCCESS) {
            printf("==PDC_SERVER[%d]: %s - HG_Forward() to server %d FAILED\n", pdc_server_rank_g, __func__,
                   server_id);
            HG_Destroy(update_region_loc_handle);
            ret_value = FAIL;
            goto done;
        }
        HG_Destroy(update_region_loc_handle);
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_add_region_storage_meta_to_bulk_buf(region_list_t *region, bulk_xfer_data_t *bulk_data)
{
    perr_t                             ret_value = SUCCEED;
    int                                i;
    uint64_t                           obj_id = 0;
    update_region_storage_meta_bulk_t *curr_buf_ptr;
    uint64_t *                         obj_id_ptr;

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

        bulk_data->buf_ptrs = (void **)calloc(sizeof(void *), PDC_BULK_XFER_INIT_NALLOC + 1);
        update_region_storage_meta_bulk_t *buf_ptrs_1d = (update_region_storage_meta_bulk_t *)calloc(
            sizeof(update_region_storage_meta_bulk_t), PDC_BULK_XFER_INIT_NALLOC);

        bulk_data->buf_sizes = (hg_size_t *)calloc(sizeof(hg_size_t), PDC_BULK_XFER_INIT_NALLOC);
        if (NULL == buf_ptrs_1d || NULL == bulk_data->buf_sizes) {
            printf("==PDC_SERVER[%d]: %s - calloc FAILED!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }

        // first element of bulk_buf is the obj_id
        bulk_data->buf_ptrs[0]  = (void *)calloc(sizeof(uint64_t), 1);
        bulk_data->buf_sizes[0] = sizeof(uint64_t);

        for (i = 1; i < PDC_BULK_XFER_INIT_NALLOC + 1; i++) {
            bulk_data->buf_ptrs[i]  = &buf_ptrs_1d[i];
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

    obj_id_ptr = (uint64_t *)bulk_data->buf_ptrs[0];

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
        *obj_id_ptr          = obj_id;
        bulk_data->target_id = PDC_get_server_by_obj_id(obj_id, pdc_server_size_g);
        bulk_data->obj_id    = obj_id;
    }

    // Copy data from region to corresponding buf ptr in bulk_data
    curr_buf_ptr = (update_region_storage_meta_bulk_t *)(bulk_data->buf_ptrs[idx]);
    PDC_region_list_t_to_transfer(region, &curr_buf_ptr->region_transfer);
    strcpy(curr_buf_ptr->storage_location, region->storage_location);
    curr_buf_ptr->offset = region->offset;

    bulk_data->idx++;
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_update_region_storage_meta_bulk_local(update_region_storage_meta_bulk_t **bulk_ptrs, int cnt)
{
    perr_t                             ret_value = SUCCEED;
    int                                i;
    pdc_metadata_t *                   target_meta = NULL;
    region_list_t *                    region_elt = NULL, *new_region = NULL;
    update_region_storage_meta_bulk_t *bulk_ptr;
    int                                update_success = 0, express_insert = 0;
    uint64_t                           obj_id;

    FUNC_ENTER(NULL);

    if (NULL == bulk_ptrs || cnt == 0 || NULL == bulk_ptrs[0]) {
        printf("==PDC_SERVER[%d]: %s invalid input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    obj_id = *(uint64_t *)bulk_ptrs[0];

    // First ptr in buf_ptrs is the obj_id
    for (i = 1; i < cnt; i++) {

        bulk_ptr = (update_region_storage_meta_bulk_t *)(bulk_ptrs[i]);

        // Create a new region for each and copy the data from bulk data
        new_region = (region_list_t *)calloc(1, sizeof(region_list_t));
        PDC_region_transfer_t_to_list_t(&bulk_ptr->region_transfer, new_region);
        new_region->data_size = PDC_get_region_size(new_region);
        strcpy(new_region->storage_location, bulk_ptr->storage_location);
        new_region->offset = bulk_ptr->offset;

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
            DL_FOREACH(target_meta->storage_region_list_head, region_elt)
            {
                if (PDC_is_same_region_list(region_elt, new_region) == 1) {
                    // Update location and offset
                    strcpy(region_elt->storage_location, new_region->storage_location);
                    region_elt->offset = new_region->offset;
                    update_success     = 1;

                    printf("==PDC_SERVER[%d]: overwrite existing region location/offset\n",
                           pdc_server_rank_g);
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

done:
    FUNC_LEAVE(ret_value);
}

static hg_return_t
update_storage_meta_bulk_rpc_cb(const struct hg_cb_info *callback_info)
{
    hg_handle_t                             handle = callback_info->info.forward.handle;
    pdc_int_ret_t                           bulk_rpc_ret;
    hg_return_t                             ret = HG_SUCCESS;
    update_region_storage_meta_bulk_args_t *cb_args =
        (update_region_storage_meta_bulk_args_t *)callback_info->arg;

    // Sent the bulk handle with rpc and get a response
    ret = HG_Get_output(handle, &bulk_rpc_ret);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get output\n");
        goto done;
    }

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
        cb_args->cb((update_storage_meta_list_t *)cb_args->meta_list_target, cb_args->n_updated);

    /* Free memory handle */
    ret = HG_Bulk_free(cb_args->bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free bulk data handle\n");
        goto done;
    }

done:
    return ret;
}

perr_t
PDC_Server_update_region_storage_meta_bulk_mpi(bulk_xfer_data_t *bulk_data)
{
    perr_t ret_value = SUCCEED;
#ifdef ENABLE_MPI
    int                                i;
    uint32_t                           server_id = 0;
    update_region_storage_meta_bulk_t *recv_buf  = NULL;
    void **                            all_meta  = NULL;
#endif

    FUNC_ENTER(NULL);

#ifdef ENABLE_MPI
    server_id        = bulk_data->target_id;
    int meta_cnt     = bulk_data->idx - 1; // idx includes the first element of buf_ptrs, which is count
    int data_size    = sizeof(update_region_storage_meta_bulk_t) * meta_cnt;
    int all_meta_cnt = meta_cnt * pdc_server_size_g;

    // Only recv server needs allocation
    if (server_id == (uint32_t)pdc_server_rank_g) {
        recv_buf = (update_region_storage_meta_bulk_t *)calloc(pdc_server_size_g, data_size);
    }

    // bulk_data->buf_ptrs[0] is number of metadata to be updated
    // Note: during add to buf_ptr process, we actually have the big 1D array allocated to buf_ptrs[1]
    //       so all metadata can be transferred with one single reduce
    // TODO: here we assume each server has exactly the same number of metadata
    MPI_Gather(bulk_data->buf_ptrs[1], data_size, MPI_CHAR, recv_buf, data_size, MPI_CHAR, server_id,
               MPI_COMM_WORLD);

    // Now server_id has all the data in recv_buf, start update all
    if (server_id == (uint32_t)pdc_server_rank_g) {
        all_meta    = (void **)calloc(sizeof(void *), all_meta_cnt + 1);
        all_meta[0] = bulk_data->buf_ptrs[0];
        for (i = 1; i < all_meta_cnt + 1; i++) {
            all_meta[i] = &recv_buf[i - 1];
        }

        ret_value = PDC_Server_update_region_storage_meta_bulk_local(
            (update_region_storage_meta_bulk_t **)all_meta, all_meta_cnt + 1);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - update_region_storage_meta_bulk_local FAILED!\n",
                   pdc_server_rank_g, __func__);
            goto done;
        }
        update_local_region_count_g += all_meta_cnt;
    } // end of if

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
}

perr_t
PDC_Server_update_region_storage_meta_bulk_with_cb(bulk_xfer_data_t *          bulk_data, perr_t (*cb)(),
                                                   update_storage_meta_list_t *meta_list_target,
                                                   int *                       n_updated)
{
    hg_return_t hg_ret;
    perr_t      ret_value = SUCCEED;

    uint32_t    server_id = 0;
    hg_handle_t rpc_handle;
    hg_bulk_t   bulk_handle;

    bulk_rpc_in_t                           bulk_rpc_in;
    update_region_storage_meta_bulk_args_t *cb_args;

    FUNC_ENTER(NULL);

    cb_args =
        (update_region_storage_meta_bulk_args_t *)calloc(1, sizeof(update_region_storage_meta_bulk_args_t));
    server_id = bulk_data->target_id;

    if (server_id == (uint32_t)pdc_server_rank_g) {

        ret_value = PDC_Server_update_region_storage_meta_bulk_local(
            (update_region_storage_meta_bulk_t **)bulk_data->buf_ptrs, bulk_data->idx);
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
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", pdc_server_rank_g,
                   server_id);
            ret_value = FAIL;
            goto done;
        }

        // Send the bulk handle to the target with RPC
        hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, bulk_rpc_register_id_g,
                           &rpc_handle);
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
        bulk_rpc_in.origin      = pdc_server_rank_g;
        bulk_rpc_in.cnt         = bulk_data->idx;
        bulk_rpc_in.bulk_handle = bulk_handle;

        cb_args->cb               = cb;
        cb_args->meta_list_target = meta_list_target;
        cb_args->n_updated        = n_updated;
        cb_args->server_id        = server_id;
        cb_args->bulk_handle      = bulk_handle;
        cb_args->rpc_handle       = rpc_handle;

        /* Forward call to remote addr */
        hg_ret = HG_Forward(rpc_handle, update_storage_meta_bulk_rpc_cb, cb_args, &bulk_rpc_in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not forward call\n");
            ret_value = FAIL;
            goto done;
        }

        meta_list_target->is_updated = 1;
    } // end of else

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
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
static perr_t
PDC_Server_read_overlap_regions(uint32_t ndim, uint64_t *req_start, uint64_t *req_count,
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

    FUNC_ENTER(NULL);

    *total_read_bytes = 0;
    if (ndim > 3 || ndim <= 0) {
        printf("==PDC_SERVER[%d]: dim=%" PRIu32 " unsupported yet!", pdc_server_rank_g, ndim);
        ret_value = FAIL;
        goto done;
    }

    if (req_count && req_count[0] == 0) {
        is_all_selected = 1;
        req_start[0]    = 0;
        req_count[0]    = storage_count[0];
        total_bytes     = storage_count[0];
        goto all_select;
    }

    // Get the actual start and count of region in storage
    if (PDC_get_overlap_start_count(ndim, req_start, req_count, storage_start, storage_count, overlap_start,
                                    overlap_count) != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_get_overlap_start_count FAILED!\n", pdc_server_rank_g);
        ret_value = FAIL;
        goto done;
    }

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

all_select:
    // TODO: additional optimization to check if any dimension is entirely selected
    if (ndim == 1 || is_all_selected == 1) {
        // Can read the entire storage region at once

#ifdef ENABLE_TIMING
        struct timeval pdc_timer_start1, pdc_timer_end1;
        gettimeofday(&pdc_timer_start1, 0);
#endif

        // Check if current file ptr is at correct pos
        uint64_t cur_off = (uint64_t)ftell(fp);
        if (cur_off != storage_offset) {
            fseek(fp, storage_offset, SEEK_SET);
        }

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: read storage offset %" PRIu64 ", buf_offset  %" PRIu64 "\n",
                   pdc_server_rank_g, storage_offset, buf_offset);
        }

        read_bytes = fread(buf + buf_offset, 1, total_bytes, fp);

#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end1, 0);
        double region_read_time1 = PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
        if (is_debug_g) {
            printf("==PDC_SERVER[%d]: fseek + fread %" PRIu64 " bytes, %.2fs\n", pdc_server_rank_g,
                   read_bytes, region_read_time1);
            fflush(stdout);
        }
#endif

        n_contig_MB += read_bytes / 1048576.0;
        n_contig_read++;
        if (read_bytes != total_bytes) {
            printf("==PDC_SERVER[%d]: %s - fread failed actual read bytes %" PRIu64 ", should be %" PRIu64
                   "\n",
                   pdc_server_rank_g, __func__, read_bytes, total_bytes);
            ret_value = FAIL;
            goto done;
        }
        *total_read_bytes += read_bytes;

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: Read entire storage region, size=%" PRIu64 "\n", pdc_server_rank_g,
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
                if (read_bytes != overlap_count[0]) {
                    printf("==PDC_SERVER[%d]: %s - fread failed!\n", pdc_server_rank_g, __func__);
                    ret_value = FAIL;
                    goto done;
                }
                *total_read_bytes += read_bytes;
            } // for each row
        }     // ndim=2
        else if (ndim == 3) {
            uint64_t buf_serialize_offset;
            /* fseek (fp, storage_offset, SEEK_SET); */
            for (j = 0; j < overlap_count[2]; j++) {
                fseek(fp, storage_offset + j * storage_count[0] * storage_count[1], SEEK_SET);

                for (i = 0; i < overlap_count[1]; i++) {
                    // Move to next row's begining position
                    if (i != 0)
                        fseek(fp, storage_count[0] - overlap_count[0], SEEK_CUR);

                    buf_serialize_offset = buf_offset + i * req_count[0] + j * req_count[0] * req_count[1];
                    if (is_debug_g == 1) {
                        printf("Read to buf offset: %" PRIu64 "\n", buf_serialize_offset);
                    }

                    read_bytes = fread(buf + buf_serialize_offset, 1, overlap_count[0], fp);
                    n_contig_MB += read_bytes / 1048576.0;
                    n_contig_read++;
                    if (read_bytes != overlap_count[0]) {
                        printf("==PDC_SERVER[%d]: %s - fread failed!\n", pdc_server_rank_g, __func__);
                        ret_value = FAIL;
                        goto done;
                    }
                    *total_read_bytes += read_bytes;
                    if (is_debug_g == 1) {
                        printf("z: %" PRIu64 ", j: %" PRIu64 ", Read data size=%" PRIu64 ": [%.*s]\n", j, i,
                               overlap_count[0], (int)overlap_count[0], (char *)buf + buf_serialize_offset);
                    }
                } // for each row
            }
        }
    } // end else (ndim != 1 && !is_all_selected);

    n_fread_g += n_contig_read;
    fread_total_MB += n_contig_MB;

    if (total_bytes != *total_read_bytes) {
        printf("==PDC_SERVER[%d]: %s - read size error!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

void
PDC_init_bulk_xfer_data_t(bulk_xfer_data_t *a)
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
static perr_t
PDC_Server_read_one_region(region_list_t *read_region)
{
    perr_t         ret_value         = SUCCEED;
    size_t         read_bytes        = 0;
    size_t         total_read_bytes  = 0;
    uint32_t       n_storage_regions = 0;
    region_list_t *region_elt;
    FILE *         fp_read        = NULL;
    char *         prev_path      = NULL;
    int            is_shm_created = 0, is_read_succeed = 0;
#ifdef ENABLE_TIMING
    double fopen_time;
#endif

    FUNC_ENTER(NULL);

    if (read_region->access_type != PDC_READ || read_region->n_overlap_storage_region == 0 ||
        read_region->overlap_storage_regions == NULL) {

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
    total_mem_cache_size_mb_g += (read_region->data_size / 1048576);

    // Now for each storage region that overlaps with request region,
    // read with the corresponding offset and size
    DL_FOREACH(read_region->overlap_storage_regions, region_elt)
    {

        if (is_debug_g == 1) {
            printf("==PDC_SERVER[%d]: Found overlapping storage regions %d\n", pdc_server_rank_g,
                   n_storage_regions);
        }

        if (region_elt->storage_location[0] == 0) {
            printf("==PDC_SERVER[%d]: empty overlapping storage location \n", pdc_server_rank_g);
            PDC_print_storage_region_list(region_elt);
            fflush(stdout);
            continue;
        }

        // If a new file needs to be opened
        if (prev_path == NULL || strcmp(region_elt->storage_location, prev_path) != 0) {

            if (fp_read != NULL) {
                fclose(fp_read);
                fp_read = NULL;
            }

#ifdef ENABLE_TIMING
            struct timeval pdc_timer_start2, pdc_timer_end2;
            gettimeofday(&pdc_timer_start2, 0);
#endif

            fp_read = fopen(region_elt->storage_location, "rb");
            if (fp_read == NULL) {
                printf("==PDC_SERVER[%d]: fopen failed [%s]\n", pdc_server_rank_g,
                       read_region->storage_location);
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
        ret_value = PDC_Server_read_overlap_regions(read_region->ndim, read_region->start, read_region->count,
                                                    region_elt->start, region_elt->count, fp_read,
                                                    region_elt->offset, read_region->buf, &read_bytes);

        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: error with PDC_Server_read_overlap_regions\n", pdc_server_rank_g);
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
        printf("==PDC_SERVER[%d]: Read data total size %" PRIu64 ", fopen time: %.3f\n", pdc_server_rank_g,
               total_read_bytes, fopen_time);
        fflush(stdout);
    }
#endif

    read_region->is_data_ready = 1;
    read_region->is_io_done    = 1;

done:
    // Close shm if read failed
    if (is_shm_created == 1 && is_read_succeed == 0) {
        PDC_Server_close_shm(read_region, 1);
        read_region->is_data_ready = 0;
        read_region->is_io_done    = 0;
    }
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

/*
 * Read with POSIX within one file, based on the region list
 * after the server has accumulated requests from all node local clients












 *


 * \param  region_list_head[IN]       Region info of IO request
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t
PDC_Server_posix_one_file_io(region_list_t *region_list_head)
{
    perr_t         ret_value  = SUCCEED;
    size_t         read_bytes = 0, write_bytes = 0;
    size_t         total_read_bytes = 0;
    uint64_t       offset           = 0;
    uint32_t       i                = 0;
    region_list_t *region_elt = NULL, *previous_region = NULL;
    FILE *         fp_read = NULL, *fp_write = NULL;
    char *         prev_path = NULL;
#ifdef ENABLE_LUSTRE
    int stripe_count, stripe_size;
#endif

    FUNC_ENTER(NULL);

    if (NULL == region_list_head) {
        printf("==PDC_SERVER[%d]: %s - NULL input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

// For read requests, it's better to aggregate read requests from all node-local clients
// and query once, rather than query one by one, so we aggregate at the beginning
#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start1, pdc_timer_end1;
    gettimeofday(&pdc_timer_start1, 0);
#endif

    DL_FOREACH(region_list_head, region_elt)
    {
        if (region_elt->access_type == PDC_READ) {

            if (region_elt->is_io_done == 1)
                continue;
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
    DL_FOREACH(region_list_head, region_elt)
    {
        if (region_elt->access_type == PDC_READ) {
            if (region_elt->is_io_done == 1 && region_elt->is_shm_closed != 1) {
                printf("==PDC_SERVER[%d]: found cached data!\n", pdc_server_rank_g);
                if (region_elt->access_type == PDC_READ &&
                    current_read_from_cache_cnt_g < total_read_from_cache_cnt_g)
                    current_read_from_cache_cnt_g++;
                else
                    continue;
            }

            // Prepare the shared memory for transfer back to client
            snprintf(region_elt->shm_addr, ADDR_MAX, "/PDC%d_%d", pdc_server_rank_g, rand());
            ret_value = PDC_create_shm_segment(region_elt);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - Error with shared memory creation\n", pdc_server_rank_g,
                       __func__);
                continue;
            }

            // Check if the region is cached to BB
            if (strstr(region_elt->cache_location, "PDCcacheBB") != NULL) {

#ifdef ENABLE_TIMING
                struct timeval pdc_timer_start11, pdc_timer_end11;
                gettimeofday(&pdc_timer_start1, 0);
#endif
                if (prev_path == NULL || strcmp(region_elt->cache_location, prev_path) != 0) {
                    if (fp_read != NULL)
                        fclose(fp_read);
                    fp_read = fopen(region_elt->cache_location, "rb");
                    if (fp_read == NULL) {
                        printf("==PDC_SERVER[%d]: %s - unable to open file [%s]\n", pdc_server_rank_g,
                               __func__, region_elt->cache_location);
                    }
                    n_fopen_g++;
                }
                offset = ftell(fp_read);
                if (offset < region_elt->cache_offset)
                    fseek(fp_read, region_elt->cache_offset - offset, SEEK_CUR);
                else
                    fseek(fp_read, region_elt->cache_offset, SEEK_SET);

                if (region_elt->data_size == 0) {
                    printf("==PDC_SERVER[%d]: %s - region data_size is 0\n", pdc_server_rank_g, __func__);
                    continue;
                }
                read_bytes = fread(region_elt->buf, 1, region_elt->data_size, fp_read);
                if (read_bytes != region_elt->data_size) {
                    printf("==PDC_SERVER[%d]: %s - read size %zu is not expected %" PRIu64 "\n",
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
                    // If a new file needs to be opened
                    if (prev_path == NULL ||
                        strcmp(region_elt->overlap_storage_regions[i].storage_location, prev_path) != 0) {

                        if (fp_read != NULL) {
                            fclose(fp_read);
                            fp_read = NULL;
                        }

#ifdef ENABLE_TIMING
                        struct timeval pdc_timer_start2, pdc_timer_end2;
                        gettimeofday(&pdc_timer_start2, 0);
#endif

                        if (strlen(region_elt->overlap_storage_regions[i].storage_location) > 0) {
                            fp_read = fopen(region_elt->overlap_storage_regions[i].storage_location, "rb");
                            n_fopen_g++;
                        }
                        else {
                            printf("==PDC_SERVER[%d]: %s - NULL storage location\n", pdc_server_rank_g,
                                   __func__);
                            fp_read = NULL;
                        }

#ifdef ENABLE_TIMING
                        gettimeofday(&pdc_timer_end2, 0);
                        server_fopen_time_g +=
                            PDC_get_elapsed_time_double(&pdc_timer_start2, &pdc_timer_end2);
#endif

                        if (fp_read == NULL) {
                            printf("==PDC_SERVER[%d]: fopen failed [%s]\n", pdc_server_rank_g,
                                   region_elt->storage_location);
                            ret_value = FAIL;
                            goto done;
                        }
                    }

                    // Request: elt->start/count
                    // Storage: region_elt->overlap_storage_regions[i]->start/count
                    ret_value = PDC_Server_read_overlap_regions(
                        region_elt->ndim, region_elt->start, region_elt->count,
                        region_elt->overlap_storage_regions[i].start,
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
                    printf("==PDC_SERVER[%d]: Read data total size %zu\n", pdc_server_rank_g,
                           total_read_bytes);
                    fflush(stdout);
                }
                offset += total_read_bytes;

            } // end else read from storage
            if (is_debug_g == 1) {
                printf("==PDC_SERVER[%d]: Read data total size %zu\n", pdc_server_rank_g, total_read_bytes);
                fflush(stdout);
            }
            region_elt->is_data_ready = 1;
            region_elt->is_io_done    = 1;

        } // end of READ
        else if (region_elt->access_type == PDC_WRITE) {
            if (region_elt->is_io_done == 1)
                continue;

            // Assumes all regions are written to one file
            if (region_elt->storage_location[0] == 0) {
                ret_value                 = FAIL;
                region_elt->is_data_ready = -1;
                goto done;
            }

            // Open file if needed:
            //   if this is first time to write data, or
            //   writing to a different location from last write.
            if (previous_region == NULL ||
                strcmp(region_elt->storage_location, previous_region->storage_location) != 0) {

                // Only need to mkdir once
                PDC_mkdir(region_elt->storage_location);

#ifdef ENABLE_LUSTRE
                // Set Lustre stripe only if this is Lustre
                // NOTE: this only applies to NERSC Lustre on Cori and Edison
                if (strstr(region_elt->storage_location, "/global/cscratch") != NULL ||
                    strstr(region_elt->storage_location, "/scratch1/scratchdirs") != NULL ||
                    strstr(region_elt->storage_location, "/scratch2/scratchdirs") != NULL) {

                    // When env var PDC_NOST_PER_FILE is not set
                    if (pdc_nost_per_file_g != 1)
                        stripe_count = 248 / pdc_server_size_g;
                    else
                        stripe_count = pdc_nost_per_file_g;
                    stripe_size = lustre_stripe_size_mb_g; // MB
                    PDC_Server_set_lustre_stripe(region_elt->storage_location, stripe_count, stripe_size);
                }
#endif

                // Close previous file
                if (fp_write != NULL) {
                    fclose(fp_write);
                    fp_write = NULL;
                }

                // TODO: need to recycle file space in cases of data update and delete

#ifdef ENABLE_TIMING
                struct timeval pdc_timer_start4, pdc_timer_end4;
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
                    printf("==PDC_SERVER[%d]: fopen failed [%s]\n", pdc_server_rank_g,
                           region_elt->storage_location);
                    ret_value = FAIL;
                    goto done;
                }
            } // End open file

            // Get the current write offset
            offset = ftell(fp_write);

#ifdef ENABLE_TIMING
            struct timeval pdc_timer_start5, pdc_timer_end5;
            gettimeofday(&pdc_timer_start5, 0);
#endif

            // Actual write (append)
            write_bytes = fwrite(region_elt->buf, 1, region_elt->data_size, fp_write);
            if (write_bytes != region_elt->data_size) {
                printf("==PDC_SERVER[%d]: fwrite to [%s] FAILED, region off %" PRIu64 ", size %" PRIu64 ", "
                       "actual writeen %zu!\n",
                       pdc_server_rank_g, region_elt->storage_location, offset, region_elt->data_size,
                       write_bytes);
                ret_value = FAIL;
                goto done;
            }
            n_fwrite_g++;
            fwrite_total_MB += write_bytes / 1048576.0;

#ifdef ENABLE_TIMING
            gettimeofday(&pdc_timer_end5, 0);
            double region_write_time = PDC_get_elapsed_time_double(&pdc_timer_start5, &pdc_timer_end5);
            server_write_time_g += region_write_time;
            if (is_debug_g == 1) {
                printf("==PDC_SERVER[%d]: fwrite %" PRIu64 " bytes, %.2fs\n", pdc_server_rank_g, write_bytes,
                       region_write_time);
            }
#endif

            // Generate histogram
            if (gen_hist_g == 1) {
                uint64_t nelem = region_elt->data_size / PDC_get_var_type_size(region_elt->meta->data_type);
                region_elt->region_hist = PDC_gen_hist(region_elt->meta->data_type, nelem, region_elt->buf);
            }

            if (is_debug_g == 1) {
                printf("Write data offset: %" PRIu64 ", size %" PRIu64 ", to [%s]\n", offset,
                       region_elt->data_size, region_elt->storage_location);
            }
            region_elt->is_data_ready = 1;
            region_elt->offset        = offset;

            ret_value = PDC_Server_update_region_storagelocation_offset(region_elt, PDC_UPDATE_STORAGE);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: failed to update region storage info!\n", pdc_server_rank_g);
                goto done;
            }
            previous_region = region_elt;

            region_elt->is_io_done = 1;

        } // end of WRITE
        else {
            printf("==PDC_SERVER[%d]: %s- unsupported access type\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
        }
    } // end DL_FOREACH region IO request (region)

done:
    if (fp_write != NULL) {
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
}

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
static perr_t
PDC_Server_data_io_direct(pdc_access_t io_type, uint64_t obj_id, struct pdc_region_info *region_info,
                          void *buf)
{
    perr_t         ret_value = SUCCEED;
    region_list_t *io_region = NULL;
#ifdef ENABLE_LUSTRE
    int stripe_count, stripe_size;
#endif
    FUNC_ENTER(NULL);

    is_server_direct_io_g = 1;
    io_region             = (region_list_t *)calloc(1, sizeof(region_list_t));
    PDC_init_region_list(io_region);
    PDC_region_info_to_list_t(region_info, io_region);

    // Generate a location for data storage for data server to write
    char *data_path                = NULL;
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
    PDC_mkdir(io_region->storage_location);

#ifdef ENABLE_LUSTRE
    stripe_count = 248 / pdc_server_size_g;
    stripe_size  = lustre_stripe_size_mb_g; // MB
    PDC_Server_set_lustre_stripe(io_region->storage_location, stripe_count, stripe_size);

    if (is_debug_g == 1 && pdc_server_rank_g == 0) {
        printf("storage_location is %s\n", io_region->storage_location);
        /* printf("lustre is enabled\n"); */
    }
#endif

    io_region->access_type = io_type;
    io_region->data_size   = PDC_get_region_size(io_region);
    io_region->buf         = buf;

    // Need to get the metadata
    ret_value = PDC_Server_get_metadata_by_id_with_cb(obj_id, PDC_Server_regions_io, io_region);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Server_posix_write(int fd, void *buf, uint64_t write_size)
{
    // Write 1GB at a time
    uint64_t write_bytes = 0, max_write_size = 1073741824;
    perr_t   ret_value = SUCCEED;
    ssize_t  ret;

    FUNC_ENTER(NULL);

    while (write_size > max_write_size) {
        ret = write(fd, buf, max_write_size);
        if (ret < 0 || ret != (ssize_t)max_write_size) {
            printf("==PDC_SERVER[%d]: write %d failed\n", pdc_server_rank_g, fd);
            ret_value = FAIL;
            goto done;
        }
        write_bytes += ret;
        buf += max_write_size;
        write_size -= max_write_size;
    }
    ret = write(fd, buf, write_size);
    if (ret < 0 || ret != (ssize_t)write_size) {
        printf("==PDC_SERVER[%d]: write %d failed\n", pdc_server_rank_g, fd);
        ret_value = FAIL;
        goto done;
    }
    write_bytes += ret;

    if (write_bytes != write_size) {
        printf("==PDC_SERVER[%d]: write %d failed, not all data written %lu/%lu\n", pdc_server_rank_g, fd,
               write_bytes, write_size);
        ret_value = FAIL;
    }

done:
    FUNC_LEAVE(ret_value);
}
#ifdef PDC_SERVER_CACHE

/*
 * Check if the first region is contained inside the second region or the second region is contained inside
 * the first region or they have overlapping relation.
 */
int
PDC_check_region_relation(uint64_t *offset, uint64_t *size, uint64_t *offset2, uint64_t *size2, int ndim)
{
    int i;
    int flag;
    flag = 1;
    for (i = 0; i < ndim; ++i) {
        if (offset2[i] > offset[i] || offset2[i] + size2[i] < offset[i] + size[i]) {
            flag = 0;
        }
    }
    if (flag) {
        return PDC_REGION_CONTAINED;
    }
    for (i = 0; i < ndim; ++i) {
        if (offset[i] > offset2[i] || offset[i] + size[i] < offset2[i] + size2[i]) {
            flag = 0;
        }
    }
    if (flag) {
        return PDC_REGION_CONTAINED_BY;
    }
    flag = 1;
    for (i = 0; i < ndim; ++i) {
        if (offset2[i] + size2[i] <= offset[i] || offset2[i] >= offset[i] + size[i]) {
            flag = 0;
        }
    }
    if (flag) {
        return PDC_REGION_PARTIAL_OVERLAP;
    }
    else {
        return PDC_REGION_NO_OVERLAP;
    }
    return 0;
}

/*
 * This function assumes two regions have overlaping relations. We create a new region for the overlaping
 * part.
 */
int
extract_overlaping_region(const uint64_t *offset, const uint64_t *size, const uint64_t *offset2,
                          const uint64_t *size2, int ndim, uint64_t **offset_merged, uint64_t **size_merged)
{
    int i;
    *offset_merged = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    *size_merged   = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    for (i = 0; i < ndim; ++i) {
        if (offset2[i] > offset[i]) {
            offset_merged[0][i] = offset2[i];
            size_merged[0][i]   = offset[i] + size[i] - offset2[i];
        }
        else {
            offset_merged[0][i] = offset[i];
            size_merged[0][i]   = offset2[i] + size2[i] - offset[i];
        }
    }
    return 0;
}

/*
 * A function used by PDC_region_merge.
 * This function copies contiguous memory buffer from buf and buf2 to buf_merged using region views.
 */
static int
pdc_region_merge_buf_copy(const uint64_t *offset, const uint64_t *size, const uint64_t *offset2,
                          const uint64_t *size2, const char *buf, const char *buf2, char **buf_merged,
                          int unit, int connect_flag)
{
    uint64_t overlaps;
    int      i;
    for (i = 0; i < connect_flag; ++i) {
        unit *= size[i];
    }

    if (offset[connect_flag] < offset2[connect_flag]) {
        if (offset[connect_flag] + size[connect_flag] > offset2[connect_flag] + size2[connect_flag]) {
            memcpy(*buf_merged, buf, unit * (offset2[connect_flag] - offset[connect_flag]));
            *buf_merged += unit * (offset2[connect_flag] - offset[connect_flag]);
            memcpy(*buf_merged, buf2, unit * size2[connect_flag]);
            *buf_merged += unit * size2[connect_flag];
            memcpy(*buf_merged, buf2,
                   unit * (offset[connect_flag] + size[connect_flag] -
                           (offset2[connect_flag] + size2[connect_flag])));
            *buf_merged += unit * (offset[connect_flag] + size[connect_flag] -
                                   (offset2[connect_flag] + size2[connect_flag]));
        }
        else {
            memcpy(*buf_merged, buf, unit * (offset2[connect_flag] - offset[connect_flag]));
            *buf_merged += unit * (offset2[connect_flag] - offset[connect_flag]);
            memcpy(*buf_merged, buf2, unit * size2[connect_flag]);
            *buf_merged += unit * size2[connect_flag];
        }
    }
    else {
        if (offset[connect_flag] + size[connect_flag] > offset2[connect_flag] + size2[connect_flag]) {
            memcpy(*buf_merged, buf2, unit * size2[connect_flag]);
            *buf_merged += unit * size2[connect_flag];
            overlaps = offset2[connect_flag] + size2[connect_flag] - offset[connect_flag];
            memcpy(*buf_merged, buf + overlaps * unit, unit * (size[connect_flag] - overlaps));
            *buf_merged += unit * (size[connect_flag] - overlaps);
        }
        else {
            memcpy(*buf_merged, buf2, unit * size2[connect_flag]);
            *buf_merged += unit * size2[connect_flag];
        }
    }
    return 0;
}
/*
 * This function merges two regions. The two regions must have the same offset/size in all dimensions but one.
 * The dimension that is not the same must not have gaps between the two regions.
 */
int
PDC_region_merge(const char *buf, const char *buf2, const uint64_t *offset, const uint64_t *size,
                 const uint64_t *offset2, const uint64_t *size2, char **buf_merged_ptr,
                 uint64_t **offset_merged, uint64_t **size_merged, int ndim, int unit)
{
    int      connect_flag, i, j;
    uint64_t tmp_buf_size;
    char *   buf_merged;
    connect_flag = -1;
    // Detect if two regions are connected. This means one dimension is fully connected and all other
    // dimensions are identical.
    for (i = 0; i < ndim; ++i) {
        // A dimension is detached, immediately return with failure
        if (offset[i] > offset2[i] + size2[i] || offset2[i] > offset[i] + size[i]) {
            return PDC_MERGE_FAILED;
        }
        // If we pass the previous condition, this dimension is connected between these two regions.
        if (offset[i] != offset2[i] || size[i] != size2[i]) {
            // Passing the above condition means this dimension can the connecting but not fully cotained
            // relation.
            if (connect_flag == -1) {
                // Set connect_flag to be the current dimension (non longer -1)
                connect_flag = i;
            }
            else {
                // We have seen such a dimension before, immediately return with failure.
                return PDC_MERGE_FAILED;
            }
        }
    }
    // If connect_flag is not set, we set it to the last dimension by default.
    if (connect_flag == -1) {
        connect_flag = ndim - 1;
    }
    // If we reach here, then the two regions can be merged into one.
    *offset_merged = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    *size_merged   = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    for (i = 0; i < ndim; ++i) {
        if (i != connect_flag) {
            offset_merged[0][i] = offset[i];
            size_merged[0][i]   = size[i];
        }
    }

    // Workout the final offset of the region in the connecting dimension
    if (offset[connect_flag] > offset2[connect_flag]) {
        offset_merged[0][connect_flag] = offset2[connect_flag];
    }
    else {
        offset_merged[0][connect_flag] = offset[connect_flag];
    }
    // Workout the final length of the region in the connecting dimension
    if (offset[connect_flag] + size[connect_flag] > offset2[connect_flag] + size2[connect_flag]) {
        size_merged[0][connect_flag] =
            offset[connect_flag] + size[connect_flag] - offset_merged[0][connect_flag];
    }
    else {
        size_merged[0][connect_flag] =
            offset2[connect_flag] + size2[connect_flag] - offset_merged[0][connect_flag];
    }
    // Start merging memory buffers. The second region will overwrite the first region data if there are
    // overlaps. This implementation will split three different dimensions in multiple branches. It would be
    // more elegant to write the code in terms of recursion, but I do not want to introduce unnecessary bugs
    // as we are only dealing with a few cases here.
    tmp_buf_size = size_merged[0][0];
    for (i = 1; i < ndim; ++i) {
        tmp_buf_size *= size_merged[0][i];
    }
    buf_merged      = (char *)malloc(sizeof(char) * tmp_buf_size);
    *buf_merged_ptr = buf_merged;
    if (ndim == 1) {
        pdc_region_merge_buf_copy(offset, size, offset2, size2, buf, buf2, &buf_merged, unit, connect_flag);
    }
    else if (ndim == 2) {
        if (connect_flag == 0) {
            // Note size[1] must equal to size2[1] after the previous checking.
            for (i = 0; i < (int)size[1]; ++i) {
                pdc_region_merge_buf_copy(offset, size, offset2, size2, buf, buf2, &buf_merged, unit,
                                          connect_flag);
            }
        }
        else {
            pdc_region_merge_buf_copy(offset, size, offset2, size2, buf, buf2, &buf_merged, unit,
                                      connect_flag);
        }
    }
    else if (ndim == 3) {
        if (connect_flag == 0) {
            // Note size[1] must equal to size2[1] after the previous checking, similarly for size[2] and
            // size2[2]
            for (i = 0; i < (int)size[2]; ++i) {
                for (j = 0; j < (int)size[1]; ++j) {
                    pdc_region_merge_buf_copy(offset, size, offset2, size2, buf, buf2, &buf_merged, unit,
                                              connect_flag);
                }
            }
        }
        else if (connect_flag == 1) {
            // Note size[2] must equal to size2[2] after the previous checking.
            for (i = 0; i < (int)size[2]; ++i) {
                pdc_region_merge_buf_copy(offset, size, offset2, size2, buf, buf2, &buf_merged, unit,
                                          connect_flag);
            }
        }
        else if (connect_flag == 2) {
            pdc_region_merge_buf_copy(offset, size, offset2, size2, buf, buf2, &buf_merged, unit,
                                      connect_flag);
        }
    }
    return PDC_MERGE_SUCCESS;
}

/*
 * Copy data from one buffer to another defined by region views.
 * offset/length is the region associated with the buf. offset2/length2 is the region associated with the
 * buf2. The region defined by offset/length has to contain the region defined by offset2/length2. direction
 * defines whether copy to the cache region or copy from the cache region, 0 means from source buffer to
 * target buffer. 1 means the other way round.
 */
int
PDC_region_cache_copy(char *buf, char *buf2, const uint64_t *offset, const uint64_t *size,
                      const uint64_t *offset2, const uint64_t *size2, int ndim, size_t unit, int direction)
{
    char *    src, *dst;
    uint64_t  i, j;
    uint64_t *local_offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    memcpy(local_offset, offset2, sizeof(uint64_t) * ndim);
    /* Rescale I/O request to cache region offsets. */
    for (i = 0; i < (uint64_t)ndim; ++i) {
        local_offset[i] -= offset[i];
    }
    if (ndim == 1) {
        if (direction) {
            src = buf2;
            dst = buf + local_offset[0] * unit;
        }
        else {
            dst = buf2;
            src = buf + local_offset[0] * unit;
        }

        memcpy(dst, src, unit * size2[0]);
    }
    else if (ndim == 2) {
        for (i = 0; i < size2[0]; ++i) {
            if (direction) {
                src = buf2;
                dst = buf + (local_offset[1] + (local_offset[0] + i) * size[1]) * unit;
            }
            else {
                dst = buf2;
                src = buf + (local_offset[1] + (local_offset[0] + i) * size[1]) * unit;
            }
            memcpy(dst, src, unit * size2[1]);
            buf2 += size2[1] * unit;
        }
    }
    else if (ndim == 3) {
        for (i = 0; i < size2[0]; ++i) {
            for (j = 0; j < size2[1]; ++j) {
                if (direction) {
                    src = buf2;
                    dst = buf + ((local_offset[0] + i) * size[1] * size[2] + (local_offset[1] + j) * size[2] +
                                 local_offset[2]) *
                                    unit;
                }
                else {
                    dst = buf2;
                    src = buf + ((local_offset[0] + i) * size[1] * size[2] + (local_offset[1] + j) * size[2] +
                                 local_offset[2]) *
                                    unit;
                }
                memcpy(dst, src, unit * size2[2]);
                buf2 += size2[2] * unit;
            }
        }
    }
    free(local_offset);
    return 0;
}

/*
 * This function cache metadata and data for a region write operation.
 * We store 1 object per element in the end of an array. Per object, there is a array of regions. The new
 * region is appended to the end of the region array after object searching by ID. This will result linear
 * search complexity for subregion search.
 */

int
PDC_region_cache_register(uint64_t obj_id, const char *buf, size_t buf_size, const uint64_t *offset,
                          const uint64_t *size, int ndim, size_t unit)
{
    hg_thread_mutex_lock(&pdc_obj_cache_list_mutex);

    pdc_obj_cache *         obj_cache_iter, *obj_cache = NULL;
    struct pdc_region_info *region_cache_info;
    obj_cache_iter = obj_cache_list;
    while (obj_cache_iter != NULL) {
        if (obj_cache_iter->obj_id == obj_id) {
            obj_cache = obj_cache_iter;
            break;
        }
        obj_cache_iter = obj_cache_iter->next;
    }

    if (obj_cache == NULL) {
        if (obj_cache_list != NULL) {
            obj_cache_list_end->next = (pdc_obj_cache *)malloc(sizeof(pdc_obj_cache));
            obj_cache_list_end       = obj_cache_list_end->next;
            obj_cache_list_end->next = NULL;

            obj_cache_list_end->obj_id           = obj_id;
            obj_cache_list_end->region_cache     = NULL;
            obj_cache_list_end->region_cache_end = NULL;
        }
        else {
            obj_cache_list     = (pdc_obj_cache *)malloc(sizeof(pdc_obj_cache));
            obj_cache_list_end = obj_cache_list;

            obj_cache_list_end->obj_id           = obj_id;
            obj_cache_list_end->region_cache     = NULL;
            obj_cache_list_end->region_cache_end = NULL;
            obj_cache_list_end->next             = NULL;
        }
        obj_cache = obj_cache_list_end;
    }

    if (obj_cache->region_cache == NULL) {
        obj_cache->region_cache           = (pdc_region_cache *)malloc(sizeof(pdc_region_cache));
        obj_cache->region_cache_end       = obj_cache->region_cache;
        obj_cache->region_cache_end->next = NULL;
    }
    else {
        obj_cache->region_cache_end->next = (pdc_region_cache *)malloc(sizeof(pdc_region_cache));
        obj_cache->region_cache_end       = obj_cache->region_cache_end->next;
        obj_cache->region_cache_end->next = NULL;
    }

    /* printf("checkpoint region_obj_cache_size = %d\n", obj_cache->region_obj_cache_size); */
    obj_cache->region_cache_end->region_cache_info =
        (struct pdc_region_info *)malloc(sizeof(struct pdc_region_info));
    region_cache_info         = obj_cache->region_cache_end->region_cache_info;
    region_cache_info->ndim   = ndim;
    region_cache_info->offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region_cache_info->size   = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
    region_cache_info->buf    = (char *)malloc(sizeof(char) * buf_size);
    region_cache_info->unit   = unit;

    memcpy(region_cache_info->offset, offset, sizeof(uint64_t) * ndim);
    memcpy(region_cache_info->size, size, sizeof(uint64_t) * ndim);
    memcpy(region_cache_info->buf, buf, sizeof(char) * buf_size);
    // printf("created cache region at offset %llu, buf size %llu, unit = %ld, ndim = %ld, obj_id = %llu\n",
    //       offset[0], buf_size, unit, ndim, (long long unsigned)obj_cache->obj_id);

    gettimeofday(&(obj_cache->timestamp), NULL);

    hg_thread_mutex_unlock(&pdc_obj_cache_list_mutex);

    return 0;
}

int
PDC_region_cache_free()
{
    pdc_obj_cache *   obj_cache_iter, *obj_temp;
    pdc_region_cache *region_cache_iter, *region_temp;
    obj_cache_iter = obj_cache_list;
    while (obj_cache_iter != NULL) {
        region_cache_iter = obj_cache_iter->region_cache;
        while (region_cache_iter != NULL) {
            free(region_cache_iter->region_cache_info);
            region_temp       = region_cache_iter;
            region_cache_iter = region_cache_iter->next;
            free(region_temp);
        }
        obj_temp       = obj_cache_iter;
        obj_cache_iter = obj_cache_iter->next;
        free(obj_temp);
    }
    return 0;
}

perr_t
PDC_Server_data_write_out2(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit)
{
    perr_t                ret_value      = SUCCEED;
    data_server_region_t *region         = NULL;
    region_list_t *       overlap_region = NULL;
    int                   is_overlap     = 0;
    uint64_t              i, j, pos, overlap_start[DIM_MAX] = {0}, overlap_count[DIM_MAX] = {0},
                        overlap_start_local[DIM_MAX] = {0};

    FUNC_ENTER(NULL);

    uint64_t write_size;
    if (region_info->ndim >= 1)
        write_size = unit * region_info->size[0];
    if (region_info->ndim >= 2)
        write_size *= region_info->size[1];

    if (region_info->ndim >= 3)
        write_size *= region_info->size[2];

    region = PDC_Server_get_obj_region(obj_id);
    if (region == NULL) {
        printf("cannot locate file handle\n");
        goto done;
    }

    if ((region->fd <= 0) && region->storage_location) {
        region->fd = open(region->storage_location, O_RDWR, 0666);
    }

    region_list_t *request_region = (region_list_t *)calloc(1, sizeof(region_list_t));
    for (i = 0; i < region_info->ndim; i++) {
        request_region->start[i] = region_info->offset[i];
        request_region->count[i] = region_info->size[i];
    }
    request_region->ndim      = region_info->ndim;
    request_region->unit_size = unit;
    strcpy(request_region->storage_location, region->storage_location);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    double         write_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

    // Detect overwrite
    region_list_t *elt;
    DL_FOREACH(region->region_storage_head, elt)
    {
        if (PDC_is_contiguous_region_overlap(elt, request_region) == 1) {
            is_overlap++;
            overlap_region = elt;

            // Get the actual start and count of region in storage
            if (PDC_get_overlap_start_count(region_info->ndim, request_region->start, request_region->count,
                                            overlap_region->start, overlap_region->count, overlap_start,
                                            overlap_count) != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_get_overlap_start_count FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }

            // local (relative) region start
            for (i = 0; i < region_info->ndim; i++)
                overlap_start_local[i] = overlap_start[i] % overlap_region->count[i];

            if (region_info->ndim == 1) {
                // 1D can overwrite data in region directly
                pos = (overlap_start[0] - region_info->offset[0]) * unit;
                if (pos > write_size) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, write_size);
                    ret_value = -1;
                    goto done;
                }

                lseek(region->fd, overlap_region->offset + overlap_start_local[0] * unit, SEEK_SET);
                ret_value = PDC_Server_posix_write(region->fd, buf + pos, write_size);
                if (ret_value != SUCCEED) {
                    printf("==PDC_SERVER[%d]: PDC_Server_posix_write FAILED!\n", pdc_server_rank_g);
                    ret_value = FAIL;
                    goto done;
                }
                // No need to update metadata
            }
            else if (region_info->ndim == 2) {
                // 2D/3D: generally it's a good idea to read entire region, overwrite overlap part,
                // and write back to avoid fragmented writes.
                void *tmp_buf = malloc(overlap_region->data_size);

                if (pread(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }

                // Overlap start position
                pos = ((overlap_start[0] - region_info->offset[0]) * overlap_region->count[1] +
                       overlap_start[1] - region_info->offset[1]) *
                      unit;
                if (pos > overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, overlap_region->data_size);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    memcpy(tmp_buf + i * overlap_region->count[1] * unit + overlap_start_local[1] * unit,
                           buf + pos, overlap_count[1] * unit);
                    pos += region_info->size[1] * unit;
                    if (pos > overlap_region->data_size) {
                        printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                               pdc_server_rank_g, pos, overlap_region->data_size);
                        ret_value = -1;
                        goto done;
                    }
                }
                if (pwrite(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Failed to write enough bytes\n", pdc_server_rank_g);
                }
                free(tmp_buf);
                // No need to update metadata
            } // End 2D
            else if (region_info->ndim == 3) {
                void *tmp_buf = malloc(overlap_region->data_size);
                // Read entire region
                if (pread(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }

                pos = ((overlap_start[0] - region_info->offset[0]) * overlap_region->count[1] *
                           overlap_region->count[2] +
                       (overlap_start[1] - region_info->offset[1]) * overlap_region->count[2] +
                       (overlap_start[2] - region_info->offset[2])) *
                      unit;
                if (pos > overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, overlap_region->data_size);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    for (j = overlap_start_local[1]; j < overlap_start_local[1] + overlap_count[1]; j++) {
                        /* printf("i=%llu, j=%llu, pos=%llu, pos2=%llu, size=%llu, total size=%llu\n", i, j,
                         * pos, */
                        /*         i*overlap_region->count[2]*overlap_region->count[1]*unit
                         * +j*overlap_region->count[2]*unit+region_info->offset[2], */
                        /*         region_info->size[2]*unit, overlap_region->data_size); */
                        memcpy(tmp_buf + i * overlap_region->count[2] * overlap_region->count[1] * unit +
                                   j * overlap_region->count[2] * unit + overlap_start_local[2] * unit,
                               buf + pos, overlap_count[2] * unit);

                        pos += region_info->size[2] * unit;
                        if (pos > overlap_region->data_size) {
                            printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                                   pdc_server_rank_g, pos, overlap_region->data_size);
                            ret_value = -1;
                            goto done;
                        }
                    }
                }
                if (pwrite(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Failed to write enough bytes\n", pdc_server_rank_g);
                }
                free(tmp_buf);
                // No need to update metadata
            } // End 3D
        }     // End is overlap

    } // End DL_FOREACH storage region list

    if (is_overlap == 0) {
        request_region->offset = lseek(region->fd, 0, SEEK_END);

        ret_value = PDC_Server_posix_write(region->fd, buf, write_size);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_posix_write FAILED!\n", pdc_server_rank_g);
            ret_value = FAIL;
            goto done;
        }

        // Store storage information
        request_region->data_size = write_size;
        DL_APPEND(region->region_storage_head, request_region);
    }
    else
        free(request_region);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    write_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("==PDC_SERVER[%d]: write region time: %.4f, %llu bytes\n", pdc_server_rank_g, write_total_sec,
           write_size);
    fflush(stdout);
#endif

    /* printf("==PDC_SERVER[%d]: write region %llu bytes\n", pdc_server_rank_g, request_region->data_size); */
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_data_write_out(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit)
{
    int               flag;
    pdc_obj_cache *   obj_cache, *obj_cache_iter;
    pdc_region_cache *region_cache_iter;

    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    // Write 1GB at a time

    uint64_t write_size = 0;
    if (region_info->ndim >= 1)
        write_size = unit * region_info->size[0];
    if (region_info->ndim >= 2)
        write_size *= region_info->size[1];
    if (region_info->ndim >= 3)
        write_size *= region_info->size[2];

    obj_cache = NULL;
    // Look up for the object in the cache list
    obj_cache_iter = obj_cache_list;
    while (obj_cache_iter != NULL) {
        if (obj_cache_iter->obj_id == obj_id) {
            obj_cache = obj_cache_iter;
        }
        obj_cache_iter = obj_cache_iter->next;
    }
    flag = 1;
    if (obj_cache != NULL) {
        // If we have region that is contained inside a cached region, we can directly modify the cache region
        // data.
        region_cache_iter = obj_cache->region_cache;
        while (region_cache_iter != NULL) {
            if (PDC_check_region_relation(
                    region_info->offset, region_info->size, region_cache_iter->region_cache_info->offset,
                    region_cache_iter->region_cache_info->size,
                    region_cache_iter->region_cache_info->ndim) == PDC_REGION_CONTAINED) {
                PDC_region_cache_copy(region_cache_iter->region_cache_info->buf, buf,
                                      region_cache_iter->region_cache_info->offset,
                                      region_cache_iter->region_cache_info->size, region_info->offset,
                                      region_info->size, region_cache_iter->region_cache_info->ndim, unit, 1);
                flag = 0;
                break;
            }
            region_cache_iter = region_cache_iter->next;
        }
    }
    if (flag) {
        PDC_region_cache_register(obj_id, buf, write_size, region_info->offset, region_info->size,
                                  region_info->ndim, unit);
    }
    // PDC_Server_data_write_out2(obj_id, region_info, buf, unit);

    // done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_data_read_from2(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit)
{
    perr_t                       ret_value        = SUCCEED;
    ssize_t /*read_bytes = 0, */ total_read_bytes = 0, request_bytes = unit, my_read_bytes = 0;
    data_server_region_t *       region = NULL;
    region_list_t *              elt;
    // int flag = 0;
    uint64_t i, j, pos, overlap_start[DIM_MAX] = {0}, overlap_count[DIM_MAX] = {0},
                        overlap_start_local[DIM_MAX] = {0};

    FUNC_ENTER(NULL);

    region = PDC_Server_get_obj_region(obj_id);
    if (region == NULL) {
        printf("cannot locate file handle\n");
        goto done;
    }
    // Was opened previously and closed.
    // The location string is cached, so we utilize
    // that to reopen the file.
    if (region->fd <= 0) {
        region->fd = open(region->storage_location, O_RDWR, 0666);
    }

    region_list_t request_region;
    request_region.ndim = region_info->ndim;
    for (i = 0; i < region_info->ndim; i++) {
        request_region.start[i] = region_info->offset[i];
        request_region.count[i] = region_info->size[i];
        request_bytes *= region_info->size[i];
    }

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    double         read_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

    region_list_t *storage_region = NULL;
    DL_FOREACH(region->region_storage_head, elt)
    {
        // flag = 0;

        if (PDC_is_contiguous_region_overlap(elt, &request_region) == 1) {
            storage_region = elt;
            // flag = 1;

            // Get the actual start and count of region in storage
            if (PDC_get_overlap_start_count(region_info->ndim, request_region.start, request_region.count,
                                            elt->start, elt->count, overlap_start,
                                            overlap_count) != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_get_overlap_start_count FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }

            // local (relative) region start
            for (i = 0; i < region_info->ndim; i++)
                overlap_start_local[i] = overlap_start[i] % elt->count[i];

            if (region_info->ndim == 1) {
                pos = (overlap_start[0] - region_info->offset[0]) * unit;
                if (pos > (uint64_t)request_bytes) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, request_bytes);
                    ret_value = -1;
                    goto done;
                }
                // read_bytes = pread(region->fd, buf + pos, overlap_count[0] * unit,
                //                   storage_region->offset + overlap_start_local[0] * unit);
                if (pread(region->fd, buf + pos, overlap_count[0] * unit,
                          storage_region->offset + overlap_start_local[0] * unit) !=
                    (ssize_t)(overlap_count[0] * unit)) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }
                my_read_bytes = overlap_count[0] * unit;
                /* printf("storage offset %llu, region offset %llu, read %d bytes\n", storage_region->offset,

                 * overlap_count[0]*unit, read_bytes); */
            }
            else if (region_info->ndim == 2) {
                void *tmp_buf = malloc(storage_region->data_size);
                // Read entire region
                // read_bytes = pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset);
                if (pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset) !=
                    (ssize_t)storage_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }
                // Extract requested data
                pos = ((overlap_start[0] - region_info->offset[0]) * storage_region->count[1] +
                       overlap_start[1] - region_info->offset[1]) *
                      unit;
                if (pos > (uint64_t)request_bytes) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, request_bytes);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    memcpy(buf + pos,
                           tmp_buf + i * storage_region->count[1] * unit + overlap_start_local[1] * unit,
                           overlap_count[1] * unit);
                    pos += region_info->size[1] * unit;
                    if (pos > (uint64_t)request_bytes) {
                        printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                               pdc_server_rank_g, pos, request_bytes);
                        ret_value = -1;
                        goto done;
                    }
                    my_read_bytes += overlap_count[1] * unit;
                }
                free(tmp_buf);
            }
            else if (region_info->ndim == 3) {
                void *tmp_buf = malloc(storage_region->data_size);
                // Read entire region
                // read_bytes = pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset);
                if (pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset) !=
                    (ssize_t)storage_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }
                // Extract requested data
                pos = ((overlap_start[0] - region_info->offset[0]) * storage_region->count[1] *
                           storage_region->count[2] +
                       (overlap_start[1] - region_info->offset[1]) * storage_region->count[2] +
                       (overlap_start[2] - region_info->offset[2])) *
                      unit;
                if (pos > (uint64_t)request_bytes) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, request_bytes);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    for (j = overlap_start_local[1]; j < overlap_start_local[1] + overlap_count[1]; j++) {
                        /* printf("i=%llu, j=%llu, pos=%llu, pos2=%llu, size=%llu, total size=%llu\n", i, j,
                         * pos, */
                        /*         i*storage_region->count[2]*storage_region->count[1]*unit

                         * +j*storage_region->count[2]*unit+region_info->offset[2], */
                        /*         region_info->size[2]*unit, storage_region->data_size); */
                        memcpy(buf + pos,
                               tmp_buf + i * storage_region->count[2] * storage_region->count[1] * unit +
                                   j * storage_region->count[2] * unit + overlap_start_local[2] * unit,
                               overlap_count[2] * unit);
                        pos += region_info->size[2] * unit;
                        if (pos > (uint64_t)request_bytes) {
                            printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                                   pdc_server_rank_g, pos, request_bytes);
                            ret_value = -1;
                            goto done;
                        }
                        my_read_bytes += overlap_count[2] * unit;
                    }
                }
                free(tmp_buf);
            }
            /*

                        if (read_bytes == -1) {

                            char errmsg[256];
                            // printf("==PDC_SERVER[%d]: pread %d failed (%d)\n", pdc_server_rank_g,
               region->fd,
                            // strerror_r(errno, errmsg, sizeof(errmsg)));
                            goto done;

                        }
            */
            total_read_bytes += my_read_bytes;

        } // End is overlap

        if (total_read_bytes >= request_bytes)
            break;
    } // End DL_FOREACH storage region list

    if (total_read_bytes < request_bytes) {
        printf("==PDC_SERVER[%d]: read less bytes than expected %lu / %ld\n", pdc_server_rank_g,
               total_read_bytes, request_bytes);
        ret_value = -1;
    }
#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    read_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("==PDC_SERVER[%d]: read region time: %.4f, %llu bytes\n", pdc_server_rank_g, read_total_sec,
           read_bytes);
    fflush(stdout);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

int
PDC_region_cache_flush(uint64_t obj_id)
{
    pdc_obj_cache *         obj_cache = NULL, *obj_cache_iter;
    pdc_region_cache *      region_cache_iter, *region_cache_temp;
    struct pdc_region_info *region_cache_info;

    obj_cache_iter = obj_cache_list;
    while (obj_cache_iter != NULL) {
        if (obj_cache_iter->obj_id == obj_id) {
            obj_cache = obj_cache_iter;
            break;
        }
        obj_cache_iter = obj_cache_iter->next;
    }
    if (obj_cache == NULL) {
        printf("server error: flushing object that does not exist\n");
    }
    region_cache_iter = obj_cache->region_cache;
    while (region_cache_iter != NULL) {
        region_cache_info = region_cache_iter->region_cache_info;
        PDC_Server_data_write_out2(obj_id, region_cache_info, region_cache_info->buf,
                                   region_cache_info->unit);
        free(region_cache_info->offset);
        free(region_cache_info->size);
        free(region_cache_info->buf);
        free(region_cache_info);
        region_cache_temp = region_cache_iter;
        region_cache_iter = region_cache_iter->next;
        free(region_cache_temp);
    }
    obj_cache->region_cache = NULL;
    gettimeofday(&(obj_cache->timestamp), NULL);
    return 0;
}

int
PDC_region_cache_flush_all()
{
    pdc_obj_cache *obj_cache_iter, *obj_cache_temp;
    hg_thread_mutex_lock(&pdc_obj_cache_list_mutex);

    obj_cache_iter = obj_cache_list;
    while (obj_cache_iter != NULL) {
        PDC_region_cache_flush(obj_cache_iter->obj_id);
        obj_cache_temp = obj_cache_iter;
        obj_cache_iter = obj_cache_iter->next;
        free(obj_cache_temp);
    }
    hg_thread_mutex_unlock(&pdc_obj_cache_list_mutex);
    return 0;
}

void *
PDC_region_cache_clock_cycle(void *ptr)
{
    pdc_obj_cache *obj_cache, *obj_cache_iter;
    struct timeval current_time;
    if (ptr == NULL) {
        obj_cache_iter = NULL;
    }
    while (1) {
        pthread_mutex_lock(&pdc_cache_mutex);
        if (!pdc_recycle_close_flag) {
            hg_thread_mutex_lock(&pdc_obj_cache_list_mutex);
            gettimeofday(&current_time, NULL);
            obj_cache_iter = obj_cache_list;
            while (obj_cache_iter != NULL) {
                obj_cache = obj_cache_iter;
                if (current_time.tv_sec - obj_cache->timestamp.tv_sec > 10) {
                    PDC_region_cache_flush(obj_cache->obj_id);
                }
                obj_cache_iter = obj_cache_iter->next;
            }
            hg_thread_mutex_unlock(&pdc_obj_cache_list_mutex);
        }
        else {
            pthread_mutex_unlock(&pdc_cache_mutex);
            break;
        }
        pthread_mutex_unlock(&pdc_cache_mutex);
        sleep(0.75);
    }
    return 0;
}

perr_t
PDC_Server_data_read_from(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);
    // PDC_Server_data_read_from2(obj_id, region_info, buf, unit);

    PDC_region_fetch(obj_id, region_info, buf, unit);
    // done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * This function search for an object cache by ID, then copy data from the region to buf if the request region
 * is fully contained inside the cache region.
 */
int
PDC_region_fetch(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit)
{
    hg_thread_mutex_lock(&pdc_obj_cache_list_mutex);
    pdc_obj_cache *         obj_cache = NULL, *obj_cache_iter;
    int                     flag      = 1;
    size_t                  j;
    pdc_region_cache *      region_cache_iter;
    struct pdc_region_info *region_cache_info = NULL;
    obj_cache_iter                            = obj_cache_list;
    while (obj_cache_iter != NULL) {
        if (obj_cache_iter->obj_id == obj_id) {
            obj_cache = obj_cache_iter;
        }
        obj_cache_iter = obj_cache_iter->next;
    }
    if (obj_cache != NULL) {
        // printf("region fetch for obj id %llu\n", obj_cache->obj_id);

        // Check if the input region is contained inside any cache region.
        region_cache_iter = obj_cache->region_cache;
        while (region_cache_iter != NULL) {
            flag              = 1;
            region_cache_info = region_cache_iter->region_cache_info;
            for (j = 0; j < region_info->ndim; ++j) {
                if (region_info->offset[j] < region_cache_info->offset[j] ||
                    region_info->offset[j] + region_info->size[j] >
                        region_cache_info->offset[j] + region_cache_info->size[j]) {
                    flag = 0;
                }
            }
            region_cache_iter = region_cache_iter->next;
            if (flag) {
                break;
            }
            else {
                region_cache_info = NULL;
            }
        }
        if (region_cache_info != NULL && unit == region_cache_info->unit) {
            PDC_region_cache_copy(region_cache_info->buf, buf, region_cache_info->offset,
                                  region_cache_info->size, region_info->offset, region_info->size,
                                  region_cache_info->ndim, unit, 0);
        }
        else {
            region_cache_info = NULL;
        }
    }
    if (region_cache_info == NULL) {
        PDC_region_cache_flush(obj_id);
        PDC_Server_data_read_from2(obj_id, region_info, buf, unit);
    }
    hg_thread_mutex_unlock(&pdc_obj_cache_list_mutex);
    return 0;
}

#else
// No PDC_SERVER_CACHE
perr_t
PDC_Server_data_write_out(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit)
{
    perr_t ret_value = SUCCEED;
    data_server_region_t *region = NULL;
    region_list_t *overlap_region = NULL;
    int is_overlap = 0;
    uint64_t i, j, pos, overlap_start[DIM_MAX] = {0}, overlap_count[DIM_MAX] = {0},
                        overlap_start_local[DIM_MAX] = {0};

    FUNC_ENTER(NULL);

    uint64_t write_size;
    if (region_info->ndim >= 1)
        write_size = unit * region_info->size[0];
    if (region_info->ndim >= 2)
        write_size *= region_info->size[1];

    if (region_info->ndim >= 3)
        write_size *= region_info->size[2];

    region = PDC_Server_get_obj_region(obj_id);
    if (region == NULL) {
        printf("cannot locate file handle\n");
        goto done;
    }

    if ((region->fd <= 0) && region->storage_location) {
        region->fd = open(region->storage_location, O_RDWR, 0666);
    }

    region_list_t *request_region = (region_list_t *)calloc(1, sizeof(region_list_t));
    for (i = 0; i < region_info->ndim; i++) {
        request_region->start[i] = region_info->offset[i];
        request_region->count[i] = region_info->size[i];
    }
    request_region->ndim = region_info->ndim;
    request_region->unit_size = unit;
    strcpy(request_region->storage_location, region->storage_location);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    double write_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

    // Detect overwrite
    region_list_t *elt;
    DL_FOREACH(region->region_storage_head, elt)
    {
        if (PDC_is_contiguous_region_overlap(elt, request_region) == 1) {
            is_overlap++;
            overlap_region = elt;

            // Get the actual start and count of region in storage
            if (PDC_get_overlap_start_count(region_info->ndim, request_region->start, request_region->count,
                                            overlap_region->start, overlap_region->count, overlap_start,
                                            overlap_count) != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_get_overlap_start_count FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }

            // local (relative) region start
            for (i = 0; i < region_info->ndim; i++)
                overlap_start_local[i] = overlap_start[i] % overlap_region->count[i];

            if (region_info->ndim == 1) {
                // 1D can overwrite data in region directly
                pos = (overlap_start[0] - region_info->offset[0]) * unit;
                if (pos > write_size) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, write_size);
                    ret_value = -1;
                    goto done;
                }

                lseek(region->fd, overlap_region->offset + overlap_start_local[0] * unit, SEEK_SET);
                ret_value = PDC_Server_posix_write(region->fd, buf + pos, write_size);
                if (ret_value != SUCCEED) {
                    printf("==PDC_SERVER[%d]: PDC_Server_posix_write FAILED!\n", pdc_server_rank_g);
                    ret_value = FAIL;
                    goto done;
                }
                // No need to update metadata
            }
            else if (region_info->ndim == 2) {
                // 2D/3D: generally it's a good idea to read entire region, overwrite overlap part,
                // and write back to avoid fragmented writes.
                void *tmp_buf = malloc(overlap_region->data_size);

                if (pread(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }

                // Overlap start position
                pos = ((overlap_start[0] - region_info->offset[0]) * overlap_region->count[1] +
                       overlap_start[1] - region_info->offset[1]) *
                      unit;
                if (pos > overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, overlap_region->data_size);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    memcpy(tmp_buf + i * overlap_region->count[1] * unit + overlap_start_local[1] * unit,
                           buf + pos, overlap_count[1] * unit);
                    pos += region_info->size[1] * unit;
                    if (pos > overlap_region->data_size) {
                        printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                               pdc_server_rank_g, pos, overlap_region->data_size);
                        ret_value = -1;
                        goto done;
                    }
                }
                if (pwrite(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Failed to write enough bytes\n", pdc_server_rank_g);
                }
                free(tmp_buf);
                // No need to update metadata
            } // End 2D
            else if (region_info->ndim == 3) {
                void *tmp_buf = malloc(overlap_region->data_size);
                // Read entire region
                if (pread(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }

                pos = ((overlap_start[0] - region_info->offset[0]) * overlap_region->count[1] *
                           overlap_region->count[2] +
                       (overlap_start[1] - region_info->offset[1]) * overlap_region->count[2] +
                       (overlap_start[2] - region_info->offset[2])) *
                      unit;
                if (pos > overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, overlap_region->data_size);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    for (j = overlap_start_local[1]; j < overlap_start_local[1] + overlap_count[1]; j++) {
                        /* printf("i=%llu, j=%llu, pos=%llu, pos2=%llu, size=%llu, total size=%llu\n", i, j,
                         * pos, */
                        /*         i*overlap_region->count[2]*overlap_region->count[1]*unit
                         * +j*overlap_region->count[2]*unit+region_info->offset[2], */
                        /*         region_info->size[2]*unit, overlap_region->data_size); */
                        memcpy(tmp_buf + i * overlap_region->count[2] * overlap_region->count[1] * unit +
                                   j * overlap_region->count[2] * unit + overlap_start_local[2] * unit,
                               buf + pos, overlap_count[2] * unit);

                        pos += region_info->size[2] * unit;
                        if (pos > overlap_region->data_size) {
                            printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                                   pdc_server_rank_g, pos, overlap_region->data_size);
                            ret_value = -1;
                            goto done;
                        }
                    }
                }
                if (pwrite(region->fd, tmp_buf, overlap_region->data_size, overlap_region->offset) !=
                    (ssize_t)overlap_region->data_size) {
                    printf("==PDC_SERVER[%d]: Failed to write enough bytes\n", pdc_server_rank_g);
                }
                free(tmp_buf);
                // No need to update metadata
            } // End 3D
        }     // End is overlap

    } // End DL_FOREACH storage region list

    if (is_overlap == 0) {
        request_region->offset = lseek(region->fd, 0, SEEK_END);

        ret_value = PDC_Server_posix_write(region->fd, buf, write_size);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: PDC_Server_posix_write FAILED!\n", pdc_server_rank_g);
            ret_value = FAIL;
            goto done;
        }

        // Store storage information
        request_region->data_size = write_size;
        DL_APPEND(region->region_storage_head, request_region);
    }
    else
        free(request_region);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    write_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("==PDC_SERVER[%d]: write region time: %.4f, %llu bytes\n", pdc_server_rank_g, write_total_sec,
           write_size);
    fflush(stdout);
#endif

    /* printf("==PDC_SERVER[%d]: write region %llu bytes\n", pdc_server_rank_g, request_region->data_size); */
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
} // End PDC_Server_data_write_out

// No PDC_SERVER_CACHE
perr_t
PDC_Server_data_read_from(uint64_t obj_id, struct pdc_region_info *region_info, void *buf, size_t unit)
{
    perr_t ret_value = SUCCEED;
    ssize_t /*read_bytes = 0, */ total_read_bytes = 0, request_bytes = unit, my_read_bytes = 0;
    data_server_region_t *region = NULL;
    region_list_t *elt;
    // int flag = 0;
    uint64_t i, j, pos, overlap_start[DIM_MAX] = {0}, overlap_count[DIM_MAX] = {0},
                        overlap_start_local[DIM_MAX] = {0};

    FUNC_ENTER(NULL);

    region = PDC_Server_get_obj_region(obj_id);
    if (region == NULL) {
        printf("cannot locate file handle\n");
        goto done;
    }
    // Was opened previously and closed.
    // The location string is cached, so we utilize
    // that to reopen the file.
    if (region->fd <= 0) {
        region->fd = open(region->storage_location, O_RDWR, 0666);
    }

    region_list_t request_region;
    request_region.ndim = region_info->ndim;
    for (i = 0; i < region_info->ndim; i++) {
        request_region.start[i] = region_info->offset[i];
        request_region.count[i] = region_info->size[i];
        request_bytes *= region_info->size[i];
    }

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    double read_total_sec;
    gettimeofday(&pdc_timer_start, 0);
#endif

    region_list_t *storage_region = NULL;
    DL_FOREACH(region->region_storage_head, elt)
    {
        // flag = 0;

        if (PDC_is_contiguous_region_overlap(elt, &request_region) == 1) {
            storage_region = elt;
            // flag = 1;

            // Get the actual start and count of region in storage
            if (PDC_get_overlap_start_count(region_info->ndim, request_region.start, request_region.count,
                                            elt->start, elt->count, overlap_start,
                                            overlap_count) != SUCCEED) {
                printf("==PDC_SERVER[%d]: PDC_get_overlap_start_count FAILED!\n", pdc_server_rank_g);
                ret_value = FAIL;
                goto done;
            }

            // local (relative) region start
            for (i = 0; i < region_info->ndim; i++)
                overlap_start_local[i] = overlap_start[i] % elt->count[i];

            if (region_info->ndim == 1) {
                pos = (overlap_start[0] - region_info->offset[0]) * unit;
                if (pos > (uint64_t)request_bytes) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, request_bytes);
                    ret_value = -1;
                    goto done;
                }
                // read_bytes = pread(region->fd, buf + pos, overlap_count[0] * unit,
                //                   storage_region->offset + overlap_start_local[0] * unit);
                if (pread(region->fd, buf + pos, overlap_count[0] * unit,
                          storage_region->offset + overlap_start_local[0] * unit) !=
                    (ssize_t)(overlap_count[0] * unit)) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }
                my_read_bytes = overlap_count[0] * unit;
                /* printf("storage offset %llu, region offset %llu, read %d bytes\n", storage_region->offset,

                 * overlap_count[0]*unit, read_bytes); */
            }
            else if (region_info->ndim == 2) {
                void *tmp_buf = malloc(storage_region->data_size);
                // Read entire region
                // read_bytes = pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset);
                if (pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset) !=
                    (ssize_t)storage_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }
                // Extract requested data
                pos = ((overlap_start[0] - region_info->offset[0]) * storage_region->count[1] +
                       overlap_start[1] - region_info->offset[1]) *
                      unit;
                if (pos > (uint64_t)request_bytes) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, request_bytes);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    memcpy(buf + pos,
                           tmp_buf + i * storage_region->count[1] * unit + overlap_start_local[1] * unit,
                           overlap_count[1] * unit);
                    pos += region_info->size[1] * unit;
                    if (pos > (uint64_t)request_bytes) {
                        printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                               pdc_server_rank_g, pos, request_bytes);
                        ret_value = -1;
                        goto done;
                    }
                    my_read_bytes += overlap_count[1] * unit;
                }
                free(tmp_buf);
            }
            else if (region_info->ndim == 3) {
                void *tmp_buf = malloc(storage_region->data_size);
                // Read entire region
                // read_bytes = pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset);
                if (pread(region->fd, tmp_buf, storage_region->data_size, storage_region->offset) !=
                    (ssize_t)storage_region->data_size) {
                    printf("==PDC_SERVER[%d]: pread failed to read enough bytes\n", pdc_server_rank_g);
                }
                // Extract requested data
                pos = ((overlap_start[0] - region_info->offset[0]) * storage_region->count[1] *
                           storage_region->count[2] +
                       (overlap_start[1] - region_info->offset[1]) * storage_region->count[2] +
                       (overlap_start[2] - region_info->offset[2])) *
                      unit;
                if (pos > (uint64_t)request_bytes) {
                    printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n", pdc_server_rank_g,
                           pos, request_bytes);
                    ret_value = -1;
                    goto done;
                }

                for (i = overlap_start_local[0]; i < overlap_start_local[0] + overlap_count[0]; i++) {
                    for (j = overlap_start_local[1]; j < overlap_start_local[1] + overlap_count[1]; j++) {
                        /* printf("i=%llu, j=%llu, pos=%llu, pos2=%llu, size=%llu, total size=%llu\n", i, j,

                         * pos, */
                        /*         i*storage_region->count[2]*storage_region->count[1]*unit

                         * +j*storage_region->count[2]*unit+region_info->offset[2], */
                        /*         region_info->size[2]*unit, storage_region->data_size); */
                        memcpy(buf + pos,
                               tmp_buf + i * storage_region->count[2] * storage_region->count[1] * unit +
                                   j * storage_region->count[2] * unit + overlap_start_local[2] * unit,
                               overlap_count[2] * unit);
                        pos += region_info->size[2] * unit;
                        if (pos > (uint64_t)request_bytes) {
                            printf("==PDC_SERVER[%d]: Error with buf pos calculation %lu / %ld!\n",
                                   pdc_server_rank_g, pos, request_bytes);
                            ret_value = -1;
                            goto done;
                        }
                        my_read_bytes += overlap_count[2] * unit;
                    }
                }
                free(tmp_buf);
            }
            /*

                        if (read_bytes == -1) {

                            char errmsg[256];
                            // printf("==PDC_SERVER[%d]: pread %d failed (%d)\n", pdc_server_rank_g,
               region->fd,
                            // strerror_r(errno, errmsg, sizeof(errmsg)));
                            goto done;

                        }
            */
            total_read_bytes += my_read_bytes;

        } // End is overlap

        if (total_read_bytes >= request_bytes)
            break;
    } // End DL_FOREACH storage region list

    if (total_read_bytes < request_bytes) {
        printf("==PDC_SERVER[%d]: read less bytes than expected %lu / %ld\n", pdc_server_rank_g,
               total_read_bytes, request_bytes);
        ret_value = -1;
    }
#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    read_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("==PDC_SERVER[%d]: read region time: %.4f, %llu bytes\n", pdc_server_rank_g, read_total_sec,
           read_bytes);
    fflush(stdout);
#endif

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
#endif // End else PDC_SERVER_CACHE

perr_t
PDC_Server_data_write_direct(uint64_t obj_id, struct pdc_region_info *region_info, void *buf)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    ret_value = PDC_Server_data_io_direct(PDC_WRITE, obj_id, region_info, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_data_write_direct() "
               "error with PDC_Server_data_io_direct()\n",
               pdc_server_rank_g);
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_data_read_direct(uint64_t obj_id, struct pdc_region_info *region_info, void *buf)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    ret_value = PDC_Server_data_io_direct(PDC_READ, obj_id, region_info, buf);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: PDC_Server_data_read_direct() "
               "error with PDC_Server_data_io_direct()\n",
               pdc_server_rank_g);
        goto done;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t

PDC_Server_get_local_storage_meta_with_one_name(storage_meta_query_one_name_args_t *args)
{
    perr_t          ret_value  = SUCCEED;
    pdc_metadata_t *meta       = NULL;
    region_list_t * region_elt = NULL, *region_head = NULL, *res_region_list = NULL;
    int             region_count = 0, i = 0;

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
    args->n_res     = region_count;
    res_region_list = (region_list_t *)calloc(region_count, sizeof(region_list_t));

    // Copy location and offset
    i = 0;
    DL_FOREACH(region_head, region_elt)
    {
        if (i >= region_count) {
            printf("==PDC_SERVER[%d] %s - More regions %d than allocated %d\n", pdc_server_rank_g, __func__,
                   i, region_count);
            ret_value = FAIL;
            goto done;
        }
        PDC_region_list_t_deep_cp(region_elt, &res_region_list[i]);
        res_region_list[i].prev = NULL;
        res_region_list[i].next = NULL;
        DL_APPEND(args->overlap_storage_region_list, &res_region_list[i]);
        i++;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Get the storage metadata of *1* object
 *
 * \param  in[IN/OUT]            inpupt (obj_name)
 *                               output (n_storage_region, file_path, offset, size)
 *
 * \return Non-negative on success/Negative on failure
 */
static perr_t
PDC_Server_get_all_storage_meta_with_one_name(storage_meta_query_one_name_args_t *args)
{
    hg_return_t                  hg_ret;
    perr_t                       ret_value = SUCCEED;
    uint32_t                     server_id = 0;
    hg_handle_t                  rpc_handle;
    storage_meta_name_query_in_t in;

    FUNC_ENTER(NULL);

    server_id = PDC_get_server_by_name(args->name, pdc_server_size_g);
    if (server_id == (uint32_t)pdc_server_rank_g) {
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
            printf("==PDC_SERVER[%d]: %s - will get storage meta from remote server %d\n", pdc_server_rank_g,
                   __func__, server_id);
            fflush(stdout);
        }

        if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
            printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", pdc_server_rank_g,
                   server_id);
            ret_value = FAIL;
            goto done;
        }
        n_get_remote_storage_meta_g++;

        // Add current task and relevant data ptrs to s2s task queue, so later when receiving bulk transfer
        // request we know which task that bulk data is needed
        in.obj_name  = args->name;
        in.origin_id = pdc_server_rank_g;

        in.task_id = PDC_add_task_to_list(&pdc_server_s2s_task_head_g, args->cb, args->cb_args,
                                          &pdc_server_task_id_g, &pdc_server_task_mutex_g);

        hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr,
                           storage_meta_name_query_register_id_g, &rpc_handle);

        hg_ret = HG_Forward(rpc_handle, PDC_check_int_ret_cb, NULL, &in);
        if (hg_ret != HG_SUCCESS) {
            printf("==PDC_SERVER[%d]: %s - Could not start HG_Forward to server %u\n", pdc_server_rank_g,
                   __func__, server_id);
            HG_Destroy(rpc_handle);
            ret_value = FAIL;
            goto done;
        }
        HG_Destroy(rpc_handle);
    } // end else

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Accumulate storage metadata until reached n_total, then perform the corresponding callback (Read data)
 *
 * \param  in[IN]               input with type accumulate_storage_meta_t
 *
 * \return Non-negative on success/Negative on failure

 */
static perr_t
PDC_Server_accumulate_storage_meta_then_read(storage_meta_query_one_name_args_t *in)
{
    perr_t                     ret_value = SUCCEED;
    accumulate_storage_meta_t *accu_meta;
    region_list_t *            req_region = NULL, *region_elt = NULL, *read_list_head = NULL;
    int                        i, is_sort_read;
    size_t                     j;

    FUNC_ENTER(NULL);

    accu_meta = in->accu_meta;

    // Add current input to accumulate_storage_meta structure
    accu_meta->storage_meta[accu_meta->n_accumulated++] = in;

    // Trigger the read procedure when we have accumulated all storage meta
    if (accu_meta->n_accumulated >= accu_meta->n_total) {
        is_sort_read       = 0;
        char *sort_request = getenv("PDC_SORT_READ");
        if (NULL != sort_request)
            is_sort_read = 1;

#ifdef ENABLE_TIMING
        struct timeval pdc_timer_start, pdc_timer_end;
        double         read_total_sec;
        gettimeofday(&pdc_timer_start, 0);
#endif

        // Attach the overlapping storage regions we got to the request region
        for (i = 0; i < accu_meta->n_accumulated; i++) {
            req_region                           = accu_meta->storage_meta[i]->req_region;
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
                    req_region->count[j] = PDC_MAX(req_region->count[j] + req_region->start[j],
                                                   region_elt->count[j] + region_elt->start[j]);
                }
            }
            // Insert the request to a list based on file path and offset
            if (1 == is_sort_read)
                DL_INSERT_INORDER(read_list_head, req_region, region_list_path_offset_cmp);
            else
                DL_APPEND(read_list_head, req_region);
        }

        DL_FOREACH(read_list_head, region_elt)
        {

            // Read data to shm
            ret_value = PDC_Server_read_one_region(region_elt);
            if (ret_value != SUCCEED) {
                printf("==PDC_SERVER[%d]: %s - Error with PDC_Server_read_one_region\n", pdc_server_rank_g,
                       __func__);
            }
        }

        // Sort (restore) the order of read_list_head according to the original order from request
        DL_SORT(read_list_head, PDC_region_list_seq_id_cmp);
#ifdef ENABLE_TIMING
        gettimeofday(&pdc_timer_end, 0);
        read_total_sec = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
        printf("==PDC_SERVER[%d]: read %d objects time: %.4f, n_fread: %d, n_fopen: %d, is_sort_read: %d\n",
               pdc_server_rank_g, accu_meta->n_accumulated, read_total_sec, n_fread_g, n_fopen_g,
               is_sort_read);
        fflush(stdout);
#endif

        // send all shm info to client
        ret_value = PDC_Server_notify_client_multi_io_complete(accu_meta->client_id, accu_meta->client_seq_id,
                                                               accu_meta->n_total, read_list_head);
    } // End if

    // TODO free many things
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_query_read_names(query_read_names_args_t *query_read_args)
{
    perr_t                              ret_value = SUCCEED;
    int                                 i;
    storage_meta_query_one_name_args_t *query_name_args = NULL;

    FUNC_ENTER(NULL);

    // Temp storage to accumulate all storage meta of the requested objects
    // Each task should have one such structure
    accumulate_storage_meta_t *accmulate_meta =
        (accumulate_storage_meta_t *)calloc(1, sizeof(accumulate_storage_meta_t));
    accmulate_meta->n_total       = query_read_args->cnt;
    accmulate_meta->client_id     = query_read_args->client_id;
    accmulate_meta->client_seq_id = query_read_args->client_seq_id;
    accmulate_meta->storage_meta  = (storage_meta_query_one_name_args_t **)calloc(
        query_read_args->cnt, sizeof(storage_meta_query_one_name_args_t *));

    // Now we need to retrieve their storage metadata, some can be found in local metadata server,
    // others are stored on remote metadata servers
    for (i = 0; i < query_read_args->cnt; i++) {
        // query_name_args is the struct to store all storage metadata of each request obj (obj_name)
        query_name_args =
            (storage_meta_query_one_name_args_t *)calloc(1, sizeof(storage_meta_query_one_name_args_t));
        query_name_args->seq_id     = i;
        query_name_args->req_region = (region_list_t *)calloc(1, sizeof(region_list_t));
        PDC_init_region_list(query_name_args->req_region);
        query_name_args->req_region->access_type = PDC_READ;
        // TODO: if requesting partial data, adjust the actual region info accordingly
        query_name_args->name           = query_read_args->obj_names[i];
        query_name_args->cb             = PDC_Server_accumulate_storage_meta_then_read;
        query_name_args->cb_args        = query_name_args;
        query_name_args->accu_meta      = accmulate_meta;
        accmulate_meta->storage_meta[i] = query_name_args;

        PDC_Server_get_all_storage_meta_with_one_name(query_name_args);
    }

    // After query all the object names, wait for the bulk xfer from the remote servers
    // When all storage metadata have been collected, the read operation will be triggered
    // in PDC_Server_accumulate_storage_meta_then_read().

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_Server_query_read_names_cb(const struct hg_cb_info *callback_info)
{
    PDC_Server_query_read_names((query_read_names_args_t *)callback_info->arg);

    return HG_SUCCESS;
}

hg_return_t
PDC_Server_storage_meta_name_query_bulk_respond_cb(const struct hg_cb_info *callback_info)
{
    hg_return_t   ret    = HG_SUCCESS;
    hg_handle_t   handle = callback_info->info.forward.handle;
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

done:
    return ret;
}

// Get all storage meta of the one requested object name and bulk xfer to original requested server
hg_return_t
PDC_Server_storage_meta_name_query_bulk_respond(const struct hg_cb_info *callback_info)
{
    hg_return_t                         hg_ret = HG_SUCCESS;
    perr_t                              ret_value;
    storage_meta_name_query_in_t *      args;
    storage_meta_query_one_name_args_t *query_args;
    hg_handle_t                         rpc_handle;
    hg_bulk_t                           bulk_handle;
    bulk_rpc_in_t                       bulk_rpc_in;
    void **                             buf_ptrs;
    hg_size_t *                         buf_sizes;
    uint32_t                            server_id;
    region_info_transfer_t **           region_infos;
    region_list_t *                     region_elt;
    int                                 i, j;
    FUNC_ENTER(NULL);

    args = (storage_meta_name_query_in_t *)callback_info->arg;

    // Now metadata object is local
    query_args = (storage_meta_query_one_name_args_t *)calloc(1, sizeof(storage_meta_query_one_name_args_t));
    query_args->name = args->obj_name;

    ret_value = PDC_Server_get_local_storage_meta_with_one_name(query_args);
    if (ret_value != SUCCEED) {

        printf("==PDC_SERVER[%d]: %s - get local storage location ERROR!\n", pdc_server_rank_g, __func__);
        goto done;
    }

    // Now the storage meta is stored in query_args->regions;
    server_id = args->origin_id;
    if (PDC_Server_lookup_server_id(server_id) != SUCCEED) {
        printf("==PDC_SERVER[%d]: Error getting remote server %d addr via lookup\n", pdc_server_rank_g,
               server_id);
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

    int nbuf     = 3 * query_args->n_res + 1;
    buf_sizes    = (hg_size_t *)calloc(sizeof(hg_size_t), nbuf);
    buf_ptrs     = (void **)calloc(sizeof(void *), nbuf);
    region_infos = (region_info_transfer_t **)calloc(sizeof(region_info_transfer_t *), query_args->n_res);

    // buf_ptrs[0]: task_id
    buf_ptrs[0] = &(args->task_id);

    buf_sizes[0] = sizeof(int);

    // We need path, offset, region info (region_info_transfer_t)
    i = 1;
    j = 0;
    DL_FOREACH(query_args->overlap_storage_region_list, region_elt)
    {
        region_infos[j] = (region_info_transfer_t *)calloc(sizeof(region_info_transfer_t), 1);
        PDC_region_list_t_to_transfer(region_elt, region_infos[j]);

        if (region_elt->cache_location[0] != 0) {
            buf_ptrs[i]     = region_elt->cache_location;
            buf_ptrs[i + 1] = &(region_elt->cache_offset);
        }
        else {
            buf_ptrs[i]     = region_elt->storage_location;
            buf_ptrs[i + 1] = &(region_elt->offset);
        }
        buf_ptrs[i + 2]  = region_infos[j];
        buf_sizes[i]     = strlen(buf_ptrs[i]) + 1;
        buf_sizes[i + 1] = sizeof(uint64_t);
        buf_sizes[i + 2] = sizeof(region_info_transfer_t);
        i += 3;
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
    fflush(stdout);
    FUNC_LEAVE(hg_ret);
}

// We have received the storage metadata from remote meta server, which is stored in region_list_head
// Now we want to find the corresponding task the previously requested the meta retrival and attach
// the received storage meta to it.
// Each task was previously created with
//      cb   = PDC_Server_accumulate_storage_meta_then_read
//      args = (storage_meta_query_one_name_args_t *args)
perr_t
PDC_Server_proc_storage_meta_bulk(int task_id, int n_regions, region_list_t *region_list_head)
{
    perr_t                              ret_value = SUCCEED;
    storage_meta_query_one_name_args_t *query_args;

    FUNC_ENTER(NULL);

    pdc_task_list_t *task =
        PDC_find_task_from_list(&pdc_server_s2s_task_head_g, task_id, &pdc_server_task_mutex_g);
    if (task == NULL) {
        printf("==PDC_SERVER[%d]: %s - Error getting task %d\n", pdc_server_rank_g, __func__, task_id);
        ret_value = FAIL;
        goto done;
    }

    // Add the result storage regions to accumulate_storage_meta
    query_args                              = (storage_meta_query_one_name_args_t *)task->cb_args;
    query_args->overlap_storage_region_list = region_list_head;
    query_args->n_res                       = n_regions;

    // Execute callback function associated with this task
    if (task->cb != NULL)
        task->cb(task->cb_args); // should equals to PDC_Server_accumulate_storage_meta_then_read(query_args)

    PDC_del_task_from_list(&pdc_server_s2s_task_head_g, task, &pdc_server_task_mutex_g);

done:
    fflush(stdout);

    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_add_client_shm_to_cache(int cnt, void *buf_cp)
{
    perr_t                     ret_value = SUCCEED;
    int                        i, j;
    region_storage_meta_t *    storage_metas = (region_storage_meta_t *)buf_cp;
    pdc_data_server_io_list_t *io_list_elt, *io_list_target;
    region_list_t *            new_region;

    FUNC_ENTER(NULL);

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_read_list_mutex_g);
#endif
    // Have the server update the metadata or write to burst buffer
    for (i = 0; i < cnt; i++) {
        // Find existing io_list target for this object
        io_list_target = NULL;
        DL_FOREACH(pdc_data_server_read_list_head_g, io_list_elt)
        if (storage_metas[i].obj_id == io_list_elt->obj_id) {
            io_list_target = io_list_elt;
            break;
        }

        // If not found, create and insert one to the read list
        if (NULL == io_list_target) {
            io_list_target = (pdc_data_server_io_list_t *)calloc(1, sizeof(pdc_data_server_io_list_t));
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

        new_region = (region_list_t *)calloc(1, sizeof(region_list_t));
        PDC_region_transfer_t_to_list_t(&storage_metas[i].region_transfer, new_region);
        strcpy(new_region->shm_addr, storage_metas[i].storage_location);
        new_region->offset    = storage_metas[i].offset;
        new_region->data_size = storage_metas[i].size;

        // Open shared memory and map to data buf
        new_region->shm_fd = shm_open(new_region->shm_addr, O_RDONLY, 0666);
        if (new_region->shm_fd == -1) {
            printf("==PDC_SERVER[%d]: %s - Shared memory open failed [%s]!\n", pdc_server_rank_g, __func__,
                   new_region->shm_addr);
            ret_value = FAIL;
            goto done;
        }

        new_region->buf =
            mmap(0, new_region->data_size, PROT_READ, MAP_SHARED, new_region->shm_fd, new_region->offset);
        if (new_region->buf == MAP_FAILED) {
            // printf("==PDC_SERVER[%d]: Map failed: %s\n", pdc_server_rank_g, strerror(errno));
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
}

/* For data query */
void
PDC_Server_free_query_task(query_task_t *task)
{
    int i;
    if (NULL != task->query)

        PDCquery_free_all(task->query);

    if (task->coords)
        free(task->coords);

    for (i = 0; i < task->n_read_data_region; i++) {
        if (task->data_arr && task->data_arr[i])
            free(task->data_arr[i]);
    }
    if (task->data_arr)
        free(task->data_arr);
    if (task->n_hits_from_server)
        free(task->n_hits_from_server);

    free(task);
}

void
PDC_query_visit_leaf_with_cb(pdc_query_t *query, perr_t (*func)(pdc_query_t *arg))
{
    if (NULL == query)
        return;

    PDC_query_visit_leaf_with_cb(query->left, func);
    PDC_query_visit_leaf_with_cb(query->right, func);

    if (NULL == query->left && NULL == query->right)
        func(query);
    return;
}

static perr_t
PDC_Server_data_read_to_buf_1_region(region_list_t *region)
{
    perr_t   ret_value = SUCCEED;
    uint64_t offset, read_bytes;
    FILE *   fp_read = NULL;

    if (region->is_data_ready == 1)
        return SUCCEED;

    fp_read = fopen(region->storage_location, "rb");
    if (NULL == fp_read) {
        printf("==PDC_SERVER[%d]: fopen failed [%s]\n", pdc_server_rank_g, region->storage_location);
        ret_value = FAIL;
        goto done;
    }
    n_fopen_g++;

    offset = ftell(fp_read);
    if (offset < region->offset)
        fseek(fp_read, region->offset - offset, SEEK_CUR);
    else
        fseek(fp_read, region->offset, SEEK_SET);

    if (region->data_size == 0) {
        printf("==PDC_SERVER[%d]: %s - region data_size is 0\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    region->buf = malloc(region->data_size);

    read_bytes = fread(region->buf, 1, region->data_size, fp_read);
    if (read_bytes != region->data_size) {
        printf("==PDC_SERVER[%d]: %s - read size %" PRIu64 " is not expected %" PRIu64 "\n",
               pdc_server_rank_g, __func__, read_bytes, region->data_size);
        ret_value = FAIL;
        goto done;
    }

    region->is_data_ready = 1;
    region->is_io_done    = 1;

done:
    if (fp_read)
        fclose(fp_read);
    return ret_value;
}

static perr_t
PDC_Server_data_read_to_buf(region_list_t *region_list_head)
{
    perr_t         ret_value = SUCCEED;
    region_list_t *region_elt;
    char *         prev_path = NULL;
    uint64_t       offset, read_bytes;
    FILE *         fp_read    = NULL;
    int            read_count = 0;

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start1, pdc_timer_end1;
    gettimeofday(&pdc_timer_start1, 0);
#endif

    DL_FOREACH(region_list_head, region_elt)
    {

        if (region_elt->is_data_ready == 1)
            continue;

        if (prev_path == NULL || strcmp(region_elt->storage_location, prev_path) != 0) {
            if (fp_read != NULL)
                fclose(fp_read);
            fp_read = fopen(region_elt->storage_location, "rb");
            if (NULL == fp_read) {
                printf("==PDC_SERVER[%d]: fopen failed [%s]\n", pdc_server_rank_g,
                       region_elt->storage_location);
            }
            n_fopen_g++;
        }

        offset = ftell(fp_read);
        if (offset < region_elt->offset)
            fseek(fp_read, region_elt->offset - offset, SEEK_CUR);
        else
            fseek(fp_read, region_elt->offset, SEEK_SET);

        if (region_elt->data_size == 0) {
            printf("==PDC_SERVER[%d]: %s - region data_size is 0\n", pdc_server_rank_g, __func__);
            continue;
        }

        region_elt->buf = malloc(region_elt->data_size);

        read_bytes = fread(region_elt->buf, 1, region_elt->data_size, fp_read);
        if (read_bytes != region_elt->data_size) {
            printf("==PDC_SERVER[%d]: %s - read size %" PRIu64 " is not expected %" PRIu64 "\n",
                   pdc_server_rank_g, __func__, read_bytes, region_elt->data_size);
            continue;
        }
        read_count++;

        region_elt->is_data_ready = 1;
        region_elt->is_io_done    = 1;
    }

    if (fp_read)
        fclose(fp_read);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end1, 0);
    double read_time = PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
    server_read_time_g += read_time;
    if (region_list_head != NULL) {
        printf("==PDC_SERVER[%d]: %s finished reading obj %" PRIu64 " of %d regions, %.2f seconds!\n",
               pdc_server_rank_g, __func__, region_list_head->obj_id, read_count, read_time);
    }
    else
        printf("==PDC_SERVER[%d]: %s no regions have been read!\n", pdc_server_rank_g, __func__);
#endif

    fflush(stdout);
    return ret_value;
}

static int
PDC_region_has_hits_from_hist(pdc_query_constraint_t *constraint, pdc_histogram_t *region_hist)
{
    pdc_query_op_t lop;
    double         value, value2;

    if (constraint == NULL || region_hist == NULL) {
        printf("==PDC_SERVER[%d]: %s -  NULL input!\n", pdc_server_rank_g, __func__);
        return -1;
    }
    /*
        switch (constraint->type) {
            case PDC_FLOAT:
                value  = (double)(*((float *)&constraint->value));
                value2 = (double)(*((float *)&constraint->value2));
                break;
            case PDC_DOUBLE:
                value  = (double)(*((double *)&constraint->value));
                value2 = (double)(*((double *)&constraint->value2));
                break;
            case PDC_INT:
                value  = (double)(*((int *)&constraint->value));
                value2 = (double)(*((int *)&constraint->value2));
                break;
            case PDC_UINT:
                value  = (double)(*((uint32_t *)&constraint->value));
                value2 = (double)(*((uint32_t *)&constraint->value2));
                break;
            case PDC_INT64:
                value  = (double)(*((int64_t *)&constraint->value));
                value2 = (double)(*((int64_t *)&constraint->value2));
                break;
            case PDC_UINT64:
                value  = (double)(*((uint64_t *)&constraint->value));
                value2 = (double)(*((uint64_t *)&constraint->value2));
                break;
            default:
                printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g, __func__);
                return -1;
        }
    */
    switch (constraint->type) {
        case PDC_FLOAT:
            value  = (double)constraint->value;
            value2 = (double)constraint->value2;
            break;
        case PDC_DOUBLE:
            value  = (double)constraint->value;
            value2 = (double)constraint->value2;
            break;
        case PDC_INT:
            value  = (double)constraint->value;
            value2 = (double)constraint->value2;
            break;
        case PDC_UINT:
            value  = (double)constraint->value;
            value2 = (double)constraint->value2;
            break;
        case PDC_INT64:
            value  = (double)constraint->value;
            value2 = (double)constraint->value2;
            break;
        case PDC_UINT64:
            value  = (double)constraint->value;
            value2 = (double)constraint->value2;
            break;
        default:
            printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g, __func__);
            return -1;
    }

    lop = constraint->op;

    if (constraint->is_range == 1) {
        if (value > region_hist->range[region_hist->nbin * 2 - 1] || value2 < region_hist->range[0]) {
            return 0;
        }
    }
    else {
        // one sided
        if (lop == PDC_LT || lop == PDC_LTE) {
            if (value < region_hist->range[0])
                return 0;
        }
        else if (lop == PDC_GT || lop == PDC_GTE) {
            if (value > region_hist->range[region_hist->nbin * 2 - 1])
                return 0;
        }
    }

    return 1;
}

/*
static perr_t
PDC_constraint_get_nhits_from_hist(pdc_query_constraint_t *constraint, pdc_histogram_t *region_hist,
                                  uint64_t *min_hits, uint64_t *max_hits)
{
    perr_t ret_value = SUCCEED;
    pdc_query_op_t lop;
    double value, value2;
    int    i, lidx, ridx;

    if (constraint == NULL || region_hist == NULL || min_hits == NULL || max_hits == NULL) {
        printf("==PDC_SERVER[%d]: %s -  NULL input!\n", pdc_server_rank_g, __func__);

        goto done;
    }

    switch(constraint->type) {
        case PDC_FLOAT :
            value  = (double)(*((float*)&constraint->value));
            value2 = (double)(*((float*)&constraint->value2));
            break;
        case PDC_DOUBLE:
            value  = (double)(*((double*)&constraint->value));
            value2 = (double)(*((double*)&constraint->value2));
            break;
        case PDC_INT:
            value  = (double)(*((int*)&constraint->value));
            value2 = (double)(*((int*)&constraint->value2));
            break;
        case PDC_UINT:
            value  = (double)(*((uint32_t*)&constraint->value));
            value2 = (double)(*((uint32_t*)&constraint->value2));
            break;
        case PDC_INT64:
            value  = (double)(*((int64_t*)&constraint->value));
            value2 = (double)(*((int64_t*)&constraint->value2));

            break;
        case PDC_UINT64:
            value  = (double)(*((uint64_t*)&constraint->value));
            value2 = (double)(*((uint64_t*)&constraint->value2));
            break;
        default:
            printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
    }

    lop = constraint->op;
    *min_hits = *max_hits = 0;
    lidx = 0;
    ridx = region_hist->nbin*2 - 1;

    if (constraint->is_range == 1) {
        // No overlap at all
        if (value > region_hist->range[region_hist->nbin*2-1] || value2 < region_hist->range[0]) {
            goto done;
        }

        // Find the value range in hist that includes the queried range
        i = 0;
        while (i < region_hist->nbin*2 && region_hist->range[i] < value) {
            lidx = i;
            i += 2;
        }

        i = region_hist->nbin*2 - 1;
        while (i > 0 && region_hist->range[i] > value2) {
            ridx = i;
            i -= 2;
        }
    }
    else {
        // one sided
        if (lop == PDC_LT || lop == PDC_LTE) {
            i = region_hist->nbin*2 - 1;
            while (i > 0 && region_hist->range[i] > value) {
                ridx = i;
                i -= 2;
            }
            lidx = 0;
            value2 = value;
            value = -DBL_MAX;
        }
        else if (lop == PDC_GT || lop == PDC_GTE) {
            i = 0;
            while (i < region_hist->nbin*2 && region_hist->range[i] < value) {
                lidx = i;
                i += 2;
            }
            ridx = region_hist->nbin*2 - 1;
            value2 = DBL_MAX;
        }
    }

    for (i = lidx/2; i <= ridx/2; i++) {
        (*max_hits) += region_hist->bin[i];
        if (region_hist->range[i*2] >= value && region_hist->range[i*2+1] <= value2) {

            (*min_hits) += region_hist->bin[i];
        }
    }

done:
    return ret_value;
}
*/

static perr_t
PDC_Server_load_query_data(query_task_t *task, pdc_query_t *query, pdc_query_combine_op_t combine_op)
{
    perr_t                     ret_value  = SUCCEED;
    region_list_t *            req_region = NULL, *region_tmp = NULL;
    region_list_t *            storage_region_list_head = NULL;
    pdc_data_server_io_list_t *io_list_elt, *io_list_target = NULL;
    uint64_t                   obj_id;
    int                        iter, count, is_same_region, i, can_skip;

    pdc_query_constraint_t *constraint = query->constraint;
    storage_region_list_head           = constraint->storage_region_list_head;

    if (NULL == constraint || NULL == storage_region_list_head) {
        printf("==PDC_SERVER[%d]: %s -  NULL query constraint/storage region!\n", pdc_server_rank_g,
               __func__);
        goto done;
    }

    obj_id = constraint->obj_id;

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_lock(&data_read_list_mutex_g);
#endif

    // Iterate io list, find the IO list of this obj
    DL_FOREACH(pdc_data_server_read_list_head_g, io_list_elt)
    {
        if (obj_id == io_list_elt->obj_id) {
            io_list_target = io_list_elt;
            break;
        }
    }

    // If not found, create and insert one to the list
    if (NULL == io_list_target) {
        // pdc_data_server_io_list_t maintains the request list for one object id,
        // write and read are separate lists
        io_list_target = (pdc_data_server_io_list_t *)calloc(1, sizeof(pdc_data_server_io_list_t));
        if (NULL == io_list_target) {
            printf("==PDC_SERVER[%d]: %s -  ERROR allocating pdc_data_server_io_list_t!\n", pdc_server_rank_g,
                   __func__);
            ret_value = FAIL;
            goto done;
        }
        io_list_target->obj_id           = obj_id;
        io_list_target->total            = 0; // not used yet
        io_list_target->count            = 0;
        io_list_target->ndim             = 0;
        io_list_target->region_list_head = NULL;

        // Add to the io list
        DL_APPEND(pdc_data_server_read_list_head_g, io_list_target);
        io_list_target->count++;
    }

#ifdef ENABLE_MULTITHREAD
    hg_thread_mutex_unlock(&data_read_list_mutex_g);
#endif

    DL_COUNT(storage_region_list_head, region_tmp, count);

    // Iterate over current list and check for existing identical regions in the io list
    iter = -1;
    DL_FOREACH(storage_region_list_head, req_region)
    {
        iter++;

        if (combine_op == PDC_QUERY_OR) {
            if (task->invalid_region_ids != NULL) {
                free(task->invalid_region_ids);
                task->invalid_region_ids = NULL;
                task->ninvalid_region    = 0;
            }
        }
        else if (combine_op == PDC_QUERY_AND && task->invalid_region_ids != NULL) {
            can_skip = 0;
            for (i = 0; i < task->ninvalid_region; i++) {
                if (iter == task->invalid_region_ids[i]) {
                    can_skip = 1;
                    break;
                }
            }
            if (can_skip == 1)
                continue;
        }

        // use histogram to see if we need to read this region
        if (gen_hist_g == 1) {

            if (req_region->region_hist->nbin == 0) {
                printf("==PDC_SERVER[%d]: %s -  ERROR histogram is empty!\n", pdc_server_rank_g, __func__);
                fflush(stdout);
            }

            if (PDC_region_has_hits_from_hist(constraint, req_region->region_hist) == 0) {
                /* printf("==PDC_SERVER[%d]: Region [%" PRIu64 ", %" PRIu64 "], skipped by histogram\n", */
                /*         pdc_server_rank_g, req_region->start[0], req_region->count[0]); */

                if (task->invalid_region_ids == NULL)
                    task->invalid_region_ids = (int *)calloc(count, sizeof(int));

                can_skip = 0;
                for (i = 0; i < task->ninvalid_region; i++) {
                    if (task->invalid_region_ids[i] == iter) {
                        can_skip = 1;
                        break;
                    }
                }
                if (can_skip == 0) {
                    task->invalid_region_ids[task->ninvalid_region] = iter;
                    task->ninvalid_region++;
                }
                continue;
            }
        }

        is_same_region = 0;
        DL_FOREACH(io_list_target->region_list_head, region_tmp)
        {
            is_same_region = PDC_is_same_region_list(region_tmp, req_region);
            if (1 == is_same_region)
                break;
        }

        if (1 != is_same_region) {
            // append current request region to the io list
            region_list_t *new_region = (region_list_t *)calloc(1, sizeof(region_list_t));
            if (new_region == NULL) {
                printf("==PDC_SERVER: ERROR allocating new_region!\n");
                ret_value = FAIL;
                goto done;
            }
            req_region->io_cache_region = new_region;
            PDC_region_list_t_deep_cp(req_region, new_region);
            new_region->is_data_ready = 0;
            memset(new_region->shm_addr, 0, sizeof(char) * ADDR_MAX);
            new_region->buf             = NULL;
            new_region->shm_fd          = 0;
            new_region->io_cache_region = NULL;
            new_region->access_type     = PDC_READ;

            DL_APPEND(io_list_target->region_list_head, new_region);
        }
    } // Ened DL_FOREACH

    // Currently reads all regions of a query constraint together
    // TODO: potential optimization: aggregate all I/O requests
    ret_value = PDC_Server_data_read_to_buf(io_list_target->region_list_head);
    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - PDC_Server_data_read_to_shm FAILED!\n", pdc_server_rank_g, __func__);
        goto done;
    }

done:
    fflush(stdout);
    return ret_value;
}

perr_t
region_index_to_coord(int ndim, uint64_t idx, uint64_t *sizes, uint64_t *coord)
{
    if (sizes == NULL || coord == NULL) {
        printf("==PDC_SERVER[%d]: %s - input NULL!\n", pdc_server_rank_g, __func__);
        return FAIL;
    }

    if (ndim > 3) {
        printf("==PDC_SERVER[%d]: %s - dimension > 3 not supported!\n", pdc_server_rank_g, __func__);
        return FAIL;
    }

    if (ndim == 3) {
        coord[2] = idx / (sizes[1] * sizes[0]);
        idx -= sizes[1] * sizes[0];
    }
    if (ndim == 2) {
        coord[1] = idx / sizes[0];
        idx -= sizes[0];
    }
    coord[0] = idx;

    return SUCCEED;
}

uint64_t
coord_to_region_index(size_t ndim, uint64_t *coord, region_list_t *region, int unit_size)
{
    uint64_t off = 0;

    if (ndim == 0 || coord == NULL || region == NULL || region->start[0] == 0 || region->count[0] == 0) {
        printf("==PDC_SERVER[%d]: %s - input NULL!\n", pdc_server_rank_g, __func__);
        return 0;
    }

    if (ndim > 3) {
        printf("==PDC_SERVER[%d]: %s - cannot handle dim > 3!\n", pdc_server_rank_g, __func__);
        return 0;
    }

    if (ndim == 3)
        off = (coord[2] - region->start[2] / unit_size) * region->count[0] / unit_size * region->count[1] /
              unit_size;
    if (ndim == 2)
        off += (coord[1] - region->start[1] / unit_size) * region->count[0] / unit_size;

    off += (coord[0] - region->start[0] / unit_size);

    return off;
}

static int
is_coord_within_region(int ndim, uint64_t *coords, region_list_t *region_constraint, int unit_size)
{
    int      i;
    uint64_t coord[3];

    if (coords == NULL || region_constraint == NULL || ndim > 3)
        return -1;

    for (i = 0; i < ndim; i++) {
        coord[i] = coords[i];
        coord[i] *= unit_size;
        if (coord[i] < region_constraint->start[i] ||
            coord[i] > region_constraint->start[i] + region_constraint->count[i]) {
            return -1;
        }
    }
    return 1;
}

static int
is_idx_within_region(uint64_t idx, region_list_t *region, region_list_t *region_constraint, int unit_size)
{
    size_t   i, ndim;
    uint64_t coord[DIM_MAX];

    if (region == NULL || region_constraint == NULL)
        return -1;

    ndim = region->ndim;
    region_index_to_coord(ndim, idx, region->count, (uint64_t *)&coord);

    for (i = 0; i < ndim; i++) {
        coord[i] *= unit_size;
        coord[i] += region->start[i];
        if (coord[i] < region_constraint->start[i] ||
            coord[i] > region_constraint->start[i] + region_constraint->count[i]) {
            return -1;
        }
    }
    return 1;
}

int
compare_coords_1d(const void *a, const void *b)
{
    return (memcmp(a, b, sizeof(uint64_t)));
}

int
compare_coords_2d(const void *a, const void *b)
{
    return (memcmp(a, b, sizeof(uint64_t) * 2));
}

int
compare_coords_3d(const void *a, const void *b)
{
    return (memcmp(a, b, sizeof(uint64_t) * 3));
}

/* perr_t QUERY_EVALUATE_SCAN_OPT(uint64_t _n, float *_data, pdc_query_op_t _op, void *_value, */
/*                                pdc_selection_t *_sel, region_list_t *_region, int _unit_size, */
/*                                region_list_t *_region_constraint, pdc_query_combine_op_t _combine_op) */
/* { */
/*     perr_t ret_value; */
#define MACRO_QUERY_EVALUATE_SCAN_OPT(TYPE, _n, _data, _op, _value, _sel, _region, _unit_size,               \
                                      _region_constraint, _combine_op)                                       \
    ({                                                                                                       \
        uint64_t idx, iii, jjj, ttt, cur_count = 0, istart, has_dup;                                         \
        int      is_good, _ndim;                                                                             \
        TYPE *   edata = (TYPE *)(_data);                                                                    \
        _ndim          = (_region)->ndim;                                                                    \
        istart         = (_sel)->nhits * _ndim;                                                              \
        if (_ndim > 3) {                                                                                     \
            printf("==PDC_SERVER[%d]: %s - dimension > 3 not supported!\n", pdc_server_rank_g, __func__);    \
            ret_value = FAIL;                                                                                \
            goto done;                                                                                       \
        }                                                                                                    \
        if ((_combine_op) == PDC_QUERY_NONE || (_combine_op) == PDC_QUERY_OR) {                              \
            for (iii = 0; iii < (_n); iii++) {                                                               \
                is_good = 0;                                                                                 \
                if ((_region_constraint) &&                                                                  \
                    is_idx_within_region(iii, (_region), (_region_constraint), (_unit_size)) != 1)           \
                    continue;                                                                                \
                switch (_op) {                                                                               \
                    case PDC_GT:                                                                             \
                        if (edata[iii] > *((TYPE *)(_value)))                                                \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_LT:                                                                             \
                        if (edata[iii] < *((TYPE *)(_value)))                                                \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_GTE:                                                                            \
                        if (edata[iii] >= *((TYPE *)(_value)))                                               \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_LTE:                                                                            \
                        if (edata[iii] <= *((TYPE *)(_value)))                                               \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_EQ:                                                                             \
                        if (edata[iii] == *((TYPE *)(_value)))                                               \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    default:                                                                                 \
                        printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g,      \
                               __func__);                                                                    \
                        ret_value = FAIL;                                                                    \
                        goto done;                                                                           \
                }                                                                                            \
                if (is_good == 1) {                                                                          \
                    if ((istart + cur_count + 1) * _ndim > ((_sel)->coords_alloc)) {                         \
                        ((_sel)->coords_alloc) *= 2;                                                         \
                        ((_sel)->coords) =                                                                   \
                            (uint64_t *)realloc(((_sel)->coords), (_sel)->coords_alloc * sizeof(uint64_t));  \
                        if (NULL == ((_sel)->coords)) {                                                      \
                            printf("==PDC_SERVER[%d]: %s - error with malloc!\n", pdc_server_rank_g,         \
                                   __func__);                                                                \
                            ret_value = FAIL;                                                                \
                            goto done;                                                                       \
                        }                                                                                    \
                    }                                                                                        \
                    ttt = iii;                                                                               \
                    if (_ndim == 3) {                                                                        \
                        (_sel)->coords[istart + cur_count * _ndim + 2] =                                     \
                            ttt / ((_region)->count[1] / (_unit_size)) *                                     \
                                ((_region)->count[0] / (_unit_size)) +                                       \
                            (_region)->start[2] / (_unit_size);                                              \
                        ttt -= (_sel)->coords[istart + cur_count * _ndim + 2] *                              \
                               ((_region)->count[1] / (_unit_size)) * ((_region)->count[0] / (_unit_size));  \
                    }                                                                                        \
                    if (_ndim == 2) {                                                                        \
                        (_sel)->coords[istart + cur_count * _ndim + 1] =                                     \
                            ttt / ((_region)->count[0] / (_unit_size)) + (_region)->start[1] / (_unit_size); \
                        ttt -= (_sel)->coords[istart + cur_count * _ndim + 1] *                              \
                               ((_region)->count[0] / (_unit_size));                                         \
                    }                                                                                        \
                    (_sel)->coords[istart + cur_count * _ndim] = ttt + (_region)->start[0] / (_unit_size);   \
                    cur_count++;                                                                             \
                }                                                                                            \
            }                                                                                                \
            ((_sel)->nhits) += cur_count;                                                                    \
        }                                                                                                    \
        else if ((_combine_op) == PDC_QUERY_AND) {                                                           \
            has_dup = 0;                                                                                     \
            for (idx = 0; idx < (_sel)->nhits; idx++) {                                                      \
                if ((_sel)->coords[idx * (_ndim)] == ULLONG_MAX) {                                           \
                    continue;                                                                                \
                }                                                                                            \
                if (is_coord_within_region(_ndim, &(_sel)->coords[idx * (_ndim)], _region, _unit_size) !=    \
                    1) {                                                                                     \
                    continue;                                                                                \
                }                                                                                            \
                iii     = coord_to_region_index(_ndim, &(_sel)->coords[idx * (_ndim)], _region, _unit_size); \
                is_good = 0;                                                                                 \
                /* No need to check for region constraint as a query has one region constraint only */       \
                switch (_op) {                                                                               \
                    case PDC_GT:                                                                             \
                        if (edata[iii] > *((TYPE *)(_value)))                                                \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_LT:                                                                             \
                        if (edata[iii] < *((TYPE *)(_value)))                                                \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_GTE:                                                                            \
                        if (edata[iii] >= *((TYPE *)(_value)))                                               \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_LTE:                                                                            \
                        if (edata[iii] <= *((TYPE *)(_value)))                                               \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    case PDC_EQ:                                                                             \
                        if (edata[iii] == *((TYPE *)(_value)))                                               \
                            is_good = 1;                                                                     \
                        break;                                                                               \
                    default:                                                                                 \
                        printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g,      \
                               __func__);                                                                    \
                        ret_value = FAIL;                                                                    \
                        goto done;                                                                           \
                }                                                                                            \
                if (is_good != 1) {                                                                          \
                    /* Invalidate the coord by setting it to max value */                                    \
                    (_sel)->coords[idx * (_ndim)] = ULLONG_MAX;                                              \
                    has_dup++;                                                                               \
                }                                                                                            \
            }                                                                                                \
            /* Now get rid of the invalidated elements */                                                    \
            iii = jjj = 0;                                                                                   \
            if (has_dup > 0) {                                                                               \
                for (idx = 0; idx < (_sel)->nhits; idx++) {                                                  \
                    while ((_sel)->coords[idx * (_ndim)] == ULLONG_MAX && idx < (_sel)->nhits) {             \
                        iii++;                                                                               \
                        idx++;                                                                               \
                    }                                                                                        \
                    if (idx != jjj && idx < (_sel)->nhits) {                                                 \
                        memcpy(&(_sel)->coords[jjj * (_ndim)], &(_sel)->coords[idx * (_ndim)],               \
                               _ndim * sizeof(uint64_t));                                                    \
                    }                                                                                        \
                    jjj++;                                                                                   \
                }                                                                                            \
                if (iii > (_sel)->nhits)                                                                     \
                    printf("==PDC_SERVER[%d]: ERROR! invalidated more elements than total\n",                \
                           pdc_server_rank_g);                                                               \
                else                                                                                         \
                    ((_sel)->nhits) -= iii;                                                                  \
            }                                                                                                \
        }                                                                                                    \
    })

#define MACRO_QUERY_RANGE_EVALUATE_SCAN_OPT(TYPE, _n, _data, _lo_op, _lo, _hi_op, _hi, _sel, _region,        \
                                            _unit_size, _region_constraint, _combine_op)                     \
    ({                                                                                                       \
        uint64_t idx, iii, jjj, ttt, cur_count = 0, istart, has_dup;                                         \
        int      is_good, _ndim;                                                                             \
        TYPE *   edata = (TYPE *)(_data);                                                                    \
        _ndim          = (_region)->ndim;                                                                    \
        istart         = (_sel)->nhits * _ndim;                                                              \
        if (_ndim > 3) {                                                                                     \
            printf("==PDC_SERVER[%d]: %s - dimension > 3 not supported!\n", pdc_server_rank_g, __func__);    \
            ret_value = FAIL;                                                                                \
            goto done;                                                                                       \
        }                                                                                                    \
        if ((_combine_op) == PDC_QUERY_NONE || (_combine_op) == PDC_QUERY_OR) {                              \
            for (iii = 0; iii < (_n); iii++) {                                                               \
                is_good = 0;                                                                                 \
                if ((_region_constraint) &&                                                                  \
                    is_idx_within_region(iii, (_region), (_region_constraint), (_unit_size)) != 1)           \
                    continue;                                                                                \
                if ((_lo_op) == PDC_GT && (_hi_op) == PDC_LT) {                                              \
                    if (edata[iii] > (_lo) && edata[iii] < (_hi))                                            \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else if ((_lo_op) == PDC_GTE && (_hi_op) == PDC_LT) {                                        \
                    if (edata[iii] >= (_lo) && edata[iii] < (_hi))                                           \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else if ((_lo_op) == PDC_GT && (_hi_op) == PDC_LTE) {                                        \
                    if (edata[iii] > (_lo) && edata[iii] <= (_hi))                                           \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else if ((_lo_op) == PDC_GTE && (_hi_op) == PDC_LTE) {                                       \
                    if (edata[iii] >= (_lo) && edata[iii] <= (_hi))                                          \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else {                                                                                       \
                    printf("==PDC_SERVER[%d]: %s - error with range op! \n", pdc_server_rank_g, __func__);   \
                    ret_value = FAIL;                                                                        \
                    goto done;                                                                               \
                }                                                                                            \
                if (is_good == 1) {                                                                          \
                    if ((istart + cur_count + 1) * _ndim > ((_sel)->coords_alloc)) {                         \
                        ((_sel)->coords_alloc) *= 2;                                                         \
                        ((_sel)->coords) =                                                                   \
                            (uint64_t *)realloc(((_sel)->coords), (_sel)->coords_alloc * sizeof(uint64_t));  \
                        if (NULL == ((_sel)->coords)) {                                                      \
                            printf("==PDC_SERVER[%d]: %s - error with malloc!\n", pdc_server_rank_g,         \
                                   __func__);                                                                \
                            ret_value = FAIL;                                                                \
                            goto done;                                                                       \
                        }                                                                                    \
                    }                                                                                        \
                    ttt = iii;                                                                               \
                    if (_ndim == 3) {                                                                        \
                        (_sel)->coords[istart + cur_count * _ndim + 2] =                                     \
                            ttt / ((_region)->count[1] / (_unit_size)) *                                     \
                                ((_region)->count[0] / (_unit_size)) +                                       \
                            (_region)->start[2] / (_unit_size);                                              \
                        ttt -= (_sel)->coords[istart + cur_count * _ndim + 2] *                              \
                               ((_region)->count[1] / (_unit_size)) * ((_region)->count[0] / (_unit_size));  \
                    }                                                                                        \
                    if (_ndim == 2) {                                                                        \
                        (_sel)->coords[istart + cur_count * _ndim + 1] =                                     \
                            ttt / ((_region)->count[0] / (_unit_size)) + (_region)->start[1] / (_unit_size); \
                        ttt -= (_sel)->coords[istart + cur_count * _ndim + 1] *                              \
                               ((_region)->count[0] / (_unit_size));                                         \
                    }                                                                                        \
                    (_sel)->coords[istart + cur_count * _ndim] = ttt + (_region)->start[0] / (_unit_size);   \
                    cur_count++;                                                                             \
                }                                                                                            \
            }                                                                                                \
            ((_sel)->nhits) += cur_count;                                                                    \
        } /* End if left is NULL */                                                                          \
        else if ((_combine_op) == PDC_QUERY_AND) {                                                           \
            has_dup = 0;                                                                                     \
            for (idx = 0; idx < (_sel)->nhits; idx++) {                                                      \
                if ((_sel)->coords[idx * (_ndim)] == ULLONG_MAX) {                                           \
                    continue;                                                                                \
                }                                                                                            \
                if (is_coord_within_region(_ndim, &(_sel)->coords[idx * (_ndim)], _region, _unit_size) !=    \
                    1) {                                                                                     \
                    continue;                                                                                \
                }                                                                                            \
                iii     = coord_to_region_index(_ndim, &(_sel)->coords[idx * (_ndim)], _region, _unit_size); \
                is_good = 0;                                                                                 \
                /* No need to check for region constraint as a query has one region constraint only */       \
                if ((_lo_op) == PDC_GT && (_hi_op) == PDC_LT) {                                              \
                    if (edata[iii] > (_lo) && edata[iii] < (_hi))                                            \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else if ((_lo_op) == PDC_GTE && (_hi_op) == PDC_LT) {                                        \
                    if (edata[iii] >= (_lo) && edata[iii] < (_hi))                                           \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else if ((_lo_op) == PDC_GT && (_hi_op) == PDC_LTE) {                                        \
                    if (edata[iii] > (_lo) && edata[iii] <= (_hi))                                           \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else if ((_lo_op) == PDC_GTE && (_hi_op) == PDC_LTE) {                                       \
                    if (edata[iii] >= (_lo) && edata[iii] <= (_hi))                                          \
                        is_good = 1;                                                                         \
                }                                                                                            \
                else {                                                                                       \
                    printf("==PDC_SERVER[%d]: %s - error with range op! \n", pdc_server_rank_g, __func__);   \
                    ret_value = FAIL;                                                                        \
                    goto done;                                                                               \
                }                                                                                            \
                if (is_good != 1) {                                                                          \
                    /* Invalidate the coord by setting it to max value */                                    \
                    (_sel)->coords[idx * (_ndim)] = ULLONG_MAX;                                              \
                    has_dup++;                                                                               \
                }                                                                                            \
            }                                                                                                \
            /* Now get rid of the invalidated elements */                                                    \
            iii = jjj = 0;                                                                                   \
            if (has_dup > 0) {                                                                               \
                for (idx = 0; idx < (_sel)->nhits; idx++) {                                                  \
                    while ((_sel)->coords[idx * (_ndim)] == ULLONG_MAX && idx < (_sel)->nhits) {             \
                        iii++;                                                                               \
                        idx++;                                                                               \
                    }                                                                                        \
                    if (idx != jjj && idx < (_sel)->nhits) {                                                 \
                        memcpy(&(_sel)->coords[jjj * (_ndim)], &(_sel)->coords[idx * (_ndim)],               \
                               (_ndim) * sizeof(uint64_t));                                                  \
                    }                                                                                        \
                    jjj++;                                                                                   \
                }                                                                                            \
                if (iii > (_sel)->nhits)                                                                     \
                    printf("==PDC_SERVER[%d]: ERROR! invalidated more elements than total\n",                \
                           pdc_server_rank_g);                                                               \
                else                                                                                         \
                    ((_sel)->nhits) -= iii;                                                                  \
            }                                                                                                \
        }                                                                                                    \
    })

#ifdef ENABLE_FASTBIT
void
PDC_gen_fastbit_idx_name(char *out, char *prefix, uint64_t obj_id, int timestep, int ndim, uint64_t *start,
                         uint64_t *count)
{

    if (ndim == 1) {
        sprintf(out, "%s-%" PRIu64 "-%d-%" PRIu64 "-%" PRIu64 "", prefix, obj_id, timestep, start[0],
                count[0]);
    }
    else if (ndim == 2) {
        sprintf(out, "%s-%" PRIu64 "-%d-%" PRIu64 "-%" PRIu64 "-%" PRIu64 "-%" PRIu64 "", prefix, obj_id,
                timestep, start[0], start[1], count[0], count[1]);
    }
    else if (ndim == 3) {
        sprintf(out,
                "%s-%" PRIu64 "-%d-%" PRIu64 "-%" PRIu64 "-%" PRIu64 "-%" PRIu64 "-%" PRIu64 "-%" PRIu64 "",
                prefix, obj_id, timestep, start[0], start[1], start[2], count[0], count[1], count[2]);
    }
}

perr_t
generate_write_fastbit_idx(uint64_t obj_id, void *data, uint64_t dataCount, FastBitDataType ft, int timestep,
                           int ndim, uint64_t *start, uint64_t *count)
{
    perr_t    ret_value = SUCCEED;
    long int  fastbitErr;
    char      bmsName[128];
    char      keyName[128];
    char      offName[128];
    char      out_name[256];
    double *  keys    = NULL;
    int64_t * offsets = NULL;
    uint32_t *bms     = NULL;
    uint64_t  nk = 0, no = 0, nb = 0;
    FILE *    fp;

    FUNC_ENTER(NULL);

    PDC_gen_fastbit_idx_name(bmsName, "bms", obj_id, timestep, ndim, start, count);
    PDC_gen_fastbit_idx_name(keyName, "key", obj_id, timestep, ndim, start, count);
    PDC_gen_fastbit_idx_name(offName, "off", obj_id, timestep, ndim, start, count);

    fastbitErr = fastbit_iapi_register_array(bmsName, ft, data, dataCount);
    if (fastbitErr < 0) {
        printf("==PDC_SERVER[%d]: %s - ERROR with fastbit_iapi_register_array\n", pdc_server_rank_g,
               __func__);
        ret_value = FAIL;
        goto done;
    }

    fastbitErr = fastbit_iapi_build_index(bmsName, (const char *)gBinningOption);
    if (fastbitErr < 0) {
        printf("==PDC_SERVER[%d]: %s - ERROR with fastbit_iapi_build_index\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    fastbitErr = fastbit_iapi_deconstruct_index(bmsName, &keys, &nk, &offsets, &no, &bms, &nb);
    if (fastbitErr < 0) {
        printf("==PDC_SERVER[%d]: %s - ERROR with fastbit_iapi_deconstruct_index\n", pdc_server_rank_g,
               __func__);
        ret_value = FAIL;
        goto done;
    }

    // Need to write out bms, key, and offset
    char  storage_location[256];
    char *data_path;
    char *user_specified_data_path = getenv("PDC_DATA_LOC");
    if (user_specified_data_path != NULL)
        data_path = user_specified_data_path;
    else {
        data_path = getenv("SCRATCH");
        if (data_path == NULL)
            data_path = ".";
    }

    sprintf(storage_location, "%s/fastbit_idx/%" PRIu64 "/", data_path, obj_id);
    PDC_mkdir(storage_location);

    sprintf(out_name, "%s/%s", storage_location, bmsName);
    fp = fopen(out_name, "w");
    if (fp == NULL) {
        printf("==PDC_SERVER[%d]: %s - unable to open file [%s]\n", pdc_server_rank_g, __func__, out_name);
        goto done;
    }
    fwrite(bms, nb, sizeof(uint32_t), fp);
    fclose(fp);

    sprintf(out_name, "%s/%s", storage_location, keyName);
    fp = fopen(out_name, "w");
    if (fp == NULL) {
        printf("==PDC_SERVER[%d]: %s - unable to open file [%s]\n", pdc_server_rank_g, __func__, out_name);
        goto done;
    }
    fwrite(keys, nk, sizeof(double), fp);
    fclose(fp);

    sprintf(out_name, "%s/%s", storage_location, offName);
    fp = fopen(out_name, "w");
    if (fp == NULL) {
        printf("==PDC_SERVER[%d]: %s - unable to open file [%s]\n", pdc_server_rank_g, __func__, out_name);
        goto done;
    }
    fwrite(offsets, no, sizeof(int64_t), fp);
    fclose(fp);

done:
    if (bms)
        free(bms);
    if (keys)
        free(keys);
    if (offsets)
        free(offsets);
    fastbit_iapi_free_all();
    FUNC_LEAVE(ret_value);
}

int
PDC_bmreader(void *ctx, uint64_t start, uint64_t count, uint32_t *buf)
{
    const uint32_t *bms = (uint32_t *)ctx + start;
    unsigned        j;
    for (j = 0; j < count; ++j) {
        buf[j] = bms[j];
    }

    return 0;
}

int
queryData(const char *name)
{
    uint64_t               nhits, i;
    uint64_t *             buf;
    double                 val1 = 0.0, val2 = 10.0;
    FastBitSelectionHandle sel1 = fastbit_selection_osr(name, FastBitCompareGreater, val1);
    FastBitSelectionHandle sel2 = fastbit_selection_osr(name, FastBitCompareLess, val2);
    FastBitSelectionHandle sel  = fastbit_selection_combine(sel1, FastBitCombineAnd, sel2);

    nhits = fastbit_selection_evaluate(sel);
    printf("Query has %" PRIu64 " hits\n", nhits);

    buf = (uint64_t *)calloc(nhits, sizeof(uint64_t));

    nhits = fastbit_selection_get_coordinates(sel, buf, nhits, 0);

    printf("Coordinates:\n");
    for (i = 0; i < nhits; i++) {
        printf(", %" PRIu64 "", buf[i]);
    }
    printf("\n");

    free(buf);

    fastbit_iapi_free_all();
    return 1;
}

int
PDC_load_fastbit_index(char *idx_name, uint64_t obj_id, FastBitDataType dtype, int timestep, uint64_t ndim,
                       uint64_t *dims, uint64_t *start, uint64_t *count, uint32_t **bms, double **keys,
                       int64_t **offsets)
{
    char     bmsName[128];
    char     keyName[128];
    char     offName[128];
    char     out_name[256];
    uint64_t nk = 0, no = 0, nb = 0, size;
    FILE *   fp;

    PDC_gen_fastbit_idx_name(bmsName, "bms", obj_id, timestep, ndim, start, count);
    PDC_gen_fastbit_idx_name(keyName, "key", obj_id, timestep, ndim, start, count);
    PDC_gen_fastbit_idx_name(offName, "off", obj_id, timestep, ndim, start, count);

    char  storage_location[256];
    char *data_path;
    char *user_specified_data_path = getenv("PDC_DATA_LOC");
    if (user_specified_data_path != NULL)
        data_path = user_specified_data_path;

    else {
        data_path = getenv("SCRATCH");
        if (data_path == NULL)
            data_path = ".";
    }

    sprintf(storage_location, "%s/fastbit_idx/%" PRIu64 "/", data_path, obj_id);

    sprintf(out_name, "%s/%s", storage_location, bmsName);
    fp = fopen(out_name, "r");
    if (fp == NULL) {
        printf("==PDC_SERVER[%d]: %s - ERROR opening file [%s]!\n", pdc_server_rank_g, __func__, out_name);
        return -1;
    }
    fseek(fp, 0, SEEK_END);
    size = ftell(fp);
    nb   = size / sizeof(uint32_t);
    *bms = (uint32_t *)calloc(nb, sizeof(uint32_t));
    fseek(fp, 0, SEEK_SET);
    fread(*bms, nb, sizeof(uint32_t), fp);
    fclose(fp);

    sprintf(out_name, "%s/%s", storage_location, keyName);
    fp = fopen(out_name, "r");
    if (fp == NULL) {
        printf("==PDC_SERVER[%d]: %s - ERROR opening file [%s]!\n", pdc_server_rank_g, __func__, out_name);
        return -1;
    }
    fseek(fp, 0, SEEK_END);
    size  = ftell(fp);
    nk    = size / sizeof(double);
    *keys = (double *)calloc(nk, sizeof(double));
    fseek(fp, 0, SEEK_SET);
    fread(*keys, nk, sizeof(double), fp);
    fclose(fp);

    sprintf(out_name, "%s/%s", storage_location, offName);
    fp = fopen(out_name, "r");
    if (fp == NULL) {
        printf("==PDC_SERVER[%d]: %s - ERROR opening file [%s]!\n", pdc_server_rank_g, __func__, out_name);
        return -1;
    }
    fseek(fp, 0, SEEK_END);
    size     = ftell(fp);
    no       = size / sizeof(int64_t);
    *offsets = (int64_t *)calloc(no, sizeof(int64_t));
    fseek(fp, 0, SEEK_SET);
    fread(*offsets, no, sizeof(int64_t), fp);
    fclose(fp);

    fastbit_iapi_register_array_index_only(idx_name, dtype, dims, ndim, *keys, nk, *offsets, no, *bms,
                                           PDC_bmreader);

    return 1;
}

perr_t
PDC_query_fastbit_idx(region_list_t *region, pdc_query_constraint_t *constraint, uint64_t *nhit,
                      uint64_t **coords)
{
    perr_t          ret_value = SUCCEED;
    FastBitDataType ft        = 0;
    uint64_t        type_size;
    uint32_t *      bms     = NULL;
    double *        keys    = NULL;
    int64_t *       offsets = NULL;
    int             timestep;
    double          v1, v2;
    size_t          i;
    uint64_t        start[DIM_MAX], count[DIM_MAX];
    pdc_var_type_t  dtype;

    FUNC_ENTER(NULL);

    if (region == NULL || constraint == NULL || coords == NULL || nhit == NULL) {
        printf("==PDC_SERVER[%d]: %s - ERROR with input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    dtype = constraint->type;
    if (PDC_INT == dtype)
        ft = FastBitDataTypeInt;
    else if (PDC_INT64 == dtype)
        ft = FastBitDataTypeLong;
    else if (PDC_UINT64 == dtype)
        ft = FastBitDataTypeULong;
    else if (PDC_UINT == dtype)
        ft = FastBitDataTypeUInt;
    else if (PDC_FLOAT == dtype)
        ft = FastBitDataTypeFloat;
    else if (PDC_DOUBLE == dtype)
        ft = FastBitDataTypeDouble;

    type_size = PDC_get_var_type_size(dtype);

    for (i = 0; i < region->ndim; i++) {
        start[i] = region->start[i] / type_size;
        count[i] = region->count[i] / type_size;
    }

    timestep = 0;
    char idx_name[128];
    PDC_gen_fastbit_idx_name(idx_name, "bms", region->obj_id, timestep, region->ndim, start, count);
    PDC_load_fastbit_index(idx_name, region->obj_id, ft, timestep, region->ndim, count, start, count, &bms,
                           &keys, &offsets);

    FastBitSelectionHandle sel = NULL, sel1, sel2;
    FastBitCompareType     ct1, ct2;

    switch (constraint->type) {
        case PDC_FLOAT:
            v1 = (double)(*((float *)&constraint->value));
            v2 = (double)(*((float *)&constraint->value2));
            break;
        case PDC_DOUBLE:
            v1 = *((double *)&constraint->value);
            v2 = *((double *)&constraint->value2);
            break;
        case PDC_INT:
            v1 = (double)(*((int *)&constraint->value));
            v2 = (double)(*((int *)&constraint->value2));
            break;
        case PDC_UINT:
            v1 = (double)(*((uint32_t *)&constraint->value));
            v2 = (double)(*((uint32_t *)&constraint->value2));
            break;
        case PDC_INT64:
            v1 = (double)(*((int64_t *)&constraint->value));
            v2 = (double)(*((int64_t *)&constraint->value2));
            break;
        case PDC_UINT64:
            v1 = (double)(*((uint64_t *)&constraint->value));
            v2 = (double)(*((uint64_t *)&constraint->value2));
            break;
        default:
            printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g, __func__);
            ret_value = FAIL;
            goto done;
    } // End switch

    if (constraint->op == PDC_GT)
        ct1 = FastBitCompareGreater;
    else if (constraint->op == PDC_GTE)
        ct1 = FastBitCompareGreaterEqual;
    else if (constraint->op == PDC_LT)
        ct1 = FastBitCompareLess;
    else if (constraint->op == PDC_LTE)
        ct1 = FastBitCompareLessEqual;

    if (constraint->is_range == 1) {
        if (constraint->op2 == PDC_GT)
            ct2 = FastBitCompareGreater;
        else if (constraint->op2 == PDC_GTE)
            ct2 = FastBitCompareGreaterEqual;
        if (constraint->op2 == PDC_LT)
            ct2 = FastBitCompareLess;
        else if (constraint->op2 == PDC_LTE)
            ct2 = FastBitCompareLessEqual;

        sel1 = fastbit_selection_osr(idx_name, ct1, v1);
        sel2 = fastbit_selection_osr(idx_name, ct2, v2);
        sel  = fastbit_selection_combine(sel1, FastBitCombineAnd, sel2);
    }
    else {
        sel = fastbit_selection_osr(idx_name, ct1, v1);
    }
    *nhit = fastbit_selection_evaluate(sel);
    if (*nhit > 0) {
        *coords = malloc(*nhit * sizeof(uint64_t));
        fastbit_selection_get_coordinates(sel, *coords, *nhit, 0);
    }

    if (bms)
        free(bms);
    if (keys)
        free(keys);
    if (offsets)
        free(offsets);
    if (sel)
        fastbit_selection_free(sel);
    fastbit_iapi_free_all();
done:
    if (ret_value == FAIL)
        *nhit = 0;
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_gen_fastbit_idx(region_list_t *region, pdc_var_type_t dtype)
{
    perr_t          ret_value = SUCCEED;
    int             i;
    FastBitDataType ft;
    uint64_t        dataCount, start[DIM_MAX], count[DIM_MAX], type_size;

    FUNC_ENTER(NULL);

    if (region == NULL || region->buf == NULL) {
        printf("==PDC_SERVER[%d]: %s - ERROR with input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (PDC_INT == dtype)
        ft = FastBitDataTypeInt;
    else if (PDC_INT64 == dtype)
        ft = FastBitDataTypeLong;
    else if (PDC_UINT64 == dtype)
        ft = FastBitDataTypeULong;
    else if (PDC_UINT == dtype)
        ft = FastBitDataTypeUInt;
    else if (PDC_FLOAT == dtype)
        ft = FastBitDataTypeFloat;
    else if (PDC_DOUBLE == dtype)
        ft = FastBitDataTypeDouble;

    type_size = PDC_get_var_type_size(dtype);
    dataCount = region->data_size / type_size;

    for (i = 0; i < region->ndim; i++) {
        count[i] = region->count[i] / type_size;
        start[i] = region->start[i] / type_size;
    }

    generate_write_fastbit_idx(region->obj_id, (void *)region->buf, dataCount, ft, 0, region->ndim, start,
                               count);

done:
    FUNC_LEAVE(ret_value);
}

#endif

static perr_t
PDC_Server_query_evaluate_merge_opt(pdc_query_t *query, query_task_t *task, pdc_query_t *left,
                                    pdc_query_combine_op_t combine_op)
{
    perr_t           ret_value = SUCCEED;
    region_list_t *  region_elt, *region_list_head, *cache_region, tmp_region, *region_constraint = NULL;
    pdc_selection_t *sel = query->sel;
    uint64_t         nelem;
    size_t           i, j, unit_size;
    pdc_query_op_t   op = PDC_QUERY_OR, lop = PDC_QUERY_OR, rop = PDC_QUERY_OR;
    float            flo = .0, fhi = .0;
    double           dlo = .0, dhi = .0;
    int              ilo = 0, ihi = 0, ndim, count = 0;
    uint32_t         ulo = 0, uhi = 0;
    int64_t          i64lo = 0, i64hi = 0;
    uint64_t         ui64lo = 0, ui64hi = 0;
    void *           value = NULL, *buf = NULL;
    int              n_eval_region = 0, can_skip, region_iter = 0;

    printf("==PDC_SERVER[%d]: %s - start query evaluation!\n", pdc_server_rank_g, __func__);
    fflush(stdout);

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    struct timeval pdc_timer_start1, pdc_timer_end1;
    gettimeofday(&pdc_timer_start, 0);
#endif

    // query is guarenteed to be non-leaf nodes
    if (query == NULL) {
        printf("==PDC_SERVER[%d]: %s - input query NULL!\n", pdc_server_rank_g, __func__);
        goto done;
    }

    // Need to go through each region for query evaluation, so get region head
    region_list_head = (region_list_t *)query->constraint->storage_region_list_head;
    if (NULL == region_list_head) {
        printf("==PDC_SERVER[%d]: %s - error with storage_region_list_head!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }
    unit_size = PDC_get_var_type_size(query->constraint->type);

    if (task->ndim <= 0 || task->ndim > 3)
        task->ndim = region_list_head->ndim;

    ndim = task->ndim;
    if (ndim <= 0 || ndim > 3) {
        printf("==PDC_SERVER[%d]: %s - error with ndim = %d!\n", pdc_server_rank_g, __func__, ndim);
        ret_value = FAIL;
        goto done;
    }

    // Calculate total number of elements of all regions
    if (task->total_elem == 0) {
        DL_FOREACH(region_list_head, region_elt)
        {
            task->total_elem += region_elt->data_size / unit_size;
        }
    }

    // No need to evaluate a query if a previous one has selected all and combining with OR
    if (sel->nhits == task->total_elem && combine_op == PDC_QUERY_OR) {
        goto done;
    }
    else if (sel->nhits == 0 && combine_op == PDC_QUERY_AND) {
        goto done;
    }

    // Set up region constraint if the query has one
    memset(&tmp_region, 0, sizeof(region_list_t));
    region_constraint = NULL;
    if (task->region_constraint->ndim > 0) {
        // Convert logical tmp_region to bytes
        PDC_region_list_t_deep_cp(task->region_constraint, &tmp_region);
        for (i = 0; i < task->region_constraint->ndim; i++) {
            tmp_region.start[i] *= unit_size;
            tmp_region.count[i] *= unit_size;
        }
        region_constraint = &tmp_region;
    }

    // Check if there is a range query that we can combine the evaluation
    if (query->constraint->is_range == 1) {
        /*
    switch (query->constraint->type) {

                    case PDC_FLOAT:
                        flo = *((float *)&query->constraint->value);
                        fhi = *((float *)&query->constraint->value2);
                        break;
                    case PDC_DOUBLE:
                        dlo = *((double *)&query->constraint->value);
                        dhi = *((double *)&query->constraint->value2);
                        break;
                    case PDC_INT:
                        ilo = *((int *)&query->constraint->value);
                        ihi = *((int *)&query->constraint->value2);
                        break;
                    case PDC_UINT:
                        ulo = *((uint32_t *)&query->constraint->value);
                        uhi = *((uint32_t *)&query->constraint->value2);
                        break;
                    case PDC_INT64:
                        i64lo = *((int64_t *)&query->constraint->value);
                        i64hi = *((int64_t *)&query->constraint->value2);
                        break;
                    case PDC_UINT64:
                        ui64lo = *((uint64_t *)&query->constraint->value);
                        ui64hi = *((uint64_t *)&query->constraint->value2);
                        break;
                    default:
                        printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g,
           __func__); ret_value = FAIL; goto done; } // End switch
        */
        switch (query->constraint->type) {
            case PDC_FLOAT:
                flo = (float)query->constraint->value;
                fhi = (float)query->constraint->value2;
                break;
            case PDC_DOUBLE:
                dlo = (double)query->constraint->value;
                dhi = (double)query->constraint->value2;
                break;
            case PDC_INT:
                ilo = (int)query->constraint->value;
                ihi = (int)query->constraint->value2;
                break;
            case PDC_UINT:
                ulo = (uint32_t)query->constraint->value;
                uhi = (uint32_t)query->constraint->value2;
                break;
            case PDC_INT64:
                i64lo = (int64_t)query->constraint->value;
                i64hi = (int64_t)query->constraint->value2;
                break;
            case PDC_UINT64:
                ui64lo = (uint64_t)query->constraint->value;
                ui64hi = (uint64_t)query->constraint->value2;
                break;
            default:
                printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g, __func__);
                ret_value = FAIL;
                goto done;
        } // End switch

        lop = query->constraint->op;
        rop = query->constraint->op2;
    }
    else {
        op    = query->constraint->op;
        value = &(query->constraint->value);
    }

    DL_COUNT(region_list_head, region_elt, count);
    if (use_fastbit_idx_g == 1) {
#ifdef ENABLE_FASTBIT
        region_iter = -1;
        DL_FOREACH(region_list_head, region_elt)
        {
            region_iter++;

            if (combine_op == PDC_QUERY_AND && task->invalid_region_ids != NULL) {
                can_skip = 0;
                for (i = 0; i < task->ninvalid_region; i++) {
                    if (region_iter == task->invalid_region_ids[i]) {
                        can_skip = 1;
                        break;
                    }
                }
                if (can_skip == 1)
                    continue;
            }

            // Skip non-overlap regions with the region constraint

            if (region_constraint && region_constraint->ndim > 0) {
                if (PDC_is_contiguous_region_overlap(region_elt, region_constraint) != 1)
                    continue;
            }

            // Skip region based on histogram
            if (gen_hist_g == 1) {
                if (PDC_region_has_hits_from_hist(query->constraint, region_elt->region_hist) == 0) {
                    if (task->invalid_region_ids == NULL)
                        task->invalid_region_ids = (int *)calloc(count, sizeof(int));

                    can_skip = 0;
                    for (i = 0; i < task->ninvalid_region; i++) {
                        if (task->invalid_region_ids[i] == region_iter) {
                            can_skip = 1;
                            break;
                        }
                    }
                    if (can_skip == 0) {
                        task->invalid_region_ids[task->ninvalid_region] = region_iter;
                        task->ninvalid_region++;
                    }
                    continue;
                }
            }

            uint64_t idx_nhits = 0, *idx_coords = NULL, tmp_coord[DIM_MAX];
            PDC_query_fastbit_idx(region_elt, query->constraint, &idx_nhits, &idx_coords);
            if (idx_nhits > region_elt->data_size / unit_size) {
                printf("==PDC_SERVER[%d]: %s - idx_nhits = %" PRIu64 " may be too large!\n",
                       pdc_server_rank_g, __func__, idx_nhits);
            }

            if (idx_nhits > 0) {
                if ((idx_nhits + sel->nhits + 10) * ndim >= sel->coords_alloc) {
                    if (sel->coords_alloc < idx_nhits)
                        sel->coords_alloc = 2 * idx_nhits;
                    else
                        sel->coords_alloc *= 2;
                    sel->coords = (uint64_t *)realloc(sel->coords, sel->coords_alloc * sizeof(uint64_t));
                }
                for (iter = 0; iter < idx_nhits; iter++) {
                    if (ndim > 1)
                        region_index_to_coord(ndim, idx_coords[iter], region_elt->count, tmp_coord);
                    else
                        tmp_coord[0] = idx_coords[iter];
                    for (j = 0; j < ndim; j++) {
                        tmp = (sel->nhits + iter) * ndim + j;
                        if (tmp > sel->coords_alloc) {
                            printf("==PDC_SERVER[%d]: %s - coord array overflow %" PRIu64 "/ %" PRIu64 "!\n",
                                   pdc_server_rank_g, __func__, tmp, sel->coords_alloc);
                        }
                        else
                            sel->coords[tmp] = tmp_coord[j] + region_elt->start[j] / unit_size;
                    }
                }
                sel->nhits += idx_nhits;

                if (idx_coords)
                    free(idx_coords);
            }

            n_eval_region++;
        }
#endif
    } // End if use fastbit
    else {
        // Load data
        printf("==PDC_SERVER[%d]: %s - start loading data!\n", pdc_server_rank_g, __func__);
        fflush(stdout);
        PDC_Server_load_query_data(task, query, combine_op);

        region_iter = -1;
        DL_FOREACH(region_list_head, region_elt)
        {
            region_iter++;

            if (combine_op == PDC_QUERY_AND && task->invalid_region_ids != NULL) {
                can_skip = 0;
                for (i = 0; (int)i < task->ninvalid_region; i++) {
                    if (region_iter == task->invalid_region_ids[i]) {
                        can_skip = 1;
                        break;
                    }
                }
                if (can_skip == 1)
                    continue;
            }

            // Skip non-overlap regions with the region constraint

            if (region_constraint && region_constraint->ndim > 0) {
                if (PDC_is_contiguous_region_overlap(region_elt, region_constraint) != 1)
                    continue;
            }

            cache_region = region_elt;
            if (cache_region->io_cache_region != NULL)
                cache_region = cache_region->io_cache_region;

            // Skip regions that has no data (skipped at data load phase when we know it has no hits)
            if (cache_region->is_data_ready != 1)
                continue;

            // Skip region based on histogram
            if (gen_hist_g == 1) {
                if (PDC_region_has_hits_from_hist(query->constraint, region_elt->region_hist) == 0) {
                    if (task->invalid_region_ids == NULL)
                        task->invalid_region_ids = (int *)calloc(count, sizeof(int));

                    can_skip = 0;
                    for (i = 0; (int)i < task->ninvalid_region; i++) {
                        if (task->invalid_region_ids[i] == region_iter) {
                            can_skip = 1;
                            break;
                        }
                    }
                    if (can_skip == 0) {
                        task->invalid_region_ids[task->ninvalid_region] = region_iter;
                        task->ninvalid_region++;
                    }
                    continue;
                }
            }

#ifdef ENABLE_FASTBIT
            if (gen_fastbit_idx_g == 1) {
                PDC_gen_fastbit_idx(cache_region, query->constraint->type);
            }
#endif

            buf   = cache_region->buf;
            nelem = cache_region->count[0];
            for (i = 1; i < cache_region->ndim; i++)
                nelem *= cache_region->count[i];
            nelem /= unit_size;

            switch (query->constraint->type) {
                case PDC_FLOAT:
                    if (query->constraint->is_range == 1) {
                        MACRO_QUERY_RANGE_EVALUATE_SCAN_OPT(float, nelem, buf, lop, flo, rop, fhi, sel,
                                                            region_elt, unit_size, region_constraint,
                                                            combine_op);
                    }
                    else {
                        MACRO_QUERY_EVALUATE_SCAN_OPT(float, nelem, buf, op, value, sel, region_elt,
                                                      unit_size, region_constraint, combine_op);
                    }
                    break;
                case PDC_DOUBLE:
                    if (query->constraint->is_range == 1) {
                        MACRO_QUERY_RANGE_EVALUATE_SCAN_OPT(double, nelem, buf, lop, dlo, rop, dhi, sel,
                                                            region_elt, unit_size, region_constraint,
                                                            combine_op);
                    }
                    else {
                        MACRO_QUERY_EVALUATE_SCAN_OPT(double, nelem, buf, op, value, sel, region_elt,
                                                      unit_size, region_constraint, combine_op);
                    }
                    break;
                case PDC_INT:
                    if (query->constraint->is_range == 1) {
                        MACRO_QUERY_RANGE_EVALUATE_SCAN_OPT(int, nelem, buf, lop, ilo, rop, ihi, sel,
                                                            region_elt, unit_size, region_constraint,
                                                            combine_op);
                    }
                    else {
                        MACRO_QUERY_EVALUATE_SCAN_OPT(int, nelem, buf, op, value, sel, region_elt, unit_size,
                                                      region_constraint, combine_op);
                    }
                    break;
                case PDC_UINT:
                    if (query->constraint->is_range == 1) {
                        MACRO_QUERY_RANGE_EVALUATE_SCAN_OPT(uint32_t, nelem, buf, lop, ulo, rop, uhi, sel,
                                                            region_elt, unit_size, region_constraint,
                                                            combine_op);
                    }
                    else {
                        MACRO_QUERY_EVALUATE_SCAN_OPT(uint32_t, nelem, buf, op, value, sel, region_elt,
                                                      unit_size, region_constraint, combine_op);
                    }
                    break;
                case PDC_INT64:
                    if (query->constraint->is_range == 1) {
                        MACRO_QUERY_RANGE_EVALUATE_SCAN_OPT(int64_t, nelem, buf, lop, i64lo, rop, i64hi, sel,
                                                            region_elt, unit_size, region_constraint,
                                                            combine_op);
                    }
                    else {
                        MACRO_QUERY_EVALUATE_SCAN_OPT(int64_t, nelem, buf, op, value, sel, region_elt,
                                                      unit_size, region_constraint, combine_op);
                    }
                    break;
                case PDC_UINT64:
                    if (query->constraint->is_range == 1) {
                        MACRO_QUERY_RANGE_EVALUATE_SCAN_OPT(uint64_t, nelem, buf, lop, ui64lo, rop, ui64hi,
                                                            sel, region_elt, unit_size, region_constraint,
                                                            combine_op);
                    }
                    else {
                        MACRO_QUERY_EVALUATE_SCAN_OPT(uint64_t, nelem, buf, op, value, sel, region_elt,
                                                      unit_size, region_constraint, combine_op);
                    }
                    break;
                default:
                    printf("==PDC_SERVER[%d]: %s - error with operator type!\n", pdc_server_rank_g, __func__);
                    ret_value = FAIL;
                    goto done;
            } // End switch

            n_eval_region++;
        } // End DL_FOREACH
    }     // End not use fastbit

    if (n_eval_region == 0 && combine_op == PDC_QUERY_AND) {
        if (sel->nhits > 0) {
            sel->nhits = 0;
            free(sel->coords);
            sel->coords_alloc = 0;
            sel->coords       = NULL;
        }
    }

#ifdef ENABLE_TIMING
    if (pdc_server_rank_g == 0 || pdc_server_rank_g == 1)
        gettimeofday(&pdc_timer_start1, 0);
#endif

    // Remove duplicates
    if (combine_op == PDC_QUERY_OR && sel->nhits > 1 &&
        left->constraint->obj_id != query->constraint->obj_id) {
        if (ndim == 1) {
            qsort(sel->coords, sel->nhits, sizeof(uint64_t), compare_coords_1d);
        }
        else if (ndim == 2) {
            qsort(sel->coords, sel->nhits, sizeof(uint64_t) * 2, compare_coords_2d);
        }
        else if (ndim == 3) {
            qsort(sel->coords, sel->nhits, sizeof(uint64_t) * 3, compare_coords_3d);
        }

        j = 0;
        for (i = 0; i < sel->nhits - 1; i++) {
            if (memcmp(&sel->coords[i * ndim], &sel->coords[(i + 1) * ndim], ndim * sizeof(uint64_t)) != 0) {
                memcpy(&sel->coords[j * ndim], &sel->coords[i * ndim], ndim * sizeof(uint64_t));
                j++;
            }
        }
        memcpy(&sel->coords[j * ndim], &sel->coords[(sel->nhits - 1) * ndim], ndim * sizeof(uint64_t));
        j++;
        sel->nhits = j;
    }

#ifdef ENABLE_TIMING
    if (pdc_server_rank_g == 0 || pdc_server_rank_g == 1) {
        gettimeofday(&pdc_timer_end1, 0);
        double rm_dup_time = PDC_get_elapsed_time_double(&pdc_timer_start1, &pdc_timer_end1);
        printf("==PDC_SERVER[%d]: remove duplicate time %.4fs\n", pdc_server_rank_g, rm_dup_time);
    }
#endif

done:
#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    double query_eval_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("==PDC_SERVER[%d]: evaluated %d regions of %" PRIu64 ": %" PRIu64 "/ %" PRIu64
           " hits, time %.4fs\n",
           pdc_server_rank_g, n_eval_region, query->constraint->obj_id, sel->nhits, task->total_elem,
           query_eval_time);
#endif

    fflush(stdout);
    return ret_value;
}

void
PDC_query_visit_all_with_cb_arg(pdc_query_t *query, void (*func)(pdc_query_t *, void *), void *arg)
{
    if (NULL == query)
        return;

    func(query, arg);

    PDC_query_visit_all_with_cb_arg(query->left, func, arg);
    PDC_query_visit_all_with_cb_arg(query->right, func, arg);

    return;
}

void
PDC_query_visit(pdc_query_t *query,
                perr_t (*func)(pdc_query_t *, query_task_t *, pdc_query_t *, pdc_query_combine_op_t),
                query_task_t *arg, pdc_query_t *left, pdc_query_combine_op_t combine_op)
{
    if (NULL == query)
        return;

    PDC_query_visit(query->left, func, arg, left, combine_op);
    PDC_query_visit(query->right, func, arg, query->left, query->combine_op);

    if (NULL == query->left && NULL == query->right)
        func(query, arg, left, combine_op);

    return;
}

void
PDC_query_visit_leaf_with_cb_arg(pdc_query_t *query, void (*func)(pdc_query_t *, void *), void *arg)
{
    if (NULL == query)
        return;

    PDC_query_visit_leaf_with_cb_arg(query->left, func, arg);
    PDC_query_visit_leaf_with_cb_arg(query->right, func, arg);

    if (NULL == query->left && NULL == query->right)
        func(query, arg);

    return;
}

void
has_more_storage_region_to_recv(pdc_query_t *query, void *arg)
{
    int *has_more = (int *)arg;
    if (query == NULL || query->constraint == NULL) {
        return;
    }

    if (query->constraint->storage_region_list_head == NULL)
        *has_more = 1;
}

perr_t
attach_cache_storage_region_to_query(pdc_query_t *query)
{
    cache_storage_region_t *cache_region_elt;

    if (NULL == query->constraint) {
        printf("==PDC_SERVER[%d]: %s - query->constraint is NULL!\n", pdc_server_rank_g, __func__);
        return FAIL;
    }

    DL_FOREACH(cache_storage_region_head_g, cache_region_elt)
    {
        if (cache_region_elt->obj_id == query->constraint->obj_id) {
            query->constraint->storage_region_list_head = cache_region_elt->storage_region_head;
            break;
        }
    }

    return SUCCEED;
}

void
attach_sel_to_query(pdc_query_t *query, void *sel)
{
    if (query != NULL)
        query->sel = sel;
}

perr_t
attach_local_storage_region_to_query(pdc_query_t *query)
{
    pdc_metadata_t *meta;

    if (NULL == query->constraint) {
        printf("==PDC_SERVER[%d]: %s - query->constraint is NULL!\n", pdc_server_rank_g, __func__);
        return FAIL;
    }

    meta = PDC_Server_get_obj_metadata(query->constraint->obj_id);
    if (NULL == meta) {
        printf("==PDC_SERVER[%d]: %s - cannot find metadata %" PRIu64 "!\n", pdc_server_rank_g, __func__,
               query->constraint->obj_id);
        return FAIL;
    }

    query->constraint->storage_region_list_head = meta->storage_region_list_head;
    return SUCCEED;
}

static perr_t
PDC_Server_send_nhits_to_server(query_task_t *task)
{
    perr_t       ret_value = SUCCEED;
    hg_return_t  hg_ret;
    send_nhits_t in;
    hg_handle_t  handle;
    int          server_id;

    FUNC_ENTER(NULL);

    server_id = task->manager;
    if (server_id >= pdc_server_size_g) {
        printf("==PDC_SERVER[%d]: %s - server_id %d invalid!\n", pdc_server_rank_g, __func__, server_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - pdc_remote_server_info_g is NULL\n", pdc_server_rank_g,
                __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g[server_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_server_id(server_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - PDC_Server_lookup failed!\n", pdc_server_rank_g,
                    __func__);
            ret_value = FAIL;
            goto done;
        }
    }

    hg_ret =
        HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, send_nhits_register_id_g, &handle);
    if (hg_ret != HG_SUCCESS) {
        ret_value = FAIL;
        goto done;
    }

    in.query_id = task->query_id;
    in.nhits    = task->query->sel->nhits;

    hg_ret = HG_Forward(handle, PDC_check_int_ret_cb, NULL, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Server_send_nhits_to_client(query_task_t *task)
{
    perr_t      ret_value = SUCCEED;
    hg_return_t hg_ret;

    send_nhits_t in;
    hg_handle_t  handle;
    int          client_id;

    FUNC_ENTER(NULL);

    client_id = task->client_id;
    if (client_id >= pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: %s - client_id %d invalid!\n", pdc_server_rank_g, __func__, client_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - pdc_client_info_g is NULL\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g[client_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_client(client_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - PDC_Server_lookup_client failed!\n", pdc_server_rank_g,
                    __func__);
            ret_value = FAIL;
            goto done;
        }
    }

    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr, send_nhits_register_id_g, &handle);
    if (hg_ret != HG_SUCCESS) {
        ret_value = FAIL;
        goto done;
    }

    // Fill input structure
    in.nhits    = task->nhits;
    in.query_id = task->query_id;

    printf("==PDC_SERVER[%d]: %s - sending %" PRIu64 " nhits to client!\n", pdc_server_rank_g, __func__,
           in.nhits);

    fflush(stdout);

    hg_ret = HG_Forward(handle, PDC_check_int_ret_cb, NULL, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

hg_return_t
PDC_recv_nhits(const struct hg_cb_info *callback_info)
{
    hg_return_t   ret = HG_SUCCESS;
    send_nhits_t *in  = (send_nhits_t *)callback_info->arg;
    query_task_t *task_elt;

    DL_FOREACH(query_task_list_head_g, task_elt)
    {
        if (task_elt->query_id == in->query_id) {
            task_elt->nhits += in->nhits;
            task_elt->n_recv++;
            break;
        }
    }

    if (task_elt == NULL) {
        printf("==PDC_SERVER[%d]: %s - Invalid task ID!\n", pdc_server_rank_g, __func__);
        task_elt = query_task_list_head_g;
    }

    // When received all results from the working servers, send the aggregated result back to client
    if (task_elt && task_elt->n_recv >= task_elt->n_sent_server)
        PDC_Server_send_nhits_to_client(task_elt);

    free(in);

    return ret;
}

static perr_t
PDC_Server_send_coords_to_client(query_task_t *task)
{
    perr_t        ret_value = SUCCEED;
    hg_return_t   hg_ret;
    hg_handle_t   handle;
    hg_bulk_t     bulk_handle;
    bulk_rpc_in_t in;
    hg_size_t     buf_sizes;
    void *        buf;
    int           client_id;

    FUNC_ENTER(NULL);

    client_id = task->client_id;
    if (client_id >= pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: %s - client_id %d invalid!\n", pdc_server_rank_g, __func__, client_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - pdc_client_info_g is NULL\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g[client_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_client(client_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - PDC_Server_lookup_client failed!\n", pdc_server_rank_g,
                    __func__);
            ret_value = FAIL;
            goto done;
        }
    }

    if (pdc_server_size_g == 1) {
        buf       = task->query->sel->coords;
        buf_sizes = task->query->sel->nhits * sizeof(uint64_t) * task->ndim;
        in.ndim   = task->ndim;
        in.cnt    = task->query->sel->nhits;
    }
    else {
        buf       = task->coords;
        buf_sizes = task->nhits * sizeof(uint64_t) * task->ndim;
        in.ndim   = task->ndim;
        in.cnt    = task->nhits;
    }

    if (in.cnt > 0) {
        hg_ret = HG_Bulk_create(hg_class_g, 1, &buf, &buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
        /* printf("==PDC_SERVER[%d]: %s - created bulk handle %p!\n", pdc_server_rank_g, __func__,
         * bulk_handle); */
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not create bulk data handle\n");
            ret_value = FAIL;
            goto done;
        }
    }

    in.seq_id      = task->query_id;
    in.origin      = pdc_server_rank_g;
    in.op_id       = PDC_BULK_QUERY_COORDS;
    in.bulk_handle = bulk_handle;

    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr, send_bulk_rpc_register_id_g, &handle);
    if (hg_ret != HG_SUCCESS) {
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Forward(handle, PDC_check_int_ret_cb, NULL, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Server_send_coords_to_server(query_task_t *task)
{
    perr_t        ret_value = SUCCEED;
    hg_return_t   hg_ret;
    hg_handle_t   handle;
    hg_bulk_t     bulk_handle = NULL;
    bulk_rpc_in_t in;
    hg_size_t     buf_sizes;
    void *        buf;
    int           server_id;

    FUNC_ENTER(NULL);

    server_id = task->manager;
    if (server_id >= pdc_server_size_g) {
        printf("==PDC_SERVER[%d]: %s - server_id %d invalid!\n", pdc_server_rank_g, __func__, server_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - pdc_server_info_g is NULL\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g[server_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_server_id(server_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - PDC_Server_lookup_server_id failed!\n", pdc_server_rank_g,
                    __func__);
            ret_value = FAIL;
            goto done;
        }
    }

    if (task->query->sel->nhits > 0) {
        buf       = task->query->sel->coords;
        buf_sizes = task->query->sel->nhits * sizeof(uint64_t) * task->ndim;
        hg_ret    = HG_Bulk_create(hg_class_g, 1, &buf, &buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not create bulk data handle\n");
            ret_value = FAIL;
            goto done;
        }
    }

    // Fill input structure
    in.ndim        = task->ndim;
    in.cnt         = task->query->sel->nhits;
    in.seq_id      = task->query_id;
    in.origin      = pdc_server_rank_g;
    in.op_id       = PDC_BULK_QUERY_COORDS;
    in.bulk_handle = bulk_handle;

    hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, send_bulk_rpc_register_id_g,
                       &handle);
    if (hg_ret != HG_SUCCESS) {
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Forward(handle, PDC_check_int_ret_cb, NULL, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

pdc_query_constraint_t *
PDC_Server_get_constraint_from_query(pdc_query_t *query, uint64_t obj_id)
{
    pdc_query_constraint_t *constraint;
    if (query == NULL)
        return NULL;

    if (query->constraint && query->constraint->obj_id == obj_id)
        return query->constraint;

    constraint = PDC_Server_get_constraint_from_query(query->left, obj_id);
    if (constraint != NULL)
        return constraint;

    constraint = PDC_Server_get_constraint_from_query(query->left, obj_id);
    if (constraint != NULL)
        return constraint;

    return NULL;
}

uint64_t
coord_to_offset(size_t ndim, uint64_t *coord, uint64_t *start, uint64_t *count, size_t unit_size)
{
    uint64_t off = 0;

    if (ndim == 0 || coord == NULL || start == NULL || count == NULL) {
        printf("==PDC_SERVER[%d]: %s - input NULL!\n", pdc_server_rank_g, __func__);
        return -1;
    }

    if (ndim > 3) {
        printf("==PDC_SERVER[%d]: %s - cannot handle dim > 3!\n", pdc_server_rank_g, __func__);
        return 0;
    }

    if (ndim == 3)
        off = (coord[2] * unit_size - start[2]) * count[0] * count[1];
    if (ndim == 2)
        off += (coord[1] * unit_size - start[1]) * count[0];

    off += (coord[0] * unit_size - start[0]);

    return off;
}

int
is_coord_in_region(int ndim, uint64_t *coord, size_t unit_size, region_list_t *region)
{
    int i;
    if (ndim == 0 || coord == NULL || region == NULL) {
        printf("==PDC_SERVER[%d]: %s - input NULL!\n", pdc_server_rank_g, __func__);
        return -1;
    }

    for (i = 0; i < ndim; i++) {
        if (coord[i] * unit_size < region->start[i] ||
            coord[i] * unit_size > region->start[i] + region->count[i]) {
            return -1;
        }
    }
    return 1;
}

perr_t
PDC_send_data_to_client(int client_id, void *buf, size_t ndim, size_t unit_size, uint64_t count, int id,
                        int client_seq_id)
{
    perr_t        ret_value = SUCCEED;
    hg_return_t   hg_ret;
    hg_handle_t   handle;
    hg_bulk_t     bulk_handle;
    bulk_rpc_in_t in;
    hg_size_t     buf_sizes;

    FUNC_ENTER(NULL);

    if (client_id >= pdc_client_num_g) {
        printf("==PDC_SERVER[%d]: %s - client_id %d invalid!\n", pdc_server_rank_g, __func__, client_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - pdc_client_info_g is NULL\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_client_info_g[client_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_client(client_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - PDC_Server_lookup_client failed!\n", pdc_server_rank_g,
                    __func__);
            ret_value = FAIL;
            goto done;
        }
    }

    buf_sizes = count * unit_size;

    if (buf != NULL && buf_sizes != 0) {
        hg_ret = HG_Bulk_create(hg_class_g, 1, &buf, &buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
        /* printf("==PDC_SERVER[%d]: %s - created bulk handle %p!\n", pdc_server_rank_g, __func__,
         * bulk_handle); */
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "Could not create bulk data handle\n");
            ret_value = FAIL;
            goto done;
        }
    }

    in.ndim        = ndim;
    in.cnt         = count;
    in.seq_id2     = client_seq_id;
    in.seq_id      = id;
    in.origin      = pdc_server_rank_g;
    in.op_id       = PDC_BULK_SEND_QUERY_DATA;
    in.bulk_handle = bulk_handle;

    hg_ret = HG_Create(hg_context_g, pdc_client_info_g[client_id].addr, send_bulk_rpc_register_id_g, &handle);
    if (hg_ret != HG_SUCCESS) {
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Forward(handle, PDC_check_int_ret_cb, NULL, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

// Receive coords from other servers
hg_return_t
PDC_Server_read_coords(const struct hg_cb_info *callback_info)
{
    hg_return_t             ret  = HG_SUCCESS;
    query_task_t *          task = (query_task_t *)callback_info->arg;
    pdc_query_constraint_t *constraint;
    region_list_t *         storage_region_head, *region_elt, *cache_region;
    size_t                  ndim, unit_size;
    uint64_t                i, *coord, data_off, buf_off, my_size;

    // We will read task->my_read_coords, from task->my_read_obj_id
    constraint = PDC_Server_get_constraint_from_query(task->query, task->my_read_obj_id);
    if (NULL != constraint) {
        storage_region_head = constraint->storage_region_list_head;
        ndim                = storage_region_head->ndim;
        unit_size           = PDC_get_var_type_size(constraint->type);
        my_size             = task->my_nread_coords * unit_size;
        task->my_data       = malloc(my_size);

        if (NULL == task->my_data) {
            printf("==PDC_SERVER[%d]: %s - error allocating %" PRIu64 " bytes for data read!\n",
                   pdc_server_rank_g, __func__, task->my_nread_coords * unit_size);
            goto done;
        }

        data_off = 0;
        for (i = 0; i < task->my_nread_coords; i++) {
            coord = &(task->my_read_coords[i * ndim]);
            DL_FOREACH(storage_region_head, region_elt)
            {
                if (is_coord_in_region(ndim, coord, unit_size, region_elt) == 1) {
                    buf_off = coord_to_offset(ndim, coord, region_elt->start, region_elt->count, unit_size);
                    cache_region = region_elt;
                    if (region_elt->io_cache_region != NULL)
                        cache_region = region_elt->io_cache_region;

                    if (cache_region->is_io_done != 1) {
                        PDC_Server_data_read_to_buf_1_region(cache_region);
                    }
                    /* float *tmp = cache_region->buf + buf_off; */
                    memcpy(task->my_data + data_off, cache_region->buf + buf_off, unit_size);
                    data_off += unit_size;
                    break;
                }
            }
        } // End for

        PDC_send_data_to_client(task->client_id, task->my_data, ndim, unit_size, task->my_nread_coords,
                                task->query_id, task->client_seq_id);
    }
    else {
        // Requested object is not part of query, need to find their storage data and then read from
        // storage

        printf("==PDC_SERVER[%d]: %s - Requested object is not here, need to find its storage data!\n",
               pdc_server_rank_g, __func__);
        goto done;
    }

done:
    fflush(stdout);

    return ret;
}

// Receive coords from other servers
hg_return_t
PDC_recv_read_coords(const struct hg_cb_info *callback_info)
{
    hg_return_t         ret               = HG_SUCCESS;
    hg_bulk_t           local_bulk_handle = callback_info->info.bulk.local_handle;
    struct bulk_args_t *bulk_args         = (struct bulk_args_t *)callback_info->arg;
    query_task_t *      task_elt          = NULL;
    uint64_t            nhits, obj_id;
    uint32_t            ndim;
    int                 query_id, origin;
    void *              buf;

    pdc_int_ret_t out;
    out.ret = 1;

    if (callback_info->ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Error in callback");
        ret     = HG_PROTOCOL_ERROR;
        out.ret = -1;
        goto done;
    }
    else {
        nhits    = bulk_args->cnt;
        ndim     = bulk_args->ndim;
        query_id = bulk_args->query_id;
        origin   = bulk_args->origin;
        obj_id   = bulk_args->obj_id;

        if (nhits == 0) {
            printf("==PDC_SERVER[%d]: %s - received 0 read coords!\n", pdc_server_rank_g, __func__);
            goto done;
        }
        if (nhits * ndim * sizeof(uint64_t) != bulk_args->nbytes) {
            printf("==PDC_SERVER[%d]: %s - receive buf size not expected %" PRIu64 " / %zu!\n",
                   pdc_server_rank_g, __func__, nhits * ndim * sizeof(uint64_t), bulk_args->nbytes);
        }

        ret = HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, (void **)&buf,
                             NULL, NULL);

        DL_FOREACH(query_task_list_head_g, task_elt)
        {
            if (task_elt->query_id == query_id) {

                task_elt->my_read_coords = (uint64_t *)malloc(bulk_args->nbytes);
                memcpy(task_elt->my_read_coords, buf, bulk_args->nbytes);

                task_elt->my_read_obj_id  = obj_id;
                task_elt->my_nread_coords = nhits;
                task_elt->client_seq_id   = bulk_args->client_seq_id;

                break;
            }
        }

        if (task_elt == NULL) {
            printf("==PDC_SERVER[%d]: %s - Invalid task ID %d!\n", pdc_server_rank_g, __func__, query_id);
            goto done;
        }
        fprintf(stderr, "==PDC_SERVER[%d]: received read coords from server %d!\n", pdc_server_rank_g,
                origin);
    } // End else

done:
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

    ret = HG_Respond(bulk_args->handle, PDC_Server_read_coords, task_elt, &out);
    if (ret != HG_SUCCESS)
        fprintf(stderr, "Could not respond\n");

    ret = HG_Destroy(bulk_args->handle);
    if (ret != HG_SUCCESS)
        fprintf(stderr, "Could not destroy handle\n");

    free(bulk_args);
    return ret;
}

// Receive coords from other servers
hg_return_t
PDC_recv_coords(const struct hg_cb_info *callback_info)
{
    hg_return_t         ret               = HG_SUCCESS;
    hg_bulk_t           local_bulk_handle = callback_info->info.bulk.local_handle;
    struct bulk_args_t *bulk_args         = (struct bulk_args_t *)callback_info->arg;
    query_task_t *      task_elt;
    uint64_t            nhits = 0, total_hits;
    size_t              ndim, unit_size;
    int                 i, query_id, origin, found_task;

    void *buf;

    pdc_int_ret_t out;
    out.ret = 1;

    if (callback_info->ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Error in callback");
        ret     = HG_PROTOCOL_ERROR;
        out.ret = -1;
        goto done;
    }
    else {
        nhits     = bulk_args->cnt;
        ndim      = bulk_args->ndim;
        query_id  = bulk_args->query_id;
        origin    = bulk_args->origin;
        unit_size = ndim * sizeof(uint64_t);

        if (nhits > 0) {
            if (nhits * unit_size * ndim != bulk_args->nbytes) {
                printf("==PDC_SERVER[%d]: %s - receive size is unexpected %" PRIu64 " / %" PRIu64 "!\n",
                       pdc_server_rank_g, __func__, (uint64_t)nhits * unit_size * ndim,
                       (uint64_t)bulk_args->nbytes);
            }

            ret = HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, (void **)&buf,
                                 NULL, NULL);
        }

        found_task = 0;
        DL_FOREACH(query_task_list_head_g, task_elt)
        {
            if (task_elt->query_id == query_id) {
                found_task = 1;
                break;
            }
        }
        if (found_task == 0) {
            // Need to create a task and insert to global list
            task_elt           = (query_task_t *)calloc(1, sizeof(query_task_t));
            task_elt->query_id = query_id;

            DL_APPEND(query_task_list_head_g, task_elt);
        }

        if (NULL == task_elt->coords_arr)
            task_elt->coords_arr = (uint64_t **)calloc(pdc_server_size_g, sizeof(uint64_t *));
        if (NULL == task_elt->n_hits_from_server)
            task_elt->n_hits_from_server = (uint64_t *)calloc(pdc_server_size_g, sizeof(uint64_t));

        if (nhits > 0) {
            task_elt->coords_arr[origin] = (uint64_t *)malloc(bulk_args->nbytes);
            memcpy(task_elt->coords_arr[origin], buf, bulk_args->nbytes);
            task_elt->n_hits_from_server[origin] = nhits;
        }
        task_elt->nhits += nhits;
        task_elt->n_recv++;

        // When received all results from the working servers, send the aggregated result back to client
        if (task_elt->n_recv == task_elt->n_sent_server) {
            total_hits = 0;
            for (i = 0; i < pdc_server_size_g; i++)
                total_hits += task_elt->n_hits_from_server[i];

            if (total_hits > 0) {
                task_elt->coords = (uint64_t *)malloc(total_hits * unit_size);
                uint64_t off     = 0, size;
                for (i = 0; i < pdc_server_size_g; i++) {
                    if (task_elt->coords_arr[i] != NULL) {
                        size = task_elt->n_hits_from_server[i] * unit_size;
                        memcpy(task_elt->coords + off, task_elt->coords_arr[i], size);
                        free(task_elt->coords_arr[i]);
                        task_elt->coords_arr[i] = NULL;
                        off += task_elt->n_hits_from_server[i];
                    }
                }
                free(task_elt->coords_arr);
                task_elt->coords_arr = NULL;
            }

            printf("==PDC_SERVER[%d]: received all %d query results, send to client!\n", pdc_server_rank_g,
                   task_elt->n_recv);
            PDC_Server_send_coords_to_client(task_elt);
        }
    } // End else

done:
    fflush(stdout);
    if (nhits > 0) {
        ret = HG_Bulk_free(local_bulk_handle);
        if (ret != HG_SUCCESS) {
            fprintf(stderr, "Could not free HG bulk handle\n");
            return ret;
        }
    }

    ret = HG_Respond(bulk_args->handle, NULL, NULL, &out);
    if (ret != HG_SUCCESS)
        fprintf(stderr, "Could not respond\n");

    ret = HG_Destroy(bulk_args->handle);
    if (ret != HG_SUCCESS)
        fprintf(stderr, "Could not destroy handle\n");

    free(bulk_args);

    return ret;
}

static perr_t
PDC_Server_send_query_result_to_client(query_task_t *task)
{
    perr_t ret_value = SUCCEED;

    if (task->get_op == PDC_QUERY_GET_NHITS) {
        ret_value = PDC_Server_send_nhits_to_client(task);
    }
    else if (task->get_op == PDC_QUERY_GET_SEL) {
        ret_value = PDC_Server_send_coords_to_client(task);
    }
    else if (task->get_op == PDC_QUERY_GET_DATA) {
    }
    else {
        printf("==PDC_SERVER[%d]: %s - Invalid get_op type!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (ret_value != SUCCEED) {
        printf("==PDC_SERVER[%d]: %s - error sending query result to client!\n", pdc_server_rank_g, __func__);
        goto done;
    }

done:
    return ret_value;
}

static perr_t
PDC_Server_send_query_result_to_manager(query_task_t *task)
{
    perr_t ret_value = SUCCEED;

    if (task->get_op == PDC_QUERY_GET_NHITS) {
        ret_value = PDC_Server_send_nhits_to_server(task);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - error with PDC_Server_send_nhits_to_server!\n", pdc_server_rank_g,
                   __func__);
            goto done;
        }
    }
    else if (task->get_op == PDC_QUERY_GET_SEL) {
        ret_value = PDC_Server_send_coords_to_server(task);
        if (ret_value != SUCCEED) {
            printf("==PDC_SERVER[%d]: %s - error with PDC_Server_send_coords_to_server!\n", pdc_server_rank_g,
                   __func__);
            goto done;
        }
    }
    else {
        printf("==PDC_SERVER[%d]: %s - Invalid get_op type!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    // TODO: free the task_list at close time

    printf("==PDC_SERVER[%d]: sent query results to manager %d!\n", pdc_server_rank_g, task->manager);
done:
    fflush(stdout);

    return ret_value;
}

perr_t
PDC_Server_do_query(query_task_t *task)
{
    perr_t ret_value = SUCCEED;

    if (task == NULL || task->is_done == 1) {
        goto done;
    }

#ifdef ENABLE_TIMING
    struct timeval pdc_timer_start, pdc_timer_end;
    gettimeofday(&pdc_timer_start, 0);
#endif

    // Evaluate query
    PDC_query_visit(task->query, PDC_Server_query_evaluate_merge_opt, task, NULL, PDC_QUERY_NONE);

    // No need to store the coords for nhits
    if (task->get_op == PDC_QUERY_GET_NHITS) {
        if (task->query && task->query->sel && task->query->sel->coords_alloc > 0 &&
            task->query->sel->coords) {
            free(task->query->sel->coords);
            task->query->sel->coords       = 0;
            task->query->sel->coords_alloc = 0;
        }
    }

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    double query_process_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("==PDC_SERVER[%d]: query processing time %.4fs\n", pdc_server_rank_g, query_process_time);
#endif

    task->is_done = 1;

done:
    return ret_value;
}

perr_t
add_storage_region_to_buf(void **in_buf, uint64_t *buf_alloc, uint64_t *buf_off, const region_list_t *region)
{
    perr_t   ret_value = SUCCEED;
    void *   buf       = *in_buf;
    uint64_t my_size, tmp_size;

    if (in_buf == NULL || *in_buf == NULL || region == NULL || buf_alloc == NULL || buf_off == NULL ||
        region->storage_location[0] == '\0') {
        printf("==PDC_SERVER[%d]: %s - ERROR! NULL input!\n", pdc_server_rank_g, __func__);
        goto done;
    }

    my_size = 4 + strlen(region->storage_location) + 1 + sizeof(region_info_transfer_t) + 20;
    if (region->region_hist != NULL) {
        my_size += (8 * (region->region_hist->nbin) * 3);
    }

    if (*buf_off + my_size + 1024 > *buf_alloc) {
        (*buf_alloc) *= 2;
        buf = realloc(buf, *buf_alloc);
    }

    int *loc_len = (int *)(buf + *buf_off);
    *loc_len     = strlen(region->storage_location) + 1;
    (*buf_off) += sizeof(int);

    char *storage_location = (char *)(buf + *buf_off);
    sprintf(storage_location, "%s", region->storage_location);
    (*buf_off) += (*loc_len);

    region_info_transfer_t *region_info = (region_info_transfer_t *)(buf + *buf_off);
    region_info->ndim                   = region->ndim;
    if (region->ndim >= 3) {
        region_info->start_2 = region->start[2];
        region_info->count_2 = region->count[2];
    }
    if (region->ndim >= 2) {
        region_info->start_1 = region->start[1];
        region_info->count_1 = region->count[1];
    }
    region_info->start_0 = region->start[0];
    region_info->count_0 = region->count[0];
    (*buf_off) += sizeof(region_info_transfer_t);

    uint64_t *offset = (uint64_t *)(buf + *buf_off);
    *offset          = region->offset;
    (*buf_off) += sizeof(uint64_t);

    uint64_t *size = (uint64_t *)(buf + *buf_off);
    *size          = region->data_size;
    (*buf_off) += sizeof(uint64_t);

    int *has_hist = (int *)(buf + *buf_off);
    if (region->region_hist != NULL) {
        *has_hist = 1;
        (*buf_off) += sizeof(int);

        pdc_histogram_t *hist = (pdc_histogram_t *)(buf + *buf_off);
        hist->dtype           = region->region_hist->dtype;
        hist->nbin            = region->region_hist->nbin;
        hist->incr            = region->region_hist->incr;
        (*buf_off) += sizeof(pdc_histogram_t);

        double *range = (double *)(buf + *buf_off);
        tmp_size      = region->region_hist->nbin * 2 * sizeof(double);
        memcpy(range, region->region_hist->range, tmp_size);
        (*buf_off) += tmp_size;

        uint64_t *bin = (uint64_t *)(buf + *buf_off);
        tmp_size      = region->region_hist->nbin * sizeof(uint64_t);
        memcpy(bin, region->region_hist->bin, tmp_size);
        (*buf_off) += tmp_size;
    }
    else {
        *has_hist = 0;
        (*buf_off) += sizeof(int);
    }

done:
    return ret_value;
}

perr_t
add_to_cache_storage_region(uint64_t obj_id, region_list_t *region)
{
    cache_storage_region_t *elt, *new_cache_region;
    int                     found = 0;

    DL_FOREACH(cache_storage_region_head_g, elt)
    {
        if (elt->obj_id == obj_id) {
            found = 1;
            break;
        }
    }

    if (found == 0) {
        new_cache_region         = (cache_storage_region_t *)calloc(sizeof(cache_storage_region_t), 1);
        new_cache_region->obj_id = obj_id;
        DL_PREPEND(cache_storage_region_head_g, new_cache_region);
        DL_PREPEND(new_cache_region->storage_region_head, region);
    }
    else {
        DL_PREPEND(elt->storage_region_head, region);
    }

    return SUCCEED;
}

perr_t
PDC_send_query_metadata_bulk(bulk_rpc_in_t *in, void *buf, uint64_t buf_sizes, int server_id)
{

    perr_t      ret_value = SUCCEED;
    hg_return_t hg_ret;
    hg_handle_t handle      = NULL;
    hg_bulk_t   bulk_handle = NULL;

    FUNC_ENTER(NULL);

    if (buf == NULL || buf_sizes == 0 || server_id < 0) {
        printf("==PDC_SERVER[%d]: %s - ERROR with input!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (server_id >= (int32_t)pdc_server_size_g) {
        printf("==PDC_SERVER[%d]: %s - server_id %d invalid!\n", pdc_server_rank_g, __func__, server_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - pdc_server_info_g is NULL\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g[server_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_server_id(server_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - PDC_Server_lookup_server_id failed!\n", pdc_server_rank_g,
                    __func__);
            ret_value = FAIL;
            goto done;
        }
    }

    hg_ret = HG_Bulk_create(hg_class_g, 1, &buf, &buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        ret_value = FAIL;
        goto done;
    }

    in->bulk_handle = bulk_handle;

    hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, send_bulk_rpc_register_id_g,
                       &handle);
    if (hg_ret != HG_SUCCESS) {
        ret_value = FAIL;
        goto done;
    }

    printf("==PDC_SERVER[%d]: %s - sending %" PRIu64 " meta to server %d!\n", pdc_server_rank_g, __func__,
           in->cnt, server_id);
    fflush(stdout);

    hg_ret = HG_Forward(handle, PDC_check_int_ret_cb, NULL, in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    if (handle)
        HG_Destroy(handle);
    FUNC_LEAVE(ret_value);
}

void
attach_storage_region_to_query(pdc_query_t *query, region_list_t *region)
{
    if (NULL == query)
        return;

    attach_storage_region_to_query(query->left, region);
    attach_storage_region_to_query(query->right, region);

    // Note: currently attaches the received region to all query constraints of the same object,
    //       and uses the same "prev" "next" pointers
    if (NULL == query->left && NULL == query->right && NULL != query->constraint) {
        if (query->constraint->obj_id == region->obj_id) {
            query->constraint->storage_region_list_head = region;
        }
    }
}

void
PDC_Server_distribute_query_storage_info(query_task_t *task, uint64_t obj_id, int *obj_idx, uint64_t *obj_ids,
                                         int op)
{
    pdc_metadata_t *meta = NULL;
    int             i, server_id, count, avg_count, nsent, nsent_server;
    region_list_t * elt, *new_region = NULL;
    void *          region_bulk_buf;
    uint64_t        buf_alloc = 0, buf_off = 0;
    bulk_rpc_in_t   header;

    // Check if a previous query condition has already send the storage regions
    // there can be multiple pass to the same object, we only need to send it once
    for (i = 0; i < *obj_idx; i++) {
        if (obj_id == obj_ids[i]) {
            goto done;
        }
    }

    meta = PDC_Server_get_obj_metadata(obj_id);
    if (NULL != meta) {
        task->ndim        = meta->ndim;
        obj_ids[*obj_idx] = obj_id;
        (*obj_idx)++;

        DL_COUNT(meta->storage_region_list_head, elt, count);
        if (task->n_sent_server == 0) {
            if (count >= pdc_server_size_g)
                task->n_sent_server = pdc_server_size_g - 1; // Exclude manager
            else
                task->n_sent_server = count;
        }

        if (meta->all_storage_region_distributed == 1)
            goto done;

        printf("==PDC_SERVER[%d]: found metadata for %" PRIu64 ", %d regions!\n", pdc_server_rank_g, obj_id,
               count);

        // Need to distribute storage metadata to other servers
        avg_count           = ceil((1.0 * count) / task->n_sent_server);
        nsent               = 0;
        nsent_server        = 0;
        server_id           = 0;
        task->n_sent_server = ceil((1.0 * count) / avg_count);

        buf_alloc       = 4096 * avg_count;
        region_bulk_buf = calloc(buf_alloc, 1);

        memset(&header, 0, sizeof(bulk_rpc_in_t));
        header.seq_id    = task->query_id;
        header.op_id     = op;
        header.origin    = pdc_server_rank_g;
        header.obj_id    = obj_id;
        header.data_type = meta->data_type;
        header.cnt       = avg_count;
        header.ndim      = meta->ndim;
        header.op_id     = PDC_BULK_QUERY_METADATA;

        // Iterate over all storage regions and send one by on
        DL_FOREACH(meta->storage_region_list_head, elt)
        {

            // Do not send to manager
            if (server_id == task->manager) {
                server_id++;
                server_id %= pdc_server_size_g;
            }

            if (nsent > count) {
                printf("==PDC_SERVER[%d]: ERROR sending more storage meta (%d) than expected (%d)!\n",
                       pdc_server_rank_g, nsent, count);
                fflush(stdout);
            }

            if (server_id == pdc_server_rank_g) {
                // If needs to send to self, make a copy of the storage metadata
                new_region = (region_list_t *)calloc(1, sizeof(region_list_t));
                PDC_region_list_t_deep_cp(elt, new_region);

                add_to_cache_storage_region(obj_id, new_region);
            }
            else {
                add_storage_region_to_buf(&region_bulk_buf, &buf_alloc, &buf_off, (const region_list_t *)elt);
            }

            nsent++;
            nsent_server++;

            if (nsent_server >= avg_count || nsent == count) {
                // Last server may get less than average
                if (nsent == count)
                    header.cnt = count - avg_count * (task->n_sent_server - 1);

                if (server_id != pdc_server_rank_g) {
                    PDC_send_query_metadata_bulk(&header, region_bulk_buf, buf_off, server_id);

                    // new buf for another server
                    buf_off         = 0;
                    region_bulk_buf = calloc(buf_alloc, 1);
                }
                else {
                    task->n_recv_obj++;
                    attach_storage_region_to_query(task->query, new_region);
                }

                nsent_server = 0;

                server_id++;
                server_id %= pdc_server_size_g;
            }
        } // End DL_FOREACH

        printf("==PDC_SERVER[%d]: distributed all storage meta of %" PRIu64 "!\n", pdc_server_rank_g, obj_id);
        fflush(stdout);
        meta->all_storage_region_distributed = 1;

    } // end if (NULL != meta)

done:
    fflush(stdout);
    return;
}

void
PDC_Server_distribute_query_workload(query_task_t *task, pdc_query_t *query, int *obj_idx, uint64_t *obj_ids,
                                     int op)
{
    // Postorder tranversal
    if (NULL != query) {
        PDC_Server_distribute_query_workload(task, query->left, obj_idx, obj_ids, op);
        PDC_Server_distribute_query_workload(task, query->right, obj_idx, obj_ids, op);

        // Iterate over storage regions and prepare for their distribution to other servers
        if (NULL == query->left && NULL == query->right)
            PDC_Server_distribute_query_storage_info(task, query->constraint->obj_id, obj_idx, obj_ids, op);
    }
    return;
}

hg_return_t
PDC_recv_query_metadata_bulk(const struct hg_cb_info *callback_info)
{
    hg_return_t             ret               = HG_SUCCESS;
    hg_bulk_t               local_bulk_handle = callback_info->info.bulk.local_handle;
    struct bulk_args_t *    bulk_args         = (struct bulk_args_t *)callback_info->arg;
    void *                  buf;
    region_list_t *         regions = NULL;
    int                     i, nregion, *loc_len_ptr, *has_hist_ptr, found_task;
    uint64_t                buf_off, *offset_ptr = NULL, *size_ptr = NULL;
    char *                  loc_ptr         = NULL;
    region_info_transfer_t *region_info_ptr = NULL;
    pdc_histogram_t *       hist_ptr        = NULL;
    query_task_t *          task_elt        = NULL;
    pdc_query_t *           query           = NULL;

    pdc_int_ret_t out;
    out.ret = 1;

    printf("==PDC_SERVER[%d]: %s - received %d query metadata from %d!\n", pdc_server_rank_g, __func__,
           bulk_args->cnt, bulk_args->origin);
    fflush(stdout);

    // TODO: test
    if (callback_info->ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Error in callback");
        ret     = HG_PROTOCOL_ERROR;
        out.ret = -1;
        goto done;
    }
    else {

        nregion = bulk_args->cnt;

        if (nregion <= 0) {
            printf("==PDC_SERVER[%d]: %s - ERROR! 0 query metadata received!\n", pdc_server_rank_g, __func__);
            goto done;
        }
        ret = HG_Bulk_access(local_bulk_handle, 0, bulk_args->nbytes, HG_BULK_READWRITE, 1, (void **)&buf,
                             NULL, NULL);

        regions = (region_list_t *)calloc(nregion, sizeof(region_list_t));

        buf_off = 0;
        for (i = 0; i < nregion; i++) {
            if (buf_off > bulk_args->nbytes) {
                printf("==PDC_SERVER[%d]: %s - ERROR! buf overflow %d! 1\n", pdc_server_rank_g, __func__, i);
                fflush(stdout);
            }
            loc_len_ptr = (int *)(buf + buf_off);
            buf_off += sizeof(int);

            loc_ptr = (char *)(buf + buf_off);
            strncpy(regions[i].storage_location, loc_ptr, *loc_len_ptr);
            buf_off += (*loc_len_ptr);

            region_info_ptr = (region_info_transfer_t *)(buf + buf_off);
            PDC_region_transfer_t_to_list_t(region_info_ptr, &regions[i]);
            buf_off += sizeof(region_info_transfer_t);

            offset_ptr        = (uint64_t *)(buf + buf_off);
            regions[i].offset = *offset_ptr;
            buf_off += sizeof(uint64_t);

            size_ptr             = (uint64_t *)(buf + buf_off);
            regions[i].data_size = *size_ptr;
            buf_off += sizeof(uint64_t);

            has_hist_ptr = (int *)(buf + buf_off);
            buf_off += sizeof(int);

            if (buf_off > bulk_args->nbytes) {
                printf("==PDC_SERVER[%d]: %s - ERROR! buf overflow %d!2\n", pdc_server_rank_g, __func__, i);
                fflush(stdout);
            }

            if (*has_hist_ptr == 1) {
                hist_ptr = (pdc_histogram_t *)(buf + buf_off);
                buf_off += sizeof(pdc_histogram_t);
                hist_ptr->range = (double *)(buf + buf_off);
                buf_off += (hist_ptr->nbin * 2 * sizeof(double));
                hist_ptr->bin = (uint64_t *)(buf + buf_off);
                buf_off += (hist_ptr->nbin * sizeof(uint64_t));

                regions[i].region_hist = (pdc_histogram_t *)calloc(1, sizeof(pdc_histogram_t));
                PDC_copy_hist(regions[i].region_hist, hist_ptr);

                if (regions[i].region_hist->nbin == 0 || regions[i].region_hist->nbin > 1000) {
                    printf("==PDC_SERVER[%d]: %s ERROR received hist nbin=%d\n", pdc_server_rank_g, __func__,
                           regions[i].region_hist->nbin);
                }
            }

            if (buf_off > bulk_args->nbytes) {
                printf("==PDC_SERVER[%d]: %s - ERROR! buf overflow %d! 3\n", pdc_server_rank_g, __func__, i);
                fflush(stdout);
            }
            regions[i].obj_id = bulk_args->obj_id;
            regions[i].ndim   = bulk_args->ndim;
        }

        if (buf_off > bulk_args->nbytes) {
            printf("==PDC_SERVER[%d]: %s - ERROR! buf overflow after!\n", pdc_server_rank_g, __func__);
            fflush(stdout);
        }

        found_task = 0;
        DL_FOREACH(query_task_list_head_g, task_elt)
        {
            if (task_elt->query_id == bulk_args->query_id) {
                found_task     = 1;
                task_elt->ndim = bulk_args->ndim;
                break;
            }
        }

        if (found_task == 0) {
            // Need to create a task and insert to global list
            task_elt           = (query_task_t *)calloc(1, sizeof(query_task_t));
            task_elt->query_id = bulk_args->query_id;
            task_elt->ndim     = bulk_args->ndim;

            DL_APPEND(query_task_list_head_g, task_elt);
        }

        query = task_elt->query;
        task_elt->n_recv_obj++;

        for (i = 0; i < nregion; i++)
            add_to_cache_storage_region(bulk_args->obj_id, &regions[i]);

        if (bulk_args->op == PDC_RECV_REGION_DO_READ) {
            goto done;
        }

        attach_storage_region_to_query(query, &regions[nregion - 1]);
    } // End else

done:
    ret = HG_Bulk_free(local_bulk_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free HG bulk handle\n");
        return ret;
    }

    ret = HG_Respond(bulk_args->handle, NULL, NULL, &out);
    if (ret != HG_SUCCESS)
        fprintf(stderr, "Could not respond\n");

    ret = HG_Destroy(bulk_args->handle);
    if (ret != HG_SUCCESS)
        fprintf(stderr, "Could not destroy handle\n");

    if (bulk_args->op == PDC_RECV_REGION_DO_READ)
        return ret;

    if (bulk_args->origin == task_elt->prev_server_id || task_elt->prev_server_id == -1) {
        uint64_t *obj_ids = (uint64_t *)calloc(pdc_server_size_g, sizeof(uint64_t));
        int       obj_idx = 0;
        PDC_Server_distribute_query_workload(task_elt, query, &obj_idx, obj_ids, PDC_RECV_REGION_DO_QUERY);
        free(obj_ids);
    }

    int has_more = 0;
    PDC_query_visit_leaf_with_cb_arg(query, has_more_storage_region_to_recv, &has_more);
    if (has_more == 0) {
        // Process query
        PDC_Server_do_query(task_elt);

        // Send the result to manager
        PDC_Server_send_query_result_to_manager(task_elt);
    }

    free(bulk_args);
    return ret;
}

hg_return_t
PDC_Server_recv_data_query(const struct hg_cb_info *callback_info)
{
    hg_return_t       ret        = HG_SUCCESS;
    pdc_query_xfer_t *query_xfer = (pdc_query_xfer_t *)callback_info->arg;
    int               obj_idx    = 0;
    uint64_t *        obj_ids;
    query_task_t *    new_task       = NULL, *task_elt;
    int               query_id_exist = 0;
    pdc_query_t *     query;

    query = PDC_deserialize_query(query_xfer);
    if (NULL == query) {
        printf("==PDC_SERVER[%d]: deserialize query FAILED!\n", pdc_server_rank_g);
        goto done;
    }

    query->sel               = (pdc_selection_t *)calloc(1, sizeof(pdc_selection_t));
    query->sel->nhits        = 0;
    query->sel->coords_alloc = 8192;
    query->sel->coords       = (uint64_t *)calloc(query->sel->coords_alloc, sizeof(uint64_t));
    if (NULL == query->sel->coords) {
        printf("==PDC_SERVER[%d]: %s - error with calloc!\n", pdc_server_rank_g, __func__);
        goto done;
    }

    // Attach the query sel to all query nodes
    PDC_query_visit_all_with_cb_arg(query, attach_sel_to_query, query->sel);

    // Add query to global task list
    DL_FOREACH(query_task_list_head_g, task_elt)
    {
        if (task_elt->query_id == query_xfer->query_id) {
            query_id_exist = 1;
            new_task       = task_elt;
            printf("==PDC_SERVER[%d]: %s - query id already exist!\n", pdc_server_rank_g, __func__);
            fflush(stdout);
            break;
        }
    }

    if (0 == query_id_exist) {
        new_task = (query_task_t *)calloc(1, sizeof(query_task_t));
        DL_APPEND(query_task_list_head_g, new_task);
    }

    new_task->query_id          = query_xfer->query_id;
    new_task->client_id         = query_xfer->client_id;
    new_task->manager           = query_xfer->manager;
    new_task->query             = query;
    new_task->n_unique_obj      = query_xfer->n_unique_obj;
    new_task->obj_ids           = (uint64_t *)calloc(query_xfer->n_unique_obj, sizeof(uint64_t));
    new_task->get_op            = query_xfer->get_op;
    new_task->region_constraint = (region_list_t *)query->region_constraint;
    new_task->next_server_id    = query_xfer->next_server_id;
    new_task->prev_server_id    = query_xfer->prev_server_id;

    if (is_debug_g == 1) {
        printf("==PDC_SERVER[%d]: %s - appended new query task %d to list head\n", pdc_server_rank_g,
               __func__, new_task->query_id);
    }

    // find metadata of all queried objects and distribute to other servers
    // Traverse the query tree and process the query constraints that has metadata stored in here
    if (pdc_server_size_g == 1) {

        // Attach local storage region to query
        PDC_query_visit_leaf_with_cb(query, attach_local_storage_region_to_query);

        PDC_Server_do_query(new_task);

        PDC_Server_send_query_result_to_client(new_task);
    }
    else {
        PDC_query_visit_leaf_with_cb(query, attach_cache_storage_region_to_query);

        // Manager distributes data first
        if (pdc_server_rank_g == new_task->manager) {
            obj_ids = (uint64_t *)calloc(query_xfer->n_unique_obj, sizeof(uint64_t));
            obj_idx = 0;
            PDC_Server_distribute_query_workload(new_task, query, &obj_idx, obj_ids,
                                                 PDC_RECV_REGION_DO_QUERY);
            free(obj_ids);
            goto done;
        }

        int has_more = 0;
        PDC_query_visit_leaf_with_cb_arg(query, has_more_storage_region_to_recv, &has_more);
        if (has_more == 0) {
            // Process query
            PDC_Server_do_query(new_task);

            // Send the result to manager
            PDC_Server_send_query_result_to_manager(new_task);
        }
    }

done:
    if (query_xfer)
        PDC_query_xfer_free(query_xfer);
    fflush(stdout);

    return ret;
}

perr_t
PDC_Server_send_read_coords_to_server(int server_id, uint64_t *coord, uint64_t ncoords, int ndim,
                                      int query_id, int client_seq_id, uint64_t obj_id, uint64_t total_hits)
{
    perr_t        ret_value = SUCCEED;
    hg_return_t   hg_ret;
    hg_handle_t   handle;
    hg_bulk_t     bulk_handle = NULL;
    bulk_rpc_in_t in;
    hg_size_t     buf_sizes;
    void *        buf;

    FUNC_ENTER(NULL);

    if (server_id >= pdc_server_size_g) {
        printf("==PDC_SERVER[%d]: %s - server_id %d invalid!\n", pdc_server_rank_g, __func__, server_id);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g == NULL) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - pdc_server_info_g is NULL\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_remote_server_info_g[server_id].addr_valid == 0) {
        ret_value = PDC_Server_lookup_server_id(server_id);
        if (ret_value != SUCCEED) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - PDC_Server_lookup_server_id failed!\n", pdc_server_rank_g,
                    __func__);
            ret_value = FAIL;
            goto done;
        }
    }

    buf       = coord;
    buf_sizes = ncoords * ndim * sizeof(uint64_t);
    hg_ret    = HG_Bulk_create(hg_class_g, 1, &buf, &buf_sizes, HG_BULK_READ_ONLY, &bulk_handle);
    /* printf("==PDC_SERVER[%d]: %s - created bulk handle %p!\n", pdc_server_rank_g, __func__,
     * bulk_handle);
     */
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not create bulk data handle\n");
        ret_value = FAIL;
        goto done;
    }

    // Fill input structure
    in.ndim        = ndim;
    in.cnt         = ncoords;
    in.seq_id      = query_id;
    in.seq_id2     = client_seq_id;
    in.obj_id      = obj_id;
    in.origin      = pdc_server_rank_g;
    in.op_id       = PDC_BULK_READ_COORDS;
    in.bulk_handle = bulk_handle;
    in.total       = total_hits;

    hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[server_id].addr, send_bulk_rpc_register_id_g,
                       &handle);
    if (hg_ret != HG_SUCCESS) {
        ret_value = FAIL;
        goto done;
    }

    hg_ret = HG_Forward(handle, PDC_check_int_ret_cb, NULL, &in);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
        ret_value = FAIL;
        goto done;
    }

done:
    HG_Destroy(handle);

    return ret_value;
}

static perr_t
PDC_Server_send_query_obj_read_to_all_server(query_task_t *task, uint64_t obj_id)
{
    hg_return_t           hg_ret;
    perr_t                ret_value = SUCCEED;
    int                   i;
    get_sel_data_rpc_in_t in;
    hg_handle_t           handle;

    in.obj_id   = obj_id;
    in.query_id = task->query_id;
    in.origin   = task->client_id;

    for (i = 0; i < pdc_server_size_g; i++) {
        // Skip myself and the manager
        if (i == pdc_server_rank_g || i == task->manager)
            continue;

        hg_ret = HG_Create(hg_context_g, pdc_remote_server_info_g[i].addr,
                           send_read_sel_obj_id_rpc_register_id_g, &handle);
        if (hg_ret != HG_SUCCESS) {
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Forward(handle, NULL, NULL, &in);
        if (hg_ret != HG_SUCCESS) {
            fprintf(stderr, "==PDC_SERVER[%d]: %s - HG_Forward failed!\n", pdc_server_rank_g, __func__);
            HG_Destroy(handle);
            ret_value = FAIL;
            goto done;
        }

        hg_ret = HG_Destroy(handle);
    }

done:
    fflush(stdout);

    return ret_value;
}

hg_return_t
PDC_Server_recv_read_sel_obj_data(const struct hg_cb_info *callback_info)
{
    hg_return_t             ret = HG_SUCCESS;
    get_sel_data_rpc_in_t * in  = (get_sel_data_rpc_in_t *)callback_info->arg;
    query_task_t *          task_elt, *task = NULL;
    uint64_t                nhits, *coord, *coords = NULL, obj_id, buf_off, my_size, data_off, i;
    size_t                  ndim, unit_size;
    cache_storage_region_t *cache_region_elt;
    region_list_t *         storage_region_head = NULL, *cache_region, *region_elt;
    pdc_var_type_t          data_type;

    // find task
    DL_FOREACH(query_task_list_head_g, task_elt)
    {
        if (task_elt->query_id == in->query_id) {
            task = task_elt;
            break;
        }
    }

    if (NULL == task) {
        printf("==PDC_SERVER[%d]: %s - cannot find task id = %d, obj = %" PRIu64 "\n", pdc_server_rank_g,
               __func__, in->query_id, in->obj_id);
        goto done;
    }
    coords = task->query->sel->coords;
    nhits  = task->query->sel->nhits;
    ndim   = task->ndim;
    obj_id = in->obj_id;

    // If I have not participated in previous query, just skip
    if (NULL == coords) {
        goto done;
    }

    // Get storage region
    DL_FOREACH(cache_storage_region_head_g, cache_region_elt)
    {
        if (cache_region_elt->obj_id == obj_id) {
            storage_region_head = cache_region_elt->storage_region_head;
            data_type           = (pdc_var_type_t)cache_region_elt->data_type;
            break;
        }
    }

    if (NULL == storage_region_head) {
        printf("==PDC_SERVER[%d]: %s - cannot find cached storage region query_id=%d, obj_id=%" PRIu64 "\n",
               pdc_server_rank_g, __func__, in->query_id, in->obj_id);
        goto done;
    }

    ndim          = storage_region_head->ndim;
    unit_size     = PDC_get_var_type_size(data_type);
    my_size       = nhits * unit_size;
    task->my_data = malloc(my_size);
    if (NULL == task->my_data) {
        printf("==PDC_SERVER[%d]: %s - error allocating %" PRIu64 " bytes for data read!\n",
               pdc_server_rank_g, __func__, nhits * unit_size);
        goto done;
    }

    // We will read task->coords, from obj_id
    data_off = 0;
    for (i = 0; i < nhits; i++) {
        coord = &(coords[i * ndim]);
        DL_FOREACH(storage_region_head, region_elt)
        {
            if (is_coord_in_region(ndim, coord, unit_size, region_elt) == 1) {
                buf_off      = coord_to_offset(ndim, coord, region_elt->start, region_elt->count, unit_size);
                cache_region = region_elt;
                if (region_elt->io_cache_region != NULL)
                    cache_region = region_elt->io_cache_region;
                if (cache_region->is_io_done != 1) {
                    PDC_Server_data_read_to_buf_1_region(cache_region);
                }
                memcpy(task->my_data + data_off, cache_region->buf + buf_off, unit_size);
                data_off += unit_size;
                break;
            }
        }
    } // End for

    // Send read data back to client
    PDC_send_data_to_client(task->client_id, task->my_data, ndim, unit_size, nhits, task->query_id,
                            pdc_server_rank_g);
done:
    fflush(stdout);

    return ret;
}

hg_return_t
PDC_Server_recv_get_sel_data(const struct hg_cb_info *callback_info)
{
    hg_return_t            ret = HG_SUCCESS;
    get_sel_data_rpc_in_t *in  = (get_sel_data_rpc_in_t *)callback_info->arg;
    query_task_t *         task_elt, *task = NULL;
    pdc_metadata_t *       meta;
    struct hg_cb_info      fake_callback_info;

    DL_FOREACH(query_task_list_head_g, task_elt)
    {
        if (task_elt->query_id == in->query_id) {
            task            = task_elt;
            task->client_id = in->origin;
            break;
        }
    }

    if (NULL == task) {
        printf("==PDC_SERVER[%d]: %s - cannot find query task id=%d\n", pdc_server_rank_g, __func__,
               in->query_id);
        goto done;
    }

    // Find metadata object
    meta = PDC_Server_get_obj_metadata(in->obj_id);
    if (NULL == meta) {
        printf("==PDC_SERVER[%d]: %s - cannot find metadata object id=%" PRIu64 "\n", pdc_server_rank_g,
               __func__, in->obj_id);
        goto done;
    }

    if (pdc_server_size_g != 1) {
        if (meta->all_storage_region_distributed != 1) {
            // Storage metadata have not been distributed
            uint64_t *obj_ids = (uint64_t *)calloc(pdc_server_size_g, sizeof(uint64_t));
            int       obj_idx = 0;
            PDC_Server_distribute_query_storage_info(task, in->obj_id, &obj_idx, obj_ids,
                                                     PDC_RECV_REGION_DO_READ);
            free(obj_ids);
        }
        // Send query id and obj_id to other servers, as they have cached the corresponding coords
        PDC_Server_send_query_obj_read_to_all_server(task, in->obj_id);

        // If I'm not manager, also needs to do the work
        if (pdc_server_rank_g != task->manager) {
            fake_callback_info.arg = in;
            fake_callback_info.ret = HG_SUCCESS;
            PDC_Server_recv_read_sel_obj_data((const struct hg_cb_info *)&fake_callback_info);
        }
    }
    else {
        // Only one server, go ahead and read myself
        PDC_Server_recv_read_sel_obj_data(&fake_callback_info);
    }

done:
    if (in)
        free(in);
    return ret;
}
