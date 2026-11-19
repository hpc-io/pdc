#ifndef PDC_SERVER_REGION_REQUEST_HANDLER_H
#define PDC_SERVER_REGION_REQUEST_HANDLER_H

#include "pdc_timing.h"

#include <errno.h>
#include <stdlib.h>

#if defined(ENABLE_MULTITHREAD) && !defined(PDC_SERVER_CACHE)
/* Cache-disabled transfers share open descriptors and per-object region lists. */
static pthread_mutex_t uncached_transfer_mutex_g = PTHREAD_MUTEX_INITIALIZER;
#endif

/* Only completed single-read responses may enter this one-slot cache. */
static pthread_mutex_t read_response_mutex_g = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t  read_response_once_g  = PTHREAD_ONCE_INIT;
static void *          read_response_buffer_g;
static size_t          read_response_bytes_g;
static size_t          read_response_limit_g = (size_t)256 * 1024 * 1024;

static void
PDC_read_response_cache_init(void)
{
    const char *value = getenv("PDC_SERVER_READ_BUFFER_CACHE_MAX_MB");
    if (value != NULL) {
        char *end;
        errno                 = 0;
        unsigned long long mb = strtoull(value, &end, 10);
        if (*value != '\0' && *value != '-' && *end == '\0' && errno == 0 && mb <= SIZE_MAX / (1024 * 1024))
            read_response_limit_g = (size_t)mb * 1024 * 1024;
        else
            LOG_WARNING("Invalid PDC_SERVER_READ_BUFFER_CACHE_MAX_MB; using default\n");
    }
}

static void *
PDC_read_response_buffer_get(size_t bytes)
{
    void *buffer = NULL, *discard = NULL;
    pthread_once(&read_response_once_g, PDC_read_response_cache_init);
    pthread_mutex_lock(&read_response_mutex_g);
    if (read_response_bytes_g == bytes) {
        buffer = read_response_buffer_g;
    }
    else {
        discard = read_response_buffer_g;
    }
    read_response_buffer_g = NULL;
    read_response_bytes_g  = 0;
    pthread_mutex_unlock(&read_response_mutex_g);
    PDC_free(discard);
    return buffer != NULL ? buffer : PDC_malloc(bytes);
}

static void
PDC_read_response_buffer_put(void *buffer, size_t bytes)
{
    pthread_mutex_lock(&read_response_mutex_g);
    if (buffer != NULL && bytes <= read_response_limit_g && read_response_buffer_g == NULL) {
        read_response_buffer_g = buffer;
        read_response_bytes_g  = bytes;
        buffer                 = NULL;
    }
    pthread_mutex_unlock(&read_response_mutex_g);
    PDC_free(buffer);
}

void
PDC_read_response_cache_finalize(void)
{
    pthread_mutex_lock(&read_response_mutex_g);
    void *buffer           = read_response_buffer_g;
    read_response_buffer_g = NULL;
    read_response_bytes_g  = 0;
    pthread_mutex_unlock(&read_response_mutex_g);
    PDC_free(buffer);
}

static void
transfer_request_all_read_cleanup(struct transfer_request_all_local_bulk_args *request)
{
    HG_Bulk_free(request->bulk_handle);
    PDC_free(request->data_buf);
    HG_Free_input(request->handle, &request->in);
    HG_Destroy(request->handle);
    PDC_free(request->transfer_request_id);
    PDC_free(request);
}

static void
transfer_request_all_read_finish(struct transfer_request_all_local_bulk_args *request, hg_return_t result)
{
    pthread_mutex_lock(&transfer_request_status_mutex);
    pdc_transfer_status_t status =
        result == HG_SUCCESS ? PDC_TRANSFER_STATUS_COMPLETE : PDC_TRANSFER_STATUS_FAILED;
    for (int i = 0; i < request->in.n_objs; ++i)
        PDC_finish_request(request->transfer_request_id[i], status);
    pthread_mutex_unlock(&transfer_request_status_mutex);
}

hg_return_t
transfer_request_all_bulk_transfer_read_cb2(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_all_local_bulk_args2 *response = info->arg;
    struct transfer_request_all_local_bulk_args * request  = response->request;
    hg_return_t                                   ret      = info->ret;

#ifdef PDC_TIMING
    double end = MPI_Wtime();
    pdc_server_timings->PDCreg_transfer_request_inner_read_all_bulk_rpc += end - response->start_time;
    pdc_timestamp_register(pdc_transfer_request_inner_read_all_bulk_timestamps, response->start_time, end);
#endif

    if (ret != HG_SUCCESS)
        LOG_ERROR("Batched read data push failed: %s\n", HG_Error_to_string(ret));

    transfer_request_all_read_finish(request, ret);

    HG_Bulk_free(response->bulk_handle);
    PDC_free(response->data_buf);
    transfer_request_all_read_cleanup(request);
    PDC_free(response);

    FUNC_LEAVE(ret);
}

#ifdef PDC_TIMING
/* Batch read tasks can update this counter and timestamp array concurrently. */
static pthread_mutex_t batch_read_timing_mutex_g = PTHREAD_MUTEX_INITIALIZER;
#endif

static hg_size_t
transfer_request_all_read_size(const transfer_request_all_data *data, int i)
{
    hg_size_t size = data->unit[i];
    for (int j = 0; j < data->remote_ndim[i]; ++j)
        size *= data->remote_length[i][j];
    return size;
}

static hg_return_t
transfer_request_all_read(struct transfer_request_all_local_bulk_args *request)
{
    FUNC_ENTER(NULL);

    struct transfer_request_all_local_bulk_args2 *response;
    const struct hg_info *                        handle_info = HG_Get_info(request->handle);
    transfer_request_all_data                     request_data;
    hg_return_t                                   ret;
    hg_size_t                                     total_mem_size = 0;
    char *                                        ptr;

    request_data.n_objs = request->in.n_objs;
    parse_bulk_data(request->data_buf, &request_data, PDC_READ);

    for (int i = 0; i < request_data.n_objs; ++i)
        total_mem_size += transfer_request_all_read_size(&request_data, i);

    response           = PDC_malloc(sizeof(*response));
    response->request  = request;
    response->data_buf = PDC_malloc(total_mem_size);
    ptr                = response->data_buf;

#ifndef PDC_SERVER_CACHE
#ifdef ENABLE_MULTITHREAD
    pthread_mutex_lock(&uncached_transfer_mutex_g);
#endif
    data_server_region_t **regions = PDC_malloc(sizeof(*regions) * request_data.n_objs);
    for (int i = 0; i < request_data.n_objs; ++i) {
        regions[i] = PDC_Server_get_obj_region(request_data.obj_id[i]);
        PDC_Server_register_obj_region_by_pointer(regions + i, request_data.obj_id[i], 1);
    }
#endif

    for (int i = 0; i < request_data.n_objs; ++i) {
        struct pdc_region_info remote_region = {.ndim   = request_data.remote_ndim[i],
                                                .offset = request_data.remote_offset[i],
                                                .size   = request_data.remote_length[i]};

#ifdef PDC_SERVER_CACHE
        PDC_transfer_request_data_read_from(request_data.obj_id[i], request_data.obj_ndim[i],
                                            request_data.obj_dims[i], &remote_region, ptr,
                                            request_data.unit[i]);
#else
        PDC_Server_transfer_request_io(request_data.obj_id[i], request_data.obj_ndim[i],
                                       request_data.obj_dims[i], &remote_region, ptr, request_data.unit[i],
                                       0);
#endif
        ptr += transfer_request_all_read_size(&request_data, i);
    }

#ifndef PDC_SERVER_CACHE
    for (int i = 0; i < request_data.n_objs; ++i)
        PDC_Server_unregister_obj_region_by_pointer(regions[i], 1);
    PDC_free(regions);
#ifdef ENABLE_MULTITHREAD
    pthread_mutex_unlock(&uncached_transfer_mutex_g);
#endif
#endif
    clean_write_bulk_data(&request_data);

#ifdef PDC_TIMING
    double end = MPI_Wtime();
    pthread_mutex_lock(&batch_read_timing_mutex_g);
    pdc_server_timings->PDCreg_transfer_request_start_all_read_bulk_rpc += end - request->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_all_read_bulk_timestamps, request->start_time, end);
    pthread_mutex_unlock(&batch_read_timing_mutex_g);
    response->start_time = end;
#endif

    ret = HG_Bulk_create(handle_info->hg_class, 1, &response->data_buf, &total_mem_size, HG_BULK_READWRITE,
                         &response->bulk_handle);
    if (ret == HG_SUCCESS) {
        /* From submission onward, the push completion owns the request and response. */
        ret = HG_Bulk_transfer(handle_info->context, transfer_request_all_bulk_transfer_read_cb2, response,
                               HG_BULK_PUSH, handle_info->addr, request->in.local_bulk_handle, 0,
                               response->bulk_handle, 0, total_mem_size, HG_OP_ID_IGNORE);
        if (ret == HG_SUCCESS)
            FUNC_LEAVE(ret);
        HG_Bulk_free(response->bulk_handle);
    }

    LOG_ERROR("Could not submit batched read data push: %s\n", HG_Error_to_string(ret));
    transfer_request_all_read_finish(request, ret);
    PDC_free(response->data_buf);
    transfer_request_all_read_cleanup(request);
    PDC_free(response);

    FUNC_LEAVE(ret);
}

#ifdef ENABLE_MULTITHREAD
static HG_THREAD_RETURN_TYPE
transfer_request_all_read_worker(void *arg)
{
    transfer_request_all_read(arg);
    return (hg_thread_ret_t)0;
}
#endif

hg_return_t
transfer_request_all_bulk_transfer_read_cb(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_all_local_bulk_args *request = info->arg;
    hg_return_t                                  ret     = info->ret;

    if (ret != HG_SUCCESS) {
        LOG_ERROR("Batched read metadata pull failed: %s\n", HG_Error_to_string(ret));
        transfer_request_all_read_finish(request, ret);
        transfer_request_all_read_cleanup(request);
        FUNC_LEAVE(ret);
    }

#ifdef ENABLE_MULTITHREAD
    /* The work item and the metadata it references live through the data push. */
    request->read_work.func = transfer_request_all_read_worker;
    request->read_work.args = request;
    if (hg_thread_pool_post(hg_test_thread_pool_g, &request->read_work) < 0) {
        /* Mercury can report a wakeup failure after queueing the work. */
        LOG_ERROR("Could not dispatch batched read work\n");
        ret = HG_OTHER_ERROR;
    }
#else
    ret = transfer_request_all_read(request);
#endif

    FUNC_LEAVE(ret);
}

hg_return_t
transfer_request_all_bulk_transfer_write_cb(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_all_local_bulk_args *local_bulk_args = info->arg;
    transfer_request_all_data                    request_data;
    hg_return_t                                  ret = HG_SUCCESS;
    struct pdc_region_info *                     remote_reg_info;
    int                                          i;
    char                                         cur_time[64];

    gettimeofday(&last_cache_activity_timeval_g, NULL);

#ifdef PDC_TIMING
    double end = MPI_Wtime(), start;
    pdc_server_timings->PDCreg_transfer_request_start_all_write_bulk_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_all_write_bulk_timestamps, local_bulk_args->start_time,
                           end);
    start = MPI_Wtime();
#endif

    remote_reg_info     = (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));
    request_data.n_objs = local_bulk_args->in.n_objs;
    parse_bulk_data(local_bulk_args->data_buf, &request_data, PDC_WRITE);
    // print_bulk_data(&request_data);

#ifndef PDC_SERVER_CACHE
#ifdef ENABLE_MULTITHREAD
    pthread_mutex_lock(&uncached_transfer_mutex_g);
#endif
    data_server_region_t **temp_ptrs =
        (data_server_region_t **)PDC_malloc(sizeof(data_server_region_t *) * request_data.n_objs);
    for (i = 0; i < request_data.n_objs; ++i) {
        temp_ptrs[i] = PDC_Server_get_obj_region(request_data.obj_id[i]);
        PDC_Server_register_obj_region_by_pointer(temp_ptrs + i, request_data.obj_id[i], 1);
    }
#endif

    for (i = 0; i < request_data.n_objs; ++i) {
        remote_reg_info->ndim   = request_data.remote_ndim[i];
        remote_reg_info->offset = request_data.remote_offset[i];
        remote_reg_info->size   = request_data.remote_length[i];
#ifdef PDC_SERVER_CACHE
        PDC_transfer_request_data_write_out(request_data.obj_id[i], request_data.obj_ndim[i],
                                            request_data.obj_dims[i], remote_reg_info,
                                            (void *)request_data.data_buf[i], request_data.unit[i]);
#else
        PDC_Server_transfer_request_io(request_data.obj_id[i], request_data.obj_ndim[i],
                                       request_data.obj_dims[i], remote_reg_info,
                                       (void *)request_data.data_buf[i], request_data.unit[i], 1);
#endif

#if 0
        uint64_t j;
        LOG_ERROR("server write array, offset = %lu, size = %lu:", request_data.remote_offset[i][0], request_data.remote_length[i][0]);
        for ( j = 0; j < remote_reg_info->size[0]; ++j ) {
            LOG_ERROR("%d,", *(int*)(request_data.data_buf[i] + sizeof(int) * j));
        }
        LOG_ERROR("\n");
#endif
        pthread_mutex_lock(&transfer_request_status_mutex);
        PDC_finish_request(local_bulk_args->transfer_request_id[i], PDC_TRANSFER_STATUS_COMPLETE);
        pthread_mutex_unlock(&transfer_request_status_mutex);
    }

#ifndef PDC_SERVER_CACHE
    for (i = 0; i < request_data.n_objs; ++i) {
        PDC_Server_unregister_obj_region_by_pointer(temp_ptrs[i], 1);
    }
    temp_ptrs = (data_server_region_t **)PDC_free(temp_ptrs);
#ifdef ENABLE_MULTITHREAD
    pthread_mutex_unlock(&uncached_transfer_mutex_g);
#endif
#endif

    clean_write_bulk_data(&request_data);
    local_bulk_args->transfer_request_id = (uint64_t *)PDC_free(local_bulk_args->transfer_request_id);
    local_bulk_args->data_buf            = (void *)PDC_free(local_bulk_args->data_buf);
    remote_reg_info                      = (struct pdc_region_info *)PDC_free(remote_reg_info);

    HG_Bulk_free(local_bulk_args->bulk_handle);

    HG_Free_input(local_bulk_args->handle, &(local_bulk_args->in));
    HG_Destroy(local_bulk_args->handle);

    local_bulk_args = (struct transfer_request_all_local_bulk_args *)PDC_free(local_bulk_args);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_server_timings->PDCreg_transfer_request_inner_write_all_bulk_rpc += end - start;
    pdc_timestamp_register(pdc_transfer_request_inner_write_all_bulk_timestamps, start, end);
#endif

    FUNC_LEAVE(ret);
}

hg_return_t
transfer_request_wait_all_bulk_transfer_cb(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_wait_all_local_bulk_args *local_bulk_args = info->arg;
    transfer_request_wait_all_out_t                   out;

    pdc_transfer_wait_t *wait   = PDC_calloc(1, sizeof(*wait));
    hg_return_t          ret    = HG_SUCCESS;
    const pdcid_t *      ids    = local_bulk_args->data_buf;
    hg_handle_t          handle = local_bulk_args->handle;
    int                  n_objs = local_bulk_args->in.n_objs;

    /* Free decoded input while this callback still owns the RPC handle. */
    HG_Free_input(handle, &local_bulk_args->in);
    wait->result = info->ret == HG_SUCCESS ? PDC_WAIT_SUCCESS : PDC_WAIT_RETRY;
    pthread_mutex_lock(&transfer_request_status_mutex);
    if (info->ret == HG_SUCCESS) {
        for (int i = 0; i < n_objs; ++i) {
            pdc_transfer_status_t status = PDC_check_request(ids[i], 1);
            if (status == PDC_TRANSFER_STATUS_PENDING && !PDC_try_finish_request(ids[i], handle, wait, 1))
                wait->result = PDC_WAIT_RETRY;
            else if (status == PDC_TRANSFER_STATUS_FAILED && wait->result == PDC_WAIT_SUCCESS)
                wait->result = PDC_WAIT_FAILED;
        }
    }
    int respond = wait->pending == 0;
    pthread_mutex_unlock(&transfer_request_status_mutex);

    if (respond) {
        out.ret = wait->result;
        PDC_free(wait);
        ret = HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
    }

    local_bulk_args->data_buf = (void *)PDC_free(local_bulk_args->data_buf);

    HG_Bulk_free(local_bulk_args->bulk_handle);

#ifdef PDC_TIMING
    double end = MPI_Wtime();

    pdc_server_timings->PDCreg_transfer_request_wait_all_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_wait_all_timestamps, local_bulk_args->start_time, end);
#endif

    local_bulk_args = (struct transfer_request_wait_all_local_bulk_args *)PDC_free(local_bulk_args);

    FUNC_LEAVE(ret);
}

hg_return_t
transfer_request_bulk_transfer_write_cb(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                              ret             = HG_SUCCESS;
    struct pdc_region_info *                 remote_reg_info;
    uint64_t                                 obj_dims[3];

    gettimeofday(&last_cache_activity_timeval_g, NULL);

#ifdef PDC_TIMING
    double end = MPI_Wtime(), start;
    pdc_server_timings->PDCreg_transfer_request_start_write_bulk_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_write_bulk_timestamps, local_bulk_args->start_time,
                           end);
    start = MPI_Wtime();
#endif

    remote_reg_info = (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));

    remote_reg_info->ndim   = (local_bulk_args->in.remote_region).ndim;
    remote_reg_info->offset = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));
    remote_reg_info->size   = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));

    PDC_copy_region_desc((local_bulk_args->in).remote_region.start, remote_reg_info->offset,
                         remote_reg_info->ndim, remote_reg_info->ndim);
    PDC_copy_region_desc((local_bulk_args->in).remote_region.count, remote_reg_info->size,
                         remote_reg_info->ndim, remote_reg_info->ndim);
    PDC_copy_region_desc((local_bulk_args->in).obj_dims, obj_dims, remote_reg_info->ndim,
                         remote_reg_info->ndim);
/*
    printf("Server transfer request at write branch, index 1 value = %d\n",
           *((int *)(local_bulk_args->data_buf + sizeof(int))));
*/
#ifdef PDC_SERVER_CACHE
    PDC_transfer_request_data_write_out(local_bulk_args->in.obj_id, local_bulk_args->in.obj_ndim, obj_dims,
                                        remote_reg_info, (void *)local_bulk_args->data_buf,
                                        local_bulk_args->in.remote_unit);
#else
#ifdef ENABLE_MULTITHREAD
    pthread_mutex_lock(&uncached_transfer_mutex_g);
#endif
    PDC_Server_transfer_request_io(local_bulk_args->in.obj_id, local_bulk_args->in.obj_ndim, obj_dims,
                                   remote_reg_info, (void *)local_bulk_args->data_buf,
                                   local_bulk_args->in.remote_unit, 1);
#ifdef ENABLE_MULTITHREAD
    pthread_mutex_unlock(&uncached_transfer_mutex_g);
#endif
#endif
    pthread_mutex_lock(&transfer_request_status_mutex);
    PDC_finish_request(local_bulk_args->transfer_request_id, PDC_TRANSFER_STATUS_COMPLETE);
    pthread_mutex_unlock(&transfer_request_status_mutex);
    local_bulk_args->data_buf = (void *)PDC_free(local_bulk_args->data_buf);
    remote_reg_info           = (struct pdc_region_info *)PDC_free(remote_reg_info);

    HG_Bulk_free(local_bulk_args->bulk_handle);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_server_timings->PDCreg_transfer_request_inner_write_bulk_rpc += end - start;
    pdc_timestamp_register(pdc_transfer_request_inner_write_bulk_timestamps, start, end);
#endif

    FUNC_LEAVE(ret);
}

hg_return_t
transfer_request_bulk_transfer_read_cb(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                              ret;

#ifdef PDC_TIMING
    double end = MPI_Wtime(), start;
    pdc_server_timings->PDCreg_transfer_request_start_read_bulk_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_read_bulk_timestamps, local_bulk_args->start_time, end);
    start = MPI_Wtime();
#endif

    gettimeofday(&last_cache_activity_timeval_g, NULL);

    pthread_mutex_lock(&transfer_request_status_mutex);
    PDC_finish_request(local_bulk_args->transfer_request_id, PDC_TRANSFER_STATUS_COMPLETE);
    pthread_mutex_unlock(&transfer_request_status_mutex);

    ret = HG_SUCCESS;

    HG_Bulk_free(local_bulk_args->bulk_handle);
    /* The response buffer is borrowed by Mercury until this completion callback. */
    PDC_read_response_buffer_put(local_bulk_args->data_buf, local_bulk_args->total_mem_size);
    local_bulk_args = PDC_free(local_bulk_args);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    pdc_server_timings->PDCreg_transfer_request_inner_read_bulk_rpc += end - start;
    pdc_timestamp_register(pdc_transfer_request_inner_read_bulk_timestamps, start, end);
#endif
    FUNC_LEAVE(ret);
}

/* static hg_return_t */
// transfer_request_status_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request_status, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                   ret_value = HG_SUCCESS;
    transfer_request_status_in_t  in;
    transfer_request_status_out_t out;

    HG_Get_input(handle, &in);

    pthread_mutex_lock(&transfer_request_status_mutex);
    out.status = PDC_check_request(in.transfer_request_id, 1);
    pthread_mutex_unlock(&transfer_request_status_mutex);
    out.ret   = 1;
    ret_value = HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// transfer_request_wait_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request_wait, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                 ret_value = HG_SUCCESS;
    transfer_request_wait_in_t  in;
    transfer_request_wait_out_t out;
    pdc_transfer_status_t       status;
    pdc_transfer_wait_t *       wait;

#ifdef PDC_TIMING
    double start = MPI_Wtime(), end;
#endif

    HG_Get_input(handle, &in);
    pdcid_t request_id  = in.transfer_request_id;
    int     acknowledge = in.access_type == PDC_WAIT_ACK_FAILURE;
    HG_Free_input(handle, &in);
    pthread_mutex_lock(&transfer_request_status_mutex);
    status       = PDC_check_request(request_id, !acknowledge);
    int deferred = 0;
    if (!acknowledge && status == PDC_TRANSFER_STATUS_PENDING) {
        wait         = PDC_calloc(1, sizeof(*wait));
        wait->result = PDC_WAIT_SUCCESS;
        deferred     = PDC_try_finish_request(request_id, handle, wait, 0);
        if (!deferred)
            PDC_free(wait);
    }
    pthread_mutex_unlock(&transfer_request_status_mutex);
    if (!deferred) {
        out.ret = PDC_WAIT_SUCCESS;
        if (!acknowledge && status == PDC_TRANSFER_STATUS_PENDING)
            out.ret = PDC_WAIT_RETRY;
        else if (!acknowledge && status == PDC_TRANSFER_STATUS_FAILED)
            out.ret = PDC_WAIT_FAILED;
        ret_value = HG_Respond(handle, NULL, NULL, &out);
        HG_Destroy(handle);
    }

#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (in.access_type == PDC_READ) {
        pdc_server_timings->PDCreg_transfer_request_wait_read_rpc += end - start;
        pdc_timestamp_register(pdc_transfer_request_wait_read_timestamps, start, end);
    }
    else {
        pdc_server_timings->PDCreg_transfer_request_wait_write_rpc += end - start;
        pdc_timestamp_register(pdc_transfer_request_wait_write_timestamps, start, end);
    }
#endif

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// transfer_request_wait_all_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request_wait_all, handle)
{
    FUNC_ENTER(NULL);

    struct transfer_request_wait_all_local_bulk_args *local_bulk_args;
    const struct hg_info *                            info;
    transfer_request_wait_all_in_t                    in;
    hg_return_t                                       ret_value = HG_SUCCESS;

    ret_value = HG_Get_input(handle, &in);
    if (ret_value != HG_SUCCESS) {
        HG_Destroy(handle);
        FUNC_LEAVE(ret_value);
    }
    info            = HG_Get_info(handle);
    local_bulk_args = PDC_malloc(sizeof(*local_bulk_args));
    if (local_bulk_args != NULL) {
        local_bulk_args->handle      = handle;
        local_bulk_args->bulk_handle = HG_BULK_NULL;
        local_bulk_args->data_buf    = PDC_malloc(in.total_buf_size);
        local_bulk_args->in          = in;
#ifdef PDC_TIMING
        local_bulk_args->start_time = MPI_Wtime();
#endif
        if (local_bulk_args->data_buf != NULL) {
            ret_value = HG_Bulk_create(info->hg_class, 1, &local_bulk_args->data_buf,
                                       &local_bulk_args->in.total_buf_size, HG_BULK_READWRITE,
                                       &local_bulk_args->bulk_handle);
            if (ret_value == HG_SUCCESS) {
                ret_value =
                    HG_Bulk_transfer(info->context, transfer_request_wait_all_bulk_transfer_cb,
                                     local_bulk_args, HG_BULK_PULL, info->addr, in.local_bulk_handle, 0,
                                     local_bulk_args->bulk_handle, 0, in.total_buf_size, HG_OP_ID_IGNORE);
                if (ret_value == HG_SUCCESS)
                    FUNC_LEAVE(ret_value);
                HG_Bulk_free(local_bulk_args->bulk_handle);
            }
            PDC_free(local_bulk_args->data_buf);
        }
        PDC_free(local_bulk_args);
    }

    transfer_request_wait_all_out_t out = {.ret = PDC_WAIT_RETRY};
    HG_Free_input(handle, &in);
    ret_value = HG_Respond(handle, NULL, NULL, &out);
    HG_Destroy(handle);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// transfer_request_all_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request_all, handle)
{
    FUNC_ENTER(NULL);

    struct transfer_request_all_local_bulk_args *local_bulk_args;
    const struct hg_info *                       info;
    transfer_request_all_in_t                    in;
    transfer_request_all_out_t                   out;
    hg_return_t                                  ret_value = HG_SUCCESS;
    int                                          i;
    char                                         cur_time[64];

#ifdef PDC_TIMING
    double start = MPI_Wtime(), end;
#endif

    HG_Get_input(handle, &in);

    gettimeofday(&last_cache_activity_timeval_g, NULL);

    info            = HG_Get_info(handle);
    local_bulk_args = (struct transfer_request_all_local_bulk_args *)PDC_malloc(
        sizeof(struct transfer_request_all_local_bulk_args));

    // Read will return to client in the first call back (after metadata for region request is received)
    local_bulk_args->handle              = handle;
    local_bulk_args->bulk_handle         = HG_BULK_NULL;
    local_bulk_args->data_buf            = PDC_malloc(in.total_buf_size);
    local_bulk_args->in                  = in;
    local_bulk_args->transfer_request_id = (uint64_t *)PDC_malloc(sizeof(uint64_t) * in.n_objs);

    // [Tang]TODO is this necessary?
    pthread_mutex_lock(&transfer_request_id_mutex);
    /* The response carries the first ID, so every batch must reserve one contiguous range. */
    for (i = 0; i < in.n_objs; ++i) {
        local_bulk_args->transfer_request_id[i] = PDC_transfer_request_id_register();
    }
    pthread_mutex_unlock(&transfer_request_id_mutex);

    pthread_mutex_lock(&transfer_request_status_mutex);
    // Metadata ID is in ascending order. We only need to return the first value, the client knows the size.
    for (i = 0; i < in.n_objs; ++i) {
        PDC_commit_request(local_bulk_args->transfer_request_id[i]);
    }
    out.metadata_id = local_bulk_args->transfer_request_id[0];
    pthread_mutex_unlock(&transfer_request_status_mutex);

#ifdef PDC_TIMING
    local_bulk_args->start_time = MPI_Wtime();
#endif
    if (in.access_type == PDC_WRITE) {
        // Write operation receives everything in the callback, so we can free the handle and respond to user
        // here.
        ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf),
                                   &(local_bulk_args->in.total_buf_size), HG_BULK_READWRITE,
                                   &(local_bulk_args->bulk_handle));

        ret_value =
            HG_Bulk_transfer(info->context, transfer_request_all_bulk_transfer_write_cb, local_bulk_args,
                             HG_BULK_PULL, info->addr, in.local_bulk_handle, 0, local_bulk_args->bulk_handle,
                             0, local_bulk_args->in.total_buf_size, HG_OP_ID_IGNORE);
    }
    else {
        /* Keep the handle live until the start reply is submitted, even if a read completes sooner. */
        HG_Ref_incr(handle);
        ret_value =
            HG_Bulk_create(info->hg_class, 1, &local_bulk_args->data_buf, &local_bulk_args->in.total_buf_size,
                           HG_BULK_READWRITE, &local_bulk_args->bulk_handle);
        if (ret_value == HG_SUCCESS) {
            ret_value = HG_Bulk_transfer(info->context, transfer_request_all_bulk_transfer_read_cb,
                                         local_bulk_args, HG_BULK_PULL, info->addr, in.local_bulk_handle, 0,
                                         local_bulk_args->bulk_handle, 0, local_bulk_args->in.total_buf_size,
                                         HG_OP_ID_IGNORE);
        }
        if (ret_value != HG_SUCCESS) {
            LOG_ERROR("Could not submit batched read metadata pull: %s\n", HG_Error_to_string(ret_value));
            /* A start failure has not published the request IDs. */
            pthread_mutex_lock(&transfer_request_status_mutex);
            for (i = 0; i < in.n_objs; ++i) {
                PDC_finish_request(local_bulk_args->transfer_request_id[i], PDC_TRANSFER_STATUS_COMPLETE);
                PDC_check_request(local_bulk_args->transfer_request_id[i], 0);
            }
            pthread_mutex_unlock(&transfer_request_status_mutex);
            transfer_request_all_read_cleanup(local_bulk_args);
            out.metadata_id = 0;
        }
    }

    out.ret   = in.access_type == PDC_WRITE || ret_value == HG_SUCCESS;
    ret_value = HG_Respond(handle, NULL, NULL, &out);
    if (in.access_type == PDC_READ)
        HG_Destroy(handle);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (in.access_type == PDC_READ) {
        pdc_server_timings->PDCreg_transfer_request_start_all_read_rpc += end - start;
        pdc_timestamp_register(pdc_transfer_request_start_all_read_timestamps, start, end);
    }
    else {
        pdc_server_timings->PDCreg_transfer_request_start_all_write_rpc += end - start;
        pdc_timestamp_register(pdc_transfer_request_start_all_write_timestamps, start, end);
    }
#endif

    FUNC_LEAVE(ret_value);
}

hg_return_t
transfer_request_metadata_query_bulk_transfer_cb(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_metadata_query_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                                             ret             = HG_SUCCESS;
    transfer_request_metadata_query_out_t                   out;

    out.query_id =
        transfer_request_metadata_query_parse(local_bulk_args->in.n_objs, (char *)local_bulk_args->data_buf,
                                              local_bulk_args->in.is_write, &(out.total_buf_size));
    local_bulk_args->data_buf = (void *)PDC_free(local_bulk_args->data_buf);
    out.ret                   = 1;
    ret                       = HG_Respond(local_bulk_args->handle, NULL, NULL, &out);
    HG_Bulk_free(local_bulk_args->bulk_handle);
    HG_Destroy(local_bulk_args->handle);

    FUNC_LEAVE(ret);
}

/* static hg_return_t */
// transfer_request_metadata_query_cb(hg_handle_t handle)

HG_TEST_RPC_CB(transfer_request_metadata_query, handle)
{
    FUNC_ENTER(NULL);

    struct transfer_request_metadata_query_local_bulk_args *local_bulk_args;
    const struct hg_info *                                  info;
    transfer_request_metadata_query_in_t                    in;

    hg_return_t ret_value = HG_SUCCESS;

    HG_Get_input(handle, &in);
    info            = HG_Get_info(handle);
    local_bulk_args = (struct transfer_request_metadata_query_local_bulk_args *)PDC_malloc(
        sizeof(struct transfer_request_metadata_query_local_bulk_args));

    local_bulk_args->data_buf = PDC_malloc(in.total_buf_size);
    local_bulk_args->in       = in;
    local_bulk_args->handle   = handle;
    ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf), &(in.total_buf_size),
                               HG_BULK_READWRITE, &(local_bulk_args->bulk_handle));
    if (ret_value != HG_SUCCESS) {
        LOG_ERROR("Error at HG_TEST_RPC_CB(transfer_request, handle)\n");
    }

    // This is the actual data transfer. When transfer is finished, we are heading our way to the function
    // transfer_request_bulk_transfer_cb.
    ret_value = HG_Bulk_transfer(info->context, transfer_request_metadata_query_bulk_transfer_cb,
                                 local_bulk_args, HG_BULK_PULL, info->addr, in.local_bulk_handle, 0,
                                 local_bulk_args->bulk_handle, 0, in.total_buf_size, HG_OP_ID_IGNORE);

    HG_Free_input(handle, &in);

    FUNC_LEAVE(ret_value);
}

hg_return_t
transfer_request_metadata_query2_bulk_transfer_cb(const struct hg_cb_info *info)
{
    FUNC_ENTER(NULL);

    struct transfer_request_metadata_query2_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                                              ret             = HG_SUCCESS;
    transfer_request_metadata_query2_out_t                   out;

    out.ret                   = 1;
    local_bulk_args->data_buf = (void *)PDC_free(local_bulk_args->data_buf);
    ret                       = HG_Respond(local_bulk_args->handle, NULL, NULL, &out);

    HG_Bulk_free(local_bulk_args->bulk_handle);
    HG_Destroy(local_bulk_args->handle);

    FUNC_LEAVE(ret);
}

/* static hg_return_t */
// transfer_request_metadata_query2_cb(hg_handle_t handle)

HG_TEST_RPC_CB(transfer_request_metadata_query2, handle)
{
    FUNC_ENTER(NULL);

    struct transfer_request_metadata_query2_local_bulk_args *local_bulk_args;
    const struct hg_info *                                   info;
    transfer_request_metadata_query2_in_t                    in;

    hg_return_t ret_value = HG_SUCCESS;

    HG_Get_input(handle, &in);

    info            = HG_Get_info(handle);
    local_bulk_args = (struct transfer_request_metadata_query2_local_bulk_args *)PDC_malloc(
        sizeof(struct transfer_request_metadata_query2_local_bulk_args));

    local_bulk_args->handle = handle;

    // Retrieve the data buffer to be sent to client
    transfer_request_metadata_query_lookup_query_buf(in.query_id, (char **)&(local_bulk_args->data_buf));
    ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf), &(in.total_buf_size),
                               HG_BULK_READWRITE, &(local_bulk_args->bulk_handle));
    if (ret_value != HG_SUCCESS) {
        LOG_ERROR("Error at HG_TEST_RPC_CB(transfer_request, handle)\n");
    }

    // This is the actual data transfer. When transfer is finished, we are heading our way to the function
    // transfer_request_bulk_transfer_cb.
    ret_value = HG_Bulk_transfer(info->context, transfer_request_metadata_query2_bulk_transfer_cb,
                                 local_bulk_args, HG_BULK_PUSH, info->addr, in.local_bulk_handle, 0,
                                 local_bulk_args->bulk_handle, 0, in.total_buf_size, HG_OP_ID_IGNORE);

    HG_Free_input(handle, &in);

    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */

// transfer_request_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request, handle)
{
    FUNC_ENTER(NULL);

    hg_return_t                              ret_value = HG_SUCCESS;
    transfer_request_in_t                    in;
    transfer_request_out_t                   out;
    struct transfer_request_local_bulk_args *local_bulk_args;
    size_t                                   total_mem_size;
    const struct hg_info *                   info;
    struct pdc_region_info *                 remote_reg_info;
    uint64_t                                 obj_dims[3];

#ifdef PDC_TIMING
    double start = MPI_Wtime(), end;
#endif

    gettimeofday(&last_cache_activity_timeval_g, NULL);

    HG_Get_input(handle, &in);

    info = HG_Get_info(handle);

    total_mem_size =
        PDC_get_region_desc_size_bytes(in.remote_region.count, in.remote_unit, in.remote_region.ndim);

    pthread_mutex_lock(&transfer_request_id_mutex);
    out.metadata_id = PDC_transfer_request_id_register();
    pthread_mutex_unlock(&transfer_request_id_mutex);
    pthread_mutex_lock(&transfer_request_status_mutex);
    PDC_commit_request(out.metadata_id);
    pthread_mutex_unlock(&transfer_request_status_mutex);

    local_bulk_args = (struct transfer_request_local_bulk_args *)PDC_malloc(
        sizeof(struct transfer_request_local_bulk_args));
    local_bulk_args->handle         = handle;
    local_bulk_args->total_mem_size = total_mem_size;
    local_bulk_args->data_buf = in.access_type == PDC_READ ? PDC_read_response_buffer_get(total_mem_size)
                                                           : PDC_malloc(total_mem_size);
    local_bulk_args->in                  = in;
    local_bulk_args->transfer_request_id = out.metadata_id;
#ifdef PDC_TIMING
    local_bulk_args->start_time = MPI_Wtime();
#endif
    out.ret   = 1;
    ret_value = HG_Respond(handle, NULL, NULL, &out);
    if (in.access_type == PDC_WRITE) {
        ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf),
                                   (const hg_size_t *)&(local_bulk_args->total_mem_size), HG_BULK_READWRITE,
                                   &(local_bulk_args->bulk_handle));
        if (ret_value != HG_SUCCESS) {
            LOG_ERROR("Error at HG_TEST_RPC_CB(transfer_request, handle)\n");
        }

        // This is the actual data transfer. When transfer is finished, we are heading our way to the function
        // transfer_request_bulk_transfer_cb.
        ret_value = HG_Bulk_transfer(info->context, transfer_request_bulk_transfer_write_cb, local_bulk_args,
                                     HG_BULK_PULL, info->addr, in.local_bulk_handle, 0,
                                     local_bulk_args->bulk_handle, 0, total_mem_size, HG_OP_ID_IGNORE);
    }
    else {
        remote_reg_info = (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));

        remote_reg_info->ndim   = (in.remote_region).ndim;
        remote_reg_info->offset = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));
        remote_reg_info->size   = (uint64_t *)PDC_malloc(remote_reg_info->ndim * sizeof(uint64_t));

        PDC_copy_region_desc((in.remote_region).start, remote_reg_info->offset, remote_reg_info->ndim,
                             remote_reg_info->ndim);
        PDC_copy_region_desc((in.remote_region).count, remote_reg_info->size, remote_reg_info->ndim,
                             remote_reg_info->ndim);
        PDC_copy_region_desc((local_bulk_args->in).obj_dims, obj_dims, remote_reg_info->ndim,
                             remote_reg_info->ndim);

#ifdef PDC_SERVER_CACHE
        PDC_transfer_request_data_read_from(in.obj_id, in.obj_ndim, obj_dims, remote_reg_info,
                                            (void *)local_bulk_args->data_buf, in.remote_unit);
#else
#ifdef ENABLE_MULTITHREAD
        pthread_mutex_lock(&uncached_transfer_mutex_g);
#endif
        PDC_Server_transfer_request_io(in.obj_id, in.obj_ndim, obj_dims, remote_reg_info,
                                       (void *)local_bulk_args->data_buf, in.remote_unit, 0);
#ifdef ENABLE_MULTITHREAD
        pthread_mutex_unlock(&uncached_transfer_mutex_g);
#endif
#endif
        ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf),
                                   (const hg_size_t *)&(local_bulk_args->total_mem_size), HG_BULK_READWRITE,
                                   &(local_bulk_args->bulk_handle));
        if (ret_value != HG_SUCCESS) {
            LOG_ERROR("Error at HG_TEST_RPC_CB(transfer_request, handle)\n");
        }

        // This is the actual data transfer. When transfer is finished, we are heading our way to the function
        // transfer_request_bulk_transfer_cb.
        ret_value = HG_Bulk_transfer(info->context, transfer_request_bulk_transfer_read_cb, local_bulk_args,
                                     HG_BULK_PUSH, info->addr, in.local_bulk_handle, 0,
                                     local_bulk_args->bulk_handle, 0, total_mem_size, HG_OP_ID_IGNORE);
        remote_reg_info->offset = PDC_free(remote_reg_info->offset);
        remote_reg_info->size   = PDC_free(remote_reg_info->size);
        remote_reg_info         = PDC_free(remote_reg_info);
    }
    if (ret_value != HG_SUCCESS) {
        LOG_ERROR("Error at HG_TEST_RPC_CB(transfer_request, handle)\n");
    }

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

#ifdef PDC_TIMING
    end = MPI_Wtime();
    if (in.access_type == PDC_READ) {
        pdc_server_timings->PDCreg_transfer_request_start_read_rpc += end - start;
        pdc_timestamp_register(pdc_transfer_request_start_read_timestamps, start, end);
    }
    else {
        pdc_server_timings->PDCreg_transfer_request_start_write_rpc += end - start;
        pdc_timestamp_register(pdc_transfer_request_start_write_timestamps, start, end);
    }
#endif

    FUNC_LEAVE(ret_value);
}

#endif