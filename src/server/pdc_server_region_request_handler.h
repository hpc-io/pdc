hg_return_t
transfer_request_all_bulk_transfer_read_cb2(const struct hg_cb_info *info)
{
    hg_return_t                                   ret              = HG_SUCCESS;
    struct transfer_request_all_local_bulk_args2 *local_bulk_args2 = info->arg;
    int                                           i;
    FUNC_ENTER(NULL);
#ifdef PDC_TIMING
    double end, start;
#endif

    // printf("entering transfer_request_all_bulk_transfer_read_cb2\n");
    pthread_mutex_lock(&transfer_request_status_mutex);
    for (i = 0; i < local_bulk_args2->request_data.n_objs; ++i) {
        PDC_finish_request(local_bulk_args2->transfer_request_id[i]);
    }
    pthread_mutex_unlock(&transfer_request_status_mutex);
    clean_write_bulk_data(&(local_bulk_args2->request_data));
    free(local_bulk_args2->data_buf);
    free(local_bulk_args2->transfer_request_id);
    HG_Bulk_free(local_bulk_args2->bulk_handle);
    HG_Destroy(local_bulk_args2->handle);
    free(local_bulk_args2);
    // printf("finishing transfer_request_all_bulk_transfer_read_cb2\n");

#ifdef PDC_TIMING
    // transfer_request_inner_read_all_bulk is purely for transferring read data from server to client.
    end   = MPI_Wtime();
    start = local_bulk_args2->start_time;
    pdc_server_timings->PDCreg_transfer_request_inner_read_all_bulk_rpc += end - start;
    pdc_timestamp_register(pdc_transfer_request_inner_read_all_bulk_timestamps, start, end);
#endif

    FUNC_LEAVE(ret);
}

hg_return_t
transfer_request_all_bulk_transfer_read_cb(const struct hg_cb_info *info)
{
    struct transfer_request_all_local_bulk_args2 *local_bulk_args2;
    struct transfer_request_all_local_bulk_args * local_bulk_args = info->arg;
    const struct hg_info *                        handle_info;
    transfer_request_all_data                     request_data;
    hg_return_t                                   ret = HG_SUCCESS;
    struct pdc_region_info *                      remote_reg_info;
    int                                           i, j;
    uint64_t                                      total_mem_size, mem_size;
    char *                                        ptr;

    FUNC_ENTER(NULL);

#ifdef PDC_TIMING
    double end;
#endif

    // printf("entering transfer_request_all_bulk_transfer_read_cb\n");
    handle_info         = HG_Get_info(local_bulk_args->handle);
    request_data.n_objs = local_bulk_args->in.n_objs;
    parse_bulk_data(local_bulk_args->data_buf, &request_data, PDC_READ);
    // print_bulk_data(&request_data);

    remote_reg_info = (struct pdc_region_info *)malloc(sizeof(struct pdc_region_info));
    total_mem_size  = 0;
    for (i = 0; i < request_data.n_objs; ++i) {
        mem_size = request_data.unit[i];
        for (j = 0; j < request_data.remote_ndim[i]; ++j) {
            mem_size *= request_data.remote_length[i][j];
        }
        total_mem_size += mem_size;
    }

    local_bulk_args2 = (struct transfer_request_all_local_bulk_args2 *)malloc(
        sizeof(struct transfer_request_all_local_bulk_args2));
    local_bulk_args2->data_buf = (char *)malloc(total_mem_size);
    ptr                        = local_bulk_args2->data_buf;

#ifndef PDC_SERVER_CACHE
    data_server_region_t **temp_ptrs =
        (data_server_region_t **)malloc(sizeof(data_server_region_t *) * request_data.n_objs);
    for (i = 0; i < request_data.n_objs; ++i) {
        temp_ptrs[i] = PDC_Server_get_obj_region(request_data.obj_id[i]);
        PDC_Server_register_obj_region_by_pointer(temp_ptrs + i, request_data.obj_id[i], 1);
    }
#endif
    for (i = 0; i < request_data.n_objs; ++i) {
        remote_reg_info->ndim   = request_data.remote_ndim[i];
        remote_reg_info->offset = request_data.remote_offset[i];
        remote_reg_info->size   = request_data.remote_length[i];

        mem_size = request_data.unit[i];
        for (j = 0; j < request_data.remote_ndim[i]; ++j) {
            mem_size *= request_data.remote_length[i][j];
        }

#ifdef PDC_SERVER_CACHE
        PDC_transfer_request_data_read_from(request_data.obj_id[i], request_data.obj_ndim[i],
                                            request_data.obj_dims[i], remote_reg_info, (void *)ptr,
                                            request_data.unit[i]);
#else
        PDC_Server_transfer_request_io(request_data.obj_id[i], request_data.obj_ndim[i],
                                       request_data.obj_dims[i], remote_reg_info, (void *)ptr,
                                       request_data.unit[i], 0);
#endif
        /*
                        fprintf(stderr, "server read array, offset = %lu, size = %lu:",
           request_data.remote_offset[i][0], request_data.remote_length[i][0]); uint64_t k; for ( k = 0; k <
           remote_reg_info->size[0]; ++k ) { fprintf(stderr, "%d,", *(int*)(ptr + sizeof(int) * k));
                        }
                        fprintf(stderr, "\n");
        */
        ptr += mem_size;
    }
#ifndef PDC_SERVER_CACHE
    for (i = 0; i < request_data.n_objs; ++i) {
        PDC_Server_unregister_obj_region_by_pointer(temp_ptrs[i], 1);
    }
    free(temp_ptrs);
#endif

#ifdef PDC_TIMING
    // PDCreg_transfer_request_wait_all_read_bulk includes the timing for transfering metadata and read I/O
    // time.
    end = MPI_Wtime();
    pdc_server_timings->PDCreg_transfer_request_start_all_read_bulk_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_all_read_bulk_timestamps, local_bulk_args->start_time,
                           end);

    local_bulk_args2->start_time = end;
#endif
    local_bulk_args2->handle              = local_bulk_args->handle;
    local_bulk_args2->transfer_request_id = local_bulk_args->transfer_request_id;
    local_bulk_args2->request_data        = request_data;

    ret = HG_Bulk_create(handle_info->hg_class, 1, &(local_bulk_args2->data_buf), &total_mem_size,
                         HG_BULK_READWRITE, &(local_bulk_args2->bulk_handle));
    if (ret != HG_SUCCESS) {
        printf("Error at transfer_request_all_bulk_transfer_read_cb(const struct hg_cb_info *info): @ line "
               "%d \n",
               __LINE__);
    }

    // This is the actual data transfer. When transfer is finished, we are heading our way to the function
    // transfer_request_bulk_transfer_cb.
    ret =
        HG_Bulk_transfer(handle_info->context, transfer_request_all_bulk_transfer_read_cb2, local_bulk_args2,
                         HG_BULK_PUSH, handle_info->addr, local_bulk_args->in.local_bulk_handle, 0,
                         local_bulk_args2->bulk_handle, 0, total_mem_size, HG_OP_ID_IGNORE);
    if (ret != HG_SUCCESS) {
        printf("Error at transfer_request_all_bulk_transfer_read_cb(const struct hg_cb_info *info): @ line "
               "%d \n",
               __LINE__);
    }
    // pointers in request_data are freed in the next call back function
    free(local_bulk_args->data_buf);
    free(remote_reg_info);

    HG_Bulk_free(local_bulk_args->bulk_handle);

    HG_Free_input(local_bulk_args->handle, &(local_bulk_args->in));

    free(local_bulk_args);

    FUNC_LEAVE(ret);
}

hg_return_t
transfer_request_all_bulk_transfer_write_cb(const struct hg_cb_info *info)
{
    struct transfer_request_all_local_bulk_args *local_bulk_args = info->arg;
    transfer_request_all_data                    request_data;
    hg_return_t                                  ret = HG_SUCCESS;
    struct pdc_region_info *                     remote_reg_info;
    int                                          i;

    FUNC_ENTER(NULL);

#ifdef PDC_TIMING
    double end = MPI_Wtime(), start;
    pdc_server_timings->PDCreg_transfer_request_start_all_write_bulk_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_all_write_bulk_timestamps, local_bulk_args->start_time,
                           end);
    start = MPI_Wtime();
#endif

    // printf("entering transfer_request_all_bulk_transfer_write_cb\n");
    remote_reg_info     = (struct pdc_region_info *)malloc(sizeof(struct pdc_region_info));
    request_data.n_objs = local_bulk_args->in.n_objs;
    parse_bulk_data(local_bulk_args->data_buf, &request_data, PDC_WRITE);
    // print_bulk_data(&request_data);
#ifndef PDC_SERVER_CACHE
    data_server_region_t **temp_ptrs =
        (data_server_region_t **)malloc(sizeof(data_server_region_t *) * request_data.n_objs);
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
        /*
                        uint64_t j;
                        fprintf(stderr, "server write array, offset = %lu, size = %lu:",
           request_data.remote_offset[i][0], request_data.remote_length[i][0]); for ( j = 0; j <
           remote_reg_info->size[0]; ++j ) { fprintf(stderr, "%d,", *(int*)(request_data.data_buf[i] +
           sizeof(int) * j));
                        }
                        fprintf(stderr, "\n");
        */
        pthread_mutex_lock(&transfer_request_status_mutex);
        PDC_finish_request(local_bulk_args->transfer_request_id[i]);
        pthread_mutex_unlock(&transfer_request_status_mutex);
    }
#ifndef PDC_SERVER_CACHE
    for (i = 0; i < request_data.n_objs; ++i) {
        PDC_Server_unregister_obj_region_by_pointer(temp_ptrs[i], 1);
    }
    free(temp_ptrs);
#endif

    clean_write_bulk_data(&request_data);
    free(local_bulk_args->transfer_request_id);
    free(local_bulk_args->data_buf);
    free(remote_reg_info);

    HG_Bulk_free(local_bulk_args->bulk_handle);

    HG_Free_input(local_bulk_args->handle, &(local_bulk_args->in));
    HG_Destroy(local_bulk_args->handle);

    free(local_bulk_args);

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
    struct transfer_request_wait_all_local_bulk_args *local_bulk_args = info->arg;
    transfer_request_wait_all_out_t                   out;

    pdcid_t               transfer_request_id;
    hg_return_t           ret = HG_SUCCESS;
    int                   i, fast_return;
    char *                ptr;
    int *                 handle_ref;
    pdc_transfer_status_t status;

    FUNC_ENTER(NULL);

    // free is in PDC_finish_request
    fast_return = 1;
    handle_ref  = (int *)calloc(1, sizeof(int));
    pthread_mutex_lock(&transfer_request_status_mutex);
    ptr = local_bulk_args->data_buf;
    for (i = 0; i < local_bulk_args->in.n_objs; ++i) {
        transfer_request_id = *((pdcid_t *)ptr);
        ptr += sizeof(pdcid_t);
        status = PDC_check_request(transfer_request_id);
        // printf("processing transfer_id = %llu\n", (long long unsigned)transfer_request_id);
        if (status == PDC_TRANSFER_STATUS_PENDING) {
            PDC_try_finish_request(transfer_request_id, local_bulk_args->handle, handle_ref, 1);
        }
        if (status != PDC_TRANSFER_STATUS_COMPLETE) {
            fast_return = 0;
        }
    }
    pthread_mutex_unlock(&transfer_request_status_mutex);
    /*

        printf("HG_TEST_RPC_CB(transfer_request_wait, handle): exiting the wait function at server side @
       %d\n",
               __LINE__);
    */
    if (fast_return) {
        free(handle_ref);
        out.ret = 1;
        ret     = HG_Respond(local_bulk_args->handle, NULL, NULL, &out);
        HG_Free_input(local_bulk_args->handle, &(local_bulk_args->in));
        HG_Destroy(local_bulk_args->handle);
    }
    else {
        HG_Free_input(local_bulk_args->handle, &(local_bulk_args->in));
    }

    free(local_bulk_args->data_buf);

    HG_Bulk_free(local_bulk_args->bulk_handle);

    free(local_bulk_args);

#ifdef PDC_TIMING
    double end = MPI_Wtime();

    pdc_server_timings->PDCreg_transfer_request_wait_all_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_wait_all_timestamps, local_bulk_args->start_time, end);
#endif

    FUNC_LEAVE(ret);
}

hg_return_t
transfer_request_bulk_transfer_write_cb(const struct hg_cb_info *info)
{
    struct transfer_request_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                              ret             = HG_SUCCESS;
    struct pdc_region_info *                 remote_reg_info;
    uint64_t                                 obj_dims[3];

    FUNC_ENTER(NULL);

#ifdef PDC_TIMING
    double end = MPI_Wtime(), start;
    pdc_server_timings->PDCreg_transfer_request_start_write_bulk_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_write_bulk_timestamps, local_bulk_args->start_time,
                           end);
    start = MPI_Wtime();
#endif

    // printf("entering transfer bulk callback\n");

    remote_reg_info = (struct pdc_region_info *)malloc(sizeof(struct pdc_region_info));

    remote_reg_info->ndim   = (local_bulk_args->in.remote_region).ndim;
    remote_reg_info->offset = (uint64_t *)malloc(remote_reg_info->ndim * sizeof(uint64_t));
    remote_reg_info->size   = (uint64_t *)malloc(remote_reg_info->ndim * sizeof(uint64_t));
    if (remote_reg_info->ndim >= 1) {
        (remote_reg_info->offset)[0] = (local_bulk_args->in.remote_region).start_0;
        (remote_reg_info->size)[0]   = (local_bulk_args->in.remote_region).count_0;
        obj_dims[0]                  = (local_bulk_args->in).obj_dim0;
    }
    if (remote_reg_info->ndim >= 2) {
        (remote_reg_info->offset)[1] = (local_bulk_args->in.remote_region).start_1;
        (remote_reg_info->size)[1]   = (local_bulk_args->in.remote_region).count_1;
        obj_dims[1]                  = (local_bulk_args->in).obj_dim1;
    }
    if (remote_reg_info->ndim >= 3) {
        (remote_reg_info->offset)[2] = (local_bulk_args->in.remote_region).start_2;
        (remote_reg_info->size)[2]   = (local_bulk_args->in.remote_region).count_2;
        obj_dims[2]                  = (local_bulk_args->in).obj_dim2;
    }
/*
    printf("Server transfer request at write branch, index 1 value = %d\n",
           *((int *)(local_bulk_args->data_buf + sizeof(int))));
*/
#ifdef PDC_SERVER_CACHE
    PDC_transfer_request_data_write_out(local_bulk_args->in.obj_id, local_bulk_args->in.obj_ndim, obj_dims,
                                        remote_reg_info, (void *)local_bulk_args->data_buf,
                                        local_bulk_args->in.remote_unit);
#else
    PDC_Server_transfer_request_io(local_bulk_args->in.obj_id, local_bulk_args->in.obj_ndim, obj_dims,
                                   remote_reg_info, (void *)local_bulk_args->data_buf,
                                   local_bulk_args->in.remote_unit, 1);
#endif
    pthread_mutex_lock(&transfer_request_status_mutex);
    PDC_finish_request(local_bulk_args->transfer_request_id);
    pthread_mutex_unlock(&transfer_request_status_mutex);
    free(local_bulk_args->data_buf);
    free(remote_reg_info);

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
    struct transfer_request_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                              ret;
    FUNC_ENTER(NULL);

#ifdef PDC_TIMING
    double end = MPI_Wtime(), start;
    pdc_server_timings->PDCreg_transfer_request_start_read_bulk_rpc += end - local_bulk_args->start_time;
    pdc_timestamp_register(pdc_transfer_request_start_read_bulk_timestamps, local_bulk_args->start_time, end);
    start = MPI_Wtime();
#endif

    pthread_mutex_lock(&transfer_request_status_mutex);
    PDC_finish_request(local_bulk_args->transfer_request_id);
    pthread_mutex_unlock(&transfer_request_status_mutex);

    ret = HG_SUCCESS;

    HG_Bulk_free(local_bulk_args->bulk_handle);

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
    hg_return_t                   ret_value = HG_SUCCESS;
    transfer_request_status_in_t  in;
    transfer_request_status_out_t out;

    FUNC_ENTER(NULL);
    HG_Get_input(handle, &in);

    // printf("entering the status function at server side @ line %d\n", __LINE__);
    pthread_mutex_lock(&transfer_request_status_mutex);
    out.status = PDC_check_request(in.transfer_request_id);
    pthread_mutex_unlock(&transfer_request_status_mutex);
    out.ret   = 1;
    ret_value = HG_Respond(handle, NULL, NULL, &out);
    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// transfer_request_wait_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request_wait, handle)
{
    hg_return_t                 ret_value = HG_SUCCESS;
    transfer_request_wait_in_t  in;
    transfer_request_wait_out_t out;
    pdc_transfer_status_t       status;
    int                         fast_return = 0;
    int *                       handle_ref;

    FUNC_ENTER(NULL);
#ifdef PDC_TIMING
    double start = MPI_Wtime(), end;
#endif

    HG_Get_input(handle, &in);
    /*
        printf("HG_TEST_RPC_CB(transfer_request_wait, handle): entering the wait function at server side @
       %d\n",
               __LINE__);
    */
    pthread_mutex_lock(&transfer_request_status_mutex);
    status = PDC_check_request(in.transfer_request_id);
    if (status == PDC_TRANSFER_STATUS_PENDING) {
        handle_ref = (int *)calloc(1, sizeof(int));
        PDC_try_finish_request(in.transfer_request_id, handle, handle_ref, 0);
    }
    else {
        fast_return = 1;
    }
    pthread_mutex_unlock(&transfer_request_status_mutex);
    /*
        printf("HG_TEST_RPC_CB(transfer_request_wait, handle): exiting the wait function at server side @
       %d\n",
               __LINE__);
    */
    if (fast_return) {
        out.ret   = 1;
        ret_value = HG_Respond(handle, NULL, NULL, &out);
        HG_Free_input(handle, &in);
        HG_Destroy(handle);
    }
    else {
        HG_Free_input(handle, &in);
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

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// transfer_request_wait_all_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request_wait_all, handle)
{
    struct transfer_request_wait_all_local_bulk_args *local_bulk_args;
    const struct hg_info *                            info;
    transfer_request_wait_all_in_t                    in;
    hg_return_t                                       ret_value = HG_SUCCESS;
    FUNC_ENTER(NULL);
    HG_Get_input(handle, &in);

    info = HG_Get_info(handle);

    local_bulk_args = (struct transfer_request_wait_all_local_bulk_args *)malloc(
        sizeof(struct transfer_request_wait_all_local_bulk_args));

    local_bulk_args->handle   = handle;
    local_bulk_args->data_buf = malloc(in.total_buf_size);
    local_bulk_args->in       = in;

#ifdef PDC_TIMING
    local_bulk_args->start_time = MPI_Wtime();
#endif
    // Process this request after we receive all request ID.
    ret_value =
        HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf), &(local_bulk_args->in.total_buf_size),
                       HG_BULK_READWRITE, &(local_bulk_args->bulk_handle));
    ret_value =
        HG_Bulk_transfer(info->context, transfer_request_wait_all_bulk_transfer_cb, local_bulk_args,
                         HG_BULK_PULL, info->addr, in.local_bulk_handle, 0, local_bulk_args->bulk_handle, 0,

                         local_bulk_args->in.total_buf_size, HG_OP_ID_IGNORE);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */
// transfer_request_all_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request_all, handle)
{
    struct transfer_request_all_local_bulk_args *local_bulk_args;
    const struct hg_info *                       info;
    transfer_request_all_in_t                    in;
    transfer_request_all_out_t                   out;
    hg_return_t                                  ret_value = HG_SUCCESS;
    int                                          i;

    FUNC_ENTER(NULL);

#ifdef PDC_TIMING
    double start = MPI_Wtime(), end;
#endif

    HG_Get_input(handle, &in);

    info            = HG_Get_info(handle);
    local_bulk_args = (struct transfer_request_all_local_bulk_args *)malloc(
        sizeof(struct transfer_request_all_local_bulk_args));

    // Read will return to client in the first call back (after metadata for region request is received)
    local_bulk_args->handle              = handle;
    local_bulk_args->data_buf            = malloc(in.total_buf_size);
    local_bulk_args->in                  = in;
    local_bulk_args->transfer_request_id = (uint64_t *)malloc(sizeof(uint64_t) * in.n_objs);

    pthread_mutex_lock(&transfer_request_id_mutex);
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
        // Read operation has to receive region metadata first. There will be another bulk transfer triggered
        // in the callback.
        ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf),
                                   &(local_bulk_args->in.total_buf_size), HG_BULK_READWRITE,
                                   &(local_bulk_args->bulk_handle));
        ret_value =
            HG_Bulk_transfer(info->context, transfer_request_all_bulk_transfer_read_cb, local_bulk_args,
                             HG_BULK_PULL, info->addr, in.local_bulk_handle, 0, local_bulk_args->bulk_handle,
                             0, local_bulk_args->in.total_buf_size, HG_OP_ID_IGNORE);
    }

    out.ret   = 1;
    ret_value = HG_Respond(handle, NULL, NULL, &out);

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

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

hg_return_t
transfer_request_metadata_query_bulk_transfer_cb(const struct hg_cb_info *info)
{
    struct transfer_request_metadata_query_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                                             ret             = HG_SUCCESS;
    transfer_request_metadata_query_out_t                   out;

    FUNC_ENTER(NULL);
    // printf("transfer_request_metadata_query_bulk_transfer_cb: checkpoint %d\n", __LINE__);
    out.query_id =
        transfer_request_metadata_query_parse(local_bulk_args->in.n_objs, (char *)local_bulk_args->data_buf,
                                              local_bulk_args->in.is_write, &(out.total_buf_size));
    // printf("transfer_request_metadata_query_bulk_transfer_cb: checkpoint %d\n", __LINE__);
    free(local_bulk_args->data_buf);
    // printf("transfer_request_metadata_query_bulk_transfer_cb: checkpoint %d\n", __LINE__);

    out.ret = 1;
    ret     = HG_Respond(local_bulk_args->handle, NULL, NULL, &out);
    HG_Bulk_free(local_bulk_args->bulk_handle);
    HG_Destroy(local_bulk_args->handle);
    FUNC_LEAVE(ret);
}

/* static hg_return_t */
// transfer_request_metadata_query_cb(hg_handle_t handle)

HG_TEST_RPC_CB(transfer_request_metadata_query, handle)
{
    struct transfer_request_metadata_query_local_bulk_args *local_bulk_args;
    const struct hg_info *                                  info;
    transfer_request_metadata_query_in_t                    in;

    hg_return_t ret_value = HG_SUCCESS;

    FUNC_ENTER(NULL);
    HG_Get_input(handle, &in);
    info            = HG_Get_info(handle);
    local_bulk_args = (struct transfer_request_metadata_query_local_bulk_args *)malloc(
        sizeof(struct transfer_request_metadata_query_local_bulk_args));

    local_bulk_args->data_buf = malloc(in.total_buf_size);
    local_bulk_args->in       = in;
    local_bulk_args->handle   = handle;
    // printf("transfer_request_metadata_query: checkpoint %d\n", __LINE__);
    ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf), &(in.total_buf_size),
                               HG_BULK_READWRITE, &(local_bulk_args->bulk_handle));
    if (ret_value != HG_SUCCESS) {
        printf("Error at HG_TEST_RPC_CB(transfer_request, handle): @ line %d \n", __LINE__);
    }

    // This is the actual data transfer. When transfer is finished, we are heading our way to the function
    // transfer_request_bulk_transfer_cb.
    ret_value = HG_Bulk_transfer(info->context, transfer_request_metadata_query_bulk_transfer_cb,
                                 local_bulk_args, HG_BULK_PULL, info->addr, in.local_bulk_handle, 0,
                                 local_bulk_args->bulk_handle, 0, in.total_buf_size, HG_OP_ID_IGNORE);

    HG_Free_input(handle, &in);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

hg_return_t
transfer_request_metadata_query2_bulk_transfer_cb(const struct hg_cb_info *info)
{
    struct transfer_request_metadata_query2_local_bulk_args *local_bulk_args = info->arg;
    hg_return_t                                              ret             = HG_SUCCESS;
    transfer_request_metadata_query2_out_t                   out;

    FUNC_ENTER(NULL);

    out.ret = 1;
    // printf("transfer_request_metadata_query2_bulk_transfer_cb: checkpoint %d, data_buf = %lld\n", __LINE__,
    // (long long int)local_bulk_args->data_buf);
    free(local_bulk_args->data_buf);
    ret = HG_Respond(local_bulk_args->handle, NULL, NULL, &out);
    HG_Bulk_free(local_bulk_args->bulk_handle);
    HG_Destroy(local_bulk_args->handle);
    FUNC_LEAVE(ret);
}

/* static hg_return_t */
// transfer_request_metadata_query2_cb(hg_handle_t handle)

HG_TEST_RPC_CB(transfer_request_metadata_query2, handle)
{
    struct transfer_request_metadata_query2_local_bulk_args *local_bulk_args;
    const struct hg_info *                                   info;
    transfer_request_metadata_query2_in_t                    in;

    hg_return_t ret_value = HG_SUCCESS;

    FUNC_ENTER(NULL);
    HG_Get_input(handle, &in);

    info            = HG_Get_info(handle);
    local_bulk_args = (struct transfer_request_metadata_query2_local_bulk_args *)malloc(
        sizeof(struct transfer_request_metadata_query2_local_bulk_args));

    local_bulk_args->handle = handle;

    // Retrieve the data buffer to be sent to client
    // printf("transfer_request_metadata_query2: checkpoint %d, total_buf_size = %lu\n", __LINE__,
    // in.total_buf_size);
    transfer_request_metadata_query_lookup_query_buf(in.query_id, (char **)&(local_bulk_args->data_buf));
    // printf("transfer_request_metadata_query2: checkpoint %d\n", __LINE__);
    ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf), &(in.total_buf_size),
                               HG_BULK_READWRITE, &(local_bulk_args->bulk_handle));
    if (ret_value != HG_SUCCESS) {
        printf("Error at HG_TEST_RPC_CB(transfer_request, handle): @ line %d \n", __LINE__);
    }

    // This is the actual data transfer. When transfer is finished, we are heading our way to the function
    // transfer_request_bulk_transfer_cb.
    ret_value = HG_Bulk_transfer(info->context, transfer_request_metadata_query2_bulk_transfer_cb,
                                 local_bulk_args, HG_BULK_PUSH, info->addr, in.local_bulk_handle, 0,
                                 local_bulk_args->bulk_handle, 0, in.total_buf_size, HG_OP_ID_IGNORE);

    HG_Free_input(handle, &in);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/* static hg_return_t */

// transfer_request_cb(hg_handle_t handle)
HG_TEST_RPC_CB(transfer_request, handle)
{
    hg_return_t                              ret_value = HG_SUCCESS;
    transfer_request_in_t                    in;
    transfer_request_out_t                   out;
    struct transfer_request_local_bulk_args *local_bulk_args;
    size_t                                   total_mem_size;
    const struct hg_info *                   info;
    struct pdc_region_info *                 remote_reg_info;
    uint64_t                                 obj_dims[3];

    FUNC_ENTER(NULL);

#ifdef PDC_TIMING
    double start = MPI_Wtime(), end;
#endif

    HG_Get_input(handle, &in);

    info = HG_Get_info(handle);

    total_mem_size = in.remote_unit;
    if (in.remote_region.ndim >= 1) {
        total_mem_size *= in.remote_region.count_0;
    }
    if (in.remote_region.ndim >= 2) {
        total_mem_size *= in.remote_region.count_1;
    }
    if (in.remote_region.ndim >= 3) {
        total_mem_size *= in.remote_region.count_2;
    }
    pthread_mutex_lock(&transfer_request_id_mutex);
    out.metadata_id = PDC_transfer_request_id_register();
    pthread_mutex_unlock(&transfer_request_id_mutex);
    pthread_mutex_lock(&transfer_request_status_mutex);
    PDC_commit_request(out.metadata_id);
    pthread_mutex_unlock(&transfer_request_status_mutex);

    local_bulk_args =
        (struct transfer_request_local_bulk_args *)malloc(sizeof(struct transfer_request_local_bulk_args));
    local_bulk_args->handle              = handle;
    local_bulk_args->total_mem_size      = total_mem_size;
    local_bulk_args->data_buf            = malloc(total_mem_size);
    local_bulk_args->in                  = in;
    local_bulk_args->transfer_request_id = out.metadata_id;
#ifdef PDC_TIMING
    local_bulk_args->start_time = MPI_Wtime();
#endif
    /*
        printf("server check obj ndim %d, dims [%" PRIu64 ", %" PRIu64 ", %" PRIu64 "]\n", (int)in.obj_ndim,
               in.obj_dim0, in.obj_dim1, in.obj_dim2);
    */
    // printf("HG_TEST_RPC_CB(transfer_request, handle) checkpoint @ line %d\n", __LINE__);
    out.ret   = 1;
    ret_value = HG_Respond(handle, NULL, NULL, &out);
    if (in.access_type == PDC_WRITE) {
        ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf),
                                   &(local_bulk_args->total_mem_size), HG_BULK_READWRITE,
                                   &(local_bulk_args->bulk_handle));
        if (ret_value != HG_SUCCESS) {
            printf("Error at HG_TEST_RPC_CB(transfer_request, handle): @ line %d \n", __LINE__);
        }

        // This is the actual data transfer. When transfer is finished, we are heading our way to the function
        // transfer_request_bulk_transfer_cb.
        ret_value = HG_Bulk_transfer(info->context, transfer_request_bulk_transfer_write_cb, local_bulk_args,
                                     HG_BULK_PULL, info->addr, in.local_bulk_handle, 0,
                                     local_bulk_args->bulk_handle, 0, total_mem_size, HG_OP_ID_IGNORE);
    }
    else {
        // in.access_type == PDC_READ

        remote_reg_info = (struct pdc_region_info *)malloc(sizeof(struct pdc_region_info));

        remote_reg_info->ndim   = (in.remote_region).ndim;
        remote_reg_info->offset = (uint64_t *)malloc(remote_reg_info->ndim * sizeof(uint64_t));
        remote_reg_info->size   = (uint64_t *)malloc(remote_reg_info->ndim * sizeof(uint64_t));
        if (remote_reg_info->ndim >= 1) {
            (remote_reg_info->offset)[0] = (in.remote_region).start_0;
            (remote_reg_info->size)[0]   = (in.remote_region).count_0;
            obj_dims[0]                  = in.obj_dim0;
        }
        if (remote_reg_info->ndim >= 2) {
            (remote_reg_info->offset)[1] = (in.remote_region).start_1;
            (remote_reg_info->size)[1]   = (in.remote_region).count_1;
            obj_dims[1]                  = in.obj_dim1;
        }
        if (remote_reg_info->ndim >= 3) {
            (remote_reg_info->offset)[2] = (in.remote_region).start_2;
            (remote_reg_info->size)[2]   = (in.remote_region).count_2;
            obj_dims[2]                  = in.obj_dim2;
        }
#ifdef PDC_SERVER_CACHE
        PDC_transfer_request_data_read_from(in.obj_id, in.obj_ndim, obj_dims, remote_reg_info,
                                            (void *)local_bulk_args->data_buf, in.remote_unit);
#else
        PDC_Server_transfer_request_io(in.obj_id, in.obj_ndim, obj_dims, remote_reg_info,
                                       (void *)local_bulk_args->data_buf, in.remote_unit, 0);
#endif
        /*
                printf("ndim = %d\n", in.obj_ndim);
                if (in.obj_ndim == 2) {
                    printf("offset[0] = %" PRIu64 ", length[0] = %" PRIu64 ", offset[1] = %" PRIu64 ",
           length[1] = %" PRIu64 "\n", (remote_reg_info->offset)[0], (remote_reg_info->size)[0],
           (remote_reg_info->offset)[1], (remote_reg_info->size)[1]);
                }
                uint64_t total_data_size = (remote_reg_info->size)[0];
                if (remote_reg_info->ndim >= 2) {
                    total_data_size *= (remote_reg_info->size)[1];
                }
                if (remote_reg_info->ndim >= 3) {
                    total_data_size *= (remote_reg_info->size)[2];
                }
                int *int_ptr = local_bulk_args->data_buf;
                uint64_t i;
                 printf("Read data print out\n");
                for ( i = 0; i < total_data_size; ++i ) {
                    printf("%d ", int_ptr[i]);
                }
                printf("\n");
                printf("--------------------\n");
        */
        /*
                printf("Server transfer request at read branch index 1 value is %d\n",
                       *((int *)(local_bulk_args->data_buf + sizeof(int))));
        */
        ret_value = HG_Bulk_create(info->hg_class, 1, &(local_bulk_args->data_buf),
                                   &(local_bulk_args->total_mem_size), HG_BULK_READWRITE,
                                   &(local_bulk_args->bulk_handle));
        if (ret_value != HG_SUCCESS) {
            printf("Error at HG_TEST_RPC_CB(transfer_request, handle): @ line %d \n", __LINE__);
        }

        // This is the actual data transfer. When transfer is finished, we are heading our way to the function
        // transfer_request_bulk_transfer_cb.
        ret_value = HG_Bulk_transfer(info->context, transfer_request_bulk_transfer_read_cb, local_bulk_args,
                                     HG_BULK_PUSH, info->addr, in.local_bulk_handle, 0,
                                     local_bulk_args->bulk_handle, 0, total_mem_size, HG_OP_ID_IGNORE);
        free(remote_reg_info);
    }
    if (ret_value != HG_SUCCESS) {
        printf("Error at HG_TEST_RPC_CB(transfer_request, handle): @ line %d \n", __LINE__);
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

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
