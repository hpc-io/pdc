#include "pdc_client_server_common.h"
#include "pdc_server_data.h"
static int io_by_region_g = 1;

static int
get_server_rank()
{
#ifdef ENABLE_MPI
    int result;
    MPI_Comm_rank(MPI_COMM_WORLD, &result);
    return result;
#else
    return 0;
#endif
}

perr_t PDC_server_transfer_request_init() {
    FUNC_ENTER(NULL);

    transfer_request_status_list = NULL;
    pthread_mutex_init(&transfer_request_status_mutex, NULL);
    pthread_mutex_init(&transfer_request_id_mutex, NULL);
    transfer_request_id_g = 1;

    FUNC_LEAVE(SUCCEED);
}

perr_t PDC_server_transfer_request_finalize() {
    FUNC_ENTER(NULL);

    pthread_mutex_destroy(&transfer_request_status_mutex);
    pthread_mutex_destroy(&transfer_request_id_mutex);


    FUNC_LEAVE(SUCCEED);
}

/*
 * Create a new linked list node for a region transfer request and append it to the end of the linked list.
 * Thread-safe function, lock required ahead of time.
 */
perr_t
PDC_commit_request(uint64_t transfer_request_id)
{
    pdc_transfer_request_status *ptr;
    perr_t                       ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    if (transfer_request_status_list == NULL) {
        transfer_request_status_list =
            (pdc_transfer_request_status *)malloc(sizeof(pdc_transfer_request_status));
        transfer_request_status_list->status              = PDC_TRANSFER_STATUS_PENDING;
        transfer_request_status_list->handle_ref          = NULL;
        transfer_request_status_list->out_type            = -1;
        transfer_request_status_list->transfer_request_id = transfer_request_id;
        transfer_request_status_list->next                = NULL;
        transfer_request_status_list_end                  = transfer_request_status_list;
    }
    else {
        ptr                   = transfer_request_status_list_end;
        ptr->next             = (pdc_transfer_request_status *)malloc(sizeof(pdc_transfer_request_status));
        ptr->next->status     = PDC_TRANSFER_STATUS_PENDING;
        ptr->next->handle_ref = NULL;
        ptr->next->out_type   = -1;
        ptr->next->transfer_request_id   = transfer_request_id;
        ptr->next->next                  = NULL;
        transfer_request_status_list_end = ptr->next;
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Search a linked list for a transfer request.
 * Set the entry status to PDC_TRANSFER_STATUS_COMPLETE.
 * Thread-safe function, lock required ahead of time.
 */
perr_t
PDC_finish_request(uint64_t transfer_request_id)
{
    pdc_transfer_request_status *   ptr, *tmp = NULL;
    perr_t                          ret_value = SUCCEED;
    transfer_request_wait_out_t     out;
    transfer_request_wait_all_out_t out_all;

    FUNC_ENTER(NULL);

    ptr = transfer_request_status_list;
    while (ptr != NULL) {
        if (ptr->transfer_request_id == transfer_request_id) {
            ptr->status = PDC_TRANSFER_STATUS_COMPLETE;
            if (ptr->handle_ref != NULL) {
                /* Wait request is going to be returned, so we are not expecting any further checks for the
                 * current request. Immediately eject the current transfer request out of the list.*/
                ptr->handle_ref[0]--;
                if (!ptr->handle_ref[0]) {
                    if (ptr->out_type == -1) {
                        printf("PDC SERVER PDC_finish_request out type unset error %d\n", __LINE__);
                    }
                    if (ptr->out_type) {
                        out_all.ret = 1;
                        ret_value   = HG_Respond(ptr->handle, NULL, NULL, &out_all);
                    }
                    else {
                        out.ret   = 1;
                        ret_value = HG_Respond(ptr->handle, NULL, NULL, &out);
                    }
                    HG_Destroy(ptr->handle);
                    free(ptr->handle_ref);
                }
                if (tmp != NULL) {
                    /* Case for removing the any nodes but the first one. */
                    tmp->next = ptr->next;
                    /* Free pointer is the last list node, so we set the end to the previous one. */
                    if (ptr->next == NULL) {
                        transfer_request_status_list_end = tmp;
                    }
                    free(ptr);
                }
                else {
                    /* Case for removing the first node, i.e ptr == transfer_request_status_list*/
                    tmp                          = transfer_request_status_list;
                    transfer_request_status_list = transfer_request_status_list->next;
                    free(tmp);
                    /* Free pointer is the last list node, so nothing is left in the list. */
                    if (transfer_request_status_list == NULL) {
                        transfer_request_status_list_end = NULL;
                    }
                }
                break;
            }
        }
        tmp = ptr;
        ptr = ptr->next;
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Search a linked list for a region transfer request.
 * Remove the linked list node and free its memory.
 * Return the status of the region transfer request.
 * Thread-safe function, lock required ahead of time.
 */
pdc_transfer_status_t
PDC_check_request(uint64_t transfer_request_id)
{
    pdc_transfer_request_status *ptr, *tmp = NULL;
    pdc_transfer_status_t        ret_value = PDC_TRANSFER_STATUS_NOT_FOUND;
    FUNC_ENTER(NULL);

    ptr = transfer_request_status_list;
    while (ptr != NULL) {
        if (ptr->transfer_request_id == transfer_request_id) {
            ret_value = ptr->status;
            if (ptr->handle_ref != NULL) {
                ret_value = PDC_TRANSFER_STATUS_COMPLETE;
                return ret_value;
            }
            if (ret_value == PDC_TRANSFER_STATUS_COMPLETE) {
                if (tmp != NULL) {
                    /* Case for removing the any nodes but the first one. */
                    tmp->next = ptr->next;
                    /* Free pointer is the last list node, so we set the end to the previous one. */
                    if (ptr->next == NULL) {
                        transfer_request_status_list_end = tmp;
                    }
                    free(ptr);
                }
                else {
                    /* Case for removing the first node, i.e ptr == transfer_request_status_list*/
                    tmp                          = transfer_request_status_list;
                    transfer_request_status_list = transfer_request_status_list->next;
                    free(tmp);
                    /* Free pointer is the last list node, so nothing is left in the list. */
                    if (transfer_request_status_list == NULL) {
                        transfer_request_status_list_end = NULL;
                    }
                }
            }
            break;
        }
        tmp = ptr;
        ptr = ptr->next;
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Search a linked list for a region transfer request.
 * Bind an RPC handle to the transfer request, so the RPC can be returned when the PDC_finish_request function
 * is called. Thread-safe function, lock required ahead of time.
 */
pdc_transfer_status_t
PDC_try_finish_request(uint64_t transfer_request_id, hg_handle_t handle, int *handle_ref, int out_type)
{
    pdc_transfer_request_status *ptr;
    pdc_transfer_status_t        ret_value = PDC_TRANSFER_STATUS_NOT_FOUND;
    FUNC_ENTER(NULL);

    ptr = transfer_request_status_list;
    while (ptr != NULL) {
        if (ptr->transfer_request_id == transfer_request_id) {
            ptr->handle     = handle;
            ptr->out_type   = out_type;
            ptr->handle_ref = handle_ref;
            handle_ref[0]++;
            break;
        }
        ptr = ptr->next;
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Generate a remote transfer request ID in a very fast way.
 * What happen if we have one request pending and call the register 2^64 times? This could result a repetitive
 * transfer request ID generated.
 * TODO: Scan the entire transfer list and search for repetitive nodes.
 * Not a thread-safe function, need protection from pthread_mutex_lock(&transfer_request_id_mutex);
 */

pdcid_t
PDC_transfer_request_id_register()
{
    pdcid_t ret_value;

    FUNC_ENTER(NULL);

    ret_value = transfer_request_id_g;
    transfer_request_id_g++;

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Core I/O functions for region transfer request.
 * Nonzero io_by_region_g will trigger region by region storage. Otherwise file flatten strategy is used
 */
#define PDC_POSIX_IO(fd, buf, io_size, is_write)                                                             \
    if (is_write) {                                                                                          \
        if (write(fd, buf, io_size) != io_size) {                                                            \
            printf("server POSIX write failed\n");                                                           \
        }                                                                                                    \
    }                                                                                                        \
    else {                                                                                                   \
        if (read(fd, buf, io_size) != io_size) {                                                             \
            printf("server POSIX read failed\n");                                                            \
        }                                                                                                    \
    }

perr_t
PDC_Server_transfer_request_io(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                               struct pdc_region_info *region_info, void *buf, size_t unit, int is_write)
{
    perr_t   ret_value = SUCCEED;
    int      fd;
    char *   data_path                = NULL;
    char *   user_specified_data_path = NULL;
    char     storage_location[ADDR_MAX];
    ssize_t  io_size;
    uint64_t i, j;

    int server_rank = get_server_rank();

    FUNC_ENTER(NULL);

    if (io_by_region_g || obj_ndim == 0) {
        // PDC_Server_register_obj_region(obj_id);
        if (is_write) {
            PDC_Server_data_write_out(obj_id, region_info, buf, unit);
        }
        else {
            PDC_Server_data_read_from(obj_id, region_info, buf, unit);
        }
        // PDC_Server_unregister_obj_region(obj_id);
        goto done;
    }
    if (obj_ndim != (int)region_info->ndim) {
        printf("Server I/O error: Obj dim does not match obj dim\n");
        goto done;
    }

    user_specified_data_path = getenv("PDC_DATA_LOC");
    if (user_specified_data_path != NULL) {
        data_path = user_specified_data_path;
    }
    else {
        data_path = getenv("SCRATCH");
        if (data_path == NULL)
            data_path = ".";
    }
    // Data path prefix will be $SCRATCH/pdc_data/$obj_id/
    snprintf(storage_location, ADDR_MAX, "%.200s/pdc_data/%" PRIu64 "/server%d/s%04d.bin", data_path, obj_id,
             server_rank, server_rank);
    PDC_mkdir(storage_location);

    fd = open(storage_location, O_RDWR | O_CREAT, 0666);
    if (region_info->ndim == 1) {
        // printf("server I/O checkpoint 1D\n");
        lseek(fd, region_info->offset[0] * unit, SEEK_SET);
        io_size = region_info->size[0] * unit;
        PDC_POSIX_IO(fd, buf, io_size, is_write);
    }
    else if (region_info->ndim == 2) {
        // Check we can directly write the contiguous chunk to the file
        if (region_info->offset[1] == 0 && region_info->size[1] == obj_dims[1]) {
            // printf("server I/O checkpoint 2D 1\n");
            lseek(fd, region_info->offset[0] * obj_dims[1] * unit, SEEK_SET);
            io_size = region_info->size[0] * obj_dims[1] * unit;
            PDC_POSIX_IO(fd, buf, io_size, is_write);
        }
        else {
            // printf("server I/O checkpoint 2D 2\n");
            // We have to write line by line
            for (i = 0; i < region_info->size[0]; ++i) {
                /*
                                printf("lseek to %lld\n",
                                       (long long int)((i + region_info->offset[0]) * obj_dims[1] +
                   region_info->offset[1]));
                */
                lseek(fd, ((i + region_info->offset[0]) * obj_dims[1] + region_info->offset[1]) * unit,
                      SEEK_SET);
                io_size = region_info->size[1] * unit;
                PDC_POSIX_IO(fd, buf, io_size, is_write);
                buf += io_size;
            }
        }
    }
    else if (region_info->ndim == 3) {
        // Check we can directly write the contiguous chunk to the file
        if (region_info->offset[1] == 0 && region_info->size[1] == obj_dims[1] &&
            region_info->offset[2] == 0 && region_info->size[2] == obj_dims[2]) {
            lseek(fd, region_info->offset[0] * region_info->size[1] * region_info->size[2] * unit, SEEK_SET);
            io_size = region_info->size[0] * region_info->size[1] * region_info->size[2] * unit;
            // printf("server I/O checkpoint 3D 1\n");
            PDC_POSIX_IO(fd, buf, io_size, is_write);
        }
        else if (region_info->offset[2] == 0 && region_info->size[2] == obj_dims[2]) {
            // printf("server I/O checkpoint 3D 2\n");
            // We have to write plane by plane
            for (i = 0; i < region_info->size[0]; ++i) {
                lseek(fd,
                      ((i + region_info->offset[0]) * obj_dims[1] * obj_dims[2] +
                       region_info->offset[1] * obj_dims[2]) *
                          unit,
                      SEEK_SET);
                io_size = region_info->size[1] * obj_dims[2] * unit;
                PDC_POSIX_IO(fd, buf, io_size, is_write);
                buf += io_size;
            }
        }
        else {
            /*
                        printf("server I/O checkpoint 3D 3, obj dims [%" PRIu64 ", %" PRIu64 ", %" PRIu64
                               "], region [%" PRIu64 ", %" PRIu64 ", %" PRIu64 "] size [%" PRIu64 ", %" PRIu64
                               ", %" PRIu64 "]\n",
                               obj_dims[0], obj_dims[1], obj_dims[2], region_info->offset[0],
               region_info->offset[1], region_info->offset[2], region_info->size[0], region_info->size[1],
               region_info->size[2]);
            */
            // We have to write line by line
            for (i = 0; i < region_info->size[0]; ++i) {
                for (j = 0; j < region_info->size[1]; ++j) {
                    lseek(fd,
                          ((region_info->offset[0] + i) * obj_dims[1] * obj_dims[2] +
                           (region_info->offset[1] + j) * obj_dims[2] + region_info->offset[2]) *
                              unit,
                          SEEK_SET);
                    io_size = region_info->size[2] * unit;
                    PDC_POSIX_IO(fd, buf, io_size, is_write);
                    buf += io_size;
                }
            }
        }
    }
    close(fd);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

int
clean_write_bulk_data(transfer_request_all_data *request_data)
{
    free(request_data->obj_id);
    free(request_data->obj_ndim);
    free(request_data->remote_ndim);
    free(request_data->remote_offset);
    free(request_data->unit);
    free(request_data->data_buf);
    return 0;
}
/*
static int
print_bulk_data(transfer_request_all_data *request_data)
{
    int      i, j;
    uint64_t data_size;
    printf("total number of objects: %d\n", request_data->n_objs);
    for (i = 0; i < request_data->n_objs; ++i) {
        data_size = request_data->remote_length[i][0] * request_data->unit[i];
        for (j = 1; j < request_data->remote_ndim[i]; ++j) {
            data_size *= request_data->remote_length[i][j];
        }

        printf("----object %d----\n", i);
        printf("object dimension: %d\n", request_data->remote_ndim[i]);
        printf("object unit: %" PRIu64 "\n", request_data->unit[i]);
    }
    return 0;
}
*/
int
parse_bulk_data(void *buf, transfer_request_all_data *request_data, pdc_access_t access_type)
{
    char *   ptr = (char *)buf;
    int      i, j;
    uint64_t data_size;

    // preallocate arrays of size number of objects
    request_data->obj_id        = (pdcid_t *)malloc(sizeof(pdcid_t) * request_data->n_objs);
    request_data->obj_ndim      = (int *)malloc(sizeof(int) * request_data->n_objs);
    request_data->remote_ndim   = (int *)malloc(sizeof(int) * request_data->n_objs);
    request_data->remote_offset = (uint64_t **)malloc(sizeof(uint64_t *) * request_data->n_objs * 3);
    request_data->remote_length = request_data->remote_offset + request_data->n_objs;
    request_data->obj_dims      = request_data->remote_length + request_data->n_objs;
    request_data->unit          = (size_t *)malloc(sizeof(size_t) * request_data->n_objs);
    request_data->data_buf      = (char **)malloc(sizeof(char *) * request_data->n_objs);

    /*
     * The following times n_objs (one set per object).
     *     obj_id: sizeof(pdcid_t)
     *     obj_ndim: sizeof(int)
     *     remote remote_ndim: sizeof(int)
     *     unit: sizeof(size_t)
     */
    for (i = 0; i < request_data->n_objs; ++i) {
        request_data->obj_id[i] = *((pdcid_t *)ptr);
        ptr += sizeof(pdcid_t);
        request_data->obj_ndim[i] = *((int *)ptr);
        ptr += sizeof(int);
        request_data->remote_ndim[i] = *((int *)ptr);
        ptr += sizeof(int);
        request_data->unit[i] = *((pdcid_t *)ptr);
        ptr += sizeof(size_t);
    }
    /*
     * For each of objects
     *     remote region offset: size(uint64_t) * region_ndim
     *     remote region length: size(uint64_t) * region_ndim
     *     obj_dims: size(uint64_t) * remote_ndim
     *     buf: computed from region length (summed up)
     */
    for (i = 0; i < request_data->n_objs; ++i) {
        request_data->remote_offset[i] = (uint64_t *)ptr;
        ptr += request_data->remote_ndim[i] * sizeof(uint64_t);
        request_data->remote_length[i] = (uint64_t *)ptr;
        ptr += request_data->remote_ndim[i] * sizeof(uint64_t);
        request_data->obj_dims[i] = (uint64_t *)ptr;
        ptr += request_data->obj_ndim[i] * sizeof(uint64_t);
        if (access_type == PDC_WRITE) {
            data_size = request_data->remote_length[i][0] * request_data->unit[i];
            for (j = 1; j < request_data->remote_ndim[i]; ++j) {
                data_size *= request_data->remote_length[i][j];
            }
            request_data->data_buf[i] = (char *)ptr;
            ptr += data_size;
        }
    }

    return 0;
}
