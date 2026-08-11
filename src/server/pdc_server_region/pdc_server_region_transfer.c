#include "pdc_client_server_common.h"
#include "pdc_server_data.h"
#include "pdc_timing.h"
#include "pdc_logger.h"
#include "pdc_malloc.h"
#include "pdc_server_metadata.h"
#include "pdc_server_region_cache.h"

#include <errno.h>
#include <assert.h>

/**
 * Used to indicate if current storage strategy supports
 * region dim resizing
 */
int
try_reset_dims()
{
    return false;
}

perr_t
PDC_server_transfer_request_init()
{
    FUNC_ENTER(NULL);

    transfer_request_status_list = NULL;
    pthread_mutex_init(&transfer_request_status_mutex, NULL);
    pthread_mutex_init(&transfer_request_id_mutex, NULL);
    transfer_request_id_g = 1;

    FUNC_LEAVE(SUCCEED);
}

perr_t
PDC_server_transfer_request_finalize()
{
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
    FUNC_ENTER(NULL);

    pdc_transfer_request_status *ptr;
    perr_t                       ret_value = SUCCEED;

    if (transfer_request_status_list == NULL) {
        transfer_request_status_list =
            (pdc_transfer_request_status *)PDC_malloc(sizeof(pdc_transfer_request_status));
        transfer_request_status_list->status              = PDC_TRANSFER_STATUS_PENDING;
        transfer_request_status_list->handle_ref          = NULL;
        transfer_request_status_list->out_type            = -1;
        transfer_request_status_list->transfer_request_id = transfer_request_id;
        transfer_request_status_list->next                = NULL;
        transfer_request_status_list_end                  = transfer_request_status_list;
    }
    else {
        ptr               = transfer_request_status_list_end;
        ptr->next         = (pdc_transfer_request_status *)PDC_malloc(sizeof(pdc_transfer_request_status));
        ptr->next->status = PDC_TRANSFER_STATUS_PENDING;
        ptr->next->handle_ref            = NULL;
        ptr->next->out_type              = -1;
        ptr->next->transfer_request_id   = transfer_request_id;
        ptr->next->next                  = NULL;
        transfer_request_status_list_end = ptr->next;
    }

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
    FUNC_ENTER(NULL);

    pdc_transfer_request_status *   ptr, *tmp = NULL;
    perr_t                          ret_value = SUCCEED;
    transfer_request_wait_out_t     out;
    transfer_request_wait_all_out_t out_all;
    char                            cur_time[64];

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
                        LOG_ERROR("PDC SERVER PDC_finish_request out type unset error\n");
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
                    ptr->handle_ref = (int *)PDC_free(ptr->handle_ref);
                }
                if (tmp != NULL) {
                    /* Case for removing the any nodes but the first one. */
                    tmp->next = ptr->next;
                    /* Free pointer is the last list node, so we set the end to the previous one. */
                    if (ptr->next == NULL) {
                        transfer_request_status_list_end = tmp;
                    }
                    ptr = (pdc_transfer_request_status *)PDC_free(ptr);
                }
                else {
                    /* Case for removing the first node, i.e ptr == transfer_request_status_list*/
                    tmp                          = transfer_request_status_list;
                    transfer_request_status_list = transfer_request_status_list->next;
                    tmp                          = (pdc_transfer_request_status *)PDC_free(tmp);
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
    FUNC_ENTER(NULL);

    pdc_transfer_request_status *ptr, *tmp = NULL;
    pdc_transfer_status_t        ret_value = PDC_TRANSFER_STATUS_NOT_FOUND;

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
                    ptr = (pdc_transfer_request_status *)PDC_free(ptr);
                }
                else {
                    /* Case for removing the first node, i.e ptr == transfer_request_status_list*/
                    tmp                          = transfer_request_status_list;
                    transfer_request_status_list = transfer_request_status_list->next;
                    tmp                          = (pdc_transfer_request_status *)PDC_free(tmp);
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
    FUNC_ENTER(NULL);

    pdc_transfer_request_status *ptr;
    pdc_transfer_status_t        ret_value = PDC_TRANSFER_STATUS_NOT_FOUND;

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
    FUNC_ENTER(NULL);

    pdcid_t ret_value;

    ret_value = transfer_request_id_g;
    transfer_request_id_g++;

    FUNC_LEAVE(ret_value);
}

/*
 * Core I/O functions for region transfer request.
 */
#define PDC_POSIX_IO(fd, buf, io_size, is_write)                                                             \
    if (is_write) {                                                                                          \
        if (write(fd, buf, io_size) != io_size) {                                                            \
            LOG_ERROR("server POSIX write failed\n");                                                        \
        }                                                                                                    \
    }                                                                                                        \
    else {                                                                                                   \
        if (read(fd, buf, io_size) != io_size) {                                                             \
            LOG_ERROR("server POSIX read failed\n");                                                         \
        }                                                                                                    \
    }

/**
 * Returns path to dir to store data regions
 */
static inline char *
get_data_path()
{
    FUNC_ENTER(NULL);

    char *user_specified_data_path;
    char *data_path;

    user_specified_data_path = getenv("PDC_DATA_LOC");
    if (user_specified_data_path != NULL) {
        if (strpbrk(user_specified_data_path, ";&|`$<>") != NULL)
            user_specified_data_path = NULL;
    }
    if (user_specified_data_path != NULL) {
        data_path = user_specified_data_path;
    }
    else {
        data_path = getenv("SCRATCH");
        if (data_path != NULL && strpbrk(data_path, ";&|`$<>") != NULL)
            data_path = NULL;
        if (data_path == NULL)
            data_path = ".";
    }

    FUNC_LEAVE(data_path);
}

/**
 * Handler for STORE_FLATTENED_SINGLE_FILE strategy
 */
static perr_t
PDC_Server_data_io_flattened(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                             struct pdc_region_info *region_info, void *buf, size_t unit, int is_write)
{
    FUNC_ENTER(NULL);

    perr_t   ret_value = SUCCEED;
    int      fd;
    char *   data_path = NULL;
    char     storage_location[ADDR_MAX];
    ssize_t  io_size;
    uint64_t i, j;

    if (obj_ndim != (int)region_info->ndim)
        PGOTO_ERROR(FAIL, "Obj dim does not match region dim\n");

    data_path = get_data_path();

    // Data path prefix will be $SCRATCH/pdc_data/$obj_id/server$rank/s$rank.bin
    snprintf(storage_location, ADDR_MAX, "%.200s/pdc_data/%" PRIu64 "/server%d/s%04d.bin", data_path, obj_id,
             PDC_get_rank(), PDC_get_rank());
    PDC_mkdir(storage_location);
    if (strpbrk(storage_location, ";&|`$<>") != NULL)
        PGOTO_ERROR(FAIL, "Invalid characters in storage location");
    fd = open(storage_location, O_RDWR | O_CREAT, 0600);
    if (region_info->ndim == 1) {
        lseek(fd, region_info->offset[0] * unit, SEEK_SET);
        io_size = region_info->size[0] * unit;
        PDC_POSIX_IO(fd, buf, io_size, is_write);
    }
    else if (region_info->ndim == 2) {
        // Check we can directly write the contiguous chunk to the file
        if (region_info->offset[1] == 0 && region_info->size[1] == obj_dims[1]) {
            lseek(fd, region_info->offset[0] * obj_dims[1] * unit, SEEK_SET);
            io_size = region_info->size[0] * obj_dims[1] * unit;
            PDC_POSIX_IO(fd, buf, io_size, is_write);
        }
        else {
            // We have to write line by line
            for (i = 0; i < region_info->size[0]; ++i) {
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
            PDC_POSIX_IO(fd, buf, io_size, is_write);
        }
        else if (region_info->offset[2] == 0 && region_info->size[2] == obj_dims[2]) {
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
    FUNC_LEAVE(ret_value);
}

/**
 * Constructs file name for STORE_FLATTENED_REGION_PER_FILE storage strategy
 * The caller is responbible for freeing the returned pointer
 */
static char *
get_storage_location_region_per_file(int obj_id, int obj_ndim, const uint64_t *indices)
{
    FUNC_ENTER(NULL);

    char *storage_location = PDC_calloc(1, sizeof(char) * ADDR_MAX);
    /**
     * This is the filename suffix
     * Each dimension can be a max of 20 character hence the DIM_MAX * 20
     * Also there is an '_' between each number and NULL terminator hence the DIM_MAX - 1 + 1
     */
    uint32_t storage_location_suffix_max_size = (obj_ndim * 20) + obj_ndim - 1 + 1;
    // this is largest 64 bit number represented in base 10 assci and a NULL terminator hence + 1
    uint32_t uint64_t_max_assci_size = 20 + 1;

    // create temp strings and 0 out the data
    char storage_location_suffix[storage_location_suffix_max_size];
    memset(storage_location_suffix, 0, storage_location_suffix_max_size);
    char num_str[uint64_t_max_assci_size];
    memset(num_str, 0, uint64_t_max_assci_size);

    for (int i = 0; i < obj_ndim; i++) {
        // NOTE: we validated earlier that file_dims[i] != 0
        snprintf(num_str, sizeof(num_str), "%" PRIu64, indices[i]);
        strcat(storage_location_suffix, num_str);
        // dont' add '_' unless there is another character after
        if (i + 1 != obj_ndim)
            strcat(storage_location_suffix, "_");
    }

    // Data path prefix will be $SCRATCH/pdc_data/$obj_id/server$rank/s$rank_$suffix.bin
    snprintf(storage_location, ADDR_MAX, "%.200s/pdc_data/%" PRIu64 "/server%d/s%04d_%s.bin", get_data_path(),
             obj_id, PDC_get_rank(), PDC_get_rank(), storage_location_suffix);

    PDC_mkdir(storage_location);

    FUNC_LEAVE(storage_location);
}

#define FILE_START 0
#define FILE_END   1

/*
 * Converts a multi-dimensional index into a single linear index
 * based on the provided dimensions.
 *
 * This function is used to map N-dimensional coordinates to a 1D index
 * suitable for accessing flattened arrays
 */
static uint64_t
flatten_index(const uint64_t *indices, const uint64_t *dims, int ndim)
{
    FUNC_ENTER(NULL);

    uint64_t ret_value = 0;
    uint64_t stride    = 1;
    for (int i = ndim - 1; i >= 0; i--) {
        ret_value += indices[i] * stride;
        stride *= dims[i];
    }

    FUNC_LEAVE(ret_value);
}

/**
 * Perform I/O for an object region under the STORE_FLATTENED_REGION_PER_FILE strategy.
 *
 * This storage strategy breaks an N-dimensional object into fixed-size "chunks"
 * (sub-regions) that are each stored in separate files. Given an object region
 * (offset + size per dimension), this routine:
 *
 *   1. Determines which file chunks the region intersects with and allocates
 *      temporary buffers to hold their contents.
 *   2. Opens each corresponding file, reading existing data into memory (or
 *      zero-filling if the file does not exist yet).
 *   3. Maps each element of the target region to its correct file buffer and
 *      local offset within that chunk.
 *   4. Copies data between the user buffer and file buffers:
 *        - If `is_write != 0`, copies user buffer -> file buffers.
 *        - If `is_write == 0`, copies file buffers -> user buffer.
 *   5. If writing, writes the modified buffers back to their corresponding files.
 *
 * Parameters:
 *   obj_id      - Identifier of the object being accessed
 *   obj_ndim    - Number of dimensions of the object
 *   obj_dims    - Global dimensions of the object
 *   file_dims   - Per-file chunk dimensions
 *   region_info - Region of the object to read or write
 *   buf         - User buffer for input or output
 *   unit        - Size of a single data element in bytes
 *   is_write    - Nonzero for write, zero for read
 *
 * Returns:
 *   SUCCEED on success, FAIL otherwise.
 */
static perr_t
PDC_Server_data_io_region_per_file(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                   const uint64_t *file_dims, struct pdc_region_info *region_info, void *buf,
                                   size_t unit, int is_write)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    dim, *fds = NULL;
    char **temp_bufs = NULL, *all_temp_bufs = NULL, *temp_buf_ptr = NULL, *user_buf_ptr = NULL,
         *storage_location = NULL;
    uint64_t local_indices[obj_ndim], local_coords[obj_ndim], dims[obj_ndim], total_elements,
        temp_bufs_array[obj_ndim][2], indices[obj_ndim], i, total_files, file_chunk_elements,
        *elem_to_buf_idx = NULL, *elem_to_local_idx = NULL, file_indices_local[obj_ndim], buf_idx, local_idx,
        e, remainder, coords[obj_ndim];
    size_t  file_chunk_bytes;
    ssize_t bytes_read, cur_bytes_read, bytes_written, cur_bytes_written;

    struct timeval _t0, _t1;
    double         total_open_time   = 0.0;
    double         total_pread_time  = 0.0;
    double         total_memcpy_time = 0.0;
    double         total_pwrite_time = 0.0;

    if (obj_ndim != (int)region_info->ndim)
        PGOTO_ERROR(FAIL, "Obj dim does not match region dim");

    for (i = 0; i < (uint64_t)obj_ndim; i++) {
        if (file_dims[i] == 0)
            PGOTO_ERROR(FAIL, "File dimension %d is zero", i);
    }

    // Compute file index spans
    for (i = 0; i < (uint64_t)obj_ndim; i++) {
        temp_bufs_array[i][FILE_START] = region_info->offset[i] / file_dims[i];
        temp_bufs_array[i][FILE_END]   = (region_info->offset[i] + region_info->size[i] - 1) / file_dims[i];
    }

    total_files = 1;
    for (i = 0; i < (uint64_t)obj_ndim; i++) {
        dims[i] = temp_bufs_array[i][FILE_END] - temp_bufs_array[i][FILE_START] + 1;
        total_files *= dims[i];
    }

    file_chunk_elements = PDC_get_region_desc_size(file_dims, obj_ndim);
    file_chunk_bytes    = file_chunk_elements * unit;

    // Fast path: region fits in a single file chunk, write directly
    if (total_files == 1) {
        double t_open, t_io, t_close;

        storage_location = get_storage_location_region_per_file(obj_id, obj_ndim, temp_bufs_array[0]);

        gettimeofday(&_t0, NULL);
        int fd = open(storage_location, O_RDWR | O_CREAT, 0644);
        gettimeofday(&_t1, NULL);
        t_open = (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;

        storage_location = PDC_free(storage_location);
        if (fd < 0)
            PGOTO_ERROR(FAIL, "Failed to open file: %s", strerror(errno));

        gettimeofday(&_t0, NULL);
        if (is_write) {
            bytes_written = 0;
            while ((size_t)bytes_written < file_chunk_bytes) {
                cur_bytes_written =
                    pwrite(fd, (char *)buf + bytes_written, file_chunk_bytes - bytes_written, bytes_written);
                if (cur_bytes_written < 0) {
                    close(fd);
                    PGOTO_ERROR(FAIL, "Fast path pwrite failed: %s", strerror(errno));
                }
                bytes_written += cur_bytes_written;
            }
        }
        else {
            bytes_read = 0;
            while ((size_t)bytes_read < file_chunk_bytes) {
                cur_bytes_read =
                    pread(fd, (char *)buf + bytes_read, file_chunk_bytes - bytes_read, bytes_read);
                if (cur_bytes_read < 0) {
                    close(fd);
                    PGOTO_ERROR(FAIL, "Fast path pread failed: %s", strerror(errno));
                }
                if (cur_bytes_read == 0)
                    break;
                bytes_read += cur_bytes_read;
            }
        }
        gettimeofday(&_t1, NULL);
        t_io = (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;

        gettimeofday(&_t0, NULL);
        close(fd);
        gettimeofday(&_t1, NULL);
        t_close = (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;

        LOG_INFO(
            "region_per_file obj_id=%lu %s FAST PATH: file_chunk=%.2fMB open=%.4fs %s=%.4fs close=%.4fs\n",
            obj_id, (is_write) ? "write" : "read", (double)file_chunk_bytes / (1024.0 * 1024.0), t_open,
            (is_write) ? "pwrite" : "pread", t_io, t_close);
        PGOTO_DONE(SUCCEED);
    }

    LOG_INFO("region_per_file obj_id=%lu %s: total_files=%lu file_chunk=%.2fMB total_data=%.2fMB\n", obj_id,
             (is_write) ? "write" : "read", total_files, (double)file_chunk_bytes / (1024.0 * 1024.0),
             (double)(total_files * file_chunk_bytes) / (1024.0 * 1024.0));

    // Allocate contiguous temp buffer
    all_temp_bufs = PDC_malloc(total_files * file_chunk_bytes);
    temp_bufs     = PDC_malloc(total_files * sizeof(char *));
    for (i = 0; i < total_files; i++)
        temp_bufs[i] = all_temp_bufs + i * file_chunk_bytes;
    fds = PDC_malloc(total_files * sizeof(int));

    // Open files and read contents (pread avoids need for lseek)
    for (i = 0; i < (uint64_t)obj_ndim; i++)
        indices[i] = temp_bufs_array[i][FILE_START];

    for (;;) {
        storage_location = get_storage_location_region_per_file(obj_id, obj_ndim, indices);

        for (int d = 0; d < obj_ndim; d++)
            local_indices[d] = indices[d] - temp_bufs_array[d][FILE_START];
        buf_idx = flatten_index(local_indices, dims, obj_ndim);
        if (buf_idx >= total_files)
            PGOTO_ERROR(FAIL, "buf_idx exceeded total_files");

        errno = 0;
        gettimeofday(&_t0, NULL);
        fds[buf_idx] = open(storage_location, O_RDWR | O_CREAT, 0644);
        gettimeofday(&_t1, NULL);
        total_open_time += (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;

        if (fds[buf_idx] < 0) {
            if (errno == ENOENT) {
                memset(temp_bufs[buf_idx], 0, file_chunk_bytes);
            }
            else {
                storage_location = PDC_free(storage_location);
                PGOTO_ERROR(FAIL, "Failed to open file %s: %s", storage_location, strerror(errno));
            }
        }
        else {
            bytes_read = 0;
            gettimeofday(&_t0, NULL);
            while ((size_t)bytes_read < file_chunk_bytes) {
                cur_bytes_read = pread(fds[buf_idx], temp_bufs[buf_idx] + bytes_read,
                                       file_chunk_bytes - bytes_read, bytes_read);
                if (cur_bytes_read < 0) {
                    close(fds[buf_idx]);
                    PGOTO_ERROR(FAIL, "Failed to read file %s: %s", storage_location, strerror(errno));
                }
                if (cur_bytes_read == 0)
                    break;
                bytes_read += cur_bytes_read;
            }
            gettimeofday(&_t1, NULL);
            total_pread_time += (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;

            if ((size_t)bytes_read < file_chunk_bytes)
                memset(temp_bufs[buf_idx] + bytes_read, 0, file_chunk_bytes - bytes_read);
        }

        storage_location = PDC_free(storage_location);

        // Increment indices multi-dimensionally
        dim = obj_ndim - 1;
        while (dim >= 0) {
            if (++indices[dim] <= temp_bufs_array[dim][FILE_END])
                break;
            indices[dim] = temp_bufs_array[dim][FILE_START];
            dim--;
        }
        if (dim < 0)
            break;
    }

    // Precompute mappings for element-wise phase
    total_elements = 1;
    for (i = 0; i < (uint64_t)obj_ndim; i++)
        total_elements *= region_info->size[i];

    elem_to_buf_idx   = PDC_malloc(total_elements * sizeof(uint64_t));
    elem_to_local_idx = PDC_malloc(total_elements * sizeof(uint64_t));

    gettimeofday(&_t0, NULL);
    for (e = 0; e < total_elements; e++) {
        remainder = e;
        for (int d = obj_ndim - 1; d >= 0; d--) {
            coords[d] = remainder % region_info->size[d];
            remainder /= region_info->size[d];
            coords[d] += region_info->offset[d];
        }
        for (int d = 0; d < obj_ndim; d++) {
            file_indices_local[d] = (coords[d] / file_dims[d]) - temp_bufs_array[d][FILE_START];
            local_coords[d]       = coords[d] % file_dims[d];
        }
        elem_to_buf_idx[e]   = flatten_index(file_indices_local, dims, obj_ndim);
        elem_to_local_idx[e] = flatten_index(local_coords, file_dims, obj_ndim);
    }
    gettimeofday(&_t1, NULL);
    double total_precompute_time = (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;

    // Copy data
    gettimeofday(&_t0, NULL);
    for (e = 0; e < total_elements; e++) {
        buf_idx      = elem_to_buf_idx[e];
        local_idx    = elem_to_local_idx[e];
        temp_buf_ptr = temp_bufs[buf_idx] + local_idx * unit;
        user_buf_ptr = (char *)buf + e * unit;
        if (is_write)
            memcpy(temp_buf_ptr, user_buf_ptr, unit);
        else
            memcpy(user_buf_ptr, temp_buf_ptr, unit);
    }
    gettimeofday(&_t1, NULL);
    total_memcpy_time = (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;

    // Write back files if writing
    if (is_write) {
        for (i = 0; i < (uint64_t)obj_ndim; i++)
            indices[i] = temp_bufs_array[i][FILE_START];

        for (;;) {
            for (int d = 0; d < obj_ndim; d++)
                local_indices[d] = indices[d] - temp_bufs_array[d][FILE_START];
            buf_idx = flatten_index(local_indices, dims, obj_ndim);

            bytes_written = 0;
            gettimeofday(&_t0, NULL);
            while ((size_t)bytes_written < file_chunk_bytes) {
                cur_bytes_written = pwrite(fds[buf_idx], temp_bufs[buf_idx] + bytes_written,
                                           file_chunk_bytes - bytes_written, bytes_written);
                if (cur_bytes_written < 0) {
                    close(fds[buf_idx]);
                    PGOTO_ERROR(FAIL, "Failed to write file descriptor %d: %s", fds[buf_idx],
                                strerror(errno));
                }
                bytes_written += cur_bytes_written;
            }
            gettimeofday(&_t1, NULL);
            total_pwrite_time += (_t1.tv_sec - _t0.tv_sec) + (_t1.tv_usec - _t0.tv_usec) / 1e6;
            close(fds[buf_idx]);

            dim = obj_ndim - 1;
            while (dim >= 0) {
                if (++indices[dim] <= temp_bufs_array[dim][FILE_END])
                    break;
                indices[dim] = temp_bufs_array[dim][FILE_START];
                dim--;
            }
            if (dim < 0)
                break;
        }
    }

    LOG_INFO("region_per_file obj_id=%lu %s DONE: open=%.4fs pread=%.4fs precompute=%.4fs memcpy=%.4fs "
             "pwrite=%.4fs\n",
             obj_id, (is_write) ? "write" : "read", total_open_time, total_pread_time, total_precompute_time,
             total_memcpy_time, total_pwrite_time);
done:
    if (temp_bufs)
        temp_bufs = PDC_free(temp_bufs);
    if (all_temp_bufs)
        all_temp_bufs = PDC_free(all_temp_bufs);
    if (fds)
        fds = PDC_free(fds);
    if (elem_to_buf_idx)
        elem_to_buf_idx = PDC_free(elem_to_buf_idx);
    if (elem_to_local_idx)
        elem_to_local_idx = PDC_free(elem_to_local_idx);

    FUNC_LEAVE(ret_value);
}

/**
 * Used decide how to split object into chunks each of which will be a file on disk
 */
static perr_t
PDC_shrink_file_dims(uint64_t *temp_file_dims, const uint64_t *obj_dims, uint8_t obj_ndim, size_t unit)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    for (int i = 0; i < obj_ndim; i++) {
        temp_file_dims[i] = obj_dims[i];
    }
    // Default to 128  MB
    uint64_t max_bytes_per_file = 32ULL * 4ULL * 1024 * 1024;

    /**
     * We need to reduce the region file size to a reasonable size
     * The file size is malloced in this storage strategy possibly several times
     * So we need to make sure this can fit in memory
     * The following strategy halves the largest dimension until
     * the file size is < max_bytes_per_file
     */
    while (PDC_get_region_desc_size_bytes(temp_file_dims, unit, obj_ndim) > max_bytes_per_file) {
        int max_dim = 0;
        for (int i = 1; i < obj_ndim; i++) {
            if (temp_file_dims[i] > temp_file_dims[max_dim])
                max_dim = i;
        }
        if (temp_file_dims[max_dim] <= 1)
            PGOTO_ERROR(FAIL, "Cannot reduce dimension %d further", max_dim);
        temp_file_dims[max_dim] /= 2;
    }

done:
    FUNC_LEAVE(ret_value);
}

/**
 * Used to flush regions to the storage system
 * If the cache is disabled it is called immediately on PDC_WRITE or PDC_READ
 * If the cache is enabled it is called when evicting regions from the cache
 * or when reading a region into the cache
 */
perr_t
PDC_Server_transfer_request_io(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                               struct pdc_region_info *region_info, void *buf, size_t unit, int is_write,
                               pdc_region_writeout_strategy_t strategy)
{
    FUNC_ENTER(NULL);
    LOG_DEBUG("PDC_Server_transfer_request_io was called\n");
    perr_t ret_value = SUCCEED;
    int    my_rank   = PDC_get_rank();

    // --- Validate input parameters ---
    if (obj_id == 0)
        PGOTO_ERROR(FAIL, "obj_id is zero");
    if (!region_info)
        PGOTO_ERROR(FAIL, "region_info is NULL");
    if (obj_ndim < 0 || obj_ndim > DIM_MAX)
        PGOTO_ERROR(FAIL, "obj_ndim (%d) is invalid", obj_ndim);
    if (!obj_dims && obj_ndim > 0)
        PGOTO_ERROR(FAIL, "obj_dims is NULL but obj_ndim > 0");
    if (!buf)
        PGOTO_ERROR(FAIL, "buf is NULL");
    if (unit == 0)
        PGOTO_ERROR(FAIL, "Unit is zero");

    LOG_INFO("PDC_Server_transfer_request_io: obj_id=%lu strategy=%d\n", obj_id, (int)strategy);

    /**
     * Switch between storage strategies and hand off to correct handler
     */
    if (strategy == STORE_REGION_BY_REGION_SINGLE_FILE || obj_ndim == 0) {
        if (my_rank == 0)
            LOG_INFO("Running %s storage strategy STORE_REGION_BY_REGION_SINGLE_FILE\n",
                     (is_write) ? "write" : "read");
        if (is_write)
            PGOTO_DONE(PDC_Server_data_write_out(obj_id, region_info, buf, unit));
        else
            PGOTO_DONE(PDC_Server_data_read_from(obj_id, region_info, buf, unit));
    }
    else if (strategy == STORE_FLATTENED_SINGLE_FILE) {
        if (my_rank == 0)
            LOG_INFO("Running %s storage strategy STORE_FLATTENED_SINGLE_FILE\n",
                     (is_write) ? "write" : "read");
        PGOTO_DONE(
            PDC_Server_data_io_flattened(obj_id, obj_ndim, obj_dims, region_info, buf, unit, is_write));
    }
    else if (strategy == STORE_FLATTENED_REGION_PER_FILE) {
        uint64_t        temp_file_dims[DIM_MAX];
        pdc_metadata_t *meta      = PDC_Server_get_obj_metadata(obj_id);
        int             use_split = 0;
        if (meta) {
            use_split = 1;
            for (int i = 0; i < obj_ndim; i++) {
                if (meta->obj_split_elems[i] == 0) {
                    use_split = 0;
                    break;
                }
            }
        }
        else {
            LOG_INFO("STORE_FLATTENED_REGION_PER_FILE: failed to get metadata for obj_id %lu, falling back "
                     "to auto split\n",
                     obj_id);
        }
        if (use_split) {
            LOG_INFO("STORE_FLATTENED_REGION_PER_FILE: using user-defined split for obj_id %lu\n", obj_id);
            for (int i = 0; i < obj_ndim; i++) {
                temp_file_dims[i] = (uint64_t)meta->obj_split_elems[i];
                LOG_INFO("  dim[%d]: obj_dim=%lu, file_dim=%lu (user)\n", i, obj_dims[i], temp_file_dims[i]);
            }
        }
        else {
            LOG_INFO("STORE_FLATTENED_REGION_PER_FILE: using auto split for obj_id %lu\n", obj_id);
            if (PDC_shrink_file_dims(temp_file_dims, obj_dims, obj_ndim, unit) != SUCCEED)
                PGOTO_ERROR(FAIL, "Error with PDC_shrink_file_dims");
            for (int i = 0; i < obj_ndim; i++)
                LOG_INFO("  dim[%d]: obj_dim=%lu, file_dim=%lu (auto)\n", i, obj_dims[i], temp_file_dims[i]);
        }
        LOG_INFO("STORE_FLATTENED_REGION_PER_FILE: dispatching %s for obj_id %lu, ndim=%d\n",
                 (is_write) ? "write" : "read", obj_id, obj_ndim);
        PGOTO_DONE(PDC_Server_data_io_region_per_file(obj_id, obj_ndim, obj_dims, temp_file_dims, region_info,
                                                      buf, unit, is_write));
    }
    else
        PGOTO_ERROR(FAIL, "Invalid storage strategy");

done:
    FUNC_LEAVE(ret_value);
}

int
clean_write_bulk_data(transfer_request_all_data *request_data)
{
    FUNC_ENTER(NULL);

    request_data->obj_id        = (pdcid_t *)PDC_free(request_data->obj_id);
    request_data->obj_ndim      = (int *)PDC_free(request_data->obj_ndim);
    request_data->remote_ndim   = (int *)PDC_free(request_data->remote_ndim);
    request_data->remote_offset = (uint64_t **)PDC_free(request_data->remote_offset);
    request_data->unit          = (size_t *)PDC_free(request_data->unit);
    request_data->data_buf      = (char **)PDC_free(request_data->data_buf);

    FUNC_LEAVE(0);
}

int
parse_bulk_data(void *buf, transfer_request_all_data *request_data, pdc_access_t access_type)
{
    FUNC_ENTER(NULL);

    char *   ptr = (char *)buf;
    int      i, j;
    uint64_t data_size;

    // preallocate arrays of size number of objects
    request_data->obj_id            = (pdcid_t *)PDC_malloc(sizeof(pdcid_t) * request_data->n_objs);
    request_data->obj_ndim          = (int *)PDC_malloc(sizeof(int) * request_data->n_objs);
    request_data->remote_ndim       = (int *)PDC_malloc(sizeof(int) * request_data->n_objs);
    request_data->remote_offset     = (uint64_t **)PDC_malloc(sizeof(uint64_t *) * request_data->n_objs * 3);
    request_data->remote_length     = request_data->remote_offset + request_data->n_objs;
    request_data->obj_dims          = request_data->remote_length + request_data->n_objs;
    request_data->unit              = (size_t *)PDC_malloc(sizeof(size_t) * request_data->n_objs);
    request_data->data_buf          = (char **)PDC_malloc(sizeof(char *) * request_data->n_objs);
    request_data->writeout_strategy = (uint8_t *)PDC_malloc(sizeof(uint8_t) * request_data->n_objs);

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
        request_data->writeout_strategy[i] = *((uint8_t *)ptr);
        ptr += sizeof(uint8_t);
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

    FUNC_LEAVE(0);
}
