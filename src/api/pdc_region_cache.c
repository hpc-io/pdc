#include "pdc_region_cache.h"
#include "pdc_timing.h"

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
 * If two regions overlap with each other, region 2 override the contents occupied by region 1.
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
            // Copy data for region 1 that does not overlap with region 2
            memcpy(*buf_merged, buf, unit * (offset2[connect_flag] - offset[connect_flag]));
            *buf_merged += unit * (offset2[connect_flag] - offset[connect_flag]);
            // Copy data for region 2 that does not overlap with region 1.
            memcpy(*buf_merged, buf2, unit * size2[connect_flag]);
            *buf_merged += unit * size2[connect_flag];
            // Copy overlaping region with region 2 data
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
PDC_region_cache_register(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims, const char *buf,
                          size_t buf_size, const uint64_t *offset, const uint64_t *size, int ndim,
                          size_t unit)
{
    pdc_obj_cache *         obj_cache_iter, *obj_cache = NULL;
    struct pdc_region_info *region_cache_info;
    if (obj_ndim != ndim && obj_ndim > 0) {
        printf("PDC_region_cache_register reports obj_ndim != ndim, %d != %d\n", obj_ndim, ndim);
    }

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
        obj_cache_list_end->ndim = obj_ndim;
        if (obj_ndim) {
            obj_cache_list_end->dims = (uint64_t *)malloc(sizeof(uint64_t) * obj_ndim);
            memcpy(obj_cache_list_end->dims, obj_dims, sizeof(uint64_t) * obj_ndim);
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
PDC_transfer_request_data_write_out(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                    struct pdc_region_info *region_info, void *buf, size_t unit)
{
    int               flag;
    pdc_obj_cache *   obj_cache, *obj_cache_iter;
    pdc_region_cache *region_cache_iter;
    // char *            buf_merged;
    // uint64_t *        offset_merged, size_merged;
    // int               merge_status;

    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);
#ifdef PDC_TIMING
    double start = MPI_Wtime();
#endif

    // Write 1GB at a time

    uint64_t write_size = 0;
    if (region_info->ndim >= 1)
        write_size = unit * region_info->size[0];
    if (region_info->ndim >= 2)
        write_size *= region_info->size[1];
    if (region_info->ndim >= 3)
        write_size *= region_info->size[2];

    pthread_mutex_lock(&pdc_obj_cache_list_mutex);

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
            /*
             else {
                            merge_status = PDC_region_merge(buf, region_cache_iter->region_cache_info->buf,
             region_info->offset, region_info->size, region_cache_iter->region_cache_info->offset,
             region_cache_iter->region_cache_info->size, &buf_merged, &offset_merged, &size_merged, ndim,
             unit); if ( merge_status == PDC_MERGE_SUCCESS ) {

                            }
                        }
            */
            region_cache_iter = region_cache_iter->next;
        }
    }
    if (flag) {
        PDC_region_cache_register(obj_id, obj_ndim, obj_dims, buf, write_size, region_info->offset,
                                  region_info->size, region_info->ndim, unit);
        /*
                PDC_Server_transfer_request_io(obj_id, obj_ndim, obj_dims, region_info,
                                               buf, unit, 1);
        */
    }
    pthread_mutex_unlock(&pdc_obj_cache_list_mutex);

    // PDC_Server_data_write_out2(obj_id, region_info, buf, unit);
#ifdef PDC_TIMING
    server_timings->PDCcache_write += MPI_Wtime() - start;
#endif

    // done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

int
PDC_region_cache_flush_by_pointer(uint64_t obj_id, pdc_obj_cache *obj_cache)
{
    pdc_region_cache *      region_cache_iter, *region_cache_temp;
    struct pdc_region_info *region_cache_info;
    region_cache_iter = obj_cache->region_cache;
    while (region_cache_iter != NULL) {
        region_cache_info = region_cache_iter->region_cache_info;
        PDC_Server_transfer_request_io(obj_id, obj_cache->ndim, obj_cache->dims, region_cache_info,
                                       region_cache_info->buf, region_cache_info->unit, 1);
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
PDC_region_cache_flush(uint64_t obj_id)
{
    pdc_obj_cache *obj_cache = NULL, *obj_cache_iter;

    obj_cache_iter = obj_cache_list;
    while (obj_cache_iter != NULL) {
        if (obj_cache_iter->obj_id == obj_id) {
            obj_cache = obj_cache_iter;
            break;
        }
        obj_cache_iter = obj_cache_iter->next;
    }
    if (obj_cache == NULL) {
        // printf("server error: flushing object that does not exist\n");
        return 1;
    }
    PDC_region_cache_flush_by_pointer(obj_id, obj_cache);
    return 0;
}

int
PDC_region_cache_flush_all()
{
    pdc_obj_cache *obj_cache_iter, *obj_cache_temp;
    pthread_mutex_lock(&pdc_obj_cache_list_mutex);

    obj_cache_iter = obj_cache_list;
    while (obj_cache_iter != NULL) {
        PDC_region_cache_flush_by_pointer(obj_cache_iter->obj_id, obj_cache_iter);
        obj_cache_temp = obj_cache_iter;
        obj_cache_iter = obj_cache_iter->next;
        if (obj_cache_temp->ndim) {
            free(obj_cache_temp->dims);
        }

        free(obj_cache_temp);
    }
    obj_cache_list = NULL;
    pthread_mutex_unlock(&pdc_obj_cache_list_mutex);
    return 0;
}

void *
PDC_region_cache_clock_cycle(void *ptr)
{
    //    pdc_obj_cache *obj_cache, *obj_cache_iter;
    //    struct timeval current_time;
    if (ptr == NULL) {
        obj_cache_iter = NULL;
    }
    while (1) {
        pthread_mutex_lock(&pdc_cache_mutex);
        if (!pdc_recycle_close_flag) {
            /*
                        pthread_mutex_lock(&pdc_obj_cache_list_mutex);
                        gettimeofday(&current_time, NULL);
                        obj_cache_iter = obj_cache_list;
                        while (obj_cache_iter != NULL) {
                            obj_cache = obj_cache_iter;
                            if (current_time.tv_sec - obj_cache->timestamp.tv_sec > 10) {
                                PDC_region_cache_flush_by_pointer(obj_cache->obj_id, obj_cache);
                            }
                            obj_cache_iter = obj_cache_iter->next;
                        }
                        pthread_mutex_unlock(&pdc_obj_cache_list_mutex);
            */
        }
        else {
            pthread_mutex_unlock(&pdc_cache_mutex);
            break;
        }
        pthread_mutex_unlock(&pdc_cache_mutex);
        sleep(1);
    }
    return 0;
}

perr_t
PDC_transfer_request_data_read_from(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                    struct pdc_region_info *region_info, void *buf, size_t unit)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);
#ifdef PDC_TIMING
    double start = MPI_Wtime();
#endif
    // PDC_Server_data_read_from2(obj_id, region_info, buf, unit);
    pthread_mutex_lock(&pdc_obj_cache_list_mutex);
    PDC_region_fetch(obj_id, obj_ndim, obj_dims, region_info, buf, unit);
    pthread_mutex_unlock(&pdc_obj_cache_list_mutex);

#ifdef PDC_TIMING
    server_timings->PDCcache_read += MPI_Wtime() - start;
#endif
    // done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * This function search for an object cache by ID, then copy data from the region to buf if the request region
 * is fully contained inside the cache region.
 */
int
PDC_region_fetch(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims, struct pdc_region_info *region_info,
                 void *buf, size_t unit)
{
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
        if (obj_cache != NULL) {
            PDC_region_cache_flush_by_pointer(obj_id, obj_cache);
        }
        PDC_Server_transfer_request_io(obj_id, obj_ndim, obj_dims, region_info, buf, unit, 0);
    }
    return 0;
}
#endif
