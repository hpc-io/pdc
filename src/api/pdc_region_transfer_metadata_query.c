#include "pdc_region.h"
#include "pdc_obj.h"
#include "pdc_client_server_common.h"
#include <stdlib.h>
#include <string.h>
#include "pdc_region.h"

typedef struct pdc_region_metadata_pkg {
    uint64_t *                      reg_offset;
    uint64_t *                      reg_size;
    uint32_t                        data_server_id;
    struct pdc_region_metadata_pkg *next;
} pdc_region_metadata_pkg;

typedef struct pdc_obj_metadata_pkg {
    int                          ndim;
    uint64_t                     obj_id;
    pdc_region_metadata_pkg *    regions;
    pdc_region_metadata_pkg *    regions_end;
    struct pdc_obj_metadata_pkg *next;
} pdc_obj_metadata_pkg;

typedef struct pdc_obj_region_metadata {
    uint64_t  obj_id;
    uint64_t *reg_offset;
    uint64_t *reg_size;
    int       ndim;
} pdc_obj_region_metadata;

typedef struct pdc_metadata_query_buf {
    uint64_t                       id;
    char *                         buf;
    struct pdc_metadata_query_buf *next;
} pdc_metadata_query_buf;

static pdc_obj_metadata_pkg *  metadata_server_objs;
static pdc_obj_metadata_pkg *  metadata_server_objs_end;
static uint64_t *              data_server_bytes;
static int                     pdc_server_size;
static uint64_t                query_id_g;
static pdc_metadata_query_buf *metadata_query_buf_head;
static pdc_metadata_query_buf *metadata_query_buf_end;

static perr_t   transfer_request_metadata_reg_append(pdc_region_metadata_pkg *regions, int ndim,
                                                     uint64_t *reg_offset, uint64_t *reg_size, size_t unit,
                                                     uint32_t data_server_id, uint8_t region_partition);
static uint64_t transfer_request_metadata_query_append(uint64_t obj_id, int ndim, uint64_t *reg_offset,
                                                       uint64_t *reg_size, size_t unit,
                                                       uint32_t data_server_id, uint8_t region_partition);
static uint64_t metadata_query_buf_create(pdc_obj_region_metadata *regions, int size,
                                          uint64_t *total_buf_size_ptr);

perr_t
transfer_request_metadata_query_init(int pdc_server_size_input, char *checkpoint)
{
    hg_return_t ret_value = HG_SUCCESS;
    char *      ptr;
    int         n_objs, reg_count;
    int         i, j;
    FUNC_ENTER(NULL);

    metadata_server_objs     = NULL;
    metadata_server_objs_end = NULL;
    metadata_query_buf_head  = NULL;
    metadata_query_buf_end   = NULL;
    data_server_bytes        = (uint64_t *)calloc(pdc_server_size, sizeof(uint64_t));
    pdc_server_size          = pdc_server_size_input;
    query_id_g               = 100000;
    ptr                      = checkpoint;

    if (checkpoint) {
        n_objs = *(int *)ptr;
        ptr += sizeof(int);
        for (i = 0; i < n_objs; ++i) {
            if (metadata_server_objs) {
                metadata_server_objs_end->next =
                    (pdc_obj_metadata_pkg *)malloc(sizeof(pdc_obj_region_metadata));
                metadata_server_objs_end = metadata_server_objs_end->next;
            }
            else {
                metadata_server_objs     = (pdc_obj_metadata_pkg *)malloc(sizeof(pdc_obj_region_metadata));
                metadata_server_objs_end = metadata_server_objs;
            }

            metadata_server_objs_end->obj_id = *(uint64_t *)ptr;
            ptr += sizeof(uint64_t);
            metadata_server_objs_end->ndim = *(int *)ptr;
            ptr += sizeof(int);
            reg_count = *(int *)ptr;
            ptr += sizeof(int);

            metadata_server_objs_end->regions =
                (pdc_region_metadata_pkg *)malloc(sizeof(pdc_region_metadata_pkg));
            metadata_server_objs_end->regions_end = metadata_server_objs_end->regions;

            metadata_server_objs_end->regions_end->next = NULL;
            metadata_server_objs_end->regions_end->reg_offset =
                (uint64_t *)malloc(sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
            metadata_server_objs_end->regions_end->reg_size =
                metadata_server_objs_end->regions_end->reg_offset + metadata_server_objs_end->ndim;
            metadata_server_objs_end->regions_end->data_server_id = *(uint32_t *)ptr;
            ptr += sizeof(uint32_t);
            memcpy(metadata_server_objs_end->regions_end->reg_offset, ptr,
                   sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
            ptr += sizeof(uint64_t) * metadata_server_objs_end->ndim * 2;

            for (j = 1; j < reg_count; ++j) {
                metadata_server_objs_end->regions->next =
                    (pdc_region_metadata_pkg *)malloc(sizeof(pdc_region_metadata_pkg));
                metadata_server_objs_end->regions_end = metadata_server_objs_end->regions_end->next;

                metadata_server_objs_end->regions_end->next = NULL;
                metadata_server_objs_end->regions_end->reg_offset =
                    (uint64_t *)malloc(sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
                metadata_server_objs_end->regions_end->reg_size =
                    metadata_server_objs_end->regions_end->reg_offset + metadata_server_objs_end->ndim;
                metadata_server_objs_end->regions_end->data_server_id = *(uint32_t *)ptr;
                ptr += sizeof(uint32_t);
                memcpy(metadata_server_objs_end->regions_end->reg_offset, ptr,
                       sizeof(uint64_t) * metadata_server_objs_end->ndim * 2);
                ptr += sizeof(uint64_t) * metadata_server_objs_end->ndim * 2;
            }
        }
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
transfer_request_metadata_query_checkpoint(char **checkpoint, uint64_t *checkpoint_size)
{
    hg_return_t              ret_value = HG_SUCCESS;
    pdc_obj_metadata_pkg *   obj_temp, *obj_temp2;
    pdc_region_metadata_pkg *region_temp, *region_temp2;
    char *                   ptr;
    int                      reg_count, obj_count;
    FUNC_ENTER(NULL);
    // First value is the size of objects
    *checkpoint_size = sizeof(int);
    obj_count        = 0;

    obj_temp = metadata_server_objs;
    while (obj_temp) {
        // ndim + region count + object ID
        *checkpoint_size += sizeof(int) * 2 + sizeof(uint64_t);
        region_temp = obj_temp->regions;
        while (region_temp) {
            // data server ID + region information
            *checkpoint_size += sizeof(uint32_t) + sizeof(uint64_t) * 2 * obj_temp->ndim;
            region_temp = region_temp->next;
        }
        obj_temp = obj_temp->next;
        obj_count++;
    }
    *checkpoint = (char *)malloc(*checkpoint_size);
    ptr         = *checkpoint;
    memcpy(ptr, &obj_count, sizeof(int));
    ptr += sizeof(int);

    obj_temp = metadata_server_objs;
    while (obj_temp) {
        memcpy(ptr, &(obj_temp->obj_id), sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        memcpy(ptr, &(obj_temp->ndim), sizeof(int));
        ptr += sizeof(int);

        reg_count   = 0;
        region_temp = obj_temp->regions;
        while (region_temp) {
            reg_count++;
            region_temp = region_temp->next;
        }
        memcpy(ptr, &reg_count, sizeof(int));
        ptr += sizeof(int);

        region_temp = obj_temp->regions;
        while (region_temp) {
            memcpy(ptr, &(region_temp->data_server_id), sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            memcpy(ptr, &(region_temp->reg_offset), sizeof(uint64_t) * obj_temp->ndim * 2);
            ptr += sizeof(uint64_t) * obj_temp->ndim * 2;
            region_temp2 = region_temp;
            region_temp  = region_temp->next;
            free(region_temp2->reg_offset);
            free(region_temp2);
        }
        obj_temp2 = obj_temp;
        obj_temp  = obj_temp->next;
        free(obj_temp2);
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * Wrap the overlapping portions for each of the regions into a contiguous buffer.
 * Output is an ID that can be used to trace this buffer.
 */
static uint64_t
metadata_query_buf_create(pdc_obj_region_metadata *regions, int size, uint64_t *total_buf_size_ptr)
{
    pdc_obj_metadata_pkg *   temp;
    pdc_region_metadata_pkg *region_metadata;
    int                      i;
    uint64_t                 total_data_size;
    pdc_metadata_query_buf * query_buf;
    uint64_t                 query_id;
    uint64_t *               overlap_offset, *overlap_size;
    char *                   ptr;
    int *                    transfer_request_counters;
    int                      transfer_request_counter_total;

    FUNC_ENTER(NULL);
    // Iterate through all input regions. We compute the total buf size in this loop
    total_data_size                = sizeof(int);
    transfer_request_counter_total = 0;
    transfer_request_counters      = (int *)calloc(size, sizeof(int));
    for (i = 0; i < size; ++i) {
        temp = metadata_server_objs;
        // First check which obj list
        while (temp) {
            if (temp->obj_id == regions[i].obj_id) {
                break;
            }
            temp = temp->next;
        }
        // IF found, we compare all regions and see if there are any overlaps.
        if (temp) {
            region_metadata = temp->regions;
            while (region_metadata) {
                if (check_overlap(regions[i].ndim, region_metadata->reg_offset, region_metadata->reg_size,
                                  regions[i].reg_offset, regions[i].reg_size)) {
                    // How many regions this transfer request overlaps with.
                    transfer_request_counters[i]++;
                }
                // Data server ID + region offset + region size
                region_metadata = region_metadata->next;
            }
            transfer_request_counter_total += transfer_request_counters[i];
            total_data_size += sizeof(int) + transfer_request_counters[i] *
                                                 (sizeof(uint32_t) + sizeof(uint64_t) * regions[i].ndim * 2);
        }
        else {
            printf("metadata_query_buf_create: Unable to find the object with ID %lu\n",
                   (long int)regions[i].obj_id);
        }
    }
    if (!total_data_size) {
        query_id = 0;
        goto done;
    }

    query_buf = (pdc_metadata_query_buf *)malloc(sizeof(pdc_metadata_query_buf));
    // Free query_buf->buf in transfer_request_metadata_query2_bulk_transfer_cb.
    query_buf->buf  = (char *)malloc(total_data_size);
    query_buf->next = NULL;
    query_buf->id   = query_id_g;
    query_id_g++;
    ptr = query_buf->buf;
    // printf("metadata_query_buf_create query_id = %lu, size of buf = %lu, buf address = %lld @ line %d\n",
    // query_buf->id, total_data_size, (long long int)query_buf, __LINE__);
    memcpy(ptr, &transfer_request_counter_total, sizeof(int));
    ptr += sizeof(int);
    // printf("metadata_query_buf_create transfer_request_counter_total = %d @ %d\n",
    // transfer_request_counter_total, __LINE__);
    // Iterate through all input regions. We fill in the buffer.
    for (i = 0; i < size; ++i) {
        temp = metadata_server_objs;
        // First check which obj list
        while (temp) {
            if (temp->obj_id == regions[i].obj_id) {
                break;
            }
            temp = temp->next;
        }
        // IF found, we compare all regions and see if there are any overlaps.
        memcpy(ptr, transfer_request_counters + i, sizeof(int));
        ptr += sizeof(int);
        if (temp) {
            region_metadata = temp->regions;
            while (region_metadata) {
                PDC_region_overlap_detect(regions[i].ndim, region_metadata->reg_offset,
                                          region_metadata->reg_size, regions[i].reg_offset,
                                          regions[i].reg_size, &overlap_offset, &overlap_size);
                if (overlap_offset) {
                    // data_server_id + region offset + region size
                    memcpy(ptr, &(region_metadata->data_server_id), sizeof(uint32_t));
                    ptr += sizeof(uint32_t);
                    memcpy(ptr, overlap_offset, sizeof(uint64_t) * regions[i].ndim);
                    ptr += sizeof(uint64_t) * regions[i].ndim;
                    memcpy(ptr, overlap_size, sizeof(uint64_t) * regions[i].ndim);
                    ptr += sizeof(uint64_t) * regions[i].ndim;
                    // printf("overlapping offset = %lu, overlapping size = %lu\n", overlap_offset[0],
                    // overlap_size[0]);
                }
                // overlap_size is freed together.
                free(overlap_offset);
                region_metadata = region_metadata->next;
            }
        }
    }
    if (metadata_query_buf_head) {
        metadata_query_buf_end->next = query_buf;
        metadata_query_buf_end       = query_buf;
    }
    else {
        metadata_query_buf_head = query_buf;
        metadata_query_buf_end  = query_buf;
    }
    query_id = query_buf->id;
done:
    *total_buf_size_ptr = total_data_size;

    free(transfer_request_counters);
    FUNC_LEAVE(query_id);
}

/**
 * Find previously stored query buffer and return the buffer. Delete the entry from the linked list.
 */

perr_t
transfer_request_metadata_query_lookup_query_buf(uint64_t query_id, char **buf_ptr)
{
    pdc_metadata_query_buf *metadata_query, *previous;
    perr_t                  ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    previous       = NULL;
    int i          = 0;
    metadata_query = metadata_query_buf_head;
    while (metadata_query) {
        if (metadata_query->id == query_id) {
            *buf_ptr = metadata_query->buf;

            if (metadata_query_buf_head == metadata_query) {
                metadata_query_buf_head = metadata_query_buf_head->next;
            }
            else {
                previous->next = metadata_query->next;
            }
            if (metadata_query_buf_end == metadata_query) {
                metadata_query_buf_end = previous;
            }
            free(metadata_query);
            goto done;
        }
        i++;
        previous       = metadata_query;
        metadata_query = metadata_query->next;
    }
    *buf_ptr = NULL;
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/*
 * We generate a metadata_query ID for later referencing. Parse input buffer and scan local metadata.
 * Output: query ID, the query entry contains obj ID, data server ID and overlapping region (client can
 * directly use these information to forward its requests)
 */
uint64_t
transfer_request_metadata_query_parse(int32_t n_objs, char *buf, uint8_t is_write,
                                      uint64_t *total_buf_size_ptr)
{
    char *                   ptr = buf;
    int                      i;
    uint64_t                 query_id = 0;
    size_t                   unit;
    uint64_t                 data_server_id;
    uint8_t                  region_partition;
    pdc_obj_region_metadata *region_metadata =
        (pdc_obj_region_metadata *)malloc(sizeof(pdc_obj_region_metadata) * n_objs);

    FUNC_ENTER(NULL);
    for (i = 0; i < n_objs; ++i) {
        region_metadata[i].obj_id = *((uint64_t *)ptr);
        ptr += sizeof(uint64_t);
        data_server_id = *((uint32_t *)ptr);
        ptr += sizeof(uint32_t);
        region_partition = *((uint8_t *)ptr);
        ptr += sizeof(uint8_t);
        region_metadata[i].ndim = *((int *)ptr);
        ptr += sizeof(int);
        unit = *((size_t *)ptr);
        ptr += sizeof(size_t);
        region_metadata[i].reg_offset = (uint64_t *)ptr;
        ptr += sizeof(uint64_t) * region_metadata[i].ndim;
        region_metadata[i].reg_size = (uint64_t *)ptr;
        ptr += sizeof(uint64_t) * region_metadata[i].ndim;
        if (is_write) {
            transfer_request_metadata_query_append(region_metadata[i].obj_id, region_metadata[i].ndim,
                                                   region_metadata[i].reg_offset, region_metadata[i].reg_size,
                                                   unit, data_server_id, region_partition);
        }
    }
    // printf("transfer_request_metadata_query_parse: checkpoint %d\n", __LINE__);
    query_id = metadata_query_buf_create(region_metadata, n_objs, total_buf_size_ptr);
    free(region_metadata);
    // printf("transfer_request_metadata_query_parse: checkpoint %d\n", __LINE__);
    fflush(stdout);
    FUNC_LEAVE(query_id);
}

static perr_t
transfer_request_metadata_reg_append(pdc_region_metadata_pkg *regions, int ndim, uint64_t *reg_offset,
                                     uint64_t *reg_size, size_t unit, uint32_t data_server_id,
                                     uint8_t region_partition)
{
    hg_return_t ret_value = HG_SUCCESS;
    uint64_t    min_bytes;
    uint64_t    min_bytes_server;
    int         i;
    uint64_t    total_reg_size;
    FUNC_ENTER(NULL);

    regions->next = NULL;

    regions->reg_offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim * 2);
    regions->reg_size   = regions->reg_offset + ndim;

    memcpy(regions->reg_offset, reg_offset, sizeof(uint64_t) * ndim);
    memcpy(regions->reg_size, reg_size, sizeof(uint64_t) * ndim);

    if (region_partition == PDC_REGION_DYNAMIC) {
        min_bytes        = data_server_bytes[0];
        min_bytes_server = 0;

        for (i = 0; i < pdc_server_size; ++i) {
            if (min_bytes < data_server_bytes[i]) {
                min_bytes        = data_server_bytes[i];
                min_bytes_server = i;
            }
        }

        regions->data_server_id = min_bytes_server;
        total_reg_size          = unit;
        for (i = 0; i < ndim; ++i) {
            total_reg_size *= reg_size[i];
        }

        data_server_bytes[min_bytes_server] += total_reg_size;
    }
    else {
        regions->data_server_id = data_server_id;
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static uint64_t
transfer_request_metadata_query_append(uint64_t obj_id, int ndim, uint64_t *reg_offset, uint64_t *reg_size,
                                       size_t unit, uint32_t data_server_id, uint8_t region_partition)
{
    pdc_obj_metadata_pkg *   temp;
    pdc_region_metadata_pkg *region_metadata;
    pdc_region_metadata_pkg *temp_region_metadata;

    FUNC_ENTER(NULL);
    // printf("transfer_request_metadata_query_append: checkpoint %d\n", __LINE__);
    temp = metadata_server_objs;
    while (temp) {
        if (temp->obj_id == obj_id) {
            break;
        }
        temp = temp->next;
    }
    // printf("transfer_request_metadata_query_append: checkpoint %d\n", __LINE__);
    if (temp == NULL) {
        temp = (pdc_obj_metadata_pkg *)malloc(sizeof(pdc_obj_metadata_pkg));
        if (metadata_server_objs) {
            metadata_server_objs_end->next = temp;
            metadata_server_objs_end       = temp;
        }
        else {
            metadata_server_objs     = temp;
            metadata_server_objs_end = temp;
        }
        metadata_server_objs_end->regions     = NULL;
        metadata_server_objs_end->regions_end = NULL;
        metadata_server_objs_end->next        = NULL;
        metadata_server_objs_end->obj_id      = obj_id;
        metadata_server_objs_end->ndim        = ndim;
    }
    // printf("transfer_request_metadata_query_append: checkpoint %d\n", __LINE__);
    region_metadata = temp->regions;
    while (region_metadata) {
        if (detect_region_contained(reg_offset, reg_size, region_metadata->reg_offset,
                                    region_metadata->reg_size, ndim)) {
            // printf("%lu, %lu, %lu ,%lu\n", reg_offset[0], reg_size[0], region_metadata->reg_offset[0],
            //       region_metadata->reg_size[0]);
            // printf("---------------transfer_request_metadata_query_append: detected repeated requests\n");
            FUNC_LEAVE(region_metadata->data_server_id);
        }
        region_metadata = region_metadata->next;
    }
    // Reaching this line means that we are creating a new region and append it to the end of the object list.
    // printf("transfer_request_metadata_query_append: checkpoint %d\n", __LINE__);
    temp_region_metadata = (pdc_region_metadata_pkg *)malloc(sizeof(pdc_region_metadata_pkg));
    if (temp->regions) {
        temp->regions_end->next = temp_region_metadata;
        temp->regions_end       = temp_region_metadata;
    }
    else {
        temp->regions     = temp_region_metadata;
        temp->regions_end = temp_region_metadata;
    }
    transfer_request_metadata_reg_append(temp_region_metadata, ndim, reg_offset, reg_size, unit,
                                         data_server_id, region_partition);
    // printf("transfer_request_metadata_query_append: checkpoint %d\n", __LINE__);
    fflush(stdout);
    FUNC_LEAVE(temp->regions_end->data_server_id);
}
