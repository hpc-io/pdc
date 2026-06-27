#include "pdc_obj.h"
#include "pdc_client_server_common.h"
#include <stdlib.h>
#include <string.h>
#include "pdc_region.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

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
static pthread_mutex_t         metadata_query_mutex;

static perr_t   transfer_request_metadata_reg_append(pdc_region_metadata_pkg *regions, int ndim,
                                                     uint64_t *reg_offset, uint64_t *reg_size, size_t unit,
                                                     uint32_t data_server_id, uint8_t region_partition);
static uint64_t transfer_request_metadata_query_append(uint64_t obj_id, int ndim, uint64_t *reg_offset,
                                                       uint64_t *reg_size, size_t unit,
                                                       uint32_t data_server_id, uint8_t region_partition);
static uint64_t metadata_query_buf_create(pdc_obj_region_metadata *regions, int size,
                                          uint64_t *total_buf_size_ptr);

perr_t
transfer_request_metadata_query_init_bulki(int pdc_server_size_input, BULKI *checkpoint_bulki)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    metadata_server_objs     = NULL;
    metadata_server_objs_end = NULL;
    metadata_query_buf_head  = NULL;
    metadata_query_buf_end   = NULL;
    pdc_server_size          = pdc_server_size_input;
    data_server_bytes        = (uint64_t *)PDC_calloc(pdc_server_size, sizeof(uint64_t));
    query_id_g               = 100000;

    pthread_mutex_init(&metadata_query_mutex, NULL);

    if (checkpoint_bulki != NULL) {
        BULKI_Entity *objects_array =
            BULKI_get(checkpoint_bulki, BULKI_singleton_ENTITY("objects", PDC_STRING));

        if (objects_array == NULL || objects_array->pdc_type != PDC_BULKI) {
            LOG_ERROR("Invalid transfer query checkpoint: missing or invalid 'objects' field\n");
            PGOTO_ERROR(FAIL, "Invalid checkpoint format");
        }

        BULKI_Entity_Iterator *obj_iter = Bent_iterator_init(objects_array, NULL, PDC_BULKI);

        while (Bent_iterator_has_next_BULKI(obj_iter)) {
            BULKI *obj_bulki = Bent_iterator_next_BULKI(obj_iter);

            pdc_obj_metadata_pkg *obj_pkg = (pdc_obj_metadata_pkg *)PDC_malloc(sizeof(pdc_obj_metadata_pkg));

            BULKI_Entity *obj_id_ent = BULKI_get(obj_bulki, BULKI_singleton_ENTITY("obj_id", PDC_STRING));
            if (obj_id_ent == NULL) {
                LOG_ERROR("Missing obj_id in checkpoint object\n");
                PDC_free(obj_pkg);
                continue;
            }
            memcpy(&obj_pkg->obj_id, obj_id_ent->data, sizeof(uint64_t));

            BULKI_Entity *ndim_ent = BULKI_get(obj_bulki, BULKI_singleton_ENTITY("ndim", PDC_STRING));
            if (ndim_ent == NULL) {
                LOG_ERROR("Missing ndim in checkpoint object\n");
                PDC_free(obj_pkg);
                continue;
            }
            memcpy(&obj_pkg->ndim, ndim_ent->data, sizeof(int));

            obj_pkg->regions     = NULL;
            obj_pkg->regions_end = NULL;
            obj_pkg->next        = NULL;

            BULKI_Entity *regions_array = BULKI_get(obj_bulki, BULKI_singleton_ENTITY("regions", PDC_STRING));

            if (regions_array != NULL && regions_array->pdc_type == PDC_BULKI) {
                BULKI_Entity_Iterator *region_iter = Bent_iterator_init(regions_array, NULL, PDC_BULKI);

                while (Bent_iterator_has_next_BULKI(region_iter)) {
                    BULKI *region_bulki = Bent_iterator_next_BULKI(region_iter);

                    pdc_region_metadata_pkg *region_pkg =
                        (pdc_region_metadata_pkg *)PDC_malloc(sizeof(pdc_region_metadata_pkg));

                    region_pkg->reg_offset = (uint64_t *)PDC_malloc(sizeof(uint64_t) * obj_pkg->ndim * 2);
                    region_pkg->reg_size   = region_pkg->reg_offset + obj_pkg->ndim;

                    BULKI_Entity *server_id_ent =
                        BULKI_get(region_bulki, BULKI_singleton_ENTITY("data_server_id", PDC_STRING));
                    if (server_id_ent != NULL) {
                        memcpy(&region_pkg->data_server_id, server_id_ent->data, sizeof(uint32_t));
                    }
                    else {
                        LOG_ERROR("Missing data_server_id in checkpoint region\n");
                        PDC_free(region_pkg->reg_offset);
                        PDC_free(region_pkg);
                        continue;
                    }

                    BULKI_Entity *offset_size_ent =
                        BULKI_get(region_bulki, BULKI_singleton_ENTITY("reg_offset_size", PDC_STRING));
                    if (offset_size_ent != NULL) {
                        memcpy(region_pkg->reg_offset, offset_size_ent->data,
                               sizeof(uint64_t) * obj_pkg->ndim * 2);
                    }
                    else {
                        LOG_ERROR("Missing reg_offset_size in checkpoint region\n");
                        PDC_free(region_pkg->reg_offset);
                        PDC_free(region_pkg);
                        continue;
                    }

                    region_pkg->next = NULL;

                    if (obj_pkg->regions == NULL) {
                        obj_pkg->regions     = region_pkg;
                        obj_pkg->regions_end = region_pkg;
                    }
                    else {
                        obj_pkg->regions_end->next = region_pkg;
                        obj_pkg->regions_end       = region_pkg;
                    }
                }
            }

            if (metadata_server_objs == NULL) {
                metadata_server_objs     = obj_pkg;
                metadata_server_objs_end = obj_pkg;
            }
            else {
                metadata_server_objs_end->next = obj_pkg;
                metadata_server_objs_end       = obj_pkg;
            }
        }

        LOG_DEBUG("Transfer query checkpoint restored successfully\n");
    }

done:
    FUNC_LEAVE(ret_value);
}

/**
 * Entry function for this class. Should be only called once at the beginning of Server init.
 * If checkpoint is not NULL, then load previously checkpointed metadata to static variables.
 */
perr_t
transfer_request_metadata_query_init(int pdc_server_size_input, char *checkpoint)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    char *      ptr;
    int         n_objs, reg_count, ndim;
    int         i, j;

    metadata_server_objs     = NULL;
    metadata_server_objs_end = NULL;
    metadata_query_buf_head  = NULL;
    metadata_query_buf_end   = NULL;
    pdc_server_size          = pdc_server_size_input;
    data_server_bytes        = (uint64_t *)PDC_calloc(pdc_server_size, sizeof(uint64_t));
    query_id_g               = 100000;
    ptr                      = checkpoint;
    pthread_mutex_init(&metadata_query_mutex, NULL);

    if (checkpoint) {
        n_objs = *(int *)ptr;
        if (n_objs <= 0 || n_objs > 1000000) {
            LOG_ERROR("transfer_request_metadata_query_init: invalid n_objs %d\n", n_objs);
            FUNC_LEAVE(FAIL);
        }
        ptr += sizeof(int);
        for (i = 0; i < n_objs; ++i) {
            if (metadata_server_objs) {
                metadata_server_objs_end->next =
                    (pdc_obj_metadata_pkg *)PDC_malloc(sizeof(pdc_obj_metadata_pkg));
                metadata_server_objs_end = metadata_server_objs_end->next;
            }
            else {
                metadata_server_objs     = (pdc_obj_metadata_pkg *)PDC_malloc(sizeof(pdc_obj_metadata_pkg));
                metadata_server_objs_end = metadata_server_objs;
            }

            metadata_server_objs_end->obj_id = *(uint64_t *)ptr;
            ptr += sizeof(uint64_t);
            /* Read ndim into a local, validate it, then use the validated local
             * for every allocation/copy so the bound is provable to analysis. */
            ndim = *(int *)ptr;
            ptr += sizeof(int);
            if (ndim <= 0 || ndim > 4) {
                LOG_ERROR("Invalid ndim %d\n", ndim);
                FUNC_LEAVE(FAIL);
            }
            metadata_server_objs_end->ndim = ndim;
            reg_count                      = *(int *)ptr;
            ptr += sizeof(int);
            if (reg_count < 0 || reg_count > 1000000) {
                LOG_ERROR("Invalid reg_count %d\n", reg_count);
                FUNC_LEAVE(FAIL);
            }

            metadata_server_objs_end->regions =
                (pdc_region_metadata_pkg *)PDC_malloc(sizeof(pdc_region_metadata_pkg));
            metadata_server_objs_end->regions_end = metadata_server_objs_end->regions;

            metadata_server_objs_end->regions_end->next = NULL;
            metadata_server_objs_end->regions_end->reg_offset =
                (uint64_t *)PDC_malloc(sizeof(uint64_t) * (size_t)ndim * 2);
            metadata_server_objs_end->regions_end->reg_size =
                metadata_server_objs_end->regions_end->reg_offset + ndim;
            metadata_server_objs_end->regions_end->data_server_id = *(uint32_t *)ptr;
            ptr += sizeof(uint32_t);
            memcpy(metadata_server_objs_end->regions_end->reg_offset, ptr,
                   sizeof(uint64_t) * (size_t)ndim * 2);
            ptr += sizeof(uint64_t) * (size_t)ndim * 2;

            for (j = 1; j < reg_count; ++j) {
                metadata_server_objs_end->regions->next =
                    (pdc_region_metadata_pkg *)PDC_malloc(sizeof(pdc_region_metadata_pkg));
                metadata_server_objs_end->regions_end = metadata_server_objs_end->regions_end->next;

                metadata_server_objs_end->regions_end->next = NULL;
                metadata_server_objs_end->regions_end->reg_offset =
                    (uint64_t *)PDC_malloc(sizeof(uint64_t) * (size_t)ndim * 2);
                metadata_server_objs_end->regions_end->reg_size =
                    metadata_server_objs_end->regions_end->reg_offset + ndim;
                metadata_server_objs_end->regions_end->data_server_id = *(uint32_t *)ptr;
                ptr += sizeof(uint32_t);
                memcpy(metadata_server_objs_end->regions_end->reg_offset, ptr,
                       sizeof(uint64_t) * (size_t)ndim * 2);
                ptr += sizeof(uint64_t) * (size_t)ndim * 2;
            }
        }
    }

    FUNC_LEAVE(ret_value);
}

/**
 * Finalize function of this class. Should be called only once at the end of Server finalize.
 */
perr_t
transfer_request_metadata_query_finalize()
{
    FUNC_ENTER(NULL);

    hg_return_t              ret_value = HG_SUCCESS;
    pdc_obj_metadata_pkg *   obj_temp, *obj_temp2;
    pdc_region_metadata_pkg *region_temp, *region_temp2;

    obj_temp = metadata_server_objs;
    while (obj_temp) {
        region_temp = obj_temp->regions;
        while (region_temp) {
            region_temp2             = region_temp;
            region_temp              = region_temp->next;
            region_temp2->reg_offset = (uint64_t *)PDC_free(region_temp2->reg_offset);
            region_temp2             = (pdc_region_metadata_pkg *)PDC_free(region_temp2);
        }
        obj_temp2 = obj_temp;
        obj_temp  = obj_temp->next;
        obj_temp2 = (pdc_obj_metadata_pkg *)PDC_free(obj_temp2);
    }
    metadata_server_objs = NULL;

    pthread_mutex_destroy(&metadata_query_mutex);

    FUNC_LEAVE(ret_value);
}

perr_t
transfer_request_metadata_query_checkpoint_bulki(BULKI **checkpoint_bulki)
{
    FUNC_ENTER(NULL);

    perr_t                   ret_value = SUCCEED;
    pdc_obj_metadata_pkg *   obj_temp;
    pdc_region_metadata_pkg *region_temp;
    int                      obj_count = 0;
    BULKI *                  bulki     = NULL;

    //    if (checkpoint_bulki == NULL) {
    //        LOG_ERROR("checkpoint_bulki output parameter is NULL\n");
    //        PGOTO_ERROR(FAIL, "Invalid parameter");
    //    }

    pthread_mutex_lock(&metadata_query_mutex);

    obj_temp = metadata_server_objs;
    while (obj_temp) {
        obj_count++;
        obj_temp = obj_temp->next;
    }

    bulki                       = BULKI_init(1);
    BULKI_Entity *objects_array = empty_BULKI_Array_Entity_with_capacity(obj_count > 0 ? obj_count : 1);

    obj_temp = metadata_server_objs;
    while (obj_temp) {
        BULKI *obj_bulki = BULKI_init(3);
        int    reg_count = 0;

        region_temp = obj_temp->regions;
        while (region_temp) {
            reg_count++;
            region_temp = region_temp->next;
        }

        BULKI_put_incremental(obj_bulki, BULKI_singleton_ENTITY("obj_id", PDC_STRING),
                              BULKI_ENTITY(&obj_temp->obj_id, 1, PDC_UINT64, PDC_CLS_ITEM));

        BULKI_put_incremental(obj_bulki, BULKI_singleton_ENTITY("ndim", PDC_STRING),
                              BULKI_ENTITY(&obj_temp->ndim, 1, PDC_INT, PDC_CLS_ITEM));

        BULKI_Entity *regions_array = empty_BULKI_Array_Entity_with_capacity(reg_count > 0 ? reg_count : 1);

        region_temp = obj_temp->regions;
        while (region_temp) {
            BULKI *region_bulki = BULKI_init(2);

            BULKI_put_incremental(region_bulki, BULKI_singleton_ENTITY("data_server_id", PDC_STRING),
                                  BULKI_ENTITY(&region_temp->data_server_id, 1, PDC_UINT32, PDC_CLS_ITEM));

            BULKI_put_incremental(
                region_bulki, BULKI_singleton_ENTITY("reg_offset_size", PDC_STRING),
                BULKI_ENTITY(region_temp->reg_offset, obj_temp->ndim * 2, PDC_UINT64, PDC_CLS_ARRAY));

            BULKI_ENTITY_append_BULKI_incremental(regions_array, region_bulki);
            region_temp = region_temp->next;
        }

        BULKI_put_incremental(obj_bulki, BULKI_singleton_ENTITY("regions", PDC_STRING), regions_array);

        BULKI_ENTITY_append_BULKI_incremental(objects_array, obj_bulki);

        obj_temp = obj_temp->next;
    }

    BULKI_put_incremental(bulki, BULKI_singleton_ENTITY("objects", PDC_STRING), objects_array);

    pthread_mutex_unlock(&metadata_query_mutex);

    *checkpoint_bulki = bulki;

    LOG_DEBUG("Transfer query checkpoint created: %d objects\n", obj_count);

done:
    if (ret_value != SUCCEED && bulki != NULL) {
        BULKI_free(bulki, 1);
    }

    FUNC_LEAVE(ret_value);
}

/**
 * Checkpoint static variables in this file into a contiguous buffer.
 * checkpoint_size is the total number of bytes allocated.
 * Format of checkpoint:
 *    for each obj:
 *        ndim (sizeof(int)) + number of regions (sizeof(int)) + obj_id (sizeof(uint64_t))
 *        for each region:
 *            data server ID (sizeof(uint32_t)) + offset/ength (sizeof(uint64_t) * ndim * 2)
 */
perr_t
transfer_request_metadata_query_checkpoint(char **checkpoint, uint64_t *checkpoint_size)
{
    FUNC_ENTER(NULL);

    hg_return_t              ret_value = HG_SUCCESS;
    pdc_obj_metadata_pkg *   obj_temp;
    pdc_region_metadata_pkg *region_temp;
    char *                   ptr;
    int                      reg_count, obj_count;

    pthread_mutex_lock(&metadata_query_mutex);

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
    *checkpoint = (char *)PDC_malloc(*checkpoint_size);
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
            region_temp = region_temp->next;
        }
        obj_temp = obj_temp->next;
    }
    pthread_mutex_unlock(&metadata_query_mutex);

    FUNC_LEAVE(ret_value);
}

/*
 * Wrap the overlapping portions for each of the regions into a contiguous buffer.
 * Output is an ID that can be used to trace this buffer.
 */
static uint64_t
metadata_query_buf_create(pdc_obj_region_metadata *regions, int size, uint64_t *total_buf_size_ptr)
{
    FUNC_ENTER(NULL);

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

    // Iterate through all input regions. We compute the total buf size in this loop
    total_data_size                = sizeof(int);
    transfer_request_counter_total = 0;
    if (size <= 0 || size > 1000000) {
        LOG_ERROR("metadata_query_buf_create: invalid size %d\n", size);
        FUNC_LEAVE(0);
    }
    transfer_request_counters = (int *)PDC_calloc(size, sizeof(int));
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
            LOG_ERROR("metadata_query_buf_create: Unable to find the object with ID %lu\n",
                      (long int)regions[i].obj_id);
        }
    }
    if (!total_data_size) {
        query_id = 0;
        goto done;
    }

    query_buf = (pdc_metadata_query_buf *)PDC_malloc(sizeof(pdc_metadata_query_buf));
    // Free query_buf->buf in transfer_request_metadata_query2_bulk_transfer_cb.
    query_buf->buf  = (char *)PDC_malloc(total_data_size);
    query_buf->next = NULL;
    query_buf->id   = query_id_g;
    query_id_g++;
    ptr = query_buf->buf;
    memcpy(ptr, &transfer_request_counter_total, sizeof(int));
    ptr += sizeof(int);
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
                }
                // overlap_size is freed together.
                overlap_offset  = (uint64_t *)PDC_free(overlap_offset);
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

    transfer_request_counters = (int *)PDC_free(transfer_request_counters);
    FUNC_LEAVE(query_id);
}

/**
 * Find previously stored query buffer and return the buffer. Delete the entry from the linked list.
 */
perr_t
transfer_request_metadata_query_lookup_query_buf(uint64_t query_id, char **buf_ptr)
{
    FUNC_ENTER(NULL);

    pdc_metadata_query_buf *metadata_query, *previous;
    perr_t                  ret_value = SUCCEED;

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
            metadata_query = (pdc_metadata_query_buf *)PDC_free(metadata_query);
            PGOTO_DONE(ret_value);
        }
        i++;
        previous       = metadata_query;
        metadata_query = metadata_query->next;
    }
    *buf_ptr = NULL;
done:
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
    FUNC_ENTER(NULL);

    char *                   ptr = buf;
    int                      i;
    uint64_t                 query_id = 0;
    size_t                   unit;
    uint64_t                 data_server_id;
    uint8_t                  region_partition;
    pdc_obj_region_metadata *region_metadata;

    if (n_objs <= 0 || n_objs > 1000000) {
        LOG_ERROR("transfer_request_metadata_query_parse: invalid n_objs %d\n", n_objs);
        FUNC_LEAVE(0);
    }
    region_metadata = (pdc_obj_region_metadata *)PDC_malloc(sizeof(pdc_obj_region_metadata) * n_objs);

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
    query_id        = metadata_query_buf_create(region_metadata, n_objs, total_buf_size_ptr);
    region_metadata = (pdc_obj_region_metadata *)PDC_free(region_metadata);
    FUNC_LEAVE(query_id);
}

static perr_t
transfer_request_metadata_reg_append(pdc_region_metadata_pkg *regions, int ndim, uint64_t *reg_offset,
                                     uint64_t *reg_size, size_t unit, uint32_t data_server_id,
                                     uint8_t region_partition)
{
    FUNC_ENTER(NULL);

    hg_return_t ret_value = HG_SUCCESS;
    uint64_t    min_bytes;
    uint64_t    min_bytes_server;
    int         i;
    uint64_t    total_reg_size;

    regions->next = NULL;

    regions->reg_offset = (uint64_t *)PDC_malloc(sizeof(uint64_t) * ndim * 2);
    regions->reg_size   = regions->reg_offset + ndim;

    memcpy(regions->reg_offset, reg_offset, sizeof(uint64_t) * ndim);
    memcpy(regions->reg_size, reg_size, sizeof(uint64_t) * ndim);

    if (region_partition == PDC_REGION_DYNAMIC) {

        min_bytes        = data_server_bytes[0];
        min_bytes_server = 0;

        for (i = 1; i < pdc_server_size; ++i) {
            if (min_bytes > data_server_bytes[i]) {
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

    FUNC_LEAVE(ret_value);
}

static uint64_t
transfer_request_metadata_query_append(uint64_t obj_id, int ndim, uint64_t *reg_offset, uint64_t *reg_size,
                                       size_t unit, uint32_t data_server_id, uint8_t region_partition)
{
    FUNC_ENTER(NULL);

    pdc_obj_metadata_pkg *   temp;
    pdc_region_metadata_pkg *region_metadata;
    pdc_region_metadata_pkg *temp_region_metadata;

    temp = metadata_server_objs;
    while (temp) {
        if (temp->obj_id == obj_id) {
            break;
        }
        temp = temp->next;
    }
    if (temp == NULL) {
        temp = (pdc_obj_metadata_pkg *)PDC_malloc(sizeof(pdc_obj_metadata_pkg));
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
    region_metadata = temp->regions;
    while (region_metadata) {
        if (detect_region_contained(reg_offset, reg_size, region_metadata->reg_offset,
                                    region_metadata->reg_size, ndim)) {
            FUNC_LEAVE(region_metadata->data_server_id);
        }
        region_metadata = region_metadata->next;
    }
    // Reaching this line means that we are creating a new region and append it to the end of the object list.
    temp_region_metadata = (pdc_region_metadata_pkg *)PDC_malloc(sizeof(pdc_region_metadata_pkg));
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
    FUNC_LEAVE(temp->regions_end->data_server_id);
}
