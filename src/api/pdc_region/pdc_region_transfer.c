/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------
 * Author: Qiao Kang qiaokang124@gmail.com
 * This file is the core I/O module of the PDC region transfer at the client side.
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2022, The Regents of the
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

#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include "pdc_utlist.h"
#include "pdc_config.h"
#include "pdc_id_pkg.h"
#include "pdc_obj.h"
#include "pdc_obj_pkg.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_region.h"
#include "pdc_region_pkg.h"
#include "pdc_obj_pkg.h"
#include "pdc_interface.h"
#include "pdc_transforms_pkg.h"
#include "pdc_client_connect.h"
#include "pdc_analysis_pkg.h"
#include "pdc_logger.h"
#include <mpi.h>

#define PDC_MERGE_TRANSFER_MIN_COUNT 50

/**
 * PDC region transfer class. Contains essential information for performing non-blocking PDC client I/O
 * operations
 */
typedef struct pdc_transfer_request {
    pdcid_t obj_id;
    pdcid_t local_obj_id;
    // Data server ID for sending data to, used by object static only.
    uint32_t data_server_id;
    // Metadata server ID for sending data to, used by region_dynamic only.
    uint32_t metadata_server_id;
    // List of metadata. For dynamic object partitioning strategy, the metadata_id are owned by obj_servers
    // correspondingly. For static object partitioning, this ID is managed by the server with data_server_id.
    uint64_t *metadata_id;
    // PDC_READ or PDC_WRITE
    pdc_access_t access_type;
    // Determine unit size.
    pdc_var_type_t mem_type;
    size_t         unit;
    // User data buffer
    char *buf;
    /* Used internally for 2D and 3D data */
    // Contiguous buffers for read, undefined for PDC_WRITE. Static region mapping has >= 1 number of
    // read_bulk_buf. Other mappings have size of 1.
    char **read_bulk_buf;
    // buffer used for bulk transfer in mercury
    char *new_buf;
    // For each of the contig buffer sent to a server, we have a bulk buffer.
    char **bulk_buf;
    // Reference counter for bulk_buf, if 0, we free it.
    int **                 bulk_buf_ref;
    pdc_region_partition_t region_partition;

    // Consistency semantics required by user
    pdc_consistency_t              consistency;
    pdc_region_writeout_strategy_t writeout_strategy;

    // Dynamic object partitioning (static region partitioning and dynamic region partitioning)
    int       n_obj_servers;
    uint32_t *obj_servers;
    // Used by static region partitioning, these variables are regions that overlap the static regions of data
    // servers.
    uint64_t **output_offsets;
    uint64_t **sub_offsets;
    uint64_t **output_sizes;
    // Used only when access_type == PDC_WRITE, otherwise it should be NULL.
    char **output_buf;

    // Local region
    int       local_region_ndim;
    uint64_t *local_region_offset;
    uint64_t *local_region_size;
    // Remote region
    int       remote_region_ndim;
    uint64_t *remote_region_offset;
    uint64_t *remote_region_size;
    uint64_t  total_data_size;
    // Object dimensions
    int       obj_ndim;
    uint64_t *obj_dims;
    // Pointer to object info, can be useful sometimes. We do not want to go through PDC ID list many times.
    struct _pdc_obj_info *obj_pointer;
    // Tang: for merging transfer requests with transfer start_all/wait_all
    pdcid_t merged_request_id;
    int     is_done;

    // list of bulk handles used for region request
    hg_bulk_t *bulk_handles;
    // current number of bulk handles stored in array
    uint32_t num_bulk_handles;
    // current length of the array
    uint32_t bulk_handles_capacity;
} pdc_transfer_request;

// We pack all arguments for a start_all call to the same data server in a single structure, so we do not need
// to many arguments to a function.
typedef struct pdc_transfer_request_start_all_pkg {
    // One pkg, one data server
    uint32_t data_server_id;
    // Transfer request (for fast accessing obj metadata information)
    pdc_transfer_request *transfer_request;
    // Offset/length pair (remote)
    uint64_t *remote_offset;
    uint64_t *remote_size;
    // Index of this start_all pkg corresponding to the transfer_request. Maximum size is n_obj_servers - 1.
    // Every time a start_all pkg is created, this index is set to be the index such that the data_server_id
    // appears in the transfer_request->obj_servers.
    int index;
    // Data buffer. This data buffer is contiguous according to the remote region. We assume this is after
    // transformation of local regions
    char *                                     buf;
    struct pdc_transfer_request_start_all_pkg *next;
} pdc_transfer_request_start_all_pkg;

// We pack all arguments for a wait_all call to the same data server in a single structure, so we do not need
// to many arguments to a function.
typedef struct pdc_transfer_request_wait_all_pkg {
    // Metadata_ID for waited.
    uint64_t metadata_id;
    // One pkg, one data server
    uint32_t data_server_id;
    // Record the index of the metadata_id in the current transfer_request
    int index;
    // Pointer to the transfer request
    pdc_transfer_request *                    transfer_request;
    struct pdc_transfer_request_wait_all_pkg *next;
} pdc_transfer_request_wait_all_pkg;

#define REGION_TRANSFER_INIT_BULK_HANDLES 2

/**
 * @brief Initialize a pdc_transfer_request's bulk handle array.
 *
 * This function allocates memory for an initial number of bulk handles in a transfer request.
 * Each entry is initialized to HG_BULK_NULL. The initial capacity is set to
 * REGION_TRANSFER_INIT_BULK_HANDLES.
 *
 * \param tr Pointer to a pdc_transfer_request structure to initialize.
 *
 * \return SUCCEED on success, FAIL on failure (e.g., if `tr` is NULL or memory allocation fails).
 */
perr_t
PDCregion_transfer_init_bulk_handles(pdc_transfer_request *tr)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (tr == NULL)
        PGOTO_ERROR(FAIL, "tr was NULL");

    tr->bulk_handles_capacity = REGION_TRANSFER_INIT_BULK_HANDLES;
    tr->num_bulk_handles      = 0;
    tr->bulk_handles          = (hg_bulk_t *)PDC_malloc(sizeof(hg_bulk_t) * tr->bulk_handles_capacity);

    for (int i = 0; i < tr->bulk_handles_capacity; i++)
        tr->bulk_handles[i] = HG_BULK_NULL;

done:
    FUNC_LEAVE(ret_value);
}

/**
 * @brief Add a new bulk handle to a transfer request.
 *
 * If the internal array is full, it reallocates the array with increased capacity
 * (doubling it each time). New elements are initialized to HG_BULK_NULL.
 *
 * \param tr Pointer to a pdc_transfer_request structure where the bulk handle should be added.
 * \param bulk_handle The bulk handle to add to the transfer request.
 *
 * \return SUCCEED on success, FAIL on error (e.g., invalid input or reallocation failure).
 */
perr_t
PDCregion_transfer_add_bulk_handle(pdc_transfer_request *tr, hg_bulk_t bulk_handle)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (tr == NULL)
        PGOTO_ERROR(FAIL, "Invalid pdc transfer request");
    if (tr->bulk_handles == NULL && tr->num_bulk_handles > 0)
        PGOTO_ERROR(FAIL, "PDC transfer request has bulk handles but bulk_handles is NULL");

    // Grow array if needed
    if (tr->num_bulk_handles >= tr->bulk_handles_capacity) {
        size_t new_capacity =
            tr->bulk_handles_capacity > 0 ? tr->bulk_handles_capacity * 2 : REGION_TRANSFER_INIT_BULK_HANDLES;
        hg_bulk_t *new_array = (hg_bulk_t *)PDC_realloc(tr->bulk_handles, new_capacity * sizeof(hg_bulk_t));
        if (new_array == NULL)
            PGOTO_ERROR(FAIL, "Failed to reallocate bulk handle array");

        // Initialize the new slots to HG_BULK_NULL
        for (size_t i = tr->bulk_handles_capacity; i < new_capacity; i++)
            new_array[i] = HG_BULK_NULL;

        tr->bulk_handles          = new_array;
        tr->bulk_handles_capacity = new_capacity;
    }

    tr->bulk_handles[tr->num_bulk_handles] = bulk_handle;
    tr->num_bulk_handles++;

done:
    FUNC_LEAVE(ret_value);
}

static int
sort_by_data_server_start_all(const void *elem1, const void *elem2)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
    if ((*(pdc_transfer_request_start_all_pkg **)elem1)->data_server_id >
        (*(pdc_transfer_request_start_all_pkg **)elem2)->data_server_id)
        return 1;
    if ((*(pdc_transfer_request_start_all_pkg **)elem1)->data_server_id <
        (*(pdc_transfer_request_start_all_pkg **)elem2)->data_server_id)
        return -1;
    return 0;
#pragma GCC diagnostic pop
}

static int
sort_by_metadata_server_start_all(const void *elem1, const void *elem2)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
    if ((*(pdc_transfer_request_start_all_pkg **)elem1)->transfer_request->metadata_id >
        (*(pdc_transfer_request_start_all_pkg **)elem2)->transfer_request->metadata_id)
        return 1;
    if ((*(pdc_transfer_request_start_all_pkg **)elem1)->transfer_request->metadata_id <
        (*(pdc_transfer_request_start_all_pkg **)elem2)->transfer_request->metadata_id)
        return -1;
    return 0;
#pragma GCC diagnostic pop
}

static int
sort_by_data_server_wait_all(const void *elem1, const void *elem2)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
    if ((*(pdc_transfer_request_wait_all_pkg **)elem1)->data_server_id >
        (*(pdc_transfer_request_wait_all_pkg **)elem2)->data_server_id)
        return 1;
    if ((*(pdc_transfer_request_wait_all_pkg **)elem1)->data_server_id <
        (*(pdc_transfer_request_wait_all_pkg **)elem2)->data_server_id)
        return -1;
    return 0;
#pragma GCC diagnostic pop
}

pdcid_t
PDCregion_transfer_create(void *buf, pdc_access_t access_type, pdcid_t obj_id, pdcid_t local_reg,
                          pdcid_t remote_reg)
{
    FUNC_ENTER(NULL);

    pdcid_t                 ret_value = SUCCEED;
    struct _pdc_id_info *   objinfo2;
    struct _pdc_obj_info *  obj2;
    pdc_transfer_request *  p;
    struct _pdc_id_info *   reginfo1, *reginfo2;
    struct pdc_region_info *reg1, *reg2;
    uint64_t *              ptr;
    uint64_t                unit;
    int                     j;

    if (buf == NULL)
        PGOTO_ERROR(FAIL, "Client buffer was NULL");
    if ((reginfo1 = PDC_find_id(local_reg)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", local_reg);
    reg1 = (struct pdc_region_info *)(reginfo1->obj_ptr);
    if ((reginfo2 = PDC_find_id(remote_reg)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", remote_reg);
    reg2 = (struct pdc_region_info *)(reginfo2->obj_ptr);
    if ((objinfo2 = PDC_find_id(obj_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", obj_id);

    obj2 = (struct _pdc_obj_info *)(objinfo2->obj_ptr);

    p                   = (pdc_transfer_request *)PDC_malloc(sizeof(pdc_transfer_request));
    p->obj_pointer      = obj2;
    p->mem_type         = obj2->obj_pt->obj_prop_pub->type;
    p->local_obj_id     = obj_id;
    p->obj_id           = obj2->obj_info_pub->meta_id;
    p->access_type      = access_type;
    p->buf              = buf;
    p->metadata_id      = NULL;
    p->read_bulk_buf    = NULL;
    p->new_buf          = NULL;
    p->bulk_buf         = NULL;
    p->bulk_buf_ref     = NULL;
    p->output_buf       = NULL;
    p->region_partition = ((pdc_metadata_t *)obj2->metadata)->region_partition;
    // p->region_partition   = PDC_REGION_LOCAL;
    p->data_server_id     = ((pdc_metadata_t *)obj2->metadata)->data_server_id;
    p->metadata_server_id = obj2->obj_info_pub->metadata_server_id;
    p->unit               = PDC_get_var_type_size(p->mem_type);
    p->consistency        = obj2->obj_pt->obj_prop_pub->consistency;
    p->writeout_strategy  = obj2->obj_pt->obj_prop_pub->writeout_strategy;
    p->merged_request_id  = 0;
    p->is_done            = 0;
    PDCregion_transfer_init_bulk_handles(p);
    unit = p->unit;

    p->local_region_ndim   = reg1->ndim;
    p->local_region_offset = (uint64_t *)PDC_malloc(
        sizeof(uint64_t) * (reg1->ndim * 2 + reg2->ndim * 2 + obj2->obj_pt->obj_prop_pub->ndim));
    ptr = p->local_region_offset;
    memcpy(p->local_region_offset, reg1->offset, sizeof(uint64_t) * reg1->ndim);
    ptr += reg1->ndim;
    p->local_region_size = ptr;
    memcpy(p->local_region_size, reg1->size, sizeof(uint64_t) * reg1->ndim);
    ptr += reg1->ndim;

    p->remote_region_ndim   = reg2->ndim;
    p->remote_region_offset = ptr;
    memcpy(p->remote_region_offset, reg2->offset, sizeof(uint64_t) * reg2->ndim);
    ptr += reg2->ndim;

    p->remote_region_size = ptr;
    memcpy(p->remote_region_size, reg2->size, sizeof(uint64_t) * reg2->ndim);
    ptr += reg2->ndim;

    p->obj_ndim = obj2->obj_pt->obj_prop_pub->ndim;
    p->obj_dims = ptr;
    memcpy(p->obj_dims, obj2->obj_pt->obj_prop_pub->dims, sizeof(uint64_t) * p->obj_ndim);

    p->total_data_size = unit;
    for (j = 0; j < (int)reg2->ndim; ++j) {
        p->total_data_size *= reg2->size[j];
    }
    ret_value = PDC_id_register(PDC_TRANSFER_REQUEST, p);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_close(pdcid_t transfer_request_id)
{
    FUNC_ENTER(NULL);

    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    perr_t                ret_value = SUCCEED;

    if ((transferinfo = PDC_find_id(transfer_request_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", transfer_request_id);

    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    for (int i = 0; i < transfer_request->num_bulk_handles; i++) {
        if (transfer_request->bulk_handles[i] == HG_BULK_NULL)
            LOG_WARNING("Bulk handle added to transfer request was NULL\n");
        else {
            if (HG_Bulk_free(transfer_request->bulk_handles[i]) != HG_SUCCESS)
                LOG_WARNING("Failed to free bulk handle added to transfer request\n");

            transfer_request->bulk_handles[i] = HG_BULK_NULL;
        }
    }
    if (transfer_request->bulk_handles)
        transfer_request->bulk_handles = PDC_free(transfer_request->bulk_handles);
    if (transfer_request->local_region_offset)
        transfer_request->local_region_offset = (uint64_t *)PDC_free(transfer_request->local_region_offset);
    if (transfer_request->metadata_id)
        transfer_request->metadata_id = (uint64_t *)PDC_free(transfer_request->metadata_id);
    if (transfer_request)
        transfer_request = (pdc_transfer_request *)PDC_free(transfer_request);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(transfer_request_id) < 0)
        PGOTO_ERROR(FAIL, "PDC transfer request: problem of freeing id");

done:
    PDC_Client_transfer_pthread_cnt_add(-1);
    PDC_Client_transfer_pthread_terminate();

    FUNC_LEAVE(ret_value);
}

/*
 * This function binds a transfer request to its corresponding object, so the object is aware of any ongoing
 * region transfer operation. Called when transfer request start is executed. Why do we do this? Sometimes
 * users may close an object without calling transfer request wait, so it is our responsibility to wait for
 * the request at the object's close time.
 */
static perr_t
attach_local_transfer_request(struct _pdc_obj_info *p, pdcid_t transfer_request_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (p->local_transfer_request_head != NULL) {
        p->local_transfer_request_end->next =
            (pdc_local_transfer_request *)PDC_malloc(sizeof(pdc_local_transfer_request));
        p->local_transfer_request_end       = p->local_transfer_request_end->next;
        p->local_transfer_request_end->next = NULL;
    }
    else {
        p->local_transfer_request_head =
            (pdc_local_transfer_request *)PDC_malloc(sizeof(pdc_local_transfer_request));
        p->local_transfer_request_end       = p->local_transfer_request_head;
        p->local_transfer_request_end->next = NULL;
    }
    p->local_transfer_request_end->local_id = transfer_request_id;
    p->local_transfer_request_size++;

    FUNC_LEAVE(ret_value);
}

/*
 * This function detaches a transfer request to its corresponding object.
 * Called when transfer request wait is executed.
 */
static perr_t
remove_local_transfer_request(struct _pdc_obj_info *p, pdcid_t transfer_request_id)
{
    FUNC_ENTER(NULL);

    perr_t                             ret_value = SUCCEED;
    struct pdc_local_transfer_request *previous, *temp;

    temp     = p->local_transfer_request_head;
    previous = NULL;
    while (temp != NULL) {
        if (temp->local_id == transfer_request_id) {
            if (previous == NULL) {
                // removing first element. Carefully set the head.
                previous                       = p->local_transfer_request_head;
                p->local_transfer_request_head = p->local_transfer_request_head->next;
                previous                       = (struct pdc_local_transfer_request *)PDC_free(previous);
            }
            else {
                // Not the first element, just take the current element away.
                previous->next = temp->next;
                temp           = (struct pdc_local_transfer_request *)PDC_free(temp);
            }
            p->local_transfer_request_size--;
            break;
        }
        previous = temp;
        temp     = temp->next;
    }

    FUNC_LEAVE(ret_value);
}

/*
 * Input: Ojbect dimensions + a region
 * Output: Data servers that the region will access with a static region partition. As well as overlapping
 * regions.
 */

static perr_t
static_region_partition(char *buf, int ndim, uint64_t unit, pdc_access_t access_type, uint64_t *obj_dims,
                        uint64_t *offset, uint64_t *size, int set_output_buf, int *n_data_servers,
                        uint32_t **data_server_ids, uint64_t ***sub_offsets, uint64_t ***output_offsets,
                        uint64_t ***output_sizes, char ***output_buf)
{
    FUNC_ENTER(NULL);

    perr_t   ret_value = SUCCEED;
    int      i, j;
    uint64_t static_offset, static_size;
    uint64_t x, s;
    int      split_dim;
    uint64_t region_size;

    *n_data_servers = 0;

    // Search for a valid dimension we are going to split the region.
    split_dim = -1;
    for (i = 0; i < ndim; ++i) {
        if (obj_dims[ndim - 1 - i] > (uint64_t)pdc_server_num_g) {
            split_dim = ndim - 1 - i;
            break;
        }
    }
    // All dimensions are smaller than the number of servers. No split is necessary.
    if (split_dim == -1) {
        split_dim = ndim - 1;
    }
    // Use the remainder theorem to split along one dimension of regions.
    s = obj_dims[split_dim] / pdc_server_num_g;
    if (s == 0)
        s = 1;
    x = pdc_server_num_g - obj_dims[split_dim] % pdc_server_num_g;

    *data_server_ids = (uint32_t *)PDC_malloc(sizeof(uint32_t) * pdc_server_num_g);

    *output_offsets = (uint64_t **)PDC_malloc(sizeof(uint64_t *) * pdc_server_num_g);
    *output_sizes   = (uint64_t **)PDC_malloc(sizeof(uint64_t *) * pdc_server_num_g);
    *sub_offsets    = (uint64_t **)PDC_malloc(sizeof(uint64_t *) * pdc_server_num_g);
    if (set_output_buf) {
        *output_buf = (char **)PDC_malloc(sizeof(char *) * pdc_server_num_g);
    }
    else {
        *output_buf = NULL;
    }
    // Find all data servers.

    for (i = 0; i < pdc_server_num_g; ++i) {
        // Figure out the server static offset along the significant dimension
        if ((uint64_t)i < x) {
            static_offset = i * s;
            static_size   = s;
        }
        else {
            static_offset = x * s + (i - x) * (s + 1);
            static_size   = s + 1;
        }
        // Check if this region fits into the server static region.
        if ((static_offset + static_size > offset[split_dim] && offset[split_dim] >= static_offset) ||
            (offset[split_dim] + size[split_dim] > static_offset && static_offset >= offset[split_dim])) {
            // record data server ID
            data_server_ids[0][*n_data_servers] = i;
            // The overlapping region is allocated here.
            output_offsets[0][*n_data_servers] = (uint64_t *)PDC_malloc(sizeof(uint64_t) * ndim * 3);
            output_sizes[0][*n_data_servers]   = output_offsets[0][*n_data_servers] + ndim;
            sub_offsets[0][*n_data_servers]    = output_offsets[0][*n_data_servers] + ndim * 2;
            region_size                        = unit;
            for (j = 0; j < ndim; ++j) {
                // Compute the offsets. suboffsets are relative positions towards the input region, so we can
                // copy buffers easier.
                if (j == split_dim) {
                    // Split dimension will use the overlapping arithmetics to figure out the outcome.
                    if (static_offset < offset[split_dim]) {
                        output_offsets[0][*n_data_servers][j] = offset[split_dim];
                        output_sizes[0][*n_data_servers][j] =
                            (static_offset + static_size < offset[split_dim] + size[split_dim])
                                ? (static_offset + static_size - offset[split_dim])
                                : (size[split_dim]);
                        sub_offsets[0][*n_data_servers][j] = 0;
                    }
                    else {
                        output_offsets[0][*n_data_servers][j] = static_offset;
                        output_sizes[0][*n_data_servers][j] =
                            (static_offset + static_size < offset[split_dim] + size[split_dim])
                                ? (static_size)
                                : (offset[split_dim] + size[split_dim] - static_offset);
                        sub_offsets[0][*n_data_servers][j] = static_offset - offset[split_dim];
                    }
                }
                else {
                    // Other dimensions will use the input region directly.
                    output_offsets[0][*n_data_servers][j] = offset[j];
                    output_sizes[0][*n_data_servers][j]   = size[j];
                    sub_offsets[0][*n_data_servers][j]    = 0;
                }
                region_size *= output_sizes[0][*n_data_servers][j];
            }
            // subregion is computed using the output region by aligning the offsets to its begining.
            if (set_output_buf) {
                // Copy subregion from input region to the new overlapping region.
                output_buf[0][n_data_servers[0]] = (char *)PDC_calloc(region_size * unit, sizeof(char));
                if (access_type == PDC_WRITE) {
                    memcpy_subregion(ndim, unit, PDC_WRITE, buf, size, output_buf[0][n_data_servers[0]],
                                     sub_offsets[0][n_data_servers[0]], output_sizes[0][*n_data_servers]);
                }
            }
            *n_data_servers += 1;
        }
    }
    // Shrink memory size if necessary.
    if (*n_data_servers != pdc_server_num_g) {
        *data_server_ids = (uint32_t *)PDC_realloc(*data_server_ids, sizeof(uint32_t) * n_data_servers[0]);
        *output_offsets  = (uint64_t **)PDC_realloc(*output_offsets, sizeof(uint64_t *) * n_data_servers[0]);
        *output_sizes    = (uint64_t **)PDC_realloc(*output_sizes, sizeof(uint64_t *) * n_data_servers[0]);

        *sub_offsets = (uint64_t **)PDC_realloc(*sub_offsets, sizeof(uint64_t *) * n_data_servers[0]);
        if (set_output_buf) {
            *output_buf = (char **)PDC_realloc(*output_buf, sizeof(char *) * n_data_servers[0]);
        }
    }

    FUNC_LEAVE(ret_value);
}
/*
 * Pack user memory buffer into a contiguous buffer based on local region shape.
 */
static perr_t
pack_region_buffer(char *buf, uint64_t *obj_dims, size_t total_data_size, int local_ndim,
                   uint64_t *local_offset, uint64_t *local_size, size_t unit, pdc_access_t access_type,
                   char **new_buf)
{
    FUNC_ENTER(NULL);

    uint64_t i, j;
    perr_t   ret_value = SUCCEED;
    char *   ptr;

    if (local_ndim == 1) {
        *new_buf = buf + local_offset[0] * unit;
    }
    else if (local_ndim == 2) {
        *new_buf = (char *)PDC_malloc(sizeof(char) * total_data_size);
        if (access_type == PDC_WRITE) {
            ptr = *new_buf;
            for (i = 0; i < local_size[0]; ++i) {
                memcpy(ptr, buf + ((local_offset[0] + i) * local_size[1] + local_offset[1]) * unit,
                       local_size[1] * unit);
                ptr += local_size[1] * unit;
            }
        }
    }
    else if (local_ndim == 3) {
        *new_buf = (char *)PDC_malloc(sizeof(char) * total_data_size);
        if (access_type == PDC_WRITE) {
            ptr = *new_buf;
            for (i = 0; i < local_size[0]; ++i) {
                for (j = 0; j < local_size[1]; ++j) {
                    memcpy(ptr,
                           buf + ((local_offset[0] + i) * local_size[1] * local_size[2] +
                                  (local_offset[1] + j) * local_size[2] + local_offset[2]) *
                                     unit,
                           local_size[2] * unit);
                    ptr += local_size[2] * unit;
                }
            }
        }
    }
    else {
        ret_value = FAIL;
    }

    FUNC_LEAVE(ret_value);
}

static perr_t
set_obj_server_bufs(pdc_transfer_request *transfer_request)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    transfer_request->bulk_buf     = (char **)PDC_malloc(sizeof(char *) * transfer_request->n_obj_servers);
    transfer_request->bulk_buf_ref = (int **)PDC_malloc(sizeof(int *) * transfer_request->n_obj_servers);
    transfer_request->metadata_id =
        (uint64_t *)PDC_malloc(sizeof(uint64_t) * transfer_request->n_obj_servers);

    // read_bulk_buf is filled later when the bulk transfer is packed.
    if (transfer_request->access_type == PDC_READ) {
        transfer_request->read_bulk_buf =
            (char **)PDC_malloc(sizeof(char *) * transfer_request->n_obj_servers);
    }

    FUNC_LEAVE(ret_value);
}

static perr_t
pack_region_metadata_query(pdc_transfer_request_start_all_pkg **transfer_request, int size, char **buf_ptr,
                           uint64_t *total_buf_size_ptr)
{
    FUNC_ENTER(NULL);

    perr_t   ret_value = SUCCEED;
    int      i;
    char *   ptr;
    uint64_t total_buf_size;
    uint8_t  region_partition;

    total_buf_size = 0;
    for (i = 0; i < size; ++i) {
        // ndim + Regions + obj_id + data_server id + data partition + unit
        total_buf_size += sizeof(int) +
                          sizeof(uint64_t) * 2 * transfer_request[i]->transfer_request->remote_region_ndim +
                          sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(size_t);
    }

    *buf_ptr = (char *)PDC_malloc(total_buf_size);
    ptr      = *buf_ptr;
    for (i = 0; i < size; ++i) {
        memcpy(ptr, &(transfer_request[i]->transfer_request->obj_id), sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        memcpy(ptr, &(transfer_request[i]->data_server_id), sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        region_partition = (uint8_t)transfer_request[i]->transfer_request->region_partition;
        memcpy(ptr, &region_partition, sizeof(uint8_t));
        ptr += sizeof(uint8_t);
        memcpy(ptr, &(transfer_request[i]->transfer_request->remote_region_ndim), sizeof(int));
        ptr += sizeof(int);
        memcpy(ptr, &(transfer_request[i]->transfer_request->unit), sizeof(size_t));
        ptr += sizeof(size_t);
        memcpy(ptr, transfer_request[i]->remote_offset,
               sizeof(uint64_t) * transfer_request[i]->transfer_request->remote_region_ndim);
        ptr += sizeof(uint64_t) * transfer_request[i]->transfer_request->remote_region_ndim;
        memcpy(ptr, transfer_request[i]->remote_size,
               sizeof(uint64_t) * transfer_request[i]->transfer_request->remote_region_ndim);
        ptr += sizeof(uint64_t) * transfer_request[i]->transfer_request->remote_region_ndim;
    }

    *total_buf_size_ptr = total_buf_size;

    FUNC_LEAVE(ret_value);
}

static perr_t
unpack_region_metadata_query(char *buf, pdc_transfer_request_start_all_pkg **transfer_request_input,
                             pdc_transfer_request_start_all_pkg **transfer_request_head_ptr,
                             pdc_transfer_request_start_all_pkg **transfer_request_end_ptr, int *size_ptr)
{
    FUNC_ENTER(NULL);

    perr_t                              ret_value = SUCCEED;
    pdc_transfer_request_start_all_pkg *transfer_request_head, *transfer_request_end;
    pdc_transfer_request *              local_request;
    int                                 size;
    int                                 i, j, index;
    int                                 counter;
    char *                              ptr;
    uint64_t                            region_size;
    uint64_t *                          sub_offset;

    local_request         = NULL;
    transfer_request_head = NULL;
    transfer_request_end  = NULL;
    ptr                   = buf;
    size                  = *(int *)ptr;
    ptr += sizeof(int);
    counter = 0;
    index   = 0;
    for (i = 0; i < size; ++i) {
        if (transfer_request_head) {
            transfer_request_end->next =
                (pdc_transfer_request_start_all_pkg *)PDC_malloc(sizeof(pdc_transfer_request_start_all_pkg));
            transfer_request_end = transfer_request_end->next;
        }
        else {
            transfer_request_head =
                (pdc_transfer_request_start_all_pkg *)PDC_malloc(sizeof(pdc_transfer_request_start_all_pkg));
            transfer_request_end = transfer_request_head;
        }
        // Obj ID + Obj dim + region offset + region size
        if (!counter) {
            counter = *(int *)ptr;
            ptr += sizeof(int);
            local_request                = transfer_request_input[index]->transfer_request;
            local_request->n_obj_servers = counter;
            local_request->output_offsets =
                (uint64_t **)PDC_malloc(sizeof(uint64_t *) * local_request->n_obj_servers);
            local_request->output_sizes =
                (uint64_t **)PDC_malloc(sizeof(uint64_t *) * local_request->n_obj_servers);
            local_request->sub_offsets =
                (uint64_t **)PDC_malloc(sizeof(uint64_t *) * local_request->n_obj_servers);
            local_request->obj_servers =
                (uint32_t *)PDC_malloc(sizeof(uint32_t) * local_request->n_obj_servers);
            set_obj_server_bufs(local_request);
        }
        transfer_request_end->next             = NULL;
        transfer_request_end->transfer_request = local_request;
        transfer_request_end->data_server_id   = *(uint32_t *)ptr;

        ptr += sizeof(uint32_t);
        transfer_request_end->remote_offset =
            (uint64_t *)PDC_malloc(sizeof(uint64_t) * local_request->remote_region_ndim * 3);
        transfer_request_end->remote_size =
            transfer_request_end->remote_offset + local_request->remote_region_ndim;
        sub_offset = transfer_request_end->remote_offset + local_request->remote_region_ndim * 2;
        memcpy(transfer_request_end->remote_offset, ptr,
               sizeof(uint64_t) * local_request->remote_region_ndim * 2);
        ptr += sizeof(uint64_t) * local_request->remote_region_ndim * 2;

        transfer_request_end->index = local_request->n_obj_servers - counter;
        local_request->output_offsets[local_request->n_obj_servers - counter] =
            transfer_request_end->remote_offset;
        local_request->output_sizes[local_request->n_obj_servers - counter] =
            transfer_request_end->remote_size;
        local_request->sub_offsets[local_request->n_obj_servers - counter] = sub_offset;
        local_request->obj_servers[local_request->n_obj_servers - counter] =
            transfer_request_end->data_server_id;
        region_size = local_request->unit;
        for (j = 0; j < local_request->remote_region_ndim; ++j) {
            region_size *= transfer_request_end->remote_size[j];
            sub_offset[j] = transfer_request_end->remote_offset[j] - local_request->remote_region_offset[j];
        }
        if (local_request->access_type == PDC_WRITE) {
            transfer_request_end->buf = (char *)PDC_malloc(region_size);
            memcpy_subregion(local_request->remote_region_ndim, local_request->unit,
                             local_request->access_type, transfer_request_input[index]->buf,
                             local_request->remote_region_size, transfer_request_end->buf, sub_offset,
                             transfer_request_end->remote_size);
        }
        counter--;
        if (!counter) {
            index++;
        }
    }

    *transfer_request_head_ptr = transfer_request_head;
    *transfer_request_end_ptr  = transfer_request_end;
    *size_ptr += size;

    FUNC_LEAVE(ret_value);
}

static perr_t
register_metadata(pdc_transfer_request_start_all_pkg **transfer_request_input, int input_size,
                  uint8_t is_write, pdc_transfer_request_start_all_pkg ***transfer_request_output_ptr,
                  int *output_size_ptr)
{
    FUNC_ENTER(NULL);

    perr_t                               ret_value = SUCCEED;
    int                                  i, j, index, size, output_size, remain_size, n_objs;
    pdc_transfer_request_start_all_pkg **transfer_requests;
    pdc_transfer_request_start_all_pkg * transfer_request_head, *transfer_request_front_head,
        *transfer_request_end, **transfer_request_output, *previous = NULL;
    uint64_t  total_buf_size, output_buf_size, query_id;
    char *    buf, *output_buf;
    hg_bulk_t bulk_handle;

    transfer_request_output     = NULL;
    transfer_request_front_head = NULL;
    transfer_requests           = (pdc_transfer_request_start_all_pkg **)PDC_malloc(
        sizeof(pdc_transfer_request_start_all_pkg *) * input_size);
    size = 0;
    for (i = 0; i < input_size; ++i) {
        if (transfer_request_input[i]->transfer_request->region_partition == PDC_REGION_DYNAMIC ||
            transfer_request_input[i]->transfer_request->region_partition == PDC_REGION_LOCAL) {
            transfer_requests[size] = transfer_request_input[i];
            size++;
        }
    }
    remain_size = input_size - size;
    output_size = 0;

    qsort(transfer_requests, size, sizeof(pdc_transfer_request_start_all_pkg *),
          sort_by_metadata_server_start_all);

    // Each iteration finds the first transfer that has a target meta server different from the previous one
    // index is the first transfer index
    int  current_unique_idx     = 0;
    int *unique_server_xfer_idx = NULL;
    int *unique_server_nboj     = NULL;
    if (size > 0) {
        unique_server_xfer_idx = (int *)PDC_calloc(size, sizeof(int));
        unique_server_nboj     = (int *)PDC_calloc(size, sizeof(int));
    }

    // Iterate through the input array
    for (i = 0; i < size; ++i) {
        if (i == 0 || transfer_requests[i]->transfer_request->metadata_server_id !=
                          transfer_requests[i - 1]->transfer_request->metadata_server_id) {
            // Check if the current element is different from the previous one
            // or if it's the first element
            unique_server_xfer_idx[current_unique_idx] = i;
            unique_server_nboj[current_unique_idx]     = 1;

            current_unique_idx++;
        }
        else {
            unique_server_nboj[current_unique_idx - 1]++;
        }
    }
    int num_unique_server_ids = current_unique_idx;

    // Now we will try to distribute the metadata requests to different servers across clients
    for (i = 0; i < num_unique_server_ids; i++) {
        int current_index = (pdc_client_mpi_rank_g + i) % num_unique_server_ids;
        index             = unique_server_xfer_idx[current_index];
        n_objs            = unique_server_nboj[current_index];

        pack_region_metadata_query(transfer_requests + index, n_objs, &buf, &total_buf_size);
        PDC_Client_transfer_request_metadata_query(
            &bulk_handle, buf, total_buf_size, n_objs,
            transfer_requests[index]->transfer_request->metadata_server_id, is_write, &output_buf_size,
            &query_id);
        PDCregion_transfer_add_bulk_handle(transfer_requests[index]->transfer_request, bulk_handle);
        buf = (char *)PDC_free(buf);
        if (query_id) {
            output_buf = (char *)PDC_malloc(output_buf_size);
            PDC_Client_transfer_request_metadata_query2(
                &bulk_handle, output_buf, output_buf_size, query_id,
                transfer_requests[index]->transfer_request->metadata_server_id);
            PDCregion_transfer_add_bulk_handle(transfer_requests[index]->transfer_request, bulk_handle);
            unpack_region_metadata_query(output_buf, transfer_requests + index, &transfer_request_head,
                                         &transfer_request_end, &output_size);
            output_buf = (char *)PDC_free(output_buf);

            if (transfer_request_front_head)
                previous->next = transfer_request_head;
            else
                transfer_request_front_head = transfer_request_head;

            previous = transfer_request_end;
        }
    }

    if (unique_server_xfer_idx)
        free(unique_server_xfer_idx);
    if (unique_server_nboj)
        free(unique_server_nboj);

    if (output_size) {
        transfer_request_output = (pdc_transfer_request_start_all_pkg **)PDC_malloc(
            sizeof(pdc_transfer_request_start_all_pkg *) * (output_size + remain_size));
        transfer_request_head = transfer_request_front_head;
        i                     = 0;
        while (transfer_request_head) {
            transfer_request_output[i] = transfer_request_head;
            transfer_request_head      = transfer_request_head->next;
            i++;
        }
        j = output_size;
        for (i = 0; i < input_size; ++i) {
            if (transfer_request_input[i]->transfer_request->region_partition == PDC_REGION_DYNAMIC ||
                transfer_request_input[i]->transfer_request->region_partition == PDC_REGION_LOCAL) {
                // These are replaced by newly created request pkgs.
                transfer_request_input[i] =
                    (pdc_transfer_request_start_all_pkg *)PDC_free(transfer_request_input[i]);
            }
            else {
                transfer_request_output[j] = transfer_request_input[i];
                j++;
            }
        }
        transfer_request_input = (pdc_transfer_request_start_all_pkg **)PDC_free(transfer_request_input);
        *transfer_request_output_ptr = transfer_request_output;
        *output_size_ptr             = j;
    }
    else {
        *transfer_request_output_ptr = transfer_request_input;
        *output_size_ptr             = input_size;
    }

    transfer_requests = (pdc_transfer_request_start_all_pkg **)PDC_free(transfer_requests);

    FUNC_LEAVE(ret_value);
}

/**
 * This function prepares lists of read and write requests separately for start_all function. The lists are
 * sorted in terms of data_server_id. We pack data from user buffer to contiguous buffers. Static partitioning
 * requires having at most n_data_servers number of contiguous regions.
 */
static perr_t
prepare_start_all_requests(pdcid_t *transfer_request_id, int size,
                           pdc_transfer_request_start_all_pkg ***write_transfer_request_ptr,
                           pdc_transfer_request_start_all_pkg ***read_transfer_request_ptr,
                           int *write_size_ptr, int *read_size_ptr, pdcid_t **posix_transfer_request_id_ptr,
                           int *posix_size_ptr)
{
    FUNC_ENTER(NULL);

    int                                  i, j;
    int                                  unit;
    pdc_transfer_request_start_all_pkg **write_transfer_request, **read_transfer_request, *write_request_pkgs,
        *read_request_pkgs, *write_request_pkgs_end, *read_request_pkgs_end, *request_pkgs,
        **transfer_request_output;
    int                   write_size, read_size, output_size;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    int                   set_output_buf = 0;
    int                   ret_value      = SUCCEED;

    write_request_pkgs             = NULL;
    read_request_pkgs              = NULL;
    write_size                     = 0;
    read_size                      = 0;
    posix_size_ptr[0]              = 0;
    *posix_transfer_request_id_ptr = (pdcid_t *)PDC_malloc(sizeof(pdcid_t) * size);

    for (i = 0; i < size; ++i) {
        if ((transferinfo = PDC_find_id(transfer_request_id[i])) == NULL)
            PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", transfer_request_id[i]);
        transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
        if (transfer_request->metadata_id != NULL)
            PGOTO_ERROR(FAIL, "Cannot start transfer request");
        if (transfer_request->consistency == PDC_CONSISTENCY_POSIX) {
            posix_transfer_request_id_ptr[0][posix_size_ptr[0]] = transfer_request_id[i];
            posix_size_ptr[0]++;
        }

        attach_local_transfer_request(transfer_request->obj_pointer, transfer_request_id[i]);
        unit = transfer_request->unit;
        pack_region_buffer(transfer_request->buf, transfer_request->obj_dims,
                           transfer_request->total_data_size, transfer_request->local_region_ndim,
                           transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                           transfer_request->access_type, &(transfer_request->new_buf));

        if (transfer_request->region_partition == PDC_REGION_STATIC) {
            if (transfer_request->access_type == PDC_WRITE) {
                set_output_buf = 1;
            }
            static_region_partition(transfer_request->new_buf, transfer_request->remote_region_ndim, unit,
                                    transfer_request->access_type, transfer_request->obj_dims,
                                    transfer_request->remote_region_offset,
                                    transfer_request->remote_region_size, set_output_buf,
                                    &(transfer_request->n_obj_servers), &(transfer_request->obj_servers),
                                    &(transfer_request->sub_offsets), &(transfer_request->output_offsets),
                                    &(transfer_request->output_sizes), &(transfer_request->output_buf));
            if (transfer_request->n_obj_servers == 0)
                PGOTO_ERROR(FAIL, "Error with static region partition, no server is selected");
            for (j = 0; j < transfer_request->n_obj_servers; ++j) {
                request_pkgs = (pdc_transfer_request_start_all_pkg *)PDC_malloc(
                    sizeof(pdc_transfer_request_start_all_pkg));
                request_pkgs->transfer_request = transfer_request;
                request_pkgs->data_server_id   = transfer_request->obj_servers[j];
                request_pkgs->remote_offset    = transfer_request->output_offsets[j];
                request_pkgs->remote_size      = transfer_request->output_sizes[j];
                request_pkgs->index            = j;
                // For read, we do not need the value of buf because we are not transferring data from client
                // to server
                if (transfer_request->access_type == PDC_WRITE) {
                    request_pkgs->buf = transfer_request->output_buf[j];
                }
                request_pkgs->next = NULL;
                if (transfer_request->access_type == PDC_WRITE) {
                    write_size++;
                    if (write_request_pkgs == NULL) {
                        write_request_pkgs     = request_pkgs;
                        write_request_pkgs_end = request_pkgs;
                    }
                    else {
                        write_request_pkgs_end->next = request_pkgs;
                        write_request_pkgs_end       = request_pkgs;
                    }
                }
                else if (transfer_request->access_type == PDC_READ) {
                    read_size++;
                    if (read_request_pkgs == NULL) {
                        read_request_pkgs     = request_pkgs;
                        read_request_pkgs_end = request_pkgs;
                    }
                    else {
                        read_request_pkgs_end->next = request_pkgs;
                        read_request_pkgs_end       = request_pkgs;
                    }
                }
            }
        }
        // Dynamic partitioning is handled at the end of this function by querying server.
        else if (transfer_request->region_partition == PDC_OBJ_STATIC ||
                 transfer_request->region_partition == PDC_REGION_DYNAMIC ||
                 transfer_request->region_partition == PDC_REGION_LOCAL) {
            transfer_request->n_obj_servers = 1;
            request_pkgs =
                (pdc_transfer_request_start_all_pkg *)PDC_malloc(sizeof(pdc_transfer_request_start_all_pkg));
            request_pkgs->transfer_request = transfer_request;
            request_pkgs->index            = 0;
            if (transfer_request->region_partition == PDC_OBJ_STATIC) {
                request_pkgs->data_server_id = transfer_request->data_server_id;
            }
            else {
                request_pkgs->data_server_id = PDC_get_client_data_server();
            }
            request_pkgs->remote_offset = transfer_request->remote_region_offset;
            request_pkgs->remote_size   = transfer_request->remote_region_size;
            if (transfer_request->access_type == PDC_WRITE) {
                request_pkgs->buf = transfer_request->new_buf;
            }
            request_pkgs->next = NULL;
            if (transfer_request->access_type == PDC_WRITE) {
                write_size++;
                if (write_request_pkgs == NULL) {
                    write_request_pkgs     = request_pkgs;
                    write_request_pkgs_end = request_pkgs;
                }
                else {
                    write_request_pkgs_end->next = request_pkgs;
                    write_request_pkgs_end       = request_pkgs;
                }
            }
            else if (transfer_request->access_type == PDC_READ) {
                read_size++;
                if (read_request_pkgs == NULL) {
                    read_request_pkgs     = request_pkgs;
                    read_request_pkgs_end = request_pkgs;
                }
                else {
                    read_request_pkgs_end->next = request_pkgs;
                    read_request_pkgs_end       = request_pkgs;
                }
            }
        }
        // REGION_DYNAMIC case is allocated later, once we know the number of regions we are going to access.
        if (transfer_request->region_partition != PDC_REGION_DYNAMIC &&
            transfer_request->region_partition != PDC_REGION_LOCAL) {
            set_obj_server_bufs(transfer_request);
        }
    }

    if (write_size) {
        write_transfer_request = (pdc_transfer_request_start_all_pkg **)PDC_malloc(
            sizeof(pdc_transfer_request_start_all_pkg *) * write_size);
        request_pkgs = write_request_pkgs;
        for (i = 0; i < write_size; ++i) {
            write_transfer_request[i] = request_pkgs;
            request_pkgs              = request_pkgs->next;
        }
        register_metadata(write_transfer_request, write_size, 1, &transfer_request_output, &output_size);
        *write_transfer_request_ptr = transfer_request_output;
        *write_size_ptr             = output_size;
        qsort(*write_transfer_request_ptr, *write_size_ptr, sizeof(pdc_transfer_request_start_all_pkg *),
              sort_by_data_server_start_all);
    }
    else {
        *write_size_ptr = 0;
    }

    if (read_size) {
        read_transfer_request = (pdc_transfer_request_start_all_pkg **)PDC_malloc(
            sizeof(pdc_transfer_request_start_all_pkg *) * read_size);
        request_pkgs = read_request_pkgs;
        for (i = 0; i < read_size; ++i) {
            read_transfer_request[i] = request_pkgs;
            request_pkgs             = request_pkgs->next;
        }
        register_metadata(read_transfer_request, read_size, 0, &transfer_request_output, &output_size);
        *read_transfer_request_ptr = transfer_request_output;
        *read_size_ptr             = output_size;
        qsort(*read_transfer_request_ptr, *read_size_ptr, sizeof(pdc_transfer_request_start_all_pkg *),
              sort_by_data_server_start_all);
    }
    else {
        *read_size_ptr = 0;
    }

done:
    FUNC_LEAVE(ret_value);
}

static int
finish_start_all_requests(pdc_transfer_request_start_all_pkg **write_transfer_request,
                          pdc_transfer_request_start_all_pkg **read_transfer_request, int write_size,
                          int read_size)
{
    FUNC_ENTER(NULL);

    int i;
    for (i = 0; i < write_size; ++i) {
        write_transfer_request[i] = (pdc_transfer_request_start_all_pkg *)PDC_free(write_transfer_request[i]);
    }
    for (i = 0; i < read_size; ++i) {
        read_transfer_request[i] = (pdc_transfer_request_start_all_pkg *)PDC_free(read_transfer_request[i]);
    }
    if (write_size) {
        write_transfer_request = (pdc_transfer_request_start_all_pkg **)PDC_free(write_transfer_request);
    }
    if (read_size) {
        read_transfer_request = (pdc_transfer_request_start_all_pkg **)PDC_free(read_transfer_request);
    }

    FUNC_LEAVE(0);
}

static perr_t
PDC_Client_pack_all_requests(int n_objs, pdc_transfer_request_start_all_pkg **transfer_requests,
                             pdc_access_t access_type, char **bulk_buf_ptr, size_t *total_buf_size_ptr,
                             char **read_bulk_buf)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    char * bulk_buf, *ptr, *ptr2;
    size_t total_buf_size, obj_data_size, total_obj_data_size, unit, data_size, metadata_size;
    int    i, j;

    // Calculate how large the final buffer will be

    // Metadata size
    /*
     * number of objects: sizeof(int)
     * The following times n_objs (one set per object).
     *     obj_id: remote object ID
     *     obj_ndim: sizeof(int)
     *     remote remote_ndim: sizeof(int)
     *     unit: sizeof(size_t)
     */
    metadata_size = n_objs * (sizeof(pdcid_t) + sizeof(int) * 2 + sizeof(size_t) + sizeof(uint8_t));
    // Data size, including region offsets/length pairs and actual data for I/O.
    /*
     * For each of objects
     *     remote region offset: size(uint64_t) * remote_ndim
     *     remote region length: size(uint64_t) * remote_ndim
     *     obj_dims: size(uint64_t) * remote_ndim
     *     buf: computed from region length (summed up)
     */
    data_size           = 0;
    total_obj_data_size = 0;
    for (i = 0; i < n_objs; ++i) {
        obj_data_size = transfer_requests[i]->remote_size[0] * transfer_requests[i]->transfer_request->unit;
        for (j = 1; j < transfer_requests[i]->transfer_request->remote_region_ndim; ++j) {
            obj_data_size *= transfer_requests[i]->remote_size[j];
        }
        if (access_type == PDC_WRITE) {
            data_size += sizeof(uint64_t) * transfer_requests[i]->transfer_request->remote_region_ndim * 3 +
                         obj_data_size;
        }
        else {
            total_obj_data_size += obj_data_size;
            data_size += sizeof(uint64_t) * transfer_requests[i]->transfer_request->remote_region_ndim * 3;
        }
    }
    if (access_type == PDC_WRITE) {
        total_buf_size = metadata_size + data_size;
    }
    else {
        if (metadata_size + data_size < total_obj_data_size) {
            total_buf_size = total_obj_data_size;
        }
        else {
            total_buf_size = metadata_size + data_size;
        }
    }
    bulk_buf      = (char *)PDC_malloc(total_buf_size);
    *bulk_buf_ptr = bulk_buf;
    ptr           = bulk_buf;
    ptr2          = bulk_buf;
    // Pack metadata
#define MEMCPY_INC(a, b)                                                                                     \
    {                                                                                                        \
        memcpy(ptr, a, b);                                                                                   \
        ptr += b;                                                                                            \
    }
    for (i = 0; i < n_objs; ++i) {
        unit = transfer_requests[i]->transfer_request->unit;
        MEMCPY_INC(&(transfer_requests[i]->transfer_request->obj_id), sizeof(pdcid_t));
        MEMCPY_INC(&(transfer_requests[i]->transfer_request->obj_ndim), sizeof(int));
        MEMCPY_INC(&(transfer_requests[i]->transfer_request->remote_region_ndim), sizeof(int));
        MEMCPY_INC(&unit, sizeof(size_t));
        uint8_t ws = (uint8_t)transfer_requests[i]->transfer_request->writeout_strategy;
        MEMCPY_INC(&ws, sizeof(uint8_t));
    }

    for (i = 0; i < n_objs; ++i) {
        unit          = transfer_requests[i]->transfer_request->unit;
        obj_data_size = transfer_requests[i]->remote_size[0] * unit;

        for (j = 1; j < transfer_requests[i]->transfer_request->remote_region_ndim; ++j) {
            obj_data_size *= transfer_requests[i]->remote_size[j];
        }

        if (access_type == PDC_READ) {
            read_bulk_buf[i] = ptr2;
            ptr2 += obj_data_size;
        }

        MEMCPY_INC(transfer_requests[i]->remote_offset,
                   sizeof(uint64_t) * transfer_requests[i]->transfer_request->remote_region_ndim);
        MEMCPY_INC(transfer_requests[i]->remote_size,
                   sizeof(uint64_t) * transfer_requests[i]->transfer_request->remote_region_ndim);
        MEMCPY_INC(transfer_requests[i]->transfer_request->obj_dims,
                   sizeof(uint64_t) * transfer_requests[i]->transfer_request->obj_ndim);
        // Note buf is undefined for PDC_READ
        if (access_type == PDC_WRITE) {
            MEMCPY_INC(transfer_requests[i]->buf, obj_data_size);
        }
    }
    *total_buf_size_ptr = total_buf_size;

    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Client_start_all_requests(pdc_transfer_request_start_all_pkg **transfer_requests, int size, MPI_Comm comm)
{
    FUNC_ENTER(NULL);

    perr_t    ret_value = SUCCEED;
    int       index, i, j;
    int       n_objs;
    uint64_t *metadata_id;
    char **   read_bulk_buf;
    char *    bulk_buf;
    size_t    bulk_buf_size;
    int *     bulk_buf_ref;
    hg_bulk_t bulk_handle;

    if (size == 0)
        PGOTO_DONE(ret_value);

    metadata_id   = (uint64_t *)PDC_malloc(sizeof(uint64_t) * size);
    read_bulk_buf = (char **)PDC_malloc(sizeof(char *) * size);
    index         = 0;
    for (i = 1; i < size; ++i) {
        if (transfer_requests[i]->data_server_id != transfer_requests[i - 1]->data_server_id) {
            // Freed at the wait operation (inside PDC_client_connect call)
            n_objs = i - index;
            PDC_Client_pack_all_requests(n_objs, transfer_requests + index,
                                         transfer_requests[index]->transfer_request->access_type, &bulk_buf,
                                         &bulk_buf_size, read_bulk_buf + index);
            bulk_buf_ref    = (int *)PDC_malloc(sizeof(int));
            bulk_buf_ref[0] = n_objs;
            PDC_Client_transfer_request_all(
                &bulk_handle, n_objs, transfer_requests[index]->transfer_request->access_type,
                transfer_requests[index]->data_server_id, bulk_buf, bulk_buf_size, metadata_id + index, comm);
            PDCregion_transfer_add_bulk_handle(transfer_requests[index]->transfer_request, bulk_handle);
            for (j = index; j < i; ++j) {
                // All requests share the same bulk buffer, reference counter is also shared among all
                // requests.
                transfer_requests[j]->transfer_request->bulk_buf[transfer_requests[j]->index] = bulk_buf;
                transfer_requests[j]->transfer_request->bulk_buf_ref[transfer_requests[j]->index] =
                    bulk_buf_ref;
                if (transfer_requests[j]->transfer_request->access_type == PDC_READ) {
                    transfer_requests[j]->transfer_request->read_bulk_buf[transfer_requests[j]->index] =
                        read_bulk_buf[j];
                }
                transfer_requests[j]->transfer_request->metadata_id[transfer_requests[j]->index] =
                    metadata_id[j];
            }
            index = i;
        }
    }
    if (size) {
        // Freed at the wait operation (inside PDC_client_connect call)
        n_objs = size - index;
        PDC_Client_pack_all_requests(n_objs, transfer_requests + index,
                                     transfer_requests[index]->transfer_request->access_type, &bulk_buf,
                                     &bulk_buf_size, read_bulk_buf + index);
        bulk_buf_ref    = (int *)PDC_malloc(sizeof(int));
        bulk_buf_ref[0] = n_objs;
        PDC_Client_transfer_request_all(
            &bulk_handle, n_objs, transfer_requests[index]->transfer_request->access_type,
            transfer_requests[index]->data_server_id, bulk_buf, bulk_buf_size, metadata_id + index, comm);
        PDCregion_transfer_add_bulk_handle(transfer_requests[index]->transfer_request, bulk_handle);

        for (j = index; j < size; ++j) {
            // All requests share the same bulk buffer, reference counter is also shared among all
            // requests.
            transfer_requests[j]->transfer_request->bulk_buf[transfer_requests[j]->index]     = bulk_buf;
            transfer_requests[j]->transfer_request->bulk_buf_ref[transfer_requests[j]->index] = bulk_buf_ref;
            if (transfer_requests[j]->transfer_request->access_type == PDC_READ) {
                transfer_requests[j]->transfer_request->read_bulk_buf[transfer_requests[j]->index] =
                    read_bulk_buf[j];
            }
            transfer_requests[j]->transfer_request->metadata_id[transfer_requests[j]->index] = metadata_id[j];
        }
    }

    read_bulk_buf = (char **)PDC_free(read_bulk_buf);
    metadata_id   = (uint64_t *)PDC_free(metadata_id);

done:
    FUNC_LEAVE(ret_value);
}

// Try to merge smaller requests to a large one, currently only merge write requests on same object and
// contiguous
static perr_t
merge_transfer_request_ids(pdcid_t *transfer_request_id, int size, pdcid_t *merged_request_id,
                           int *merged_size)
{
    FUNC_ENTER(NULL);

    struct _pdc_id_info *  transferinfo;
    pdcid_t                obj_id, new_local_reg, new_remote_reg;
    int                    flag = 0, i;
    void *                 new_buf;
    pdc_access_t           access_type;
    pdc_transfer_request **all_transfer_request;
    uint64_t               new_buf_size = 0, copy_off = 0;
    uint64_t               offset_local[3], size_local[3], offset_remote[3], size_remote[3];
    perr_t                 ret_value = SUCCEED;

    all_transfer_request = (pdc_transfer_request **)PDC_calloc(size, sizeof(pdc_transfer_request *));

    for (i = 0; i < size; ++i) {
        if ((transferinfo = PDC_find_id(transfer_request_id[i])) == NULL)
            PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", transfer_request_id[i]);
        all_transfer_request[i] = (pdc_transfer_request *)(transferinfo->obj_ptr);
        if (NULL == all_transfer_request[i])
            PGOTO_ERROR(FAIL, "Transfer request is NULL\n");

        // Check if every requests are REGION_LOCAL
        if (all_transfer_request[i]->region_partition != PDC_REGION_LOCAL) {
            flag = 1;
            break;
        }

        // Check if every requests are on the same object
        if (i == 0)
            obj_id = all_transfer_request[i]->local_obj_id;
        else {
            if (all_transfer_request[i]->local_obj_id != obj_id) {
                flag = 1;
                break;
            }
            // Check for contiguous
            if (all_transfer_request[i]->local_region_ndim == 1) {
                if (all_transfer_request[i]->remote_region_offset[0] !=
                    all_transfer_request[i - 1]->remote_region_offset[0] +
                        all_transfer_request[i - 1]->remote_region_size[0]) {
                    flag = 1;
                    break;
                }
            }
            else {
                // TODO: currently only check for 1D
                flag = 1;
                break;
            }
        }

        new_buf_size += all_transfer_request[i]->total_data_size;
    }

    if (flag == 0) {
        // Copy data to merged new_buf
        new_buf = (void *)PDC_malloc(new_buf_size);

        if (all_transfer_request[0]->local_region_ndim == 1) {
            offset_local[0] = all_transfer_request[0]->local_region_offset[0];
            size_local[0]   = new_buf_size;
            new_local_reg   = PDCregion_create(1, offset_local, size_local);

            offset_remote[0] = all_transfer_request[0]->remote_region_offset[0];
            size_remote[0]   = new_buf_size;
            new_remote_reg   = PDCregion_create(1, offset_remote, size_remote);

            copy_off = offset_local[0];
            for (i = 0; i < size; ++i) {
                memcpy(new_buf + copy_off, all_transfer_request[i]->buf,
                       all_transfer_request[i]->total_data_size);
                copy_off += all_transfer_request[i]->total_data_size;
                // Mark the original requests a done
                all_transfer_request[i]->is_done = 1;
            }
        }

        *merged_request_id = PDCregion_transfer_create(new_buf, all_transfer_request[0]->access_type, obj_id,
                                                       new_local_reg, new_remote_reg);
        *merged_size       = 1;
        // Add new xfer id to the first request for later wait_all use
        all_transfer_request[0]->merged_request_id = *merged_request_id;
    }

    all_transfer_request = (pdc_transfer_request **)PDC_free(all_transfer_request);

done:
    FUNC_LEAVE(SUCCEED);
}

perr_t
#ifdef ENABLE_MPI
PDCregion_transfer_start_all_common(pdcid_t *transfer_request_id, int size, MPI_Comm comm)
#else
PDCregion_transfer_start_all_common(pdcid_t *transfer_request_id, int size, int comm)
#endif
{
    FUNC_ENTER(NULL);

    perr_t                               ret_value  = SUCCEED;
    int                                  write_size = 0, read_size = 0, posix_size = 0, merged_size = 0;
    pdc_transfer_request_start_all_pkg **write_transfer_requests = NULL, **read_transfer_requests = NULL;
    pdcid_t *                            posix_transfer_request_id, *merged_request_id;

    // Merge the transfer_request_ids when they are operating on the same obj and have contiguous off, len
    if (size > PDC_MERGE_TRANSFER_MIN_COUNT) {
        merged_request_id = PDC_malloc(sizeof(pdcid_t));
        merge_transfer_request_ids(transfer_request_id, size, merged_request_id, &merged_size);
        if (merged_size == 1) {
            size                = merged_size;
            transfer_request_id = merged_request_id;
        }
    }

    // Split write and read requests. Handle them separately.
    // [Tang] NOTE: prepare_start_all_requests include several metadata RPC operations
    ret_value = prepare_start_all_requests(transfer_request_id, size, &write_transfer_requests,
                                           &read_transfer_requests, &write_size, &read_size,
                                           &posix_transfer_request_id, &posix_size);
    PDC_Client_transfer_pthread_cnt_add(size);

#ifdef ENABLE_MPI
    if (comm != 0)
        MPI_Barrier(comm);
#endif

    // Start write requests
    if (write_size > 0)
        PDC_Client_start_all_requests(write_transfer_requests, write_size, comm);
    // Start read requests
    if (read_size > 0)
        PDC_Client_start_all_requests(read_transfer_requests, read_size, comm);

    // For POSIX consistency, we block here until the data is received by the server
    if (posix_size > 0) {
        LOG_ERROR("Wait for posix requests\n");
        PDCregion_transfer_wait_all(posix_transfer_request_id, posix_size);
        posix_transfer_request_id = (pdcid_t *)PDC_free(posix_transfer_request_id);
    }

    // Clean up memory
    finish_start_all_requests(write_transfer_requests, read_transfer_requests, write_size, read_size);
#ifdef ENABLE_MPI
    if (comm != 0)
        MPI_Barrier(comm);
#endif

    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_start_all(pdcid_t *transfer_request_id, int size)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDCregion_transfer_start_all_common(transfer_request_id, size, 0);

    FUNC_LEAVE(ret_value);
}

#ifdef ENABLE_MPI
perr_t
PDCregion_transfer_start_all_mpi(pdcid_t *transfer_request_id, int size, MPI_Comm comm)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDCregion_transfer_start_all_common(transfer_request_id, size, comm);

    FUNC_LEAVE(ret_value);
}
#endif

perr_t
PDCregion_transfer_start_common(pdcid_t transfer_request_id,
#ifdef ENABLE_MPI
                                MPI_Comm comm)
#else
                                int comm)
#endif
{
    FUNC_ENTER(NULL);

    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    size_t                unit;
    int                   i;
    hg_bulk_t             bulk_handle;

    if ((transferinfo = PDC_find_id(transfer_request_id)) == NULL)
        PGOTO_DONE(ret_value);

    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);

    if (transfer_request->metadata_id != NULL)
        PGOTO_ERROR(FAIL, "PDC_Client attempted to start existing transfer request");

    // Dynamic case is implemented within the the aggregated version. The main reason is that the target data
    // server may not be unique, so we may end up sending multiple requests to the same data server.
    // Aggregated method will take care of this type of operation.
    if (transfer_request->region_partition == PDC_REGION_DYNAMIC ||
        transfer_request->region_partition == PDC_REGION_LOCAL) {
#ifdef ENABLE_MPI
        PDCregion_transfer_start_all_mpi(&transfer_request_id, 1, comm);
#else
        PDCregion_transfer_start_all(&transfer_request_id, 1);
#endif
        PGOTO_DONE(ret_value);
    }

    PDC_Client_transfer_pthread_cnt_add(1);

    attach_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);

    // Pack local region to a contiguous memory buffer
    unit = transfer_request->unit;

    // Convert user buf into a contiguous buffer called , which is determined by the shape of local objects.
    pack_region_buffer(transfer_request->buf, transfer_request->obj_dims, transfer_request->total_data_size,
                       transfer_request->local_region_ndim, transfer_request->local_region_offset,
                       transfer_request->local_region_size, unit, transfer_request->access_type,
                       &(transfer_request->new_buf));

    if (transfer_request->region_partition == PDC_REGION_STATIC) {
        // Identify which part of the region is going to which data server.
        ret_value = static_region_partition(
            transfer_request->new_buf, transfer_request->remote_region_ndim, unit,
            transfer_request->access_type, transfer_request->obj_dims, transfer_request->remote_region_offset,
            transfer_request->remote_region_size, 1, &(transfer_request->n_obj_servers),
            &(transfer_request->obj_servers), &(transfer_request->sub_offsets),
            &(transfer_request->output_offsets), &(transfer_request->output_sizes),
            &(transfer_request->output_buf));
        if (transfer_request->n_obj_servers == 0) {
            LOG_ERROR("Error with static region partition, no server is selected\n");
            return FAIL;
        }
        // The following memories will be freed in the wait function.
        transfer_request->metadata_id =
            (uint64_t *)PDC_malloc(sizeof(uint64_t) * transfer_request->n_obj_servers);
        if (transfer_request->access_type == PDC_READ) {
            transfer_request->read_bulk_buf =
                (char **)PDC_malloc(sizeof(char *) * transfer_request->n_obj_servers);
        }
        for (i = 0; i < transfer_request->n_obj_servers; ++i) {
            if (transfer_request->access_type == PDC_READ) {
                transfer_request->read_bulk_buf[i] = transfer_request->output_buf[i];
            }
            ret_value = PDC_Client_transfer_request(
                &bulk_handle, transfer_request->output_buf[i], transfer_request->obj_id,
                transfer_request->obj_servers[i], transfer_request->obj_ndim, transfer_request->obj_dims,
                transfer_request->remote_region_ndim, transfer_request->output_offsets[i],
                transfer_request->output_sizes[i], unit, transfer_request->access_type,
                transfer_request->metadata_id + i, transfer_request->writeout_strategy);
            PDCregion_transfer_add_bulk_handle(transfer_request, bulk_handle);
        }
    }
    else if (transfer_request->region_partition == PDC_OBJ_STATIC) {
        // Static object partitioning means that all requests for the same object are sent to the same data
        // server.
        transfer_request->metadata_id   = (uint64_t *)PDC_malloc(sizeof(uint64_t));
        transfer_request->n_obj_servers = 1;
        if (transfer_request->access_type == PDC_READ) {
            transfer_request->read_bulk_buf =
                (char **)PDC_malloc(sizeof(char *) * transfer_request->n_obj_servers);
            transfer_request->read_bulk_buf[0] = transfer_request->new_buf;
        }
        // Submit transfer request to server by designating data server ID, remote region info, and contiguous
        // memory buffer for copy.
        ret_value = PDC_Client_transfer_request(
            &bulk_handle, transfer_request->new_buf, transfer_request->obj_id,
            transfer_request->data_server_id, transfer_request->obj_ndim, transfer_request->obj_dims,
            transfer_request->remote_region_ndim, transfer_request->remote_region_offset,
            transfer_request->remote_region_size, unit, transfer_request->access_type,
            transfer_request->metadata_id, transfer_request->writeout_strategy);
        PDCregion_transfer_add_bulk_handle(transfer_request, bulk_handle);
    }

    // For POSIX consistency, we block here until the data is received by the server
    if (transfer_request->consistency == PDC_CONSISTENCY_POSIX) {
        PDCregion_transfer_wait(transfer_request_id);
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_start(pdcid_t transfer_request_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDCregion_transfer_start_common(transfer_request_id, 0);

    FUNC_LEAVE(ret_value);
}

#ifdef ENABLE_MPI
perr_t
PDCregion_transfer_start_mpi(pdcid_t transfer_request_id, MPI_Comm comm)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    ret_value        = PDCregion_transfer_start_common(transfer_request_id, comm);

    FUNC_LEAVE(ret_value);
}
#endif

static perr_t
release_region_buffer(char *buf, uint64_t *obj_dims, int local_ndim, uint64_t *local_offset,
                      uint64_t *local_size, size_t unit, pdc_access_t access_type, int bulk_buf_size,
                      char *new_buf, char **bulk_buf, int **bulk_buf_ref, char **read_bulk_buf)
{
    FUNC_ENTER(NULL);

    uint64_t i, j;
    int      k;

    perr_t ret_value = SUCCEED;
    char * ptr;
    if (local_ndim == 2) {
        if (access_type == PDC_READ) {
            ptr = new_buf;
            memcpy(buf, ptr, local_size[0] * local_size[1] * unit);
        }
    }
    else if (local_ndim == 3) {
        if (access_type == PDC_READ) {
            ptr = new_buf;
            memcpy(buf, ptr, local_size[0] * local_size[1] * local_size[2] * unit);
        }
    }
    if (bulk_buf_ref) {
        for (k = 0; k < bulk_buf_size; ++k) {
            bulk_buf_ref[k][0]--;
            if (!bulk_buf_ref[k][0]) {
                if (bulk_buf[k])
                    bulk_buf[k] = (char *)PDC_free(bulk_buf[k]);
                bulk_buf_ref[k] = (int *)PDC_free(bulk_buf_ref[k]);
            }
        }
        bulk_buf_ref = (int **)PDC_free(bulk_buf_ref);
        bulk_buf     = (char **)PDC_free(bulk_buf);
    }
    if (local_ndim > 1 && new_buf)
        new_buf = (char *)PDC_free(new_buf);
    if (read_bulk_buf)
        read_bulk_buf = (char **)PDC_free(read_bulk_buf);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_status(pdcid_t transfer_request_id, pdc_transfer_status_t *completed)
{
    FUNC_ENTER(NULL);

    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    size_t                unit;
    int                   i;

    if ((transferinfo = PDC_find_id(transfer_request_id)) == NULL) {
        *completed = PDC_TRANSFER_STATUS_COMPLETE;
        PGOTO_DONE(ret_value);
    }

    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    if (transfer_request->metadata_id != NULL) {
        unit = transfer_request->unit;

        if (transfer_request->region_partition == PDC_REGION_STATIC ||
            transfer_request->region_partition == PDC_REGION_DYNAMIC ||
            transfer_request->region_partition == PDC_REGION_LOCAL) {
            for (i = 0; i < transfer_request->n_obj_servers; ++i) {
                ret_value = PDC_Client_transfer_request_status(transfer_request->metadata_id[i],
                                                               transfer_request->obj_servers[i], completed);
                if (*completed != PDC_TRANSFER_STATUS_COMPLETE)
                    PGOTO_DONE(ret_value);
            }
            // If we reach here, then all transfers are finished.
            for (i = 0; i < transfer_request->n_obj_servers; ++i) {
                if (transfer_request->access_type == PDC_READ) {
                    // We copy the data from different data server regions to the contiguous buffer. Subregion
                    // copy uses sub_offset/size to align to the remote obj region.
                    memcpy_subregion(transfer_request->remote_region_ndim, unit,
                                     transfer_request->access_type, transfer_request->new_buf,
                                     transfer_request->remote_region_size, transfer_request->read_bulk_buf[i],
                                     transfer_request->sub_offsets[i], transfer_request->output_sizes[i]);
                }
                if (transfer_request->output_buf) {
                    transfer_request->output_buf[i] = (char *)PDC_free(transfer_request->output_buf[i]);
                }
                transfer_request->output_offsets[i] =
                    (uint64_t *)PDC_free(transfer_request->output_offsets[i]);
            }
            // Copy read data from a contiguous buffer back to the user buffer using local data information.
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
            transfer_request->output_offsets = (uint64_t **)PDC_free(transfer_request->output_offsets);
            if (transfer_request->output_buf) {
                transfer_request->output_buf = (char **)PDC_free(transfer_request->output_buf);
            }
            transfer_request->obj_servers = (uint32_t *)PDC_free(transfer_request->obj_servers);
        }
        else if (transfer_request->region_partition == PDC_OBJ_STATIC) {
            ret_value = PDC_Client_transfer_request_status(transfer_request->metadata_id[0],
                                                           transfer_request->data_server_id, completed);
            if (*completed != PDC_TRANSFER_STATUS_COMPLETE)
                PGOTO_DONE(ret_value);
            if (transfer_request->access_type == PDC_READ) {
                memcpy(transfer_request->new_buf, transfer_request->read_bulk_buf[0],
                       transfer_request->total_data_size);
            }
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
        }
        transfer_request->metadata_id = (uint64_t *)PDC_free(transfer_request->metadata_id);
        transfer_request->metadata_id = NULL;
        transfer_request->is_done     = 1;
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);
    }
    else {
        *completed = PDC_TRANSFER_STATUS_NOT_FOUND;
    }
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_wait_all(pdcid_t *transfer_request_id, int size)
{
    FUNC_ENTER(NULL);

    perr_t                              ret_value = SUCCEED;
    int                                 index, i, j, merged_xfer = 0, ori_size = size, is_first = 1;
    size_t                              unit;
    int                                 total_requests, n_objs;
    uint64_t *                          metadata_ids, merge_off = 0, cur_off = 0;
    pdc_transfer_request_wait_all_pkg **transfer_requests, *transfer_request_head, *transfer_request_end,
        *temp;
    hg_bulk_t bulk_handle;

    struct _pdc_id_info **transferinfos;
    struct _pdc_id_info * transfer_info;
    pdc_transfer_request *transfer_request, *merged_request;
    pdcid_t *             my_transfer_request_id = transfer_request_id;

    double t0, t1;

    if (!size)
        PGOTO_DONE(ret_value);

    transferinfos = (struct _pdc_id_info **)PDC_malloc(size * sizeof(struct _pdc_id_info *));

#ifdef ENABLE_MPI
    t0 = MPI_Wtime();
#endif

    // Check if we merged the previous request
    if (size > PDC_MERGE_TRANSFER_MIN_COUNT) {
        if ((transferinfos[0] = PDC_find_id(transfer_request_id[0])) == NULL)
            PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", transfer_request_id[0]);
        transfer_request = (pdc_transfer_request *)(transferinfos[0]->obj_ptr);
        if (transfer_request->merged_request_id != 0) {
            my_transfer_request_id = &transfer_request->merged_request_id;
            size                   = 1;
            merged_xfer            = 1;
        }
    }

    total_requests        = 0;
    transfer_request_head = NULL;
    for (i = 0; i < size; ++i) {
        if ((transferinfos[i] = PDC_find_id(my_transfer_request_id[i])) == NULL)
            continue;
        transfer_request = (pdc_transfer_request *)(transferinfos[i]->obj_ptr);
        if (1 == transfer_request->is_done)
            continue;
        if (!transfer_request->metadata_id) {
            LOG_ERROR("Attempt to wait for a transfer request "
                      "that has not been started. %d/%d\n",
                      i, size);
            continue;
        }
        total_requests += transfer_request->n_obj_servers;
        for (j = 0; j < transfer_request->n_obj_servers; ++j) {
            if (transfer_request_head) {
                transfer_request_end->next = (pdc_transfer_request_wait_all_pkg *)PDC_malloc(
                    sizeof(pdc_transfer_request_wait_all_pkg));
                transfer_request_end       = transfer_request_end->next;
                transfer_request_end->next = NULL;
            }
            else {
                transfer_request_head = (pdc_transfer_request_wait_all_pkg *)PDC_malloc(
                    sizeof(pdc_transfer_request_wait_all_pkg));
                transfer_request_head->next = NULL;
                transfer_request_end        = transfer_request_head;
            }
            if (transfer_request->region_partition == PDC_OBJ_STATIC) {
                transfer_request_end->data_server_id = transfer_request->data_server_id;
            }
            else {
                transfer_request_end->data_server_id = transfer_request->obj_servers[j];
            }
            transfer_request_end->metadata_id      = transfer_request->metadata_id[j];
            transfer_request_end->transfer_request = transfer_request;
            transfer_request_end->index            = j;
        }
    }

    transfer_requests = (pdc_transfer_request_wait_all_pkg **)PDC_malloc(
        sizeof(pdc_transfer_request_wait_all_pkg *) * total_requests);
    temp = transfer_request_head;
    for (i = 0; i < total_requests; ++i) {
        transfer_requests[i] = temp;

        temp = temp->next;
    }
    qsort(transfer_requests, total_requests, sizeof(pdc_transfer_request_wait_all_pkg *),
          sort_by_data_server_wait_all);

    metadata_ids = (uint64_t *)PDC_malloc(sizeof(uint64_t) * total_requests);
    index        = 0;
    for (i = 1; i < total_requests; ++i) {
        if (transfer_requests[i]->data_server_id != transfer_requests[i - 1]->data_server_id) {
            // Freed at the wait operation (inside PDC_client_connect call)
            n_objs = i - index;
            for (j = index; j < i; ++j) {
                metadata_ids[j] = transfer_requests[j]->metadata_id;
            }

            PDC_Client_transfer_request_wait_all(&bulk_handle, n_objs, metadata_ids + index,
                                                 transfer_requests[index]->data_server_id);
            PDCregion_transfer_add_bulk_handle(transfer_requests[index]->transfer_request, bulk_handle);
            for (j = index; j < i; ++j) {
                if (transfer_requests[j]->transfer_request->region_partition == PDC_REGION_STATIC ||
                    transfer_requests[j]->transfer_request->region_partition == PDC_REGION_DYNAMIC ||
                    transfer_requests[j]->transfer_request->region_partition == PDC_REGION_LOCAL) {
                    if (transfer_requests[j]->transfer_request->access_type == PDC_READ) {
                        // We copy the data from different data server regions to the contiguous buffer.
                        // Subregion copy uses sub_offset/size to align to the remote obj region.
                        memcpy_subregion(
                            transfer_requests[j]->transfer_request->remote_region_ndim,
                            transfer_requests[j]->transfer_request->unit,
                            transfer_requests[j]->transfer_request->access_type,
                            transfer_requests[j]->transfer_request->new_buf,
                            transfer_requests[j]->transfer_request->remote_region_size,
                            transfer_requests[j]
                                ->transfer_request->read_bulk_buf[transfer_requests[j]->index],
                            transfer_requests[j]->transfer_request->sub_offsets[transfer_requests[j]->index],
                            transfer_requests[j]
                                ->transfer_request->output_sizes[transfer_requests[j]->index]);
                    }
                    if (transfer_requests[j]->transfer_request->output_buf) {
                        transfer_requests[j]->transfer_request->output_buf[transfer_requests[j]->index] =
                            (char *)PDC_free(transfer_requests[j]
                                                 ->transfer_request->output_buf[transfer_requests[j]->index]);
                    }
                    transfer_requests[j]->transfer_request->output_offsets[transfer_requests[j]->index] =
                        (uint64_t *)PDC_free(
                            transfer_requests[j]
                                ->transfer_request->output_offsets[transfer_requests[j]->index]);
                }
            }
            index = i;
        }
    }

    if (total_requests) {
        // Freed at the wait operation (inside PDC_client_connect call)
        n_objs = total_requests - index;
        for (j = index; j < total_requests; ++j) {
            metadata_ids[j] = transfer_requests[j]->metadata_id;
        }
        PDC_Client_transfer_request_wait_all(&bulk_handle, n_objs, metadata_ids + index,
                                             transfer_requests[index]->data_server_id);
        PDCregion_transfer_add_bulk_handle(transfer_requests[index]->transfer_request, bulk_handle);
        for (j = index; j < total_requests; ++j) {
            if (transfer_requests[j]->transfer_request->region_partition == PDC_REGION_STATIC ||
                transfer_requests[j]->transfer_request->region_partition == PDC_REGION_DYNAMIC ||
                transfer_requests[j]->transfer_request->region_partition == PDC_REGION_LOCAL) {
                if (transfer_requests[j]->transfer_request->access_type == PDC_READ) {
                    // We copy the data from different data server regions to the contiguous buffer. Subregion
                    // copy uses sub_offset/size to align to the remote obj region.
                    memcpy_subregion(
                        transfer_requests[j]->transfer_request->remote_region_ndim,
                        transfer_requests[j]->transfer_request->unit,
                        transfer_requests[j]->transfer_request->access_type,
                        transfer_requests[j]->transfer_request->new_buf,
                        transfer_requests[j]->transfer_request->remote_region_size,
                        transfer_requests[j]->transfer_request->read_bulk_buf[transfer_requests[j]->index],
                        transfer_requests[j]->transfer_request->sub_offsets[transfer_requests[j]->index],
                        transfer_requests[j]->transfer_request->output_sizes[transfer_requests[j]->index]);
                }
                if (transfer_requests[j]->transfer_request->output_buf) {
                    transfer_requests[j]->transfer_request->output_buf[transfer_requests[j]->index] =
                        (char *)PDC_free(
                            transfer_requests[j]->transfer_request->output_buf[transfer_requests[j]->index]);
                }
                transfer_requests[j]->transfer_request->output_offsets[transfer_requests[j]->index] =
                    (uint64_t *)PDC_free(
                        transfer_requests[j]->transfer_request->output_offsets[transfer_requests[j]->index]);
            }
        }
    }

    // Deal with merged read requests, need to copy a large buffer to each of the original request buf
    // TODO: Currently only supports 1D merging, so only consider 1D for now
    if (merged_xfer == 1) {
        if ((transfer_info = PDC_find_id(my_transfer_request_id[0])) == NULL)
            PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", my_transfer_request_id[0]);
        merged_request = (pdc_transfer_request *)(transfer_info->obj_ptr);
        for (i = 0; i < ori_size; ++i) {
            transfer_request = (pdc_transfer_request *)(transfer_info->obj_ptr);
            if (transfer_request->access_type == PDC_READ) {
                if (is_first == 1)
                    merge_off = transfer_request->remote_region_offset[0];
                cur_off = transfer_request->remote_region_offset[0] - merge_off;
                if (!transfer_request->new_buf)
                    transfer_request->new_buf = PDC_malloc(transfer_request->total_data_size);

                memcpy(transfer_request->new_buf, merged_request->read_bulk_buf[0] + merge_off,
                       transfer_request->total_data_size);

                is_first = 0;
            }
        }
    }

    for (i = 0; i < size; ++i) {
        if (NULL == transferinfos[i])
            continue;
        transfer_request = (pdc_transfer_request *)(transferinfos[i]->obj_ptr);
        if (1 == transfer_request->is_done)
            continue;
        unit = transfer_request->unit;

        if (transfer_request->region_partition == PDC_OBJ_STATIC &&
            transfer_request->access_type == PDC_READ) {
            memcpy(transfer_request->new_buf, transfer_request->read_bulk_buf[0],
                   transfer_request->total_data_size);
        }

        release_region_buffer(
            transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
            transfer_request->local_region_offset, transfer_request->local_region_size, unit,
            transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
            transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);

        if (transfer_request->region_partition == PDC_REGION_STATIC ||
            transfer_request->region_partition == PDC_REGION_DYNAMIC ||
            transfer_request->region_partition == PDC_REGION_LOCAL) {
            transfer_request->output_offsets = (uint64_t **)PDC_free(transfer_request->output_offsets);
            transfer_request->output_sizes   = (uint64_t **)PDC_free(transfer_request->output_sizes);
            transfer_request->sub_offsets    = (uint64_t **)PDC_free(transfer_request->sub_offsets);
            if (transfer_request->output_buf) {
                transfer_request->output_buf = (char **)PDC_free(transfer_request->output_buf);
            }
            transfer_request->obj_servers = (uint32_t *)PDC_free(transfer_request->obj_servers);
        }
        transfer_request->metadata_id = (uint64_t *)PDC_free(transfer_request->metadata_id);
        transfer_request->is_done     = 1;
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id[i]);
    }

    for (i = 0; i < total_requests; ++i) {
        transfer_requests[i] = (pdc_transfer_request_wait_all_pkg *)PDC_free(transfer_requests[i]);
    }
    transfer_requests = (pdc_transfer_request_wait_all_pkg **)PDC_free(transfer_requests);
    metadata_ids      = (uint64_t *)PDC_free(metadata_ids);
    transferinfos     = (struct _pdc_id_info **)PDC_free(transferinfos);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_wait(pdcid_t transfer_request_id)
{
    FUNC_ENTER(NULL);

    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    size_t                unit;
    int                   i;

    if ((transferinfo = PDC_find_id(transfer_request_id)) == NULL)
        PGOTO_DONE(ret_value);

    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    if (transfer_request->metadata_id != NULL) {
        // For region dynamic case, it is implemented in the aggregated version for portability.
        if (transfer_request->region_partition == PDC_REGION_DYNAMIC ||
            transfer_request->region_partition == PDC_REGION_LOCAL) {
            PDCregion_transfer_wait_all(&transfer_request_id, 1);
            PGOTO_DONE(ret_value);
        }

        unit = transfer_request->unit;

        if (transfer_request->region_partition == PDC_REGION_STATIC) {

            for (i = 0; i < transfer_request->n_obj_servers; ++i) {
                ret_value = PDC_Client_transfer_request_wait(transfer_request->metadata_id[i],
                                                             transfer_request->obj_servers[i],
                                                             transfer_request->access_type);
                if (transfer_request->access_type == PDC_READ) {
                    // We copy the data from different data server regions to the contiguous buffer. Subregion
                    // copy uses sub_offset/size to align to the remote obj region.
                    memcpy_subregion(transfer_request->remote_region_ndim, unit,
                                     transfer_request->access_type, transfer_request->new_buf,
                                     transfer_request->remote_region_size, transfer_request->read_bulk_buf[i],
                                     transfer_request->sub_offsets[i], transfer_request->output_sizes[i]);
                }
                if (transfer_request->output_buf) {
                    transfer_request->output_buf[i] = (char *)PDC_free(transfer_request->output_buf[i]);
                }
                transfer_request->output_offsets[i] =
                    (uint64_t *)PDC_free(transfer_request->output_offsets[i]);
            }
            // Copy read data from a contiguous buffer back to the user buffer using local data information.
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
            transfer_request->output_offsets = (uint64_t **)PDC_free(transfer_request->output_offsets);
            transfer_request->output_sizes   = (uint64_t **)PDC_free(transfer_request->output_sizes);
            transfer_request->sub_offsets    = (uint64_t **)PDC_free(transfer_request->sub_offsets);
            if (transfer_request->output_buf) {
                transfer_request->output_buf = (char **)PDC_free(transfer_request->output_buf);
            }
            transfer_request->obj_servers = (uint32_t *)PDC_free(transfer_request->obj_servers);
        }
        else if (transfer_request->region_partition == PDC_OBJ_STATIC) {
            ret_value = PDC_Client_transfer_request_wait(transfer_request->metadata_id[0],
                                                         transfer_request->data_server_id,
                                                         transfer_request->access_type);
            if (transfer_request->access_type == PDC_READ) {
                memcpy(transfer_request->new_buf, transfer_request->read_bulk_buf[0],
                       transfer_request->total_data_size);
            }
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
        }
        transfer_request->metadata_id = (uint64_t *)PDC_free(transfer_request->metadata_id);
        transfer_request->is_done     = 1;
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);
    }
    else {
        // metadata is freed with previous wait (e.g. with posix consistency)
        ret_value = SUCCEED;
    }

done:
    FUNC_LEAVE(ret_value);
}
