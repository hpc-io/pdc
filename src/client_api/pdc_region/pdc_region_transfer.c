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
#include <mpi.h>

// pdc region transfer class. Contains essential information for performing non-blocking PDC client I/O
// perations.
typedef struct pdc_transfer_request {
    pdcid_t obj_id;
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
    // Simple counter for internal implementation usage, initialized to be zero.
    int bulk_buf_index;
    // buffer used for bulk transfer in mercury
    char *new_buf;
    // For each of the contig buffer sent to a server, we have a bulk buffer.
    char **bulk_buf;
    // Reference counter for bulk_buf, if 0, we free it.
    int **                 bulk_buf_ref;
    pdc_region_partition_t region_partition;

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
    pdcid_t                 ret_value = SUCCEED;
    struct _pdc_id_info *   objinfo2;
    struct _pdc_obj_info *  obj2;
    pdc_transfer_request *  p;
    struct _pdc_id_info *   reginfo1, *reginfo2;
    struct pdc_region_info *reg1, *reg2;
    uint64_t *              ptr;
    uint64_t                unit;
    int                     j;

    FUNC_ENTER(NULL);
    reginfo1 = PDC_find_id(local_reg);
    reg1     = (struct pdc_region_info *)(reginfo1->obj_ptr);
    reginfo2 = PDC_find_id(remote_reg);
    reg2     = (struct pdc_region_info *)(reginfo2->obj_ptr);
    objinfo2 = PDC_find_id(obj_id);
    if (objinfo2 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate remote object ID");
    obj2 = (struct _pdc_obj_info *)(objinfo2->obj_ptr);
    // remote_meta_id = obj2->obj_info_pub->meta_id;

    p                   = PDC_MALLOC(pdc_transfer_request);
    p->obj_pointer      = obj2;
    p->mem_type         = obj2->obj_pt->obj_prop_pub->type;
    p->obj_id           = obj2->obj_info_pub->meta_id;
    p->access_type      = access_type;
    p->buf              = buf;
    p->bulk_buf_index   = 0;
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
    unit                  = p->unit;
    /*
        printf("creating a request from obj %s metadata id = %llu, access_type = %d\n",
       obj2->obj_info_pub->name, (long long unsigned)obj2->obj_info_pub->meta_id, access_type);
    */
    p->local_region_ndim   = reg1->ndim;
    p->local_region_offset = (uint64_t *)malloc(
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
    /*
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            printf("rank = %d transfer request create check obj ndim %d, dims [%lld, %lld, %lld],
       local_offset[0] = %lld, " "reg1->offset[0] = %lld\n", rank, (int)p->obj_ndim, (long long
       int)p->obj_dims[0], (long long int)p->obj_dims[1], (long long int)p->obj_dims[2], (long long
       int)p->local_region_offset[0], (long long int)reg1->offset[0]);
    */
    ret_value = PDC_id_register(PDC_TRANSFER_REQUEST, p);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_close(pdcid_t transfer_request_id)
{
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    perr_t                ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    transferinfo = PDC_find_id(transfer_request_id);
    if (transferinfo == NULL) {
        goto done;
    }
    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    if (transfer_request->metadata_id == NULL) {
        goto done;
    }
    free(transfer_request->local_region_offset);
    free(transfer_request->metadata_id);
    free(transfer_request);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(transfer_request_id) < 0)
        PGOTO_ERROR(FAIL, "PDC transfer request: problem of freeing id");
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
attach_local_transfer_request(struct _pdc_obj_info *p, pdcid_t transfer_request_id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);
    if (p->local_transfer_request_head != NULL) {
        p->local_transfer_request_end->next =
            (pdc_local_transfer_request *)malloc(sizeof(pdc_local_transfer_request));
        p->local_transfer_request_end       = p->local_transfer_request_end->next;
        p->local_transfer_request_end->next = NULL;
    }
    else {
        p->local_transfer_request_head =
            (pdc_local_transfer_request *)malloc(sizeof(pdc_local_transfer_request));
        p->local_transfer_request_end       = p->local_transfer_request_head;
        p->local_transfer_request_end->next = NULL;
    }
    p->local_transfer_request_end->local_id = transfer_request_id;
    p->local_transfer_request_size++;
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
remove_local_transfer_request(struct _pdc_obj_info *p, pdcid_t transfer_request_id)
{
    perr_t                             ret_value = SUCCEED;
    struct pdc_local_transfer_request *previous, *temp;

    FUNC_ENTER(NULL);

    temp     = p->local_transfer_request_head;
    previous = NULL;
    while (temp != NULL) {
        if (temp->local_id == transfer_request_id) {
            if (previous == NULL) {
                // removing first element. Carefully set the head.
                previous                       = p->local_transfer_request_head;
                p->local_transfer_request_head = p->local_transfer_request_head->next;
                free(previous);
            }
            else {
                // Not the first element, just take the current element away.
                previous->next = temp->next;
                free(temp);
            }
            p->local_transfer_request_size--;
            break;
        }
        previous = temp;
        temp     = temp->next;
    }

    fflush(stdout);
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
    perr_t   ret_value = SUCCEED;
    int      i, j;
    uint64_t static_offset, static_size;
    uint64_t x, s;
    int      split_dim;
    uint64_t region_size;

    FUNC_ENTER(NULL);

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
    x = pdc_server_num_g - obj_dims[split_dim] % pdc_server_num_g;

    *data_server_ids = (uint32_t *)malloc(sizeof(uint32_t) * pdc_server_num_g);

    *output_offsets = (uint64_t **)malloc(sizeof(uint64_t *) * pdc_server_num_g);
    *output_sizes   = (uint64_t **)malloc(sizeof(uint64_t *) * pdc_server_num_g);
    *sub_offsets    = (uint64_t **)malloc(sizeof(uint64_t *) * pdc_server_num_g);
    if (set_output_buf) {
        *output_buf = (char **)malloc(sizeof(char *) * pdc_server_num_g);
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
        // printf("static offset = %lu, static_size = %lu, offset = %lu, size = %lu\n", (long
        // unsigned)static_offset, (long unsigned)static_size, (long unsigned)offset[split_dim], (long
        // unsigned)size[split_dim]);
        // Check if this region fits into the server static region.
        if ((static_offset + static_size > offset[split_dim] && offset[split_dim] >= static_offset) ||
            (offset[split_dim] + size[split_dim] > static_offset && static_offset >= offset[split_dim])) {
            // record data server ID
            data_server_ids[0][*n_data_servers] = i;
            // The overlapping region is allocated here.
            output_offsets[0][*n_data_servers] = (uint64_t *)malloc(sizeof(uint64_t) * ndim * 3);
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
                output_buf[0][n_data_servers[0]] = (char *)calloc(region_size * unit, sizeof(char));
                if (access_type == PDC_WRITE) {
                    memcpy_subregion(ndim, unit, PDC_WRITE, buf, size, output_buf[0][n_data_servers[0]],
                                     sub_offsets[0][n_data_servers[0]], output_sizes[0][*n_data_servers]);
                }
            }
            *n_data_servers += 1;
        }
    }
    if (*n_data_servers != pdc_server_num_g) {
        *data_server_ids = (uint32_t *)realloc(*data_server_ids, sizeof(uint32_t) * n_data_servers[0]);
        *output_offsets  = (uint64_t **)realloc(*output_offsets, sizeof(uint64_t *) * n_data_servers[0]);
        *output_sizes    = (uint64_t **)realloc(*output_sizes, sizeof(uint64_t *) * n_data_servers[0]);

        *sub_offsets = (uint64_t **)realloc(*sub_offsets, sizeof(uint64_t *) * n_data_servers[0]);
        if (set_output_buf) {
            *output_buf = (char **)realloc(*output_buf, sizeof(char *) * n_data_servers[0]);
        }
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
pack_region_buffer(char *buf, uint64_t *obj_dims, size_t total_data_size, int local_ndim,
                   uint64_t *local_offset, uint64_t *local_size, size_t unit, pdc_access_t access_type,
                   char **new_buf)
{
    uint64_t i, j;
    perr_t   ret_value = SUCCEED;
    char *   ptr;

    FUNC_ENTER(NULL);

    if (local_ndim == 1) {
        /*
                printf("checkpoint at local copy ndim == 1 local_offset[0] = %lld @ line %d\n",
                       (long long int)local_offset[0], __LINE__);
        */
        *new_buf = buf + local_offset[0] * unit;
    }
    else if (local_ndim == 2) {
        *new_buf = (char *)malloc(sizeof(char) * total_data_size);
        if (access_type == PDC_WRITE) {
            ptr = *new_buf;
            for (i = 0; i < local_size[0]; ++i) {
                memcpy(ptr, buf + ((local_offset[0] + i) * obj_dims[1] + local_offset[1]) * unit,
                       local_size[1] * unit);
                ptr += local_size[1] * unit;
            }
        }
    }
    else if (local_ndim == 3) {
        *new_buf = (char *)malloc(sizeof(char) * total_data_size);
        if (access_type == PDC_WRITE) {
            ptr = *new_buf;
            for (i = 0; i < local_size[0]; ++i) {
                for (j = 0; j < local_size[1]; ++j) {
                    memcpy(ptr,
                           buf + ((local_offset[0] + i) * obj_dims[1] * obj_dims[2] +
                                  (local_offset[1] + j) * obj_dims[2] + local_offset[2]) *
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

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
set_obj_server_bufs(pdc_transfer_request *transfer_request)
{
    perr_t ret_value = SUCCEED;
    FUNC_ENTER(NULL);

    transfer_request->bulk_buf     = (char **)malloc(sizeof(char *) * transfer_request->n_obj_servers);
    transfer_request->bulk_buf_ref = (int **)malloc(sizeof(int *) * transfer_request->n_obj_servers);
    transfer_request->metadata_id  = (uint64_t *)malloc(sizeof(uint64_t) * transfer_request->n_obj_servers);
    // read_bulk_buf is filled later when the bulk transfer is packed.
    if (transfer_request->access_type == PDC_READ) {
        transfer_request->read_bulk_buf = (char **)malloc(sizeof(char *) * transfer_request->n_obj_servers);
    }
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
pack_region_metadata_query(pdc_transfer_request_start_all_pkg **transfer_request, int size, char **buf_ptr,
                           uint64_t *total_buf_size_ptr)
{
    perr_t   ret_value = SUCCEED;
    int      i;
    char *   ptr;
    uint64_t total_buf_size;
    uint8_t  region_partition;

    FUNC_ENTER(NULL);

    total_buf_size = 0;
    for (i = 0; i < size; ++i) {
        // ndim + Regions + obj_id + data_server id + data partition + unit
        total_buf_size += sizeof(int) +
                          sizeof(uint64_t) * 2 * transfer_request[i]->transfer_request->remote_region_ndim +
                          sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(size_t);
    }

    *buf_ptr = (char *)malloc(total_buf_size);
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
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
unpack_region_metadata_query(char *buf, pdc_transfer_request_start_all_pkg **transfer_request_input,
                             pdc_transfer_request_start_all_pkg **transfer_request_head_ptr,
                             pdc_transfer_request_start_all_pkg **transfer_request_end_ptr, int *size_ptr)
{
    perr_t                              ret_value = SUCCEED;
    pdc_transfer_request_start_all_pkg *transfer_request_head, *transfer_request_end;
    pdc_transfer_request *              local_request;
    int                                 size;
    int                                 i, j, index;
    int                                 counter;
    char *                              ptr;
    uint64_t                            region_size;
    uint64_t *                          sub_offset;
    FUNC_ENTER(NULL);

    local_request         = NULL;
    transfer_request_head = NULL;
    transfer_request_end  = NULL;
    ptr                   = buf;
    size                  = *(int *)ptr;
    ptr += sizeof(int);
    // printf("unpack_region_metadata_query: received %d obj partitions\n", size);
    counter = 0;
    index   = 0;
    for (i = 0; i < size; ++i) {
        if (transfer_request_head) {
            transfer_request_end->next =
                (pdc_transfer_request_start_all_pkg *)malloc(sizeof(pdc_transfer_request_start_all_pkg));
            transfer_request_end = transfer_request_end->next;
        }
        else {
            transfer_request_head =
                (pdc_transfer_request_start_all_pkg *)malloc(sizeof(pdc_transfer_request_start_all_pkg));
            transfer_request_end = transfer_request_head;
        }
        // Obj ID + Obj dim + region offset + region size
        if (!counter) {
            counter = *(int *)ptr;
            ptr += sizeof(int);
            local_request                = transfer_request_input[index]->transfer_request;
            local_request->n_obj_servers = counter;
            local_request->output_offsets =
                (uint64_t **)malloc(sizeof(uint64_t *) * local_request->n_obj_servers);
            local_request->output_sizes =
                (uint64_t **)malloc(sizeof(uint64_t *) * local_request->n_obj_servers);
            local_request->sub_offsets =
                (uint64_t **)malloc(sizeof(uint64_t *) * local_request->n_obj_servers);
            local_request->obj_servers = (uint32_t *)malloc(sizeof(uint32_t) * local_request->n_obj_servers);
            // printf("unpack_region_metadata_query: checkpoint %d, local_request = %llu, index = %d\n",
            // __LINE__, (long long unsigned) local_request, index);
            set_obj_server_bufs(local_request);
        }
        // printf("unpack_region_metadata_query: @ line %d, i = %d, counter = %d\n", __LINE__, i, counter);
        transfer_request_end->next             = NULL;
        transfer_request_end->transfer_request = local_request;
        transfer_request_end->data_server_id   = *(uint32_t *)ptr;
        ptr += sizeof(uint32_t);
        transfer_request_end->remote_offset =
            (uint64_t *)malloc(sizeof(uint64_t) * local_request->remote_region_ndim * 3);
        transfer_request_end->remote_size =
            transfer_request_end->remote_offset + local_request->remote_region_ndim;
        sub_offset = transfer_request_end->remote_offset + local_request->remote_region_ndim * 2;
        memcpy(transfer_request_end->remote_offset, ptr,
               sizeof(uint64_t) * local_request->remote_region_ndim * 2);
        ptr += sizeof(uint64_t) * local_request->remote_region_ndim * 2;

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
        /*
                printf("unpack_region_metadata_query: @ line %d remote_region_offset = %lu, %lu,
           remote_region_size "
                       "= %lu, %lu, suboffset = %lu, %lu, remote_offset = %lu,%lu, remote_size = %lu,%lu\n",
                       __LINE__, (long unsigned)local_request->remote_region_offset[0],
                       (long unsigned)local_request->remote_region_offset[1],
                       (long unsigned)local_request->remote_region_size[0],
                       (long unsigned)local_request->remote_region_size[1], (long unsigned)sub_offset[0],
                       (long unsigned)sub_offset[1], (long unsigned)transfer_request_end->remote_offset[0],
                       (long unsigned)transfer_request_end->remote_offset[1],
                       (long unsigned)transfer_request_end->remote_size[0],
                       (long unsigned)transfer_request_end->remote_size[1]);
        */
        if (local_request->access_type == PDC_WRITE) {
            transfer_request_end->buf = (char *)malloc(region_size);
            memcpy_subregion(local_request->remote_region_ndim, local_request->unit,
                             local_request->access_type, transfer_request_input[index]->buf,
                             local_request->remote_region_size, transfer_request_end->buf, sub_offset,
                             transfer_request_end->remote_size);
        }
        counter--;
        if (!counter) {
            index++;
        }
        // printf("unpack_region_metadata_query: @ line %d\n", __LINE__);
    }

    *transfer_request_head_ptr = transfer_request_head;
    *transfer_request_end_ptr  = transfer_request_end;
    *size_ptr += size;

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
register_metadata(pdc_transfer_request_start_all_pkg **transfer_request_input, int input_size,
                  uint8_t is_write, pdc_transfer_request_start_all_pkg ***transfer_request_output_ptr,
                  int *output_size_ptr)
{
    perr_t                               ret_value = SUCCEED;
    int                                  i, j, index, size, output_size, remain_size, n_objs;
    pdc_transfer_request_start_all_pkg **transfer_requests;
    pdc_transfer_request_start_all_pkg * transfer_request_head, *transfer_request_front_head,
        *transfer_request_end, **transfer_request_output, *previous = NULL;
    uint64_t total_buf_size, output_buf_size, query_id;
    char *   buf, *output_buf;

    FUNC_ENTER(NULL);
    transfer_request_output     = NULL;
    transfer_request_front_head = NULL;
    transfer_requests           = (pdc_transfer_request_start_all_pkg **)malloc(
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

    index = 0;
    qsort(transfer_requests, size, sizeof(pdc_transfer_request_start_all_pkg *),
          sort_by_metadata_server_start_all);
    // printf("register_metadata @ line %d: input_size = %d, size = %d\n", __LINE__, input_size, size);
    for (i = 1; i < size; ++i) {
        // fprintf(stderr, "register_metadata: checkpoint %d, sending offset = %lu, size = %lu\n", __LINE__,
        // transfer_request[i]->remote_offset[0], transfer_request[i]->remote_size[0]);
        if (transfer_requests[i]->transfer_request->metadata_server_id !=
            transfer_requests[i - 1]->transfer_request->metadata_server_id) {
            n_objs = i - index;
            pack_region_metadata_query(transfer_requests + index, n_objs, &buf, &total_buf_size);
            PDC_Client_transfer_request_metadata_query(
                buf, total_buf_size, n_objs, transfer_requests[index]->transfer_request->metadata_server_id,
                is_write, &output_buf_size, &query_id);
            // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
            free(buf);
            // If it is a valid query ID, then it means regions are overlapping.
            if (query_id) {
                output_buf = (char *)malloc(output_buf_size);
                PDC_Client_transfer_request_metadata_query2(
                    output_buf, output_buf_size, query_id,
                    transfer_requests[index]->transfer_request->metadata_server_id);
                // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
                unpack_region_metadata_query(output_buf, transfer_requests + index, &transfer_request_head,
                                             &transfer_request_end, &output_size);
                // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
                free(output_buf);
                if (transfer_request_front_head) {
                    previous->next = transfer_request_head;
                }
                else {
                    transfer_request_front_head = transfer_request_head;
                }
                previous = transfer_request_end;
            }
            index = i;
        }
        // printf("register_metadata @ line %d: query_id = %lu, i = %d\n", __LINE__, (long unsigned)query_id,
        // i);
    }

    if (size) {
        n_objs = size - index;
        // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
        pack_region_metadata_query(transfer_requests + index, n_objs, &buf, &total_buf_size);
        // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
        PDC_Client_transfer_request_metadata_query(
            buf, total_buf_size, n_objs, transfer_requests[index]->transfer_request->metadata_server_id,
            is_write, &output_buf_size, &query_id);
        // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
        free(buf);
        // If it is a valid query ID, then it means regions are overlapping.
        if (query_id) {
            output_buf = (char *)malloc(output_buf_size);
            // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
            PDC_Client_transfer_request_metadata_query2(
                output_buf, output_buf_size, query_id,
                transfer_requests[index]->transfer_request->metadata_server_id);
            // fprintf(stderr, "register_metadata: checkpoint %d\n", __LINE__);
            unpack_region_metadata_query(output_buf, transfer_requests + index, &transfer_request_head,
                                         &transfer_request_end, &output_size);
            // fprintf(stderr, "register_metadata: output_size = %d, checkpoint %d\n", output_size, __LINE__);
            free(output_buf);
            if (transfer_request_front_head) {
                previous->next = transfer_request_head;
            }
            else {
                transfer_request_front_head = transfer_request_head;
            }
            previous = transfer_request_end;
        }
    }

    if (output_size) {
        transfer_request_output = (pdc_transfer_request_start_all_pkg **)malloc(
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
                free(transfer_request_input[i]);
            }
            else {
                transfer_request_output[j] = transfer_request_input[i];
                j++;
            }
        }
        free(transfer_request_input);
        *transfer_request_output_ptr = transfer_request_output;
        *output_size_ptr             = j;
    }
    else {
        *transfer_request_output_ptr = transfer_request_input;
        *output_size_ptr             = input_size;
    }

    free(transfer_requests);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

/**
 * This function prepares lists of read and write requests separately for start_all function. The lists are
 * sorted in terms of data_server_id. We pack data from user buffer to contiguous buffers. Static partitioning
 * requires having at most n_data_servers number of contiguous regions.
 */
static int
prepare_start_all_requests(pdcid_t *transfer_request_id, int size,
                           pdc_transfer_request_start_all_pkg ***write_transfer_request_ptr,
                           pdc_transfer_request_start_all_pkg ***read_transfer_request_ptr,
                           int *write_size_ptr, int *read_size_ptr)
{
    int                                  i, j;
    int                                  unit;
    pdc_transfer_request_start_all_pkg **write_transfer_request, **read_transfer_request, *write_request_pkgs,
        *read_request_pkgs, *write_request_pkgs_end, *read_request_pkgs_end, *request_pkgs,
        **transfer_request_output;
    int                   write_size, read_size, output_size;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    int                   set_output_buf = 0;

    write_request_pkgs = NULL;
    read_request_pkgs  = NULL;
    write_size         = 0;
    read_size          = 0;
    for (i = 0; i < size; ++i) {
        transferinfo     = PDC_find_id(transfer_request_id[i]);
        transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
        if (transfer_request->metadata_id != NULL) {
            printf("PDC Client PDCregion_transfer_start_all attempt to start existing transfer request @ "
                   "line %d\n",
                   __LINE__);
            return 1;
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
            for (j = 0; j < transfer_request->n_obj_servers; ++j) {
                request_pkgs =
                    (pdc_transfer_request_start_all_pkg *)malloc(sizeof(pdc_transfer_request_start_all_pkg));
                request_pkgs->transfer_request = transfer_request;
                request_pkgs->data_server_id   = transfer_request->obj_servers[j];
                request_pkgs->remote_offset    = transfer_request->output_offsets[j];
                request_pkgs->remote_size      = transfer_request->output_sizes[j];
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
                (pdc_transfer_request_start_all_pkg *)malloc(sizeof(pdc_transfer_request_start_all_pkg));
            request_pkgs->transfer_request = transfer_request;
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
        write_transfer_request = (pdc_transfer_request_start_all_pkg **)malloc(
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
        read_transfer_request = (pdc_transfer_request_start_all_pkg **)malloc(
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
    return 0;
}

static int
finish_start_all_requests(pdc_transfer_request_start_all_pkg **write_transfer_request,
                          pdc_transfer_request_start_all_pkg **read_transfer_request, int write_size,
                          int read_size)
{
    int i;
    for (i = 0; i < write_size; ++i) {
        free(write_transfer_request[i]);
    }
    for (i = 0; i < read_size; ++i) {
        free(read_transfer_request[i]);
    }
    // MPI_Barrier(MPI_COMM_WORLD);
    // fprintf(stderr, "checkpoint %d\n", __LINE__);
    if (write_size) {
        free(write_transfer_request);
    }
    if (read_size) {
        free(read_transfer_request);
    }
    return 0;
}

static perr_t
PDC_Client_pack_all_requests(int n_objs, pdc_transfer_request_start_all_pkg **transfer_requests,
                             pdc_access_t access_type, char **bulk_buf_ptr, size_t *total_buf_size_ptr,
                             char **read_bulk_buf)
{
    perr_t ret_value = SUCCEED;
    char * bulk_buf, *ptr, *ptr2;
    size_t total_buf_size, obj_data_size, total_obj_data_size, unit, data_size, metadata_size;
    int    i, j;

    FUNC_ENTER(NULL);
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
    metadata_size = n_objs * (sizeof(pdcid_t) + sizeof(int) * 2 + sizeof(size_t));
    // printf("checkpoint @ line %d\n", __LINE__);
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
        // printf("checkpoint i = %d, remote_region_size = %lu, unit = %lu @ line %d\n", i,
        // transfer_requests[i]->remote_size[0], transfer_requests[i]->transfer_request->unit,  __LINE__);
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
    // printf("checkpoint @ line %d\n", __LINE__);
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
    // printf("checkpoint @ line %d, total_buf_size = %lu, metadata_size = %lu, data_size = %lu\n", __LINE__,
    // total_buf_size, metadata_size, data_size);
    bulk_buf      = (char *)malloc(total_buf_size);
    *bulk_buf_ptr = bulk_buf;
    ptr           = bulk_buf;
    ptr2          = bulk_buf;
    // Pack metadata
#define MEMCPY_INC(a, b)                                                                                     \
    {                                                                                                        \
        memcpy(ptr, a, b);                                                                                   \
        ptr += b;                                                                                            \
    }
    // printf("checkpoint @ line %d\n", __LINE__);
    for (i = 0; i < n_objs; ++i) {
        unit = transfer_requests[i]->transfer_request->unit;
        MEMCPY_INC(&(transfer_requests[i]->transfer_request->obj_id), sizeof(pdcid_t));
        MEMCPY_INC(&(transfer_requests[i]->transfer_request->obj_ndim), sizeof(int));
        MEMCPY_INC(&(transfer_requests[i]->transfer_request->remote_region_ndim), sizeof(int));
        MEMCPY_INC(&unit, sizeof(size_t));
    }

    // printf("checkpoint @ line %d\n", __LINE__);
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
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_Client_start_all_requests(pdc_transfer_request_start_all_pkg **transfer_requests, int size)
{
    perr_t    ret_value = SUCCEED;
    int       index, i, j;
    int       n_objs;
    uint64_t *metadata_id;
    char **   read_bulk_buf;
    char *    bulk_buf;
    size_t    bulk_buf_size;
    int *     bulk_buf_ref;

    FUNC_ENTER(NULL);
    metadata_id   = (uint64_t *)malloc(sizeof(uint64_t) * size);
    read_bulk_buf = (char **)malloc(sizeof(char *) * size);
    index         = 0;
    for (i = 1; i < size; ++i) {
        if (transfer_requests[i]->data_server_id != transfer_requests[i - 1]->data_server_id) {
            // Freed at the wait operation (inside PDC_client_connect call)
            n_objs = i - index;
            PDC_Client_pack_all_requests(n_objs, transfer_requests + index,
                                         transfer_requests[index]->transfer_request->access_type, &bulk_buf,
                                         &bulk_buf_size, read_bulk_buf + index);
            bulk_buf_ref    = (int *)malloc(sizeof(int));
            bulk_buf_ref[0] = n_objs;
            // printf("checkpoint @ line %d, index = %d, dataserver_id = %d, n_objs = %d\n", __LINE__, index,
            // transfer_requests[index]->data_server_id, n_objs);
            PDC_Client_transfer_request_all(n_objs, transfer_requests[index]->transfer_request->access_type,
                                            transfer_requests[index]->data_server_id, bulk_buf, bulk_buf_size,
                                            metadata_id + index);
            for (j = index; j < i; ++j) {
                // All requests share the same bulk buffer, reference counter is also shared among all
                // requests.
                transfer_requests[j]
                    ->transfer_request->bulk_buf[transfer_requests[j]->transfer_request->bulk_buf_index] =
                    bulk_buf;
                transfer_requests[j]
                    ->transfer_request->bulk_buf_ref[transfer_requests[j]->transfer_request->bulk_buf_index] =
                    bulk_buf_ref;
                if (transfer_requests[j]->transfer_request->access_type == PDC_READ) {
                    transfer_requests[j]
                        ->transfer_request
                        ->read_bulk_buf[transfer_requests[j]->transfer_request->bulk_buf_index] =
                        read_bulk_buf[j];
                }
                transfer_requests[j]
                    ->transfer_request->metadata_id[transfer_requests[j]->transfer_request->bulk_buf_index] =
                    metadata_id[j];
                transfer_requests[j]->transfer_request->bulk_buf_index++;
            }
            index = i;
        }
    }
    if (size) {
        // Freed at the wait operation (inside PDC_client_connect call)
        n_objs = size - index;
        // printf("checkpoint @ line %d\n", __LINE__);
        PDC_Client_pack_all_requests(n_objs, transfer_requests + index,
                                     transfer_requests[index]->transfer_request->access_type, &bulk_buf,
                                     &bulk_buf_size, read_bulk_buf + index);
        // printf("checkpoint @ line %d\n", __LINE__);
        bulk_buf_ref    = (int *)malloc(sizeof(int));
        bulk_buf_ref[0] = n_objs;
        // printf("checkpoint @ line %d, index = %d, dataserver_id = %d, n_objs = %d\n", __LINE__, index,
        // transfer_requests[index]->data_server_id, n_objs);
        PDC_Client_transfer_request_all(n_objs, transfer_requests[index]->transfer_request->access_type,
                                        transfer_requests[index]->data_server_id, bulk_buf, bulk_buf_size,
                                        metadata_id + index);
        for (j = index; j < size; ++j) {
            // All requests share the same bulk buffer, reference counter is also shared among all
            // requests.
            // printf("checkpoint @ line %d, j = %d, %d\n", __LINE__, j,
            // transfer_requests[j]->data_server_id);
            transfer_requests[j]
                ->transfer_request->bulk_buf[transfer_requests[j]->transfer_request->bulk_buf_index] =
                bulk_buf;
            transfer_requests[j]
                ->transfer_request->bulk_buf_ref[transfer_requests[j]->transfer_request->bulk_buf_index] =
                bulk_buf_ref;
            if (transfer_requests[j]->transfer_request->access_type == PDC_READ) {
                transfer_requests[j]
                    ->transfer_request
                    ->read_bulk_buf[transfer_requests[j]->transfer_request->bulk_buf_index] =
                    read_bulk_buf[j];
            }
            transfer_requests[j]
                ->transfer_request->metadata_id[transfer_requests[j]->transfer_request->bulk_buf_index] =
                metadata_id[j];
            transfer_requests[j]->transfer_request->bulk_buf_index++;
        }
    }
    free(read_bulk_buf);
    free(metadata_id);

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_start_all(pdcid_t *transfer_request_id, int size)
{
    perr_t                               ret_value  = SUCCEED;
    int                                  write_size = 0, read_size = 0;
    pdc_transfer_request_start_all_pkg **write_transfer_requests = NULL, **read_transfer_requests = NULL;

    FUNC_ENTER(NULL);
    // Split write and read requests. Handle them separately.
    // printf("PDCregion_transfer_start_all: checkpoint %d\n", __LINE__);
    prepare_start_all_requests(transfer_request_id, size, &write_transfer_requests, &read_transfer_requests,
                               &write_size, &read_size);
    // printf("PDCregion_transfer_start_all: checkpoint %d, write_size = %d, read_size = %d\n", __LINE__,
    //       write_size, read_size);
    // Start write requests
    PDC_Client_start_all_requests(write_transfer_requests, write_size);
    // printf("PDCregion_transfer_start_all: checkpoint %d\n", __LINE__);
    // Start read requests
    PDC_Client_start_all_requests(read_transfer_requests, read_size);
    /*
        fprintf(stderr, "PDCregion_transfer_start_all: checkpoint %d\n", __LINE__);
        MPI_Barrier(MPI_COMM_WORLD);
    */
    // Clean up memory
    finish_start_all_requests(write_transfer_requests, read_transfer_requests, write_size, read_size);
    /*
        fprintf(stderr, "PDCregion_transfer_start_all: checkpoint %d\n", __LINE__);
        MPI_Barrier(MPI_COMM_WORLD);
    */
    FUNC_LEAVE(ret_value);
}

/**
 * Input: Sorted arrays
 * Output: A single array that is sorted, and the union of sorted arrays.
 */
#if 0
static int sorted_array_unions(const int **array, const int *input_size, int n_arrays, int **out_array, int *out_size) {
    int i;
    int total_size = 0;
    int *size;
    int temp_n_arrays;
    int min;

    size = calloc(n_arrays, sizeof(int));

    for ( i = 0; i < n_arrays; ++i ) {
        total_size += size[i];
    }
    *out_array = (int*) malloc(sizeof(int) * total_size);
    *out_size = 0;

    while (1) {
        // check how many remaining arrays do we left.
        temp_n_arrays = 0;
        for ( i = 0; i < n_arrays; ++i ) {
            if ( size[i] < input_size[i] ) {
                temp_n_arrays++;
                break;
            }
        }
        // If no more remaining arrays, we are done.
        if (!temp_n_arrays) {
            break;
        }
        // Now we figure out the minimum element of the remaining lists and append it to the end of output array        
        min = -1;
        for ( i = 0; i < n_arrays; ++i ) {
            if ( size[i] == input_size[i] ) {
                continue;
            }
            if ( min == -1 || min > array[i][size[i]] ) {
                min = array[i][size[i]];
            }
        }
        out_array[0][*out_size] = min;
        out_size[0]++;
        // Increment the arrays.
        for ( i = 0; i < n_arrays; ++i ) {
            if ( min == array[i][size[i]] ) {
                size[i]++;
            }
        }
    }
    free(size);
    *out_array = realloc(*out_array, out_size[0]);
    return 0;
}
#endif
perr_t
PDCregion_transfer_start(pdcid_t transfer_request_id)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    size_t                unit;
    int                   i;

    FUNC_ENTER(NULL);

    transferinfo = PDC_find_id(transfer_request_id);

    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);

    if (transfer_request->metadata_id != NULL) {
        printf("PDC Client PDCregion_transfer_start attempt to start existing transfer request @ line %d\n",
               __LINE__);
        ret_value = FAIL;
        goto done;
    }
    // Dynamic case is implemented within the the aggregated version. The main reason is that the target data
    // server may not be unique, so we may end up sending multiple requests to the same data server.
    // Aggregated method will take care of this type of operation.
    if (transfer_request->region_partition == PDC_REGION_DYNAMIC ||
        transfer_request->region_partition == PDC_REGION_LOCAL) {
        PDCregion_transfer_start_all(&transfer_request_id, 1);
        goto done;
    }

    attach_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);

    // Pack local region to a contiguous memory buffer
    unit = transfer_request->unit;

    // Convert user buf into a contiguous buffer called new_buf, which is determined by the shape of local
    // objects.
    pack_region_buffer(transfer_request->buf, transfer_request->obj_dims, transfer_request->total_data_size,
                       transfer_request->local_region_ndim, transfer_request->local_region_offset,
                       transfer_request->local_region_size, unit, transfer_request->access_type,
                       &(transfer_request->new_buf));
    if (transfer_request->region_partition == PDC_REGION_STATIC) {
        // Identify which part of the region is going to which data server.
        static_region_partition(transfer_request->new_buf, transfer_request->remote_region_ndim, unit,
                                transfer_request->access_type, transfer_request->obj_dims,
                                transfer_request->remote_region_offset, transfer_request->remote_region_size,
                                1, &(transfer_request->n_obj_servers), &(transfer_request->obj_servers),
                                &(transfer_request->sub_offsets), &(transfer_request->output_offsets),
                                &(transfer_request->output_sizes), &(transfer_request->output_buf));
        /*
                printf("n_obj_servers = %d\n", transfer_request->n_obj_servers);
                for ( i = 0; i < transfer_request->n_obj_servers; ++i ) {
                    printf("sub_offsets = %lu, output_offsets = %lu, output_sizes = %lu\n",
           transfer_request->sub_offsets[i][0], transfer_request->output_offsets[i][0],
           transfer_request->output_sizes[i][0]);
                }
        */
        // The following memories will be freed in the wait function.
        transfer_request->metadata_id =
            (uint64_t *)malloc(sizeof(uint64_t) * transfer_request->n_obj_servers);
        if (transfer_request->access_type == PDC_READ) {
            transfer_request->read_bulk_buf =
                (char **)malloc(sizeof(char *) * transfer_request->n_obj_servers);
        }
        for (i = 0; i < transfer_request->n_obj_servers; ++i) {
            if (transfer_request->access_type == PDC_READ) {
                transfer_request->read_bulk_buf[i] = transfer_request->output_buf[i];
            }
            ret_value = PDC_Client_transfer_request(
                transfer_request->output_buf[i], transfer_request->obj_id, transfer_request->obj_servers[i],
                transfer_request->obj_ndim, transfer_request->obj_dims, transfer_request->remote_region_ndim,
                transfer_request->output_offsets[i], transfer_request->output_sizes[i], unit,
                transfer_request->access_type, transfer_request->metadata_id + i);
        }
    }
    else if (transfer_request->region_partition == PDC_OBJ_STATIC) {
        // Static object partitioning means that all requests for the same object are sent to the same data
        // server.
        transfer_request->metadata_id   = (uint64_t *)malloc(sizeof(uint64_t));
        transfer_request->n_obj_servers = 1;
        if (transfer_request->access_type == PDC_READ) {
            transfer_request->read_bulk_buf =
                (char **)malloc(sizeof(char *) * transfer_request->n_obj_servers);
            transfer_request->read_bulk_buf[0] = transfer_request->new_buf;
        }
        // Submit transfer request to server by designating data server ID, remote region info, and contiguous
        // memory buffer for copy.
        ret_value = PDC_Client_transfer_request(
            transfer_request->new_buf, transfer_request->obj_id, transfer_request->data_server_id,
            transfer_request->obj_ndim, transfer_request->obj_dims, transfer_request->remote_region_ndim,
            transfer_request->remote_region_offset, transfer_request->remote_region_size, unit,
            transfer_request->access_type, transfer_request->metadata_id);
    }
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
release_region_buffer(char *buf, uint64_t *obj_dims, int local_ndim, uint64_t *local_offset,
                      uint64_t *local_size, size_t unit, pdc_access_t access_type, int bulk_buf_size,
                      char *new_buf, char **bulk_buf, int **bulk_buf_ref, char **read_bulk_buf)
{
    uint64_t i, j;
    int      k;

    perr_t ret_value = SUCCEED;
    char * ptr;
    FUNC_ENTER(NULL);
    if (local_ndim == 2) {
        if (access_type == PDC_READ) {
            ptr = new_buf;
            for (i = 0; i < local_size[0]; ++i) {
                memcpy(buf + ((local_offset[0] + i) * obj_dims[1] + local_offset[1]) * unit, ptr,
                       local_size[1] * unit);
                ptr += local_size[1] * unit;
            }
        }
    }
    else if (local_ndim == 3) {
        if (access_type == PDC_READ) {
            ptr = new_buf;
            for (i = 0; i < local_size[0]; ++i) {
                for (j = 0; j < local_size[1]; ++j) {
                    memcpy(buf + ((local_offset[0] + i) * obj_dims[1] * obj_dims[2] +
                                  (local_offset[1] + j) * obj_dims[2] + local_offset[2]) *
                                     unit,
                           ptr, local_size[2] * unit);
                    ptr += local_size[2] * unit;
                }
            }
        }
    }
    if (bulk_buf_ref) {
        for (k = 0; k < bulk_buf_size; ++k) {
            bulk_buf_ref[k][0]--;
            if (!bulk_buf_ref[k][0]) {
                if (bulk_buf[k]) {
                    free(bulk_buf[k]);
                }
                free(bulk_buf_ref[k]);
            }
        }
        free(bulk_buf_ref);
        free(bulk_buf);
    }
    if (local_ndim > 1 && new_buf) {
        free(new_buf);
    }
    if (read_bulk_buf) {
        free(read_bulk_buf);
    }

    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_status(pdcid_t transfer_request_id, pdc_transfer_status_t *completed)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    size_t                unit;
    int                   i;

    FUNC_ENTER(NULL);

    transferinfo     = PDC_find_id(transfer_request_id);
    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    if (transfer_request->metadata_id != NULL) {
        unit = transfer_request->unit;

        if (transfer_request->region_partition == PDC_REGION_STATIC ||
            transfer_request->region_partition == PDC_REGION_DYNAMIC ||
            transfer_request->region_partition == PDC_REGION_LOCAL) {
            for (i = 0; i < transfer_request->n_obj_servers; ++i) {
                ret_value = PDC_Client_transfer_request_status(transfer_request->metadata_id[i],
                                                               transfer_request->obj_servers[i], completed);
                if (*completed != PDC_TRANSFER_STATUS_COMPLETE) {
                    goto done;
                }
            }
            // If we reach here, then all transfers are finished.
            for (i = 0; i < transfer_request->n_obj_servers; ++i) {
                // printf("finished waiting for data from server %d, output_offsets = %lu, output_size = %lu,
                // metadata_id = %lu\n", transfer_request->obj_servers[i],
                // transfer_request->output_offsets[i][0], transfer_request->output_sizes[i][0],
                // transfer_request->metadata_id[i]);
                if (transfer_request->access_type == PDC_READ) {
                    // We copy the data from different data server regions to the contiguous buffer. Subregion
                    // copy uses sub_offset/size to align to the remote obj region.
                    // printf("sub_offsets = %lu, output_size = %lu, remote_region_size = %lu\n",
                    // transfer_request->sub_offsets[i][0], transfer_request->output_sizes[i][0],
                    // transfer_request->remote_region_size[0]);
                    memcpy_subregion(transfer_request->remote_region_ndim, unit,
                                     transfer_request->access_type, transfer_request->new_buf,
                                     transfer_request->remote_region_size, transfer_request->read_bulk_buf[i],
                                     transfer_request->sub_offsets[i], transfer_request->output_sizes[i]);
                }
                if (transfer_request->output_buf) {
                    free(transfer_request->output_buf[i]);
                }
                free(transfer_request->output_offsets[i]);
                // free(transfer_request->output_sizes[i]);
                // free(transfer_request->sub_offsets[i]);
            }
            // Copy read data from a contiguous buffer back to the user buffer using local data information.
            // printf("rank %d checkpoint %d\n", pdc_client_mpi_rank_g, __LINE__);
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
            free(transfer_request->output_offsets);
            // free(transfer_request->output_sizes);
            // free(transfer_request->sub_offsets);
            if (transfer_request->output_buf) {
                free(transfer_request->output_buf);
            }
            free(transfer_request->obj_servers);
        }
        else if (transfer_request->region_partition == PDC_OBJ_STATIC) {
            ret_value = PDC_Client_transfer_request_status(transfer_request->metadata_id[0],
                                                           transfer_request->data_server_id, completed);
            if (*completed != PDC_TRANSFER_STATUS_COMPLETE) {
                goto done;
            }
            /*
                        uint64_t k;
                        printf("print bulk_buf :");
                        for ( k = 0; k < transfer_request->local_region_size[0] * 2; ++k ) {
                            printf("%d,", *(int*) (transfer_request->bulk_buf + sizeof(int) * k));
                        }
                        printf("\n");
            */
            if (transfer_request->access_type == PDC_READ) {
                // printf("copy %lu bytes of data\n", transfer_request->total_data_size);
                memcpy(transfer_request->new_buf, transfer_request->read_bulk_buf[0],
                       transfer_request->total_data_size);
            }
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
        }
        free(transfer_request->metadata_id);
        transfer_request->metadata_id = NULL;
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);
    }
    else {
        *completed = PDC_TRANSFER_STATUS_NOT_FOUND;
    }
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_wait_all(pdcid_t *transfer_request_id, int size)
{
    perr_t                              ret_value = SUCCEED;
    int                                 index, i, j;
    size_t                              unit;
    int                                 total_requests, n_objs;
    uint64_t *                          metadata_ids;
    pdc_transfer_request_wait_all_pkg **transfer_requests, *transfer_request_head, *transfer_request_end,
        *temp;

    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;

    FUNC_ENTER(NULL);
    // printf("entered PDCregion_transfer_wait_all @ line %d\n", __LINE__);
    total_requests        = 0;
    transfer_request_head = NULL;
    for (i = 0; i < size; ++i) {
        transferinfo     = PDC_find_id(transfer_request_id[i]);
        transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
        if (!transfer_request->metadata_id) {
            fprintf(stderr,
                    "PDCregion_transfer_wait_all [rank %d] @ line %d: Attempt to wait for a transfer request "
                    "that has not been started.\n",
                    pdc_client_mpi_rank_g, __LINE__);
            ret_value = FAIL;
            goto done;
        }
        total_requests += transfer_request->n_obj_servers;
        for (j = 0; j < transfer_request->n_obj_servers; ++j) {
            if (transfer_request_head) {
                transfer_request_end->next =
                    (pdc_transfer_request_wait_all_pkg *)malloc(sizeof(pdc_transfer_request_wait_all_pkg));
                transfer_request_end       = transfer_request_end->next;
                transfer_request_end->next = NULL;
            }
            else {
                transfer_request_head =
                    (pdc_transfer_request_wait_all_pkg *)malloc(sizeof(pdc_transfer_request_wait_all_pkg));
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
    transfer_requests = (pdc_transfer_request_wait_all_pkg **)malloc(
        sizeof(pdc_transfer_request_wait_all_pkg *) * total_requests);
    temp = transfer_request_head;
    for (i = 0; i < total_requests; ++i) {
        transfer_requests[i] = temp;
        temp                 = temp->next;
    }
    qsort(transfer_requests, total_requests, sizeof(pdc_transfer_request_wait_all_pkg *),
          sort_by_data_server_wait_all);

    metadata_ids = (uint64_t *)malloc(sizeof(uint64_t) * total_requests);
    index        = 0;
    for (i = 1; i < total_requests; ++i) {
        if (i && transfer_requests[i]->data_server_id != transfer_requests[i - 1]->data_server_id) {
            // Freed at the wait operation (inside PDC_client_connect call)
            n_objs = i - index;
            for (j = index; j < i; ++j) {
                metadata_ids[j] = transfer_requests[j]->metadata_id;
            }
            PDC_Client_transfer_request_wait_all(n_objs, metadata_ids + index,
                                                 transfer_requests[index]->data_server_id);
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
                        free(transfer_requests[j]->transfer_request->output_buf[transfer_requests[j]->index]);
                    }
                    free(transfer_requests[j]->transfer_request->output_offsets[transfer_requests[j]->index]);
                    // free(transfer_requests[j]->transfer_request->output_sizes[transfer_requests[j]->index]);
                    // free(transfer_requests[j]->transfer_request->sub_offsets[transfer_requests[j]->index]);
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
        PDC_Client_transfer_request_wait_all(n_objs, metadata_ids + index,
                                             transfer_requests[index]->data_server_id);
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
                /*
                                        uint64_t k;
                                        fprintf(stderr,"print new_buf :");
                                        for ( k = 0; k < transfer_request->local_region_size[0]; ++k ) {
                                            fprintf(stderr,"%d,", *(int*) (transfer_request->new_buf +
                   sizeof(int) * k));

                                        }
                                        printf("\n");
                */
                if (transfer_requests[j]->transfer_request->output_buf) {
                    free(transfer_requests[j]->transfer_request->output_buf[transfer_requests[j]->index]);
                }
                free(transfer_requests[j]->transfer_request->output_offsets[transfer_requests[j]->index]);
                // free(transfer_requests[j]->transfer_request->output_sizes[transfer_requests[j]->index]);
                // free(transfer_requests[j]->transfer_request->sub_offsets[transfer_requests[j]->index]);
            }
        }
    }

    for (i = 0; i < size; ++i) {
        transferinfo     = PDC_find_id(transfer_request_id[i]);
        transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
        unit             = transfer_request->unit;

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
            free(transfer_request->output_offsets);
            free(transfer_request->output_sizes);
            free(transfer_request->sub_offsets);
            if (transfer_request->output_buf) {
                free(transfer_request->output_buf);
            }
            free(transfer_request->obj_servers);
        }
        free(transfer_request->metadata_id);
        transfer_request->metadata_id = NULL;
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id[i]);
    }

    for (i = 0; i < total_requests; ++i) {
        free(transfer_requests[i]);
    }
    free(transfer_requests);
    free(metadata_ids);
    /*
            for (i = 0; i < size; ++i) {
                PDCregion_transfer_wait(transfer_request_id[i]);
            }
    */
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_wait(pdcid_t transfer_request_id)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    size_t                unit;
    int                   i;

    FUNC_ENTER(NULL);

    transferinfo     = PDC_find_id(transfer_request_id);
    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    if (transfer_request->metadata_id != NULL) {
        // For region dynamic case, it is implemented in the aggregated version for portability.
        if (transfer_request->region_partition == PDC_REGION_DYNAMIC ||
            transfer_request->region_partition == PDC_REGION_LOCAL) {
            PDCregion_transfer_wait_all(&transfer_request_id, 1);
            goto done;
        }

        unit = transfer_request->unit;

        if (transfer_request->region_partition == PDC_REGION_STATIC) {

            for (i = 0; i < transfer_request->n_obj_servers; ++i) {
                ret_value = PDC_Client_transfer_request_wait(transfer_request->metadata_id[i],
                                                             transfer_request->obj_servers[i],
                                                             transfer_request->access_type);
                // printf("finished waiting for data from server %d, output_offsets = %lu, output_size = %lu,
                // metadata_id = %lu\n", transfer_request->obj_servers[i],
                // transfer_request->output_offsets[i][0], transfer_request->output_sizes[i][0],
                // transfer_request->metadata_id[i]);
                if (transfer_request->access_type == PDC_READ) {
                    // We copy the data from different data server regions to the contiguous buffer. Subregion
                    // copy uses sub_offset/size to align to the remote obj region.
                    // printf("sub_offsets = %lu, output_size = %lu, remote_region_size = %lu\n",
                    // transfer_request->sub_offsets[i][0], transfer_request->output_sizes[i][0],
                    // transfer_request->remote_region_size[0]);
                    memcpy_subregion(transfer_request->remote_region_ndim, unit,
                                     transfer_request->access_type, transfer_request->new_buf,
                                     transfer_request->remote_region_size, transfer_request->read_bulk_buf[i],
                                     transfer_request->sub_offsets[i], transfer_request->output_sizes[i]);
                }
                if (transfer_request->output_buf) {
                    free(transfer_request->output_buf[i]);
                }
                free(transfer_request->output_offsets[i]);
                // free(transfer_request->output_sizes[i]);
                // free(transfer_request->sub_offsets[i]);
            }
            // Copy read data from a contiguous buffer back to the user buffer using local data information.
            // printf("rank %d checkpoint %d\n", pdc_client_mpi_rank_g, __LINE__);
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
            free(transfer_request->output_offsets);
            free(transfer_request->output_sizes);
            free(transfer_request->sub_offsets);
            if (transfer_request->output_buf) {
                free(transfer_request->output_buf);
            }
            free(transfer_request->obj_servers);
        }
        else if (transfer_request->region_partition == PDC_OBJ_STATIC) {
            ret_value = PDC_Client_transfer_request_wait(transfer_request->metadata_id[0],
                                                         transfer_request->data_server_id,
                                                         transfer_request->access_type);
            /*
                        uint64_t k;
                        printf("print bulk_buf :");
                        for ( k = 0; k < transfer_request->local_region_size[0] * 2; ++k ) {
                            printf("%d,", *(int*) (transfer_request->bulk_buf + sizeof(int) * k));
                        }
                        printf("\n");
            */
            if (transfer_request->access_type == PDC_READ) {
                // printf("copy %lu bytes of data\n", transfer_request->total_data_size);
                memcpy(transfer_request->new_buf, transfer_request->read_bulk_buf[0],
                       transfer_request->total_data_size);
            }
            release_region_buffer(
                transfer_request->buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
                transfer_request->local_region_offset, transfer_request->local_region_size, unit,
                transfer_request->access_type, transfer_request->n_obj_servers, transfer_request->new_buf,
                transfer_request->bulk_buf, transfer_request->bulk_buf_ref, transfer_request->read_bulk_buf);
        }
        free(transfer_request->metadata_id);
        transfer_request->metadata_id = NULL;
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);
    }
    else {
        printf("PDC Client PDCregion_transfer_status attempt to check status for inactive transfer request @ "
               "line %d\n",
               __LINE__);
        ret_value = FAIL;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
