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
#include "../server/pdc_utlist.h"
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

static perr_t pdc_region_close(struct pdc_region_info *op);
static perr_t pdc_transfer_request_close();

perr_t
PDC_region_init()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the region IDs */
    if (PDC_register_type(PDC_REGION, (PDC_free_t)pdc_region_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize region interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_transfer_request_init()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the region IDs */
    if (PDC_register_type(PDC_TRANSFER_REQUEST, (PDC_free_t)pdc_transfer_request_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize region interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_list_null()
{
    perr_t ret_value = SUCCEED;
    int    nelemts;

    FUNC_ENTER(NULL);

    // list is not empty
    nelemts = PDC_id_list_null(PDC_REGION);
    if (nelemts > 0) {
        if (PDC_id_list_clear(PDC_REGION) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
pdc_region_close(struct pdc_region_info *op)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    free(op->size);
    free(op->offset);
    if (op->obj != NULL)
        op->obj = PDC_FREE(struct _pdc_obj_info, op->obj);
    op = PDC_FREE(struct pdc_region_info, op);

    FUNC_LEAVE(ret_value);
}

perr_t
pdc_transfer_request_close()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_close(pdcid_t region_id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(region_id) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_end()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (PDC_destroy_type(PDC_REGION) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy region interface");
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
static pdc_transfer_status_t check_local_transfer_request(struct _pdc_obj_info *p, pdcid_t
transfer_request_id) { pdc_transfer_status_t ret_value; struct pdc_local_transfer_request *temp;

    FUNC_ENTER(NULL);

    ret_value = PDC_TRANSFER_STATUS_NOT_FOUND;

    temp = p->local_transfer_request_head;

    while ( temp != NULL ) {
        if (temp->local_id == transfer_request_id) {
            ret_value = PDC_TRANSFER_STATUS_PENDING;
        }
        temp = temp->next;
    }
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
*/
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

    p                 = PDC_MALLOC(pdc_transfer_request);
    p->obj_pointer    = obj2;
    p->mem_type       = obj2->obj_pt->obj_prop_pub->type;
    p->obj_id         = obj2->obj_info_pub->meta_id;
    p->access_type    = access_type;
    p->buf            = buf;
    p->metadata_id    = 0;
    p->data_server_id = ((pdc_metadata_t *)obj2->metadata)->data_server_id;
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
    if (transfer_request->metadata_id > 0) {
        goto done;
    }
    free(transfer_request->local_region_offset);
    free(transfer_request);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(transfer_request_id) < 0)
        PGOTO_ERROR(FAIL, "PDC transfer request: problem of freeing id");
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

int
sort_by_data_server(const void *elem1, const void *elem2)
{
    const pdc_transfer_request *f = *(const pdc_transfer_request **)elem1;
    const pdc_transfer_request *s = *(const pdc_transfer_request **)elem2;
    if (f->data_server_id > s->data_server_id)
        return 1;
    if (f->data_server_id < s->data_server_id)
        return -1;
    return 0;
}

perr_t
PDCregion_transfer_start_all(pdcid_t *transfer_request_id, int size)
{
    perr_t                ret_value = SUCCEED;
    int                   i, j;
    void **               buf;
    pdcid_t *             obj_id;
    int *                 obj_ndim;
    uint64_t **           obj_dims;
    int *                 local_ndim;
    uint64_t **           local_offset;
    uint64_t **           local_size;
    int *                 remote_ndim;
    uint64_t **           remote_offset;
    uint64_t **           remote_size;
    pdc_var_type_t *      mem_type;
    pdcid_t *             metadata_id;
    char **               new_buf_ptr, **read_bulk_buf = NULL;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;
    int *                 new_buf_ref;
    int                   write_size, read_size, mem_size;
    int                   index;

    pdc_transfer_request **write_transfer_request, **read_transfer_request;

    FUNC_ENTER(NULL);
    // Split write and read requests. Handle them separately.
    write_transfer_request = (pdc_transfer_request **)malloc(sizeof(pdc_transfer_request *) * size);
    write_size             = 0;
    for (i = 0; i < size; ++i) {
        transferinfo     = PDC_find_id(transfer_request_id[i]);
        transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
        if (transfer_request->metadata_id > 0) {
            printf("PDC Client PDCregion_transfer_start_all attempt to start existing transfer request @ "
                   "line %d\n",
                   __LINE__);
            ret_value = FAIL;
            goto done;
        }
        attach_local_transfer_request(transfer_request->obj_pointer, transfer_request_id[i]);
        if (transfer_request->access_type == PDC_WRITE) {
            write_transfer_request[write_size] = transfer_request;
            write_size++;
        }
    }
    read_size             = 0;
    read_transfer_request = write_transfer_request + write_size;
    for (i = 0; i < size; ++i) {
        transferinfo     = PDC_find_id(transfer_request_id[i]);
        transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
        if (transfer_request->access_type == PDC_READ) {
            read_transfer_request[read_size] = transfer_request;
            read_size++;
        }
    }
    qsort(write_transfer_request, write_size, sizeof(pdc_transfer_request *), sort_by_data_server);
    qsort(read_transfer_request, read_size, sizeof(pdc_transfer_request *), sort_by_data_server);

    if (write_size > read_size) {
        mem_size = write_size;
    }
    else {
        mem_size = read_size;
    }
    // Arrays of request arguments. These are freed at the end of this function.
    buf           = (void **)malloc(sizeof(void *) * mem_size);
    obj_id        = (pdcid_t *)malloc(sizeof(pdcid_t) * mem_size);
    obj_ndim      = (int *)malloc(sizeof(int) * mem_size);
    obj_dims      = (uint64_t **)malloc(sizeof(uint64_t *) * mem_size);
    local_ndim    = (int *)malloc(sizeof(int) * mem_size);
    local_offset  = (uint64_t **)malloc(sizeof(uint64_t *) * mem_size);
    local_size    = (uint64_t **)malloc(sizeof(uint64_t *) * mem_size);
    remote_ndim   = (int *)malloc(sizeof(int) * mem_size);
    remote_offset = (uint64_t **)malloc(sizeof(uint64_t *) * mem_size);
    remote_size   = (uint64_t **)malloc(sizeof(uint64_t *) * mem_size);
    mem_type      = (pdc_var_type_t *)malloc(sizeof(pdc_var_type_t) * mem_size);
    metadata_id   = (pdcid_t *)malloc(sizeof(pdcid_t) * mem_size);
    new_buf_ptr   = (char **)malloc(sizeof(char *) * mem_size);
    if (read_size) {
        read_bulk_buf = (char **)malloc(sizeof(char *) * mem_size);
    }
    /*
        for (i = 0; i < size; ++i) {
            PDCregion_transfer_start(transfer_request_id[i]);
        }
    */
    // Need to differentiate write and read operations. Perform write first. We do one data_server_id at a
    // time.
    index = 0;
    for (i = 0; i < write_size; ++i) {
        if (i && write_transfer_request[i]->data_server_id != write_transfer_request[i - 1]->data_server_id) {
            // Freed at the wait operation (inside PDC_client_connect call)
            new_buf_ref = (int *)malloc(sizeof(int));
            PDC_Client_transfer_request_all(
                i - index, buf + index, write_transfer_request[i - 1]->data_server_id, obj_id,
                obj_ndim + index, obj_dims + index, local_ndim + index, local_offset + index,
                local_size + index, remote_ndim + index, remote_offset + index, remote_size + index,
                mem_type + index, PDC_WRITE, metadata_id + index, NULL, new_buf_ptr + index, new_buf_ref);
            for (j = index; j < i; ++j) {
                // All requests share the same bulk buffer, reference counter is also shared among all
                // requests.
                write_transfer_request[j]->new_buf     = new_buf_ptr[j];
                write_transfer_request[j]->new_buf_ref = new_buf_ref;
                // Extract remote ID.
                write_transfer_request[j]->metadata_id = metadata_id[j];
            }
            index = i;
        }

        buf[i]           = write_transfer_request[i]->buf;
        obj_id[i]        = write_transfer_request[i]->obj_id;
        obj_ndim[i]      = write_transfer_request[i]->obj_ndim;
        obj_dims[i]      = write_transfer_request[i]->obj_dims;
        local_ndim[i]    = write_transfer_request[i]->local_region_ndim;
        local_offset[i]  = write_transfer_request[i]->local_region_offset;
        local_size[i]    = write_transfer_request[i]->local_region_size;
        remote_ndim[i]   = write_transfer_request[i]->remote_region_ndim;
        remote_offset[i] = write_transfer_request[i]->remote_region_offset;
        remote_size[i]   = write_transfer_request[i]->remote_region_size;
        mem_type[i]      = write_transfer_request[i]->mem_type;
    }
    if (write_size) {
        // Freed at the wait operation (inside PDC_client_connect call)
        new_buf_ref = (int *)malloc(sizeof(int));
        PDC_Client_transfer_request_all(
            write_size - index, buf + index, write_transfer_request[write_size - 1]->data_server_id, obj_id,
            obj_ndim + index, obj_dims + index, local_ndim + index, local_offset + index, local_size + index,
            remote_ndim + index, remote_offset + index, remote_size + index, mem_type + index, PDC_WRITE,
            metadata_id + index, NULL, new_buf_ptr + index, new_buf_ref);
    }
    for (i = index; i < write_size; ++i) {
        // All requests share the same bulk buffer, reference counter is also shared among all requests.
        write_transfer_request[i]->new_buf     = new_buf_ptr[i];
        write_transfer_request[i]->new_buf_ref = new_buf_ref;
        // Extract remote ID.
        write_transfer_request[i]->metadata_id = metadata_id[i];
    }
    // Read request is implemented as the following.
    /*
        for ( i = 0; i < size; ++i ) {
            // All requests share the same bulk buffer, reference counter is also shared among all requests.
            transferinfo     = PDC_find_id(transfer_request_id[i]);
            transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
            if (transfer_request->access_type == PDC_READ) {
                PDCregion_transfer_start(transfer_request_id[i]);
            }
        }
    */

    // Start for read operation.
    index = 0;
    for (i = 0; i < read_size; ++i) {
        if (i && read_transfer_request[i]->data_server_id != read_transfer_request[i - 1]->data_server_id) {
            // Freed at the wait operation (inside PDC_client_connect call)
            new_buf_ref = (int *)malloc(sizeof(int));
            PDC_Client_transfer_request_all(
                i - index, buf + index, read_transfer_request[i - 1]->data_server_id, obj_id + index,
                obj_ndim + index, obj_dims + index, local_ndim + index, local_offset + index,
                local_size + index, remote_ndim + index, remote_offset + index, remote_size + index,
                mem_type + index, PDC_READ, metadata_id + index, read_bulk_buf + index, new_buf_ptr + index,
                new_buf_ref);
            for (j = index; j < i; ++j) {
                // All requests share the same bulk buffer, reference counter is also shared among all
                // requests.
                read_transfer_request[j]->new_buf       = new_buf_ptr[j];
                read_transfer_request[j]->new_buf_ref   = new_buf_ref;
                read_transfer_request[j]->read_bulk_buf = read_bulk_buf[j];
                // Extract remote ID.
                read_transfer_request[j]->metadata_id = metadata_id[j];
            }
            index = i;
        }

        buf[i]           = read_transfer_request[i]->buf;
        obj_id[i]        = read_transfer_request[i]->obj_id;
        obj_ndim[i]      = read_transfer_request[i]->obj_ndim;
        obj_dims[i]      = read_transfer_request[i]->obj_dims;
        local_ndim[i]    = read_transfer_request[i]->local_region_ndim;
        local_offset[i]  = read_transfer_request[i]->local_region_offset;
        local_size[i]    = read_transfer_request[i]->local_region_size;
        remote_ndim[i]   = read_transfer_request[i]->remote_region_ndim;
        remote_offset[i] = read_transfer_request[i]->remote_region_offset;
        remote_size[i]   = read_transfer_request[i]->remote_region_size;
        mem_type[i]      = read_transfer_request[i]->mem_type;
    }
    if (read_size) {
        // Freed at the wait operation (inside PDC_client_connect call)
        new_buf_ref = (int *)malloc(sizeof(int));
        PDC_Client_transfer_request_all(
            read_size - index, buf + index, read_transfer_request[i - 1]->data_server_id, obj_id + index,
            obj_ndim + index, obj_dims + index, local_ndim + index, local_offset + index, local_size + index,
            remote_ndim + index, remote_offset + index, remote_size + index, mem_type + index, PDC_READ,
            metadata_id + index, read_bulk_buf + index, new_buf_ptr + index, new_buf_ref);
    }

    for (i = index; i < read_size; ++i) {
        // All requests share the same bulk buffer, reference counter is also shared among all requests.
        read_transfer_request[i]->new_buf       = new_buf_ptr[i];
        read_transfer_request[i]->read_bulk_buf = read_bulk_buf[i];
        read_transfer_request[i]->new_buf_ref   = new_buf_ref;
        // Extract remote ID.
        read_transfer_request[i]->metadata_id = metadata_id[i];
    }
    if (read_size) {
        free(read_bulk_buf);
    }

    free(buf);
    free(obj_id);
    free(obj_ndim);
    free(obj_dims);
    free(local_ndim);
    free(local_offset);
    free(local_size);
    free(remote_ndim);
    free(remote_offset);
    free(remote_size);
    free(mem_type);
    free(metadata_id);
    free(new_buf_ptr);
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_start(pdcid_t transfer_request_id)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;

    FUNC_ENTER(NULL);

    transferinfo     = PDC_find_id(transfer_request_id);
    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);

    if (transfer_request->metadata_id > 0) {
        printf("PDC Client PDCregion_transfer_start attempt to start existing transfer request @ line %d\n",
               __LINE__);
        ret_value = FAIL;
        goto done;
    }
    attach_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);
    if (transfer_request->metadata_id == 0) {
        ret_value = PDC_Client_transfer_request(
            transfer_request->buf, transfer_request->obj_id, transfer_request->data_server_id,
            transfer_request->obj_ndim, transfer_request->obj_dims, transfer_request->local_region_ndim,
            transfer_request->local_region_offset, transfer_request->local_region_size,
            transfer_request->remote_region_ndim, transfer_request->remote_region_offset,
            transfer_request->remote_region_size, transfer_request->mem_type, transfer_request->access_type,
            &(transfer_request->metadata_id), &(transfer_request->read_bulk_buf),
            &(transfer_request->new_buf), &(transfer_request->new_buf_ref));
    }
    else {
        printf("PDC Client PDCregion_transfer_start attempt to start existing transfer request @ line %d\n",
               __LINE__);
        ret_value = FAIL;
    }
done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_status(pdcid_t transfer_request_id, pdc_transfer_status_t *completed)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;

    FUNC_ENTER(NULL);

    transferinfo     = PDC_find_id(transfer_request_id);
    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    if (transfer_request->metadata_id != 0) {
        ret_value = PDC_Client_transfer_request_status(
            transfer_request->metadata_id, transfer_request->data_server_id, completed, transfer_request->buf,
            transfer_request->new_buf, transfer_request->obj_dims, transfer_request->local_region_ndim,
            transfer_request->local_region_offset, transfer_request->local_region_size,
            transfer_request->mem_type, transfer_request->access_type, transfer_request->read_bulk_buf,
            transfer_request->new_buf_ref);
        if (*completed != PDC_TRANSFER_STATUS_PENDING) {
            transfer_request->metadata_id = 0;
        }
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);
    }
    else {
        *completed = PDC_TRANSFER_STATUS_NOT_FOUND;
    }
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_wait_all(pdcid_t *transfer_request_id, int size)
{
    perr_t                 ret_value = SUCCEED;
    struct _pdc_id_info *  transferinfo;
    pdc_transfer_request **transfer_request;
    int                    i;
    int                    index;
    char **                buf;
    pdcid_t *              metadata_id;
    uint64_t **            obj_dims;
    int *                  local_ndim;
    uint64_t **            local_offset;
    uint64_t **            local_size;
    pdc_access_t *         access_type;
    pdc_var_type_t *       mem_type;
    int **                 new_buf_ref;
    char **                new_buf, **read_bulk_buf = NULL;

    FUNC_ENTER(NULL);

    buf           = (char **)malloc(sizeof(char *) * size);
    metadata_id   = (pdcid_t *)malloc(sizeof(pdcid_t) * size);
    obj_dims      = (uint64_t **)malloc(sizeof(uint64_t *) * size);
    local_ndim    = (int *)malloc(sizeof(int) * size);
    local_offset  = (uint64_t **)malloc(sizeof(uint64_t *) * size);
    local_size    = (uint64_t **)malloc(sizeof(uint64_t *) * size);
    mem_type      = (pdc_var_type_t *)malloc(sizeof(pdc_var_type_t) * size);
    access_type   = (pdc_access_t *)malloc(sizeof(pdc_access_t) * size);
    new_buf_ref   = (int **)malloc(sizeof(int **) * size);
    new_buf       = (char **)malloc(sizeof(char *) * size);
    read_bulk_buf = (char **)malloc(sizeof(char *) * size);

    transfer_request = (pdc_transfer_request **)malloc(sizeof(pdc_transfer_request *) * size);
    // Extract transfer request from ID.
    for (i = 0; i < size; ++i) {
        transferinfo        = PDC_find_id(transfer_request_id[i]);
        transfer_request[i] = (pdc_transfer_request *)(transferinfo->obj_ptr);
        if (transfer_request[i]->metadata_id == 0) {
            printf("PDC Client PDCregion_transfer_start_all attempt to wait transfer request that does not "
                   "exist @ line %d\n",
                   __LINE__);
            ret_value = FAIL;
            goto done;
        }
        remove_local_transfer_request(transfer_request[i]->obj_pointer, transfer_request_id[i]);
    }
    // Sort is required for grouping requests with the same data server.
    qsort(transfer_request, size, sizeof(pdc_transfer_request *), sort_by_data_server);
    // Process one data server requests at a time. All requests for a data server are packed and send
    // together.
    index = 0;
    for (i = 0; i < size; ++i) {
        if (i && transfer_request[i]->data_server_id != transfer_request[i - 1]->data_server_id) {
            PDC_Client_transfer_request_wait_all(
                i - index, metadata_id + index, transfer_request[i - 1]->data_server_id, access_type + index,
                buf + index, new_buf + index, obj_dims + index, local_ndim + index, local_offset + index,
                local_size + index, mem_type + index, read_bulk_buf + index, new_buf_ref + index);
            index = i;
        }

        buf[i]           = transfer_request[i]->buf;
        metadata_id[i]   = transfer_request[i]->metadata_id;
        obj_dims[i]      = transfer_request[i]->obj_dims;
        local_ndim[i]    = transfer_request[i]->local_region_ndim;
        local_offset[i]  = transfer_request[i]->local_region_offset;
        local_size[i]    = transfer_request[i]->local_region_size;
        mem_type[i]      = transfer_request[i]->mem_type;
        access_type[i]   = transfer_request[i]->access_type;
        new_buf[i]       = transfer_request[i]->new_buf;
        new_buf_ref[i]   = transfer_request[i]->new_buf_ref;
        read_bulk_buf[i] = transfer_request[i]->read_bulk_buf;
    }
    if (size) {
        PDC_Client_transfer_request_wait_all(size - index, metadata_id + index,
                                             transfer_request[size - 1]->data_server_id, access_type + index,
                                             buf + index, new_buf + index, obj_dims + index,
                                             local_ndim + index, local_offset + index, local_size + index,
                                             mem_type + index, read_bulk_buf + index, new_buf_ref + index);
    }
    // Any further wait calls will immediately return.
    for (i = 0; i < size; ++i) {
        transfer_request[i]->metadata_id = 0;
    }
done:
    free(transfer_request);
    free(buf);
    free(metadata_id);
    free(obj_dims);
    free(local_ndim);
    free(local_offset);
    free(local_size);
    free(mem_type);
    free(access_type);
    free(new_buf);
    free(new_buf_ref);
    free(read_bulk_buf);
    /*
        for (i = 0; i < size; ++i) {
            PDCregion_transfer_wait(transfer_request_id[i]);
        }
    */
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_transfer_wait(pdcid_t transfer_request_id)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * transferinfo;
    pdc_transfer_request *transfer_request;

    FUNC_ENTER(NULL);

    transferinfo     = PDC_find_id(transfer_request_id);
    transfer_request = (pdc_transfer_request *)(transferinfo->obj_ptr);
    if (transfer_request->metadata_id != 0) {
        ret_value = PDC_Client_transfer_request_wait(
            transfer_request->metadata_id, transfer_request->data_server_id, transfer_request->access_type,
            transfer_request->buf, transfer_request->new_buf, transfer_request->obj_dims,
            transfer_request->local_region_ndim, transfer_request->local_region_offset,
            transfer_request->local_region_size, transfer_request->mem_type, transfer_request->read_bulk_buf,
            transfer_request->new_buf_ref);
        transfer_request->metadata_id = 0;
        remove_local_transfer_request(transfer_request->obj_pointer, transfer_request_id);
    }
    else {
        printf("PDC Client PDCregion_transfer_status attempt to check status for inactive transfer request @ "
               "line %d\n",
               __LINE__);
        ret_value = FAIL;
    }
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCregion_create(psize_t ndims, uint64_t *offset, uint64_t *size)
{
    pdcid_t                 ret_value = 0;
    struct pdc_region_info *p         = NULL;
    pdcid_t                 new_id;
    size_t                  i = 0;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct pdc_region_info);
    if (!p)
        PGOTO_ERROR(ret_value, "PDC region memory allocation failed");
    p->ndim     = ndims;
    p->obj      = NULL;
    p->offset   = (uint64_t *)malloc(ndims * sizeof(uint64_t));
    p->size     = (uint64_t *)malloc(ndims * sizeof(uint64_t));
    p->mapping  = 0;
    p->local_id = 0;
    for (i = 0; i < ndims; i++) {
        (p->offset)[i] = offset[i];
        (p->size)[i]   = size[i];
    }
    new_id      = PDC_id_register(PDC_REGION, p);
    p->local_id = new_id;
    ret_value   = new_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCbuf_obj_map(void *buf, pdc_var_type_t local_type, pdcid_t local_reg, pdcid_t remote_obj,
               pdcid_t remote_reg)
{
    pdcid_t               ret_value = SUCCEED;
    size_t                i;
    struct _pdc_id_info * objinfo2;
    struct _pdc_obj_info *obj2;
    pdcid_t               remote_meta_id;

    pdc_var_type_t          remote_type;
    struct _pdc_id_info *   reginfo1, *reginfo2;
    struct pdc_region_info *reg1, *reg2;

    FUNC_ENTER(NULL);

    reginfo1 = PDC_find_id(local_reg);
    reg1     = (struct pdc_region_info *)(reginfo1->obj_ptr);

    objinfo2 = PDC_find_id(remote_obj);
    if (objinfo2 == NULL)

        PGOTO_ERROR(FAIL, "cannot locate remote object ID");
    obj2           = (struct _pdc_obj_info *)(objinfo2->obj_ptr);
    remote_meta_id = obj2->obj_info_pub->meta_id;
    remote_type    = obj2->obj_pt->obj_prop_pub->type;

    reginfo2 = PDC_find_id(remote_reg);
    reg2     = (struct pdc_region_info *)(reginfo2->obj_ptr);
    if (obj2->obj_pt->obj_prop_pub->ndim != reg2->ndim)
        PGOTO_ERROR(FAIL, "remote object dimension and region dimension does not match");
    for (i = 0; i < reg2->ndim; i++)
        if ((obj2->obj_pt->obj_prop_pub->dims)[i] < (reg2->size)[i])
            PGOTO_ERROR(FAIL, "remote object region size error");

    ret_value = PDC_Client_buf_map(local_reg, remote_meta_id, reg1->ndim, reg1->size, reg1->offset,
                                   local_type, buf, remote_type, reg1, reg2);

    if (ret_value == SUCCEED) {
        /*
         * For analysis and/or transforms, we only identify the target region as being mapped.
         * The lock/unlock protocol for writing will protect the target from being written by
         * more than one source.
         */
        PDC_check_transform(PDC_DATA_MAP, reg2);
        PDC_inc_ref(remote_obj);
        PDC_inc_ref(remote_reg);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct pdc_region_info *
PDCregion_get_info(pdcid_t reg_id)
{
    struct pdc_region_info *ret_value = NULL;
    struct pdc_region_info *info      = NULL;
    struct _pdc_id_info *   region;

    FUNC_ENTER(NULL);

    region = PDC_find_id(reg_id);
    if (region == NULL)
        PGOTO_ERROR(NULL, "cannot locate region");

    info      = (struct pdc_region_info *)(region->obj_ptr);
    ret_value = info;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCbuf_obj_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id)
{
    perr_t                  ret_value = SUCCEED;
    struct _pdc_id_info *   info1;
    struct _pdc_obj_info *  object1;
    struct pdc_region_info *reginfo;
    pdc_var_type_t          data_type;

    FUNC_ENTER(NULL);

    info1 = PDC_find_id(remote_obj_id);
    if (info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object1   = (struct _pdc_obj_info *)(info1->obj_ptr);
    data_type = object1->obj_pt->obj_prop_pub->type;

    info1 = PDC_find_id(remote_reg_id);
    if (info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate region ID");
    reginfo = (struct pdc_region_info *)(info1->obj_ptr);

    ret_value = PDC_Client_buf_unmap(object1->obj_info_pub->meta_id, remote_reg_id, reginfo, data_type);

    if (ret_value == SUCCEED) {
        PDC_dec_ref(remote_obj_id);
        PDC_dec_ref(remote_reg_id);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCreg_obtain_lock(pdcid_t obj_id, pdcid_t reg_id, pdc_access_t access_type, pdc_lock_mode_t lock_mode)
{
    perr_t                  ret_value = SUCCEED;
    struct _pdc_obj_info *  object_info;
    struct pdc_region_info *region_info;
    pdc_var_type_t          data_type;
    pbool_t                 obtained;

    FUNC_ENTER(NULL);

    object_info = PDC_obj_get_info(obj_id);
    data_type   = object_info->obj_pt->obj_prop_pub->type;
    region_info = PDCregion_get_info(reg_id);
    ret_value =
        PDC_Client_region_lock(object_info, region_info, access_type, lock_mode, data_type, &obtained);

    PDC_free_obj_info(object_info);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCreg_release_lock(pdcid_t obj_id, pdcid_t reg_id, pdc_access_t access_type)
{
    perr_t                  ret_value = SUCCEED;
    pbool_t                 released;
    struct _pdc_obj_info *  object_info;
    struct pdc_region_info *region_info;
    pdc_var_type_t          data_type;

    FUNC_ENTER(NULL);

    object_info = PDC_obj_get_info(obj_id);
    data_type   = object_info->obj_pt->obj_prop_pub->type;
    region_info = PDCregion_get_info(reg_id);

    ret_value = PDC_Client_region_release(object_info, region_info, access_type, data_type, &released);

    PDC_free_obj_info(object_info);

    FUNC_LEAVE(ret_value);
}
