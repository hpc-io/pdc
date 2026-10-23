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
#include "pdc_client_connect.h"
#include <mpi.h>

static perr_t pdc_region_close(struct pdc_region_info *op);
static perr_t pdc_transfer_request_close();

perr_t
PDC_region_init()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    /* Initialize the atom group for the region IDs */
    if (PDC_register_type(PDC_REGION, (PDC_free_t)pdc_region_close) < 0)
        PGOTO_ERROR(FAIL, "Unable to initialize region interface");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_transfer_request_init()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    /* Initialize the atom group for the region IDs */
    if (PDC_register_type(PDC_TRANSFER_REQUEST, (PDC_free_t)pdc_transfer_request_close) < 0)
        PGOTO_ERROR(FAIL, "Unable to initialize region interface");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_list_null()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    nelemts;

    // list is not empty
    nelemts = PDC_id_list_null(PDC_REGION);
    if (nelemts > 0) {
        if (PDC_id_list_clear(PDC_REGION) < 0)
            PGOTO_ERROR(FAIL, "Failed to clear object list");
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
pdc_region_close(struct pdc_region_info *op)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    op->size   = (uint64_t *)PDC_free(op->size);
    op->offset = (uint64_t *)PDC_free(op->offset);

    if (op->obj != NULL)
        op->obj = (struct _pdc_obj_info *)(intptr_t)PDC_free(op->obj);
    op = (struct pdc_region_info *)(intptr_t)PDC_free(op);

    FUNC_LEAVE(ret_value);
}

perr_t
pdc_transfer_request_close()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    FUNC_LEAVE(ret_value);
}

perr_t
PDCregion_close(pdcid_t region_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(region_id) < 0)
        PGOTO_ERROR(FAIL, "Object: problem of freeing id");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_region_end()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    if (PDC_destroy_type(PDC_REGION) < 0)
        PGOTO_ERROR(FAIL, "Failed to destroy region interface");

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCregion_create(psize_t ndims, uint64_t *offset, uint64_t *size)
{
    FUNC_ENTER(NULL);

    pdcid_t                 ret_value = 0;
    struct pdc_region_info *p         = NULL;
    pdcid_t                 new_id;
    size_t                  i = 0;

    p = (struct pdc_region_info *)PDC_malloc(sizeof(struct pdc_region_info));
    if (!p)
        PGOTO_ERROR(ret_value, "PDC region memory allocation failed");
    p->ndim     = ndims;
    p->obj      = NULL;
    p->offset   = (uint64_t *)PDC_malloc(ndims * sizeof(uint64_t));
    p->size     = (uint64_t *)PDC_malloc(ndims * sizeof(uint64_t));
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
    FUNC_LEAVE(ret_value);
}

perr_t
PDCbuf_obj_map(void *buf, pdc_var_type_t local_type, pdcid_t local_reg, pdcid_t remote_obj,
               pdcid_t remote_reg)
{
    FUNC_ENTER(NULL);

    pdcid_t               ret_value = SUCCEED;
    size_t                i;
    struct _pdc_id_info * objinfo2;
    struct _pdc_obj_info *obj2;
    pdcid_t               remote_meta_id;

    pdc_var_type_t          remote_type;
    struct _pdc_id_info *   reginfo1, *reginfo2;
    struct pdc_region_info *reg1, *reg2;

    if ((reginfo1 = PDC_find_id(local_reg)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", local_reg);
    reg1 = (struct pdc_region_info *)(reginfo1->obj_ptr);

    if ((objinfo2 = PDC_find_id(remote_obj)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", remote_obj);
    obj2           = (struct _pdc_obj_info *)(objinfo2->obj_ptr);
    remote_meta_id = obj2->obj_info_pub->meta_id;
    remote_type    = obj2->obj_pt->obj_prop_pub->type;

    if ((reginfo2 = PDC_find_id(remote_reg)) == NULL)
        PGOTO_ERROR(0, "Failed to find PDC ID: %d", remote_reg);
    reg2 = (struct pdc_region_info *)(reginfo2->obj_ptr);
    if (obj2->obj_pt->obj_prop_pub->ndim != reg2->ndim)
        PGOTO_ERROR(FAIL, "Remote object dimension and region dimension does not match");
    for (i = 0; i < reg2->ndim; i++)
        if ((obj2->obj_pt->obj_prop_pub->dims)[i] < (reg2->size)[i])
            PGOTO_ERROR(FAIL, "Remote object region size error");

    ret_value = PDC_Client_buf_map(local_reg, remote_meta_id, reg1->ndim, reg1->size, reg1->offset,
                                   local_type, buf, remote_type, reg1, reg2, obj2);

    if (ret_value == SUCCEED) {
        PDC_inc_ref(remote_obj);
        PDC_inc_ref(remote_reg);
    }

done:
    FUNC_LEAVE(ret_value);
}

struct pdc_region_info *
PDCregion_get_info(pdcid_t reg_id)
{
    FUNC_ENTER(NULL);

    struct pdc_region_info *ret_value = NULL;
    struct pdc_region_info *info      = NULL;
    struct _pdc_id_info *   region;

    if ((region = PDC_find_id(reg_id)) == NULL)
        PGOTO_ERROR(NULL, "Failed to find PDC ID: %d", reg_id);

    info      = (struct pdc_region_info *)(region->obj_ptr);
    ret_value = info;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCbuf_obj_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id)
{
    FUNC_ENTER(NULL);

    perr_t                  ret_value = SUCCEED;
    struct _pdc_id_info *   info1;
    struct _pdc_obj_info *  object1;
    struct pdc_region_info *reginfo;
    pdc_var_type_t          data_type;

    if ((info1 = PDC_find_id(remote_obj_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", remote_obj_id);
    object1   = (struct _pdc_obj_info *)(info1->obj_ptr);
    data_type = object1->obj_pt->obj_prop_pub->type;

    if ((info1 = PDC_find_id(remote_obj_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", remote_obj_id);
    reginfo = (struct pdc_region_info *)(info1->obj_ptr);

    ret_value =
        PDC_Client_buf_unmap(object1->obj_info_pub->meta_id, remote_reg_id, reginfo, data_type, object1);

    if (ret_value == SUCCEED) {
        PDC_dec_ref(remote_obj_id);
        PDC_dec_ref(remote_reg_id);
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCreg_obtain_lock(pdcid_t obj_id, pdcid_t reg_id, pdc_access_t access_type, pdc_lock_mode_t lock_mode)
{
    FUNC_ENTER(NULL);

    perr_t                  ret_value = SUCCEED;
    struct _pdc_obj_info *  object_info;
    struct pdc_region_info *region_info;
    pdc_var_type_t          data_type;
    pbool_t                 obtained;
    struct _pdc_id_info *   info1;

    if ((info1 = PDC_find_id(obj_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", obj_id);
    object_info = (struct _pdc_obj_info *)(info1->obj_ptr);
    // object_info = PDC_obj_get_info(obj_id);
    data_type   = object_info->obj_pt->obj_prop_pub->type;
    region_info = PDCregion_get_info(reg_id);
    ret_value   = PDC_Client_region_lock(object_info->obj_info_pub->meta_id, object_info, region_info,
                                       access_type, lock_mode, data_type, &obtained);

    // PDC_free_obj_info(object_info);
done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCreg_release_lock(pdcid_t obj_id, pdcid_t reg_id, pdc_access_t access_type)
{
    FUNC_ENTER(NULL);

    perr_t                  ret_value = SUCCEED;
    pbool_t                 released;
    struct _pdc_obj_info *  object_info;
    struct pdc_region_info *region_info;
    pdc_var_type_t          data_type;
    struct _pdc_id_info *   info1;

    if ((info1 = PDC_find_id(obj_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", obj_id);
    object_info = (struct _pdc_obj_info *)(info1->obj_ptr);
    // object_info = PDC_obj_get_info(obj_id);
    data_type   = object_info->obj_pt->obj_prop_pub->type;
    region_info = PDCregion_get_info(reg_id);

    ret_value = PDC_Client_region_release(object_info->obj_info_pub->meta_id, object_info, region_info,
                                          access_type, data_type, &released);

    // PDC_free_obj_info(object_info);
done:
    FUNC_LEAVE(ret_value);
}
