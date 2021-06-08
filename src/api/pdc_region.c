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

static perr_t pdc_region_close(struct pdc_region_info *op);

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
