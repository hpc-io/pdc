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

#include "../server/pdc_utlist.h"
#include "pdc_config.h"
#include "pdc_malloc.h"
#include "pdc_id_pkg.h"
#include "pdc_cont.h"
#include "pdc_prop_pkg.h"
#include "pdc_obj_pkg.h"
#include "pdc_obj.h"
#include "pdc_interface.h"
#include "pdc_transforms_pkg.h"
#include "pdc_analysis_pkg.h"
#include "pdc_client_connect.h"
#include <time.h>
#include <stdlib.h>
#include <unistd.h>

static perr_t PDC_obj_close(struct _pdc_obj_info *op);

perr_t
PDC_obj_init()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the object IDs */
    if (PDC_register_type(PDC_OBJ, (PDC_free_t)PDC_obj_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize object interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCobj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id)
{
    uint64_t               meta_id;
    pdcid_t                ret_value = 0;
    struct _pdc_cont_info *cont_info;
    struct _pdc_obj_prop * obj_prop;
    struct _pdc_obj_info * p       = NULL;
    struct _pdc_id_info *  id_info = NULL;
    perr_t                 ret;
    size_t                 i;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct _pdc_obj_info);
    if (!p)
        PGOTO_ERROR(0, "PDC object memory allocation failed");
    p->metadata         = NULL;
    p->region_list_head = NULL;

    if (cont_id == 0) {
        meta_id = 0;
    }
    else {
        id_info   = PDC_find_id(cont_id);
        cont_info = (struct _pdc_cont_info *)(id_info->obj_ptr);

        /* struct _pdc_cont_info field */
        p->cont = PDC_CALLOC(struct _pdc_cont_info);
        if (!p->cont)
            PGOTO_ERROR(0, "PDC object container memory allocation failed");
        memcpy(p->cont, cont_info, sizeof(struct _pdc_cont_info));

        p->cont->cont_info_pub = PDC_CALLOC(struct pdc_cont_info);
        if (!p->cont->cont_info_pub)
            PGOTO_ERROR(0, "PDC object pub container memory allocation failed");
        memcpy(p->cont->cont_info_pub, cont_info->cont_info_pub, sizeof(struct pdc_cont_info));
        if (cont_info->cont_info_pub->name)
            p->cont->cont_info_pub->name = strdup(cont_info->cont_info_pub->name);

        p->cont->cont_pt = PDC_CALLOC(struct _pdc_cont_prop);
        if (!p->cont->cont_pt)
            PGOTO_ERROR(0, "PDC object container property memory allocation failed");
        memcpy(p->cont->cont_pt, cont_info->cont_pt, sizeof(struct _pdc_cont_prop));

        p->cont->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
        if (!p->cont->cont_pt->pdc)
            PGOTO_ERROR(0, "PDC object container property pdc memory allocation failed");
        p->cont->cont_pt->pdc->name     = strdup(cont_info->cont_pt->pdc->name);
        p->cont->cont_pt->pdc->local_id = cont_info->cont_pt->pdc->local_id;
        meta_id                         = p->cont->cont_info_pub->meta_id;
    }

    id_info  = PDC_find_id(obj_prop_id);
    obj_prop = (struct _pdc_obj_prop *)(id_info->obj_ptr);

    /* struct _pdc_obj_prop field */
    p->obj_pt = PDC_CALLOC(struct _pdc_obj_prop);
    if (!p->obj_pt)
        PGOTO_ERROR(0, "PDC object property memory allocation failed");
    memcpy(p->obj_pt, obj_prop, sizeof(struct _pdc_obj_prop));
    if (obj_prop->app_name)
        p->obj_pt->app_name = strdup(obj_prop->app_name);
    p->obj_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (!p->obj_pt->pdc)
        PGOTO_ERROR(0, "cannot allocate ret_value->pdc");
    p->obj_pt->pdc->name     = strdup(obj_prop->pdc->name);
    p->obj_pt->pdc->local_id = obj_prop->pdc->local_id;

    /* struct pdc_obj_prop field */
    p->obj_pt->obj_prop_pub = PDC_CALLOC(struct pdc_obj_prop);
    if (!p->obj_pt->obj_prop_pub)
        PGOTO_ERROR(0, "cannot allocate ret_value->obj_pt->obj_prop_pub");
    p->obj_pt->obj_prop_pub->ndim = obj_prop->obj_prop_pub->ndim;
    p->obj_pt->obj_prop_pub->dims = malloc(obj_prop->obj_prop_pub->ndim * sizeof(uint64_t));
    if (!p->obj_pt->obj_prop_pub->dims)
        PGOTO_ERROR(0, "cannot allocate ret_value->dims");
    for (i = 0; i < obj_prop->obj_prop_pub->ndim; i++)
        p->obj_pt->obj_prop_pub->dims[i] = obj_prop->obj_prop_pub->dims[i];

    p->obj_pt->obj_prop_pub->type = obj_prop->obj_prop_pub->type;
    if (obj_prop->app_name)
        p->obj_pt->app_name = strdup(obj_prop->app_name);
    if (obj_prop->data_loc)
        p->obj_pt->data_loc = strdup(obj_prop->data_loc);
    if (obj_prop->tags)
        p->obj_pt->tags = strdup(obj_prop->tags);

    p->obj_info_pub = PDC_MALLOC(struct pdc_obj_info);
    if (!p->obj_info_pub)
        PGOTO_ERROR(0, "PDC pub object memory allocation failed");
    p->obj_info_pub->name      = strdup(obj_name);
    p->obj_info_pub->server_id = 0;
    p->obj_info_pub->local_id  = PDC_id_register(PDC_OBJ, p);
    ret = PDC_Client_send_name_recv_id(obj_name, meta_id, obj_prop_id, &(p->obj_info_pub->meta_id));
    if (ret == FAIL)
        PGOTO_ERROR(0, "Unable to create object on server!");

    p->obj_info_pub->obj_pt = PDC_CALLOC(struct pdc_obj_prop);
    if (!p->obj_info_pub->obj_pt)
        PGOTO_ERROR(0, "PDC object prop memory allocation failed");
    memcpy(p->obj_info_pub->obj_pt, p->obj_pt->obj_prop_pub, sizeof(struct pdc_obj_prop));
    p->obj_info_pub->obj_pt->ndim = obj_prop->obj_prop_pub->ndim;
    p->obj_info_pub->obj_pt->dims = malloc(obj_prop->obj_prop_pub->ndim * sizeof(uint64_t));
    if (!p->obj_info_pub->obj_pt->dims)
        PGOTO_ERROR(0, "failed to allocate obj pub property memory");
    for (i = 0; i < obj_prop->obj_prop_pub->ndim; i++)
        p->obj_info_pub->obj_pt->dims[i] = obj_prop->obj_prop_pub->dims[i];

    // PDC_Client_attach_metadata_to_local_obj((char *)obj_name, p->meta_id, p->cont->meta_id, p);

    ret_value = p->obj_info_pub->local_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDC_obj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id, _pdc_obj_location_t location)
{
    pdcid_t                ret_value = 0;
    struct _pdc_obj_info * p         = NULL;
    struct _pdc_id_info *  id_info   = NULL;
    struct _pdc_cont_info *cont_info = NULL;
    struct _pdc_obj_prop * obj_prop;
    uint64_t               meta_id;
    size_t                 i;
    perr_t                 ret = SUCCEED;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct _pdc_obj_info);
    if (!p)
        PGOTO_ERROR(0, "PDC object memory allocation failed");
    p->metadata         = NULL;
    p->location         = location;
    p->region_list_head = NULL;

    if (cont_id == 0) {
        meta_id = 0;
    }
    else {
        id_info = PDC_find_id(cont_id);
        /* struct _pdc_cont_info field */
        cont_info = (struct _pdc_cont_info *)(id_info->obj_ptr);
        p->cont   = PDC_CALLOC(struct _pdc_cont_info);
        if (!p->cont)
            PGOTO_ERROR(0, "PDC object container memory allocation failed");
        memcpy(p->cont, cont_info, sizeof(struct _pdc_cont_info));

        /* struct pdc_cont_info field */
        p->cont->cont_info_pub = PDC_CALLOC(struct pdc_cont_info);
        if (!p->cont->cont_info_pub)
            PGOTO_ERROR(0, "PDC object pub container memory allocation failed");
        memcpy(p->cont->cont_info_pub, cont_info->cont_info_pub, sizeof(struct pdc_cont_info));
        if (cont_info->cont_info_pub->name)
            p->cont->cont_info_pub->name = strdup(cont_info->cont_info_pub->name);

        /* struct _pdc_cont_prop field */
        p->cont->cont_pt = PDC_CALLOC(struct _pdc_cont_prop);
        if (!p->cont->cont_pt)
            PGOTO_ERROR(0, "PDC object container property memory allocation failed");
        memcpy(p->cont->cont_pt, cont_info->cont_pt, sizeof(struct _pdc_cont_prop));

        /* struct _pdc_class field */
        p->cont->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
        if (!p->cont->cont_pt->pdc)
            PGOTO_ERROR(0, "PDC object container property pdc memory allocation failed");
        if (cont_info->cont_pt->pdc->name)
            p->cont->cont_pt->pdc->name = strdup(cont_info->cont_pt->pdc->name);
        p->cont->cont_pt->pdc->local_id = cont_info->cont_pt->pdc->local_id;
        meta_id                         = p->cont->cont_info_pub->meta_id;
    }

    id_info  = PDC_find_id(obj_prop_id);
    obj_prop = (struct _pdc_obj_prop *)(id_info->obj_ptr);

    /* struct _pdc_obj_prop field */
    p->obj_pt = PDC_CALLOC(struct _pdc_obj_prop);
    if (!p->obj_pt)
        PGOTO_ERROR(0, "PDC object property memory allocation failed");
    memcpy(p->obj_pt, obj_prop, sizeof(struct _pdc_obj_prop));
    if (obj_prop->app_name)
        p->obj_pt->app_name = strdup(obj_prop->app_name);
    if (obj_prop->data_loc)
        p->obj_pt->data_loc = strdup(obj_prop->data_loc);
    if (obj_prop->tags)
        p->obj_pt->tags = strdup(obj_prop->tags);
    p->obj_pt->locus = PDC_get_execution_locus();

    p->obj_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (!p->obj_pt->pdc)
        PGOTO_ERROR(0, "cannot allocate ret_value->pdc");
    if (obj_prop->pdc->name)
        p->obj_pt->pdc->name = strdup(obj_prop->pdc->name);
    p->obj_pt->pdc->local_id = obj_prop->pdc->local_id;

    /* struct pdc_obj_prop field */
    p->obj_pt->obj_prop_pub = PDC_CALLOC(struct pdc_obj_prop);
    if (!p->obj_pt->obj_prop_pub)
        PGOTO_ERROR(0, "cannot allocate ret_value->obj_pt->obj_prop_pub");
    p->obj_pt->obj_prop_pub->ndim = obj_prop->obj_prop_pub->ndim;
    p->obj_pt->obj_prop_pub->dims = malloc(obj_prop->obj_prop_pub->ndim * sizeof(uint64_t));
    if (!p->obj_pt->obj_prop_pub->dims)
        PGOTO_ERROR(0, "cannot allocate ret_value->dims");
    for (i = 0; i < obj_prop->obj_prop_pub->ndim; i++)
        p->obj_pt->obj_prop_pub->dims[i] = obj_prop->obj_prop_pub->dims[i];
    p->obj_pt->obj_prop_pub->type = obj_prop->obj_prop_pub->type;
    if (obj_prop->app_name)
        p->obj_pt->app_name = strdup(obj_prop->app_name);
    if (obj_prop->data_loc)
        p->obj_pt->data_loc = strdup(obj_prop->data_loc);
    if (obj_prop->tags)
        p->obj_pt->tags = strdup(obj_prop->tags);

    /* struct pdc_obj_info field */
    p->obj_info_pub = PDC_MALLOC(struct pdc_obj_info);
    if (!p->obj_info_pub)
        PGOTO_ERROR(0, "PDC pub object memory allocation failed");
    p->obj_info_pub->name      = strdup(obj_name);
    p->obj_info_pub->local_id  = PDC_id_register(PDC_OBJ, p);
    p->obj_info_pub->meta_id   = 0;
    p->obj_info_pub->server_id = 0;
    if (location == PDC_OBJ_GLOBAL) {
        ret = PDC_Client_send_name_recv_id(obj_name, p->cont->cont_info_pub->meta_id, obj_prop_id,
                                           &(p->obj_info_pub->meta_id));
        if (ret == FAIL)
            PGOTO_ERROR(0, "Unable to create object on server!");
    }

    PDC_Client_attach_metadata_to_local_obj(obj_name, p->obj_info_pub->meta_id, meta_id, p);

    p->obj_info_pub->obj_pt = PDC_CALLOC(struct pdc_obj_prop);
    if (!p->obj_info_pub->obj_pt)
        PGOTO_ERROR(0, "PDC object prop memory allocation failed");
    memcpy(p->obj_info_pub->obj_pt, p->obj_pt->obj_prop_pub, sizeof(struct pdc_obj_prop));
    p->obj_info_pub->obj_pt->ndim = obj_prop->obj_prop_pub->ndim;
    p->obj_info_pub->obj_pt->dims = malloc(obj_prop->obj_prop_pub->ndim * sizeof(uint64_t));
    if (!p->obj_info_pub->obj_pt->dims)
        PGOTO_ERROR(0, "failed to allocate obj pub property memory");
    for (i = 0; i < obj_prop->obj_prop_pub->ndim; i++)
        p->obj_info_pub->obj_pt->dims[i] = obj_prop->obj_prop_pub->dims[i];

    ret_value = p->obj_info_pub->local_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_obj_list_null()
{
    perr_t ret_value = SUCCEED;
    int    nelemts;

    FUNC_ENTER(NULL);

    // list is not empty
    nelemts = PDC_id_list_null(PDC_OBJ);
    if (nelemts > 0) {
        if (PDC_id_list_clear(PDC_OBJ) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_obj_close(struct _pdc_obj_info *op)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    free((void *)(op->obj_info_pub->name));
    free(op->cont->cont_info_pub->name);
    op->cont->cont_info_pub = PDC_FREE(struct pdc_cont_info, op->cont->cont_info_pub);
    free(op->cont->cont_pt->pdc->name);
    op->cont->cont_pt->pdc = PDC_FREE(struct _pdc_class, op->cont->cont_pt->pdc);
    op->cont->cont_pt      = PDC_FREE(struct _pdc_cont_prop, op->cont->cont_pt);
    op->cont               = PDC_FREE(struct _pdc_cont_info, op->cont);

    free(op->obj_pt->pdc->name);
    op->obj_pt->pdc = PDC_FREE(struct _pdc_class, op->obj_pt->pdc);
    free(op->obj_pt->obj_prop_pub->dims);
    op->obj_pt->obj_prop_pub = PDC_FREE(struct pdc_obj_prop, op->obj_pt->obj_prop_pub);
    free(op->obj_pt->app_name);
    free(op->obj_pt->data_loc);
    free(op->obj_pt->tags);
    op->obj_pt = PDC_FREE(struct _pdc_obj_prop, op->obj_pt);
    if (op->metadata != NULL)
        free(op->metadata);

    free(op->obj_info_pub->obj_pt->dims);
    op->obj_info_pub->obj_pt = PDC_FREE(struct pdc_obj_prop, op->obj_info_pub->obj_pt);
    op->obj_info_pub         = PDC_FREE(struct pdc_obj_info, op->obj_info_pub);

    op = PDC_FREE(struct _pdc_obj_info, op);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_close(pdcid_t obj_id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(obj_id) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_obj_end()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (PDC_destroy_type(PDC_OBJ) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy object interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static pdcid_t
PDCobj_open_common(const char *obj_name, pdcid_t pdc, int is_col)
{
    pdcid_t               ret_value = 0;
    perr_t                ret       = SUCCEED;
    struct _pdc_obj_info *p         = NULL;
    pdc_metadata_t *      out       = NULL;
    pdcid_t               obj_prop;
    size_t                i;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct _pdc_obj_info);
    if (!p)
        PGOTO_ERROR(0, "PDC object memory allocation failed");
    p->cont = PDC_CALLOC(struct _pdc_cont_info);
    if (!p->cont)
        PGOTO_ERROR(0, "PDC object container memory allocation failed");
    p->cont->cont_info_pub = PDC_CALLOC(struct pdc_cont_info);
    if (!p->cont->cont_info_pub)
        PGOTO_ERROR(0, "PDC object pub container memory allocation failed");
    p->cont->cont_pt = PDC_CALLOC(struct _pdc_cont_prop);
    if (!p->cont->cont_pt)
        PGOTO_ERROR(0, "PDC object container property memory allocation failed");
    p->cont->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (!p->cont->cont_pt->pdc)
        PGOTO_ERROR(0, "PDC object container property pdc memory allocation failed");
    p->obj_pt = PDC_CALLOC(struct _pdc_obj_prop);
    if (!p->obj_pt)
        PGOTO_ERROR(0, "PDC object property memory allocation failed");
    p->obj_pt->obj_prop_pub = PDC_CALLOC(struct pdc_obj_prop);
    if (!p->obj_pt->obj_prop_pub)
        PGOTO_ERROR(0, "PDC object property memory allocation failed");
    p->obj_info_pub = PDC_MALLOC(struct pdc_obj_info);
    if (!p->obj_info_pub)
        PGOTO_ERROR(0, "PDC pub object memory allocation failed");
    p->obj_info_pub->obj_pt = PDC_CALLOC(struct pdc_obj_prop);
    if (!p->obj_info_pub->obj_pt)
        PGOTO_ERROR(0, "PDC object prop memory allocation failed");
    p->obj_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (!p->obj_pt->pdc)
        PGOTO_ERROR(0, "cannot allocate ret_value->pdc");

    // contact metadata server
    if (is_col == 0)
        ret = PDC_Client_query_metadata_name_timestep(obj_name, 0, &out);
    else
        ret = PDC_Client_query_metadata_name_timestep_agg(obj_name, 0, &out);

    if (ret == FAIL)
        PGOTO_ERROR(0, "query object failed");

    obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_dims(obj_prop, out->ndim, out->dims);
    PDCprop_set_obj_type(obj_prop, out->data_type);

    p->cont->cont_info_pub->meta_id = out->cont_id;
    p->obj_pt->obj_prop_pub->ndim   = out->ndim;
    p->obj_pt->obj_prop_pub->dims   = malloc(out->ndim * sizeof(uint64_t));
    if (!p->obj_pt->obj_prop_pub->dims)
        PGOTO_ERROR(0, "cannot allocate ret_value->obj_prop_pub->dims");
    for (i = 0; i < out->ndim; i++)
        p->obj_pt->obj_prop_pub->dims[i] = out->dims[i];
    /* 'app_name' is a char array */
    if (strlen(out->app_name) > 0)
        p->obj_pt->app_name = strdup(out->app_name);
    p->obj_pt->obj_prop_pub->type = out->data_type;
    p->obj_pt->time_step          = out->time_step;
    p->obj_pt->user_id            = out->user_id;

    if (out->transform_state > 0) {
        p->obj_pt->locus                        = SERVER_MEMORY;
        p->obj_pt->data_state                   = out->transform_state;
        p->obj_pt->transform_prop.storage_order = out->current_state.storage_order;
        p->obj_pt->transform_prop.dtype         = out->current_state.dtype;
        p->obj_pt->transform_prop.ndim          = out->current_state.ndim;
        for (i = 0; i < out->current_state.ndim; i++)
            p->obj_pt->transform_prop.dims[i] = out->current_state.dims[i];
    }
    p->metadata = out;

    /* struct pdc_obj_info field */
    /* 'obj_name' is a char array */
    if (strlen(out->obj_name) > 0)
        p->obj_info_pub->name = strdup(out->obj_name);
    p->obj_info_pub->meta_id  = out->obj_id;
    p->obj_info_pub->local_id = PDC_id_register(PDC_OBJ, p);

    memcpy(p->obj_info_pub->obj_pt, p->obj_pt->obj_prop_pub, sizeof(struct pdc_obj_prop));
    p->obj_info_pub->obj_pt->dims = malloc(p->obj_pt->obj_prop_pub->ndim * sizeof(uint64_t));
    if (!p->obj_info_pub->obj_pt->dims)
        PGOTO_ERROR(0, "failed to allocate obj pub property memory");
    for (i = 0; i < p->obj_pt->obj_prop_pub->ndim; i++)
        p->obj_info_pub->obj_pt->dims[i] = p->obj_pt->obj_prop_pub->dims[i];

    ret_value = p->obj_info_pub->local_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCobj_open(const char *obj_name, pdcid_t pdc)
{
    pdcid_t ret_value;
    FUNC_ENTER(NULL);

    ret_value = PDCobj_open_common(obj_name, pdc, 0);

    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCobj_open_col(const char *obj_name, pdcid_t pdc)
{
    pdcid_t ret_value;
    FUNC_ENTER(NULL);

    ret_value = PDCobj_open_common(obj_name, pdc, 1);

    FUNC_LEAVE(ret_value);
}

obj_handle *
PDCobj_iter_start(pdcid_t cont_id)
{
    obj_handle *        ret_value = NULL;
    obj_handle *        objhl     = NULL;
    struct PDC_id_type *type_ptr;

    FUNC_ENTER(NULL);

    type_ptr = (pdc_id_list_g->PDC_id_type_list_g)[PDC_OBJ];
    if (type_ptr == NULL)
        PGOTO_ERROR(NULL, "object list is empty");
    objhl = (&type_ptr->ids)->head;

    while (objhl != NULL &&
           ((struct _pdc_obj_info *)(objhl->obj_ptr))->cont->cont_info_pub->local_id != cont_id) {
        objhl = PDC_LIST_NEXT(objhl, entry);
    }

    ret_value = objhl;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pbool_t
PDCobj_iter_null(obj_handle *ohandle)
{
    pbool_t ret_value = FALSE;

    FUNC_ENTER(NULL);

    if (ohandle == NULL)
        ret_value = TRUE;

    FUNC_LEAVE(ret_value);
}

obj_handle *
PDCobj_iter_next(obj_handle *ohandle, pdcid_t cont_id)
{
    obj_handle *ret_value = NULL;
    obj_handle *next      = NULL;

    FUNC_ENTER(NULL);

    if (ohandle == NULL)
        PGOTO_ERROR(NULL, "no next object");
    next = PDC_LIST_NEXT(ohandle, entry);

    while (next != NULL &&
           ((struct _pdc_obj_info *)(next->obj_ptr))->cont->cont_info_pub->local_id != cont_id) {
        next = PDC_LIST_NEXT(ohandle, entry);
    }

    ret_value = next;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct pdc_obj_info *
PDCobj_iter_get_info(obj_handle *ohandle)
{
    struct pdc_obj_info * ret_value = NULL;
    struct _pdc_obj_info *info      = NULL;
    unsigned              i;

    FUNC_ENTER(NULL);

    info = (struct _pdc_obj_info *)(ohandle->obj_ptr);
    if (info == NULL)
        PGOTO_ERROR(NULL, "PDC container info memory allocation failed");

    ret_value = PDC_CALLOC(struct pdc_obj_info);
    if (!ret_value)
        PGOTO_ERROR(NULL, "failed to allocate memory");
    memcpy(ret_value, info->obj_info_pub, sizeof(struct pdc_obj_info));

    ret_value->obj_pt = PDC_CALLOC(struct pdc_obj_prop);
    if (!ret_value->obj_pt)
        PGOTO_ERROR(NULL, "failed to allocate memory");
    memcpy(ret_value->obj_pt, info->obj_info_pub->obj_pt, sizeof(struct pdc_obj_prop));
    ret_value->obj_pt->dims = malloc(ret_value->obj_pt->ndim * sizeof(uint64_t));
    if (!ret_value->obj_pt->dims)
        PGOTO_ERROR(0, "failed to allocate obj pub property memory");
    for (i = 0; i < ret_value->obj_pt->ndim; i++)
        ret_value->obj_pt->dims[i] = info->obj_info_pub->obj_pt->dims[i];

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id)
{
    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct _pdc_obj_prop *)(info->obj_ptr))->user_id = user_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name)
{
    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct _pdc_obj_prop *)(info->obj_ptr))->app_name = strdup(app_name);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step)
{
    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct _pdc_obj_prop *)(info->obj_ptr))->time_step = time_step;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_data_loc(pdcid_t obj_prop, char *loc)
{
    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct _pdc_obj_prop *)(info->obj_ptr))->data_loc = strdup(loc);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags)
{
    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct _pdc_obj_prop *)(info->obj_ptr))->tags = strdup(tags);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * info;
    struct _pdc_obj_prop *prop;
    int                   i = 0;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop                     = (struct _pdc_obj_prop *)(info->obj_ptr);
    prop->obj_prop_pub->ndim = ndim;
    prop->obj_prop_pub->dims = (uint64_t *)malloc(ndim * sizeof(uint64_t));

    for (i = 0; i < ndim; i++)
        (prop->obj_prop_pub->dims)[i] = dims[i];

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_type(pdcid_t obj_prop, pdc_var_type_t type)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * info;
    struct _pdc_obj_prop *prop;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop                     = (struct _pdc_obj_prop *)(info->obj_ptr);
    prop->obj_prop_pub->type = type;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf)
{
    perr_t                ret_value = SUCCEED;
    struct _pdc_id_info * info;
    struct _pdc_obj_prop *prop;

    FUNC_ENTER(NULL);

    info = PDC_find_id(obj_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop      = (struct _pdc_obj_prop *)(info->obj_ptr);
    prop->buf = buf;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void **
PDCobj_buf_retrieve(pdcid_t obj_id)
{
    void **               ret_value = NULL;
    struct _pdc_id_info * info;
    struct _pdc_obj_info *object;
    void **               buffer;

    FUNC_ENTER(NULL);
    info = PDC_find_id(obj_id);
    if (info == NULL)
        PGOTO_ERROR(NULL, "cannot locate object ID");
    object    = (struct _pdc_obj_info *)(info->obj_ptr);
    buffer    = &(object->obj_pt->buf);
    ret_value = buffer;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct _pdc_obj_info *
PDC_obj_get_info(pdcid_t obj_id)
{
    struct _pdc_obj_info *ret_value = NULL;
    struct _pdc_obj_info *info      = NULL;
    struct _pdc_id_info * obj;
    size_t                i;

    FUNC_ENTER(NULL);

    obj = PDC_find_id(obj_id);
    if (obj == NULL)
        PGOTO_ERROR(NULL, "cannot locate object");

    info      = (struct _pdc_obj_info *)(obj->obj_ptr);
    ret_value = PDC_CALLOC(struct _pdc_obj_info);
    if (ret_value)
        memcpy(ret_value, info, sizeof(struct _pdc_obj_info));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value");

    /* struct pdc_obj_info field */
    ret_value->obj_info_pub = PDC_CALLOC(struct pdc_obj_info);
    if (ret_value->obj_info_pub)
        memcpy(ret_value->obj_info_pub, info->obj_info_pub, sizeof(struct pdc_obj_info));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value");
    if (info->obj_info_pub->name)
        ret_value->obj_info_pub->name = strdup(info->obj_info_pub->name);
    else
        ret_value->obj_info_pub->name = NULL;

    ret_value->obj_info_pub->obj_pt = PDC_CALLOC(struct pdc_obj_prop);
    if (!ret_value->obj_info_pub->obj_pt)
        PGOTO_ERROR(NULL, "failed to allocate memory");
    memcpy(ret_value->obj_info_pub->obj_pt, info->obj_info_pub->obj_pt, sizeof(struct pdc_obj_prop));
    ret_value->obj_info_pub->obj_pt->dims = malloc(ret_value->obj_info_pub->obj_pt->ndim * sizeof(uint64_t));
    if (!ret_value->obj_info_pub->obj_pt->dims)
        PGOTO_ERROR(0, "failed to allocate obj pub property memory");
    for (i = 0; i < ret_value->obj_info_pub->obj_pt->ndim; i++)
        ret_value->obj_info_pub->obj_pt->dims[i] = info->obj_info_pub->obj_pt->dims[i];

    // fill in by query function
    ret_value->metadata = NULL;

    // fill in struct _pdc_cont_info field in ret_value->cont
    ret_value->cont = PDC_CALLOC(struct _pdc_cont_info);
    if (ret_value->cont)
        memcpy(ret_value->cont, info->cont, sizeof(struct _pdc_cont_info));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont");

    ret_value->cont->cont_info_pub = PDC_CALLOC(struct pdc_cont_info);
    if (ret_value->cont->cont_info_pub)
        memcpy(ret_value->cont->cont_info_pub, info->cont->cont_info_pub, sizeof(struct pdc_cont_info));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont->cont_info_pub");
    if (info->cont->cont_info_pub->name)
        ret_value->cont->cont_info_pub->name = strdup(info->cont->cont_info_pub->name);
    else
        ret_value->cont->cont_info_pub->name = NULL;

    ret_value->cont->cont_pt = PDC_CALLOC(struct _pdc_cont_prop);
    if (ret_value->cont->cont_pt)
        memcpy(ret_value->cont->cont_pt, info->cont->cont_pt, sizeof(struct _pdc_cont_prop));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont->cont_pt");
    ret_value->cont->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (ret_value->cont->cont_pt->pdc) {
        ret_value->cont->cont_pt->pdc->local_id = info->cont->cont_pt->pdc->local_id;
        if (info->cont->cont_pt->pdc->name)
            ret_value->cont->cont_pt->pdc->name = strdup(info->cont->cont_pt->pdc->name);
        else
            ret_value->cont->cont_pt->pdc->name = NULL;
    }
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont->cont_pt->pdc");

    // fill in struct _pdc_obj_prop field in ret_value->obj_pt
    ret_value->obj_pt = PDC_CALLOC(struct _pdc_obj_prop);
    if (ret_value->obj_pt)
        memcpy(ret_value->obj_pt, info->obj_pt, sizeof(struct _pdc_obj_prop));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->obj_pt");
    ret_value->obj_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (ret_value->obj_pt->pdc) {
        ret_value->obj_pt->pdc->local_id = info->obj_pt->pdc->local_id;
        if (info->obj_pt->pdc->name)
            ret_value->obj_pt->pdc->name = strdup(info->obj_pt->pdc->name);
        else
            ret_value->obj_pt->pdc->name = NULL;
    }
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->obj_pt->pdc");

    ret_value->obj_pt->obj_prop_pub = PDC_CALLOC(struct pdc_obj_prop);
    if (ret_value->obj_pt->obj_prop_pub)
        memcpy(ret_value->obj_pt->obj_prop_pub, info->obj_pt->obj_prop_pub, sizeof(struct pdc_obj_prop));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->obj_pt");
    ret_value->obj_pt->obj_prop_pub->dims = malloc(ret_value->obj_pt->obj_prop_pub->ndim * sizeof(uint64_t));
    if (ret_value->obj_pt->obj_prop_pub->dims) {
        for (i = 0; i < ret_value->obj_pt->obj_prop_pub->ndim; i++) {
            ret_value->obj_pt->obj_prop_pub->dims[i] = info->obj_pt->obj_prop_pub->dims[i];
        }
    }
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->obj_pt->dims");
    if (info->obj_pt->app_name)
        ret_value->obj_pt->app_name = strdup(info->obj_pt->app_name);
    else
        ret_value->obj_pt->app_name = NULL;
    if (info->obj_pt->data_loc)
        ret_value->obj_pt->data_loc = strdup(info->obj_pt->data_loc);
    else
        ret_value->obj_pt->data_loc = NULL;
    if (info->obj_pt->tags)
        ret_value->obj_pt->tags = strdup(info->obj_pt->tags);
    else
        ret_value->obj_pt->tags = NULL;

    ret_value->region_list_head = NULL;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_free_obj_info(struct _pdc_obj_info *obj)
{
    perr_t ret_value = TRUE;

    FUNC_ENTER(NULL);

    assert(obj);

    if (obj->obj_info_pub->name != NULL)
        free(obj->obj_info_pub->name);
    obj->obj_info_pub = PDC_FREE(struct pdc_obj_info, obj->obj_info_pub);

    if (obj->metadata != NULL)
        free(obj->metadata);

    if (obj->cont != NULL) {
        if (obj->cont->cont_info_pub->name != NULL)
            free(obj->cont->cont_info_pub->name);
        obj->cont->cont_info_pub = PDC_FREE(struct pdc_cont_info, obj->cont->cont_info_pub);
        if (obj->cont->cont_pt->pdc->name != NULL)
            free(obj->cont->cont_pt->pdc->name);
        obj->cont->cont_pt->pdc = PDC_FREE(struct _pdc_class, obj->cont->cont_pt->pdc);
        obj->cont->cont_pt      = PDC_FREE(struct _pdc_cont_prop, obj->cont->cont_pt);
        obj->cont               = PDC_FREE(struct _pdc_cont_info, obj->cont);
    }

    if (obj->obj_pt != NULL) {
        if (obj->obj_pt->pdc != NULL) {
            if (obj->obj_pt->pdc->name != NULL)
                free(obj->obj_pt->pdc->name);
            obj->obj_pt->pdc = PDC_FREE(struct _pdc_class, obj->obj_pt->pdc);
        }
        if (obj->obj_pt->obj_prop_pub->dims != NULL)
            free(obj->obj_pt->obj_prop_pub->dims);
        obj->obj_pt->obj_prop_pub = PDC_FREE(struct pdc_obj_prop, obj->obj_pt->obj_prop_pub);
        if (obj->obj_pt->app_name != NULL)
            free(obj->obj_pt->app_name);
        if (obj->obj_pt->data_loc != NULL)
            free(obj->obj_pt->data_loc);
        if (obj->obj_pt->tags != NULL)
            free(obj->obj_pt->tags);
        obj->obj_pt = PDC_FREE(struct _pdc_obj_prop, obj->obj_pt);
    }

    if (obj->region_list_head != NULL)
        free(obj->region_list_head);

    obj = PDC_FREE(struct _pdc_obj_info, obj);

    FUNC_LEAVE(ret_value);
}

struct pdc_obj_info *
PDCobj_get_info(pdcid_t obj_id)
{
    struct pdc_obj_info * ret_value = NULL;
    struct _pdc_obj_info *tmp       = NULL;
    /* pdcid_t obj_id; */

    FUNC_ENTER(NULL);

    /* obj_id = PDC_find_byname(PDC_OBJ, obj_name); */

    tmp = PDC_obj_get_info(obj_id);

    ret_value = PDC_CALLOC(struct pdc_obj_info);
    if (!ret_value)
        PGOTO_ERROR(NULL, "failed to allocate memory");

    ret_value = tmp->obj_info_pub;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCobj_del(pdcid_t obj_id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = PDC_Client_del_metadata(obj_id, 0);

    // done:
    FUNC_LEAVE(ret_value);
}
