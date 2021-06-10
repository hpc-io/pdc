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

#include "pdc_malloc.h"
#include "pdc_id_pkg.h"
#include "pdc_prop.h"
#include "pdc_prop_pkg.h"
#include "pdc_interface.h"
#include <string.h>

static perr_t pdc_prop_cont_close(struct _pdc_cont_prop *cp);

static perr_t pdc_prop_obj_close(struct _pdc_obj_prop *cp);

perr_t
PDC_prop_init()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the container property IDs */
    if (PDC_register_type(PDC_CONT_PROP, (PDC_free_t)pdc_prop_cont_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize container property interface");

    /* Initialize the atom group for the object property IDs */
    if (PDC_register_type(PDC_OBJ_PROP, (PDC_free_t)pdc_prop_obj_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize object property interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCprop_create(pdc_prop_type_t type, pdcid_t pdcid)
{
    pdcid_t                ret_value = 0;
    struct _pdc_cont_prop *p         = NULL;
    struct _pdc_obj_prop * q         = NULL;
    struct _pdc_id_info *  id_info   = NULL;
    struct _pdc_class *    pdc_class;
    pdcid_t                new_id_c;
    pdcid_t                new_id_o;

    FUNC_ENTER(NULL);

    if (type == PDC_CONT_CREATE) {
        p = PDC_MALLOC(struct _pdc_cont_prop);
        if (!p)
            PGOTO_ERROR(0, "PDC container property memory allocation failed");
        p->cont_life    = PDC_PERSIST;
        new_id_c        = PDC_id_register(PDC_CONT_PROP, p);
        p->cont_prop_id = new_id_c;
        id_info         = PDC_find_id(pdcid);
        pdc_class       = (struct _pdc_class *)(id_info->obj_ptr);
        p->pdc          = PDC_CALLOC(struct _pdc_class);
        if (p->pdc == NULL)
            PGOTO_ERROR(0, "PDC class allocation failed");
        if (pdc_class->name)
            p->pdc->name = strdup(pdc_class->name);
        p->pdc->local_id = pdc_class->local_id;

        ret_value = new_id_c;
    }
    if (type == PDC_OBJ_CREATE) {
        q = PDC_MALLOC(struct _pdc_obj_prop);
        if (!q)
            PGOTO_ERROR(0, "PDC object property memory allocation failed");
        q->obj_prop_pub = PDC_MALLOC(struct pdc_obj_prop);
        if (!q->obj_prop_pub)
            PGOTO_ERROR(0, "PDC object pub property memory allocation failed");
        q->obj_prop_pub->ndim        = 0;
        q->obj_prop_pub->dims        = NULL;
        q->obj_prop_pub->type        = PDC_UNKNOWN;
        q->data_loc                  = NULL;
        q->app_name                  = NULL;
        q->time_step                 = 0;
        q->tags                      = NULL;
        q->buf                       = NULL;
        new_id_o                     = PDC_id_register(PDC_OBJ_PROP, q);
        q->obj_prop_pub->obj_prop_id = new_id_o;
        id_info                      = PDC_find_id(pdcid);
        pdc_class                    = (struct _pdc_class *)(id_info->obj_ptr);
        q->pdc                       = PDC_CALLOC(struct _pdc_class);
        if (q->pdc == NULL)
            PGOTO_ERROR(0, "PDC class allocation failed");
        if (pdc_class->name)
            q->pdc->name = strdup(pdc_class->name);
        q->pdc->local_id = pdc_class->local_id;
        q->type_extent   = 0;
        q->data_state    = 0;
        q->locus         = CLIENT_MEMORY;
        memset(&q->transform_prop, 0, sizeof(struct _pdc_transform_state));

        ret_value = new_id_o;
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCprop_obj_dup(pdcid_t prop_id)
{
    pdcid_t               ret_value = 0;
    struct _pdc_obj_prop *q         = NULL;
    struct _pdc_obj_prop *info      = NULL;
    struct _pdc_id_info * prop      = NULL;
    pdcid_t               new_id;
    size_t                i;

    FUNC_ENTER(NULL);

    prop = PDC_find_id(prop_id);
    if (prop == NULL)
        PGOTO_ERROR(0, "cannot locate object property");
    info = (struct _pdc_obj_prop *)(prop->obj_ptr);

    q = PDC_CALLOC(struct _pdc_obj_prop);
    if (!q)
        PGOTO_ERROR(0, "PDC object property memory allocation failed");
    if (info->app_name)
        q->app_name = strdup(info->app_name);
    q->time_step = info->time_step;
    if (info->tags)
        q->tags = strdup(info->tags);
    q->data_loc = NULL;
    q->buf      = NULL;

    /* struct obj_prop_pub field */
    q->obj_prop_pub = PDC_MALLOC(struct pdc_obj_prop);
    if (!q->obj_prop_pub)
        PGOTO_ERROR(0, "PDC object property memory allocation failed");
    new_id                       = PDC_id_register(PDC_OBJ_PROP, q);
    q->obj_prop_pub->obj_prop_id = new_id;
    q->obj_prop_pub->ndim        = info->obj_prop_pub->ndim;
    q->obj_prop_pub->dims        = (uint64_t *)malloc(info->obj_prop_pub->ndim * sizeof(uint64_t));
    q->obj_prop_pub->type        = PDC_UNKNOWN;
    for (i = 0; i < info->obj_prop_pub->ndim; i++)
        (q->obj_prop_pub->dims)[i] = (info->obj_prop_pub->dims)[i];

    /* struct _pdc_class field */
    q->pdc = PDC_CALLOC(struct _pdc_class);
    if (!q->pdc)
        PGOTO_ERROR(0, "PDC class memory allocation failed");
    if (info->pdc->name)
        q->pdc->name = strdup(info->pdc->name);
    q->pdc->local_id = info->pdc->local_id;

    ret_value = new_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_prop_cont_list_null()
{
    perr_t ret_value = SUCCEED;
    int    nelemts;

    FUNC_ENTER(NULL);

    // list is not empty
    nelemts = PDC_id_list_null(PDC_CONT_PROP);
    if (nelemts > 0) {
        if (PDC_id_list_clear(PDC_CONT_PROP) < 0)
            PGOTO_ERROR(FAIL, "fail to clear container property list");
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_prop_obj_list_null()
{
    perr_t ret_value = SUCCEED;
    int    nelemts;

    FUNC_ENTER(NULL);

    // list is not empty
    nelemts = PDC_id_list_null(PDC_OBJ_PROP);
    if (nelemts > 0) {
        if (PDC_id_list_clear(PDC_OBJ_PROP) < 0)
            PGOTO_ERROR(FAIL, "fail to clear obj property list");
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
pdc_prop_cont_close(struct _pdc_cont_prop *cp)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    free(cp->pdc->name);
    cp->pdc = PDC_FREE(struct _pdc_class, cp->pdc);
    cp      = PDC_FREE(struct _pdc_cont_prop, cp);

    FUNC_LEAVE(ret_value);
}

static perr_t
pdc_prop_obj_close(struct _pdc_obj_prop *cp)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    free(cp->pdc->name);
    cp->pdc = PDC_FREE(struct _pdc_class, cp->pdc);
    if (cp->obj_prop_pub->dims != NULL) {
        free(cp->obj_prop_pub->dims);
        cp->obj_prop_pub->dims = NULL;
    }
    free(cp->app_name);
    free(cp->tags);
    free(cp->data_loc);
    cp = PDC_FREE(struct _pdc_obj_prop, cp);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_close(pdcid_t id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(id) < 0)
        PGOTO_ERROR(FAIL, "property: problem of freeing id");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_prop_end()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (PDC_destroy_type(PDC_CONT_PROP) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy container property interface");

    if (PDC_destroy_type(PDC_OBJ_PROP) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy object property interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct _pdc_cont_prop *
PDCcont_prop_get_info(pdcid_t cont_prop)
{
    struct _pdc_cont_prop *ret_value = NULL;
    struct _pdc_cont_prop *info      = NULL;
    struct _pdc_id_info *  prop;

    FUNC_ENTER(NULL);

    prop = PDC_find_id(cont_prop);
    if (prop == NULL)
        PGOTO_ERROR(NULL, "cannot allocate container property");
    info = (struct _pdc_cont_prop *)(prop->obj_ptr);

    ret_value = PDC_CALLOC(struct _pdc_cont_prop);
    if (!ret_value)
        PGOTO_ERROR(NULL, "PDC container property memory allocation failed");
    ret_value->cont_life    = info->cont_life;
    ret_value->cont_prop_id = info->cont_prop_id;

    ret_value->pdc = PDC_CALLOC(struct _pdc_class);
    if (!ret_value->pdc)
        PGOTO_ERROR(NULL, "cannot allocate ret_value->pdc");
    if (info->pdc->name)
        ret_value->pdc->name = strdup(info->pdc->name);
    ret_value->pdc->local_id = info->pdc->local_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct pdc_obj_prop *
PDCobj_prop_get_info(pdcid_t obj_prop)
{
    struct pdc_obj_prop * ret_value = NULL;
    struct _pdc_obj_prop *info      = NULL;
    struct _pdc_id_info * prop;
    size_t                i;

    FUNC_ENTER(NULL);

    prop = PDC_find_id(obj_prop);
    if (prop == NULL)
        PGOTO_ERROR(NULL, "cannot locate object property");
    info = (struct _pdc_obj_prop *)(prop->obj_ptr);

    ret_value = PDC_CALLOC(struct pdc_obj_prop);
    if (ret_value == NULL)
        PGOTO_ERROR(NULL, "PDC object property memory allocation failed");
    memcpy(ret_value, info->obj_prop_pub, sizeof(struct pdc_obj_prop));

    ret_value->dims = malloc(info->obj_prop_pub->ndim * sizeof(uint64_t));
    if (ret_value->dims == NULL)
        PGOTO_ERROR(NULL, "cannot allocate ret_value->dims");
    for (i = 0; i < info->obj_prop_pub->ndim; i++)
        ret_value->dims[i] = info->obj_prop_pub->dims[i];

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct _pdc_obj_prop *
PDC_obj_prop_get_info(pdcid_t obj_prop)
{
    struct _pdc_obj_prop *ret_value = NULL;
    struct _pdc_obj_prop *info      = NULL;
    struct _pdc_id_info * prop;
    size_t                i;

    FUNC_ENTER(NULL);

    prop = PDC_find_id(obj_prop);
    if (prop == NULL)
        PGOTO_ERROR(NULL, "cannot locate object property");
    info = (struct _pdc_obj_prop *)(prop->obj_ptr);

    ret_value = PDC_CALLOC(struct _pdc_obj_prop);
    if (ret_value == NULL)
        PGOTO_ERROR(NULL, "PDC object property memory allocation failed");
    memcpy(ret_value, info, sizeof(struct _pdc_obj_prop));
    if (info->app_name)
        ret_value->app_name = strdup(info->app_name);
    if (info->app_name)
        ret_value->app_name = strdup(info->app_name);
    if (info->data_loc)
        ret_value->data_loc = strdup(info->data_loc);
    if (info->tags)
        ret_value->tags = strdup(info->tags);

    /* struct _pdc_class field */
    ret_value->pdc = PDC_CALLOC(struct _pdc_class);
    if (ret_value->pdc == NULL)
        PGOTO_ERROR(NULL, "cannot allocate ret_value->pdc");
    if (info->pdc->name)
        ret_value->pdc->name = strdup(info->pdc->name);
    ret_value->pdc->local_id = info->pdc->local_id;

    /* struct pdc_obj_prop field */
    ret_value->obj_prop_pub = PDC_CALLOC(struct pdc_obj_prop);
    if (ret_value->obj_prop_pub == NULL)
        PGOTO_ERROR(NULL, "PDC object pub property memory allocation failed");
    memcpy(ret_value->obj_prop_pub, info->obj_prop_pub, sizeof(struct pdc_obj_prop));
    ret_value->obj_prop_pub->dims = malloc(info->obj_prop_pub->ndim * sizeof(uint64_t));
    if (ret_value->obj_prop_pub->dims == NULL)
        PGOTO_ERROR(NULL, "cannot allocate ret_value->obj_prop_pub->dims");
    for (i = 0; i < info->obj_prop_pub->ndim; i++)
        ret_value->obj_prop_pub->dims[i] = info->obj_prop_pub->dims[i];

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

// Utility function for internal use.
perr_t
PDC_obj_prop_free(struct _pdc_obj_prop *cp)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = pdc_prop_obj_close(cp);

    FUNC_LEAVE(ret_value);
}
