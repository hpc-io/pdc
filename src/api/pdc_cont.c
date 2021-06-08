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

#include "pdc_cont.h"
#include "pdc_cont_pkg.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_id_pkg.h"
#include "pdc_interface.h"
#include "pdc_query.h"
#include "pdc_client_connect.h"
#include <string.h>

static perr_t PDC_cont_close(struct _pdc_cont_info *cp);

perr_t
PDC_cont_init()
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the container IDs */
    if (PDC_register_type(PDC_CONT, (PDC_free_t)PDC_cont_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize container interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_create(const char *cont_name, pdcid_t cont_prop_id)
{
    pdcid_t                ret_value = 0;
    perr_t                 ret       = SUCCEED;
    struct _pdc_cont_info *p         = NULL;
    struct _pdc_cont_prop *cont_prop = NULL;
    struct _pdc_id_info *  id_info   = NULL;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct _pdc_cont_info);
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");

    p->cont_info_pub = PDC_MALLOC(struct pdc_cont_info);
    if (!p->cont_info_pub)
        PGOTO_ERROR(0, "PDC pub container memory allocation failed");
    p->cont_info_pub->name = strdup(cont_name);

    id_info   = PDC_find_id(cont_prop_id);
    cont_prop = (struct _pdc_cont_prop *)(id_info->obj_ptr);

    p->cont_pt = PDC_CALLOC(struct _pdc_cont_prop);
    if (!p->cont_pt)
        PGOTO_ERROR(0, "PDC container prop memory allocation failed");
    memcpy(p->cont_pt, cont_prop, sizeof(struct _pdc_cont_prop));

    p->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (!p->cont_pt->pdc)
        PGOTO_ERROR(0, "PDC container pdc class memory allocation failed");
    if (cont_prop->pdc->name)
        p->cont_pt->pdc->name = strdup(cont_prop->pdc->name);
    p->cont_pt->pdc->local_id = cont_prop->pdc->local_id;

    ret = PDC_Client_create_cont_id(cont_name, cont_prop_id, &(p->cont_info_pub->meta_id));
    if (ret == FAIL)
        PGOTO_ERROR(0, "Unable to create container on the server!");

    p->cont_info_pub->local_id = PDC_id_register(PDC_CONT, p);

    ret_value = p->cont_info_pub->local_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_create_col(const char *cont_name, pdcid_t cont_prop_id)
{
    pdcid_t                ret_value = 0;
    perr_t                 ret       = SUCCEED;
    struct _pdc_cont_info *p         = NULL;
    struct _pdc_cont_prop *cont_prop = NULL;
    struct _pdc_id_info *  id_info   = NULL;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct _pdc_cont_info);
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");

    p->cont_info_pub = PDC_MALLOC(struct pdc_cont_info);
    if (!p->cont_info_pub)
        PGOTO_ERROR(0, "PDC pub container memory allocation failed");
    p->cont_info_pub->name = strdup(cont_name);

    id_info   = PDC_find_id(cont_prop_id);
    cont_prop = (struct _pdc_cont_prop *)(id_info->obj_ptr);

    p->cont_pt = PDC_CALLOC(struct _pdc_cont_prop);
    if (!p->cont_pt)
        PGOTO_ERROR(0, "PDC container prop memory allocation failed");
    memcpy(p->cont_pt, cont_prop, sizeof(struct _pdc_cont_prop));

    p->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (!p->cont_pt->pdc)
        PGOTO_ERROR(0, "PDC container pdc class memory allocation failed");
    if (cont_prop->pdc->name)
        p->cont_pt->pdc->name = strdup(cont_prop->pdc->name);
    p->cont_pt->pdc->local_id = cont_prop->pdc->local_id;

    ret = PDC_Client_create_cont_id_mpi(cont_name, cont_prop_id, &(p->cont_info_pub->meta_id));
    if (ret == FAIL)
        PGOTO_ERROR(0, "Unable to create container object on server!");

    p->cont_info_pub->local_id = PDC_id_register(PDC_CONT, p);
    ret_value                  = p->cont_info_pub->local_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDC_cont_create_local(pdcid_t pdc, const char *cont_name, uint64_t cont_meta_id)
{
    pdcid_t                ret_value = 0;
    struct _pdc_cont_info *p         = NULL;
    struct _pdc_cont_prop *cont_prop = NULL;
    struct _pdc_id_info *  id_info   = NULL;
    pdcid_t                cont_prop_id;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct _pdc_cont_info);
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");

    p->cont_info_pub = PDC_MALLOC(struct pdc_cont_info);
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");
    p->cont_info_pub->name    = strdup(cont_name);
    p->cont_info_pub->meta_id = cont_meta_id;

    cont_prop_id = PDCprop_create(PDC_CONT_CREATE, pdc);

    id_info    = PDC_find_id(cont_prop_id);
    cont_prop  = (struct _pdc_cont_prop *)(id_info->obj_ptr);
    p->cont_pt = PDC_CALLOC(struct _pdc_cont_prop);
    if (!p->cont_pt)
        PGOTO_ERROR(0, "PDC container prop memory allocation failed");
    memcpy(p->cont_pt, cont_prop, sizeof(struct _pdc_cont_prop));

    p->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (!p->cont_pt->pdc)
        PGOTO_ERROR(0, "PDC container pdc class memory allocation failed");

    if (cont_prop->pdc->name)
        p->cont_pt->pdc->name = strdup(cont_prop->pdc->name);
    p->cont_pt->pdc->local_id = cont_prop->pdc->local_id;

    p->cont_info_pub->local_id = PDC_id_register(PDC_CONT, p);
    ret_value                  = p->cont_info_pub->local_id;

    PDCprop_close(cont_prop_id);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_cont_list_null()
{
    perr_t ret_value = SUCCEED;
    int    nelemts;

    FUNC_ENTER(NULL);

    // list is not empty
    nelemts = PDC_id_list_null(PDC_CONT);
    if (nelemts > 0) {
        if (PDC_id_list_clear(PDC_CONT) < 0)
            PGOTO_ERROR(FAIL, "fail to clear container list");
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_cont_close(struct _pdc_cont_info *cp)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    free((void *)(cp->cont_info_pub->name));
    cp->cont_info_pub = PDC_FREE(struct pdc_cont_info, cp->cont_info_pub);
    free(cp->cont_pt->pdc->name);
    cp->cont_pt->pdc = PDC_FREE(struct _pdc_class, cp->cont_pt->pdc);
    cp->cont_pt      = PDC_FREE(struct _pdc_cont_prop, cp->cont_pt);
    cp               = PDC_FREE(struct _pdc_cont_info, cp);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_close(pdcid_t id)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(id) < 0)
        PGOTO_ERROR(FAIL, "container: problem of freeing id");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_cont_end()
{
    perr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER(NULL);

    if (PDC_destroy_type(PDC_CONT) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy container interface");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_open(const char *cont_name, pdcid_t pdc)
{
    pdcid_t ret_value = 0;
    perr_t  ret;
    pdcid_t cont_id;
    pdcid_t cont_meta_id;

    FUNC_ENTER(NULL);

    ret = PDC_Client_query_container_name(cont_name, &cont_meta_id);
    if (ret == FAIL)
        PGOTO_ERROR(0, "query container name failed");
    cont_id   = PDC_cont_create_local(pdc, cont_name, cont_meta_id);
    ret_value = cont_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_open_col(const char *cont_name, pdcid_t pdc)
{
    pdcid_t ret_value = 0;
    perr_t  ret;
    pdcid_t cont_id;
    pdcid_t cont_meta_id;

    FUNC_ENTER(NULL);

    ret = PDC_Client_query_container_name_col(cont_name, &cont_meta_id);
    if (ret == FAIL)
        PGOTO_ERROR(0, "query container name failed");
    cont_id   = PDC_cont_create_local(pdc, cont_name, cont_meta_id);
    ret_value = cont_id;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct _pdc_cont_info *
PDC_cont_get_info(pdcid_t cont_id)
{
    struct _pdc_cont_info *ret_value = NULL;
    struct _pdc_cont_info *info      = NULL;
    struct _pdc_id_info *  id_info   = NULL;

    FUNC_ENTER(NULL);

    id_info = PDC_find_id(cont_id);
    info    = (struct _pdc_cont_info *)(id_info->obj_ptr);

    ret_value = PDC_CALLOC(struct _pdc_cont_info);
    if (ret_value)
        memcpy(ret_value, info, sizeof(struct _pdc_cont_info));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value");

    ret_value->cont_info_pub = PDC_CALLOC(struct pdc_cont_info);
    if (ret_value->cont_info_pub)
        memcpy(ret_value, info, sizeof(struct pdc_cont_info));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont_info_pub");
    if (info->cont_info_pub->name)
        ret_value->cont_info_pub->name = strdup(info->cont_info_pub->name);

    ret_value->cont_pt = PDC_MALLOC(struct _pdc_cont_prop);
    if (ret_value->cont_pt)
        memcpy(ret_value->cont_pt, info->cont_pt, sizeof(struct _pdc_cont_prop));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont_pt");
    ret_value->cont_pt->pdc = PDC_CALLOC(struct _pdc_class);
    if (ret_value->cont_pt->pdc) {
        ret_value->cont_pt->pdc->local_id = info->cont_pt->pdc->local_id;
        if (info->cont_pt->pdc->name)
            ret_value->cont_pt->pdc->name = strdup(info->cont_pt->pdc->name);
    }
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont_pt->pdc");

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct pdc_cont_info *
PDCcont_get_info(const char *cont_name)
{
    struct pdc_cont_info * ret_value = NULL;
    struct _pdc_cont_info *tmp       = NULL;
    pdcid_t                cont_id;

    FUNC_ENTER(NULL);

    cont_id = PDC_find_byname(PDC_CONT, cont_name);

    tmp = PDC_cont_get_info(cont_id);

    ret_value = PDC_CALLOC(struct pdc_cont_info);
    if (!ret_value)
        PGOTO_ERROR(NULL, "cannot allocate memory");

    ret_value = tmp->cont_info_pub;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

cont_handle *
PDCcont_iter_start()
{
    cont_handle *       ret_value = NULL;
    cont_handle *       conthl    = NULL;
    struct PDC_id_type *type_ptr;

    FUNC_ENTER(NULL);

    type_ptr = (pdc_id_list_g->PDC_id_type_list_g)[PDC_CONT];
    if (type_ptr == NULL)
        PGOTO_ERROR(NULL, "container list is empty");
    conthl    = (&type_ptr->ids)->head;
    ret_value = conthl;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pbool_t
PDCcont_iter_null(cont_handle *chandle)
{
    pbool_t ret_value = FALSE;

    FUNC_ENTER(NULL);

    if (chandle == NULL)
        ret_value = TRUE;

    FUNC_LEAVE(ret_value);
}

cont_handle *
PDCcont_iter_next(cont_handle *chandle)
{
    cont_handle *ret_value = NULL;
    cont_handle *next      = NULL;

    FUNC_ENTER(NULL);

    if (chandle == NULL)
        PGOTO_ERROR(NULL, "no next container");
    next      = PDC_LIST_NEXT(chandle, entry);
    ret_value = next;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

struct pdc_cont_info *
PDCcont_iter_get_info(cont_handle *chandle)
{
    struct pdc_cont_info * ret_value = NULL;
    struct _pdc_cont_info *info      = NULL;

    FUNC_ENTER(NULL);

    info = (struct _pdc_cont_info *)(chandle->obj_ptr);
    if (info == NULL)
        PGOTO_ERROR(NULL, "PDC container info memory allocation failed");

    ret_value = PDC_CALLOC(struct pdc_cont_info);
    if (!ret_value)
        PGOTO_ERROR(NULL, "failed to allocate memory");

    ret_value = info->cont_info_pub;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_persist(pdcid_t cont_id)
{
    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    FUNC_ENTER(NULL);

    info = PDC_find_id(cont_id);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate container ID");

    ((struct _pdc_cont_info *)info->obj_ptr)->cont_pt->cont_life = PDC_PERSIST;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_cont_lifetime(pdcid_t cont_prop, pdc_lifetime_t cont_lifetime)
{
    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    FUNC_ENTER(NULL);

    info = PDC_find_id(cont_prop);
    if (info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate container property ID");
    ((struct _pdc_cont_prop *)(info->obj_ptr))->cont_life = cont_lifetime;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}
