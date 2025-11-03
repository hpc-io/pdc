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
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    /* Initialize the atom group for the container IDs */
    if (PDC_register_type(PDC_CONT, (PDC_free_t)PDC_cont_close) < 0)
        PGOTO_ERROR(FAIL, "Unable to initialize container interface");

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_create(const char *cont_name, pdcid_t cont_prop_id)
{
    FUNC_ENTER(NULL);

    pdcid_t                ret_value = 0;
    perr_t                 ret       = SUCCEED;
    struct _pdc_cont_info *p         = NULL;
    struct _pdc_cont_prop *cont_prop = NULL;
    struct _pdc_id_info *  id_info   = NULL;

    p = (struct _pdc_cont_info *)PDC_malloc(sizeof(struct _pdc_cont_info));
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");

    p->cont_info_pub = (struct pdc_cont_info *)PDC_malloc(sizeof(struct pdc_cont_info));
    if (!p->cont_info_pub)
        PGOTO_ERROR(0, "PDC pub container memory allocation failed");
    p->cont_info_pub->name = strdup(cont_name);

    if ((id_info = PDC_find_id(cont_prop_id)) == NULL)
        PGOTO_ERROR(0, "Failed to find PDC ID: %d", cont_prop_id);
    cont_prop = (struct _pdc_cont_prop *)(id_info->obj_ptr);

    p->cont_pt = (struct _pdc_cont_prop *)PDC_calloc(1, sizeof(struct _pdc_cont_prop));
    if (!p->cont_pt)
        PGOTO_ERROR(0, "PDC container prop memory allocation failed");
    memcpy(p->cont_pt, cont_prop, sizeof(struct _pdc_cont_prop));

    p->cont_pt->pdc = (struct _pdc_class *)PDC_calloc(1, sizeof(struct _pdc_class));
    if (!p->cont_pt->pdc)
        PGOTO_ERROR(0, "PDC container pdc class memory allocation failed");
    if (cont_prop->pdc->name)
        p->cont_pt->pdc->name = strdup(cont_prop->pdc->name);
    p->cont_pt->pdc->local_id = cont_prop->pdc->local_id;

    ret = PDC_Client_create_cont_id(cont_name, cont_prop_id, &(p->cont_info_pub->meta_id));

    if (ret == FAIL)
        PGOTO_ERROR(0, "Unable to create container on the server");

    p->cont_info_pub->local_id = PDC_id_register(PDC_CONT, p);

    ret_value = p->cont_info_pub->local_id;

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_create_col(const char *cont_name, pdcid_t cont_prop_id)
{
    FUNC_ENTER(NULL);

    pdcid_t                ret_value = 0;
    perr_t                 ret       = SUCCEED;
    struct _pdc_cont_info *p         = NULL;
    struct _pdc_cont_prop *cont_prop = NULL;
    struct _pdc_id_info *  id_info   = NULL;

    FUNC_ENTER(NULL);

    p = (struct _pdc_cont_info *)PDC_malloc(sizeof(struct _pdc_cont_info));
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");

    p->cont_info_pub = (struct pdc_cont_info *)PDC_malloc(sizeof(struct pdc_cont_info));
    if (!p->cont_info_pub)
        PGOTO_ERROR(0, "PDC pub container memory allocation failed");
    p->cont_info_pub->name = strdup(cont_name);

    if ((id_info = PDC_find_id(cont_prop_id)) == NULL)
        PGOTO_ERROR(0, "Failed to find PDC ID: %d", cont_prop_id);
    cont_prop = (struct _pdc_cont_prop *)(id_info->obj_ptr);

    p->cont_pt = (struct _pdc_cont_prop *)PDC_calloc(1, sizeof(struct _pdc_cont_prop));
    if (!p->cont_pt)
        PGOTO_ERROR(0, "PDC container prop memory allocation failed");
    memcpy(p->cont_pt, cont_prop, sizeof(struct _pdc_cont_prop));

    p->cont_pt->pdc = (struct _pdc_class *)PDC_calloc(1, sizeof(struct _pdc_class));
    if (!p->cont_pt->pdc)
        PGOTO_ERROR(0, "PDC container pdc class memory allocation failed");
    if (cont_prop->pdc->name)
        p->cont_pt->pdc->name = strdup(cont_prop->pdc->name);
    p->cont_pt->pdc->local_id = cont_prop->pdc->local_id;

    ret = PDC_Client_create_cont_id_mpi(cont_name, cont_prop_id, &(p->cont_info_pub->meta_id));
    if (ret == FAIL)
        PGOTO_ERROR(0, "Unable to create container object on server");

    p->cont_info_pub->local_id = PDC_id_register(PDC_CONT, p);
    ret_value                  = p->cont_info_pub->local_id;

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDC_cont_create_local(pdcid_t pdc, const char *cont_name, uint64_t cont_meta_id)
{
    FUNC_ENTER(NULL);

    pdcid_t                ret_value = 0;
    struct _pdc_cont_info *p         = NULL;
    struct _pdc_cont_prop *cont_prop = NULL;
    struct _pdc_id_info *  id_info   = NULL;
    pdcid_t                cont_prop_id;

    p = (struct _pdc_cont_info *)PDC_malloc(sizeof(struct _pdc_cont_info));
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");

    p->cont_info_pub = (struct pdc_cont_info *)PDC_malloc(sizeof(struct pdc_cont_info));
    if (!p)
        PGOTO_ERROR(0, "PDC container memory allocation failed");
    p->cont_info_pub->name    = strdup(cont_name);
    p->cont_info_pub->meta_id = cont_meta_id;

    cont_prop_id = PDCprop_create(PDC_CONT_CREATE, pdc);

    if ((id_info = PDC_find_id(cont_prop_id)) == NULL)
        PGOTO_ERROR(0, "Failed to find PDC ID: %d", cont_prop_id);
    cont_prop  = (struct _pdc_cont_prop *)(id_info->obj_ptr);
    p->cont_pt = (struct _pdc_cont_prop *)PDC_calloc(1, sizeof(struct _pdc_cont_prop));
    if (!p->cont_pt)
        PGOTO_ERROR(0, "PDC container prop memory allocation failed");
    memcpy(p->cont_pt, cont_prop, sizeof(struct _pdc_cont_prop));

    p->cont_pt->pdc = (struct _pdc_class *)PDC_calloc(1, sizeof(struct _pdc_class));
    if (!p->cont_pt->pdc)
        PGOTO_ERROR(0, "PDC container pdc class memory allocation failed");

    if (cont_prop->pdc->name)
        p->cont_pt->pdc->name = strdup(cont_prop->pdc->name);
    p->cont_pt->pdc->local_id = cont_prop->pdc->local_id;

    p->cont_info_pub->local_id = PDC_id_register(PDC_CONT, p);
    ret_value                  = p->cont_info_pub->local_id;

    PDCprop_close(cont_prop_id);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_cont_list_null()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    int    nelemts;

    // list is not empty
    nelemts = PDC_id_list_null(PDC_CONT);
    if (nelemts > 0) {
        if (PDC_id_list_clear(PDC_CONT) < 0)
            PGOTO_ERROR(FAIL, "Failed to clear container list");
    }

done:
    FUNC_LEAVE(ret_value);
}

static perr_t
PDC_cont_close(struct _pdc_cont_info *cp)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    cp->cont_info_pub->name = (char *)PDC_free((void *)(cp->cont_info_pub->name));
    cp->cont_info_pub       = (struct pdc_cont_info *)(intptr_t)PDC_free(cp->cont_info_pub);
    cp->cont_pt->pdc->name  = (char *)PDC_free(cp->cont_pt->pdc->name);
    cp->cont_pt->pdc        = (struct _pdc_class *)(intptr_t)PDC_free(cp->cont_pt->pdc);
    cp->cont_pt             = (struct _pdc_cont_prop *)(intptr_t)PDC_free(cp->cont_pt);
    cp                      = (struct _pdc_cont_info *)(intptr_t)PDC_free(cp);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_close(pdcid_t id)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    /* When the reference count reaches zero the resources are freed */
    if (PDC_dec_ref(id) < 0)
        PGOTO_ERROR(FAIL, "Container: problem of freeing id");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_cont_end()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (PDC_destroy_type(PDC_CONT) < 0)
        PGOTO_ERROR(FAIL, "Unable to destroy container interface");

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_open(const char *cont_name, pdcid_t pdc)
{
    FUNC_ENTER(NULL);

    pdcid_t ret_value = 0;
    perr_t  ret;
    pdcid_t cont_id;
    pdcid_t cont_meta_id;

    ret = PDC_Client_query_container_name(cont_name, &cont_meta_id);
    if (ret == FAIL)
        PGOTO_ERROR(0, "Query container name failed");
    if (cont_meta_id == 0)
        PGOTO_ERROR(0, "Query container not found");

    cont_id   = PDC_cont_create_local(pdc, cont_name, cont_meta_id);
    ret_value = cont_id;

done:
    FUNC_LEAVE(ret_value);
}

pdcid_t
PDCcont_open_col(const char *cont_name, pdcid_t pdc)
{
    FUNC_ENTER(NULL);

    pdcid_t ret_value = 0;
    perr_t  ret;
    pdcid_t cont_id;
    pdcid_t cont_meta_id;

    ret = PDC_Client_query_container_name_col(cont_name, &cont_meta_id);
    if (ret == FAIL)
        PGOTO_ERROR(0, "Query container name failed");
    cont_id   = PDC_cont_create_local(pdc, cont_name, cont_meta_id);
    ret_value = cont_id;

done:
    FUNC_LEAVE(ret_value);
}

struct _pdc_cont_info *
PDC_cont_get_info(pdcid_t cont_id)
{
    FUNC_ENTER(NULL);

    struct _pdc_cont_info *ret_value = NULL;
    struct _pdc_cont_info *info      = NULL;
    struct _pdc_id_info *  id_info   = NULL;

    if ((id_info = PDC_find_id(cont_id)) == NULL)
        PGOTO_ERROR(NULL, "Failed to find PDC ID: %d", cont_id);

    info      = (struct _pdc_cont_info *)(id_info->obj_ptr);
    ret_value = (struct _pdc_cont_info *)PDC_calloc(1, sizeof(struct _pdc_cont_info));
    if (ret_value)
        memcpy(ret_value, info, sizeof(struct _pdc_cont_info));
    else
        PGOTO_ERROR(NULL, "Cannot allocate ret_value");

    ret_value->cont_info_pub = (struct pdc_cont_info *)PDC_calloc(1, sizeof(struct pdc_cont_info));
    if (ret_value->cont_info_pub)
        memcpy(ret_value->cont_info_pub, info->cont_info_pub, sizeof(struct pdc_cont_info));

    if (info->cont_info_pub->name)
        ret_value->cont_info_pub->name = strdup(info->cont_info_pub->name);

    ret_value->cont_pt = (struct _pdc_cont_prop *)PDC_malloc(sizeof(struct _pdc_cont_prop));
    if (ret_value->cont_pt)
        memcpy(ret_value->cont_pt, info->cont_pt, sizeof(struct _pdc_cont_prop));
    else
        PGOTO_ERROR(NULL, "Cannot allocate ret_value->cont_pt");
    ret_value->cont_pt->pdc = (struct _pdc_class *)PDC_calloc(1, sizeof(struct _pdc_class));
    if (ret_value->cont_pt->pdc) {
        ret_value->cont_pt->pdc->local_id = info->cont_pt->pdc->local_id;
        if (info->cont_pt->pdc->name)
            ret_value->cont_pt->pdc->name = strdup(info->cont_pt->pdc->name);
    }
    else
        PGOTO_ERROR(NULL, "Cannot allocate ret_value->cont_pt->pdc");

done:
    FUNC_LEAVE(ret_value);
}

struct pdc_cont_info *
PDCcont_get_info(const char *cont_name)
{
    FUNC_ENTER(NULL);

    struct pdc_cont_info * ret_value = NULL;
    struct _pdc_cont_info *tmp       = NULL;
    pdcid_t                cont_id;

    cont_id = PDC_find_byname(PDC_CONT, cont_name);

    tmp = PDC_cont_get_info(cont_id);

    ret_value = (struct pdc_cont_info *)PDC_calloc(1, sizeof(struct pdc_cont_info));
    if (!ret_value)
        PGOTO_ERROR(NULL, "Cannot allocate memory");

    ret_value = tmp->cont_info_pub;

done:
    FUNC_LEAVE(ret_value);
}

cont_handle *
PDCcont_iter_start()
{
    FUNC_ENTER(NULL);

    cont_handle *       ret_value = NULL;
    cont_handle *       conthl    = NULL;
    struct PDC_id_type *type_ptr;

    type_ptr = (pdc_id_list_g->PDC_id_type_list_g)[PDC_CONT];
    if (type_ptr == NULL)
        PGOTO_ERROR(NULL, "Container list is empty");
    conthl    = (&type_ptr->ids)->head;
    ret_value = conthl;

done:
    FUNC_LEAVE(ret_value);
}

pbool_t
PDCcont_iter_null(cont_handle *chandle)
{
    FUNC_ENTER(NULL);

    pbool_t ret_value = FALSE;
    if (chandle == NULL)
        ret_value = TRUE;

    FUNC_LEAVE(ret_value);
}

cont_handle *
PDCcont_iter_next(cont_handle *chandle)
{
    FUNC_ENTER(NULL);

    cont_handle *ret_value = NULL;
    cont_handle *next      = NULL;

    if (chandle == NULL)
        PGOTO_ERROR(NULL, "No next container");
    next      = PDC_LIST_NEXT(chandle, entry);
    ret_value = next;

done:
    FUNC_LEAVE(ret_value);
}

struct pdc_cont_info *
PDCcont_iter_get_info(cont_handle *chandle)
{
    FUNC_ENTER(NULL);

    struct pdc_cont_info * ret_value = NULL;
    struct _pdc_cont_info *info      = NULL;

    info = (struct _pdc_cont_info *)(chandle->obj_ptr);
    if (info == NULL)
        PGOTO_ERROR(NULL, "PDC container info memory allocation failed");

    ret_value = (struct pdc_cont_info *)PDC_calloc(1, sizeof(struct pdc_cont_info));
    if (!ret_value)
        PGOTO_ERROR(NULL, "Failed to allocate memory");

    ret_value = info->cont_info_pub;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCcont_persist(pdcid_t cont_id)
{
    FUNC_ENTER(NULL);

    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    if ((info = PDC_find_id(cont_id)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", cont_id);

    ((struct _pdc_cont_info *)info->obj_ptr)->cont_pt->cont_life = PDC_PERSIST;

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCprop_set_cont_lifetime(pdcid_t cont_prop, pdc_lifetime_t cont_lifetime)
{
    FUNC_ENTER(NULL);

    perr_t               ret_value = SUCCEED;
    struct _pdc_id_info *info;

    if ((info = PDC_find_id(cont_prop)) == NULL)
        PGOTO_ERROR(FAIL, "Failed to find PDC ID: %d", cont_prop);
    ((struct _pdc_cont_prop *)(info->obj_ptr))->cont_life = cont_lifetime;

done:
    FUNC_LEAVE(ret_value);
}
