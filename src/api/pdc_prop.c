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

#include <string.h>
#include "pdc_prop.h"
#include "pdc_interface.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"

static perr_t PDCprop__cont_close(struct PDC_cont_prop *cp);

static perr_t PDCprop__obj_close(struct PDC_obj_prop *cp);

/* PDC container property ID class */
/*
static const PDCID_class_t PDC_CONT_PROP_CLS[1] = {{
    PDC_CONT_PROP,
    0,
    0,
    (PDC_free_t)PDCprop__cont_close
}};
*/
/* PDC object property ID class */
/*
static const PDCID_class_t PDC_OBJ_PROP_CLS[1] = {{
    PDC_OBJ_PROP,
    0,
    0,
    (PDC_free_t)PDCprop__obj_close
}};
*/

perr_t PDCprop_init()
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);
    /* Initialize the atom group for the container property IDs */
    if(PDC_register_type(PDC_CONT_PROP, (PDC_free_t)PDCprop__cont_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize container property interface");

    /* Initialize the atom group for the object property IDs */
    if(PDC_register_type(PDC_OBJ_PROP, (PDC_free_t)PDCprop__obj_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize object property interface");

done:
    FUNC_LEAVE(ret_value);
} /* end PDCprop_init() */

pdcid_t PDCprop_create(PDC_prop_type type, pdcid_t pdcid)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_cont_prop *p = NULL;
    struct PDC_obj_prop *q = NULL;
    struct PDC_id_info *id_info = NULL;
    pdcid_t new_id_c;
    pdcid_t new_id_o;
 
    FUNC_ENTER(NULL);

    if (type == PDC_CONT_CREATE) {
        p = PDC_MALLOC(struct PDC_cont_prop);
        if(!p)
            PGOTO_ERROR(FAIL, "PDC container property memory allocation failed\n");
        p->cont_life = PDC_PERSIST;
        new_id_c = PDC_id_register(PDC_CONT_PROP, p);
        p->cont_prop_id = new_id_c;
        id_info = PDC_find_id(pdcid);
        p->pdc = (struct PDC_class *)(id_info->obj_ptr);
        ret_value = new_id_c;
    }
    if(type == PDC_OBJ_CREATE) {
        q = PDC_MALLOC(struct PDC_obj_prop);
        if(!q)
            PGOTO_ERROR(FAIL, "PDC object property memory allocation failed\n");
        q->obj_life = PDC_TRANSIENT;
        q->ndim = 0;
        q->dims = NULL;
        q->data_loc = NULL;
        q->type = PDC_UNKNOWN; 
	    q->app_name = NULL;
        q->time_step = 0;
	    q->tags = NULL;
        q->buf = NULL;
        new_id_o = PDC_id_register(PDC_OBJ_PROP, q);
        q->obj_prop_id = new_id_o;
        id_info = PDC_find_id(pdcid);
        q->pdc = (struct PDC_class *)(id_info->obj_ptr);
        ret_value = new_id_o;
    }
    
done:
    FUNC_LEAVE(ret_value);
} /* end PDCprop_create() */

pdcid_t PDCprop_obj_dup(pdcid_t prop_id)
{
    pdcid_t ret_value = SUCCEED;
    struct  PDC_obj_prop *q = NULL;
    struct  PDC_obj_prop *info = NULL;
    struct  PDC_id_info *prop = NULL;
    pdcid_t new_id;
    size_t  i;

    FUNC_ENTER(NULL);

    prop = PDC_find_id(prop_id);
    if(prop == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property");
    info = (struct PDC_obj_prop *)(prop->obj_ptr);

    q = PDC_MALLOC(struct PDC_obj_prop);
    if(!q)
        PGOTO_ERROR(FAIL, "PDC object property memory allocation failed\n");
    q->obj_life = info->obj_life;
    q->ndim = info->ndim;
    q->dims = (uint64_t *)malloc(info->ndim * sizeof(uint64_t));
    for(i=0; i<info->ndim; i++)
        (q->dims)[i] = (info->dims)[i];
    q->app_name = strdup(info->app_name);
    q->time_step = info->time_step;
    q->tags = strdup(info->tags);
    new_id = PDC_id_register(PDC_OBJ_PROP, q);
    q->obj_prop_id = new_id;
    q->pdc = info->pdc;
    q->data_loc = NULL;
    q->type = PDC_UNKNOWN;
    q->buf = NULL;
    ret_value = new_id;

done:
    FUNC_LEAVE(ret_value);
} /* end PDCprop_create() */

perr_t PDC_prop_cont_list_null()
{
    int nelemts;
    
    perr_t ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER(NULL);
    // list is not empty
    nelemts = PDC_id_list_null(PDC_CONT_PROP);
    if(nelemts > 0) {
        /* printf("%d element(s) in the container property list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_CONT_PROP) < 0)
            PGOTO_ERROR(FAIL, "fail to clear container property list");
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_prop_obj_list_null()
{
    perr_t ret_value = SUCCEED;   /* Return value */
    int nelemts;

    FUNC_ENTER(NULL);

    // list is not empty
    nelemts = PDC_id_list_null(PDC_OBJ_PROP);
    if(nelemts > 0) {
        /* printf("%d element(s) in the object property list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_OBJ_PROP) < 0)
            PGOTO_ERROR(FAIL, "fail to clear obj property list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

static perr_t PDCprop__cont_close(struct PDC_cont_prop *cp)
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    cp = PDC_FREE(struct PDC_cont_prop, cp);
    
    FUNC_LEAVE(ret_value);
} /* end PDCprop__cont_close() */

static perr_t PDCprop__obj_close(struct PDC_obj_prop *cp)
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(cp->dims != NULL) {
        free(cp->dims);
        cp->dims = NULL;
    }
    free(cp->app_name);
    free(cp->tags);
    free(cp->data_loc);
    cp = PDC_FREE(struct PDC_obj_prop, cp);
    
    FUNC_LEAVE(ret_value);
} /* end PDCprop__obj_close() */

perr_t PDCprop_close(pdcid_t id)
{
    perr_t ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(id) < 0)
        PGOTO_ERROR(FAIL, "property: problem of freeing id");

done:
    FUNC_LEAVE(ret_value);
} /* end of PDCprop_close() */

perr_t PDCprop_end()
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(PDC_destroy_type(PDC_CONT_PROP) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy container property interface");

    if(PDC_destroy_type(PDC_OBJ_PROP) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy object property interface");

done:
    FUNC_LEAVE(ret_value);
} /* end of PDCprop_end() */

struct PDC_cont_prop *PDCcont_prop_get_info(pdcid_t cont_prop)
{
    struct PDC_cont_prop *ret_value = NULL;
    struct PDC_cont_prop *info =  NULL;
    struct PDC_id_info *prop;
    
    FUNC_ENTER(NULL);
    
    prop = PDC_find_id(cont_prop);
    if(prop == NULL)
        PGOTO_ERROR(NULL, "cannot locate container property");
    
    info = (struct PDC_cont_prop *)(prop->obj_ptr);
    ret_value = info;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCcont_prop_get_info() */

struct PDC_obj_prop *PDCobj_prop_get_info(pdcid_t obj_prop)
{
    struct PDC_obj_prop *ret_value = NULL;
    struct PDC_obj_prop *info =  NULL;
    struct PDC_id_info *prop;
    
    FUNC_ENTER(NULL);
    
    prop = PDC_find_id(obj_prop);
    if(prop == NULL)
        PGOTO_ERROR(NULL, "cannot locate object property");
    
    info = (struct PDC_obj_prop *)(prop->obj_ptr);
    ret_value = info;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_prop_get_info() */
