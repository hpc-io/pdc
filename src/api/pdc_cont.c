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
#include "pdc_cont.h"
#include "pdc_cont_private.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_atomic.h"
#include "pdc_client_connect.h"

static perr_t pdc_cont_close(struct PDC_cont_info *cp);

perr_t pdc_cont_init()
{
    perr_t ret_value = SUCCEED;     

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the container IDs */
    if(PDC_register_type(PDC_CONT, (PDC_free_t)pdc_cont_close) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize container interface");

done:
    FUNC_LEAVE(ret_value); 
} 

pdcid_t PDCcont_create(const char *cont_name, pdcid_t cont_prop_id)
{
    pdcid_t ret_value = 0;
    pdcid_t new_id;
    struct PDC_cont_info *p = NULL;
    struct PDC_id_info *id_info = NULL;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct PDC_cont_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC container memory allocation failed\n");
    p->name = strdup(cont_name);
    
    id_info = pdc_find_id(cont_prop_id);
    p->cont_pt = (struct PDC_cont_prop *)(id_info->obj_ptr);
 
    /* ret_value = PDC_Client_create_cont_id(cont_name, cont_prop_id, &(p->meta_id)); */
    ret_value = PDC_Client_create_cont_id(cont_name, &(p->meta_id));
    if (ret_value == FAIL) {
        ret_value = -1;
        PGOTO_ERROR(FAIL,"Unable to create container object on server!\n");
    }
   
    new_id = pdc_id_register(PDC_CONT, p);
    p->local_id = new_id;
    ret_value = new_id;


done:
    FUNC_LEAVE(ret_value);
}

perr_t pdc_cont_list_null()
{
    perr_t ret_value = SUCCEED; 
    int nelemts;
    
    FUNC_ENTER(NULL);
    // list is not empty
    nelemts = pdc_id_list_null(PDC_CONT);
    if(nelemts > 0) {
        /* printf("%d element(s) in the container list will be automatically closed by PDC_close()\n", nelemts); */
        if(pdc_id_list_clear(PDC_CONT) < 0)
            PGOTO_ERROR(FAIL, "fail to clear container list");
    }

done:
    FUNC_LEAVE(ret_value);
}

static perr_t pdc_cont_close(struct PDC_cont_info *cp)
{
    perr_t ret_value = SUCCEED;     

    FUNC_ENTER(NULL);

    free((void*)(cp->name));
    cp = PDC_FREE(struct PDC_cont_info, cp);
    
    FUNC_LEAVE(ret_value);
} 

perr_t PDCcont_close(pdcid_t id)
{
    perr_t ret_value = SUCCEED;   

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if(pdc_dec_ref(id) < 0)
        PGOTO_ERROR(FAIL, "container: problem of freeing id");
    
done:
    FUNC_LEAVE(ret_value);
} 

perr_t pdc_cont_end()
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(pdc_destroy_type(PDC_CONT) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy container interface");
    
done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCcont_open(const char *cont_name)
{
    pdcid_t ret_value = 0;
    pdcid_t cont_id;

    FUNC_ENTER(NULL);

    // should wait for response from server 
    // look up in the list for now
    cont_id = pdc_find_byname(PDC_CONT, cont_name);
    // if contained is closed locally, contact metadata server
    if(cont_id == 0)
        PGOTO_ERROR(ret_value, "cannot locate container");
    pdc_inc_ref(cont_id);
    ret_value = cont_id;
    
done:
    FUNC_LEAVE(ret_value);
} 

struct PDC_cont_info *PDCcont_get_info(pdcid_t cont_id)
{
    struct PDC_cont_info *ret_value = NULL;
    struct PDC_cont_info *info = NULL;
    struct PDC_id_info *id_info = NULL;
    
    FUNC_ENTER(NULL);
    
    id_info = pdc_find_id(cont_id);
    info = (struct PDC_cont_info *)(id_info->obj_ptr);
    
    ret_value = PDC_CALLOC(struct PDC_cont_info);
    if(ret_value)
        memcpy(ret_value, info, sizeof(struct PDC_cont_info));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value");
    if(info->name)
        ret_value->name = strdup(info->name);
    else
        ret_value->name = NULL;
    
    ret_value->cont_pt = PDC_MALLOC(struct PDC_cont_prop);
    if(ret_value->cont_pt)
        memcpy(ret_value->cont_pt, info->cont_pt, sizeof(struct PDC_cont_prop));
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont_pt");
    ret_value->cont_pt->pdc = PDC_CALLOC(struct PDC_class);
    if(ret_value->cont_pt->pdc) {
        ret_value->cont_pt->pdc->local_id = info->cont_pt->pdc->local_id;
        if(info->cont_pt->pdc->name)
            ret_value->cont_pt->pdc->name = strdup(info->cont_pt->pdc->name);
        else
            ret_value->cont_pt->pdc->name = NULL;
    }
    else
        PGOTO_ERROR(NULL, "cannot allocate ret_value->cont_pt->pdc");
    
done:
    FUNC_LEAVE(ret_value);
}

cont_handle *PDCcont_iter_start()
{
    cont_handle *ret_value = NULL;
    cont_handle *conthl = NULL;
    struct PDC_id_type *type_ptr;

    FUNC_ENTER(NULL);

    type_ptr  = (pdc_id_list_g->PDC_id_type_list_g)[PDC_CONT];
    if(type_ptr == NULL) 
        PGOTO_ERROR(NULL, "container list is empty");
    conthl = (&type_ptr->ids)->head;
    ret_value = conthl;
    
done:
    FUNC_LEAVE(ret_value);
} 

pbool_t PDCcont_iter_null(cont_handle *chandle)
{
    pbool_t ret_value = FALSE;
    
    FUNC_ENTER(NULL);
    
    if(chandle == NULL)
        ret_value = TRUE;
    
    FUNC_LEAVE(ret_value); 
}

cont_handle *PDCcont_iter_next(cont_handle *chandle)
{
    cont_handle *ret_value = NULL;
    cont_handle *next = NULL;

    FUNC_ENTER(NULL);

    if(chandle == NULL)
        PGOTO_ERROR(NULL, "no next container");
    next = PDC_LIST_NEXT(chandle, entry); 
    ret_value = next;
    
done:
    FUNC_LEAVE(ret_value);
} 

struct PDC_cont_info *PDCcont_iter_get_info(cont_handle *chandle)
{
    struct PDC_cont_info *ret_value = NULL;
    struct PDC_cont_info *info = NULL;

    FUNC_ENTER(NULL);

    info = (struct PDC_cont_info *)(chandle->obj_ptr);
    if(info == NULL)
        PGOTO_ERROR(NULL, "PDC container info memory allocation failed");
    
    ret_value = info;
    
done:
    FUNC_LEAVE(ret_value);
} 

perr_t PDCcont_persist(pdcid_t cont_id)
{
    perr_t ret_value = SUCCEED;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = pdc_find_id(cont_id);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate container ID");

    ((struct PDC_cont_info *)info->obj_ptr)->cont_pt->cont_life = PDC_PERSIST;
    
done:
    FUNC_LEAVE(ret_value);
} 

perr_t PDCprop_set_cont_lifetime(pdcid_t cont_prop, PDC_lifetime cont_lifetime)
{
    perr_t ret_value = SUCCEED;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    info = pdc_find_id(cont_prop);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate container property ID");
    ((struct PDC_cont_prop *)(info->obj_ptr))->cont_life = cont_lifetime;
    
done:
    FUNC_LEAVE(ret_value);
}
