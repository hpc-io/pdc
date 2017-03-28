#include "pdc_obj.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"

static perr_t PDCobj__close(PDC_obj_info_t *op);

static perr_t PDCregion__close(PDC_region_info_t *op);

/* PDC object ID class */
static const PDCID_class_t PDC_OBJ_CLS[1] = {{
    PDC_OBJ,                            /* ID class value */
    0,                                  /* Class flags */
    0,                                  /* # of reserved IDs for class */
    (PDC_free_t)PDCobj__close           /* Callback routine for closing objects of this class */
}};

/* PDC region ID class */
static const PDCID_class_t PDC_REGION_CLS[1] = {{
    PDC_REGION,                         /* ID class value */
    0,                                  /* Class flags */
    0,                                  /* # of reserved IDs for class */
    (PDC_free_t)PDCregion__close        /* Callback routine for closing regions of this class */
}};

perr_t PDCobj_init(PDC_CLASS_t *pc) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the object IDs */
    if(PDC_register_type(PDC_OBJ_CLS, pc) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize object interface");

done:
    FUNC_LEAVE(ret_value);
} /* end PDCobj_init() */

perr_t PDCregion_init(PDC_CLASS_t *pc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* Initialize the atom group for the region IDs */
    if(PDC_register_type(PDC_REGION_CLS, pc) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize region interface");
    
done:
    FUNC_LEAVE(ret_value);
} /* end PDCregion_init() */


pdcid_t PDCobj_create(pdcid_t pdc, pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop){
    pdcid_t ret_value = SUCCEED;
    PDC_obj_info_t *p = NULL;
    
    FUNC_ENTER(NULL);
    
    p = PDC_MALLOC(PDC_obj_info_t);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC object memory allocation failed\n");
    p->name = strdup(obj_name);
    p->cont = cont_id;
    p->pdc = pdc;
    p->obj_prop = obj_create_prop;
    p->mapping = 0;
    // will contact server to get ID
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    pdcid_t new_id = PDC_id_register(PDC_OBJ, p, pdc);

    perr_t ret;
    ret = PDC_Client_send_name_recv_id(pdc, cont_id, obj_name, obj_create_prop, &(p->meta_id));

    ret_value = new_id;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_obj_list_null(pdcid_t pdc) {
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    // list is not empty
    int nelemts = PDC_id_list_null(PDC_OBJ, pdc);
    if(nelemts > 0) {
        /* printf("%d element(s) in the object list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_OBJ, pdc) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_region_list_null(pdcid_t pdc) {
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    // list is not empty
    int nelemts = PDC_id_list_null(PDC_REGION, pdc);
    if(nelemts > 0) {
        /* printf("%d element(s) in the region list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_REGION, pdc) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj__close(PDC_obj_info_t *op) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    op = PDC_FREE(PDC_obj_info_t, op);
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCregion__close(PDC_region_info_t *op) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    free(op->size);
    op = PDC_FREE(PDC_region_info_t, op);
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_close(pdcid_t obj_id, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(obj_id, pdc) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCregion_close(pdcid_t region_id, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(region_id, pdc) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_end(pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(PDC_destroy_type(PDC_OBJ, pdc) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy object interface");
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_end() */

perr_t PDCregion_end(pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    if(PDC_destroy_type(PDC_REGION, pdc) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy region interface");
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCregion_end() */

pdcid_t PDCobj_open(pdcid_t cont_id, const char *obj_name, pdcid_t pdc) {
    pdcid_t ret_value = SUCCEED;
    
    FUNC_ENTER(NULL);
    
    // should wait for response from server
    // look up in the list for now
    pdcid_t ret_value1;
    ret_value1 = PDC_find_byname(PDC_OBJ, obj_name, pdc);
    if(ret_value1 <= 0)
        PGOTO_ERROR(FAIL, "cannot locate object");
    pdc_inc_ref(ret_value1, pdc);
    ret_value = ret_value1;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_open() */

obj_handle *PDCobj_iter_start(pdcid_t cont_id, pdcid_t pdc_id) {
    obj_handle *ret_value = NULL;
    obj_handle *objhl = NULL;
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc_id;
    PDC_id_type_t *type_ptr  = (pc->PDC_id_type_list_g)[PDC_OBJ];
    if(type_ptr == NULL)
        PGOTO_ERROR(NULL, "object list is empty");
    objhl = (&type_ptr->ids)->head;
    while(objhl!=NULL && ((PDC_obj_info_t *)(objhl->obj_ptr))->cont!=cont_id) {
        objhl = PDC_LIST_NEXT(objhl, entry);
    }
    ret_value = objhl;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_start() */

pbool_t PDCobj_iter_null(obj_handle *ohandle) {
    pbool_t ret_value = FALSE;
    
    FUNC_ENTER(NULL);
    
    if(ohandle == NULL)
        ret_value = TRUE;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_null() */

obj_handle *PDCobj_iter_next(obj_handle *ohandle, pdcid_t cont_id) {
    obj_handle *ret_value = NULL;
    obj_handle *next = NULL;
    
    FUNC_ENTER(NULL);
    
    if(ohandle == NULL)
        PGOTO_ERROR(NULL, "no next object");
    next = PDC_LIST_NEXT(ohandle, entry);
    while(next!=NULL && ((PDC_obj_info_t *)(next->obj_ptr))->cont!= cont_id) {
        next = PDC_LIST_NEXT(ohandle, entry);
    }
    ret_value = next;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_next() */

PDC_obj_info_t *PDCobj_iter_get_info(obj_handle *ohandle) {
    PDC_obj_info_t *ret_value = NULL;
    PDC_obj_info_t *info = NULL;
    
    FUNC_ENTER(NULL);
    info = (PDC_obj_info_t *)(ohandle->obj_ptr);
    if(info == NULL)
        PGOTO_ERROR(NULL, "PDC container info memory allocation failed");
    ret_value = info;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_lifetime(pdcid_t obj_prop, PDC_lifetime obj_lifetime, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((PDC_obj_prop_t *)(info->obj_ptr))->obj_life = obj_lifetime;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((PDC_obj_prop_t *)(info->obj_ptr))->user_id = user_id;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((PDC_obj_prop_t *)(info->obj_ptr))->app_name = strdup(app_name);
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((PDC_obj_prop_t *)(info->obj_ptr))->time_step = time_step;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((PDC_obj_prop_t *)(info->obj_ptr))->tags = strdup(tags);
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    PDC_obj_prop_t *prop = (PDC_obj_prop_t *)(info->obj_ptr);
    prop->ndim = ndim;
    prop->dims = (uint64_t *)malloc(ndim * sizeof(uint64_t));
    
    int i = 0;
    for(i=0; i<ndim; i++)
        (prop->dims)[i] = dims[i];
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_type(pdcid_t obj_prop, PDC_var_type_t type, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    PDC_obj_prop_t *prop = (PDC_obj_prop_t *)(info->obj_ptr);
    prop->type = type;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    PDC_obj_prop_t *prop = (PDC_obj_prop_t *)(info->obj_ptr);
    prop->buf = buf;
done:
    FUNC_LEAVE(ret_value);
}

void **PDCobj_buf_retrieve(pdcid_t obj_id, pdcid_t pdc) {
    void **ret_value = NULL;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *info = PDC_find_id(obj_id, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    PDC_obj_info_t *object = (PDC_obj_info_t *)(info->obj_ptr);
    pdcid_t propid = object->obj_prop;
    info = PDC_find_id(propid, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    PDC_obj_prop_t *prop = (PDC_obj_prop_t *)(info->obj_ptr);
    void **buffer = &(prop->buf);
    ret_value = buffer;
done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCregion_create(size_t ndims, uint64_t offset, uint64_t *size, pdcid_t pdc_id) {
    pdcid_t ret_value = SUCCEED;         /* Return value */
    PDC_region_info_t *p = NULL;
    
    FUNC_ENTER(NULL);
    
    p = PDC_MALLOC(PDC_region_info_t);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC region memory allocation failed\n");
    p->ndim = ndims;
    p->offset = offset;
    p->size = (uint64_t *)malloc(ndims * sizeof(uint64_t));
    int i = 0;
    for(i=0; i<ndims; i++)
        (p->size)[i] = size[i];
    // data type?
    pdcid_t new_id = PDC_id_register(PDC_REGION, p, pdc_id);
    ret_value = new_id;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_map(pdcid_t from_obj, pdcid_t from_reg, pdcid_t to_obj, pdcid_t to_reg, pdcid_t pdc_id) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
	ret_value = PDC_Client_send_region_map(from_obj, from_reg, to_obj, to_reg); 
    // PDC_CLASS_t defined in pdc_interface.h
    // PDC_obj_info_t defined in pdc_obj_pkg.h
    // PDC_region_info_t defined in pdc_obj_pkg.h
    // start mapping
    // state that there is mapping to other objects
//    object1->mapping = 1;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_buf_map(void *buf, pdcid_t from_reg, pdcid_t obj_id, pdcid_t to_reg, pdcid_t pdc_id) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc_id;
    PDC_id_info_t *info1 = PDC_find_id(obj_id, pc);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    PDC_obj_info_t *object1 = (PDC_obj_info_t *)(info1->obj_ptr);
    
    // check if ndim matches between object and region
    pdcid_t propid = object1->obj_prop;
    info1 = PDC_find_id(propid, pc);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    PDC_obj_prop_t *prop = (PDC_obj_prop_t *)(info1->obj_ptr);
    
    PDC_id_info_t *reginfo1 = PDC_find_id(from_reg, pc);
    PDC_id_info_t *reginfo2 = PDC_find_id(to_reg, pc);
    PDC_region_info_t *reg1 = (PDC_region_info_t *)(reginfo1->obj_ptr);
    PDC_region_info_t *reg2 = (PDC_region_info_t *)(reginfo2->obj_ptr);
    
    if(reg1->ndim != reg2->ndim || prop->ndim != reg1->ndim || prop->ndim != reg2->ndim)
        PGOTO_ERROR(FAIL, "cannot map between regions of different dimensions");
    // start mapping
    // state that there is mapping to other objects
    object1->mapping = 1;
    // Effectively calls server “subscribe” for updates for that region
    // Callback called on region update
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_unmap(pdcid_t obj_id) {
    // call to server
    // no more updates / unsubscribed from service
}

PDC_obj_info_t *PDCobj_get_info(pdcid_t obj_id, pdcid_t pdc) {
    PDC_obj_info_t *ret_value = NULL;
    PDC_obj_info_t *info =  NULL;
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc;
    PDC_id_info_t *obj = PDC_find_id(obj_id, pc);
    if(obj == NULL)
        PGOTO_ERROR(NULL, "cannot locate object");
    
    info = (PDC_obj_info_t *)(obj->obj_ptr);
    ret_value = info;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_get_info() */

perr_t PDCobj_release(pdcid_t obj_id, pdcid_t pdc_id) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    PDC_CLASS_t *pc = (PDC_CLASS_t *)pdc_id;
    PDC_id_info_t *info = PDC_find_id(obj_id, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    PDC_obj_info_t *object = (PDC_obj_info_t *)(info->obj_ptr);
    pdcid_t propid = object->obj_prop;
    info = PDC_find_id(propid, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    PDC_obj_prop_t *prop = (PDC_obj_prop_t *)(info->obj_ptr);
    prop->buf = NULL;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_update_region(pdcid_t obj_id, pdcid_t region) {
    // call to the server
}

perr_t PDCobj_invalidate_region(pdcid_t obj_id, pdcid_t region) {
    // call to the server
}

perr_t PDCobj_sync(pdcid_t obj_id) {
    // call to the server
}

perr_t PDCprop_set_obj_loci_prop(pdcid_t obj_create_prop, PDC_loci locus, PDC_transform A) {
}

perr_t PDCprop_set_obj_transform(pdcid_t obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus) {
}

obj_handle PDCview_iter_start(pdcid_t view_id) {
}
