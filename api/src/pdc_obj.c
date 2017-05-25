#include "pdc_obj.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"
#include "pdc_client_server_common.h"

static perr_t PDCobj__close(struct PDC_obj_info *op);

static perr_t PDCregion__close(struct PDC_region_info *op);

/* PDC object ID class */
/*
static const PDCID_class_t PDC_OBJ_CLS[1] = {{
    PDC_OBJ,
    0,
    0,
    (PDC_free_t)PDCobj__close
}};
*/
/* PDC region ID class */
/*
static const PDCID_class_t PDC_REGION_CLS[1] = {{
    PDC_REGION,
    0,
    0,
    (PDC_free_t)PDCregion__close        
}};
*/

perr_t PDCobj_init(PDC_CLASS_t *pc)
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the object IDs */
    if(PDC_register_type(PDC_OBJ, (PDC_free_t)PDCobj__close, pc) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize object interface");

done:
    FUNC_LEAVE(ret_value);
} /* end PDCobj_init() */

perr_t PDCregion_init(PDC_CLASS_t *pc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* Initialize the atom group for the region IDs */
    if(PDC_register_type(PDC_REGION, (PDC_free_t)PDCregion__close, pc) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize region interface");
    
done:
    FUNC_LEAVE(ret_value);
} /* end PDCregion_init() */


pdcid_t PDCobj_create(pdcid_t pdc, pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop)
{
    pdcid_t ret_value = SUCCEED;
    struct PDC_obj_info *p = NULL;
    PDC_CLASS_t *pc;
    perr_t ret;
    
    FUNC_ENTER(NULL);
    
    p = PDC_MALLOC(struct PDC_obj_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC object memory allocation failed\n");
    p->name = strdup(obj_name);
    p->cont = cont_id;
    p->pdc = pdc;
    p->obj_prop = obj_create_prop;
//    p->mapping = 0;
    // will contact server to get ID
    pc = (PDC_CLASS_t *)pdc;
    p->local_id = PDC_id_register(PDC_OBJ, p, pdc);

    ret = PDC_Client_send_name_recv_id(pdc, cont_id, obj_name, obj_create_prop, &(p->meta_id));

    ret_value = p->local_id;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_obj_list_null(pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;   /* Return value */
    int nelemts;
    
    FUNC_ENTER(NULL);
    
    // list is not empty
    nelemts = PDC_id_list_null(PDC_OBJ, pdc);
    if(nelemts > 0) {
        /* printf("%d element(s) in the object list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_OBJ, pdc) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDC_region_list_null(pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;   /* Return value */
    int nelemts;
    
    FUNC_ENTER(NULL);
    // list is not empty
    nelemts = PDC_id_list_null(PDC_REGION, pdc);
    if(nelemts > 0) {
        /* printf("%d element(s) in the region list will be automatically closed by PDC_close()\n", nelemts); */
        if(PDC_id_list_clear(PDC_REGION, pdc) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj__close(struct PDC_obj_info *op)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    op = PDC_FREE(struct PDC_obj_info, op);
    
    FUNC_LEAVE(ret_value);
}

perr_t PDCregion__close(struct PDC_region_info *op)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    free(op->size);
    op = PDC_FREE(struct PDC_region_info, op);
    
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_close(pdcid_t obj_id, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(obj_id, pdc) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCregion_close(pdcid_t region_id, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    
    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(region_id, pdc) < 0)
        PGOTO_ERROR(FAIL, "object: problem of freeing id");
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_end(pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(PDC_destroy_type(PDC_OBJ, pdc) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy object interface");
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_end() */

perr_t PDCregion_end(pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    if(PDC_destroy_type(PDC_REGION, pdc) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy region interface");
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCregion_end() */

pdcid_t PDCobj_open(pdcid_t cont_id, const char *obj_name, pdcid_t pdc)
{
    pdcid_t ret_value = SUCCEED;
    pdcid_t obj_id;
    
    FUNC_ENTER(NULL);
    
    // should wait for response from server
    // look up in the list for now
    obj_id = PDC_find_byname(PDC_OBJ, obj_name, pdc);
    if(obj_id <= 0)
        PGOTO_ERROR(FAIL, "cannot locate object");
    pdc_inc_ref(obj_id, pdc);
    ret_value = obj_id;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_open() */

obj_handle *PDCobj_iter_start(pdcid_t cont_id, pdcid_t pdc_id)
{
    obj_handle *ret_value = NULL;
    obj_handle *objhl = NULL;
    PDC_CLASS_t *pc;
    struct PDC_id_type *type_ptr;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc_id;
    type_ptr  = (pc->PDC_id_type_list_g)[PDC_OBJ];
    if(type_ptr == NULL)
        PGOTO_ERROR(NULL, "object list is empty");
    objhl = (&type_ptr->ids)->head;
    while(objhl!=NULL && ((struct PDC_obj_info *)(objhl->obj_ptr))->cont!=cont_id) {
        objhl = PDC_LIST_NEXT(objhl, entry);
    }
    ret_value = objhl;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_start() */

pbool_t PDCobj_iter_null(obj_handle *ohandle)
{
    pbool_t ret_value = FALSE;
    
    FUNC_ENTER(NULL);
    
    if(ohandle == NULL)
        ret_value = TRUE;
    
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_null() */

obj_handle *PDCobj_iter_next(obj_handle *ohandle, pdcid_t cont_id)
{
    obj_handle *ret_value = NULL;
    obj_handle *next = NULL;
    
    FUNC_ENTER(NULL);
    
    if(ohandle == NULL)
        PGOTO_ERROR(NULL, "no next object");
    next = PDC_LIST_NEXT(ohandle, entry);
    while(next!=NULL && ((struct PDC_obj_info *)(next->obj_ptr))->cont!= cont_id) {
        next = PDC_LIST_NEXT(ohandle, entry);
    }
    ret_value = next;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_iter_next() */

struct PDC_obj_info *PDCobj_iter_get_info(obj_handle *ohandle)
{
    struct PDC_obj_info *ret_value = NULL;
    struct PDC_obj_info *info = NULL;
    
    FUNC_ENTER(NULL);
    info = (struct PDC_obj_info *)(ohandle->obj_ptr);
    if(info == NULL)
        PGOTO_ERROR(NULL, "PDC container info memory allocation failed");
    
    ret_value = info;
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_lifetime(pdcid_t obj_prop, PDC_lifetime obj_lifetime, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->obj_life = obj_lifetime;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->user_id = user_id;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->app_name = strdup(app_name);
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->time_step = time_step;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_data_loc(pdcid_t obj_prop, char *loc, pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->data_loc = strdup(loc);
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    ((struct PDC_obj_prop *)(info->obj_ptr))->tags = strdup(tags);
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    prop->ndim = ndim;
    prop->dims = (uint64_t *)malloc(ndim * sizeof(uint64_t));
    
    int i = 0;
    for(i=0; i<ndim; i++)
        (prop->dims)[i] = dims[i];
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_type(pdcid_t obj_prop, PDC_var_type_t type, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    prop->type = type;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf, pdcid_t pdc)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_prop, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    prop->buf = buf;
    
done:
    FUNC_LEAVE(ret_value);
}

void **PDCobj_buf_retrieve(pdcid_t obj_id, pdcid_t pdc)
{
    void **ret_value = NULL;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    struct PDC_obj_info *object;
    pdcid_t propid;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    info = PDC_find_id(obj_id, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object = (struct PDC_obj_info *)(info->obj_ptr);
    propid = object->obj_prop;
    info = PDC_find_id(propid, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    void **buffer = &(prop->buf);
    ret_value = buffer;
    
done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCregion_create(size_t ndims, uint64_t *offset, uint64_t *size, pdcid_t pdc_id)
{
    pdcid_t ret_value = SUCCEED;         /* Return value */
    struct PDC_region_info *p = NULL;
    pdcid_t new_id;
    
    FUNC_ENTER(NULL);
    
    p = PDC_MALLOC(struct PDC_region_info);
    if(!p)
        PGOTO_ERROR(FAIL,"PDC region memory allocation failed\n");
    p->ndim = ndims;
    p->offset = (uint64_t *)malloc(ndims * sizeof(uint64_t));
    p->size = (uint64_t *)malloc(ndims * sizeof(uint64_t));
    p->mapping = 0;
    p->local_id = 0;
    int i = 0;
    for(i=0; i<ndims; i++) {
        (p->offset)[i] = offset[i];
        (p->size)[i] = size[i];
    }
    // data type?
    new_id = PDC_id_register(PDC_REGION, p, pdc_id);
    p->local_id = new_id;
    ret_value = new_id;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_map(pdcid_t local_obj, pdcid_t local_reg, pdcid_t remote_obj, pdcid_t remote_reg, pdcid_t pdc_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    int i;
    PDC_CLASS_t *pc;
    struct PDC_id_info *objinfo1 = NULL, *objinfo2 = NULL;
    struct PDC_obj_info *obj1, *obj2;
    pdcid_t local_meta_id, remote_meta_id;
    pdcid_t propid1, propid2;
    struct PDC_id_info *propinfo1, *propinfo2;
    struct PDC_obj_prop *prop1, *prop2;
    PDC_var_type_t local_type, remote_type;
    void *local_data, *remote_data;
    struct PDC_id_info *reginfo1, *reginfo2;
    struct PDC_region_info *reg1, *reg2;
    size_t ndim;
    
    FUNC_ENTER(NULL);
    
    // PDC_CLASS_t defined in pdc_interface.h
    // PDC_obj_info defined in pdc_obj_pkg.h
    // PDC_region_info defined in pdc_obj_pkg.h
    
    pc = (PDC_CLASS_t *)pdc_id;
    
    objinfo1 = PDC_find_id(local_obj, pc);
    if(objinfo1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    obj1 = (struct PDC_obj_info *)(objinfo1->obj_ptr);
    local_meta_id = obj1->meta_id;
    propid1 = obj1->obj_prop;
    propinfo1 = PDC_find_id(propid1, pc);
    prop1 = (struct PDC_obj_prop *)(propinfo1->obj_ptr);
    local_type = prop1->type;
    local_data = prop1->buf;

    reginfo1 = PDC_find_id(local_reg, pc);
    reg1 = (struct PDC_region_info *)(reginfo1->obj_ptr);
    if(prop1->ndim != reg1->ndim)
        PGOTO_ERROR(FAIL, "local object dimension and region dimension does not match");
    for(i=0; i<prop1->ndim; i++) 
        if((prop1->dims)[i] < ((reg1->size)[i] + (reg1->offset)[i]))
            PGOTO_ERROR(FAIL, "local object region size error");
 
    objinfo2 = PDC_find_id(remote_obj, pc);
    if(objinfo2 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    obj2 = (struct PDC_obj_info *)(objinfo2->obj_ptr);
    remote_meta_id = obj2->meta_id;
    propid2 = obj2->obj_prop;
    propinfo2 = PDC_find_id(propid2, pc);
    prop2 = (struct PDC_obj_prop *)(propinfo2->obj_ptr);
    remote_type = prop2->type;
    remote_data = prop2->buf;
  
    reginfo2 = PDC_find_id(remote_reg, pc);
    reg2 = (struct PDC_region_info *)(reginfo2->obj_ptr);
    if(prop2->ndim != reg2->ndim)
        PGOTO_ERROR(FAIL, "remote object dimension and region dimension does not match");
    for(i=0; i<prop2->ndim; i++)
        if((prop2->dims)[i] < ((reg2->size)[i] + (reg2->offset)[i]))
            PGOTO_ERROR(FAIL, "remote object region size error");

    // TODO: assume ndim is the same
    ndim = reg1->ndim;
    
    //TODO: assume type is the same
    // start mapping
	ret_value = PDC_Client_send_region_map(local_meta_id, local_reg, remote_meta_id, remote_reg, ndim, prop1->dims, reg1->offset, reg1->size, local_type, local_data, reg2->offset, remote_type);

    if(ret_value == SUCCEED) {
        // state in origin obj that there is mapping
//        obj1->mapping = 1;
        reg1->mapping = 1;
        pdc_inc_ref(local_obj, pdc_id);
        pdc_inc_ref(local_reg, pdc_id);
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_buf_map(void *buf, pdcid_t from_reg, pdcid_t obj_id, pdcid_t to_reg, pdcid_t pdc_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info1;
    pdcid_t propid;
    struct PDC_obj_prop *prop;
    struct PDC_id_info *reginfo1, *reginfo2;
    struct PDC_region_info *reg1, *reg2;
    struct PDC_obj_info *object1;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc_id;
    info1 = PDC_find_id(obj_id, pc);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object1 = (struct PDC_obj_info *)(info1->obj_ptr);
    
    // check if ndim matches between object and region
    propid = object1->obj_prop;
    info1 = PDC_find_id(propid, pc);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info1->obj_ptr);
    
    reginfo1 = PDC_find_id(from_reg, pc);
    reginfo2 = PDC_find_id(to_reg, pc);
    reg1 = (struct PDC_region_info *)(reginfo1->obj_ptr);
    reg2 = (struct PDC_region_info *)(reginfo2->obj_ptr);
    
    // start mapping
    // state that there is mapping to other objects
//    object1->mapping = 1;
    reg1->mapping = 1;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj_unmap(pdcid_t obj_id, pdcid_t pdc_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info1;
    struct PDC_obj_info *object1;

    FUNC_ENTER(NULL);

    pc = (PDC_CLASS_t *)pdc_id;
    info1 = PDC_find_id(obj_id, pc);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object1 = (struct PDC_obj_info *)(info1->obj_ptr);
    ret_value = PDC_Client_send_object_unmap(object1->meta_id, pdc_id);
//    object1->mapping = 0;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCreg_unmap(pdcid_t obj_id, pdcid_t reg_id, pdcid_t pdc_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info1;
    struct PDC_obj_info *object1;
    struct PDC_region_info *reginfo;
    
    FUNC_ENTER(NULL);

    pc = (PDC_CLASS_t *)pdc_id;
    info1 = PDC_find_id(obj_id, pc);
    if(info1 == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object1 = (struct PDC_obj_info *)(info1->obj_ptr);
    ret_value = PDC_Client_send_region_unmap(object1->meta_id, reg_id, pdc_id);
    if(ret_value == SUCCEED) {
        PDC_dec_ref(obj_id, pdc_id);
        if(PDC_dec_ref(reg_id, pdc_id) == 1) {
            reginfo = PDCregion_get_info(reg_id, obj_id, pdc_id);
            reginfo->mapping = 0;
        }
    }
done:
    FUNC_LEAVE(ret_value);
}

struct PDC_obj_info *PDCobj_get_info(pdcid_t obj_id, pdcid_t pdc)
{
    struct PDC_obj_info *ret_value = NULL;
    struct PDC_obj_info *info =  NULL;
    PDC_CLASS_t *pc;
    struct PDC_id_info *obj;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc;
    obj = PDC_find_id(obj_id, pc);
    if(obj == NULL)
        PGOTO_ERROR(NULL, "cannot locate object");
    
    info = (struct PDC_obj_info *)(obj->obj_ptr);
    ret_value = info;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_get_info() */

struct PDC_region_info *PDCregion_get_info(pdcid_t reg_id, pdcid_t obj_id, pdcid_t pdc_id)
{
    struct PDC_region_info *ret_value = NULL;
    struct PDC_region_info *info =  NULL;
    PDC_CLASS_t *pc;
    struct PDC_id_info *region;

    FUNC_ENTER(NULL);

    pc = (PDC_CLASS_t *)pdc_id;
    region = PDC_find_id(reg_id, pc);
    if(region == NULL)
        PGOTO_ERROR(NULL, "cannot locate region");

    info = (struct PDC_region_info *)(region->obj_ptr);
    ret_value = info;
    
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCregion_get_info() */

perr_t PDCobj_release(pdcid_t obj_id, pdcid_t pdc_id)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    PDC_CLASS_t *pc;
    struct PDC_id_info *info;
    struct PDC_obj_info *object;
    pdcid_t propid;
    struct PDC_obj_prop *prop;
    
    FUNC_ENTER(NULL);
    
    pc = (PDC_CLASS_t *)pdc_id;
    info = PDC_find_id(obj_id, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object ID");
    object = (struct PDC_obj_info *)(info->obj_ptr);
    propid = object->obj_prop;
    info = PDC_find_id(propid, pc);
    if(info == NULL)
        PGOTO_ERROR(FAIL, "cannot locate object property ID");
    prop = (struct PDC_obj_prop *)(info->obj_ptr);
    prop->buf = NULL;
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCreg_obtain_lock(pdcid_t pdc_id, pdcid_t cont_id, pdcid_t obj_id, pdcid_t reg_id, PDC_access_t access_type, PDC_lock_mode_t lock_mode)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    pdcid_t meta_id;
    struct PDC_obj_info *object_info;
    struct PDC_region_info *region_info;
    pbool_t obtained;
    
    FUNC_ENTER(NULL);
    
    object_info = PDCobj_get_info(obj_id, pdc_id);
    meta_id = object_info->meta_id;
    region_info = PDCregion_get_info(reg_id, obj_id, pdc_id);
    
    ret_value = PDC_Client_obtain_region_lock(pdc_id, cont_id, meta_id, region_info, access_type, lock_mode, &obtained);

    FUNC_LEAVE(ret_value);
}

perr_t PDCreg_release_lock(pdcid_t pdc_id, pdcid_t cont_id, pdcid_t obj_id, pdcid_t reg_id, PDC_access_t access_type)
{
    perr_t ret_value = SUCCEED;         /* Return value */
    pbool_t released;
    pdcid_t meta_id;
    struct PDC_obj_info *object_info;
    struct PDC_region_info *region_info;
    
    FUNC_ENTER(NULL);
    
    object_info = PDCobj_get_info(obj_id, pdc_id);
    meta_id = object_info->meta_id;
    region_info = PDCregion_get_info(reg_id, obj_id, pdc_id);
    
    ret_value = PDC_Client_release_region_lock(pdc_id, cont_id, meta_id, region_info, access_type, &released);
    
    FUNC_LEAVE(ret_value);
}
