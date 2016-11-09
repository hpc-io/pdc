#include "pdc_obj.h"
#include "pdc_malloc.h"

#include "pdc_client_server_common.h"
#include "pdc_client_connect.h"

static perr_t PDCobj__close(PDC_obj_t *op);

/* PDC object ID class */
static const PDCID_class_t PDC_OBJ_CLS[1] = {{
    PDC_OBJ,                            /* ID class value */
    0,                                  /* Class flags */
    0,                                  /* # of reserved IDs for class */
    (PDC_free_t)PDCobj__close           /* Callback routine for closing objects of this class */
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

pdcid_t PDCobj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop){
    FUNC_ENTER(NULL);

    pdcid_t obj_id;
    obj_id = PDC_Client_send_name_recv_id(obj_name);

done:
    FUNC_LEAVE(obj_id);
}

perr_t PDC_obj_list_null(pdcid_t pdc) {
    perr_t ret_value = SUCCEED;   /* Return value */
    
    FUNC_ENTER(NULL);
    // list is not empty
    int nelemts = PDC_id_list_null(PDC_OBJ, pdc);
    if(PDC_id_list_null(PDC_OBJ, pdc) > 0) {
        printf("%d element(s) in the object list will be automatically closed by PDC_close()\n", nelemts);
        if(PDC_id_list_clear(PDC_OBJ, pdc) < 0)
            PGOTO_ERROR(FAIL, "fail to clear object list");
    }
    
done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCobj__close(PDC_obj_t *op) {
    perr_t ret_value = SUCCEED;         /* Return value */
    
    FUNC_ENTER(NULL);
    
    op = PDC_FREE(PDC_obj_t, op);
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

perr_t PDCobj_end(pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(PDC_destroy_type(PDC_OBJ, pdc) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy object interface");
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCobj_end() */

pdcid_t PDCobj_open(pdcid_t cont_id, const char *obj_name) {
}

obj_handle PDCobj_iter_start(pdcid_t cont_id) {
}

pbool_t PDCobj_iter_null(obj_handle ohandle) {
}

perr_t PDCobj_iter_next(obj_handle ohandle) {
}

PDC_obj_info_t *  PDCobj_iter_get_info(obj_handle ohandle) {
}

perr_t PDCprop_set_obj_lifetime(pdcid_t obj_create_prop, PDC_lifetime obj_lifetime) {
}

perr_t PDCprop_set_obj_dims(pdcid_t obj_create_prop, PDC_int_t ndim, uint64_t *dims) {
}

perr_t PDCprop_set_obj_type(pdcid_t obj_create_prop, PDC_var_type_t type) {
}

perr_t PDCprop_set_obj_buf(pdcid_t obj_create_prop, void *buf) {
}

perr_t PDCobj_buf_retrieve(pdcid_t obj_id, void **buf, PDC_region region) {
}

obj_handle PDCview_iter_start(pdcid_t view_id) {
}

perr_t PDCobj_buf_map(pdcid_t obj_id, void *buf, PDC_region region) {
}

perr_t PDCobj_map(pdcid_t a, PDC_region xregion, pdcid_t b, PDC_region yregion) {
}

perr_t PDCobj_unmap(pdcid_t obj_id) {
}

perr_t PDCobj_release(pdcid_t obj_id) {
}

perr_t PDCobj_update_region(pdcid_t obj_id, PDC_region region) {
}

perr_t PDCobj_invalidate_region(pdcid_t obj_id, PDC_region region) {
}

perr_t PDCobj_sync(pdcid_t obj_id) {
}

perr_t PDCprop_set_obj_loci_prop(pdcid_t obj_create_prop, PDC_loci locus, PDC_transform A) {
}

perr_t PDCprop_set_obj_transform(pdcid_t obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus) {
}

