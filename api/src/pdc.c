#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "pdc.h"
#include "pdc_private.h"
#include "pdc_malloc.h"
#include "pdc_interface.h"
#include "pdc_prop_pkg.h"
#include "pdc_prop.h"


struct PDC_property {
    pdcid_t id;
};

struct PDC_container {
    const char *name;
};


pdcid_t PDCinit(PDC_prop property) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(PDC_prop_init() < 0)
        PGOTO_ERROR(FAIL, "PDC property init error");
done:
    FUNC_LEAVE(ret_value);
} /* end PDCinit() */

pdcid_t PDCtype_create(PDC_STRUCT pdc_struct) {
}

perr_t PDCtype_struct_field_insert(pdcid_t type_id, const char *name, uint64_t offset, PDC_var_type_t var_type) {
}

perr_t PDCget_loci_count(pdcid_t pdc_id, pdcid_t *nloci) {
}

perr_t PDCget_loci_info(pdcid_t pdc_id, pdcid_t n, PDC_loci_info_t *info) {
}

pdcid_t PDCcont_create(pdcid_t pdc_id, const char *cont_name, pdcid_t cont_create_prop) {
    pdcid_t ret_value = SUCCEED;
    struct PDC_container *p = NULL;

    FUNC_ENTER(NULL);

    p = PDC_MALLOC(struct PDC_container);
    if(!p)
        PGOTO_ERROR(FAIL,"memory allocation failed\n");
    p->name = cont_name;
done:
    FUNC_LEAVE(ret_value);
}

pdcid_t PDCcont_open(pdcid_t pdc_id, const char *cont_name) {
}

cont_handle PDCcont_iter_start(pdcid_t pdc_id) {
}

pbool_t PDCcont_iter_null(cont_handle chandle) {
}

perr_t PDCcont_iter_next(cont_handle chandle) {
}

PDC_cont_info_t * PDCcont_iter_get_info(cont_handle chandle) {
}

// perr_t PDCcont_persist(pdcid_t cont_id){}

perr_t PDCprop_set_cont_lifetime(pdcid_t cont_create_prop, PDC_lifetime cont_lifetime) {
}

perr_t PDCcont_close(pdcid_t cont_id) {
}

pdcid_t PDCobj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop) {
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

pdcid_t PDC_query_create(pdcid_t pdc_id, PDC_query_type_t query_type, PDC_query_op_t query_op, ...) {
}

pdcid_t PDC_query_combine(pdcid_t query1_id, PDC_com_op_mode_t combine_op, pdcid_t query2_id) {
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

perr_t PDCobj_close(pdcid_t obj_id) {
}

perr_t PDCprop_set_obj_loci_prop(pdcid_t obj_create_prop, PDC_loci locus, PDC_transform A) {
}

perr_t PDCprop_set_obj_transform(pdcid_t obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus) {
}
