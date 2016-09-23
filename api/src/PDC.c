#include "PDC.h"


pid_t PDCinit(PDC_property prop) {
}

pid_t PDCtype_create(PDC_STRUCT pdc_struct) {
}

perr_t PDCtype_struct_field_insert(pid_t type_id, const char *name, uint64_t offset, PDC_var_type var_type) {
}

perr_t PDCget_loci_count(pid_t pdc_id, pid_t *nloci) {
}

perr_t PDCget_loci_info(pid_t pdc_id, pid_t n, PDC_loci_info_t info) {
}

pid_t PDCprop_create(PDC_prop_type type) {
}

perr_t PDCprop_close(pid_t prop_id) {
}

pid_t PDCcont_create(pid_t pdc_id, const char *cont_name, pid_t cont_create_prop) {
}

pid_t PDCcont_open(pid_t pdc_id, const char *cont_name) {
}

cont_handle PDCcont_iter_start(pid_t pdc_id) {
}

bool PDCcont_iter_null(cont_handle chandle) {
}

perr_t PDCcont_iter_next(cont_handle chandle) {
}

perr_t PDCcont_iter_get_info(cont_handle chandle, PDC_cont_info_t *info) {
}

// perr_t PDCcont_persist(pid_t cont_id){}

perr_t PDCprop_set_cont_lifetime(pid_t cont_create_prop, PDC_lifetime cont_lifetime) {
}

perr_t PDCcont_close(pid_t cont_id) {
}

pid_t PDCobj_create(pid_t cont_id, const char *obj_name, pid_t obj_create_prop) {
}

perr_t PDCprop_set_obj_lifetime(pid_t obj_create_prop, PDC_lifetime obj_lifetime) {
}

perr_t PDCprop_set_obj_dims(pid_t obj_create_prop, PDC_int_t ndim, uint64_t *dims) {
}

perr_t PDCprop_set_obj_type(pid_t obj_create_prop, PDC_var_type type) {
}

perr_t PDCprop_set_obj_buf(pid_t obj_create_prop, void *buf) {
}

perr_t PDCobj_buf_retrieve(pid_t obj_id, void **buf, PDC_region region) {
}

pid_t PDCobj_open(pid_t cont_id, const char *obj_name) {
}

obj_handle PDCobj_iter_start(pid_t cont_id) {
}

bool PDCobj_iter_null(obj_handle ohandle) {
}

perr_t PDCobj_iter_next(obj_handle ohandle) {
}

perr_t PDCobj_iter_get_info(obj_handle ohandle, PDC_obj_info_t *info) {
}

pid_t PDC_query_create(pid_t pdc_id, PDC_query_type_t query_type, PDC_query_op_t query_op, ...) {
}

pid_t PDC_query_combine(pid_t query1_id, PDC_com_op_mode_t combine_op, pid_t query2_id) {
}

obj_handle PDCview_iter_start(pid_t view_id) {
}

perr_t PDCobj_buf_map(pid_t obj_id, void *buf, PDC_region region) {
}

perr_t PDCobj_map(pid_t a, PDC_region xregion, pid_t b, PDC_region yregion) {
}

perr_t PDCobj_unmap(pid_t obj_id) {
}

perr_t PDCobj_release(pid_t obj_id) {
}

perr_t PDCobj_update_region(pid_t obj_id, PDC_region region) {
}

perr_t PDCobj_invalidate_region(pid_t obj_id, PDC_region region) {
}

perr_t PDCobj_sync(pid_t obj_id) {
}

perr_t PDCobj_close(pid_t obj_id) {
}

perr_t PDCprop_set_obj_loci_prop(pid_t obj_create_prop, PDC_loci locus, PDC_major_type major) {
}

perr_t PDCprop_set_obj_transform(pid_t obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus) {
}
