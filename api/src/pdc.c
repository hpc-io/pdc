#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "pdc.h"
#include "pdc_private.h"
#include "pdc_malloc.h"
#include "pdc_interface.h"

static PDC_CLASS_t *PDC__class_create() {
    PDC_CLASS_t *ret_value = NULL;         /* Return value */

    FUNC_ENTER(NULL);

    PDC_CLASS_t *pc;
    if(NULL == (pc = PDC_CALLOC(PDC_CLASS_t)))
        PGOTO_ERROR(NULL, "create pdc class: memory allocation failed"); 
    ret_value = pc;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDC__create_class() */

pdcid_t PDC_init(PDC_prop_t property) {
    pdcid_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);
    PDC_CLASS_t *pc = PDC__class_create();
    if(pc == NULL)
	    PGOTO_ERROR(FAIL, "fail to allocate pdc type");

    if(PDCprop_init(pc) < 0)
        PGOTO_ERROR(FAIL, "PDC property init error");
    if(PDCcont_init(pc) < 0)
        PGOTO_ERROR(FAIL, "PDC container init error");

    // create pdc id
    pdcid_t pdcid = (pdcid_t)pc;
    ret_value = pdcid;
done:
    FUNC_LEAVE(ret_value);
} /* end of PDC_init() */

perr_t PDC_close(pdcid_t pdcid) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    // check every list before closing
    // container property
    if(PDC_prop_cont_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close container property");
    // object property
    if(PDC_prop_obj_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close object property");
    // container
    if(PDC_cont_list_null(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to close container");

    PDC_CLASS_t *pc = (PDC_CLASS_t *) pdcid;
    if(pc == NULL)
        PGOTO_ERROR(FAIL, "PDC init fails");
    if(PDCprop_end(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to destroy property");
    if(PDCcont_end(pdcid) < 0)
        PGOTO_ERROR(FAIL, "fail to destroy container");
    pc = PDC_FREE(PDC_CLASS_t, pc);
done:
    FUNC_LEAVE(ret_value);
} /* end of PDC_close() */

pdcid_t PDCtype_create(PDC_STRUCT pdc_struct) {
}

perr_t PDCtype_struct_field_insert(pdcid_t type_id, const char *name, uint64_t offset, PDC_var_type_t var_type) {
}

perr_t PDCget_loci_count(pdcid_t pdc_id, pdcid_t *nloci) {
}

perr_t PDCget_loci_info(pdcid_t pdc_id, pdcid_t n, PDC_loci_info_t *info) {
}

// perr_t PDCcont_persist(pdcid_t cont_id){}

perr_t PDCprop_set_cont_lifetime(pdcid_t cont_create_prop, PDC_lifetime cont_lifetime) {
}

pdcid_t PDC_query_create(pdcid_t pdc_id, PDC_query_type_t query_type, PDC_query_op_t query_op, ...) {
}

pdcid_t PDC_query_combine(pdcid_t query1_id, PDC_com_op_mode_t combine_op, pdcid_t query2_id) {
}

