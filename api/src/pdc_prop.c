#include <string.h>
#include "pdc_prop.h"
#include "pdc_interface.h"
#include "pdc_malloc.h"
#include "pdc_prop_pkg.h"

static perr_t PDCprop__close(PDC_cont_prop_t *cp);

/* PDC container property ID class */
static const PDCID_class_t PDC_CONT_PROP_CLS[1] = {{
    PDC_CONT_PROP,                  /* ID class value */
    0,                              /* Class flags */
    0,                              /* # of reserved IDs for class */
    (PDC_free_t)PDCprop__close      /* Callback routine for closing objects of this class */
}};

/* PDC object property ID class */
static const PDCID_class_t PDC_OBJ_PROP_CLS[1] = {{
    PDC_OBJ_PROP,                   /* ID class value */
    0,                              /* Class flags */
    0,                              /* # of reserved IDs for class */
    (PDC_free_t)PDCprop__close      /* Callback routine for closing objects of this class */
}};

perr_t PDC_prop_init() {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the container property IDs */
    if(PDC_register_type(PDC_CONT_PROP_CLS) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize container property interface");

    if(PDC_register_type(PDC_OBJ_PROP_CLS) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize object property interface");

done:
    FUNC_LEAVE(ret_value);
} /* end PDC_prop_init() */

pdcid_t PDCprop_create(PDC_prop_type type) {
    pdcid_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (type == PDC_CONT_CREATE) {
        PDC_cont_prop_t *p = NULL;
        p = PDC_MALLOC(PDC_cont_prop_t);
        if(!p)
            PGOTO_ERROR(FAIL, "memory allocation failed\n");
        pdcid_t new_id_c = PDC_id_register(PDC_CONT_PROP, p);
        ret_value = new_id_c;
    }
    if(type == PDC_OBJ_CREATE) {
        PDC_obj_prop_t *q = NULL;
        q = PDC_MALLOC(PDC_obj_prop_t);
      if(!q)
          PGOTO_ERROR(FAIL, "memory allocation failed\n");
        pdcid_t new_id_o = PDC_id_register(PDC_OBJ_PROP, q);
        ret_value = new_id_o;
    }
done:
    FUNC_LEAVE(ret_value);
} /* end PDCprop_create() */

static perr_t PDCprop__close(PDC_cont_prop_t *cp) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    cp = PDC_FREE(PDC_cont_prop_t, cp);
done:
    FUNC_LEAVE(ret_value);
} /* end PDCprop__close() */

perr_t PDCprop_close(pdcid_t id) {
    perr_t ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER(NULL);

    /* When the reference count reaches zero the resources are freed */
    if(PDC_dec_ref(id) < 0)
        PGOTO_ERROR(FAIL, "problem freeing id");
done:
    FUNC_LEAVE(ret_value);
} /* end PDCprop_close() */
