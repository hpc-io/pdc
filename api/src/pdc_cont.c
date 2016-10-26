#include "pdc_cont.h"
#include "pdc_malloc.h"

static perr_t PDCcont__close(PDC_cont_t *cp);

/* PDC container property ID class */
static const PDCID_class_t PDC_CONT_CLS[1] = {{
    PDC_CONT,                           /* ID class value */
    0,                                  /* Class flags */
    0,                                  /* # of reserved IDs for class */
    (PDC_free_t)PDCcont__close          /* Callback routine for closing objects of this class */
}};

perr_t PDCcont_init(PDC_CLASS_t *pc) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    /* Initialize the atom group for the container IDs */
    if(PDC_register_type(PDC_CONT_CLS, pc) < 0)
        PGOTO_ERROR(FAIL, "unable to initialize container interface");

done:
    FUNC_LEAVE(ret_value); 
} /* end PDCcont_init() */

static perr_t PDCcont__close(PDC_cont_t *cp) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    cp = PDC_FREE(PDC_cont_t, cp);
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCcont__close() */

perr_t PDCcont_end(pdcid_t pdc) {
    perr_t ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER(NULL);

    if(PDC_destroy_type(PDC_CONT, pdc) < 0)
        PGOTO_ERROR(FAIL, "unable to destroy container interface");
done:
    FUNC_LEAVE(ret_value);
} /* end of PDCcont_end() */
