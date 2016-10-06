#include <stdlib.h>
#include "pdc_interface.h"
#include "pdc_id_pkg.h"
#include "pdc_malloc.h"
#include "pdc_linkedlist.h"


/* Combine a Type number and an atom index into an atom */
#define PDCID_MAKE(g,i)   ((((pdcid_t)(g) & TYPE_MASK) << ID_BITS) |          \
                          ((pdcid_t)(i) & ID_MASK))

/* Local typedefs */

/* Atom information structure used */
typedef struct PDC_id_info {
    pdcid_t     id;             /* ID for this info                         */
    unsigned    count;          /* ref. count for this atom                 */
    unsigned    app_count;      /* ref. count of application visible atoms  */
    const void  *obj_ptr;       /* pointer associated with the atom         */
    PDC_LIST_ENTRY(PDC_id_info) entry;
} PDC_id_info_t;

/* ID type structure used */
typedef struct {
    const                       PDCID_class_t *cls;   /* Pointer to ID class                        */
    unsigned                    init_count;           /* # of times this type has been initialized  */
    unsigned                    id_count;             /* Current number of IDs held                 */
    pdcid_t                     nextid;               /* ID to use for the next atom                */
    PDC_LIST_HEAD(PDC_id_info)  ids;                  /* Head of list of IDs                        */
} PDC_id_type_t;

 
/* Variable to keep track of the number of types allocated.  Its value is the */
/* next type ID to be handed out, so it is always one greater than the number */
/* of types. */
/* Starts at 1 instead of 0 because it makes trace output look nicer.  If more */
/* types (or IDs within a type) are needed, adjust TYPE_BITS in pdc_id_pkg.h   */
/* and/or increase size of pdcid_t */
static PDC_type_t PDC_next_type = (PDC_type_t)PDC_NTYPES;

static PDC_id_type_t *PDC_id_type_list_g[PDC_MAX_NUM_TYPES];



pdcid_t PDCid_register(PDC_type_t type, const void *object) {
    pdcid_t ret_value = PDC_INVALID_ID;  /* Return value */
    if(!PDCID_IS_LIB_TYPE(type))
        PGOTO_ERROR(FAIL, "cannot call public function on library type")
    ret_value = PDC_id_register(type, object, TRUE);
done:
    return ret_value;
}


pdcid_t PDC_id_register(PDC_type_t type, const void *object, pbool_t app_ref) {
    PDC_id_type_t     *type_ptr;           /* ptr to the type               */
    PDC_id_info_t     *id_ptr;             /* ptr to the new ID information */
    pdcid_t           new_id;              /* new ID                        */
    pdcid_t           ret_value = SUCCEED; /* return value                  */

    /* Check arguments */
    if(type <= PDC_BADID || type >= PDC_next_type)
        PGOTO_ERROR(FAIL, "invalid type number");
    type_ptr = PDC_id_type_list_g[type];                  // work on init and malloc for different types space
    if(NULL == type_ptr || type_ptr->init_count <= 0)
        PGOTO_ERROR(FAIL, "invalid type");
    if(NULL == (id_ptr = PDC_MALLOC(PDC_id_info_t)));
        PGOTO_ERROR(FAIL, "memory allocation failed");

    /* Create the struct & it's ID */
    new_id = PDCID_MAKE(type, type_ptr->nextid);
    id_ptr->id = new_id;
    id_ptr->count = 1;                /*initial reference count*/
    id_ptr->app_count = !!app_ref;
    id_ptr->obj_ptr = object;

    /* Insert into the type */
    PDC_LIST_INSERT_HEAD(&type_ptr->ids, id_ptr, entry);   // H5SL_insert(type_ptr->ids, id_ptr, &id_ptr->id)
    type_ptr->id_count++;
    type_ptr->nextid++;

/*
 * Sanity check for the 'nextid' getting too large and wrapping around.
 */
    assert(type_ptr->nextid <= ID_MASK);

/* Set return value */
    ret_value = new_id;

done:
    return ret_value;
}

