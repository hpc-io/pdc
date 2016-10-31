#ifndef _pdc_interface_H
#define _pdc_interface_H

#include <stdatomic.h>
#include "pdc_public.h"
#include "pdc_private.h"
#include "pdc_error.h"
#include "pdc_linkedlist.h"
#include "pdc_id_pkg.h"

#define PDC_INVALID_ID         (-1)

#define PDCID_IS_LIB_TYPE(type) (type > 0 && type < PDC_NTYPES)

typedef _Atomic unsigned int pdc_cnt_t;

/*
 * Function for freeing objects. This function will be called with an object
 * ID type number and a pointer to the object. The function should free the
 * object and return non-negative to indicate that the object
 * can be removed from the ID type. If the function returns negative
 * (failure) then the object will remain in the ID type.
 */
typedef perr_t (*PDC_free_t)(void*);

typedef enum {
    PDC_BADID        = -1,  /* invalid Type                                */
    PDC_PROPERTY     = 1,   /* type ID for PDC property                    */
    PDC_CONT_PROP    = 2,   /* type ID for container property              */
    PDC_OBJ_PROP     = 3,   /* type ID for object property                 */
    PDC_CONT         = 4,   /* type ID for container                       */
    PDC_OBJ          = 5,   /* type ID for object                          */
    PDC_NTYPES       = 6    /* number of library types, MUST BE LAST!      */
} PDC_type_t;

typedef struct {
    PDC_type_t type_id;         /* Class ID for the type */
    unsigned flags;             /* Class behavior flags  */
    unsigned reserved;          /* Number of reserved IDs for this type */
                                /* [A specific number of type entries may be
                                 * reserved to enable "constant" values to be
                                 * handed out which are valid IDs in the type,
                                 * but which do not map to any data structures
                                 * and are not allocated dynamically later.]
                                 */
    PDC_free_t free_func;       /* Free function for object's of this type */
} PDCID_class_t;

/* Atom information structure used */
typedef struct PDC_id_info {
    pdcid_t     id;             /* ID for this info                         */
    pdc_cnt_t   count;          /* ref. count for this atom                 */
    const void  *obj_ptr;       /* pointer associated with the atom         */
    PDC_LIST_ENTRY(PDC_id_info) entry;
} PDC_id_info_t;

/* ID type structure used */
typedef struct PDC_id_type{
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

typedef struct PDC_CLASS_t {
    PDC_id_type_t *PDC_id_type_list_g[PDC_MAX_NUM_TYPES];
} PDC_CLASS_t;

/*-------------------------------------------------------------------------
 * Function:    PDC_register_type
 *
 * Purpose: Creates a new type of ID's to give out.
 *          The class is initialized or its reference count is incremented
 *          (if it is already initialized).
 *
 * Return:  Success:    Type ID of the new type
 *          Failure:    PDC_BADID
 *
 *-------------------------------------------------------------------------
 */
perr_t PDC_register_type(const PDCID_class_t *cls, PDC_CLASS_t *pc);


/*-------------------------------------------------------------------------
 * Function:    PDCid_register
 *
 * Purpose: Public interface to PDCid_register.
 *
 * Return:  Success:    New object id.
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
pdcid_t PDCid_register(PDC_type_t type, const void *object);

/*-------------------------------------------------------------------------
 * Function:    PDC_id_register
 *
 * Purpose: Registers an OBJECT in a TYPE and returns an ID for it.
 *          This routine does _not_ check for unique-ness of the objects,
 *          if you register an object twice, you will get two different
 *          IDs for it.  This routine does make certain that each ID in a
 *          type is unique.  IDs are created by getting a unique number
 *          for the type the ID is in and incorporating the type into
 *          the ID which is returned to the user.
 *
 * Return:  Success:    New object id.
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
pdcid_t PDC_id_register(PDC_type_t type, const void *object, pdcid_t pdc);

/*-------------------------------------------------------------------------
 * Function:    PDC_dec_ref
 *
 * Purpose: Decrements the number of references outstanding for an ID.
 *          This will fail if the type is not a reference counted type.
 *          The ID type's 'free' function will be called for the ID
 *          if the reference count for the ID reaches 0 and a free
 *          function has been defined at type creation time.
 *
 * Return:  Success:    New reference count.
 *
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int PDC_dec_ref(pdcid_t id, pdcid_t pdc);

/*-------------------------------------------------------------------------
 * Function:    PDC_id_list_null
 *
 * Purpose: Check if PDC_type_t id list is empty
 *
 * Return:  Success:   List is empty
 *
 *          Failure:   Negative
 *
 * -------------------------------------------------------------------------
 */
perr_t PDC_id_list_null(PDC_type_t type, pdcid_t pdc);


/*-------------------------------------------------------------------------
 * Function:    PDC_id_list_clear
 *
 * Purpose: Clear the list of a PDC_type_t type
 *
 * Return:  Success:   List is cleared
 *
 *          Failure:   Problem to clear the list
 *
 * -------------------------------------------------------------------------
 */
perr_t PDC_id_list_clear(PDC_type_t type, pdcid_t pdc);

/*-------------------------------------------------------------------------
 * Function:    PDC_destroy_type
 *
 * Purpose:    Destroy ID types
 *
 * Return:  Success:   Free type successfully
 *
 *          Failure:   Fail to free PDC type
 *
 * -------------------------------------------------------------------------
 */
perr_t PDC_destroy_type(PDC_type_t type, pdcid_t pdc);

pdcid_t PDC_find_byname(PDC_type_t type, const char *byname, pdcid_t pdc);

#endif
