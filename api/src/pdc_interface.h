#ifndef _pdc_interface_H
#define _pdc_interface_H

#include "pdc_private.h"
#include "pdc_error.h"


#define PDC_INVALID_ID         (-1)

#define PDCID_IS_LIB_TYPE(type) (type > 0 && type < PDC_NTYPES)

/*
 * Function for freeing objects. This function will be called with an object
 * ID type number and a pointer to the object. The function should free the
 * object and return non-negative to indicate that the object
 * can be removed from the ID type. If the function returns negative
 * (failure) then the object will remain in the ID type.
 */
typedef perr_t (*PDC_free_t)(void*);

typedef enum {
    PDC_BADID        = -1,  /*invalid Type                                */
    PDC_PDC          = 0,   /*type ID for PDC                             */
    PDC_PROPERTY     = 1,   /*type ID for PDC property                    */
    PDC_CONT_PROP    = 2,   /*type ID for container property              */
    PDC_OBJ_PROP     = 3,   /*type ID for object property                 */
    PDC_CONT         = 4,   /*type ID for container                       */
    PDC_OBJ          = 5,   /*type ID for object                          */
    PDC_NTYPES       = 6   /*number of library types, MUST BE LAST!       */
} PDC_type_t;

typedef struct {
    PDC_type_t type_id;        /* Class ID for the type */
    unsigned flags;             /* Class behavior flags */
    unsigned reserved;          /* Number of reserved IDs for this type */
                                /* [A specific number of type entries may be
                                 * reserved to enable "constant" values to be
                                 * handed out which are valid IDs in the type,
                                 * but which do not map to any data structures
                                 * and are not allocated dynamically later.]
                                 */
    PDC_free_t free_func;       /* Free function for object's of this type */
} PDCID_class_t;

/*-------------------------------------------------------------------------
 * Function:    PDC_register_type
 *
 * Purpose: Creates a new type of ID's to give out.
 *      The class is initialized or its reference count is incremented
 *              (if it is already initialized).
 *
 * Return:  Success:    Type ID of the new type
 *          Failure:    H5I_BADID
 *
 *-------------------------------------------------------------------------
 */
perr_t PDC_register_type(const PDCID_class_t *cls);


/*-------------------------------------------------------------------------
 * Function:    PDCid_register
 *
 * Purpose: Public interface to H5I_register.
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
 *      This routine does _not_ check for unique-ness of the objects,
 *      if you register an object twice, you will get two different
 *      IDs for it.  This routine does make certain that each ID in a
 *      type is unique.  IDs are created by getting a unique number
 *      for the type the ID is in and incorporating the type into
 *      the ID which is returned to the user.
 *
 * Return:  Success:    New object id.
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
pdcid_t PDC_id_register(PDC_type_t type, const void *object);


/*-------------------------------------------------------------------------
 * Function:    pdc_id_remove
 *
 * Purpose: Removes the specified ID from its type.
 *
 * Return:  Success:    A pointer to the object that was removed.
 *          Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *pdc_id_remove(pdcid_t id);


/*-------------------------------------------------------------------------
 * Function:    PDC_dec_ref
 *
 * Purpose: Decrements the number of references outstanding for an ID.
 *      This will fail if the type is not a reference counted type.
 *      The ID type's 'free' function will be called for the ID
 *      if the reference count for the ID reaches 0 and a free
 *      function has been defined at type creation time.
 *
 * Return:  Success:    New reference count.
 *
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int PDC_dec_ref(pdcid_t id);

#endif
