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
    PDC_CLASS        = 1,   /* type ID for PDC                             */
    PDC_CONT_PROP    = 2,   /* type ID for container property              */
    PDC_OBJ_PROP     = 3,   /* type ID for object property                 */
    PDC_CONT         = 4,   /* type ID for container                       */
    PDC_OBJ          = 5,   /* type ID for object                          */
    PDC_REGION       = 6,   /* type ID for region                          */
    PDC_NTYPES       = 7    /* number of library types, MUST BE LAST!      */
} PDC_type_t;

/*
typedef struct {
    PDC_type_t type_id;
    unsigned flags;
    unsigned reserved;
    PDC_free_t free_func;
} PDCID_class_t;
*/

/* Atom information structure used */
struct PDC_id_info {
    pdcid_t     id;             /* ID for this info                 */
    pdc_cnt_t   count;          /* ref. count for this atom         */
    const void  *obj_ptr;       /* pointer associated with the atom */
    PDC_LIST_ENTRY(PDC_id_info) entry;
};

/* ID type structure used */
struct PDC_id_type {
    PDC_free_t                  free_func;         /* Free function for object's of this type    */
    PDC_type_t                  type_id;           /* Class ID for the type                      */
//    const                     PDCID_class_t *cls;/* Pointer to ID class                        */
    unsigned                    init_count;        /* # of times this type has been initialized  */
    unsigned                    id_count;          /* Current number of IDs held                 */
    pdcid_t                     nextid;            /* ID to use for the next atom                */
    PDC_LIST_HEAD(PDC_id_info)  ids;               /* Head of list of IDs                        */
};

/* Variable to keep track of the number of types allocated.  Its value is the
 * next type ID to be handed out, so it is always one greater than the number
 * of types.
 * Starts at 1 instead of 0 because it makes trace output look nicer.  If more
 * types (or IDs within a type) are needed, adjust TYPE_BITS in pdc_id_pkg.h
 * and/or increase size of pdcid_t */
static PDC_type_t PDC_next_type = (PDC_type_t)PDC_NTYPES;

struct pdc_id_list {
    struct PDC_id_type *PDC_id_type_list_g[PDC_MAX_NUM_TYPES];
};

struct pdc_id_list *pdc_id_list_g;

/**
 * Creates a new type of ID's to give out.
 * The class is initialized or its reference count is incremented
 * (if it is already initialized).

 * \param type_id [IN]          Type ID
 * \param free_func             free type function
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_register_type(PDC_type_t type_id, PDC_free_t free_func);

/**
 * Public interface to PDCid_register.
 *
 * \param type [IN]             A enum type PDC_type_t, e.g. PDC_CONT, PDC_OBJ
 * \param object [IN]           Pointer to an object storage
 *
 * \return Type id on success/Negative on failure
 */
/*
pdcid_t PDCid_register(PDC_type_t type, const void *object);
*/

/**
 * Registers an OBJECT in a TYPE and returns an ID for it.
 * This routine does not check for uniqueness of the objects,
 * if you register an object twice, you will get two different
 * IDs for it.
 *
 * \param type [IN]             A enum type PDC_type_t, e.g. PDC_CONT, PDC_OBJ
 * \param object [IN]           Pointer to an object storage
 *
 * \return Type id on success/Negative on failure
 */
pdcid_t PDC_id_register(PDC_type_t type, const void *object);

/**
 * Increment the number of references outstanding for an ID.
 *
 * \param id [IN]               Id of type to decrease
 *
 * \return New reference count on success/Negative on failure
 */
int PDC_inc_ref(pdcid_t id);

/**
 * Decrement the number of references outstanding for an ID.
 *
 * \param id [IN]               Id of type to decrease
 *
 * \return New reference count on success/Negative on failure
 */
int PDC_dec_ref(pdcid_t id);

/**
 *  Check if PDC_type_t id list is empty
 *
 * \param type [IN]             A enum type PDC_type_t, e.g. PDC_CONT, PDC_OBJ
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_id_list_null(PDC_type_t type);


/**
 * Clear the list of a PDC_type_t type
 *
 * \param type [IN]             A enum type PDC_type_t, e.g. PDC_CONT, PDC_OBJ
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_id_list_clear(PDC_type_t type);

/**
 * To destroy ID types
 *
 * \param type [IN]             A enum type PDC_type_t, e.g. PDC_CONT, PDC_OBJ
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_destroy_type(PDC_type_t type);

/**
 * Given an object ID find the info struct that describes the object
 *
 * \param idid [IN]             Id to look up
 *
 * \return Pointer to the object's info struct on success/Null on failure
 */
struct PDC_id_info *PDC_find_id(pdcid_t idid);

/**
 * Given an object ID find the info struct that describes the object
 *
 * \param type [IN]             A enum type PDC_type_t, e.g. PDC_CONT, PDC_OBJ
 * \param byname [IN]           Name of the object to look up
 *
 * \return Id of the object on success/Negative on failure
 */
pdcid_t PDC_find_byname(PDC_type_t type, const char *byname);

#endif
