#ifndef _pdc_H
#define _pdc_H

#include <stdint.h>
#include <stdbool.h>
#include "pdc_private.h"

typedef struct {
} PDC_property;

typedef struct {
    pid_t pdc_id;
} PDC_STRUCT;

typedef enum {
    PDC_CONT_CREATE,
    PDC_OBJ_CREATE  
} PDC_prop_type;

typedef enum {
    UNKNOWN = -1,
    MEMORY,
    FLASH,
    DISK,
    FILESYSTEM,
    TAPE
} PDC_loci;

typedef struct {
} cont_handle;

typedef struct {
} obj_handle;

typedef enum {
    ROW_major,
    COL_major
} PDC_major_type;

// The lifetime of an object or container
typedef enum {
    PDC_PERSIST, 
    PDC_TRANSIENT 
} PDC_lifetime;

typedef struct {
    char *name;
} PDC_cont_info_t;

typedef struct {
// public info
    PDC_cont_info_t info;
// private stuff
} pdc_container_t;

typedef struct {
    char *name;
} PDC_obj_info_t;

typedef struct {
// public info
    PDC_obj_info_t info;
// private stuff
} pdc_object_t;

typedef struct {
} PDC_loci_info_t;

typedef struct {
    PDC_major_type type;
} PDC_transform;

typedef struct {
    uint64_t offset;
    uint64_t storage_size;
    PDC_loci locus;
} PDC_region;

/* Query match conditions */
typedef enum {
    PDC_Q_MATCH_EQUAL,        /* equal */
    PDC_Q_MATCH_NOT_EQUAL,    /* not equal */
    PDC_Q_MATCH_LESS_THAN,    /* less than */
    PDC_Q_MATCH_GREATER_THAN  /* greater than */
} PDC_query_op_t;

typedef enum {
    PDC_QUERY_OP_AND = 0,
    PDC_QUERY_OP_OR  = 1
} PDC_com_op_mode_t;

/* Query type */
typedef enum {
    PDC_Q_TYPE_DATA_ELEM,  /* selects data elements */
    PDC_Q_TYPE_ATTR_VALUE, /* selects attribute values */
    PDC_Q_TYPE_ATTR_NAME,  /* selects attributes */
    PDC_Q_TYPE_LINK_NAME,  /* selects objects */
    PDC_Q_TYPE_MISC        /* (for combine queries) selects misc objects */
} PDC_query_type_t;


/* Functions in PDC.c */

///////////////////
// PDC functions //
///////////////////

/* Initialize the PDC layer
 * Param PDC_property [IN]: A PDC_property struct  
 * Return: PDC id 
 * */
pid_t PDCinit(PDC_property prop);

/* Create a type of PDC
 * Param PDC_STRUCT [IN]: A PDC_STRUCT struct
 * Return: PDC type id 
 * */
pid_t PDCtype_create(PDC_STRUCT pdc_struct);

/* Insert fields in PDC_STRUCT 
 * Param type_id [IN]: Type of PDC, returned by PDCtype_create(struct PDC_STRUCT) 
 * Param name [IN]: Variable name to insert to PDC_STRUCT
 * Param offset [IN]: Offset of the variable in PDC_STRUCT
 * Param var_type [IN]: Variable type (enum type), choosing from PDC_var_type, i.e. PDC_int_t, PDC_float_t, etc 
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCtype_struct_field_insert(pid_t type_id, const char *name, uint64_t offset, PDC_var_type var_type);

/* get number of loci for a PDC
 * Param pdc_id [IN]: Id of the PDC
 * Param nloci [OUT]: Number of loci of the PDC residing at
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCget_loci_count(pid_t pdc_id, pid_t *nloci);

/* Get PDC info in the locus
 * Param pdc_id [IN]: Id of the PDC
 * Param n [IN]: Location of memory hierarchy of the PDC
 * Param info [OUT]: A PDC_loci_info_t struct
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCget_loci_info(pid_t pdc_id, pid_t n, PDC_loci_info_t *info);

/* Create PDC property 
 * Param type [IN]: PDC property creation type (enum type), PDC_CONT_CREATE or PDC_OBJ_CREATE
 * Return: PDC property id
 * */
pid_t PDCprop_create(PDC_prop_type type);

/* Close PDC property
 * Param prop_id [IN]: Id of the PDC property
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_close(pid_t prop_id);


/////////////////////////
// container functions //
/////////////////////////

/* Create a container 
 * Param pdc_id [IN]: Id of the PDC
 * Param cont_name [IN]: Name of the container
 * Param cont_create_prop [IN]: Id of container property, returned by PDCprop_create(PDC_CONT_CREATE)
 * Return: Container id
 * */
pid_t PDCcont_create(pid_t pdc_id, const char *cont_name, pid_t cont_create_prop);

/* Open a container 
 * Param pdc_id [IN]: Id of the PDC
 * Param cont_name [IN]: Name of the container 
 * Return: Container id 
 * */
pid_t PDCcont_open(pid_t pdc_id, const char *cont_name);

/* Iterate over containers within a PDC
 * Param pdc_id [IN]: Id of the PDC
 * Return: Container handle
 * */
cont_handle PDCcont_iter_start(pid_t pdc_id);

/* Check if container handle is pointing to NULL 
 * Param chandle [IN]: A cont_handle struct, returned by PDCcont_iter_start(pid_t pdc_id)
 * Return: 1 in case of success or 0 in case of failure
 * */
bool PDCcont_iter_null(cont_handle chandle);

/* Iterate the next container within a PDC 
 * Param chandle [IN]: A cont_handle struct, returned by PDCcont_iter_start(pid_t pdc_id)
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCcont_iter_next(cont_handle chandle);

/* Retrieve container information
 * Param chandle [IN]: A cont_handle struct, returned by PDCcont_iter_start(pid_t pdc_id)
 * Return: Pointer to a PDC_cont_info_t struct
 * */
PDC_cont_info_t * PDCcont_iter_get_info(cont_handle chandle);

/* Persist a transient container
 * Param cont_id [IN]: Id of the container, returned by PDCcont_open(pid_t pdc_id, const char *cont_name)
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCcont_persist(pid_t cont_id);
  
/* Set container lifetime 
 * Param cont_create_prop [IN]: Id of container property, returned by PDCprop_create(PDC_CONT_CREATE)
 * Param cont_lifetime [IN]: container lifetime (enum type), PDC_PERSIST or PDC_TRANSIENT
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_set_cont_lifetime(pid_t cont_create_prop, PDC_lifetime cont_lifetime);

/* Close a container 
 * Param cont_id [IN]: Container id, returned by PDCcont_open(pid_t pdc_id, const char *cont_name)
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCcont_close(pid_t cont_id);


//////////////////////
// object functions//
//////////////////////

/* Create an object  
 * Param cont_id [IN]: Id of the container
 * Param obj_name [IN]: Name of the object
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Return: object id 
 * */
pid_t PDCobj_create(pid_t cont_id, const char *obj_name, pid_t obj_create_prop); 

/* Set object lifetime 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param obj_lifetime [IN]: Object lifetime (enum type), PDC_PERSIST or PDC_TRANSIENT
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_set_obj_lifetime(pid_t obj_create_prop, PDC_lifetime obj_lifetime);

/* Set object dimensions 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param ndim [IN]: Number of dimensions
 * Param dims [IN]: Size of each dimension
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_set_obj_dims(pid_t obj_create_prop, PDC_int_t ndim, uint64_t *dims);

/* Set object type 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param type [IN]: Object variable type (enum type), choosing from PDC_var_type, i.e. PDC_int_t, PDC_float_t, etc
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_set_obj_type(pid_t obj_create_prop, PDC_var_type type);

/* Set an object buffer 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param buf [IN]: Start point of object storage
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_set_obj_buf(pid_t obj_create_prop, void *buf);

/* Retrieve the buffer of an object 
 * Param obj_id [IN]: Id of the object
 * Param buf [IN]: Start point of object storage
 * Param region [IN]: A PDC_region struct
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_buf_retrieve(pid_t obj_id, void **buf, PDC_region region);  

/* Open an object within a container
 * Param cont_id [IN]: Id of the container
 * Param obj_name [IN]: Name of the object
 * Return: Object id
 * */
pid_t PDCobj_open(pid_t cont_id, const char *obj_name);

/* Iterate over objects in a container
 * Param cont_id [IN]: Container id, returned by PDCcont_open(pid_t pdc_id, const char *cont_name)
 * Return: Object handle 
 * */
obj_handle PDCobj_iter_start(pid_t cont_id);

/* Check if object handle is pointing to NULL 
 * Param ohandle [IN]: A obj_handle struct, returned by PDCobj_iter_start(pid_t cont_id)
 * Return: 1 in case of success or 0 in case of failure
 * */
bool PDCobj_iter_null(obj_handle ohandle);

/* Iterate the next object 
 * Param ohandle [IN]: A obj_handle struct, returned by PDCobj_iter_start(pid_t cont_id)
 * Return: Non-negative on success/Negative on failure
 *  */
perr_t PDCobj_iter_next(obj_handle ohandle);

/* Get object information
 * Param ohandle [IN]: A obj_handle struct, returned by PDCobj_iter_start(pid_t cont_id)
 * Return: Pointer to a PDC_obj_info_t struct
 * */
PDC_obj_info_t * PDCobj_iter_get_info(obj_handle ohandle);

/* Query on object 
 * Param pdc_id [IN]: Id of PDC
 * Param query_type [IN]: A PDC_query_type_t struct
 * Param query_op [IN]: A PDC_query_op_t struct
 * Return: Query id
 * */
pid_t PDC_query_create(pid_t pdc_id, PDC_query_type_t query_type, PDC_query_op_t query_op, ...);
//pid_t PDC_query_obj(pid_t pdc_id, const char *varName, PDC_query_op_t query_op, const char *value);

/* Use result from  PDCquery_obj function
 * Param query1_id [IN]: Query id, returned by PDC_query_obj function
 * Param PDC_com_op_mode_t [IN]: Query Combination type (enum type), PDC_QUERY_OP_AND or PDC_QUERY_OP_OR
 * Param query2_id [IN]: Query id, returned by PDC_query_obj function
 * Return: Query id
 * */
pid_t PDC_query_combine(pid_t query1_id, PDC_com_op_mode_t combine_op, pid_t query2_id);

/* View query result
 * Param view_id [IN]: Query id, returned by PDCquery_obj(pid_t pdc_id, PDC_match_op_t match_op, ...) 
 * Return: Object handle
 * */
obj_handle PDCview_iter_start(pid_t view_id);

/* Map an application buffer to an object 
 * Param obj_id [IN]: Id of the object
 * Param buf [IN]: Start point of an application buffer
 * Param region [IN]: A PDC_region struct
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_buf_map(pid_t obj_id, void *buf, PDC_region region);  

/* Map an object 
 * Param a [IN]: Id of the source object
 * Param xregion [IN]: A PDC_region struct, region of the source object
 * Param b [IN]: Id of the destination object
 * Param yregion [IN]: A PDC_region struct, region of the destination object
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_map(pid_t a, PDC_region xregion, pid_t b, PDC_region yregion);

/* Diassociate memory object from PDC container objects 
 * Param obj_id [IN]: Id of the object
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_unmap(pid_t obj_id);

/* Release memory buffers from one memory object 
 * Param obj_id [IN]: Id of the object
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_release(pid_t obj_id);

/* Update object in the region 
 * Tell the PDC system that the region in memory is updated WRT to the container
 * Param obj_id [IN]: Id of the object
 * Param region [IN]: A PDC_region struct
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_update_region(pid_t obj_id, PDC_region region);

/* Tell the PDC system that region in the memory is stale WRT to the container
 * Param obj_id [IN]: Id of the object
 * Param region [IN]: A PDC_region struct
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_invalidate_region(pid_t obj_id, PDC_region region);

/* Object Syncranization. 
 * Param obj_id [IN]: Id of the object
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_sync(pid_t obj_id);

/* Close an object 
 * Param obj_id [IN]: Id of the object
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCobj_close(pid_t obj_id);  


/* Object transform functions */

/* Built-in transform 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param locus [IN]: Object locus setup (enum type), choosing from PDC_loci, i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc 
 * Param A [IN]: A PDC_transform struct 
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_set_obj_loci_prop(pid_t obj_create_prop, PDC_loci locus, PDC_transform A);

/* User transform 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param locus [IN]: Object source locus (enum type), choosing from PDC_loci, i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc 
 * Param A [IN]: A PDC_transform struct
 * Param dest_locus [IN]: Destination locus, choosing from PDC_loci, i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc
 * Return: Non-negative on success/Negative on failure
 * */
perr_t PDCprop_set_obj_transform(pid_t obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus);


#endif /* end of _pdc_H */ 
