#ifndef _PDC_H
#define _PDC_H

#include "PDCprivate.h"

typedef struct {
} PDC_property;

typedef struct {
} PDC_STRUCT;

typedef struct {  
    PDC_INT DPC_type;
    union {
	PDC_CONT *CONT_CREATE;
	PDC_OBJ *OBJ_CREATE;
    } type_create;
} PDC_prop_type;

typedef struct {
    UNKNOW = -1,
    MEMORY,
    FLASH,
    FILESYSTEM,
    TAPE
} PDC_loci;

typedef struct {
} PDC_obj_prop;

typedef struct {
} PDC_cont_prop;

typedef struct {
} cont_handle;

typedef struct {
} obj_handle;

typedef enum {
    ROW_major,
    COL_major
} PDC_major_type;

typedef enum {
    PDC_PERSIST, 
    PDC_TRANSIENT 
} PDC_lifetime;

typedef struct {
} PDC_cont_info_t;

typedef struct {
} PDC_obj_info_t;

typedef struct {
} PDC_loci_info_t;

typedef struct {
} PDC_q_info;

typedef struct {
} PDC_transform;

typedef struct {
} PDC_region;

/* Functions in PDCprivate.c */

/* PDC initialization
 * return pdc id */
pid_t PDCinit(struct PDC_property);

/* Create a type of PDC
 * return type id */
pid_t PDCtype_create(struct PDC_STRUCT);

/* Insert fields in PDC_STRUCT */
perr_t PDCtype_struct_field_insert(pid_t type_id, const char *name, uint64_t offset, PDC_var_type var_type);

/* get # of loci for the container pdc_id*/
perr_t PDCget_loci_count(pid_t pdc_id, pid_t & nloci);

/* get locus info for container pdc_id*/
perr_t PDCget_loci_info(pid_t pdc_id, pid_t n, PDC_loci_info_t & info);

/* Iterate over container */
cont_handle PDCcont_iter_start(pid_t pdc_id);

/* Iterate the next object */
perr_t PDCont_iter_next(cont_handle handle);

/* Check if container if NULL */
bool PDCcont_iter_null(cont_handle handle);

/* */
perr_t PDCcont_iter_get_info(cont_handle handle, PDC_cont_info_t & info);

/* create  */
pid_t PDCprop_create(PDC_prop_type type);

perr_t PDCprop_close(pid_t prop_id);

/* set container lifetime */
perr_t PDCprop_set_cont_lifetime(PDC_cont_prop cont_create_prop, PDC_lifetime cont_lifetime);

/* create container */
pid_t PDCcont_create(pid_t pdc_id, const char *cont_name, PDC_cont_prop cont_create_prop);

/* open container 
 * return container id */  
pid_t PDCcont_open(pid_t pdc_id, const char *cont_name);

/* Close container */
perr_t PDCont_close(pid_t cont_id);

/////////////////////
// object function //
/////////////////////

/* Create object  
 * return object id */
pid_t PDCobj_create(pid_t cont_id, const char *obj_name, PDC_obj_prop obj_create_prop); 

/* Open an object */
pid_t PDCobj_open(ipid_t cont_id, const char *obj_name);

/* Set object lifetime */
perr_t PDCprop_set_obj_lifetime(PDC_obj_prop obj_create_prop, PDC_lifetime obj_lifetime);

/* Set object dimensions */
perr_t PDCprop_set_obj_dims(PDC_obj_prop obj_create_prop, int ndim, uint64 *dims);

/* Set object type */
perr_t PDCprop_set_obj_type(PDC_obj_prop obj_create_prop, PDC_var_type var_type);

/* Set object buffer */
PDCprop_set_obj_buf(q_obj_create_prop, void **buf);

/* Retrieve a buffer for an object */
perr_t PDCobj_buf_retrieve(pid_t obj_id, void **buf, pid_t region);  

/* Iterate over objects in a container
 * return object handle */
obj_handle PDCobj_iter_start(pid_t cont_id);

/* Iterate the next object */
perr_t PDCobj_iter_next(obj_handle handle);

/* Query on object */
pid_t PDCquery_obj(pid_t pdc_id, const char *obj_name, PDC_q_info & info);

/* */
obj_handle PDCview_iter_start(pid_t view_id);

/* Map an application buffer to an object */
perr_t PDCobj_buf_map(pid_t obj_id, void *buf, pid_t region);  

/* Map an object */
perr_t PDCobj_map(pid_t a, PDC_region x, pid_t b, PDC_region y);

/* Diassociate memory object from PDC container objects */
perr_t PDCobj_unmap(pid_t obj_id);

/* Release memory buffers from memory object */
perr_t PDCobj_release(pid_t obj_id);

/* Object Sync */
perr_t PDCobj_sync(pid_t obj_id);

/* Update object in the region */
perr_t PDCobj_update_region(pid_t obj_id);

/* Tell PDC system the region is stale */
perr_t PDCobj_invalidate_region(pid_t obj_id, PDC_region rg);

/* Close object */
perr_t PDCobj_close(pid_t obj_id);  

////////////////////////
// transform function //
////////////////////////

/* Built-in transform */
perr_t PDCprop_set_obj_loci_prop(PDC_obj_prop obj_create_prop, PDC_loci locus, PDC_major_type major);

/* User transform */
perr_t PDCprop_set_obj_transform(PDC_obj_prop obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus);


#endif /* end of _PDC_H */ 
