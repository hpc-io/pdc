#ifndef _pdc_obj_H
#define _pdc_obj_H

#include "pdc_obj_pkg.h"
#include "pdc_error.h"
#include "pdc_interface.h"
#include "pdc_life.h"

typedef PDC_id_info_t obj_handle;

/* PDC object initialization
 * Param pc [IN]: Pointer to PDC_CLASS_t struct
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_init(PDC_CLASS_t *pc);

perr_t PDCregion_init(PDC_CLASS_t *pc);

/* Create an object  
 * Param pdc_id [IN]: Id of the pdc
 * Param cont_id [IN]: Id of the container
 * Param obj_name [IN]: Name of the object
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Return: object id 
 */
pdcid_t PDCobj_create(pdcid_t pdc_id, pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop);

/* Set object lifetime 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param obj_lifetime [IN]: Object lifetime (enum type), PDC_PERSIST or PDC_TRANSIENT
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_lifetime(pdcid_t obj_prop, PDC_lifetime obj_lifetime, pdcid_t pdc_id);

perr_t PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id, pdcid_t pdc_id);

perr_t PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name, pdcid_t pdc_id);

perr_t PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step, pdcid_t pdc_id);

perr_t PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags, pdcid_t pdc_id);

/* Set object dimensions 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param ndim [IN]: Number of dimensions
 * Param dims [IN]: Size of each dimension
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_dims(pdcid_t obj_create_prop, PDC_int_t ndim, uint64_t *dims, pdcid_t pdc_id);

/* Set object type 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param type [IN]: Object variable type (enum type), choosing from PDC_var_type_t, i.e. PDC_int_t, PDC_float_t, etc
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_type(pdcid_t obj_create_prop, PDC_var_type_t type, pdcid_t pdc_id);

/* Set an object buffer 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param buf [IN]: Start point of object storage
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_buf(pdcid_t obj_create_prop, void *buf, pdcid_t pdc_id);

/* Retrieve the buffer of an object 
 * Param obj_id [IN]: Id of the object
 * Return: Address of object buffer
 */
void ** PDCobj_buf_retrieve(pdcid_t obj_id, pdcid_t pdc_id);

/* Open an object within a container
 * Param cont_id [IN]: Id of the container
 * Param obj_name [IN]: Name of the object
 * Return: Object id
 */
pdcid_t PDCobj_open(pdcid_t cont_id, const char *obj_name, pdcid_t pdc_id);

/* Iterate over objects in a container
 * Param cont_id [IN]: Container id, returned by PDCobj_open(pdcid_t pdc_id, const char *cont_name)
 * Return: A pointer to object handle
 */
obj_handle *PDCobj_iter_start(pdcid_t cont_id, pdcid_t pdc_id);

/* Check if object handle is pointing to NULL 
 * Param ohandle [IN]: A obj_handle struct, returned by PDCobj_iter_start(pdcid_t cont_id)
 * Return: 1 in case of success or 0 in case of failure
 */
pbool_t PDCobj_iter_null(obj_handle *ohandle);

/* Iterate the next object 
 * Param ohandle [IN]: A obj_handle struct, returned by PDCobj_iter_start(pdcid_t cont_id)
 * Return: A pointer to object handle
 */
obj_handle *PDCobj_iter_next(obj_handle *ohandle, pdcid_t cont_id);

/* Get object information
 * Param ohandle [IN]: A pointer to obj_handle struct, returned by PDCobj_iter_start(pdcid_t cont_id)
 * Return: Pointer to a PDC_obj_info_t struct
 */
PDC_obj_info_t * PDCobj_iter_get_info(obj_handle *ohandle);

/* Query on object 
 * Param pdc_id [IN]: Id of PDC
 * Param query_type [IN]: A PDC_query_type_t struct
 * Param query_op [IN]: A PDC_query_op_t struct
 * Return: Query id
 */
pdcid_t PDC_query_create(pdcid_t pdc_id, PDC_query_type_t query_type, PDC_query_op_t query_op, ...);
//pdcid_t PDC_query_obj(pdcid_t pdc_id, const char *varName, PDC_query_op_t query_op, const char *value);

/* View query result
 * Param view_id [IN]: Query id, returned by PDCquery_obj(pdcid_t pdc_id, PDC_match_op_t match_op, ...) 
 * Return: Object handle
 */
obj_handle PDCview_iter_start(pdcid_t view_id);

/* Map an application buffer to an object 
 * Param obj_id [IN]: Id of the object
 * Param buf [IN]: Start point of an application buffer
 * Param region [IN]: A PDC_region struct
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_buf_map(pdcid_t obj_id, void *buf, pdcid_t region);

/* Map an object 
 * Param a [IN]: Id of the source object
 * Param xregion [IN]: A PDC_region struct, region of the source object
 * Param b [IN]: Id of the destination object
 * Param yregion [IN]: A PDC_region struct, region of the destination object
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_map(pdcid_t a, pdcid_t xregion, pdcid_t b, pdcid_t yregion, pdcid_t pdc_id);

/* Diassociate memory object from PDC container objects 
 * Param obj_id [IN]: Id of the object
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_unmap(pdcid_t obj_id);

/* Release memory buffers from one memory object 
 * Param obj_id [IN]: Id of the object
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_release(pdcid_t obj_id);

/* Update object in the region 
 * Tell the PDC system that the region in memory is updated WRT to the container
 * Param obj_id [IN]: Id of the object
 * Param region [IN]: A PDC_region struct
 *  Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_update_region(pdcid_t obj_id, pdcid_t region);

/* Tell the PDC system that region in the memory is stale WRT to the container
 * Param obj_id [IN]: Id of the object
 * Param region [IN]: A PDC_region struct
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_invalidate_region(pdcid_t obj_id, pdcid_t region);

/* Object Syncranization. 
 * Param obj_id [IN]: Id of the object
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_sync(pdcid_t obj_id);

/* Close an object 
 * Param obj_id [IN]: Id of the object
 * Param pdc_id [IN]: Id of the pdc
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCobj_close(pdcid_t obj_id, pdcid_t pdc);

perr_t PDCregion_close(pdcid_t region_id, pdcid_t pdc);

/* Object transform functions */

/* Built-in transform 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param locus [IN]: Object locus setup (enum type), choosing from PDC_loci, i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc 
 * Param A [IN]: A PDC_transform struct 
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_loci_prop(pdcid_t obj_create_prop, PDC_loci locus, PDC_transform A);

/* User transform 
 * Param obj_create_prop [IN]: Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * Param locus [IN]: Object source locus (enum type), choosing from PDC_loci, i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc 
 * Param A [IN]: A PDC_transform struct
 * Param dest_locus [IN]: Destination locus, choosing from PDC_loci, i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_transform(pdcid_t obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus);

pdcid_t PDC_define_region(uint64_t offset, uint64_t size, pdcid_t pdc_id);
#endif
