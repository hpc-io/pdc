#ifndef _pdc_obj_H
#define _pdc_obj_H

#include "pdc_obj_pkg.h"
#include "pdc_error.h"
#include "pdc_interface.h"
#include "pdc_life.h"

typedef PDC_id_info_t obj_handle;

/**
 * PDC object initialization
 *
 * \param pc [IN]               Pointer to PDC_CLASS_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_init(PDC_CLASS_t *pc);

/**
 * PDC region initialization
 *
 * \param pc [IN]               Pointer to PDC_CLASS_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_init(PDC_CLASS_t *pc);

/**
 * Create a region
 *
 * \param ndims [IN]            Number of dimensions
 * \param offset [IN]           Offset of each dimension
 * \param size [IN]             Size of each dimension
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Object id on success/Negative on failure
 */
pdcid_t PDCregion_create(size_t ndims, uint64_t *offset, uint64_t *size, pdcid_t pdc_id);

/**
 * Create an object 
 * 
 * \param pdc_id [IN]           Id of the pdc
 * \param cont_id [IN]          Id of the container
 * \param obj_name [IN]         Name of the object
 * \param obj_create_prop [IN]  Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 *
 * \return Object id on success/Negative on failure 
 */
pdcid_t PDCobj_create(pdcid_t pdc_id, pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop);

/**
 * Set object lifetime 
 *
 * \param obj_prop [IN]         Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param obj_lifetime [IN]     Object lifetime (enum type), PDC_PERSIST or PDC_TRANSIENT
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_lifetime(pdcid_t obj_prop, PDC_lifetime obj_lifetime, pdcid_t pdc_id);

/**
 * Set object user id
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param user_id [IN]          User id
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id, pdcid_t pdc_id);

/**
 * Set object application name
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param app_name [IN]         Application name
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name, pdcid_t pdc_id);

/**
 * Set object time step
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param time_step [IN]        Time step
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step, pdcid_t pdc_id);

/**
 * Set object tag
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param tags [IN]             Tags
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags, pdcid_t pdc_id);

/**
 * Set object dimension
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param ndim [IN]             Number of dimensions
 * \param dims [IN]             Size of each dimension
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims, pdcid_t pdc_id);

/**
 * Set object type
 * 
 * \param obj_prop [IN]         Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param type [IN]             Object variable type (enum type), 
 *                              i.e. PDC_int_t, PDC_float_t
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_type(pdcid_t obj_prop, PDC_var_type_t type, pdcid_t pdc_id);

/**
 * Set an object buffer 
 *
 * \param obj_prop [IN]         Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param buf [IN]              Starting point of object storage
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf, pdcid_t pdc_id);

/**
 * Retrieve the buffer of an object 
 *
 * \param obj_id [IN]           Id of the object
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Address of object buffer on success/Null on failure
 */
void ** PDCobj_buf_retrieve(pdcid_t obj_id, pdcid_t pdc_id);

/**
 * Open an object within a container
 *
 * \param cont_id [IN]          Id of the container
 * \param obj_name [IN]         Name of the object
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Object id on success/Negative on failure
 */
pdcid_t PDCobj_open(pdcid_t cont_id, const char *obj_name, pdcid_t pdc_id);

/**
 * Iterate over objects in a container
 *
 * \param cont_id [IN]          Container id, returned by 
 *                              PDCobj_open(pdcid_t pdc_id, const char *cont_name)
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return A pointer to object handle struct on success/Negative on failure
 */
obj_handle *PDCobj_iter_start(pdcid_t cont_id, pdcid_t pdc_id);

/**
 * Check if object handle is pointing to NULL 
 *
 * \param ohandle [IN]          A obj_handle struct, returned by PDCobj_iter_start(pdcid_t cont_id)
 *
 * \return 1 on success/0 on failure
 */
pbool_t PDCobj_iter_null(obj_handle *ohandle);

/**
 * Iterate the next object 
 *
 * \param ohandle [IN]          A obj_handle struct, returned by 
 *                              PDCobj_iter_start(pdcid_t cont_id)
 * \param cont_id [IN]          Id of the container
 *
 * \return A pointer to object handle struct on success/Negative on failure
 */
obj_handle *PDCobj_iter_next(obj_handle *ohandle, pdcid_t cont_id);

/**
 * Get object information
 *
 * \param ohandle [IN]          A pointer to obj_handle struct, 
 *                              returned by PDCobj_iter_start(pdcid_t cont_id)
 *
 * \return Pointer to a PDC_obj_info struct on success/Negative on failure
 */
struct PDC_obj_info * PDCobj_iter_get_info(obj_handle *ohandle);

/**
 * Query on object 

 * \param pdc_id [IN]           Id of PDC
 * \param query_type [IN]       A PDC_query_type_t struct
 * \param query_op [IN]         A PDC_query_op_t struct
 *
 * \return Query id on success/Negative on failure
 */
pdcid_t PDC_query_create(pdcid_t pdc_id, PDC_query_type_t query_type, PDC_query_op_t query_op, ...);
//pdcid_t PDC_query_obj(pdcid_t pdc_id, const char *varName, PDC_query_op_t query_op, const char *value);

/**
 * View query result
 *
 * \param view_id [IN]          Query id, returned by PDCquery_obj
 *                              e.g. pdcid_t pdc_id, PDC_match_op_t match_op, ...
 *
 * \return Object handle on success/Null on failure
 */
obj_handle *PDCview_iter_start(pdcid_t view_id);

/**
 * Map an application buffer to an object
 *
 * \param buf [IN]              Start point of an application buffer
 * \param region [IN]           Id of the source region
 * \param obj_id [IN]           Id of the target object
 * \param region [IN]           Id of the target region
 * \param pdc_id [IN]           Id of PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_buf_map(void *buf, pdcid_t from_reg, pdcid_t obj_id, pdcid_t to_reg, pdcid_t pdc_id);

/**
 * Map an object to another object
 *
 * \param from_obj [IN]         Id of the source object
 * \param from_reg [IN]         Id of the source region
 * \param from_obj [IN]         Id of the target object
 * \param from_reg [IN]         Id of the target region
 * \param pdc_id [IN]           Id of PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_map(pdcid_t from_obj, pdcid_t from_reg, pdcid_t to_obj, pdcid_t to_reg, pdcid_t pdc_id);

/**
 * Get object information
 *
 * \param obj_id [IN]           Id of the object
 * \param pdc_id [IN]           Id of PDC
 *
 * \return Pointer to PDC_obj_info_t struct on success/Null on failure
 */
struct PDC_obj_info *PDCobj_get_info(pdcid_t obj_id, pdcid_t pdc_id);

/**
 * Get object information
 *
 * \param reg_id [IN]           Id of the region
 * \param obj_id [IN]           Id of the object
 * \param pdc_id [IN]           Id of PDC
 *
 * \return Pointer to PDC_obj_info_t struct on success/Null on failure
 */
struct PDC_region_info *PDCregion_get_info(pdcid_t reg_id, pdcid_t obj_id, pdcid_t pdc_id);

/**
 * Unmap all regions within the object 
 *
 * \param obj_id [IN]           Id of the object
 * \param pdc_id [IN]           Id of PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_unmap(pdcid_t obj_id, pdcid_t pdc_id);

/**
 * Unmap the region 
 *
 * \param obj_id [IN]           Id of the object
 * \param reg_id [IN]           Id of the region
 * \param pdc_id [IN]           Id of PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCreg_unmap(pdcid_t obj_id, pdcid_t reg_id, pdcid_t pdc_id);

/**
 * Release memory buffers from one memory object 
 *
 * \param obj_id [IN]           Id of the object
 * \param pdc_id [IN]           Id of PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_release(pdcid_t obj_id, pdcid_t pdc_id);

/**
 * Update object in the region 
 * Tell the PDC system that the region in memory is updated WRT to the container
 *
 * \param obj_id [IN]           Id of the object
 * \param region_id [IN]        Id of the region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_update_region(pdcid_t obj_id, pdcid_t region_id);

/**
 * Tell the PDC system that region in the memory is stale WRT to the container
 *
 * \param obj_id [IN]           Id of the object
 * \param region_id [IN]        Id of the region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_invalidate_region(pdcid_t obj_id, pdcid_t region_id);

/**
 * Object Syncranization. 
 *
 * \param obj_id [IN]           Id of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_sync(pdcid_t obj_id);

/**
 * Close an object 
 *
 * \param obj_id [IN]           Id of the object
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_close(pdcid_t obj_id, pdcid_t pdc_id);

/**
 * Close a region
 *
 * \param region_id [IN]        Id of the object
 * \param pdc_id [IN]           Id of the pdc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_close(pdcid_t region_id, pdcid_t pdc);

/* Object transform functions */

/**
 * Built-in transform 
 *
 * \param obj_prop [IN]         Id of object property, returned by 
 *                              PDCprop_create(PDC_OBJ_CREATE)
 * \param locus [IN]            Object locus setup (enum type), choosing from PDC_loci, 
 *                              i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc
 * \param A [IN]                A PDC_transform struct 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_loci_prop(pdcid_t obj_prop, PDC_loci locus, PDC_transform A);

/**
 * User transform 
 *
 * \param obj_create_prop [IN]  Id of object property, returned by 
 *                              PDCprop_create(PDC_OBJ_CREATE)
 * \param locus [IN]            Object source locus (enum type), choosing from PDC_loci, 
 *                              i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc
 * \param A [IN]                A PDC_transform struct
 * \param dest_locus [IN]       Destination locus, choosing from PDC_loci, 
                                i.e. MEMORY, FLASH, FILESYSTEM, TAPE, etc
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_transform(pdcid_t obj_create_prop, PDC_loci pre_locus, PDC_transform A, PDC_loci dest_locus);

#endif
