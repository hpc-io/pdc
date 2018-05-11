/*
 * Copyright Notice for 
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***
 
 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.
 
 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.
 
 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#ifndef _pdc_obj_H
#define _pdc_obj_H

#include "pdc_obj_pkg.h"
#include "pdc_error.h"
#include "pdc_interface.h"
#include "pdc_life.h"
#include "pdc_client_connect.h"
#include "pdc_client_server_common.h"

typedef struct PDC_id_info obj_handle;

/**
 * Create a region
 *
 * \param ndims [IN]            Number of dimensions
 * \param offset [IN]           Offset of each dimension
 * \param size [IN]             Size of each dimension
 *
 * \return Object id on success/Zero on failure
 */
pdcid_t PDCregion_create(size_t ndims, uint64_t *offset, uint64_t *size);

/**
 * Create an object 
 * 
 * \param cont_id [IN]          Id of the container
 * \param obj_name [IN]         Name of the object
 * \param obj_create_prop [IN]  Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 *
 * \return Object id on success/Zero on failure 
 */
pdcid_t PDCobj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_create_prop);

/**
 * Set object lifetime 
 *
 * \param obj_prop [IN]         Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param obj_lifetime [IN]     Object lifetime (enum type), PDC_PERSIST or PDC_TRANSIENT
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_lifetime(pdcid_t obj_prop, PDC_lifetime obj_lifetime);

/**
 * Set object user id
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param user_id [IN]          User id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id);

/**
 * Set object data location 
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param app_name [IN]         Data location path 
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_data_loc(pdcid_t obj_prop, char *app_name);

/**
 * Set object application name
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param app_name [IN]         Application name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name);

/**
 * Set object time step
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param time_step [IN]        Time step
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step);

/**
 * Set object tag
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param tags [IN]             Tags
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags);

/**
 * Set object dimension
 *
 * \param obj_prop [IN]         Id of object property, returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param ndim [IN]             Number of dimensions
 * \param dims [IN]             Size of each dimension
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims);

/**
 * Set object type
 * 
 * \param obj_prop [IN]         Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param type [IN]             Object variable type (enum type), 
 *                              i.e. PDC_int_t, PDC_float_t
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_type(pdcid_t obj_prop, PDC_var_type_t type);

/**
 * Set an object buffer 
 *
 * \param obj_prop [IN]         Id of object property, 
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param buf [IN]              Starting point of object storage
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf);

/**
 * Retrieve the buffer of an object 
 *
 * \param obj_id [IN]           Id of the object
 *
 * \return Address of object buffer on success/Null on failure
 */
void ** PDCobj_buf_retrieve(pdcid_t obj_id);

/**
 * Open an object within a container
 *
 * \param cont_id [IN]          Id of the container
 * \param obj_name [IN]         Name of the object
 *
 * \return Object id on success/Zero on failure
 */
pdcid_t PDCobj_open(const char *obj_name);

/**
 * Iterate over objects in a container
 *
 * \param cont_id [IN]          Container id, returned by 
 *                              PDCobj_open(pdcid_t pdc_id, const char *cont_name)
 *
 * \return A pointer to object handle struct on success/NULL on failure
 */
obj_handle *PDCobj_iter_start(pdcid_t cont_id);

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
 * \return A pointer to object handle struct on success/Zero on failure
 */
obj_handle *PDCobj_iter_next(obj_handle *ohandle, pdcid_t cont_id);

/**
 * Get object information
 *
 * \param ohandle [IN]          A pointer to obj_handle struct, 
 *                              returned by PDCobj_iter_start(pdcid_t cont_id)
 *
 * \return Pointer to a PDC_obj_info struct on success/NULL on failure
 */
struct PDC_obj_info * PDCobj_iter_get_info(obj_handle *ohandle);

/**
 * View query result
 *
 * \param view_id [IN]          Query id, returned by PDCquery_obj
 *                              e.g. pdcid_t pdc_id, PDC_match_op_t match_op, ...
 *
 * \return Object handle on success/Null on failure
 */
obj_handle *PDCview_iter_start(pdcid_t view_id);

//perr_t PDCobj_encode(pdcid_t obj_id, pdcid_t *meta_id);

//pdcid_t PDCobj_decode(pdcid_t obj_id, pdcid_t meta_id);

/**
 * Map an application buffer to an object
 *
 * \param buf [IN]              Start point of an application buffer
 * \param local_type [IN]       Data type of data in memory
 * \param local_reg  [IN]       Id of the source region
 * \param remote_obj [IN]       Id of the target object
 * \param remote_reg [IN]       Id of the target region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCbuf_obj_map(void *buf, PDC_var_type_t local_type, pdcid_t local_reg, pdcid_t remote_obj, pdcid_t remote_reg);

/**
 * Map an object to another object
 *
 * \param from_obj [IN]         Id of the source object
 * \param from_reg [IN]         Id of the source region
 * \param from_obj [IN]         Id of the target object
 * \param from_reg [IN]         Id of the target region
 *
 * \return Non-negative on success/Negative on failure
 */
//perr_t PDCobj_map(pdcid_t from_obj, pdcid_t from_reg, pdcid_t to_obj, pdcid_t to_reg);

/**
 * Get object information
 *
 * \param reg_id [IN]           Id of the region
 * \param obj_id [IN]           Id of the object
 *
 * \return Pointer to PDC_obj_info struct on success/Null on failure
 */
//struct PDC_region_info *PDCregion_get_info(pdcid_t reg_id);

/**
 * Unmap all regions within the object 
 *
 * \param remote_obj_id [IN]    Id of the target object
 * \param remote_reg_id [IN]    Id of the target region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_buf_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id);

/**
 * Unmap all regions within the object 
 *
 * \param obj_id [IN]           Id of the object
 *
 * \return Non-negative on success/Negative on failure
 */
//perr_t PDCobj_unmap(pdcid_t obj_id);

/**
 * Unmap the region 
 *
 * \param obj_id [IN]           Id of the object
 * \param reg_id [IN]           Id of the region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCreg_unmap(pdcid_t obj_id, pdcid_t reg_id);

/**
 * Obtain the region lock
 *
 * \param obj_id [IN]           Id of the object
 * \param reg_id [IN]           Id of the region
 * \param access_type [IN]      Region access type: READ or WRITE
 * \param lock_mode [IN]        Lock mode of the region: BLOCK or NOBLOCK
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCreg_obtain_lock(pdcid_t obj_id, pdcid_t reg_id, PDC_access_t access_type, PDC_lock_mode_t lock_mode);

/**
 * Release the region lock
 *
 * \param obj_id [IN]           Id of the object
 * \param reg_id [IN]           Id of the region
 * \param access_type [IN]      Region access type
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCreg_release_lock(pdcid_t obj_id, pdcid_t reg_id, PDC_access_t access_type);

/**
 * Release memory buffers from one memory object 
 *
 * \param obj_id [IN]           Id of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_release(pdcid_t obj_id);

/**
 * Close an object 
 *
 * \param obj_id [IN]           Id of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCobj_close(pdcid_t obj_id);

/**
 * Close a region
 *
 * \param region_id [IN]        Id of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_close(pdcid_t region_id);

/**
 * Get object information
 *
 * \param obj_id [IN]           Id of the object
 *
 * \return Pointer to PDC_obj_info struct on success/Null on failure
 */
//struct PDC_obj_info *PDCobj_get_info(pdcid_t obj_id);

#endif
