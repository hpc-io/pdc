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

#ifndef PDC_OBJ_OBJECT_H
#define PDC_OBJ_OBJECT_H

#include "pdc_obj_pkg.h"

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * PDC object initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_obj_init();

/**
 * PDC region initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_region_init();

/**
 * Create an object
 *
 * \param cont_id [IN]          Id of the container
 * \param obj_name [IN]         Name of the object
 * \param obj_create_prop [IN]  Id of object property,
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param location [IN]         PDC_OBJ_GLOBAL/PDC_OBJ_LOCAL
 *
 * \return Object id on success/Negative on failure
 */
pdcid_t PDC_obj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id, PDCobj_location location);

/**
 * Get object information
 *
 * \param obj_id [IN]           ID of the object
 *
 * \return Pointer to PDC_obj_info struct on success/Null on failure
 */
struct PDC_obj_info *PDC_obj_get_info(pdcid_t obj_id);

/**
 * PDC object finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_obj_end();

/**
 * PDC region finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_region_end();

/**
 * Check if object list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_obj_list_null();

/**
 * Check if region list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_region_list_null();

#endif /* PDC_OBJ_OBJECT_H */
