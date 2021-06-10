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

#ifndef PDC_OBJ_PKG_H
#define PDC_OBJ_PKG_H

#include "pdc_private.h"

/****************************/
/* Library Private Typedefs */
/****************************/
typedef enum { PDC_OBJ_GLOBAL, PDC_OBJ_LOCAL } _pdc_obj_location_t;

typedef enum { PDC_NOP = 0, PDC_TRANSFORM = 1, PDC_ANALYSIS = 2 } _pdc_obj_op_type_t;

/**************************/
/* Library Private Struct */
/**************************/
struct _pdc_obj_info {
    struct pdc_obj_info *   obj_info_pub;
    _pdc_obj_location_t     location;
    void *                  metadata;
    struct _pdc_cont_info * cont;
    struct _pdc_obj_prop *  obj_pt;
    struct region_map_list *region_list_head;
};

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
 * Create an object
 *
 * \param cont_id [IN]          ID of the container
 * \param obj_name [IN]         Name of the object
 * \param obj_create_prop [IN]  ID of object property,
 *                              returned by PDCprop_create(PDC_OBJ_CREATE)
 * \param location [IN]         PDC_OBJ_GLOBAL/PDC_OBJ_LOCAL
 *
 * \return Object id on success/Negative on failure
 */
pdcid_t PDC_obj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id,
                       _pdc_obj_location_t location);

/**
 * Get object information
 *
 * \param obj_id [IN]           ID of the object
 *
 * \return Pointer to _pdc_obj_info struct on success/Null on failure
 */
struct _pdc_obj_info *PDC_obj_get_info(pdcid_t obj_id);

/**
 * Free object information
 *
 * \param obj_id [IN]           ID of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_free_obj_info(struct _pdc_obj_info *obj);

/**
 * PDC object finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_obj_end();

/**
 * Check if object list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_obj_list_null();

#endif /* PDC_OBJ_PKG_H */
