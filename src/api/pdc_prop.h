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

#ifndef PDC_PROP_H
#define PDC_PROP_H

#include "pdc_public.h"

/*******************/
/* Public Structs */
/*******************/
struct pdc_obj_prop {
    pdcid_t        obj_prop_id;
    size_t         ndim;
    uint64_t *     dims;
    pdc_var_type_t type;
};

/*******************/
/* Public Typedefs */
/*******************/
typedef enum { PDC_CONT_CREATE = 0, PDC_OBJ_CREATE } pdc_prop_type_t;

/*********************/
/* Public Prototypes */
/*********************/
/**
 * Create PDC property
 *
 * \param type [IN]             PDC property creation type (enum type),
 *                              PDC_CONT_CREATE or PDC_OBJ_CREATE
 * \param id [IN]               ID of the PDC
 *
 * \return PDC property id on success/Zero on failure
 */
pdcid_t PDCprop_create(pdc_prop_type_t type, pdcid_t pdc_id);

/**
 * Close property
 *
 * \param id [IN]               ID of the property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_close(pdcid_t id);

/**
 * Quickly create object property from an existing object property
 * Share the same property with the existing one other than data_loc, data type, buf, which are data related
 *
 * \param type [IN]             PDC property creation type (enum type),
 *                              PDC_CONT_CREATE or PDC_OBJ_CREATE
 * \param id [IN]               ID of the PDC
 *
 * \return PDC property id on success/Zero on failure
 */
pdcid_t PDCprop_obj_dup(pdcid_t prop_id);

/**
 * Get container property infomation
 *
 * \param prop_id [IN]          ID of the property
 *
 * \return Pointer to _pdc_cont_prop struct/Null on failure
 */
struct _pdc_cont_prop *PDCcont_prop_get_info(pdcid_t prop_id);

/**
 * Get object property infomation
 *
 * \param prop_id [IN]          ID of the object property
 *
 * \return Pointer to pdc_obj_prop struct/Null on failure
 */
struct pdc_obj_prop *PDCobj_prop_get_info(pdcid_t prop_id);

/**
 * Send updated metadata (stored as property) to metadata server
 *
 * \param obj_id[IN]            Object ID
 * \param prop_id[IN]           Object property
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_update(pdcid_t obj_id, pdcid_t prop_id);

/**
 * Delete a tag with a specific name and value
 *
 * \param obj_id[IN]            Object ID
 * \param tag_name [IN]         Metadta field name
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_delete(pdcid_t obj_id, char *tag_name);

/**
 * **********
 *
 * \param obj_id[IN]            Object ID
 * \param tag_name [IN]         Metadta field name
 * \param tag_value [IN]        Metadta field value
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCtag_get(pdcid_t obj_id, char *tag_name, void *tag_value);

#endif /* PDC_PROP_H */
