#ifndef _pdc_prop_H
#define _pdc_prop_H

#include "pdc_error.h"
#include "pdc_interface.h"
#include "pdc_prop_pkg.h"


/**
 * PDC container and object property initialization
 *
 * \param pc [IN]               Pointer to PDC_CLASS_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_init(PDC_CLASS_t *pc);

/**
 * Create PDC property 
 *
 * \param type [IN]             PDC property creation type (enum type), 
 *                              PDC_CONT_CREATE or PDC_OBJ_CREATE
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return PDC property id on success (0 for container and 1 for object)/Negative on failure
 */
pdcid_t PDCprop_create(PDC_prop_type type, pdcid_t pdc_id);

/**
 * Close PDC property
 *
 * \param id [IN]               Id of the PDC property
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_close(pdcid_t id, pdcid_t pdc_id);

/**
 * PDC container and object property finalize
 *
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_end(pdcid_t pdc_id);

/* private functions */

/**
 * Check if object property list is empty
 *
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_prop_obj_list_null(pdcid_t pdc_id);

/**
 * Check if container property list is empty
 *
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_prop_cont_list_null(pdcid_t pdc_id);

/**
 * Get container property infomation
 *
 * \param prop_id [IN]          Id of the property
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Pointer to PDC_cont_prop struct/Null on failure
 */
struct PDC_cont_prop *PDCcont_prop_get_info(pdcid_t prop_id, pdcid_t pdc_id);

/**
 * Get object property infomation
 *
 * \param prop_id [IN]          Id of the object property
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Pointer to PDC_obj_prop struct/Null on failure
 */
struct PDC_obj_prop *PDCobj_prop_get_info(pdcid_t prop_id, pdcid_t pdc_id);

#endif
