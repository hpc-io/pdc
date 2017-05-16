#ifndef _pdc_cont_H
#define _pdc_cont_H

#include "pdc_error.h"
#include "pdc_cont_pkg.h"
#include "pdc_interface.h"
#include "pdc_life.h"

typedef PDC_id_info_t cont_handle;

/**
 * PDC container initialization
 *
 * \param pc [IN]               Pointer to PDC_CLASS_t struct
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_init(PDC_CLASS_t *pc);

/**
 * Create a container
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_name [IN]        Name of the container
 * \param cont_create_prop [IN] Id of container property
 *                              returned by PDCprop_create(PDC_CONT_CREATE)
 *
 * \return Container id on success/Negative on failure
 */
pdcid_t PDCcont_create(pdcid_t pdc_id, const char *cont_name, pdcid_t cont_create_prop);

/**
 * Open a container
 *
 * \param pdc_id [IN]           Id of the PDC
 * \param cont_name [IN]        Name of the container
 *
 * \return Container id on success/Negative on failure
 */
pdcid_t PDCcont_open(pdcid_t pdc_id, const char *cont_name);

/**
 * Iterate over containers within a PDC
 *
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Pointer to cont_handle struct/NULL on failure
 */
cont_handle *PDCcont_iter_start(pdcid_t pdc_id);

/**
 * Check if container handle is pointing to NULL
 *
 * \param chandle [IN]          Pointer to cont_handle struct,
 *                              returned by PDCcont_iter_start(pdcid_t pdc_id)
 *
 * \return 1 on success/0 on failure
 */
pbool_t PDCcont_iter_null(cont_handle *chandle);

/**
 * Move to the next container within a PDC
 * \param chandle [IN]          Pointer to cont_handle struct, returned by
 *                              PDCcont_iter_start(pdcid_t pdc_id)
 *
 * \return Pointer to cont_handle struct/NULL on failure
 */
cont_handle *PDCcont_iter_next(cont_handle *chandle);

/**
 * Retrieve container information
 *
 * \param chandle [IN]          A cont_handle struct, returned by
 *                              PDCcont_iter_start(pdcid_t pdc_id)
 *
 * \return Pointer to a PDC_cont_info struct/NULL on failure
 */
struct PDC_cont_info * PDCcont_iter_get_info(cont_handle *chandle);

/**
 * Persist a transient container
 *
 * \param cont_id [IN]          Id of the container, returned by
 *                              PDCcont_open(pdcid_t pdc_id, const char *cont_name)
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_persist(pdcid_t cont_id, pdcid_t pdc_id);

/**
 * Set container lifetime
 *
 * \param cont_create_prop [IN] Id of container property, returned by
 *                              PDCprop_create(PDC_CONT_CREATE)
 * \param cont_lifetime [IN]    container lifetime (enum type), PDC_PERSIST or
 *                              PDC_TRANSIENT
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_cont_lifetime(pdcid_t cont_create_prop, PDC_lifetime cont_lifetime, pdcid_t pdc_id);

/**
 * Close a container
 *
 * \param cont_id [IN]          Container id, returned by
 *                              PDCcont_open(pdcid_t pdc_id, const char *cont_name)
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_close(pdcid_t cont_id, pdcid_t pdc);

/**
 * PDC container finalize
 *
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_end(pdcid_t pdc_id);

/**
 * Check if container list is empty
 *
 * \param pdc_id [IN]           Id of the PDC
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_cont_list_null(pdcid_t pdc_id);

#endif /* end _pdc_cont_H */
