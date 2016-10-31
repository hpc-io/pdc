#ifndef _pdc_cont_H
#define _pdc_cont_H

#include "pdc_error.h"
#include "pdc_cont_pkg.h"
#include "pdc_interface.h"

/* PDC container initialization
 * Param pc [IN]: Pointer to PDC_CLASS_t struct
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCcont_init(PDC_CLASS_t *pc);

/* Create a container 
 * Param pdc_id [IN]: Id of the PDC
 * Param cont_name [IN]: Name of the container
 * Param cont_create_prop [IN]: Id of container property, returned by PDCprop_create(PDC_CONT_CREATE)
 * Return: Container id
 */
pdcid_t PDCcont_create(pdcid_t pdc, const char *cont_name, pdcid_t cont_create_prop);

/* Open a container 
 * Param pdc_id [IN]: Id of the PDC
 * Param cont_name [IN]: Name of the container 
 * Return: Container id 
 */
pdcid_t PDCcont_open(pdcid_t pdc_id, const char *cont_name);

/* Iterate over containers within a PDC
 * Param pdc_id [IN]: Id of the PDC
 * Return: Container handle
 */
cont_handle *PDCcont_iter_start(pdcid_t pdc_id);

/* Check if container handle is pointing to NULL 
 * Param chandle [IN]: A cont_handle struct, returned by PDCcont_iter_start(pdcid_t pdc_id)
 * Return: 1 in case of success or 0 in case of failure
 */
pbool_t PDCcont_iter_null(cont_handle *chandle);

/* Iterate the next container within a PDC 
 * Param chandle [IN]: A cont_handle struct, returned by PDCcont_iter_start(pdcid_t pdc_id)
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCcont_iter_next(cont_handle *chandle);

/* Retrieve container information
 * Param chandle [IN]: A cont_handle struct, returned by PDCcont_iter_start(pdcid_t pdc_id)
 * Return: Pointer to a PDC_cont_info_t struct
 */
PDC_cont_info_t * PDCcont_iter_get_info(cont_handle *chandle);

/* Persist a transient container
 * Param cont_id [IN]: Id of the container, returned by PDCcont_open(pdcid_t pdc_id, const char *cont_name)
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCcont_persist(pdcid_t cont_id);

/* Set container lifetime 
 * Param cont_create_prop [IN]: Id of container property, returned by PDCprop_create(PDC_CONT_CREATE)
 * Param cont_lifetime [IN]: container lifetime (enum type), PDC_PERSIST or PDC_TRANSIENT
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_cont_lifetime(pdcid_t cont_create_prop, PDC_lifetime cont_lifetime);

/* Close a container 
 * Param cont_id [IN]: Container id, returned by PDCcont_open(pdcid_t pdc_id, const char *cont_name)
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCcont_close(pdcid_t cont_id, pdcid_t pdc);

/* PDC container finalize
 * Return: Non-negative on success/Negative on failure
 */
perr_t PDCcont_end();

#endif /* end _pdc_cont_H */
