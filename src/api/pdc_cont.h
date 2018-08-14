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

#ifndef _pdc_cont_H
#define _pdc_cont_H

#include "pdc_error.h"
#include "pdc_cont_pkg.h"
#include "pdc_interface.h"
#include "pdc_life.h"

typedef struct PDC_id_info cont_handle;

/**
 * Create a container
 *
 * \param cont_name [IN]        Name of the container
 * \param cont_create_prop [IN] Id of container property
 *                              returned by PDCprop_create(PDC_CONT_CREATE)
 *
 * \return Container id on success/Zero on failure
 */
pdcid_t PDCcont_create(const char *cont_name, pdcid_t cont_create_prop);

pdcid_t PDCcont_create_col(const char *cont_name, pdcid_t cont_prop_id);

/**
 * Open a container
 *
 * \param cont_name [IN]        Name of the container
 *
 * \return Container id on success/Zero on failure
 */
pdcid_t PDCcont_open(const char *cont_name);

/**
 * Iterate over containers within a PDC
 *
 * \return Pointer to cont_handle struct/NULL on failure
 */
cont_handle *PDCcont_iter_start();

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
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_persist(pdcid_t cont_id);

/**
 * Set container lifetime
 *
 * \param cont_create_prop [IN] Id of container property, returned by
 *                              PDCprop_create(PDC_CONT_CREATE)
 * \param cont_lifetime [IN]    container lifetime (enum type), PDC_PERSIST or
 *                              PDC_TRANSIENT
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCprop_set_cont_lifetime(pdcid_t cont_create_prop, PDC_lifetime cont_lifetime);

/**
 * Close a container
 *
 * \param cont_id [IN]          Container id, returned by
 *                              PDCcont_open(pdcid_t pdc_id, const char *cont_name)
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCcont_close(pdcid_t cont_id);

#endif 
