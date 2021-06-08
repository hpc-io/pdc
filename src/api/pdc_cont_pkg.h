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

#ifndef PDC_CONT_PKG_H
#define PDC_CONT_PKG_H

#include "pdc_public.h"

/**************************/
/* Library Private Struct */
/**************************/
struct _pdc_cont_info {
    struct pdc_cont_info * cont_info_pub;
    struct _pdc_cont_prop *cont_pt;
};

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * container initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_cont_init();

/**
 * Create a container locally
 *
 * \param pdc       [IN]        PDC ID
 * \param cont_name [IN]        Name of the container
 * \param cont_meta_id [out]    Metadata id of container
 *
 * \return Container id on success/Zero on failure
 */
pdcid_t PDC_cont_create_local(pdcid_t pdc, const char *cont_name, uint64_t cont_meta_id);

/**
 * Check if container list is empty
 *
 * \param pdc_id [IN]           ID of the PDC
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_cont_list_null();

/**
 * Return a container property
 *
 * \param cont_name [IN]        Name of the container
 *
 * \return Container struct on success/NULL on failure
 */
struct _pdc_cont_info *PDC_cont_get_info(pdcid_t cont_id);

/**
 * PDC container finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_cont_end();

#endif /* PDC_CONT_PKG_H */
