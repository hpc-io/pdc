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

#ifndef _pdc_obj_object_H
#define _pdc_obj_object_H

/**
 * PDC object initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc_obj_init();

/**
 * PDC region initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc_region_init();

/**
 * PDC object finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc_obj_end();

/**
 * PDC region finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t pdc_region_end();

/**
 * Check if object list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t pdc_obj_list_null();

/**
 * Check if region list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t pdc_region_list_null();

#endif
