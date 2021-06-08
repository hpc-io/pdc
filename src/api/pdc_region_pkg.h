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

#ifndef PDC_REGION_PKG_H
#define PDC_REGION_PKG_H

#include "pdc_private.h"

/**************************/
/* Library Private Struct */
/**************************/
struct region_map_list {
    pdcid_t                 orig_reg_id;
    pdcid_t                 des_obj_id;
    pdcid_t                 des_reg_id;
    struct region_map_list *prev;
    struct region_map_list *next;
};

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * PDC region initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_region_init();

/**
 * PDC region finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_region_end();

/**
 * Check if region list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_region_list_null();

#endif /* PDC_REGION_PKG_H */
