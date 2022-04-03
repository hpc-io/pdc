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
#ifndef PDC_TRANSFORMS_H
#define PDC_TRANSFORMS_H

#include "pdc_private.h"
#include "pdc_transform.h"

#define DATA_ANY 7

/***************************/
/* Library Private Structs */
/***************************/
struct _pdc_region_transform_ftn_info {
    pdcid_t                 object_id;
    pdcid_t                 region_id;
    int                     local_regIndex;
    int                     meta_index;
    struct pdc_region_info *src_region;
    struct pdc_region_info *dest_region;
    size_t (*ftnPtr)();
    int                      ftn_lastResult;
    int                      readyState;
    int                      nextState;
    int                      client_id;
    size_t                   type_extent;
    size_t                   dest_extent;
    pdc_var_type_t           type;
    pdc_var_type_t           dest_type;
    pdc_obj_transform_t      op_type;
    pdc_data_movement_t      when;
    _pdc_analysis_language_t lang;
    void *                   data;
    void *                   result;
};

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * To end PDC transform
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_transform_end();

#endif /* PDC_TRANSFORMS_H */
