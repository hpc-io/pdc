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

#ifndef PDC_PROP_PKG_H
#define PDC_PROP_PKG_H

#include "pdc_private.h"

/*******************/
/* Private Typedefs */
/*******************/
struct _pdc_cont_prop {
    struct _pdc_class *pdc;
    pdcid_t            cont_prop_id;
    pdc_lifetime_t     cont_life;
};

typedef struct pdc_kvtag_t {
    char *   name;
    uint32_t size;
    void *   value;
} pdc_kvtag_t;

struct _pdc_transform_state {
    _pdc_major_type_t storage_order;
    pdc_var_type_t    dtype;
    size_t            ndim;
    uint64_t          dims[4];
    int               meta_index; /* transform to this state */
};

struct _pdc_obj_prop {
    struct pdc_obj_prop *obj_prop_pub;
    struct _pdc_class *  pdc;
    uint32_t             user_id;
    char *               app_name;
    uint32_t             time_step;
    char *               data_loc;
    char *               tags;
    void *               buf;
    pdc_kvtag_t *        kvtag;

    /* The following have been added to support of PDC analysis and transforms */
    size_t                      type_extent;
    uint64_t                    locus;
    uint32_t                    data_state;
    struct _pdc_transform_state transform_prop;
};

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * PDC container and object property initialization
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_prop_init();

/**
 * PDC container and object property finalize
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDC_prop_end();

/**
 * Check if object property list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_prop_obj_list_null();

/**
 * Check if container property list is empty
 *
 * \return SUCCEED if empty/FAIL if not empty
 */
perr_t PDC_prop_cont_list_null();

/**
 * Get object property infomation
 *
 * \param prop_id [IN]          ID of the object property
 *
 * \return Pointer to _pdc_obj_prop struct/Null on failure
 */
struct _pdc_obj_prop *PDC_obj_prop_get_info(pdcid_t obj_prop);

#endif /* PDC_PROP_PKG_H */
