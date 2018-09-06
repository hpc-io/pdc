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

#ifndef _pdc_prop_pkg_H
#define _pdc_prop_pkg_H

#include "pdc_life.h"
#include "pdc_private.h"

typedef enum {
    PDC_CONT_CREATE = 0,
    PDC_OBJ_CREATE
} PDC_prop_type;

struct PDC_cont_prop {
    struct PDC_class *pdc;
    pdcid_t          cont_prop_id;
    PDC_lifetime     cont_life;
};

typedef struct pdc_kvtag_t {
    char            *name;
    uint32_t        size;
    void            *value;
} pdc_kvtag_t;


struct PDC_obj_prop {
    struct PDC_class *pdc;
    pdcid_t          obj_prop_id;
    PDC_lifetime     obj_life;
    size_t           ndim;
    uint64_t         *dims;
    PDC_var_type_t   type;
    uint32_t         user_id;
    char             *app_name;
    uint32_t         time_step;
    char             *data_loc;
    char             *tags;
    void             *buf;
    void             *metadata;
    pdc_kvtag_t      *kvtag;

    /* The following have been added to support of PDC analysis and transforms */
    size_t           type_extent;
    PDC_major_type   storage_order;
    uint64_t         locus;
    uint32_t         data_state;
};

#endif
