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

#ifndef _pdc_obj_pkg_H
#define _pdc_obj_pkg_H

#include <stdbool.h>
#include <stddef.h>
#include "pdc_public.h"
#include "pdc_private.h"

struct region_map_list{
    pdcid_t                orig_reg_id;
    pdcid_t                des_obj_id;
    pdcid_t                des_reg_id;
    struct region_map_list *prev;
    struct region_map_list *next;
};

typedef enum {
    PDC_OBJ_GLOBAL,
    PDC_OBJ_LOCAL
} PDCobj_location;

typedef enum { NA=0, READ=1, WRITE=2 } PDC_access_t;
typedef enum { BLOCK=0, NOBLOCK=1 }    PDC_lock_mode_t;

struct PDC_obj_info {
    char                   *name;
    pdcid_t                 meta_id;
    pdcid_t                 local_id;
    int32_t                 client_id;
    PDCobj_location         location;
    void                   *metadata;
    struct PDC_cont_info   *cont;
    struct PDC_obj_prop    *obj_pt;
    struct region_map_list *region_list_head;
};

struct PDC_region_info {
    pdcid_t              local_id;
    struct PDC_obj_info *obj;
    size_t               ndim;
    uint64_t            *offset;
    uint64_t            *size;
    bool                 mapping;
    bool                 registered_transform;
    void                *buf;
};

#endif
