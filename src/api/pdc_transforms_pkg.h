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

#include "pdc_obj.h"
#include "pdc_private.h"
#include "pdc_prop.h"
#include "mercury_proc_string.h"

typedef enum {
    PDC_TESTING = 0,
    PDC_FILE_IO = 1,
    PDC_DATA_MAP = 2,
    PDC_PRE_ANALYSIS = 4,
    PDC_POST_ANALYSIS = 8
} PDCobj_transform_t;

typedef enum {
    DECR_STATE = -100,
    INCR_STATE = 100,
   _STATIC_STATE = 0
} PDCstate_next_t;

typedef enum {
    DATA_IN = 1,
    DATA_OUT = 2,
    DATA_RELOCATION = 4
} PDCdata_movement_t;
	      
#define  DATA_ANY  7

struct region_transform_ftn_info {
    pdcid_t object_id;
    pdcid_t region_id;
    int local_regIndex;    
    int meta_index;
    struct PDC_region_info *src_region;
    struct PDC_region_info *dest_region;
    size_t (*ftnPtr)();
    int ftn_lastResult;
    int readyState;
    int nextState;
    int client_id;
    size_t type_extent;
    size_t dest_extent;
    PDC_var_type_t   type;
    PDC_var_type_t   dest_type;
    PDCobj_transform_t op_type;
    PDCdata_movement_t when;
    PDC_Analysis_language lang;
    void *data;
    void *result;
};

struct transform_ftn_info {
    int current_state;
    int transform_state;
    pdcid_t *object_id;
    pdcid_t *region_id;
    size_t (*ftnPtr)();
};

typedef struct {
    hg_const_string_t           ftn_name;
    hg_const_string_t           loadpath;
    pdcid_t                     object_id;
    pdcid_t                     region_id;
    int32_t                     client_index;
    int32_t                     operation_type; /* When, e.g. during mapping */
    int32_t                     start_state;
    int32_t                     next_state;
    int8_t                      op_type;
    int8_t                      when;
} transform_ftn_in_t;

typedef struct {
    pdcid_t                     object_id;
    pdcid_t                     region_id;
    int32_t                     client_index;
    int32_t                     ret;
} transform_ftn_out_t;


#endif
