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

#ifndef PDC_TRANSFORMS_COMMON_H
#define PDC_TRANSFORMS_COMMON_H

#include "pdc_public.h"
#include "mercury.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
hg_id_t PDC_analysis_ftn_register(hg_class_t *hg_class);
hg_id_t PDC_transform_ftn_register(hg_class_t *hg_class);

/************************************/
/* Local Type and Struct Definition */
/************************************/
/* Define transform_ftn_in_t */
typedef struct transform_ftn_in_t {
    hg_const_string_t ftn_name;
    hg_const_string_t loadpath;
    pdcid_t           object_id;
    pdcid_t           region_id;
    int32_t           client_index;
    int32_t           operation_type; /* When, e.g. during mapping */
    int32_t           start_state;
    int32_t           next_state;
    int8_t            op_type;
    int8_t            when;
} transform_ftn_in_t;

/* Define transform_ftn_out_t */
typedef struct transform_ftn_out_t {
    pdcid_t object_id;
    pdcid_t region_id;
    int32_t client_index;
    int32_t ret;
} transform_ftn_out_t;

/* Define hg_proc_transform_ftn_in_t */
static HG_INLINE hg_return_t
hg_proc_transform_ftn_in_t(hg_proc_t proc, void *data)
{
    hg_return_t         ret;
    transform_ftn_in_t *struct_data = (transform_ftn_in_t *)data;
    ret                             = hg_proc_hg_const_string_t(proc, &struct_data->ftn_name);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_hg_const_string_t(proc, &struct_data->loadpath);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->object_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->region_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->client_index);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->operation_type);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->start_state);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->next_state);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int8_t(proc, &struct_data->op_type);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int8_t(proc, &struct_data->when);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }

    return ret;
}

/* Define hg_proc_transform_ftn_out_t */
static HG_INLINE hg_return_t
hg_proc_transform_ftn_out_t(hg_proc_t proc, void *data)
{
    hg_return_t          ret;
    transform_ftn_out_t *struct_data = (transform_ftn_out_t *)data;
    ret                              = hg_proc_uint64_t(proc, &struct_data->object_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_uint64_t(proc, &struct_data->region_id);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->client_index);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    ret = hg_proc_int32_t(proc, &struct_data->ret);
    if (ret != HG_SUCCESS) {
        // HG_LOG_ERROR("Proc error");
        return ret;
    }
    return ret;
}

#endif /* PDC_TRANSFORMS_COMMON_H */
