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
#ifndef PDC_ANALYSIS_COMMON_H
#define PDC_ANALYSIS_COMMON_H

#include "pdc_transforms_pkg.h"
#include "pdc_analysis_and_transforms.h"

int pdc_get_transforms(struct region_transform_ftn_info ***registry);
int pdc_get_analysis_registry(struct region_analysis_ftn_info ***registry);

int check_transform(PDCobj_transform_t op_type, struct PDC_region_info *dest_region);
int check_analysis(PDCobj_transform_t op_type, struct PDC_region_info *dest_region);

int pdc_add_transform_ptr_to_registry_(struct region_transform_ftn_info *ftnPtr);

int pdc_update_transform_server_meta_index(int client_index, int meta_index);

#endif
