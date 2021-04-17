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


#ifndef PDC_ANALYSIS_SERVER_H
#define PDC_ANALYSIS_SERVER_H

#include "pdc_public.h"
#include "pdc_analysis_and_transforms_common.h"

#define PDC_TIMING 1

#if PDC_TIMING == 1
typedef struct pdc_server_timing {
    double PDCbuf_obj_map_rpc;
    double PDCbuf_obj_unmap_rpc;
    double PDCreg_obtain_lock_rpc;
    double PDCreg_release_lock_rpc;
} pdc_server_timing;

typedef struct pdc_server_timestamp {
    double *start;
    double *end;
    size_t timestamp_max_size;
    size_t timestamp_size;
} pdc_server_timestamp;

pdc_server_timing *server_timings;
pdc_server_timestamp *buf_obj_map_timestamps;
pdc_server_timestamp *buf_obj_unmap_timestamps;
pdc_server_timestamp *obtain_lock_timestamps;
pdc_server_timestamp *release_lock_timestamps;
double base_time;

int PDC_server_timing_init();
int PDC_server_timing_report();
int PDC_server_timestamp_register(pdc_server_timestamp *timestamp, double start, double end);

#endif

perr_t PDC_Server_instantiate_data_iterator(obj_data_iterator_in_t *in, obj_data_iterator_out_t *out);

int PDC_get_analysis_registry(struct _pdc_region_analysis_ftn_info ***registry);

#endif
