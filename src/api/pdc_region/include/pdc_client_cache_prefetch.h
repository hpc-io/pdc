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

#ifndef PDC_CLIENT_CACHE_PREFETCH_H
#define PDC_CLIENT_CACHE_PREFETCH_H

#include "pdc_public.h"
#include "pdc_obj.h"

/*******************************************************/
/* Public Functions for Client-side Region Prefetching */
/*******************************************************/
extern int obj_prefetch_list_len;

perr_t PDCregion_receive_prefetch_hint(pdcid_t *obj_arr, pdcid_t *reg_arr, int obj_array_len);
perr_t PDCregion_prefetch_by_objid();
perr_t PDCregion_print_prefetch_list();

/*******************************************************/
/* Private Functions for Client-side Region Prefetching */
/*******************************************************/

perr_t PDC_client_cache_prefetch_init();

#endif /* PDC_CLIENT_CACHE_PREFETCH_H */
