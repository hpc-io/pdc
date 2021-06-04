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

#ifndef PDC_HIST_H
#define PDC_HIST_H

#include "pdc_public.h"
#include "math.h"
#include <inttypes.h>
#include <string.h>

/**
 * ********
 *
 * \param obj_name [IN]         Data type
 * \param n [IN]                *******
 * \param data [IN]             *******
 *
 * \return ********
 */
pdc_histogram_t *PDC_gen_hist(pdc_var_type_t dtype, uint64_t n, void *data);

/**
 * ********
 *
 * \param hist [IN]             ********
 *
 * \return ********
 */
pdc_histogram_t *PDC_dup_hist(pdc_histogram_t *hist);

/**
 * ********
 *
 * \param n [IN]                ********
 * \param hist [IN]             ********
 *
 * \return ********
 */
pdc_histogram_t *PDC_merge_hist(int n, pdc_histogram_t **hists);

/**
 * ********
 *
 * \param hist [IN]             ********
 */
void PDC_free_hist(pdc_histogram_t *hist);

/**
 * ********
 *
 * \param hist [IN]             ********
 */
void PDC_print_hist(pdc_histogram_t *hist);

/**
 * ********
 *
 * \param to [IN]               ********
 * \param hist [IN]             ********
 */
void PDC_copy_hist(pdc_histogram_t *to, pdc_histogram_t *from);

#endif /* PDC_HIST_H */
