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

#ifndef PDC_PUBLIC_H
#define PDC_PUBLIC_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include "pdc_generic.h"

/*******************/
/* Public Typedefs */
/*******************/
typedef int                perr_t;
typedef uint64_t           pdcid_t;
typedef unsigned long long psize_t;
typedef bool               pbool_t;

typedef int    PDC_int_t;
typedef float  PDC_float_t;
typedef double PDC_double_t;

typedef pdc_c_var_type_t pdc_var_type_t;

// FIXME: common data structure should be defined in a group of common header files.
typedef struct pdc_kvtag_t {
    char *   name;
    uint32_t size;
    int8_t   type;
    void *   value;
} pdc_kvtag_t;

typedef enum { PDC_PERSIST, PDC_TRANSIENT } pdc_lifetime_t;

typedef enum { PDC_SERVER_DEFAULT = 0, PDC_SERVER_PER_CLIENT = 1 } pdc_server_selection_t;

typedef struct pdc_histogram_t {
    pdc_var_type_t dtype;
    int            nbin;
    double         incr;
    double *       range;
    uint64_t *     bin;
} pdc_histogram_t;

#define SUCCEED 0
#define FAIL    (-1)

#define PDC_SIZE_UNLIMITED UINT64_MAX

#endif /* PDC_PUBLIC_H */
