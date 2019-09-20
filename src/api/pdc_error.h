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

#ifndef PDC_ERROR_H
#define PDC_ERROR_H

#include <stdio.h>
#include <stdarg.h>
#include "pdc_public.h"
#include "pdc_private.h"

/*
 *  PDC Boolean type.
 */
#ifndef FALSE
#   define FALSE 0
#endif
#ifndef TRUE
#   define TRUE 1
#endif

extern pbool_t err_occurred;

/*
 * PGOTO_DONE macro. The argument is the return value which is
 * assigned to the `ret_value' variable.  Control branches to
 * the `done' label.
 */
#define PGOTO_DONE(ret_val)  do {       \
    ret_value = ret_val;                \
    goto done;                          \
} while (0) 

/*
 * PGOTO_ERROR macro. The arguments are the return value and an
 * error string.  The return value is assigned to a variable `ret_value' and
 * control branches to the `done' label.
 */
#define PGOTO_ERROR(ret_val, ...) do {                       \
    fprintf(stderr, "Error in %s:%d\n", __FILE__, __LINE__); \
    fprintf(stderr, " # %s(): ", __func__);                  \
    fprintf(stderr, __VA_ARGS__);                            \
    fprintf(stderr, "\n");                                   \
    PGOTO_DONE(ret_val);                                     \
}  while (0)

#endif  /* PDC_ERROR_H */
