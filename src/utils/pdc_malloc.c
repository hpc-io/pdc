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

#include <stdlib.h>
#include <assert.h>
#include "pdc_malloc.h"
#include "pdc_private.h"

void *
PDC_malloc(size_t size)
{
    void *ret_value;

    FUNC_ENTER(NULL);

    assert(size);

    if (size)
        ret_value = malloc(size);
    else
        ret_value = NULL;

    FUNC_LEAVE(ret_value);
}

void *
PDC_calloc(size_t size)
{
    void *ret_value;

    FUNC_ENTER(NULL);

    assert(size);

    if (size)
        ret_value = calloc(1, size);
    else
        ret_value = NULL;

    FUNC_LEAVE(ret_value);
}

void *
PDC_free(void *mem)
{
    void *ret_value = NULL;

    FUNC_ENTER(NULL);

    if (mem) {
        free(mem);
    }

    FUNC_LEAVE(ret_value);
}
