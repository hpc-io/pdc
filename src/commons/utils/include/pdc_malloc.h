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

#ifndef PDC_MALLOC_H
#define PDC_MALLOC_H

#include <stdlib.h>

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
/**
 * Create an object
 *
 * \param size [IN]             Size of the struct to be malloced
 */
void *PDC_malloc(size_t size);

/**
 * Create an object
 *
 * \param size [IN]             Size of the struct to be calloced
 */
void *PDC_calloc(size_t size);

/**
 * Create an object
 *
 * \param mem [IN]              Starting address of memory
 */
void *PDC_free(void *mem);

#define PDC_MALLOC(t) (t *)PDC_malloc(sizeof(t))
#define PDC_CALLOC(t) (t *)PDC_calloc(sizeof(t))

#define PDC_FREE(t, obj) (t *)(intptr_t) PDC_free(obj)

#endif /* PDC_MALLOC_H */
