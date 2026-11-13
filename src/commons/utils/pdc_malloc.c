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
#include "pdc_timing.h"

// see comment in pdc_stack_ops.c
#ifdef ENABLE_PROFILING
#undef FUNC_ENTER
#undef FUNC_LEAVE
#define FUNC_ENTER(x)
#define FUNC_LEAVE(x) return (x)
#endif

#ifdef HAVE_MALLOC_USABLE_SIZE
#include <malloc.h>
#else
// alternative header files for malloc_usable_size or similar functions.
#endif

size_t PDC_mem_usage_g;

void *
PDC_malloc(size_t size)
{
    FUNC_ENTER(NULL);

    void *ret_value;

    assert(size);

    if (size == 0 || size > (size_t)1 << 40) /* 1TB sanity cap */
        FUNC_LEAVE(NULL);

    ret_value = malloc(size);

    if (ret_value)
        PDC_mem_usage_g += size;

    if (ret_value == NULL) {
        LOG_ERROR("PDC_malloc failed to allocate %zu bytes\n", size);
        abort();
    }

    FUNC_LEAVE(ret_value);
}

void *
PDC_malloc_addsize(size_t size, size_t *mem_usage_ptr)
{
    FUNC_ENTER(NULL);

    void *ret_value = PDC_malloc(size);
    if (ret_value && mem_usage_ptr)
        *mem_usage_ptr += size;

    FUNC_LEAVE(ret_value);
}

void *
PDC_calloc(size_t count, size_t size)
{
    FUNC_ENTER(NULL);

    void *ret_value;

    assert(count);
    assert(size);

    /* 1TB sanity cap on each argument, matching PDC_malloc's cap -- count
     * used to cap at 1<<32 (4GB), which calls like
     * PDC_calloc(region_size_in_bytes, sizeof(char)) legitimately exceed
     * for any single-region write/read at or above 4GB (e.g. an 8GB
     * region landing on a single data server), silently returning NULL
     * for a perfectly valid request. */
    if (count == 0 || size == 0 || count > (size_t)1 << 40 || size > (size_t)1 << 40)
        FUNC_LEAVE(NULL);

    ret_value = calloc(count, size);

    if (ret_value)
        PDC_mem_usage_g += size;

    if (ret_value == NULL) {
        LOG_ERROR("PDC_calloc failed to allocate %zu bytes\n", count * size);
        abort();
    }

    FUNC_LEAVE(ret_value);
}

void *
PDC_calloc_addsize(size_t count, size_t size, size_t *mem_usage_ptr)
{
    FUNC_ENTER(NULL);

    void *ret_value = PDC_calloc(count, size);
    if (ret_value && mem_usage_ptr)
        *mem_usage_ptr += size;

    FUNC_LEAVE(ret_value);
}

void *
PDC_realloc_knowing_oldsize(void *ptr, size_t size, size_t old_size)
{
    FUNC_ENTER(NULL);

    void *ret_value;

    assert(size);
    size_t _old_size = old_size;
    if (size)
        ret_value = realloc(ptr, size);
    else
        ret_value = NULL;

    if (ret_value) {
        PDC_mem_usage_g += size;
        if (_old_size) {
            PDC_mem_usage_g -= _old_size;
        }
    }

    FUNC_LEAVE(ret_value);
}

void *
PDC_realloc(void *ptr, size_t size)
{
    FUNC_ENTER(NULL);

    size_t _old_size = 0;
#ifdef HAVE_MALLOC_USABLE_SIZE
    _old_size = malloc_usable_size(ptr);
#endif

    void *ret = PDC_realloc_knowing_oldsize(ptr, size, _old_size);
    if (ret == NULL) {
        LOG_ERROR("PDC_realloc failed to allocate %zu bytes\n", size);
        abort();
    }

    FUNC_LEAVE(ret);
}

void *
PDC_realloc_addsize_knowing_oldsize(void *ptr, size_t size, size_t old_size, size_t *mem_usage_ptr)
{
    FUNC_ENTER(NULL);

    size_t _old_size = old_size;
    void * ret_value = PDC_realloc_knowing_oldsize(ptr, size, _old_size);
    if (ret_value && mem_usage_ptr) {
        *mem_usage_ptr += size;
        if (_old_size) {
            *mem_usage_ptr -= _old_size;
        }
    }

    FUNC_LEAVE(ret_value);
}

void *
PDC_realloc_addsize(void *ptr, size_t size, size_t *mem_usage_ptr)
{
    FUNC_ENTER(NULL);

    size_t _old_size = 0;
#ifdef HAVE_MALLOC_USABLE_SIZE
    _old_size = malloc_usable_size(ptr);
#endif

    FUNC_LEAVE(PDC_realloc_addsize_knowing_oldsize(ptr, size, _old_size, mem_usage_ptr));
}

void *
PDC_free_knowing_old_size(void *mem, size_t old_size)
{
    FUNC_ENTER(NULL);

    size_t _old_size = old_size;
    void * ret_value = NULL;

    if (mem) {
        free(mem);
        if (_old_size)
            PDC_mem_usage_g -= _old_size;
    }

    FUNC_LEAVE(ret_value);
}

void *
PDC_free(void *mem)
{
    FUNC_ENTER(NULL);

    size_t _old_size = 0;
#ifdef HAVE_MALLOC_USABLE_SIZE
    _old_size = malloc_usable_size(mem);
#endif

    FUNC_LEAVE(PDC_free_knowing_old_size(mem, _old_size));
}

void
PDC_free_void(void *mem)
{
    FUNC_ENTER(NULL);

    PDC_free(mem);

    FUNC_LEAVE_VOID();
}

size_t
PDC_get_global_mem_usage()
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(PDC_mem_usage_g);
}