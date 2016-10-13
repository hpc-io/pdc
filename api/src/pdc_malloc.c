#include <stdlib.h>
#include <assert.h>
#include "pdc_malloc.h"
#include "pdc_private.h"


void * PDC_malloc(size_t size)
{
    void *ret_value;

    assert(size);

    if(size)
        ret_value = malloc(size);
    else
        ret_value = NULL;

    FUNC_LEAVE(ret_value);
} /* end PDC_malloc() */


void * PDC_calloc(size_t size)
{
    void *ret_value;

    assert(size);

    if(size)
        ret_value = calloc(1, size);
    else
        ret_value = NULL;

    FUNC_LEAVE(ret_value);
} /* end PDC_calloc() */


void * PDC_free(void *mem) {
    void *ret_value = NULL;
    if(mem) {
        free(mem);
    }
    FUNC_LEAVE(ret_value);
} /* end PDC_free() */
