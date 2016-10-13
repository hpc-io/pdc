#ifndef _pdc_malloc_H
#define _pdc_malloc_H

#include <stdlib.h>

void * PDC_malloc(size_t size);

void * PDC_calloc(size_t size);

#define PDC_MALLOC(t) (t *)PDC_malloc(sizeof(t))
#define PDC_CALLOC(t) (t *)PDC_calloc(sizeof(t))

#define PDC_FREE(t,obj) (t *)PDC_free(obj)
#endif
