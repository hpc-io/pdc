/* Model a transport which cannot register multiple caller segments. */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include "mercury_bulk.h"

typedef hg_return_t (*bulk_create_fn)(hg_class_t *, hg_uint32_t, void **, const hg_size_t *, hg_uint8_t,
                                      hg_bulk_t *);
hg_return_t
HG_Bulk_create(hg_class_t *hg_class, hg_uint32_t count, void **buffers, const hg_size_t *sizes,
               hg_uint8_t flags, hg_bulk_t *handle)
{
    static bulk_create_fn original;
    if (count > 1) {
        fprintf(stderr, "PDC test: rejected segmented registration\n");
        *handle = HG_BULK_NULL;
        return HG_OPNOTSUPPORTED;
    }
    if (original == NULL)
        original = (bulk_create_fn)dlsym(RTLD_NEXT, "HG_Bulk_create");
    if (original == NULL)
        abort();
    return original(hg_class, count, buffers, sizes, flags, handle);
}
