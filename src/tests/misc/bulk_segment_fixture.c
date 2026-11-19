/* Model a vector-capable transport rejecting a particular registration. */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include "mercury.h"
#include "mercury_bulk.h"

static size_t
fixture_max_segments(const na_class_t *na)
{
    (void)na;
    return 2;
}

hg_class_t *
HG_Init_opt(const char *info, uint8_t listen, const struct hg_init_info *options)
{
    hg_class_t *(*original)(const char *, uint8_t, const struct hg_init_info *) =
        dlsym(RTLD_NEXT, "HG_Init_opt");
    if (original == NULL)
        abort();
    hg_class_t *hg = original(info, listen, options);
    if (hg != NULL) {
        na_class_t *na = HG_Core_class_get_na(hg->core_class);
        /* Only the injected segmented attempt sees this limit. All real
         * operations in the fallback use a single registered segment. */
        static struct na_class_ops fixture_ops;
        fixture_ops                             = *na->ops;
        fixture_ops.mem_handle_get_max_segments = fixture_max_segments;
        na->ops                                 = &fixture_ops;
    }
    return hg;
}

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
