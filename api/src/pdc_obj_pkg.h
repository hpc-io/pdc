#ifndef _pdc_obj_pkg_H
#define _pdc_obj_pkg_H

#include <stdbool.h>
#include <stddef.h>
#include "pdc_public.h"
#include "pdc_private.h"


struct PDC_obj_info {
    const char  *name;
    pdcid_t     pdc;
    pdcid_t     cont;
    pdcid_t     obj_prop;
    pdcid_t     meta_id;
    pdcid_t     local_id;
//  bool        mapping;
};

struct PDC_region_info {
    size_t      ndim;
    uint64_t   *offset;
    uint64_t   *size;
    pdcid_t     local_id;
    bool        mapping;
//  PDC_loci    locus;
};

#endif
