#ifndef _pdc_obj_pkg_H
#define _pdc_obj_pkg_H

#include "pdc_public.h"
#include "pdc_private.h"

typedef struct PDC_obj_info_t {
    const char  *name;
    pdcid_t     pdc;
    pdcid_t     cont;
    pdcid_t     obj_prop;
}PDC_obj_info_t;

#endif
