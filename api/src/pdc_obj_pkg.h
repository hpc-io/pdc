#ifndef _pdc_obj_pkg_H
#define _pdc_obj_pkg_H

#include "pdc_public.h"
#include "pdc_private.h"

typedef struct PDC_obj_t {
    const char  *name;
    pdcid_t     cont;
    pdcid_t     obj_prop;
}PDC_obj_t;

typedef struct obj_handle {
    struct PDC_id_type *type_ptr;
    pdcid_t       pdc;
    int           count;
    struct PDC_id_info *current;
} obj_handle;

typedef struct PDC_obj_info_t {
    const char  *name;
} PDC_obj_info_t;

#endif
