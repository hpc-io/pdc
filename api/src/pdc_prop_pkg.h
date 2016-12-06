#ifndef _pdc_prop_pkg_H
#define _pdc_prop_pkg_H

#include "pdc_life.h"
#include "pdc_private.h"

typedef enum {
    PDC_CONT_CREATE = 0,
    PDC_OBJ_CREATE
} PDC_prop_type;

typedef struct PDC_cont_prop_t {
    PDC_lifetime    cont_life;
}PDC_cont_prop_t;

typedef struct PDC_obj_prop_t {
    PDC_lifetime    obj_life;
    int             ndim;
    uint64_t        *dims;
    PDC_var_type_t  type;
    uint32_t        user_id;
    char*           app_name;
    uint32_t        time_step;
    char*           tags;              //placeholder, may change in the future
    void            *buf;
    pdcid_t         *region;
}PDC_obj_prop_t;

#endif
