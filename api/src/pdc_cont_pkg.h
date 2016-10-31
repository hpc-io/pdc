#ifndef _pdc_cont_pkg_H
#define _pdc_cont_pkg_H

typedef struct PDC_cont_t {
    const char  *name;
    pdcid_t     pdc;
    pdcid_t     cont_prop;
}PDC_cont_t;

typedef struct cont_handle {
    struct PDC_id_type *type_ptr;
    pdcid_t       pdc;
    int           count;
    struct PDC_id_info *current;
} cont_handle;

typedef struct PDC_cont_info_t {
    const char  *name;
} PDC_cont_info_t;

typedef enum {
    PDC_PERSIST,
    PDC_TRANSIENT
} PDC_lifetime;

#endif
