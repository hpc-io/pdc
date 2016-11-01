#ifndef _pdc_cont_pkg_H
#define _pdc_cont_pkg_H

typedef struct PDC_cont_t {
    const char  *name;
    pdcid_t     pdc;
    pdcid_t     cont_prop;
}PDC_cont_t;

typedef struct PDC_cont_info_t {
    const char  *name;
} PDC_cont_info_t;

typedef enum {
    PDC_PERSIST,
    PDC_TRANSIENT
} PDC_lifetime;

#endif
