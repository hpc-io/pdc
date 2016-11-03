#ifndef _pdc_cont_pkg_H
#define _pdc_cont_pkg_H

#include "PDC_life.h"

typedef struct PDC_cont_info_t {
    const char      *name;
    pdcid_t         pdc;
    pdcid_t         cont_prop;
    PDC_lifetime    cont_life;
}PDC_cont_info_t;


#endif
