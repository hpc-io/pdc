#ifndef _pdc_cont_pkg_H
#define _pdc_cont_pkg_H


struct PDC_cont_info {
    const char             *name;
    pdcid_t                local_id;
    struct PDC_cont_prop   *cont_pt;
};


#endif
