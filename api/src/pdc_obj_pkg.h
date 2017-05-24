#ifndef _pdc_obj_pkg_H
#define _pdc_obj_pkg_H

#include <stdbool.h>
#include <stddef.h>
#include "pdc_public.h"
#include "pdc_private.h"

struct region_map_list{
    pdcid_t                orig_reg_id;
    pdcid_t                des_obj_id;
    pdcid_t                des_reg_id;
    struct region_map_list *prev;
    struct region_map_list *next;
};

struct PDC_obj_info {
    const char             *name;
    pdcid_t                meta_id;
    pdcid_t                local_id;
    struct PDC_cont_info   *cont;
    struct PDC_obj_prop    *obj_pt;
    struct region_map_list *region_list_head;
};

struct PDC_region_info {
    pdcid_t             local_id;
    struct PDC_obj_info *obj;
    size_t              ndim;
    uint64_t            *offset;
    uint64_t            *size;
    bool                mapping;
//  PDC_loci    locus;
};

#endif
