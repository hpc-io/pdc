#ifndef _pdc_query_H
#define _pdc_query_H

#include "pdc_public.h"
#include "pdc_hist.h"

typedef enum { 
    PDC_OP_NONE = 0, 
    PDC_GT      = 1, 
    PDC_LT      = 2, 
    PDC_GTE     = 3, 
    PDC_LTE     = 4, 
    PDC_EQ      = 5
} pdcquery_op_t; 

typedef enum { 
    PDC_QUERY_NONE = 0, 
    PDC_QUERY_AND  = 1, 
    PDC_QUERY_OR   = 2 
} pdcquery_combine_op_t; 

typedef enum { 
    PDC_QUERY_GET_NONE  = 0, 
    PDC_QUERY_GET_NHITS = 1, 
    PDC_QUERY_GET_SEL   = 2, 
    PDC_QUERY_GET_DATA  = 3, 
    PDC_QUERY_GET_HIST  = 4
} pdcquery_get_op_t; 

typedef struct pdcquery_selection_t {
    int      is_point_sel;
    int      ndim;
    uint64_t start[4];  // DIM_MAX
    uint64_t count[4];
    uint64_t n_points;
    uint64_t *points;
} pdcselection_t;

typedef struct pdcquery_constraint_t {
    pdcid_t            obj_id;
    pdcquery_op_t      op;
    PDC_var_type_t     type;
    double             value;   // Use it as a generic 64bit value
    pdcselection_t     *sel;

    void               *storage_region_list_head;
    pdcid_t            origin_server;
    int                n_sent;
    int                n_recv;
} pdcquery_constraint_t;

typedef struct pdcquery_t {
    pdcquery_constraint_t  *constraint;
    struct pdcquery_t      *left;
    struct pdcquery_t      *right;
    pdcquery_combine_op_t  combine_op;
    struct PDC_region_info *region;
    pdcselection_t         *sel;
} pdcquery_t;

pdcquery_t *PDCquery_create(pdcid_t obj_id, pdcquery_op_t op, PDC_var_type_t type, void *value);
pdcquery_t *PDCquery_and(pdcquery_t *query1, pdcquery_t *query2);
pdcquery_t *PDCquery_or(pdcquery_t *query1, pdcquery_t *query2);
perr_t      PDCquery_sel_region(pdcquery_t *query, struct PDC_region_info *obj_region);

perr_t      PDCquery_get_selection(pdcquery_t *query, pdcselection_t *sel);
perr_t      PDCquery_get_nhits(pdcquery_t *query, int *n);
perr_t      PDCquery_get_data(pdcid_t obj_id, pdcselection_t *sel, void *obj_data);
perr_t      PDCquery_get_histogram(pdcid_t obj_id, void *hist);

#endif
