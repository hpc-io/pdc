#ifndef _pdc_query_H
#define _pdc_query_H

#include "pdc_client_server_common.h"
#include "pdc_public.h"

typedef enum { PDC_OP_NONE=0, PDC_GT=1, PDC_LT=2, PDC_GTE=3, PDC_LTE=4, PDC_EQ=5} pdcquery_op_t; 
typedef enum { PDC_QUERY_NONE=0, PDC_QUERY_AND=1, PDC_QUERY_OR=2 } pdcquery_combine_op_t; 

typedef struct pdcquery_constraint_t {
    pdcid_t            obj_id;
    PDC_var_type_t     type;
    pdcquery_op_t      op;
    int                val_size;
    void               *val;
} pdcquery_constraint_t;

typedef struct pdcquery_t {
    pdcquery_constraint_t *constraint;
    pdcquery_constraint_t *left;
    pdcquery_constraint_t *right;
    pdcquery_combine_op_t combine_op;
} pdcquery_t;

typedef struct pdcquery_selection_t {
    int      ndim;
    int      is_point_sel;
    uint64_t start[DIM_MAX];
    uint64_t count[DIM_MAX];
    uint64_t n_points;
    uint64_t *points;
} pdcselection_t;

pdcquery_t *PDCquery_create(pdcid_t obj_id, pdcquery_op_t op, PDC_var_type_t type, void *value);
perr_t      PDCquery_and(pdcquery_t *query, pdcquery_t *query1, pdcquery_t *query2);
perr_t      PDCquery_or(pdcquery_t *query, pdcquery_t *query1, pdcquery_t *query2);
void        PDCquery_free(pdcquery_t *query);

perr_t      PDCquery_get_selection(pdcquery_t *query, pdcselection_t *sel);
perr_t      PDCquery_get_nhits(pdcquery_t *query, int *n);
perr_t      PDCquery_get_data(pdcid_t obj_id, pdcselection_t *sel, void *obj_data);
perr_t      PDCquery_get_histogram(pdcid_t obj_id, pdcselection_t *sel, void *obj_hist);

#endif
