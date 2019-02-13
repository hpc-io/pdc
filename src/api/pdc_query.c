#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctype.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>


#include "pdc_public.h"
#include "pdc_client_server_common.h"
#include "pdc_query.h"


pdcquery_t *PDCquery_create(pdcid_t obj_id, pdcquery_op_t op, 
                               PDC_var_type_t type, void *value)
{
    pdcquery_t *query;
    int         type_size;

    FUNC_ENTER(NULL);

    if (NULL == query || NULL == value) 
        return NULL;

    query->constraint           = (pdcquery_constraint_t*)calloc(1, sizeof(pdcquery_constraint_t));
    query->constraint->obj_id   = obj_id;
    query->constraint->op       = op;
    query->constraint->type     = type;
    query->constraint->val_size = PDC_get_var_type_size(type);
    query->constraint->val      = malloc(query->constraint->val_size);
    memcpy(query->constraint->val, value, query->constraint->val_size);

    return query;
}

perr_t PDCquery_and(pdcquery_t *constraint, pdcquery_t *constraint1, pdcquery_t *constraint2)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (NULL == constraint || NULL == constraint1 ||NULL == constraint2) {
        ret_value = FAIL;
        goto done;
    }

    constraint->left       = constraint1;
    constraint->right      = constraint1;
    constraint->constraint = NULL;
    constraint->combine_op = PDC_QUERY_AND;

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_or(pdcquery_t *constraint, pdcquery_t *constraint1, pdcquery_t *constraint2)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (NULL == constraint || NULL == constraint1 ||NULL == constraint2) {
        ret_value = FAIL;
        goto done;
    }

    constraint->left       = constraint1;
    constraint->right      = constraint1;
    constraint->constraint = NULL;
    constraint->combine_op = PDC_QUERY_OR;

done:
    FUNC_LEAVE(ret_value);
}

void PDCquery_free(pdcquery_t *query)
{
    if (NULL == query) 
        return;

    if (NULL == query->left && NULL == query->right) {
        if (query->constraint) 
            free(query->constraint);
    }
    else {
        if (NULL != query->left) 
            PDCquery_free(query->left);
        if (NULL != query->right) 
            PDCquery_free(query->right);
    }
}

perr_t PDCquery_get_nhits(pdcquery_t *constraint, int *n)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);



done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_get_selection(pdcquery_t *constraint, pdcselection_t *sel)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);



done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_get_data(pdcid_t obj_id, pdcselection_t *sel, void *obj_data)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);



done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_get_histogram(pdcid_t obj_id, pdcselection_t *sel, void *obj_hist)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);



done:
    FUNC_LEAVE(ret_value);
}

