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


pdcquery_t *PDCquery_create(pdcid_t obj_id, pdcquery_op_t op, PDC_var_type_t type, void *value)
{
    pdcquery_t *query;
    int         type_size;
    struct PDC_obj_info *obj_prop;

    FUNC_ENTER(NULL);

    if (obj_id == 0 || op == PDC_OP_NONE || NULL == value) 
        return NULL;

    obj_prop = PDC_obj_get_info(obj_id);
    if (obj_prop == NULL) {
        printf("== %s: Invalid obj_id!\n", __func__);
        return NULL;
    }

    query                       = (pdcquery_t*)calloc(1, sizeof(pdcquery_t));
    query->constraint           = (pdcquery_constraint_t*)calloc(1, sizeof(pdcquery_constraint_t));
    query->constraint->obj_id   = obj_prop->meta_id;    // Use global ID
    query->constraint->op       = op;
    query->constraint->type     = type;
    type_size = PDC_get_var_type_size(type);
    if (type_size > 8) {
        printf("Cannot handle value larger than 8 bytes!\n");
        return NULL;
    }
    memcpy(&query->constraint->value, value, type_size);

    return query;
}

perr_t PDCquery_sel_region(pdcquery_t *query, struct PDC_region_info *obj_region)
{
    if (NULL == query || NULL == obj_region) 
        return FAIL;

    query->region = obj_region;

    return SUCCEED;
}

pdcquery_t *PDCquery_and(pdcquery_t *q1, pdcquery_t *q2)
{
    pdcquery_t *query;

    FUNC_ENTER(NULL);

    if (NULL == q1 ||NULL == q2) 
        return NULL;

    query             = (pdcquery_t*)malloc(sizeof(pdcquery_t));
    query->left       = q1;
    query->right      = q2;
    query->constraint = NULL;
    query->combine_op = PDC_QUERY_AND;

    query->region = q1->region != NULL ? q1->region : q2->region;

    return query;
}

pdcquery_t *PDCquery_or(pdcquery_t *q1, pdcquery_t *q2)
{
    pdcquery_t *query;

    FUNC_ENTER(NULL);

    if (NULL == q1 ||NULL == q2) 
        return NULL;

    query             = (pdcquery_t*)malloc(sizeof(pdcquery_t));
    query->left       = q1;
    query->right      = q2;
    query->constraint = NULL;
    query->combine_op = PDC_QUERY_OR;

    query->region = q1->region != NULL ? q1->region : q2->region;

    return query;
}



perr_t PDCquery_get_nhits(pdcquery_t *query, int *n)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    ret_value = PDC_send_data_query(query, PDC_QUERY_GET_NHITS);


done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_get_selection(pdcquery_t *query, pdcselection_t *sel)
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

perr_t PDCquery_get_histogram(pdcid_t obj_id, void *hist)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);



done:
    FUNC_LEAVE(ret_value);
}

