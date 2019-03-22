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
    uint64_t   meta_id;

    FUNC_ENTER(NULL);

    if (obj_id == 0 || op == PDC_OP_NONE || NULL == value) 
        return NULL;

    if (pdc_find_id(obj_id) != NULL) {
        obj_prop = PDC_obj_get_info(obj_id);
        meta_id = obj_prop->meta_id;
    }
    else 
        meta_id = obj_id;

    query                       = (pdcquery_t*)calloc(1, sizeof(pdcquery_t));
    query->constraint           = (pdcquery_constraint_t*)calloc(1, sizeof(pdcquery_constraint_t));
    query->constraint->obj_id   = meta_id;    // Use global ID
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



perr_t PDCquery_get_nhits(pdcquery_t *query, uint64_t *n)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (query == NULL || n == NULL) {
        printf("==PDC %s - input NULL!\n");
        ret_value = FAIL;
        goto done;
    }

    ret_value = PDC_send_data_query(query, PDC_QUERY_GET_NHITS, n, NULL, NULL);

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_get_selection(pdcquery_t *query, pdcselection_t *sel)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (query == NULL || sel == NULL) {
        printf("==PDC_CLIENT[] %s - input NULL!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    memset(sel, 0, sizeof(pdcselection_t));
    ret_value = PDC_send_data_query(query, PDC_QUERY_GET_SEL, NULL, sel, NULL);

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_get_data(pdcid_t obj_id, pdcselection_t *sel, void *obj_data)
{
    perr_t ret_value = SUCCEED;
    struct PDC_obj_info *obj_prop;
    uint64_t   meta_id;

    FUNC_ENTER(NULL);

    if (obj_data == NULL || sel == NULL) {
        printf("==PDC_CLIENT[] %s - input NULL!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    if (pdc_find_id(obj_id) != NULL) {
        obj_prop = PDC_obj_get_info(obj_id);
        meta_id = obj_prop->meta_id;
    }
    else 
        meta_id = obj_id;

    ret_value = PDC_Client_get_sel_data(meta_id, sel, obj_data);

done:
    FUNC_LEAVE(ret_value);
}

perr_t PDCquery_get_histogram(pdcid_t obj_id, void *hist)
{
    perr_t ret_value = SUCCEED;
    struct PDC_obj_info *obj_prop;
    uint64_t   meta_id;

    FUNC_ENTER(NULL);

    if (pdc_find_id(obj_id) != NULL) {
        obj_prop = PDC_obj_get_info(obj_id);
        meta_id = obj_prop->meta_id;
    }
    else 
        meta_id = obj_id;

    /* ret_value = PDC_Client_get_sel_data(meta_id, sel, obj_data); */

done:
    FUNC_LEAVE(ret_value);
}

