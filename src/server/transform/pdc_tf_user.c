/**
 * NOTICE: This file is included in a custom transformation libaries.
 * It should not include any headers that are not available to the custom transformation library.
 * It should also not include any headers that define symbols that
 * may conflict with symbols in the custom transformation library.
 */

#include "pdc_tf_user.h"
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

bool
PDCtf_set_func_param(pdc_dg_t *dg, char *func_name, pdc_tf_dev_t dev, uint64_t flat_conceptual_offset,
                     void *params, uint64_t params_size)
{

    bool                 ret_value      = true;
    PDC_VECTOR_ITERATOR *dg_params_iter = NULL;

    // Find edge with name
    for (int i = 0; i < dg->edge_count; i++) {
        pdc_dg_edge_t *edge    = dg->edges[i];
        pdc_tf_func_t *tf_func = edge->data;

        if (!strcmp(func_name, tf_func->name)) {
            // Create vector if vector is NULL
            if (tf_func->pdc_tf_dg_params_vector == NULL)
                tf_func->pdc_tf_dg_params_vector = pdc_vector_create(8, 2.0);

            // Locate the params by conceptual offset
            pdc_tf_dg_params_t *pdc_dg_params = NULL;
            dg_params_iter                    = pdc_vector_iterator_new(tf_func->pdc_tf_dg_params_vector);
            while (pdc_vector_iterator_has_next(dg_params_iter)) {
                pdc_dg_params = pdc_vector_iterator_next(dg_params_iter);
                if (pdc_dg_params->flat_conceptual_offset == flat_conceptual_offset) {
                    pdc_dg_params->params      = params;
                    pdc_dg_params->params_size = params_size;
                    goto done;
                }
            }

            // Append new entry
            pdc_dg_params = calloc(1, sizeof(pdc_tf_dg_params_t));
            pdc_vector_add(tf_func->pdc_tf_dg_params_vector, pdc_dg_params);

            pdc_dg_params->flat_conceptual_offset = flat_conceptual_offset;
            pdc_dg_params->params                 = params;
            pdc_dg_params->params_size            = params_size;
            goto done;
        }
    }

    printf("Edge %s not found\n", func_name);

done:
    if (dg_params_iter != NULL)
        pdc_vector_iterator_destroy(dg_params_iter);
    return ret_value;
}

bool
PDCtf_get_func_param(pdc_dg_t *dg, char *func_name, pdc_tf_dev_t dev, uint64_t flat_conceptual_offset,
                     void **params, uint64_t *params_size)
{
    bool                 ret_value      = true;
    PDC_VECTOR_ITERATOR *dg_params_iter = NULL;

    // Find edge with name
    for (int i = 0; i < dg->edge_count; i++) {
        pdc_dg_edge_t *edge    = dg->edges[i];
        pdc_tf_func_t *tf_func = edge->data;
        if (!strcmp(func_name, tf_func->name) && tf_func->dev == dev) {
            // Check if vector is NULL
            if (tf_func->pdc_tf_dg_params_vector == NULL) {
                printf("tf_func->pdc_tf_dg_params_vector was NULL\n");
                return false;
                goto done;
            }

            // Locate the params by conceptual offset
            dg_params_iter = pdc_vector_iterator_new(tf_func->pdc_tf_dg_params_vector);
            while (pdc_vector_iterator_has_next(dg_params_iter)) {
                pdc_tf_dg_params_t *pdc_dg_params = pdc_vector_iterator_next(dg_params_iter);
                if (pdc_dg_params->flat_conceptual_offset == flat_conceptual_offset) {
                    *params      = pdc_dg_params->params;
                    *params_size = pdc_dg_params->params_size;
                    goto done;
                }
            }
            printf("Failed to locate params in func_name %s by flat conceptual offset %lu\n", func_name,
                   flat_conceptual_offset);
            return false;
            goto done;
        }
    }

    printf("Edge %s not found\n", func_name);
    ret_value = false;

done:
    if (dg_params_iter != NULL)
        pdc_vector_iterator_destroy(dg_params_iter);
    return ret_value;
}

bool
PDCtf_set_state_param(pdc_dg_t *dg, char *state_name, uint64_t flat_conceptual_offset, void *params,
                      uint64_t params_size)
{
    bool                 ret_value      = true;
    PDC_VECTOR_ITERATOR *dg_params_iter = NULL;

    // Get state from graph
    pdc_tf_state_t query_stat;
    query_stat.name = state_name;

    pdc_dg_vertex_id_t vert = PDCdg_vertex_exists(dg, &query_stat);
    if (vert == PDC_DG_INVALID_VERTEX) {
        printf("Failed to find state in PDCtf_set_state_param\n");
        ret_value = false;
        goto done;
    }

    // Get the tf state
    pdc_tf_state_t *tf_state = (pdc_tf_state_t *)dg->vertices[vert]->data;
    if (tf_state == NULL) {
        printf("Vertex data was NULL\n");
        ret_value = false;
        goto done;
    }

    // Create vector if vector is NULL
    if (tf_state->pdc_tf_dg_params_vector == NULL)
        tf_state->pdc_tf_dg_params_vector = pdc_vector_create(8, 2.0);

    // Locate the params by conceptual offset
    pdc_tf_dg_params_t *pdc_dg_params = NULL;
    dg_params_iter                    = pdc_vector_iterator_new(tf_state->pdc_tf_dg_params_vector);
    while (pdc_vector_iterator_has_next(dg_params_iter)) {
        pdc_dg_params = pdc_vector_iterator_next(dg_params_iter);
        if (pdc_dg_params->flat_conceptual_offset == flat_conceptual_offset) {
            pdc_dg_params->params      = params;
            pdc_dg_params->params_size = params_size;
            goto done;
        }
    }

    pdc_dg_params = calloc(1, sizeof(pdc_tf_dg_params_t));
    pdc_vector_add(tf_state->pdc_tf_dg_params_vector, pdc_dg_params);

    pdc_dg_params->params                 = params;
    pdc_dg_params->params_size            = params_size;
    pdc_dg_params->flat_conceptual_offset = flat_conceptual_offset;

done:
    if (dg_params_iter != NULL)
        pdc_vector_iterator_destroy(dg_params_iter);
    return ret_value;
}

bool
PDCtf_get_state_param(pdc_dg_t *dg, char *state_name, uint64_t flat_conceptual_offset, void **params,
                      uint64_t *params_size)
{
    bool                 ret_value      = true;
    PDC_VECTOR_ITERATOR *dg_params_iter = NULL;

    // Get state from graph
    pdc_tf_state_t query_stat = {.name = state_name};

    pdc_dg_vertex_id_t vert = PDCdg_vertex_exists(dg, &query_stat);
    if (vert == PDC_DG_INVALID_VERTEX) {
        printf("Failed to find state in PDCtf_get_state_param\n");
        ret_value = false;
        goto done;
    }

    // Get the tf state
    pdc_tf_state_t *tf_state = (pdc_tf_state_t *)dg->vertices[vert]->data;
    if (tf_state == NULL) {
        printf("Vertex data was NULL\n");
        ret_value = false;
        goto done;
    }

    // Check if vector is NULL
    if (tf_state->pdc_tf_dg_params_vector == NULL) {
        printf("tf_state->pdc_tf_dg_params_vector was NULL\n");
        ret_value = false;
        goto done;
    }

    // Locate the params by conceptual offset
    dg_params_iter = pdc_vector_iterator_new(tf_state->pdc_tf_dg_params_vector);
    while (pdc_vector_iterator_has_next(dg_params_iter)) {
        pdc_tf_dg_params_t *pdc_dg_params = pdc_vector_iterator_next(dg_params_iter);
        if (pdc_dg_params->flat_conceptual_offset == flat_conceptual_offset) {
            *params      = pdc_dg_params->params;
            *params_size = pdc_dg_params->params_size;
            goto done;
        }
    }

    printf("Failed to locate params in state_name %s by flat conceptual offset %lu\n", state_name,
           flat_conceptual_offset);

done:
    if (dg_params_iter != NULL)
        pdc_vector_iterator_destroy(dg_params_iter);
    return ret_value;
}
