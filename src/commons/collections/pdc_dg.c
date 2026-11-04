#include "pdc_dg.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static bool
resize_dg(pdc_dg_t *dg, uint32_t new_vertex_count, uint32_t new_edge_count)
{
    bool ret_value = true;

    if (dg == NULL) {
        ret_value = false;
        goto done;
    }

    // Resize vertices if needed
    if (dg->vertex_capacity == 0)
        dg->vertex_capacity = 1;
    if (dg->vertex_capacity < new_vertex_count) {
        while (dg->vertex_capacity < new_vertex_count)
            dg->vertex_capacity *= 2;

        dg->vertices =
            (pdc_dg_vertex_t **)realloc(dg->vertices, sizeof(pdc_dg_vertex_t *) * dg->vertex_capacity);
    }
    // Resize edges if needed
    if (dg->edge_capacity == 0)
        dg->edge_capacity = 1;
    if (dg->edge_capacity < new_edge_count) {
        while (dg->edge_capacity < new_edge_count)
            dg->edge_capacity *= 2;
        dg->edges = (pdc_dg_edge_t **)realloc(dg->edges, sizeof(pdc_dg_edge_t *) * dg->edge_capacity);
    }

done:
    return ret_value;
}

pdc_dg_t *
PDCdg_create(void *data, bool (*vertices_are_equal)(void *v1_data, void *v2_data),
             void (*dg_data_free)(void *data), void (*edge_data_free)(void *data),
             void (*vertex_data_free)(void *data))
{
    pdc_dg_t *ret_value = NULL;

    if (vertices_are_equal == NULL) {
        printf("vertices_are_equal function is required\n");
        goto done;
    }

    ret_value = (pdc_dg_t *)calloc(1, sizeof(pdc_dg_t));

    ret_value->vertex_capacity = PDC_DG_INIT_VERTEX_CAPACITY;
    ret_value->edge_capacity   = PDC_DG_INIT_EDGE_CAPACITY;
    ret_value->vertices = (pdc_dg_vertex_t **)calloc(ret_value->vertex_capacity, sizeof(pdc_dg_vertex_t *));
    ret_value->edges    = (pdc_dg_edge_t **)calloc(ret_value->edge_capacity, sizeof(pdc_dg_edge_t *));

    ret_value->vertex_count = 0;
    ret_value->edge_count   = 0;
    ret_value->data         = data;

    ret_value->dg_data_free       = dg_data_free;
    ret_value->edge_data_free     = edge_data_free;
    ret_value->vertex_data_free   = vertex_data_free;
    ret_value->vertices_are_equal = vertices_are_equal;

done:
    return ret_value;
}

void
PDCdg_destroy(pdc_dg_t *dg)
{
    if (dg == NULL) {
        printf("dg was NULL\n");
        return;
    }

    // first check that there are edges
    if (dg->edges) {
        for (int i = 0; i < dg->edge_count; i++) {
            /**
             * if user has defined a edge_data_free function
             * call it on each edge's data
             */
            if (dg->edges[i] && dg->edges[i]->data && dg->edge_data_free)
                dg->edge_data_free(dg->edges[i]->data);
            // free the edge
            if (dg->edges[i])
                free(dg->edges[i]);
        }
        free(dg->edges);
    }
    // first check that there are vertices
    if (dg->vertices) {
        for (int i = 0; i < dg->vertex_count; i++) {
            /**
             * if user has defined a vertex_data_free function
             * call it on each vertex's data
             */
            if (dg->vertices[i] && dg->vertices[i]->data && dg->vertex_data_free)
                dg->vertex_data_free(dg->vertices[i]->data);
            // free the vertex
            if (dg->vertices[i])
                free(dg->vertices[i]);
        }
        free(dg->vertices);
    }

    /**
     * if user has defined a dg_data_free function
     * call it on dg's data
     */
    if (dg->dg_data_free && dg->data)
        dg->dg_data_free(dg->data);

    free(dg);
}

pdc_dg_vertex_id_t
PDCdg_add_vertex(pdc_dg_t *dg, void *data)
{
    pdc_dg_vertex_id_t ret_value;

    if (dg == NULL) {
        printf("dg was NULL\n");
        ret_value = PDC_DG_INVALID_VERTEX;
        goto done;
    }

    if (resize_dg(dg, dg->vertex_count + 1, dg->edge_count) != true) {
        printf("Failed to resize dg\n");
        ret_value = PDC_DG_INVALID_VERTEX;
        goto done;
    }

    ret_value                          = dg->vertex_count;
    dg->vertices[ret_value]            = (pdc_dg_vertex_t *)calloc(1, sizeof(pdc_dg_vertex_t));
    dg->vertices[ret_value]->data      = data;
    dg->vertices[ret_value]->vertex_id = ret_value;

    dg->vertex_count++;

done:
    return ret_value;
}

pdc_dg_edge_id_t
PDCdg_add_edge(pdc_dg_t *dg, void *v1_data, void *v2_data, void *edge_data)
{
    pdc_dg_edge_id_t ret_value;

    if (dg == NULL) {
        printf("dg was NULL\n");
        return PDC_DG_INVALID_EDGE;
    }
    if (v1_data == NULL) {
        printf("v1_data was NULL\n");
        return PDC_DG_INVALID_EDGE;
    }
    if (v2_data == NULL) {
        printf("v2_data was NULL\n");
        return PDC_DG_INVALID_EDGE;
    }
    if (dg->vertices_are_equal(v1_data, v2_data)) {
        printf("Vertices of edge were not unique\n");
        return PDC_DG_INVALID_EDGE;
    }
    if (resize_dg(dg, dg->vertex_count, dg->edge_count + 1) != true) {
        printf("Failed to resize dg\n");
        return PDC_DG_INVALID_EDGE;
    }

    ret_value                     = dg->edge_count;
    dg->edges[ret_value]          = (pdc_dg_edge_t *)calloc(1, sizeof(pdc_dg_edge_t));
    dg->edges[ret_value]->data    = edge_data;
    dg->edges[ret_value]->edge_id = ret_value;

    pdc_dg_vertex_id_t v1_id, v2_id = PDC_DG_INVALID_VERTEX;

    if ((v1_id = PDCdg_vertex_exists(dg, v1_data)) == PDC_DG_INVALID_VERTEX)
        v1_id = PDCdg_add_vertex(dg, v1_data);
    if ((v2_id = PDCdg_vertex_exists(dg, v2_data)) == PDC_DG_INVALID_VERTEX)
        v2_id = PDCdg_add_vertex(dg, v2_data);

    dg->edges[ret_value]->v1_id = v1_id;
    dg->edges[ret_value]->v2_id = v2_id;

    dg->edge_count++;

    return ret_value;
}

pdc_dg_vertex_id_t
PDCdg_vertex_exists(pdc_dg_t *dg, void *vertex_data)
{
    if (dg == NULL) {
        printf("pdc_dg_has_vertex called with NULL dg\n");
        return PDC_DG_INVALID_VERTEX;
    }

    for (int i = 0; i < dg->vertex_count; i++) {
        if (dg->vertices_are_equal(dg->vertices[i]->data, vertex_data))
            return dg->vertices[i]->vertex_id;
    }

    return PDC_DG_INVALID_VERTEX;
}

bool
PDCdg_shortest_path(pdc_dg_t *dg, void *v1_data, void *v2_data, pdc_dg_edge_t **edges_out,
                    uint32_t *num_edges)
{
    bool                ret_value = false;
    bool *              visited   = NULL;
    pdc_dg_vertex_id_t *prev      = NULL;
    pdc_dg_vertex_id_t *queue     = NULL;
    pdc_dg_vertex_id_t *path      = NULL;
    *edges_out                    = NULL;
    *num_edges                    = 0;

    if (dg == NULL) {
        printf("dg was NULL\n");
        goto done;
    }

    // Look up vertex IDs
    pdc_dg_vertex_id_t from_vertex_id = PDCdg_vertex_exists(dg, v1_data);
    pdc_dg_vertex_id_t to_vertex_id   = PDCdg_vertex_exists(dg, v2_data);

    // Validate vertices
    if (from_vertex_id >= dg->vertex_count) {
        printf("Source vertex ID %u out of range (vertex_count=%u)\n", from_vertex_id, dg->vertex_count);
        goto done;
    }
    if (to_vertex_id >= dg->vertex_count) {
        printf("Destination vertex ID %u out of range (vertex_count=%u)\n", to_vertex_id, dg->vertex_count);
        goto done;
    }
    if (from_vertex_id == PDC_DG_INVALID_VERTEX) {
        printf("Source vertex not found\n");
        goto done;
    }
    if (to_vertex_id == PDC_DG_INVALID_VERTEX) {
        printf("Destination vertex not found\n");
        goto done;
    }

    uint32_t vertex_count = dg->vertex_count;

    // Allocate BFS data structures
    visited = (bool *)calloc(vertex_count, sizeof(bool));
    prev    = (pdc_dg_vertex_id_t *)malloc(vertex_count * sizeof(pdc_dg_vertex_id_t));
    queue   = (pdc_dg_vertex_id_t *)malloc(vertex_count * sizeof(pdc_dg_vertex_id_t));

    if (!visited || !prev || !queue) {
        printf("Failed to allocate BFS structures\n");
        goto done;
    }

    for (uint32_t i = 0; i < vertex_count; i++)
        prev[i] = PDC_DG_INVALID_VERTEX;

    // Breadth-first search
    uint32_t front = 0, rear = 0;
    visited[from_vertex_id] = true;
    queue[rear++]           = from_vertex_id;

    bool found = false;
    while (front < rear) {
        pdc_dg_vertex_id_t current = queue[front++];

        if (current == to_vertex_id) {
            found = true;
            break;
        }

        for (uint32_t i = 0; i < dg->edge_count; i++) {
            if (dg->edges[i]->v1_id == current) {
                pdc_dg_vertex_id_t neighbor = dg->edges[i]->v2_id;
                if (!visited[neighbor]) {
                    visited[neighbor] = true;
                    prev[neighbor]    = current;
                    queue[rear++]     = neighbor;
                }
            }
        }
    }

    if (!found) {
        printf("No path found from vertex %d to %d\n", from_vertex_id, to_vertex_id);
        goto done;
    }

    // Reconstruct the path in reverse
    uint32_t path_len = 0;
    for (pdc_dg_vertex_id_t at = to_vertex_id; at != PDC_DG_INVALID_VERTEX; at = prev[at])
        path_len++;

    if (path_len < 2) {
        printf("Path is too short to contain any edges\n");
        goto done;
    }

    // Recover path vertices
    path = (pdc_dg_vertex_id_t *)malloc(path_len * sizeof(pdc_dg_vertex_id_t));
    if (!path) {
        printf("Failed to allocate path array\n");
        goto done;
    }

    pdc_dg_vertex_id_t at = to_vertex_id;
    for (int i = path_len - 1; i >= 0; i--) {
        path[i] = at;
        at      = prev[at];
    }

    // Step 1: Count total edges along the path
    uint32_t total_edges = 0;
    for (uint32_t i = 0; i < path_len - 1; i++) {
        pdc_dg_vertex_id_t from = path[i];
        pdc_dg_vertex_id_t to   = path[i + 1];
        for (uint32_t j = 0; j < dg->edge_count; j++) {
            if (dg->edges[j]->v1_id == from && dg->edges[j]->v2_id == to) {
                total_edges++;
            }
        }
    }

    if (total_edges == 0) {
        printf("No edges found along path\n");
        goto done;
    }

    // Step 2: Allocate edges_out
    *edges_out = (pdc_dg_edge_t *)malloc(total_edges * sizeof(pdc_dg_edge_t));
    if (!*edges_out) {
        printf("Failed to allocate edges_out\n");
        goto done;
    }
    *num_edges = total_edges;

    // Step 3: Copy all edges
    uint32_t edge_idx = 0;
    for (uint32_t i = 0; i < path_len - 1; i++) {
        pdc_dg_vertex_id_t from = path[i];
        pdc_dg_vertex_id_t to   = path[i + 1];

        for (uint32_t j = 0; j < dg->edge_count; j++) {
            if (dg->edges[j]->v1_id == from && dg->edges[j]->v2_id == to) {
                memcpy(&(*edges_out)[edge_idx++], dg->edges[j], sizeof(pdc_dg_edge_t));
            }
        }
    }

    ret_value = true;

done:
    if (visited)
        free(visited);
    if (prev)
        free(prev);
    if (queue)
        free(queue);
    if (path)
        free(path);

    return ret_value;
}

/**
 * Retrieve data associated with vertex
 *
 * \param dg          Pointer to the graph.
 * \param vertex_id   Vertex id with data.
 *
 * \return Data of the vertex if it exists, or NULL.
 */
void *
PDCdg_get_vertex_data(pdc_dg_t *dg, pdc_dg_vertex_id_t vertex_id)
{
    void *ret_value = NULL;

    if (dg == NULL) {
        printf("dg was NULL\n");
        goto done;
    }
    if (dg->vertices == NULL) {
        printf("dg->vertices was NULL\n");
        goto done;
    }

    ret_value = dg->vertices[vertex_id]->data;

done:
    return ret_value;
}

/**
 * Retrieve data associated with edge
 *
 * \param dg          Pointer to the graph.
 * \param edge_id      Edge id with data.
 *
 * \return Data of the edge if it exists, or NULL.
 */
void *
PDCdg_get_edge_data(pdc_dg_t *dg, pdc_dg_edge_id_t edge_id)
{
    void *ret_value = NULL;

    if (dg == NULL) {
        printf("dg was NULL\n");
        goto done;
    }
    if (dg->edges == NULL) {
        printf("dg->edges was NULL\n");
        goto done;
    }

    ret_value = dg->edges[edge_id]->data;

done:
    return dg->edges[edge_id];
}