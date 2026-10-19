#ifndef PDC_DG_H
#define PDC_DG_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#define PDC_DG_INIT_EDGE_CAPACITY   4
#define PDC_DG_INIT_VERTEX_CAPACITY 4
#define PDC_DG_INVALID_EDGE         -1
#define PDC_DG_INVALID_VERTEX       -1

/**
 * Type for directed graph vertex identifiers.
 */
typedef int pdc_dg_vertex_id_t;

/**
 * Type for directed graph edge identifiers.
 */
typedef int pdc_dg_edge_id_t;

/**
 * Represents a vertex in the directed graph.
 *
 * \param vertex_id  Unique identifier for the vertex.
 * \param data       Pointer to user-defined data associated with the vertex.
 */
typedef struct pdc_dg_vertex_t {
    pdc_dg_vertex_id_t vertex_id;
    void *             data;
} pdc_dg_vertex_t;

/**
 * Represents an edge in the directed graph.
 *
 * \param edge_id  Unique identifier for the edge.
 * \param v1_id    Source vertex ID.
 * \param v2_id    Destination vertex ID.
 * \param data     Pointer to user-defined data associated with the edge.
 */
typedef struct pdc_dg_edge_t {
    pdc_dg_edge_id_t   edge_id;
    pdc_dg_vertex_id_t v1_id;
    pdc_dg_vertex_id_t v2_id;
    void *             data;
} pdc_dg_edge_t;

/**
 * Represents a directed graph structure.
 *
 * All vertex and edge memory is managed internally. When the graph is destroyed,
 * the user-defined `vertex_data_free` and `edge_data_free` functions (if provided)
 * are called once for each vertex and edge respectively. The `dg_data_free`
 * function is called once for the graph-level data. All other memory is freed
 * by the library.
 *
 * \param edges            Array of pointers to edges.
 * \param vertices         Array of pointers to vertices.
 * \param edge_count       Number of edges currently in the graph.
 * \param vertex_count     Number of vertices currently in the graph.
 * \param vertex_capacity  Maximum number of vertices before resizing.
 * \param edge_capacity    Maximum number of edges before resizing.
 * \param data             Pointer to user-defined graph-level data.
 * \param vertices_are_equal Function to compare vertex data for equality.
 * \param dg_data_free     Function to free graph-level user data.
 * \param edge_data_free   Function to free edge-level user data.
 * \param vertex_data_free Function to free vertex-level user data.
 */
typedef struct pdc_dg_t {
    pdc_dg_edge_t **  edges;
    pdc_dg_vertex_t **vertices;

    uint32_t edge_count;
    uint32_t vertex_count;

    uint32_t vertex_capacity;
    uint32_t edge_capacity;

    void *data;

    bool (*vertices_are_equal)(void *v1_data, void *v2_data);
    void (*dg_data_free)(void *data);
    void (*edge_data_free)(void *data);
    void (*vertex_data_free)(void *data);
} pdc_dg_t;

/**
 * Create a new directed graph.
 *
 * Passing NULL for any of the free function pointers is allowed if the user
 * intends to manage the memory manually. However, the vertices_are_equal
 * function must be provided.
 *
 * \param data               User data to associate with the graph.
 * \param vertices_are_equal Function to compare two vertex data pointers.
 * \param dg_data_free       Function to free graph-level data (or NULL).
 * \param edge_data_free     Function to free edge-level data (or NULL).
 * \param vertex_data_free   Function to free vertex-level data (or NULL).
 *
 * \return Pointer to the new graph on success, NULL on failure.
 */
pdc_dg_t *PDCdg_create(void *data, bool (*vertices_are_equal)(void *v1_data, void *v2_data),
                       void (*dg_data_free)(void *data), void (*edge_data_free)(void *data),
                       void (*vertex_data_free)(void *data));

/**
 * Destroy a directed graph and free all associated memory.
 *
 *  This function will:
 *  - Free all edge data using `edge_data_free`, if provided.
 *  - Free all vertex data using `vertex_data_free`, if provided.
 *  - Free the graph-level user data using `dg_data_free`, if provided.
 *  - Deallocate all internal memory used by the graph.
 *
 * \param dg Pointer to the graph to destroy.
 */
void PDCdg_destroy(pdc_dg_t *dg);

/**
 * Add a vertex to the graph.
 *
 * \param dg    Pointer to the graph.
 * \param data  User data to associate with the vertex.
 *
 * \return ID of the new vertex, or PDC_DG_INVALID_VERTEX on failure.
 */
pdc_dg_vertex_id_t PDCdg_add_vertex(pdc_dg_t *dg, void *data);

/**
 * Add a directed edge between two vertices.
 *
 * \param dg        Pointer to the graph.
 * \param v1_data   Data of the source vertex.
 * \param v2_data   Data of the destination vertex.
 * \param edge_data User data to associate with the edge.
 *
 * \return ID of the new edge, or PDC_DG_INVALID_EDGE on failure.
 */
pdc_dg_edge_id_t PDCdg_add_edge(pdc_dg_t *dg, void *v1_data, void *v2_data, void *edge_data);

/**
 * Check whether a vertex with the given data exists in the graph.
 *
 * \param dg          Pointer to the graph.
 * \param vertex_data Vertex data to check.
 *
 * \return ID of the vertex if it exists, or PDC_DG_INVALID_VERTEX.
 */
pdc_dg_vertex_id_t PDCdg_vertex_exists(pdc_dg_t *dg, void *vertex_data);

/**
 * Retrieve data associated with vertex
 *
 * \param dg          Pointer to the graph.
 * \param vertex_id   Vertex id with data.
 *
 * \return Data of the vertex if it exists, or NULL.
 */
void *PDCdg_get_vertex_data(pdc_dg_t *dg, pdc_dg_vertex_id_t vertex_id);

/**
 * Retrieve data associated with edge
 *
 * \param dg          Pointer to the graph.
 * \param edge_id      Edge id with data.
 *
 * \return Data of the edge if it exists, or NULL.
 */
void *PDCdg_get_edge_data(pdc_dg_t *dg, pdc_dg_edge_id_t edge_id);

/**
 * Find the shortest path between two vertices.
 *
 * \param dg        Pointer to the graph.
 * \param v1_data   Data for the source vertex.
 * \param v2_data   Data for the destination vertex.
 * \param edges_out Output array of edge pointers forming the path.
 * \param num_edges Output number of edges in the path.
 *
 * \return true if a path is found, false otherwise.
 */
bool PDCdg_shortest_path(pdc_dg_t *dg, void *v1_data, void *v2_data, pdc_dg_edge_t **edges_out,
                         uint32_t *num_edges);

#endif /* PDC_DG_H */
