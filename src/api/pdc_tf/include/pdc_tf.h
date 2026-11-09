#ifndef PDC_TF_H
#define PDC_TF_H

/**
 * List of FIXME:'s
 *  1. Keep track of region history
 *  2. Parse func name correctly when adding function
 */
#include "pdc_public.h"
#include "pdc_dg.h"
#include "pdc_tf_common.h"

/**
 * Output mode for PDCtf_set_output_mode (reserved; not implemented yet).
 * - PDC_TF_OVERWRITE: the transformation modifies the existing object.
 * - PDC_TF_CREATE: the transformation creates a new object for the output.
 */
typedef enum pdc_tf_output_mode_t {
    PDC_TF_OVERWRITE,
    PDC_TF_CREATE,
} pdc_tf_output_mode_t;

/**
 * @brief Returns the parsed in-memory graph behind a handle (debugging/introspection).
 *
 * @param dg_id Graph handle from PDCtf_dg_json_create.
 * @return The graph, or NULL if dg_id is unknown.
 */
pdc_dg_t *PDCtf_get_dg(pdcid_t dg_id);

/**
 * @brief Reserved: choose whether a transformation overwrites the object
 * (PDC_TF_OVERWRITE, today's behavior) or writes to new objects
 * (PDC_TF_CREATE).
 *
 * Currently a no-op. To derive new objects from existing ones, use the
 * region-analysis API (pdc_an.h).
 *
 * @return Always SUCCEED.
 */
perr_t PDCtf_set_output_mode(pdcid_t dg_id, pdc_tf_output_mode_t mode, pdcid_t *obj_ids, char **result_names,
                             int num_ids);

/**
 * @brief Frees the client-local graph handle and the state/function nodes it
 * owns. Attachments already delivered to the server, and data already
 * written, are unaffected.
 *
 * @param dg_id Graph handle from PDCtf_dg_json_create.
 * @return SUCCEED on success, FAIL if dg_id is unknown.
 */
perr_t PDCtf_close_dg(pdcid_t dg_id);

/**
 * @brief Routes transfers of one region of an object through a graph.
 *
 * Client-local: the mapping is sent to the server piggybacked on the next
 * write of a matching region, so attach before writing. Only transfers whose
 * object region matches remote_reg exactly (ndim, offset, size, element type)
 * are transformed; read the data back with the same region. Readers in other
 * processes do not need to attach -- the server remembers the mapping.
 * State names are not validated here.
 *
 * @param dg_id        Graph handle from PDCtf_dg_json_create.
 * @param obj_id       Object to attach to; its element type must be set.
 * @param remote_reg   Region of the object (the global region passed to
 *                     PDCregion_transfer_create).
 * @param client_state Graph state of the data in client memory.
 * @param store_state  Graph state of the data on storage.
 * @return SUCCEED on success; FAIL if an ID is invalid, the object type is
 *         not set, or the region already has a graph attached.
 */
perr_t PDCtf_attach_to_region(pdcid_t dg_id, pdcid_t obj_id, pdcid_t remote_reg, char *client_state,
                              char *store_state);

/**
 * @brief Routes every region written to an object through a graph. Each
 * distinct region is registered with the server on its first write.
 *
 * @param dg_id        Graph handle from PDCtf_dg_json_create.
 * @param obj_id       Object to attach to; its element type must be set.
 * @param client_state Graph state of the data in client memory.
 * @param store_state  Graph state of the data on storage.
 * @return SUCCEED on success, FAIL otherwise.
 */
perr_t PDCtf_attach_to_obj(pdcid_t dg_id, pdcid_t obj_id, char *client_state, char *store_state);

/**
 * @brief Calls PDCtf_attach_to_obj for each object in obj_ids.
 *
 * @param dg_id        Graph handle from PDCtf_dg_json_create.
 * @param obj_ids      Array of num_ids object IDs.
 * @param num_ids      Number of objects.
 * @param client_state Graph state of the data in client memory.
 * @param store_state  Graph state of the data on storage.
 * @return SUCCEED if every attach succeeded; FAIL at the first failure.
 */
perr_t PDCtf_attach_to_objs(pdcid_t dg_id, pdcid_t *obj_ids, int num_ids, char *client_state,
                            char *store_state);

/**
 * @brief Registers the graph ID type and the built-in function registry.
 * Called by PDCinit(); applications do not need to call it.
 *
 * @return SUCCEED on success, FAIL otherwise.
 */
perr_t PDCtf_init();

/**
 * @brief Prints the graph in Graphviz DOT format (CPU edges blue, GPU edges red).
 *
 * @param dg_id         Graph handle from PDCtf_dg_json_create.
 * @param write_to_file true writes to ./graph.txt, false prints to stdout.
 */
void PDCtf_print_dg(pdcid_t dg_id, bool write_to_file);

/**
 * @brief Prints the functions that would run to move data from cur_state to
 * desired_state (the shortest path through the graph).
 *
 * @param dg_id         Graph handle from PDCtf_dg_json_create.
 * @param cur_state     Starting state name.
 * @param desired_state Target state name.
 */
void PDCtf_print_exec_path(pdcid_t dg_id, char *cur_state, char *desired_state);

/**
 * @brief Parses a transformation graph JSON file (see
 * docs/source/pdc_tf_docs.rst for the schema) and returns a client-local
 * handle. Every function is resolved immediately (built-ins against the
 * registry, external ones via dlopen/dlsym of "lib_path"). The server later
 * re-opens the same path, so it must be readable there too.
 *
 * @param json_filepath Path to the JSON file describing the graph.
 * @return Graph handle, or 0 on failure (unreadable file, invalid JSON, or a
 *         function not available in this build).
 */
pdcid_t PDCtf_dg_json_create(char *json_filepath);

/**
 * free a pdc_tf_obj_t and everything it owns: the per-region state strings and region
 * mapping entries in region_mappings_vector, the vector itself, and the all_regions_state
 * strings set by PDCtf_attach_to_obj(s). Safe to call with tf_obj == NULL.
 */
void PDCtf_obj_free(struct pdc_tf_obj_t *tf_obj);

#endif /* PDC_TF_H */
