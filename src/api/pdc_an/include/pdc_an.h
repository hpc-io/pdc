#ifndef PDC_AN_H
#define PDC_AN_H

#include "pdc_public.h"
#include "pdc_dg.h"

/**
 * @brief Loads a region-analysis graph JSON file (see
 * docs/design/region_analysis.md for the schema) and registers it as a
 * client-local handle. Mirrors PDCtf_dg_json_create -- the server never
 * sees this handle, only the JSON filepath, which it re-parses and caches
 * itself the first time any state in this graph is attached.
 *
 * @param json_filepath Path to the JSON file describing the graph.
 * @return Client-local graph handle, or 0 on failure.
 */
pdcid_t PDCan_dg_json_create(char *json_filepath);

/**
 * @brief Retrieves the underlying directed graph for a client-local handle
 * returned by PDCan_dg_json_create. Mostly useful for debugging/introspection.
 */
pdc_dg_t *PDCan_get_dg(pdcid_t dg_id);

/**
 * @brief Frees the client-local resources associated with dg_id. Does not
 * affect any server-side state already attached under this graph's JSON
 * filepath.
 */
perr_t PDCan_close_dg(pdcid_t dg_id);

/**
 * @brief Attaches a graph state (input or output -- direction is inferred
 * server-side from the graph itself) to this rank's local region of an
 * object. Unlike PDCtf_attach_to_region, this issues the attachment to the
 * server immediately (there's no lazy piggyback-on-write mechanism for
 * region analysis): rejected if state_name isn't a state in this graph at
 * all.
 *
 * Once an output state is attached, there is no separate call to make it
 * run -- exactly like a PDC TF transform, reading the bound object with the
 * ordinary PDCregion_transfer_create/start/wait/close sequence transparently
 * computes it first if it isn't already materialized on disk.
 *
 * @param dg_id      Graph handle from PDCan_dg_json_create.
 * @param state_name Name of the state to attach, as declared in the graph's JSON.
 * @param obj_id     Object whose region backs this state.
 * @param region     Region (this rank's local shape/offset) backing this state.
 * @return SUCCEED on success, FAIL otherwise (see docs/design/region_analysis.md
 * section 8 for error semantics).
 */
perr_t PDCan_attach_to_region(pdcid_t dg_id, char *state_name, pdcid_t obj_id, pdcid_t region);

/**
 * @brief Registers the PDC_AN_DG client-local ID type. Called once during
 * PDCinit(), mirroring PDCtf_init().
 */
perr_t PDCan_init(void);

#endif /* PDC_AN_H */
