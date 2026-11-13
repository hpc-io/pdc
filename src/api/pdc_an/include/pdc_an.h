#ifndef PDC_AN_H
#define PDC_AN_H

#include "pdc_public.h"
#include "pdc_dg.h"

/**
 * @brief Loads a region-analysis graph JSON file (see
 * docs/source/pdc_an_docs.rst for the schema) and registers it as a
 * client-local handle. Mirrors PDCtf_dg_json_create -- the server never
 * sees this handle, only the JSON filepath, which it re-parses and caches
 * itself the first time any state in this graph is attached, so the path
 * must be readable on the server too.
 *
 * @param json_filepath Path to the JSON file describing the graph.
 * @return Client-local graph handle, or 0 on failure (unreadable file,
 *         invalid JSON/schema, or a transformation not available in this
 *         build).
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
 * from the graph itself) to this rank's region of an object.
 *
 * The call is client-local: it validates state_name against the graph and
 * records the binding. The binding is delivered to the server piggybacked on
 * the next region transfer (read or write) of a bound object -- there is no
 * separate RPC. All states taking part in one execution must be bound with
 * the same region (same ndim/offset/size), since the server matches inputs
 * and outputs by region.
 *
 * Once an output state is attached, there is no separate call to make it
 * run: an "eager" output is computed when its last input is written, and
 * reading any unmaterialized output with the ordinary
 * PDCregion_transfer_create/start/wait/close sequence computes it first.
 *
 * If the same object region already has a PDC TF graph attached
 * (PDCtf_attach_to_region/_to_obj), call that first: it is recorded here
 * and applied when the server writes/reads this state.
 *
 * See docs/source/pdc_an_docs.rst for the full semantics.
 *
 * @param dg_id      Graph handle from PDCan_dg_json_create.
 * @param state_name Name of the state to attach, as declared in the graph's JSON.
 * @param obj_id     Object whose region backs this state.
 * @param region     Region of the object (the global region passed to
 *                   PDCregion_transfer_create) backing this state.
 * @return SUCCEED on success; FAIL if dg_id, obj_id or region is invalid or
 *         state_name is not a state of this graph.
 */
perr_t PDCan_attach_to_region(pdcid_t dg_id, char *state_name, pdcid_t obj_id, pdcid_t region);

/**
 * @brief Registers the PDC_AN_DG client-local ID type. Called once during
 * PDCinit(), mirroring PDCtf_init().
 */
perr_t PDCan_init(void);

#endif /* PDC_AN_H */
