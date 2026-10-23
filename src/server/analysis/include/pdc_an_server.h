#ifndef PDC_AN_SERVER_H
#define PDC_AN_SERVER_H

#include <stdio.h>

#include "pdc_an_common.h"
#include "pdc_region.h"

/**
 * One state's binding to a concrete (obj_id, region) on this rank's data
 * server, populated by PDCan_store_attach_mapping — the server-side handler
 * for the client's PDCan_attach_to_region call. There is at most one
 * binding per state name per graph entry (a state is attached once, by
 * whichever rank owns that region).
 */
typedef struct pdc_an_binding_t {
    char *         state_name;
    pdcid_t        obj_id;
    int            obj_ndim;
    uint64_t       obj_dims[DIM_MAX];
    uint8_t        ndim;
    uint64_t       offset[DIM_MAX];
    uint64_t       size[DIM_MAX];
    pdc_var_type_t pdc_var_type;
    bool           materialized; /* only meaningful for persistent output bindings */
} pdc_an_binding_t;

/**
 * One entry in the json_filepath <-> graph_id mapping (see
 * an_graph_path_registry_g below). graph_id is assigned once, the first
 * time this data server ever loads a given filepath (in
 * PDCan_get_or_assign_graph_id), and never reused or renumbered for the
 * lifetime of the server process -- checkpoint/restart round-trips it
 * as-is (see PDCan_checkpoint/PDCan_restart_init), it does not
 * reassign IDs on restart.
 */
typedef struct pdc_an_graph_path_entry_t {
    uint32_t graph_id;
    char *   json_filepath;
} pdc_an_graph_path_entry_t;

/**
 * Every json_filepath this data server has ever loaded a graph from, each
 * assigned a small stable graph_id. Metadata that needs to reference
 * "which graph" -- currently just PDCan_checkpoint's per-entry record --
 * stores the graph_id instead of repeating the (potentially long)
 * filepath string; PDCan_get_graph_path resolves it back when the actual
 * path is needed (e.g. to re-parse the JSON on restart). This registry
 * itself still checkpoints the filepath strings, but exactly once each,
 * rather than once per thing that references a graph.
 */
extern PDC_VECTOR *an_graph_path_registry_g;

/**
 * Returns the existing graph_id for json_filepath if this data server has
 * already loaded it, otherwise assigns and registers a new one. Never
 * fails (aside from allocation failure, which aborts like the rest of
 * this codebase's PDC_malloc/PDC_calloc).
 */
uint32_t PDCan_get_or_assign_graph_id(const char *json_filepath);

/**
 * Resolves a graph_id back to the json_filepath it was assigned for.
 * Returns NULL if graph_id is unknown (e.g. a corrupt checkpoint).
 */
const char *PDCan_get_graph_path(uint32_t graph_id);

/**
 * One loaded analysis graph plus every binding attached to it so far on
 * this data server. Keyed by json_filepath (not by the client's dg_id —
 * the server resolves graphs independently by file path, exactly like PDC
 * TF's PDCtf_store_json_mapping does), since a graph is not owned by a
 * single object the way a PDC TF graph attachment is. graph_id is this
 * same filepath's entry in an_graph_path_registry_g, cached here so
 * PDCan_checkpoint doesn't need to look it up separately per entry.
 */
typedef struct pdc_an_dg_entry_t {
    char *      json_filepath;
    uint32_t    graph_id;
    pdc_dg_t *  dg;
    PDC_VECTOR *bindings_vector; /* vector of pdc_an_binding_t* */
} pdc_an_dg_entry_t;

/* Registry of loaded analysis graphs on this data server, keyed by
 * json_filepath. */
extern PDC_VECTOR *an_dg_registry_g;

/* Reverse index over OUTPUT bindings only: lets the read path recognize
 * "this (obj_id, region) is an analysis output" from the region alone,
 * without knowing which graph produced it up front. This is what
 * "attaching the graph to the output region" registers — see
 * docs/design/region_analysis.md section 6. */
extern PDC_VECTOR *an_obj_id_to_binding_vector_g;

/**
 * Server-side handler for the client's PDCan_attach_to_region call.
 * Loads (or reuses, if already loaded by an earlier attach on this
 * server) the graph at json_filepath, validates state_name exists and is
 * not "transient" (transient states have no addressable object), records
 * the binding, and — if state_name is an output state (produced by some
 * transformation in the graph) — registers it in the reverse index too.
 */
perr_t PDCan_store_attach_mapping(char *json_filepath, char *state_name, pdcid_t obj_id, uint64_t *offset,
                                  uint64_t *size, uint8_t ndim, int obj_ndim, uint64_t *obj_dims,
                                  pdc_var_type_t pdc_var_type);

/** Looks up a loaded graph entry by json_filepath. Returns NULL if not loaded. */
pdc_an_dg_entry_t *PDCan_get_dg_entry(const char *json_filepath);

/** Looks up the binding for a given state name within a graph entry, or NULL. */
pdc_an_binding_t *PDCan_find_binding(pdc_an_dg_entry_t *entry, const char *state_name);

/**
 * Checks whether (obj_id, offset, size) matches a bound analysis OUTPUT
 * region on this server. Returns true and sets *entry_out and *binding_out
 * if so.
 */
bool PDCan_region_is_analysis_output(pdcid_t obj_id, uint8_t ndim, uint64_t *offset, uint64_t *size,
                                     pdc_an_dg_entry_t **entry_out, pdc_an_binding_t **binding_out);

/**
 * Checks whether (obj_id, offset, size) matches a bound analysis INPUT
 * region on this server (any graph). Used by the write path to decide
 * whether a write is worth force-flushing immediately for eager
 * computation, without needing the full entry/binding.
 */
bool PDCan_region_is_analysis_input(pdcid_t obj_id, uint8_t ndim, uint64_t *offset, uint64_t *size);

/**
 * Executes the minimal subgraph needed to produce target_state_names:
 * a backward reachability slice from the targets through the bipartite
 * graph, topologically sorted, then run in order. States already
 * materialized (persistent, on disk from a previous execution) are read
 * back rather than recomputed; states already produced earlier in this
 * same execution are reused from an in-memory scratch map. Leaf inputs
 * are read via PDC_Server_transfer_request_io (so any PDC TF transform
 * already attached to the input object still applies transparently), and
 * persistent outputs are written the same way, so the result is
 * materialized and bound outputs (state->materialized) are updated.
 *
 * Every buffer this call allocates (leaf-input reads, transformation
 * outputs) is freed before returning; nothing survives past one call —
 * transient states are never cached across executions, by design.
 *
 * ref_ndim/ref_offset/ref_size identify the region of whichever binding
 * triggered this execution (the write or read the caller is handling).
 * A graph attached by multiple ranks -- each against its own region of a
 * shared object, or its own separate object -- accumulates multiple
 * bindings per state name in the same entry; every binding lookup made
 * during this call is scoped to the binding matching this reference
 * region, so one rank's execution can't read or write another rank's
 * bound region for the same state name.
 */
perr_t PDCan_exec_graph(pdc_an_dg_entry_t *entry, char **target_state_names, int num_targets,
                        uint8_t ref_ndim, const uint64_t *ref_offset, const uint64_t *ref_size);

/**
 * Write-path hook: called after any successful write, for every
 * (obj_id, region), from PDC_Server_transfer_request_io. A no-op unless
 * the region matches a bound analysis INPUT, in which case it's marked
 * materialized and every transformation consuming it is checked -- any
 * whose inputs are now all materialized AND whose outputs are all
 * declared "eager" (pdc_an_trigger_t; the default) is eagerly computed
 * and persisted (a fixed-point pass, so a chain of persistent
 * intermediate states cascades in one call). A transformation with a
 * transient input can never become "ready" this way -- transient states
 * have no binding to check -- and a transformation with any "lazy"
 * output is deliberately skipped here regardless of readiness; both
 * cases are only ever reached via the lazy read-triggered path in
 * PDC_Server_data_io_region_analysis.
 */
perr_t PDCan_notify_input_written(pdcid_t obj_id, uint8_t ndim, uint64_t *offset, uint64_t *size);

/**
 * Read-path hook, checked alongside PDC_Server_data_io_region_per_file_transformations
 * inside PDC_Server_transfer_request_io. Only intercepts reads: writes to
 * an analysis output are never client-initiated (they only ever happen
 * from inside PDCan_exec_graph itself), so on a write this always leaves
 * *ran_analysis false and does nothing.
 *
 * On a read of a bound, unmaterialized output: transparently computes it
 * (and everything it depends on) before serving the read, exactly like PDC
 * TF's transparent inverse transform on read — there is no separate "run
 * the analysis" step the client must call first. See
 * docs/design/region_analysis.md section 8.
 *
 * The "already materialized, just serve the read" path can't call
 * PDC_Server_transfer_request_io directly (it would re-enter this same
 * hook), so it calls PDC_Server_data_io_region_per_file_transformations
 * directly instead -- the same TF check that function itself would have
 * run first -- falling back to a plain PDC_Server_data_read_from only if
 * nothing is attached. This means an analysis output *can* also have a
 * PDC TF transform layered on top of it (e.g. compression): PDCan_exec_graph's
 * own write already goes through PDC_Server_transfer_request_io and so
 * composes with it too, symmetrically.
 */
perr_t PDC_Server_data_io_region_analysis(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                          struct pdc_region_info *region_info, void *buf, size_t unit,
                                          int is_write, bool *ran_analysis);

/**
 * Serializes an_graph_path_registry_g (the full json_filepath <-> graph_id
 * mapping, written exactly once each) followed by an_dg_registry_g: for
 * every loaded graph, its graph_id (not the filepath itself -- resolved
 * via the mapping just written) and every binding (state name,
 * obj_id/ndim/offset/size, owning object's ndim/dims, var type,
 * materialized flag). Called from PDC_Server_checkpoint() in
 * pdc_server.c, mirroring where PDC TF's transform-state checkpoint
 * block lives.
 *
 * Like PDC TF's checkpoint, this stores graph *topology* only as a
 * filepath reference, not a serialized graph -- PDCan_restart_init
 * reconstructs it by re-parsing that JSON file, so it must still exist at
 * the same path at restart time.
 */
perr_t PDCan_checkpoint(FILE *file);

/**
 * Reads what PDCan_checkpoint wrote and rebuilds an_graph_path_registry_g,
 * an_dg_registry_g, and an_obj_id_to_binding_vector_g (including each
 * binding's materialized flag, so already-computed persistent outputs are
 * not recomputed after a restart). Call PDCan_init_builtin_funcs() before
 * this, exactly like
 * PDC_Server_restart() calls PDCtf_init_builtin_funcs() before restoring
 * transform state -- graphs loaded here may reference builtin analysis
 * functions that need to already be registered.
 */
perr_t PDCan_restart_init(FILE *file);

#endif /* PDC_AN_SERVER_H */
