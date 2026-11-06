#ifndef PDC_AN_COMMON_H
#define PDC_AN_COMMON_H

#include "mercury_proc_string.h"

#include "pdc_public.h"
#include "pdc_dg.h"
#include "pdc_vector.h"
#include "pdc_an_user.h"

/* Global registry of builtin (and dlopen-resolved external) analysis
 * functions, keyed by name + device. Mirrors pdc_tf_builtin_funcs_vector_g. */
extern PDC_VECTOR *pdc_an_builtin_funcs_vector_g;

/**
 * Client-local record of one PDCan_attach_to_region call: which graph
 * state a region was bound to, plus (optionally) a PDC TF graph attached
 * to the same object/region, so it can be registered alongside the
 * analysis binding. Mirrors pdc_tf_region_state_t/pdc_tf_region_mapping_t
 * exactly -- see pdc_tf_common.h.
 */
typedef struct pdc_an_region_state_t {
    pdcid_t dg_id;
    char *  state_name;
    char *  tf_json_filepath; /* NULL if no PDC TF graph attached here */
    char *  tf_client_state;
    char *  tf_store_state;
} pdc_an_region_state_t;

typedef struct pdc_an_region_mapping_t {
    pdc_an_region_state_t region_state;
    uint8_t               ndim;
    uint64_t              offset[DIM_MAX];
    uint64_t              size[DIM_MAX];

    /* The object this state is bound to -- captured at attach time since
     * a graph's states each live on a different object, and every entry
     * piggybacked together (see pdc_an_pkg_t) needs its own identity, not
     * just whichever object happens to own the RPC this rides on. */
    uint64_t       obj_id; /* server-assigned meta_id */
    int32_t        obj_ndim;
    uint64_t       obj_dims[DIM_MAX];
    pdc_var_type_t pdc_var_type;
} pdc_an_region_mapping_t;

/**
 * This is a field in _pdc_obj_info that records region-analysis
 * attachments client-side, exactly like pdc_tf_obj_t does for PDC TF.
 * PDCan_attach_to_region only ever records into this vector -- it makes no
 * RPC. The actual server-side registration happens lazily, piggybacked on
 * whichever read or write RPC first touches a matching region (see
 * pdc_an_pkg_t on transfer_request_in_t), exactly like PDC TF's
 * pdc_tf_pkg piggyback.
 */
typedef struct pdc_an_obj_t {
    PDC_VECTOR *region_mappings_vector; /* vector of pdc_an_region_mapping_t* */
} pdc_an_obj_t;

/**
 * Unlike PDC TF (single object, one region per RPC is always enough), a
 * region-analysis output is never client-written -- only ever read -- so
 * an output binding can only reach the server piggybacked on a READ. If a
 * write RPC only piggybacked its own single input binding, the server
 * would never learn where an output belongs until the first read of it,
 * and write-triggered eager computation (which needs to know that before
 * the triggering write completes) would silently degrade to read-triggered
 * lazy computation for every multi-object graph. So a transfer RPC that
 * touches any part of a graph piggybacks this rank's *entire* known set of
 * attachments for that graph -- every input and output state's binding --
 * not just the one region this particular RPC happens to touch. The
 * server-side registration is idempotent (PDCan_store_attach_mapping), so
 * resending the same bindings on every subsequent I/O is harmless. */
#define PDC_AN_PKG_MAX_STATES 16

typedef struct pdc_an_pkg_entry_t {
    uint32_t    pdc_var_type;
    hg_string_t state_name; /* NULL = unused slot */
    uint8_t     ndim;
    uint64_t    offset[DIM_MAX];
    uint64_t    size[DIM_MAX];
    uint64_t    obj_id; /* server-assigned meta_id of the object this state is bound to */
    int32_t     obj_ndim;
    uint64_t    obj_dims[DIM_MAX];
    hg_string_t tf_json_filepath; /* NULL/empty = no composed PDC TF graph for this state */
    hg_string_t tf_client_state;
    hg_string_t tf_store_state;
} pdc_an_pkg_entry_t;

/**
 * Strings needed by the server to register every region-analysis binding
 * (and, optionally, composed PDC TF bindings) for one graph, piggybacked
 * on a transfer_request_in_t. json_filepath is shared by all entries (one
 * graph per RPC); entries beyond num_entries are unused.
 */
typedef struct pdc_an_pkg_t {
    hg_string_t        json_filepath; /* NULL/empty = no analysis graph attached to this RPC's region */
    uint32_t           num_entries;
    pdc_an_pkg_entry_t entries[PDC_AN_PKG_MAX_STATES];
} pdc_an_pkg_t;

/**
 * Checks whether obj_an has a region mapping matching (ndim, offset, size)
 * exactly. Returns true and sets *region_mapping if so. Mirrors
 * PDCtf_region_has_attached_graph (pdc_tf_common.c).
 */
bool PDCan_region_has_attached_graph(pdc_an_obj_t *obj_an, uint8_t ndim, const uint64_t *offset,
                                     const uint64_t *size, pdc_an_region_mapping_t **region_mapping);

/**
 * Client-local, per-dg_id registry of every region_mapping ever recorded
 * by PDCan_attach_to_region under that graph, across every object it
 * touches (an object's own pdc_an_obj only ever holds mappings for
 * itself). Used by PDC_Client_transfer_request to gather the full set of
 * this rank's attachments for a graph before piggybacking them. Returns
 * NULL if dg_id has no recorded mappings.
 */
PDC_VECTOR *PDCan_get_client_dg_mappings(pdcid_t dg_id);

/**
 * Records mapping (borrowed, not copied) into the client-local per-dg_id
 * registry that PDCan_get_client_dg_mappings reads from. Called once per
 * PDCan_attach_to_region, in addition to that mapping being added to the
 * target object's own pdc_an_obj->region_mappings_vector.
 */
perr_t PDCan_add_client_dg_mapping(pdcid_t dg_id, pdc_an_region_mapping_t *mapping);

/**
 * @brief Parses a JSON file describing a region-analysis graph and builds a
 * bipartite state/function directed graph from it.
 *
 * Expected JSON shape:
 * {
 *   "name": "...",
 *   "lib_path": "..."  (optional, required if any transformation is "external")
 *   "states": [
 *     { "name": "...", "persistence": "transient"|"persistent",
 *       "trigger": "eager"|"lazy" (optional, default "eager"; only
 *       meaningful for a state produced by some transformation below --
 *       see pdc_an_trigger_t in pdc_an_user.h) }
 *   ],
 *   "transformations": [
 *     { "name": "...", "device": "CPU"|"GPU", "location": "builtin"|"external",
 *       "inputs": [ "state name", ... ], "outputs": [ "state name", ... ],
 *       "params": "..." (optional) }
 *   ]
 * }
 *
 * Every name in a transformation's "inputs"/"outputs" must already appear in
 * "states" (states are declared as vertices before transformations are
 * parsed). Each state may be produced by at most one transformation.
 * External functions are loaded via dlopen/dlsym using the top-level
 * "lib_path", exactly like the PDC TF JSON format.
 *
 * @param filepath Path to the JSON file.
 * @return Pointer to the newly created pdc_dg_t, or NULL on failure.
 */
pdc_dg_t *PDCan_dg_json_create_common(char *filepath);

/**
 * @brief Creates the (initially empty) global builtin analysis function
 * registry. No builtin region-analysis functions are compiled in yet;
 * this just gives external ("location": "external") functions a registry
 * to land in, and gives future builtins somewhere to register.
 */
perr_t PDCan_init_builtin_funcs(void);

/**
 * @brief Registers a single function pointer in the global builtin
 * analysis function registry, keyed by name + device.
 */
perr_t PDCan_add_builtin_func(char *func_name, a_func_t a_func, pdc_tf_dev_t dev);

/**
 * @brief Resolves a named builtin analysis function and binds it to a
 * pdc_an_func_t's `a_func` field.
 */
perr_t PDCan_link_builtin_func(char *func_name, pdc_tf_dev_t dev, pdc_tf_location_t location,
                               const char *params_str, pdc_an_func_t *f);

/**
 * @brief Looks up a node (state or transformation) in the graph by name.
 * @return Pointer to the node, or NULL if not found.
 */
pdc_an_node_t *PDCan_dg_get_node(pdc_dg_t *dg, const char *name);

/**
 * @brief Looks up a state node by name.
 * @return Pointer to the state, or NULL if not found or not a state vertex.
 */
pdc_an_state_t *PDCan_dg_get_state(pdc_dg_t *dg, const char *name);

#endif /* PDC_AN_COMMON_H */
