#include <string.h>
#include <stdlib.h>
#include <math.h>
#ifdef CUDA_ENABLED
#include <cuda_runtime.h>
#endif

#include "pdc_an_server.h"
#include "pdc_malloc.h"
#include "pdc_client_server_common.h"
#include "pdc_server_data.h"
#include "pdc_server_region_cache.h"
#include "pdc_server_region_transfer.h"
#include "pdc_tf_common.h"
#include "pdc_tf_profiler.h"
#include "pdc_timing.h"
#include "pdc_logger.h"

PDC_VECTOR *an_dg_registry_g              = NULL;
PDC_VECTOR *an_obj_id_to_binding_vector_g = NULL;
PDC_VECTOR *an_graph_path_registry_g      = NULL;

/* Next graph_id to hand out -- monotonically increasing for the lifetime
 * of this server process (or since the last restart, which restores it
 * from the checkpointed registry's own max + 1; see PDCan_restart_init).
 * 0 is a valid id (the first graph ever loaded gets it), so there is no
 * sentinel "unassigned" value here -- callers check registry membership,
 * not the id's value, to know whether a filepath has been seen before. */
static uint32_t an_next_graph_id_g = 0;

uint32_t
PDCan_get_or_assign_graph_id(const char *json_filepath)
{
    if (an_graph_path_registry_g == NULL)
        an_graph_path_registry_g = pdc_vector_create(4, 2.0);

    PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(an_graph_path_registry_g);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_graph_path_entry_t *e = (pdc_an_graph_path_entry_t *)pdc_vector_iterator_next(iter);
        if (e != NULL && !strcmp(e->json_filepath, json_filepath)) {
            pdc_vector_iterator_destroy(iter);
            return e->graph_id;
        }
    }
    pdc_vector_iterator_destroy(iter);

    pdc_an_graph_path_entry_t *entry = PDC_malloc(sizeof(pdc_an_graph_path_entry_t));
    entry->graph_id                  = an_next_graph_id_g++;
    entry->json_filepath             = strdup(json_filepath);
    pdc_vector_add(an_graph_path_registry_g, entry);

    return entry->graph_id;
}

const char *
PDCan_get_graph_path(uint32_t graph_id)
{
    if (an_graph_path_registry_g == NULL)
        return NULL;

    PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(an_graph_path_registry_g);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_graph_path_entry_t *e = (pdc_an_graph_path_entry_t *)pdc_vector_iterator_next(iter);
        if (e != NULL && e->graph_id == graph_id) {
            pdc_vector_iterator_destroy(iter);
            return e->json_filepath;
        }
    }
    pdc_vector_iterator_destroy(iter);
    return NULL;
}

/* Set for the duration of a write-triggered eager PDCan_exec_graph call
 * (see PDCan_notify_input_written). The server is single-threaded, so this
 * is safe as a plain global rather than needing any lock.
 *
 * Two things depend on it:
 *  - PDCan_notify_input_written itself: a write inside this same call
 *    stack (an output being persisted, or a reentrant cache flush) must
 *    not recursively trigger another eager computation.
 *  - PDCan_exec_graph's leaf reads: normally each one calls
 *    PDC_region_cache_flush() first so a just-written-but-not-yet-flushed
 *    input isn't read stale. But when this exec was itself triggered by a
 *    write's post-write hook, that hook only fires *after*
 *    PDC_Server_transfer_request_io's own write already completed -- so
 *    the data is already correctly on disk, and flushing again would
 *    re-enter PDC_region_cache_flush_by_pointer for an object whose cache
 *    entry is still mid-unwind higher up this exact call stack, which
 *    corrupts its linked list (a double free), not just recurses. Skipping
 *    the redundant flush avoids that reentrancy entirely. */
static bool an_eager_exec_in_progress_g = false;

/* Pairs a binding (input or output) with the graph entry it belongs to, so
 * the reverse index can hand both back to the read/write-path hooks in one
 * lookup. */
typedef struct pdc_an_binding_index_entry_t {
    pdc_an_dg_entry_t *dg_entry;
    pdc_an_binding_t * binding;
} pdc_an_binding_index_entry_t;

/* pdc_vector_destroy() frees every stored item, which is wrong for a
 * vector whose items are borrowed pointers (not owned by the vector) --
 * frees just the vector's own backing array and struct. */
static void
an_vector_destroy_shallow(PDC_VECTOR *vector)
{
    if (vector == NULL)
        return;
    PDC_free(vector->items);
    PDC_free(vector);
}

static int
vector_find_name(PDC_VECTOR *names_vec, const char *name)
{
    if (names_vec == NULL)
        return -1;

    size_t n = pdc_vector_size(names_vec);
    for (size_t i = 0; i < n; i++) {
        char *cur = (char *)pdc_vector_get(names_vec, i);
        if (cur != NULL && !strcmp(cur, name))
            return (int)i;
    }
    return -1;
}

static bool
vector_contains_ptr(PDC_VECTOR *v, void *p)
{
    if (v == NULL)
        return false;

    size_t n = pdc_vector_size(v);
    for (size_t i = 0; i < n; i++) {
        if (pdc_vector_get(v, i) == p)
            return true;
    }
    return false;
}

pdc_an_dg_entry_t *
PDCan_get_dg_entry(const char *json_filepath)
{
    if (an_dg_registry_g == NULL || json_filepath == NULL)
        return NULL;

    PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(an_dg_registry_g);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_dg_entry_t *e = (pdc_an_dg_entry_t *)pdc_vector_iterator_next(iter);
        if (e != NULL && !strcmp(e->json_filepath, json_filepath)) {
            pdc_vector_iterator_destroy(iter);
            return e;
        }
    }
    pdc_vector_iterator_destroy(iter);
    return NULL;
}

pdc_an_binding_t *
PDCan_find_binding(pdc_an_dg_entry_t *entry, const char *state_name)
{
    if (entry == NULL || entry->bindings_vector == NULL || state_name == NULL)
        return NULL;

    PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(entry->bindings_vector);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_binding_t *b = (pdc_an_binding_t *)pdc_vector_iterator_next(iter);
        if (b != NULL && !strcmp(b->state_name, state_name)) {
            pdc_vector_iterator_destroy(iter);
            return b;
        }
    }
    pdc_vector_iterator_destroy(iter);
    return NULL;
}

/* Region-scoped binding lookup: a graph attached by multiple ranks (each
 * against its own disjoint region of a shared object, or its own separate
 * object) accumulates multiple bindings per state_name in the same entry
 * -- one per rank. PDCan_find_binding alone can't tell them apart (it just
 * returns whichever was attached first), so every lookup made *during one
 * rank's execution* needs to be scoped to that rank's own region: match on
 * state_name AND the same (ndim, offset, size) as ref_offset/ref_size,
 * which is the region of whichever binding actually triggered this
 * execution (the write or read that called into PDCan_exec_graph). This
 * mirrors the convention already used throughout the codebase (and its
 * examples/tests) that one rank attaches every state of a graph using the
 * same region across all of them. */
static pdc_an_binding_t *
PDCan_find_binding_by_region(pdc_an_dg_entry_t *entry, const char *state_name, uint8_t ref_ndim,
                             const uint64_t *ref_offset, const uint64_t *ref_size)
{
    if (entry == NULL || entry->bindings_vector == NULL || state_name == NULL)
        return NULL;

    pdc_an_binding_t *   fallback = NULL;
    PDC_VECTOR_ITERATOR *iter     = pdc_vector_iterator_new(entry->bindings_vector);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_binding_t *b = (pdc_an_binding_t *)pdc_vector_iterator_next(iter);
        if (b == NULL || strcmp(b->state_name, state_name))
            continue;

        if (fallback == NULL)
            fallback = b; /* preserves old first-match behavior if nothing matches by region */

        if (b->ndim != ref_ndim)
            continue;
        bool region_match = true;
        for (int d = 0; d < ref_ndim; d++) {
            if (b->offset[d] != ref_offset[d] || b->size[d] != ref_size[d]) {
                region_match = false;
                break;
            }
        }
        if (region_match) {
            pdc_vector_iterator_destroy(iter);
            return b;
        }
    }
    pdc_vector_iterator_destroy(iter);
    return fallback;
}

/* Shared reverse-index scan: finds the (dg_entry, binding) whose region
 * matches (obj_id, ndim, offset, size), optionally filtered to only
 * input-state or only output-state bindings (want_output: 1 = output only,
 * 0 = input only, -1 = either). */
static bool
find_binding_index_entry(pdcid_t obj_id, uint8_t ndim, const uint64_t *offset, const uint64_t *size,
                         int want_output, pdc_an_dg_entry_t **entry_out, pdc_an_binding_t **binding_out)
{
    bool ret_value = false;

    if (an_obj_id_to_binding_vector_g == NULL)
        return false;

    PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(an_obj_id_to_binding_vector_g);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_binding_index_entry_t *idx = (pdc_an_binding_index_entry_t *)pdc_vector_iterator_next(iter);
        pdc_an_binding_t *            b   = idx->binding;

        if (b->obj_id != obj_id || b->ndim != ndim)
            continue;

        if (want_output != -1) {
            pdc_an_state_t *state = PDCan_dg_get_state(idx->dg_entry->dg, b->state_name);
            if (state == NULL || (bool)want_output != state->is_output)
                continue;
        }

        bool match = true;
        for (int i = 0; i < ndim; i++) {
            if (b->offset[i] != offset[i] || b->size[i] != size[i]) {
                match = false;
                break;
            }
        }
        if (match) {
            if (entry_out != NULL)
                *entry_out = idx->dg_entry;
            if (binding_out != NULL)
                *binding_out = b;
            ret_value = true;
            break;
        }
    }
    pdc_vector_iterator_destroy(iter);

    return ret_value;
}

bool
PDCan_region_is_analysis_output(pdcid_t obj_id, uint8_t ndim, uint64_t *offset, uint64_t *size,
                                pdc_an_dg_entry_t **entry_out, pdc_an_binding_t **binding_out)
{
    return find_binding_index_entry(obj_id, ndim, offset, size, 1, entry_out, binding_out);
}

bool
PDCan_region_is_analysis_input(pdcid_t obj_id, uint8_t ndim, uint64_t *offset, uint64_t *size)
{
    return find_binding_index_entry(obj_id, ndim, offset, size, 0, NULL, NULL);
}

static pdc_an_dg_entry_t *
find_or_create_dg_entry(char *json_filepath)
{
    if (an_dg_registry_g == NULL)
        an_dg_registry_g = pdc_vector_create(4, 2.0);

    pdc_an_dg_entry_t *entry = PDCan_get_dg_entry(json_filepath);
    if (entry != NULL)
        return entry;

    pdc_dg_t *dg = PDCan_dg_json_create_common(json_filepath);
    if (dg == NULL)
        return NULL;

    entry                  = PDC_calloc(1, sizeof(pdc_an_dg_entry_t));
    entry->json_filepath   = strdup(json_filepath);
    entry->graph_id        = PDCan_get_or_assign_graph_id(json_filepath);
    entry->dg              = dg;
    entry->bindings_vector = pdc_vector_create(8, 2.0);
    pdc_vector_add(an_dg_registry_g, entry);

    return entry;
}

/* Exact-region lookup with no state_name-only fallback (unlike
 * PDCan_find_binding_by_region, which deliberately falls back to a
 * state_name-only match for the multi-rank-same-region case -- see its own
 * comment). Callers here need to know specifically whether *this* region
 * already has a binding, not merely whether the state name has ever been
 * bound anywhere. */
static pdc_an_binding_t *
find_exact_region_binding(pdc_an_dg_entry_t *entry, const char *state_name, uint8_t ndim,
                          const uint64_t *offset, const uint64_t *size)
{
    if (entry == NULL || entry->bindings_vector == NULL || state_name == NULL)
        return NULL;

    PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(entry->bindings_vector);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_binding_t *b = (pdc_an_binding_t *)pdc_vector_iterator_next(iter);
        if (b == NULL || strcmp(b->state_name, state_name) || b->ndim != ndim)
            continue;
        bool match = true;
        for (int d = 0; d < ndim; d++) {
            if (b->offset[d] != offset[d] || b->size[d] != size[d]) {
                match = false;
                break;
            }
        }
        if (match) {
            pdc_vector_iterator_destroy(iter);
            return b;
        }
    }
    pdc_vector_iterator_destroy(iter);
    return NULL;
}

perr_t
PDCan_store_attach_mapping(char *json_filepath, char *state_name, pdcid_t obj_id, uint64_t *offset,
                           uint64_t *size, uint8_t ndim, int obj_ndim, uint64_t *obj_dims,
                           pdc_var_type_t pdc_var_type)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    pdc_an_dg_entry_t *entry = find_or_create_dg_entry(json_filepath);
    if (entry == NULL)
        PGOTO_ERROR(FAIL, "Failed to load or find analysis graph \"%s\"\n", json_filepath);

    /* Now piggybacked on every read/write RPC touching a bound region
     * (exactly like PDC TF's pdc_tf_pkg), not sent once by a dedicated
     * attach RPC -- so this same (obj_id, region) arrives on every single
     * read/write, and must be a no-op past the first time or every
     * subsequent I/O would pile up a duplicate binding. */
    pdc_an_dg_entry_t *existing_entry   = NULL;
    pdc_an_binding_t * existing_binding = NULL;
    if (find_binding_index_entry(obj_id, ndim, offset, size, -1, &existing_entry, &existing_binding) &&
        existing_entry == entry && !strcmp(existing_binding->state_name, state_name))
        PGOTO_DONE(SUCCEED);

    pdc_an_state_t *state = PDCan_dg_get_state(entry->dg, state_name);
    if (state == NULL)
        PGOTO_ERROR(FAIL, "State \"%s\" not found in analysis graph \"%s\"\n", state_name, json_filepath);
    if (state->persistence == PDC_AN_TRANSIENT)
        PGOTO_ERROR(FAIL,
                    "Cannot attach a region to transient state \"%s\"; transient states have no "
                    "addressable object\n",
                    state_name);

    /* A *different* object attaching to the exact same (state_name,
     * region) slot -- e.g. a multi-timestep workload re-attaching
     * "vx_1"/"magnitude_1" to the same per-rank region that "vx_0"/
     * "magnitude_0" used -- is a new temporal instance, not a duplicate of
     * the old one. Reuse the existing binding struct in place (rather than
     * appending a second binding for the same slot) so every lookup keyed
     * off region alone (state_is_materialized, PDCan_find_binding_by_region)
     * sees only the current object, and reset materialized so this
     * instance's outputs actually get (re)computed instead of being
     * treated as already-done from the previous instance. The reverse
     * index (an_obj_id_to_binding_vector_g) holds a pointer to this same
     * struct, so mutating obj_id here is also immediately visible to
     * find_binding_index_entry(new_obj_id, ...) without any separate
     * update, and lookups for the superseded obj_id correctly stop
     * matching. */
    pdc_an_binding_t *reused = find_exact_region_binding(entry, state_name, ndim, offset, size);
    if (reused != NULL && reused->obj_id != (uint64_t)obj_id) {
        reused->obj_id   = obj_id;
        reused->obj_ndim = obj_ndim;
        memcpy(reused->obj_dims, obj_dims, (size_t)obj_ndim * sizeof(uint64_t));
        reused->pdc_var_type = pdc_var_type;
        reused->materialized = false;
        PGOTO_DONE(SUCCEED);
    }

    pdc_an_binding_t *binding = PDC_calloc(1, sizeof(pdc_an_binding_t));
    binding->state_name       = strdup(state_name);
    binding->obj_id           = obj_id;
    binding->obj_ndim         = obj_ndim;
    memcpy(binding->obj_dims, obj_dims, (size_t)obj_ndim * sizeof(uint64_t));
    binding->ndim = ndim;
    memcpy(binding->offset, offset, (size_t)ndim * sizeof(uint64_t));
    memcpy(binding->size, size, (size_t)ndim * sizeof(uint64_t));
    binding->pdc_var_type = pdc_var_type;
    binding->materialized = false;

    pdc_vector_add(entry->bindings_vector, binding);

    /* Every binding (input or output) goes into the reverse index: outputs
     * need it so the read hook recognizes them; inputs need it so the
     * write hook (PDCan_notify_input_written) recognizes them too. */
    if (an_obj_id_to_binding_vector_g == NULL)
        an_obj_id_to_binding_vector_g = pdc_vector_create(8, 2.0);

    pdc_an_binding_index_entry_t *idx = PDC_malloc(sizeof(pdc_an_binding_index_entry_t));
    idx->dg_entry                     = entry;
    idx->binding                      = binding;
    pdc_vector_add(an_obj_id_to_binding_vector_g, idx);

done:
    FUNC_LEAVE(ret_value);
}

static bool
state_is_materialized(pdc_an_dg_entry_t *entry, const char *state_name, uint8_t ref_ndim,
                      const uint64_t *ref_offset, const uint64_t *ref_size)
{
    pdc_an_binding_t *b = PDCan_find_binding_by_region(entry, state_name, ref_ndim, ref_offset, ref_size);
    return b != NULL && b->materialized;
}

/**
 * Backward reachability from target_state_names through the bipartite
 * graph: collects every transformation vertex that must run, and the
 * order returned is not yet topological (see topo_sort_funcs below).
 *
 * A state whose binding is already materialized (persistent, written to
 * storage by an earlier execution) stops the backward walk at that state,
 * exactly like a leaf input -- its producer is not added to needed_funcs,
 * so PDCan_exec_graph reads the existing bytes back via PDCan_find_binding
 * instead of recomputing them. See docs/design/region_analysis.md: "states
 * already materialized ... are reused rather than recomputed."
 */
static perr_t
collect_needed_funcs(pdc_dg_t *dg, pdc_an_dg_entry_t *entry, char **target_state_names, int num_targets,
                     PDC_VECTOR *needed_funcs, uint8_t ref_ndim, const uint64_t *ref_offset,
                     const uint64_t *ref_size)
{
    FUNC_ENTER(NULL);

    perr_t      ret_value = SUCCEED;
    PDC_VECTOR *frontier  = pdc_vector_create(8, 2.0);

    for (int i = 0; i < num_targets; i++) {
        pdc_an_node_t *state_node = PDCan_dg_get_node(dg, target_state_names[i]);
        if (state_node == NULL || state_node->kind != PDC_AN_NODE_STATE)
            PGOTO_ERROR(FAIL, "Unknown target state \"%s\"\n", target_state_names[i]);
        if (state_is_materialized(entry, target_state_names[i], ref_ndim, ref_offset, ref_size))
            continue; /* already on disk, nothing to compute for this target */
        if (!vector_contains_ptr(frontier, state_node))
            pdc_vector_add(frontier, state_node);
    }

    for (size_t idx = 0; idx < pdc_vector_size(frontier); idx++) {
        pdc_an_node_t *state_node = (pdc_an_node_t *)pdc_vector_get(frontier, idx);

        /* find the (at most one, enforced at parse time) function vertex
         * with an edge into this state */
        for (uint32_t e = 0; e < dg->edge_count; e++) {
            if (dg->edges[e]->v2_id < 0 || (uint32_t)dg->edges[e]->v2_id >= dg->vertex_count)
                continue;
            pdc_an_node_t *v2 = (pdc_an_node_t *)dg->vertices[dg->edges[e]->v2_id]->data;
            if (v2 != state_node)
                continue;

            pdc_an_node_t *func_node = (pdc_an_node_t *)dg->vertices[dg->edges[e]->v1_id]->data;
            if (func_node->kind != PDC_AN_NODE_FUNC)
                continue;
            if (vector_contains_ptr(needed_funcs, func_node))
                continue;

            pdc_vector_add(needed_funcs, func_node);

            pdc_an_func_t *f = &func_node->u.func;
            for (int k = 0; k < f->num_inputs; k++) {
                pdc_an_node_t *in_state = PDCan_dg_get_node(dg, f->input_names[k]);
                if (in_state == NULL || vector_contains_ptr(frontier, in_state))
                    continue;
                if (state_is_materialized(entry, f->input_names[k], ref_ndim, ref_offset, ref_size))
                    continue; /* already on disk, its producer need not run again */
                pdc_vector_add(frontier, in_state);
            }
        }
    }

done:
    /* frontier holds borrowed pdc_an_node_t* pointers into the live graph
     * (see an_vector_destroy_shallow) -- only the container is freed. */
    an_vector_destroy_shallow(frontier);
    FUNC_LEAVE(ret_value);
}

/** In-place topological sort of needed_funcs (Kahn's algorithm, O(n^2) — the
 * graphs this runs over are small, this is not a hot path). */
static perr_t
topo_sort_funcs(PDC_VECTOR *needed_funcs, PDC_VECTOR *exec_order)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    size_t n         = pdc_vector_size(needed_funcs);
    bool * scheduled = (n > 0) ? PDC_calloc(n, sizeof(bool)) : NULL;

    for (size_t placed = 0; placed < n;) {
        bool made_progress = false;

        for (size_t i = 0; i < n; i++) {
            if (scheduled[i])
                continue;

            pdc_an_node_t *func_node = (pdc_an_node_t *)pdc_vector_get(needed_funcs, i);
            pdc_an_func_t *f         = &func_node->u.func;

            bool ready = true;
            for (int k = 0; k < f->num_inputs && ready; k++) {
                for (size_t j = 0; j < n; j++) {
                    if (scheduled[j] || j == i)
                        continue;
                    pdc_an_node_t *other = (pdc_an_node_t *)pdc_vector_get(needed_funcs, j);
                    pdc_an_func_t *of    = &other->u.func;
                    for (int m = 0; m < of->num_outputs; m++) {
                        if (!strcmp(of->output_names[m], f->input_names[k])) {
                            ready = false;
                            break;
                        }
                    }
                    if (!ready)
                        break;
                }
            }

            if (ready) {
                pdc_vector_add(exec_order, func_node);
                scheduled[i] = true;
                placed++;
                made_progress = true;
            }
        }

        if (!made_progress)
            PGOTO_ERROR(FAIL, "Cycle detected in analysis graph while computing execution order\n");
    }

done:
    if (scheduled != NULL)
        PDC_free(scheduled);
    FUNC_LEAVE(ret_value);
}

static double
an_rolling_avg(double *history)
{
    double avg = 0.0;
    for (int i = 0; i < NUM_TF_FUNC_TIMES; i++)
        avg += history[i];
    return avg / NUM_TF_FUNC_TIMES;
}

static double
an_expected_exec_time(pdc_an_func_variant_t *v, double avg_cpu_utilization, double avg_gpu_utilization)
{
    double last_avg = an_rolling_avg(v->exec_avg_time);
    if (last_avg < 0.001)
        /* cold-start guess: no principled basis to bias the very first
         * decision toward either device, so both CPU and GPU variants
         * read the same guess here (mirrors select_best_edge's own
         * "< 0.001 -> 0.750" fallback, pdc_tf_server.c). */
        return 0.750;
    return (v->dev == PDC_TF_CPU_DEVICE) ? last_avg / fmax(0.1, 1.0 - avg_cpu_utilization)
                                         : last_avg / fmax(0.1, 1.0 - avg_gpu_utilization);
}

/**
 * Mirrors pdc_tf_server.c's select_best_edge, but chooses among the
 * device-variant candidates of ONE function vertex instead of parallel
 * edges between two state vertices: AN's bipartite graph gives every
 * transformation its own vertex (required for multi-input/output
 * functions), so there is no edge multiplicity to scan the way TF's
 * single-input/output edge-payload graph does -- the same dynamic decision
 * instead lives on pdc_an_func_t's variants array (see PDCan_link_builtin_func).
 * Deliberately does NOT consult TF's polynomial cost model
 * (pdc_tf_poly_sched.c) -- that model is fitted specifically on ZFP-GPU
 * compression data and would mispredict for magnitude/curl; only the
 * generic rolling-average/utilization fallback path is mirrored here.
 */
static pdc_an_func_variant_t *
an_select_variant(pdc_an_func_t *f, double avg_cpu_utilization, double avg_gpu_utilization)
{
    if (f->num_variants == 1)
        return &f->variants[0];

    /* Test-only override, mirroring select_best_edge's always_use_gpu
     * force-override -- lets a correctness/scheduling test pin a device
     * without depending on real utilization timing. */
    const char *force = getenv("PDC_AN_FORCE_DEVICE");
    if (force != NULL) {
        pdc_tf_dev_t forced = (!strcmp(force, "GPU")) ? PDC_TF_GPU_DEVICE : PDC_TF_CPU_DEVICE;
        for (int i = 0; i < f->num_variants; i++)
            if (f->variants[i].dev == forced) {
                LOG_WARNING("SCHED: PDC_AN_FORCE_DEVICE=%s forcing func=%s to variant %d\n", force, f->name,
                            i);
                return &f->variants[i];
            }
    }

    int    best_idx  = 0;
    double best_time = 1e9;
    for (int i = 0; i < f->num_variants; i++) {
        double exp_time = an_expected_exec_time(&f->variants[i], avg_cpu_utilization, avg_gpu_utilization);
        LOG_WARNING("SCHED: func=%s variant=%d dev=%s last_avg_time=%.4f expected_time=%.4f "
                    "best_so_far=%.4f\n",
                    f->name, i, f->variants[i].dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU",
                    an_rolling_avg(f->variants[i].exec_avg_time), exp_time, best_time);
        if (exp_time < best_time) {
            best_time = exp_time;
            best_idx  = i;
        }
    }
    LOG_WARNING("SCHED: chose func=%s variant=%d dev=%s expected_time=%.4f\n", f->name, best_idx,
                f->variants[best_idx].dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU", best_time);
    return &f->variants[best_idx];
}

static void
an_update_exec_time(pdc_an_func_variant_t *v, double projected_time, const char *func_name)
{
    v->exec_avg_time[v->cur_exec_avg_time_index] = projected_time;
    LOG_WARNING("SCHED: updated %s exec_times[%d]=%.4f new_avg=%.4f func=%s\n",
                v->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU", v->cur_exec_avg_time_index, projected_time,
                an_rolling_avg(v->exec_avg_time), func_name);
    v->cur_exec_avg_time_index = (v->cur_exec_avg_time_index + 1) % NUM_TF_FUNC_TIMES;
}

perr_t
PDCan_exec_graph(pdc_an_dg_entry_t *entry, char **target_state_names, int num_targets, uint8_t ref_ndim,
                 const uint64_t *ref_offset, const uint64_t *ref_size)
{
    FUNC_ENTER(NULL);

    perr_t    ret_value = SUCCEED;
    pdc_dg_t *dg        = entry->dg;

    PDC_VECTOR *needed_funcs = pdc_vector_create(8, 2.0);
    PDC_VECTOR *exec_order   = pdc_vector_create(8, 2.0);

    /* scratch map: state name -> buffer/region produced (or read) during
     * this single execution; every entry here is freed once at the end.
     * Nothing here survives past this call, by design (see
     * docs/design/region_analysis.md section 2: transient states are
     * never memoized across executions). */
    PDC_VECTOR *scratch_names   = pdc_vector_create(8, 2.0);
    PDC_VECTOR *scratch_bufs    = pdc_vector_create(8, 2.0);
    PDC_VECTOR *scratch_regions = pdc_vector_create(8, 2.0);

    if (collect_needed_funcs(dg, entry, target_state_names, num_targets, needed_funcs, ref_ndim, ref_offset,
                             ref_size) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to determine which transformations must run\n");
    if (topo_sort_funcs(needed_funcs, exec_order) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to order transformations\n");

    PDC_VECTOR_ITERATOR *order_iter = pdc_vector_iterator_new(exec_order);
    while (pdc_vector_iterator_has_next(order_iter)) {
        pdc_an_node_t *func_node = (pdc_an_node_t *)pdc_vector_iterator_next(order_iter);
        pdc_an_func_t *f         = &func_node->u.func;

        void **          input_bufs    = PDC_calloc((size_t)f->num_inputs, sizeof(void *));
        pdc_tf_region_t *input_regions = PDC_calloc((size_t)f->num_inputs, sizeof(pdc_tf_region_t));

        for (int k = 0; k < f->num_inputs; k++) {
            char *in_name = f->input_names[k];

            int scratch_idx = vector_find_name(scratch_names, in_name);
            if (scratch_idx >= 0) {
                input_bufs[k]    = pdc_vector_get(scratch_bufs, (size_t)scratch_idx);
                input_regions[k] = *(pdc_tf_region_t *)pdc_vector_get(scratch_regions, (size_t)scratch_idx);
                continue;
            }

            pdc_an_binding_t *binding =
                PDCan_find_binding_by_region(entry, in_name, ref_ndim, ref_offset, ref_size);
            if (binding == NULL)
                PGOTO_ERROR(FAIL,
                            "State \"%s\" has no bound region and was not produced earlier in this "
                            "execution\n",
                            in_name);

            size_t   unit  = (size_t)PDC_get_var_type_size(binding->pdc_var_type);
            uint64_t bytes = unit;
            for (int d = 0; d < binding->ndim; d++)
                bytes *= binding->size[d];

            void *in_buf = PDC_malloc(bytes);

            struct pdc_region_info region_info;
            memset(&region_info, 0, sizeof(region_info));
            region_info.ndim   = binding->ndim;
            region_info.offset = binding->offset;
            region_info.size   = binding->size;
            region_info.unit   = unit;

            /* A leaf input's most recent write may still be sitting in the
             * write-back cache, unflushed to storage -- reading straight
             * from disk would silently return stale/empty data, so flush
             * it first. Can't go through the cache-aware read wrapper
             * (PDC_transfer_request_data_read_from) instead: this code can
             * itself run nested inside that same wrapper's locked section
             * (this exec is often triggered as the fallback of an outer
             * PDC_region_fetch, e.g. for the output object being read),
             * and its mutex isn't recursive -- calling it again here would
             * self-deadlock. PDC_region_cache_flush() takes no lock, so
             * it's always safe to call directly.
             *
             * Skip it, though, when this exec was itself triggered by a
             * write's post-write hook (an_eager_exec_in_progress_g): the
             * data is already on disk by construction in that case (the
             * hook only fires after the write succeeds), and flushing
             * again would re-enter PDC_region_cache_flush_by_pointer for
             * an object whose cache entry may still be mid-unwind higher
             * up this same call stack -- see the comment on
             * an_eager_exec_in_progress_g. */
#ifdef PDC_SERVER_CACHE
            if (!an_eager_exec_in_progress_g)
                PDC_region_cache_flush(binding->obj_id);
#endif
            if (PDC_Server_transfer_request_io(binding->obj_id, binding->obj_ndim, binding->obj_dims,
                                               &region_info, in_buf, unit, 0) != SUCCEED) {
                in_buf = PDC_free(in_buf);
                PGOTO_ERROR(FAIL, "Failed to read input state \"%s\"\n", in_name);
            }

            input_bufs[k] = in_buf;
            PDCtf_set_tf_region_t(&input_regions[k], binding->ndim, binding->pdc_var_type, binding->size);

            pdc_vector_add(scratch_names, strdup(in_name));
            pdc_vector_add(scratch_bufs, in_buf);
            pdc_tf_region_t *stored_region = PDC_malloc(sizeof(pdc_tf_region_t));
            *stored_region                 = input_regions[k];
            pdc_vector_add(scratch_regions, stored_region);
        }

        void **          output_bufs    = PDC_calloc((size_t)f->num_outputs, sizeof(void *));
        pdc_tf_region_t *output_regions = PDC_calloc((size_t)f->num_outputs, sizeof(pdc_tf_region_t));

        pdc_tf_internal_param internal_params = {0};
        internal_params.dg                    = dg;

        /* Fast path (the common case today: every builtin graph currently
         * declares exactly one device per transformation) skips profiling
         * entirely. Only a transformation whose JSON omitted "device" ever
         * has num_variants > 1. */
        double avg_cpu_utilization = 0.0, avg_gpu_utilization = 0.0;
        if (f->num_variants > 1) {
            avg_cpu_utilization = pdc_tf_avg_cpu_utilization();
            pdc_tf_nvml_profiler_update();
            avg_gpu_utilization = pdc_tf_avg_gpu_utilization(0); /* first cut: GPU device 0 only */
        }
        pdc_an_func_variant_t *chosen = an_select_variant(f, avg_cpu_utilization, avg_gpu_utilization);

#ifdef CUDA_ENABLED
        /* chosen->dev can only be PDC_TF_GPU_DEVICE here when CUDA_ENABLED
         * registered a GPU variant (see PDCan_init_builtin_funcs) --
         * unreachable, not just untested, without it. */
        if (chosen->dev == PDC_TF_GPU_DEVICE) {
            cudaError_t cerr = cudaSetDevice(0);
            if (cerr != cudaSuccess)
                PGOTO_ERROR(FAIL, "Failed to set CUDA device 0 for transformation \"%s\"\n", f->name);
        }
#endif

        struct timespec an_func_t0, an_func_t1;
        clock_gettime(CLOCK_MONOTONIC, &an_func_t0);
        bool an_func_ok = chosen->a_func(&internal_params, chosen->params_str, input_bufs, input_regions,
                                         f->num_inputs, output_bufs, output_regions, f->num_outputs);
        clock_gettime(CLOCK_MONOTONIC, &an_func_t1);
        double an_exec_time =
            (an_func_t1.tv_sec - an_func_t0.tv_sec) + (an_func_t1.tv_nsec - an_func_t0.tv_nsec) / 1e9;

        /* Only "magnitude" is one of the three tracked computation
         * metrics (see PDC_stat_metric_t) -- "curl" and anything else
         * registered here isn't recorded. This is a separate, session-long
         * close-time summary metric -- independent of the per-variant
         * rolling-average scheduling bookkeeping below. */
        if (strstr(f->name, "magnitude") != NULL)
            PDC_stats_record(PDC_STAT_MAGNITUDE, an_exec_time);

        if (f->num_variants > 1) {
            double utilization =
                (chosen->dev == PDC_TF_CPU_DEVICE) ? avg_cpu_utilization : avg_gpu_utilization;
            double projected_time = an_exec_time * (1.0 - fmax(utilization, 0.1));
            an_update_exec_time(chosen, projected_time, f->name);
        }

        if (!an_func_ok) {
            PDC_free(input_bufs);
            PDC_free(input_regions);
            PDC_free(output_bufs);
            PDC_free(output_regions);
            PGOTO_ERROR(FAIL, "Transformation \"%s\" failed\n", f->name);
        }

        for (int o = 0; o < f->num_outputs; o++) {
            char *          out_name  = f->output_names[o];
            pdc_an_state_t *out_state = PDCan_dg_get_state(dg, out_name);

            if (out_state->persistence == PDC_AN_PERSISTENT) {
                pdc_an_binding_t *binding =
                    PDCan_find_binding_by_region(entry, out_name, ref_ndim, ref_offset, ref_size);
                if (binding == NULL)
                    PGOTO_ERROR(FAIL, "Persistent output \"%s\" was never attached to an object/region\n",
                                out_name);

                size_t unit = (size_t)PDC_get_var_type_size(output_regions[o].pdc_var_type);

                struct pdc_region_info region_info;
                memset(&region_info, 0, sizeof(region_info));
                region_info.ndim   = output_regions[o].ndim;
                region_info.offset = binding->offset;
                region_info.size   = output_regions[o].size;
                region_info.unit   = unit;

                if (PDC_Server_transfer_request_io(binding->obj_id, binding->obj_ndim, binding->obj_dims,
                                                   &region_info, output_bufs[o], unit, 1) != SUCCEED)
                    PGOTO_ERROR(FAIL, "Failed to write persistent output \"%s\"\n", out_name);

                binding->materialized = true;
            }

            pdc_vector_add(scratch_names, strdup(out_name));
            pdc_vector_add(scratch_bufs, output_bufs[o]);
            pdc_tf_region_t *stored_region = PDC_malloc(sizeof(pdc_tf_region_t));
            *stored_region                 = output_regions[o];
            pdc_vector_add(scratch_regions, stored_region);
        }

        PDC_free(input_bufs);
        PDC_free(input_regions);
        PDC_free(output_bufs);
        PDC_free(output_regions);
    }

done:
    if (order_iter != NULL)
        pdc_vector_iterator_destroy(order_iter);

    /* pdc_vector_destroy() frees every stored item as well as the
     * container -- exactly what we want for the scratch vectors, since
     * every buffer produced or read during this execution is owned by
     * this call (a persistent write above only reads bytes out of the
     * buffer, it does not take ownership of it). Do NOT do a manual
     * free-then-destroy here, or every item gets freed twice. */
    if (scratch_names != NULL)
        pdc_vector_destroy(scratch_names);
    if (scratch_bufs != NULL)
        pdc_vector_destroy(scratch_bufs);
    if (scratch_regions != NULL)
        pdc_vector_destroy(scratch_regions);

    /* needed_funcs/exec_order hold pdc_an_node_t* pointers into the live
     * graph -- not owned by this call. pdc_vector_destroy() would free
     * them (and corrupt the graph), so only the containers are freed. */
    an_vector_destroy_shallow(needed_funcs);
    an_vector_destroy_shallow(exec_order);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCan_notify_input_written(pdcid_t obj_id, uint8_t ndim, uint64_t *offset, uint64_t *size)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    bool   acquired  = false;

    pdc_an_dg_entry_t *entry   = NULL;
    pdc_an_binding_t * binding = NULL;
    if (!find_binding_index_entry(obj_id, ndim, offset, size, 0, &entry, &binding))
        PGOTO_DONE(SUCCEED); /* not a bound analysis input, nothing to do */

    binding->materialized = true;

    if (an_eager_exec_in_progress_g)
        /* Nested call: this write is itself a side effect of an eager
         * computation already running higher up this call stack (an
         * output being persisted, or a reentrant cache flush). The
         * materialization above is still useful bookkeeping; don't also
         * recurse into another fixed-point pass -- the outer call's own
         * pass will see this binding as materialized on its next
         * iteration. */
        PGOTO_DONE(SUCCEED);

    an_eager_exec_in_progress_g = true;
    acquired                    = true;

    /* Fixed-point pass over every transformation in the graph: any whose
     * inputs are now all materialized gets computed and persisted eagerly.
     * A single write can only ever directly ready one transformation, but
     * that transformation's own (persistent) outputs may in turn ready a
     * downstream one, so repeat until a full pass makes no further
     * progress. A transformation with any transient input can never be
     * judged "ready" this way (transient states have no binding to check)
     * -- it's left to the existing lazy read-triggered path, which
     * computes transient intermediates fresh within one call regardless. */
    bool progress = true;
    while (progress) {
        progress = false;

        for (uint32_t i = 0; i < entry->dg->vertex_count; i++) {
            pdc_an_node_t *node = (pdc_an_node_t *)entry->dg->vertices[i]->data;
            if (node->kind != PDC_AN_NODE_FUNC)
                continue;
            pdc_an_func_t *f = &node->u.func;

            bool already_done = false;
            for (int o = 0; o < f->num_outputs; o++) {
                pdc_an_binding_t *ob = PDCan_find_binding_by_region(entry, f->output_names[o], binding->ndim,
                                                                    binding->offset, binding->size);
                if (ob != NULL && ob->materialized) {
                    already_done = true;
                    break;
                }
            }
            if (already_done)
                continue;

            bool all_ready = true;
            for (int k = 0; k < f->num_inputs; k++) {
                pdc_an_state_t *in_state = PDCan_dg_get_state(entry->dg, f->input_names[k]);
                if (in_state != NULL && in_state->persistence == PDC_AN_TRANSIENT) {
                    all_ready = false;
                    break;
                }
                pdc_an_binding_t *ib = PDCan_find_binding_by_region(entry, f->input_names[k], binding->ndim,
                                                                    binding->offset, binding->size);
                if (ib == NULL || !ib->materialized) {
                    all_ready = false;
                    break;
                }
            }
            if (!all_ready)
                continue;

            /* A transformation is only eagerly triggered here if every one
             * of its declared outputs opts into eager (the default) --
             * any output declared "lazy" in the graph JSON defers this
             * whole transformation to the read-triggered path in
             * PDC_Server_data_io_region_analysis instead, regardless of
             * how/when the client attached it (see pdc_an_trigger_t). */
            bool outputs_eager = true;
            for (int o = 0; o < f->num_outputs; o++) {
                pdc_an_state_t *out_state = PDCan_dg_get_state(entry->dg, f->output_names[o]);
                if (out_state != NULL && out_state->trigger == PDC_AN_TRIGGER_LAZY) {
                    outputs_eager = false;
                    break;
                }
            }
            if (!outputs_eager)
                continue;

            if (PDCan_exec_graph(entry, f->output_names, f->num_outputs, binding->ndim, binding->offset,
                                 binding->size) != SUCCEED) {
                LOG_ERROR("Eager write-triggered analysis failed for transformation \"%s\"\n", f->name);
                continue;
            }
            progress = true;
        }
    }

done:
    if (acquired)
        an_eager_exec_in_progress_g = false;
    FUNC_LEAVE(ret_value);
}

perr_t
PDC_Server_data_io_region_analysis(uint64_t obj_id, int obj_ndim, const uint64_t *obj_dims,
                                   struct pdc_region_info *region_info, void *buf, size_t unit, int is_write,
                                   bool *ran_analysis)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    *ran_analysis    = false;

    /* Analysis outputs are never client-written directly; only
     * PDCan_exec_graph writes them. A write to an object that happens to
     * be an analysis output is therefore always an ordinary write to
     * that object (e.g. before it's ever attached as an output) and
     * should fall through to the normal storage path untouched. */
    if (is_write)
        PGOTO_DONE(SUCCEED);

    pdc_an_dg_entry_t *entry   = NULL;
    pdc_an_binding_t * binding = NULL;
    if (!PDCan_region_is_analysis_output((pdcid_t)obj_id, (uint8_t)region_info->ndim, region_info->offset,
                                         region_info->size, &entry, &binding))
        PGOTO_DONE(SUCCEED);

    *ran_analysis = true;

    /* Transparent, exactly like PDC TF's read-side inverse transform: a
     * read of an unmaterialized output always triggers the minimal
     * computation needed to produce it, with no separate "run" step the
     * client must call first.
     *
     * Guard an_eager_exec_in_progress_g here exactly like
     * PDCan_notify_input_written's write-triggered pass does, for the same
     * reason: PDCan_exec_graph below reads each leaf input via
     * PDC_Server_transfer_request_io, which re-enters this very function
     * (PDC_Server_data_io_region_analysis is called from inside
     * PDC_Server_transfer_request_io -- see that function). Left
     * unguarded, PDCan_exec_graph's per-leaf-input PDC_region_cache_flush
     * call would re-enter PDC_region_cache_flush_by_pointer for an object
     * whose cache entry can still be mid-unwind higher up this exact call
     * stack (the outer read that got us here in the first place) -- see
     * the comment on an_eager_exec_in_progress_g's declaration for why
     * that specific reentrancy corrupts a linked list rather than just
     * recursing. */
    if (!binding->materialized) {
        char *targets[1];
        targets[0] = binding->state_name;

        bool was_in_progress        = an_eager_exec_in_progress_g;
        an_eager_exec_in_progress_g = true;
        perr_t exec_ret = PDCan_exec_graph(entry, targets, 1, binding->ndim, binding->offset, binding->size);
        an_eager_exec_in_progress_g = was_in_progress;

        if (exec_ret != SUCCEED)
            PGOTO_ERROR(FAIL, "Failed to compute analysis output \"%s\"\n", binding->state_name);
    }

    /* Can't just call PDC_Server_transfer_request_io here (it would
     * re-enter this same function and recurse) -- but reading raw via
     * PDC_Server_data_read_from alone would skip any PDC TF transform
     * separately attached to this output object (e.g. compression), even
     * though PDCan_exec_graph's own write above *does* go through
     * PDC_Server_transfer_request_io and so already composes correctly
     * with one. Call the TF-specific function directly instead: it does
     * the full "read raw bytes, invert if a graph is attached" round trip
     * itself and reports back whether anything was actually attached, so
     * on-disk and materialized-cache reads stay symmetric with the write
     * path -- see docs/design/region_analysis.md. */
    bool ran_transformation = false;
    if (PDC_Server_data_io_region_per_file_transformations(obj_id, obj_ndim, obj_dims, region_info, buf, unit,
                                                           0, &ran_transformation) != SUCCEED)
        PGOTO_ERROR(FAIL, "Error with PDC_Server_data_io_region_per_file_transformations for \"%s\"\n",
                    binding->state_name);

    if (!ran_transformation && PDC_Server_data_read_from(obj_id, region_info, buf, unit) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to read materialized analysis output \"%s\"\n", binding->state_name);

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCan_checkpoint(FILE *file)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    /* json_filepath <-> graph_id mapping first, written exactly once per
     * unique path this server has ever loaded (not once per thing that
     * references a graph) -- see an_graph_path_registry_g's own comment.
     * PDCan_restart_init must rebuild this before it can resolve any
     * graph_id read below back to a filepath. */
    size_t num_paths = (an_graph_path_registry_g != NULL) ? pdc_vector_size(an_graph_path_registry_g) : 0;
    fwrite(&num_paths, sizeof(size_t), 1, file);

    for (size_t p = 0; p < num_paths; p++) {
        pdc_an_graph_path_entry_t *path_entry =
            (pdc_an_graph_path_entry_t *)pdc_vector_get(an_graph_path_registry_g, p);

        fwrite(&path_entry->graph_id, sizeof(uint32_t), 1, file);
        size_t path_len = strlen(path_entry->json_filepath) + 1;
        fwrite(&path_len, sizeof(size_t), 1, file);
        fwrite(path_entry->json_filepath, sizeof(char), path_len, file);
    }

    size_t num_graphs = (an_dg_registry_g != NULL) ? pdc_vector_size(an_dg_registry_g) : 0;
    fwrite(&num_graphs, sizeof(size_t), 1, file);

    for (size_t g = 0; g < num_graphs; g++) {
        pdc_an_dg_entry_t *entry = (pdc_an_dg_entry_t *)pdc_vector_get(an_dg_registry_g, g);

        /* graph_id, not the filepath itself -- resolved against the
         * mapping just written above. */
        fwrite(&entry->graph_id, sizeof(uint32_t), 1, file);

        size_t num_bindings = pdc_vector_size(entry->bindings_vector);
        fwrite(&num_bindings, sizeof(size_t), 1, file);

        for (size_t b = 0; b < num_bindings; b++) {
            pdc_an_binding_t *binding = (pdc_an_binding_t *)pdc_vector_get(entry->bindings_vector, b);

            size_t name_len = strlen(binding->state_name) + 1;
            fwrite(&name_len, sizeof(size_t), 1, file);
            fwrite(binding->state_name, sizeof(char), name_len, file);

            fwrite(&binding->obj_id, sizeof(pdcid_t), 1, file);
            fwrite(&binding->obj_ndim, sizeof(int), 1, file);
            fwrite(binding->obj_dims, sizeof(uint64_t), (size_t)binding->obj_ndim, file);
            fwrite(&binding->ndim, sizeof(uint8_t), 1, file);
            fwrite(binding->offset, sizeof(uint64_t), (size_t)binding->ndim, file);
            fwrite(binding->size, sizeof(uint64_t), (size_t)binding->ndim, file);
            fwrite(&binding->pdc_var_type, sizeof(pdc_var_type_t), 1, file);
            int materialized_int = binding->materialized ? 1 : 0;
            fwrite(&materialized_int, sizeof(int), 1, file);
        }
    }

    FUNC_LEAVE(ret_value);
}

perr_t
PDCan_restart_init(FILE *file)
{
    FUNC_ENTER(NULL);

    perr_t   ret_value         = SUCCEED;
    size_t   num_paths         = 0;
    uint32_t max_graph_id_seen = 0;
    bool     any_path          = false;

    /* Rebuild the json_filepath <-> graph_id mapping first -- every
     * graph_id read below (from an_dg_registry_g's own entries) is only
     * resolvable once this exists. */
    if (fread(&num_paths, sizeof(size_t), 1, file) != 1)
        PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint path-registry count\n");

    if (num_paths > 0)
        an_graph_path_registry_g = pdc_vector_create(PDC_MAX(num_paths, 8), 2.0);

    for (size_t p = 0; p < num_paths; p++) {
        pdc_an_graph_path_entry_t *path_entry = PDC_malloc(sizeof(pdc_an_graph_path_entry_t));

        if (fread(&path_entry->graph_id, sizeof(uint32_t), 1, file) != 1)
            PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint graph_id\n");

        size_t path_len = 0;
        if (fread(&path_len, sizeof(size_t), 1, file) != 1 || path_len == 0)
            PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint json_filepath length\n");
        path_entry->json_filepath = PDC_calloc(1, path_len);
        if (fread(path_entry->json_filepath, sizeof(char), path_len, file) != path_len)
            PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint json_filepath\n");

        pdc_vector_add(an_graph_path_registry_g, path_entry);
        if (!any_path || path_entry->graph_id > max_graph_id_seen)
            max_graph_id_seen = path_entry->graph_id;
        any_path = true;
    }
    /* Any graph loaded fresh after this restart (a newly attached graph
     * this checkpoint never saw) must get a graph_id that can't collide
     * with one already in the mapping just rebuilt above. */
    an_next_graph_id_g = any_path ? max_graph_id_seen + 1 : 0;

    size_t num_graphs = 0;
    if (fread(&num_graphs, sizeof(size_t), 1, file) != 1)
        PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint graph count\n");

    if (num_graphs == 0)
        PGOTO_DONE(SUCCEED);

    if (an_dg_registry_g == NULL)
        an_dg_registry_g = pdc_vector_create(PDC_MAX(num_graphs, 8), 2.0);

    for (size_t g = 0; g < num_graphs; g++) {
        uint32_t graph_id = 0;
        if (fread(&graph_id, sizeof(uint32_t), 1, file) != 1)
            PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint graph_id\n");

        const char *json_filepath = PDCan_get_graph_path(graph_id);
        if (json_filepath == NULL)
            PGOTO_ERROR(FAIL,
                        "Analysis checkpoint references graph_id %u with no matching entry in its own "
                        "path registry (corrupt checkpoint)\n",
                        graph_id);

        pdc_dg_t *dg = PDCan_dg_json_create_common((char *)json_filepath);
        if (dg == NULL)
            PGOTO_ERROR(FAIL,
                        "Failed to reload analysis graph \"%s\" on restart; the JSON graph definition "
                        "file must still exist at its original path\n",
                        json_filepath);

        pdc_an_dg_entry_t *entry = PDC_calloc(1, sizeof(pdc_an_dg_entry_t));
        entry->json_filepath     = strdup(json_filepath);
        entry->graph_id          = graph_id;
        entry->dg                = dg;

        size_t num_bindings = 0;
        if (fread(&num_bindings, sizeof(size_t), 1, file) != 1)
            PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint binding count\n");
        entry->bindings_vector = pdc_vector_create(PDC_MAX(num_bindings, 8), 2.0);

        pdc_vector_add(an_dg_registry_g, entry);

        for (size_t b = 0; b < num_bindings; b++) {
            size_t name_len = 0;
            if (fread(&name_len, sizeof(size_t), 1, file) != 1 || name_len == 0)
                PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint state name length\n");
            char *state_name = PDC_calloc(1, name_len);
            if (fread(state_name, sizeof(char), name_len, file) != name_len)
                PGOTO_ERROR(FAIL, "Failed to read analysis checkpoint state name\n");

            pdc_an_binding_t *binding = PDC_calloc(1, sizeof(pdc_an_binding_t));
            binding->state_name       = state_name;

            fread(&binding->obj_id, sizeof(pdcid_t), 1, file);
            fread(&binding->obj_ndim, sizeof(int), 1, file);
            fread(binding->obj_dims, sizeof(uint64_t), (size_t)binding->obj_ndim, file);
            fread(&binding->ndim, sizeof(uint8_t), 1, file);
            fread(binding->offset, sizeof(uint64_t), (size_t)binding->ndim, file);
            fread(binding->size, sizeof(uint64_t), (size_t)binding->ndim, file);
            fread(&binding->pdc_var_type, sizeof(pdc_var_type_t), 1, file);
            int materialized_int = 0;
            fread(&materialized_int, sizeof(int), 1, file);
            binding->materialized = (materialized_int != 0);

            pdc_vector_add(entry->bindings_vector, binding);

            /* Rebuild the reverse index exactly as PDCan_store_attach_mapping
             * would have when the binding was first attached -- every
             * binding, input or output, goes in. */
            if (an_obj_id_to_binding_vector_g == NULL)
                an_obj_id_to_binding_vector_g = pdc_vector_create(8, 2.0);

            pdc_an_binding_index_entry_t *idx = PDC_malloc(sizeof(pdc_an_binding_index_entry_t));
            idx->dg_entry                     = entry;
            idx->binding                      = binding;
            pdc_vector_add(an_obj_id_to_binding_vector_g, idx);
        }
    }

done:
    FUNC_LEAVE(ret_value);
}
