/**
 * NOTICE: This file is included in custom region-analysis libraries.
 * It should not include any headers that are not available to the custom
 * analysis library. It should also not include any headers that define
 * symbols that may conflict with symbols in the custom analysis library.
 */
#ifndef PDC_AN_USER_H
#define PDC_AN_USER_H

#include <stdbool.h>

#include "pdc_vector.h"
#include "pdc_dg.h"
#include "pdc_tf_user.h"

/**
 * Whether a state's data is kept only for the duration of one execution
 * (transient) or written to storage and reusable across executions
 * (persistent). See docs/design/region_analysis.md.
 */
typedef enum pdc_an_persistence_t {
    PDC_AN_TRANSIENT,
    PDC_AN_PERSISTENT,
    PDC_AN_NUM_PERSISTENCE
} pdc_an_persistence_t;
extern char *pdc_an_persistence_strs[];

/**
 * Whether an output state's producing transformation runs as soon as its
 * inputs are all written (eager -- PDCan_notify_input_written's
 * write-triggered pass) or is deferred until the state is first read
 * (lazy -- PDC_Server_data_io_region_analysis's read-triggered path,
 * which remains the universal fallback for ANY unmaterialized bound
 * output regardless of this setting, eager included). This is a property
 * of the graph itself, declared per output state in its JSON, not of
 * when/how a client happens to call PDCan_attach_to_region -- a lazy
 * output stays lazy even if attached before its inputs are written,
 * and an eager output attached only after its inputs are already
 * durable (e.g. in a fresh process against already-checkpointed data)
 * still just falls through to the same read-triggered path, since the
 * write it would have eagerly triggered on already happened without it.
 * Meaningless on a non-output state (never produced by any
 * transformation); defaults to eager if omitted from the JSON, matching
 * this framework's original (attach-order-determined) behavior.
 */
typedef enum pdc_an_trigger_t {
    PDC_AN_TRIGGER_EAGER,
    PDC_AN_TRIGGER_LAZY,
    PDC_AN_NUM_TRIGGER
} pdc_an_trigger_t;
extern char *pdc_an_trigger_strs[];

typedef struct pdc_an_state_t {
    char *               name;
    pdc_an_persistence_t persistence;
    pdc_an_trigger_t     trigger;   /* only meaningful once is_output is true */
    bool                 is_output; /* true once some transformation lists this state as an output */
} pdc_an_state_t;

/**
 * Prototype for region-analysis functions: multi-input, multi-output.
 *
 * input_bufs[i]/input_regions[i] describe the i-th input buffer (i < num_inputs).
 * output_bufs[i]/output_regions[i] are the i-th output slot (i < num_outputs);
 * the function must set output_bufs[i] to a heap-allocated buffer (PDC will
 * free it) and populate output_regions[i] with its shape.
 */
typedef bool (*a_func_t)(pdc_tf_internal_param *internal_param, char *params_str, void **input_bufs,
                         pdc_tf_region_t *input_regions, int num_inputs, void **output_bufs,
                         pdc_tf_region_t *output_regions, int num_outputs);

/**
 * One device-specific implementation of a transformation, with its own
 * rolling-average execution-time history (mirrors pdc_tf_func_t's
 * exec_avg_time, pdc_tf_user.h:71) so the dynamic scheduler in
 * PDCan_exec_graph can pick among several variants of the same named
 * transformation the same way PDCtf_exec_graph's select_best_edge picks
 * among parallel edges.
 */
typedef struct pdc_an_func_variant_t {
    pdc_tf_dev_t      dev;
    pdc_tf_location_t location;
    a_func_t          a_func;
    char *            params_str;
    uint32_t          cur_exec_avg_time_index;
    double            exec_avg_time[NUM_TF_FUNC_TIMES];
} pdc_an_func_variant_t;

typedef struct pdc_an_func_t {
    char *name; /* unprefixed transformation name, e.g. "merge_fields" */

    pdc_an_func_variant_t *variants;
    int                    num_variants;

    char **input_names;
    int    num_inputs;
    char **output_names;
    int    num_outputs;
} pdc_an_func_t;

typedef enum pdc_an_node_kind_t { PDC_AN_NODE_STATE, PDC_AN_NODE_FUNC } pdc_an_node_kind_t;

/**
 * A region-analysis graph is bipartite: state vertices and transformation
 * (function) vertices are both stored as pdc_dg_t vertices, distinguished by
 * `kind`. Edges run state -> function for each of the function's declared
 * inputs, and function -> state for each of its declared outputs.
 *
 * pdc_dg identifies vertices by comparing this struct's `name` field
 * (see an_vertices_are_equal in pdc_an_common.c), so function vertex names
 * are namespaced with PDC_AN_FUNC_NODE_PREFIX to guarantee they can never
 * collide with a state name.
 */
typedef struct pdc_an_node_t {
    char *             name;
    pdc_an_node_kind_t kind;
    union {
        pdc_an_state_t state;
        pdc_an_func_t  func;
    } u;
} pdc_an_node_t;

#define PDC_AN_FUNC_NODE_PREFIX "fn:"

#endif /* PDC_AN_USER_H */
