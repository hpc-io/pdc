# Region Analysis — Client-Facing Design

Status: implemented (server executor, checkpoint/restart, client-server RPC
wiring, and the client API below). No client test/example program yet.
Companion to the existing PDC Transformation Framework (PDC TF) — see `docs/source/pdc_tf_docs.rst`.

## 1. What this is

Region transformations (PDC TF) let one region of one object be transparently
transformed on write and inverted on read (compress, encrypt, ...). Region
analysis generalizes this to **multi-object, multi-in/multi-out** graphs:
a transformation node can consume regions from several different objects
and produce regions for several different (new) objects.

The two features share concepts (`states`, `device`, `location`, JSON-defined
graphs, per-rank execution with no cross-rank coordination) but are distinct:
PDC TF graphs describe how *one region* moves between states of *one object*;
region analysis graphs describe how regions from *multiple objects* combine
into new objects. They will live side by side (`PDCtf_*` vs `PDCan_*`), not
replace one another.

## 2. Core concepts

- **State**: a named node in the graph. Every state is either an *input*
  (never produced by a transformation in this graph — a leaf) or an
  *output* (produced by exactly one transformation). A state can be both
  an output of one transformation and an input to another (an
  intermediate/checkpoint state).
- **Persistence** (`transient` | `persistent`), a property of a state:
  - `transient`: the buffer exists only in server memory for the duration
    of one execution. Never written to storage, never has an object
    identity, cannot be attached to a region. Recomputed from scratch on
    every execution that needs it — nothing about a transient state
    survives between runs.
  - `persistent`: the buffer is written to storage under an object/region
    the client explicitly attaches, and a `materialized` flag lets later
    reads skip recomputing it.
- **Trigger** (`eager` | `lazy`, default `eager`), a property of an
  *output* state (meaningless on a leaf/input state): whether the
  transformation producing this state runs as soon as every one of its
  inputs is written (`eager`) or is deferred until the state is first
  read (`lazy`). This is a property of the graph, declared once in its
  JSON — not something a client encodes by choosing when to call
  `PDCan_attach_to_region` relative to writing the inputs. See §4a.
- **Transformation**: a node with N named input states and M named output
  states, a `device` (`CPU`|`GPU`), and a `location` (`builtin`|`external`).
  This is the multi-in/multi-out generalization of a PDC TF edge.
- **Graph**: the full set of states + transformations, loaded from JSON,
  identified client-side by a `dg_id`.

Reading an *output* state always transparently computes it if it isn't
already materialized — exactly like PDC TF's inverse transform on read,
with no separate "run the analysis" call — and only computes the minimal
set of transformations needed to produce the requested output. Upstream
states that are already materialized (persistent, on disk from an
earlier execution) are reused rather than recomputed. This read-triggered
path is the universal fallback for *any* unmaterialized bound output,
`eager`-declared ones included — it's what actually produces a `lazy`
output the first time it's read, and it's also what a client sees if it
attaches an `eager` output only after its inputs are already durable
(the write that would have eagerly triggered it already happened without
that attachment in place, e.g. a fresh process reading data a previous,
unrelated process wrote and checkpointed).

In addition, writing the *last* required input of an `eager`-declared
output transformation transparently triggers that computation immediately
in the write path, before the write's own RPC returns — a chain of
`persistent` intermediate `eager` outputs cascades in one pass. A
transformation with any `transient` input can never be write-triggered
this way (transient states have no binding to check readiness against),
so it's only ever reached via the read-triggered path regardless of its
own trigger declaration.

## 3. JSON graph format

```json
{
  "name": "multi_field_stats",
  "lib_path": "libanalysis.so",
  "states": [
    { "name": "temp_in",     "persistence": "transient" },
    { "name": "pressure_in", "persistence": "transient" },
    { "name": "merged",      "persistence": "transient" },
    { "name": "stats_mean",  "persistence": "persistent", "trigger": "lazy" },
    { "name": "stats_debug", "persistence": "persistent", "trigger": "lazy" }
  ],
  "transformations": [
    {
      "name": "merge_fields",
      "device": "CPU",
      "location": "builtin",
      "inputs":  [ "temp_in", "pressure_in" ],
      "outputs": [ "merged" ]
    },
    {
      "name": "reduce_stats",
      "device": "CPU",
      "location": "external",
      "inputs":  [ "merged" ],
      "outputs": [ "stats_mean", "stats_debug" ],
      "params": "window=8"
    }
  ]
}
```

Notes:
- `inputs`/`outputs` are arrays of state names (the multi-in/multi-out
  generalization of PDC TF's singular `input_state`/`output_state`).
- No object names, container names, or types appear in the JSON. Object
  identity is bound at runtime by the client (§5), not declared here —
  the graph only describes shape and dataflow, not placement.
- `lib_path` + `location: "external"` reuse the existing PDC TF
  dlopen/dlsym mechanism unchanged.
- `trigger` is optional per state (default `eager`) and only meaningful
  on an output. All of one transformation's outputs are produced
  together in a single execution, so they're only ever write-triggered
  together too: the write-triggered pass requires *every* declared
  output of a transformation to be `eager` before running it early;
  if any one of them is `lazy`, the whole transformation is left to the
  read-triggered path (§4a) — there's no such thing as write-triggering
  half of one execution's outputs.

## 4. Client API, in the order you use it

```c
pdcid_t PDCan_dg_json_create(char *json_filepath);

/* Attach a graph state (input or output) to this rank's local region of an
 * object. Direction is inferred from the graph:
 *  - state is a leaf (input)  -> region is a data source for execution.
 *  - state is produced (output) -> region is where results are written,
 *    AND the region is registered so that reading it is recognized as
 *    reading an analysis output.
 * Errors if state_name isn't a state in this graph at all. */
perr_t PDCan_attach_to_region(pdcid_t dg_id, char *state_name, pdcid_t obj_id, pdcid_t region);

perr_t PDCan_close_dg(pdcid_t dg_id);
```

There is deliberately **no run/request API and no separate read API** —
this mirrors PDC TF exactly, which has no "start the transform" call
either. Once an output state is attached to an object/region, that object
is read with the ordinary `PDCregion_transfer_create(buf, PDC_READ, obj,
reg, reg_global)` / `start`/`wait`/`close` sequence — same as reading any
PDC object, transformed or not. Whether that read is a plain storage read
or triggers computation first is decided server-side, purely by
materialization state, invisibly to this call.

## 4a. `PDCan_attach_to_region` call order does not matter

Earlier versions of the eager-vs-lazy distinction were made entirely by
*when* the client called `PDCan_attach_to_region` relative to writing the
inputs — attach before writing to get write-triggered computation, attach
only afterward to fall through to the read-triggered path. That's no
longer how it works: eager-vs-lazy is a property of the *graph JSON*
(§2's `trigger` field), checked server-side inside
`PDCan_notify_input_written` before it write-triggers a transformation.
A client can attach every state — inputs and outputs alike — before
writing anything, for both an `eager` and a `lazy` graph, and get the
declared behavior either way:

- `eager` output, attached before the writes: the last input write
  write-triggers it, exactly as before.
- `lazy` output, attached before the writes: the write-triggered pass
  sees it declared `lazy` and skips it; the first read still transparently
  triggers it, via the same read-triggered path a `lazy` output has
  always used.

The one case that's still governed by physical timing rather than the
`trigger` field: attaching an `eager` output *after* its inputs are
already durable (a later process, or a restart, discovering data someone
else already wrote). The write that would have eagerly triggered it
already happened without that attachment in place, so there's nothing
left to write-trigger — it falls through to the read-triggered path
regardless of its `eager` declaration. This is expected, not a bug: the
declaration governs what happens when a write *could* trigger it, not a
promise to retroactively compute something the moment it's attached.

## 5. Worked example (single rank shown; every rank does the same calls
   against its own regions)

```c
/* Objects are created the normal way — analysis does not create objects
 * on the client's behalf. */
pdcid_t temp_obj  = PDCobj_create(cont, "temperature", obj_prop);
pdcid_t pres_obj  = PDCobj_create(cont, "pressure",    obj_prop);
pdcid_t mean_obj  = PDCobj_create(cont, "stats_mean",  stats_prop);
pdcid_t debug_obj = PDCobj_create(cont, "stats_debug", stats_prop);

pdcid_t reg = PDCregion_create(...);   /* this rank's local region, reused
                                           across all four objects for
                                           simplicity — need not match in
                                           general, just needs to make
                                           sense to the transformation */

pdcid_t dg = PDCan_dg_json_create(AN_GRAPHS_DIR "multi_field_stats.json");
PDCan_attach_to_region(dg, "temp_in",     temp_obj,  reg);
PDCan_attach_to_region(dg, "pressure_in", pres_obj,  reg);
PDCan_attach_to_region(dg, "stats_mean",  mean_obj,  reg);
PDCan_attach_to_region(dg, "stats_debug", debug_obj, reg);

/* Ordinary writes -- nothing analysis-related triggers here, because
 * both of this graph's outputs (stats_mean, stats_debug) declare
 * "trigger": "lazy" (§3). Change either to "eager" and the same write
 * of pressure_in (the last of merge_fields' two inputs) would instead
 * transparently compute+persist reduce_stats' outputs right here, in
 * this write's own RPC -- see §4a for why that's independent of the
 * attach order shown above. */
write_region(temp_obj, reg, temp_data);
write_region(pres_obj, reg, pressure_data);

/* Reading a bound output transparently computes it (and everything it
 * depends on) the first time, then just reads the materialized bytes on
 * every read after that. */
read_region(mean_obj,  reg, mean_out);
/* -> not materialized yet -> server computes merge_fields + reduce_stats
 *    transparently, then serves the read. */
read_region(debug_obj, reg, debug_out);
/* -> reduce_stats already ran for stats_mean, and it produces stats_debug
 *    too, so this may already be materialized -- otherwise the same
 *    transparent computation happens here. */

PDCan_close_dg(dg);
```

`merged` is never attached to anything — it's `transient`, so it exists only
as an in-memory hand-off between `merge_fields` and `reduce_stats` during
whichever execution needs it, and is discarded immediately after.

## 6. Attaching outputs vs. inputs — what's actually different

Both directions use the same call, but the server does different bookkeeping:

| | Input state attach | Output state attach |
|---|---|---|
| Registers | `(dg_id, state) -> (obj_id, region)` as a data **source** | Same, as a data **sink**, *and* `(obj_id, region) -> (dg_id, state)` in a reverse index |
| Effect on reads of that object/region | None — reads are ordinary object reads, analysis is not involved | Reads are recognized as analysis-output reads; transparently trigger computation if not yet materialized |
| Effect on writes of that object/region | Marks this input materialized; if that makes some transformation's inputs all-materialized *and* all its declared outputs are `eager`, write-triggers it immediately (§2, §4a) — otherwise a no-op | N/A (outputs are never client-written; the executor is the only writer) |

The reverse index is the piece that matters most operationally: without it,
a read against `mean_obj`/`reg` would just be a normal read of whatever
bytes happen to be there (or a "no such data" error if nothing was ever
written). Attaching it as an output is what makes the region *mean*
something to the analysis system at read time.

## 7. Multi-rank / MPI semantics from the client's perspective

Every call above (`PDCan_attach_to_region`, writes, reads) is scoped to the
calling rank's own regions. A graph attached identically by every rank
against each rank's own local regions of shared objects
`temperature`/`pressure`/`stats_mean`/`stats_debug` runs independently per
rank — rank *i*'s execution only ever touches rank *i*'s regions. There is no
API for a transformation to consume a region owned by a different rank, and
none is planned; the graph is defined once but is inherently a per-rank
program, identical to how PDC TF works today.

## 8. Error semantics

- `PDCan_attach_to_region` on a state name not present in the graph: rejected.
- Read of an output state that is unmaterialized, but one of its required
  *input* states was never attached or never written: fails — this is the
  "truly cannot be produced" case.
- Otherwise, reading an unmaterialized output always transparently
  computes it; there is no "expensive output, compute only when explicitly
  asked" escape hatch (see §9 — this was considered and dropped in favor
  of matching PDC TF's fully transparent read behavior).

## 9. Explicitly out of scope for this design (follow-up work)

- Staleness/invalidation: if an input object is rewritten after a
  persistent output was materialized, the output is **not** automatically
  invalidated or recomputed. No versioning is tracked, and there is
  currently no client call to force a recompute of an already-materialized
  output.
- Output object type/dimensionality inference: the client is responsible
  for creating output objects with a type/shape the transformation
  actually produces; the graph does not declare or validate this.
- GPU device selection/scheduling across multi-input nodes (PDC TF's
  polynomial-regression scheduler exists for single-edge CPU/GPU choices
  today; whether/how it generalizes to fan-in nodes is unresolved).
- Cross-rank reductions (e.g., a global sum across all ranks' regions) —
  explicitly not supported; every transformation is rank-local by design.
- An explicit "compute this expensive output only when asked" control was
  considered (an `on_demand` flag plus a `PDCan_request_create`/
  `start`/`wait` API) and deliberately dropped in favor of full
  transparency, matching PDC TF. If a real need for it resurfaces, it
  would need a new client entry point and a new RPC (removed; see git
  history around this point for the shape it had).

## 10. Where this lives in the codebase (implementation pointers)

- Client API: `src/api/pdc_an/` — `pdc_an.h`/`pdc_an.c`, mirrors `src/api/pdc_tf/`.
- Client-server RPC: `an_attach_region_in_t` + `hg_proc_an_attach_region_in_t`
  in `src/server/include/pdc_client_server_common.h`; server callback in
  `src/server/pdc_client_server_common.c`; client-issuing function
  `PDC_Client_an_attach_region` in `src/api/pdc_client_connect.c`.
- Server graph/registry/executor: `src/server/analysis/` — `pdc_an_common.{c,h}`
  (JSON parsing into a bipartite `pdc_dg_t`, function vertices namespaced
  `fn:<name>`), `pdc_an_server.{c,h}` (registries, `PDCan_store_attach_mapping`,
  `PDCan_exec_graph`'s backward-slice + topological execution, checkpoint/restart).
- Read-path hook: `PDC_Server_data_io_region_analysis`, checked inside
  `PDC_Server_transfer_request_io()` in
  `src/server/pdc_server_region/pdc_server_region_transfer.c`, beside the
  existing `PDC_Server_data_io_region_per_file_transformations` check.
- Checkpoint/restart: `PDCan_checkpoint`/`PDCan_restart_init`, called from
  `PDC_Server_checkpoint()`/`PDC_Server_restart()` in `src/server/pdc_server.c`.
