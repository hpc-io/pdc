PDC Region Analysis
===================

Region analysis (PDC AN, the ``PDCan_*`` API) lets a client declare derived
data -- "``magnitude`` is ``sqrt(vx² + vy² + vz²)``", "``vorticity`` is the curl
of ``(u, v, w)``" -- as a JSON graph, bind each piece of that graph to a PDC
object region, and then simply **read the derived object**. The PDC server
computes the result next to the data, writes it to storage like any other
object, and serves it back; the client never moves the inputs back over the
network to compute it.

Region analysis is the multi-object generalization of :doc:`pdc_tf_docs`:
a PDC TF graph moves *one* region of *one* object between representations,
while a PDC AN graph combines regions of *several* objects into regions of
*new* objects. Both share the same concepts and run side by side; they can
also be combined (:ref:`an_with_tf`).

See :doc:`data_flyway` for building, optional dependencies and testing, and
``docs/design/region_analysis.md`` in the source tree for design rationale.

.. contents:: On this page
   :local:
   :depth: 2


Concepts
--------

**State**
   A named node in the graph that stands for one buffer of data. A state that
   no transformation produces is an *input* (a leaf). A state that a
   transformation produces is an *output*. A state can be both: the output of
   one transformation and the input of another (an intermediate state).

**Transformation**
   A function with *N* named input states and *M* named output states, a
   ``device`` (``CPU``/``GPU``) and a ``location`` (``builtin``/``external``).
   All of its outputs are produced together by one call.

**Persistence** (per state: ``transient`` | ``persistent``)
   ``persistent`` states are backed by an object region that the client
   attaches; their data is written to storage and survives across executions.
   ``transient`` states exist only in server memory for the duration of one
   execution -- they are never written, cannot be attached, and are recomputed
   every time something needs them. Use ``transient`` for intermediate results
   you never want to read back.

**Trigger** (per *output* state: ``eager`` | ``lazy``, default ``eager``)
   ``eager``: the transformation runs as soon as the last of its inputs is
   written, inside that write's RPC, and the result is persisted.
   ``lazy``: nothing runs on write; the transformation runs the first time the
   output is read. Either way, every later read is served straight from
   storage.

**Materialization**
   The server tracks, per attached persistent region, whether its data is on
   storage. Reading an output that is not yet materialized *always*
   transparently computes it (and only the minimal upstream subgraph needed),
   regardless of trigger. Already-materialized upstream states are reused, not
   recomputed.


Getting started
---------------

The complete sequence, using the built-in ``vector_magnitude`` transformation
and the graph ``an_client/graphs/vector_magnitude.json``:

.. code-block:: json

   {
     "name": "vector_magnitude",
     "states": [
       { "name": "vx", "persistence": "persistent" },
       { "name": "vy", "persistence": "persistent" },
       { "name": "vz", "persistence": "persistent" },
       { "name": "magnitude", "persistence": "persistent", "trigger": "eager" }
     ],
     "transformations": [
       {
         "name": "vector_magnitude",
         "device": "CPU",
         "location": "builtin",
         "inputs": [ "vx", "vy", "vz" ],
         "outputs": [ "magnitude" ]
       }
     ]
   }

.. code-block:: c

   #include "pdc.h"

   /* Assumes pdc, cont, and two object properties already exist:
    *   prop_in  -- PDC_FLOAT,  1 dim, PDC_REGION_STATIC
    *   prop_out -- PDC_DOUBLE, 1 dim, PDC_REGION_STATIC (vector_magnitude
    *               always produces doubles) */
   pdcid_t vx  = PDCobj_create(cont, "vx", prop_in);
   pdcid_t vy  = PDCobj_create(cont, "vy", prop_in);
   pdcid_t vz  = PDCobj_create(cont, "vz", prop_in);
   pdcid_t mag = PDCobj_create(cont, "magnitude", prop_out);

   uint64_t off[1] = {0}, len[1] = {N};
   pdcid_t  reg        = PDCregion_create(1, off, len);  /* local buffer region  */
   pdcid_t  reg_global = PDCregion_create(1, off, len);  /* region in the object */

   /* 1. Load the graph (path must be readable by client AND server). */
   pdcid_t dg = PDCan_dg_json_create("/shared/graphs/vector_magnitude.json");
   if (dg == 0)
       /* bad JSON, unknown state, or a built-in not compiled into this build */;

   /* 2. Bind every persistent state to an object region. Order does not matter. */
   PDCan_attach_to_region(dg, "vx", vx, reg_global);
   PDCan_attach_to_region(dg, "vy", vy, reg_global);
   PDCan_attach_to_region(dg, "vz", vz, reg_global);
   PDCan_attach_to_region(dg, "magnitude", mag, reg_global);

   /* 3. Write the inputs with ordinary region transfers. "magnitude" is
    *    eager, so the write of the last input (vz) computes and stores it. */
   write_region(vx, reg, reg_global, vx_data);   /* create/start/wait/close */
   write_region(vy, reg, reg_global, vy_data);
   write_region(vz, reg, reg_global, vz_data);

   /* 4. Read the output like any other object. */
   double *out = malloc(N * sizeof(double));
   pdcid_t t = PDCregion_transfer_create(out, PDC_READ, mag, reg, reg_global);
   PDCregion_transfer_start(t);
   PDCregion_transfer_wait(t);
   PDCregion_transfer_close(t);

   /* 5. Release the client-side handle. Server-side bindings are unaffected. */
   PDCan_close_dg(dg);

``write_region`` above stands for the usual
``PDCregion_transfer_create(buf, PDC_WRITE, obj, reg, reg_global)`` /
``start`` / ``wait`` / ``close`` sequence. The full, runnable version of this
program is ``src/tests/analysis/vector_magnitude_test.c``.

There is deliberately **no "run" call and no special read call**. Whether a
read of ``magnitude`` is a plain storage read or first triggers computation is
decided by the server from materialization state alone.


Writing an analysis graph
-------------------------

Top-level fields:

.. list-table::
   :header-rows: 1
   :widths: 20 12 68

   * - Field
     - Required
     - Description
   * - ``name``
     - yes
     - Graph name (informational, used in logs).
   * - ``lib_path``
     - if any transformation is ``external``
     - Shared library holding the external transformation symbols. Must be
       loadable at the same path on the client and the server.
   * - ``states``
     - yes
     - Array of state objects (below).
   * - ``transformations``
     - yes
     - Array of transformation objects (below).

State fields:

.. list-table::
   :header-rows: 1
   :widths: 20 12 68

   * - Field
     - Required
     - Description
   * - ``name``
     - yes
     - Unique state name; this is what ``PDCan_attach_to_region`` refers to.
   * - ``persistence``
     - yes
     - ``"persistent"`` or ``"transient"``.
   * - ``trigger``
     - no
     - ``"eager"`` (default) or ``"lazy"``. Only meaningful on output states.

Transformation fields:

.. list-table::
   :header-rows: 1
   :widths: 20 12 68

   * - Field
     - Required
     - Description
   * - ``name``
     - yes
     - Built-in name, or the exported symbol name in ``lib_path``.
   * - ``location``
     - yes
     - ``"builtin"`` or ``"external"``.
   * - ``device``
     - no (built-in), yes (external)
     - ``"CPU"`` or ``"GPU"``. **Omit it on a built-in** to register every
       device variant compiled into this build and let the server pick one per
       execution (see :ref:`an_devices`).
   * - ``inputs``
     - yes
     - Non-empty array of state names, in the order the function receives them.
   * - ``outputs``
     - yes
     - Non-empty array of state names, in the order the function fills them.
   * - ``params``
     - no
     - Opaque string passed to the function as ``params_str``.

Validation performed when the graph is loaded (client and server):

- every name in ``inputs``/``outputs`` must be declared in ``states``;
- a state may be produced by **at most one** transformation;
- ``persistence``, ``trigger``, ``device`` and ``location`` must be valid
  values;
- every transformation must resolve to a function in this build (built-in
  compiled in, or symbol found in ``lib_path``).

A failure in any of these makes ``PDCan_dg_json_create`` return ``0`` and logs
the reason.

Chaining transformations
^^^^^^^^^^^^^^^^^^^^^^^^

Outputs can feed further transformations. ``an_client/graphs/curl_vorticity_magnitude.json``
computes the curl of a 3-D velocity field and then the magnitude of that curl:

.. code-block:: json

   {
     "name": "curl_vorticity_magnitude",
     "states": [
       { "name": "u", "persistence": "persistent" },
       { "name": "v", "persistence": "persistent" },
       { "name": "w", "persistence": "persistent" },
       { "name": "curl_x", "persistence": "persistent", "trigger": "eager" },
       { "name": "curl_y", "persistence": "persistent", "trigger": "eager" },
       { "name": "curl_z", "persistence": "persistent", "trigger": "eager" },
       { "name": "vorticity_magnitude", "persistence": "persistent", "trigger": "eager" }
     ],
     "transformations": [
       { "name": "curl", "device": "CPU", "location": "builtin",
         "inputs": [ "u", "v", "w" ], "outputs": [ "curl_x", "curl_y", "curl_z" ] },
       { "name": "vector_magnitude", "device": "CPU", "location": "builtin",
         "inputs": [ "curl_x", "curl_y", "curl_z" ], "outputs": [ "vorticity_magnitude" ] }
     ]
   }

Here every state is persistent, so the client must attach seven object
regions, and the write of ``w`` cascades through both transformations in one
pass. If you only care about the final magnitude, declare ``curl_x``,
``curl_y`` and ``curl_z`` ``transient`` (as in
``curl_vorticity_magnitude_transient.json``) and attach only ``u``, ``v``,
``w`` and ``vorticity_magnitude`` -- the curl components then exist only in
server memory while the magnitude is being computed.

.. note::

   A transformation with any **transient** input can never be triggered by a
   write (a transient state has no region whose write could be observed), so it
   only runs when one of its downstream persistent outputs is read, whatever
   its ``trigger`` says.


Client API reference
--------------------

All functions are declared in ``pdc_an.h`` (included by ``pdc.h``).
``PDCinit`` initializes the framework; there is no separate init call.

.. c:function:: pdcid_t PDCan_dg_json_create(char *json_filepath)

   Parse and validate an analysis graph and return a client-local handle.

   :param json_filepath: Path to the graph JSON. The server later re-opens this
      same path, so it must be readable there too.
   :returns: Graph handle, or ``0`` on failure (unreadable file, invalid JSON,
      schema violation, or a function not available in this build).

.. c:function:: perr_t PDCan_attach_to_region(pdcid_t dg_id, char *state_name, pdcid_t obj_id, pdcid_t region)

   Bind graph state ``state_name`` to ``region`` of object ``obj_id`` for the
   calling rank. Whether the binding is an input (data source) or an output
   (data sink, and recognized as an analysis output on read) is inferred from
   the graph.

   The call is client-local: it validates the state name and records the
   binding, and the binding is delivered to the server piggybacked on the next
   region transfer that touches a matching object region (no extra RPC).

   :param dg_id: Handle from :c:func:`PDCan_dg_json_create`.
   :param state_name: A state declared in the graph. Attaching a ``transient``
      state is meaningless; attach only persistent ones.
   :param obj_id: Object backing the state. Its type must already be set.
   :param region: The region *in the object* (the ``reg_global`` you pass to
      ``PDCregion_transfer_create``) that backs this state for this rank.
   :returns: ``SUCCEED``, or ``FAIL`` if the handle, object or region is
      invalid or ``state_name`` is not a state of this graph.

.. c:function:: perr_t PDCan_close_dg(pdcid_t dg_id)

   Free the client-local graph handle. Bindings already delivered to the
   server, and data already materialized, are unaffected.

.. c:function:: pdc_dg_t *PDCan_get_dg(pdcid_t dg_id)

   Return the parsed in-memory graph behind a handle, for debugging or
   introspection. Returns ``NULL`` for an unknown handle.


Execution semantics
-------------------

What a write does
^^^^^^^^^^^^^^^^^

Writing a region bound to an **input** state is an ordinary write. Afterwards
the server marks that input materialized and checks every transformation that
consumes it. A transformation runs immediately, inside the same write RPC,
when

- all of its inputs are persistent, bound (for the same region, see
  :ref:`an_shapes`), and materialized, **and**
- *all* of its outputs are declared ``eager`` (one ``lazy`` output defers the
  whole transformation to the read path).

Its outputs are written to their bound regions, marked materialized, and the
check repeats for downstream transformations, so a chain of eager outputs
cascades in one pass. Because this happens before the write returns, an eager
graph makes the *last* input write slower and every subsequent read cheap.

What a read does
^^^^^^^^^^^^^^^^

Reading a region bound to an **output** state:

- materialized → served from storage, no computation;
- not materialized → the server walks backwards from that state, computes the
  minimal set of transformations needed (reusing materialized upstream
  states, recomputing transient ones), persists every persistent output it
  produces, and then serves the read.

Reading a region bound to an input state is an ordinary read.

Choosing ``eager`` vs ``lazy``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- ``eager`` -- results should be ready by the time anyone looks (in-situ
  analysis during a simulation), and the write path can absorb the cost.
- ``lazy`` -- results may never be read, or are read much later (post-hoc
  analysis); keeps the write path as fast as plain I/O.

The trigger is a property of the graph, not of call order: attaching every
state before writing is fine for both. The one timing-dependent case is
attaching an ``eager`` output **after** its inputs are already durable (e.g.
a later job analysing data a previous job wrote): the write that would have
triggered it already happened, so it is computed on first read instead.

.. _an_shapes:

Regions, shapes and types
^^^^^^^^^^^^^^^^^^^^^^^^^

- **Bind every state of one execution to the same region.** The server
  matches the inputs and outputs that belong together by region: the
  ``ndim``, ``offset`` and ``size`` of the region passed to
  ``PDCan_attach_to_region``. For a given rank, attach each state (each in its
  own object) with the same global region, exactly as the quick start does
  with ``reg_global``, and read the output back with that same region.
  Reading a different or partial region of an output object is an ordinary
  read and does not trigger analysis.
- The analysis framework does not create objects. Create every bound object
  yourself, with the element type and dimensionality the transformation
  actually produces -- the graph does not declare or check this.
- Each bound region is passed to the function as a ``pdc_tf_region_t``
  (``ndim``, ``pdc_var_type``, ``size[]``) together with its buffer.
- The built-ins check that their inputs agree in shape and type and fail
  otherwise.

Multiple ranks
^^^^^^^^^^^^^^

Every call is scoped to the calling rank's own regions. The usual pattern is
for every rank to load the same graph and attach its *own* local region of
each shared object. Each rank's execution only ever touches that rank's
regions: there is no cross-rank input, halo exchange, or global reduction.
For a stencil like ``curl``, each rank's block is differentiated
independently (block-local boundaries).

Persistence across restarts
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Attached bindings (graph path, state, object, region, element type) and their
materialization flags are saved with the server checkpoint and restored on
restart, so outputs computed
before a restart are not recomputed after it.


.. _an_with_tf:

Combining analysis with transformations
---------------------------------------

Any bound object region can also have a PDC TF graph attached, for example to
store the analysis output compressed:

.. code-block:: c

   /* Attach the TF graph FIRST: PDCan_attach_to_region records any TF graph
    * already attached to the same object region and forwards it with the
    * analysis binding. */
   pdcid_t tf_dg = PDCtf_dg_json_create("/shared/graphs/zfp.json");
   PDCtf_attach_to_region(tf_dg, mag, reg_global, "decompressed", "compressed");

   pdcid_t an_dg = PDCan_dg_json_create("/shared/graphs/vector_magnitude.json");
   PDCan_attach_to_region(an_dg, "vx", vx, reg_global);
   PDCan_attach_to_region(an_dg, "vy", vy, reg_global);
   PDCan_attach_to_region(an_dg, "vz", vz, reg_global);
   PDCan_attach_to_region(an_dg, "magnitude", mag, reg_global);

When the server materializes ``magnitude`` it is compressed on its way to
storage, and a client read decompresses it transparently. Runnable example: ``src/tests/analysis/vector_magnitude_compressed_test.c``.

.. warning::

   Call ``PDCtf_attach_*`` **before** ``PDCan_attach_to_region`` for the same
   object region. A TF graph attached afterwards is not seen by the analysis
   binding, and the analysis output would be stored uncompressed.


.. _an_devices:

CPU/GPU variants
----------------

A built-in can be registered for several devices (``vector_magnitude`` has a
GPU variant in CUDA builds). If a transformation in the graph names a
``device``, exactly that variant is used. If it **omits** ``device``, every
compiled-in variant is attached and the server chooses one per execution from
each variant's rolling average execution time and current CPU/GPU
utilization (example: ``an_client/graphs/vector_magnitude_dynamic.json``).
Each decision is logged as a ``SCHED:`` line. Set
``PDC_AN_FORCE_DEVICE=CPU`` or ``GPU`` on the server to pin the choice.


Built-in analysis functions
---------------------------

``vector_magnitude``
   Inputs: one or more regions of identical shape and identical element type
   (``PDC_FLOAT`` or ``PDC_DOUBLE``). Output: one ``PDC_DOUBLE`` region of the
   same shape holding ``sqrt(Σ input_i²)`` element-wise. With two inputs this
   is a 2-D magnitude (``vector_magnitude_2d.json``). Devices: CPU; GPU with
   CUDA.

``curl``
   Inputs: exactly three ``PDC_FLOAT`` 3-D regions ``u``, ``v``, ``w`` of
   identical shape (this rank's ``nx × ny × nz`` block). Outputs: exactly
   three ``PDC_DOUBLE`` regions ``curl_x``, ``curl_y``, ``curl_z`` of the same
   shape, computed with finite differences over the local block. Device: CPU.


Writing your own analysis function
----------------------------------

External analysis functions are loaded with ``dlopen``/``dlsym`` from the
graph's ``lib_path``; the symbol name is the transformation's ``name``.
Include only ``pdc_an_user.h`` (installed with PDC) and match ``a_func_t``:

.. code-block:: c

   #include <stdlib.h>
   #include "pdc_an_user.h"

   /* Element-wise sum of N float inputs into one float output. */
   bool
   sum_fields(pdc_tf_internal_param *internal_param, char *params_str,
              void **input_bufs, pdc_tf_region_t *input_regions, int num_inputs,
              void **output_bufs, pdc_tf_region_t *output_regions, int num_outputs)
   {
       (void)internal_param;
       (void)params_str;
       if (num_inputs < 1 || num_outputs != 1)
           return false;

       size_t n = 1;
       for (size_t d = 0; d < input_regions[0].ndim; d++)
           n *= input_regions[0].size[d];

       float *out = malloc(n * sizeof(float));   /* PDC frees it */
       if (out == NULL)
           return false;
       for (size_t i = 0; i < n; i++) {
           out[i] = 0.0f;
           for (int k = 0; k < num_inputs; k++)
               out[i] += ((float *)input_bufs[k])[i];
       }

       output_bufs[0]    = out;
       output_regions[0] = input_regions[0];     /* same shape and type */
       return true;
   }

.. code-block:: bash

   gcc -shared -fPIC -I$PDC_DIR/include -o libmyanalysis.so sum_fields.c

.. code-block:: json

   {
     "name": "sum",
     "lib_path": "/shared/lib/libmyanalysis.so",
     "states": [
       { "name": "a",     "persistence": "persistent" },
       { "name": "b",     "persistence": "persistent" },
       { "name": "total", "persistence": "persistent", "trigger": "lazy" }
     ],
     "transformations": [
       { "name": "sum_fields", "device": "CPU", "location": "external",
         "inputs": [ "a", "b" ], "outputs": [ "total" ] }
     ]
   }

Function contract:

- ``input_bufs[i]`` / ``input_regions[i]`` (``i < num_inputs``) follow the
  order of ``inputs`` in the JSON. Treat inputs as read-only; do not free
  them.
- For each ``i < num_outputs`` (order of ``outputs``), set ``output_bufs[i]``
  to a **heap-allocated** buffer, which PDC takes ownership of and frees, and
  fill ``output_regions[i]`` with its ``ndim``, ``pdc_var_type`` and ``size``.
- ``params_str`` is the transformation's ``params`` string, or ``NULL``.
- Return ``true`` on success, ``false`` on failure (the read or write that
  triggered the execution then fails).
- External transformations must declare ``device`` explicitly.


Limitations
---------------

- **No invalidation.** Rewriting an input after an output was materialized
  does not recompute or invalidate the output, and there is currently no call
  to force recomputation.
- **Rank-local only.** No cross-rank inputs or reductions.
- **No output type inference.** The client must create output objects with
  the right type and shape.
- **Missing inputs.** Reading an unmaterialized output whose required inputs
  were never bound or never written fails.
