PDC Transformations
===================

The PDC Transformation Framework (PDC TF, the ``PDCtf_*`` API) lets a client
attach a pipeline of functions -- compression, encryption, or your own code --
to a region or an entire object. The PDC server then runs the pipeline
**transparently inside ordinary region transfers**: data is transformed on its
way to storage when written and inverted on its way back when read, so the
client always reads back the representation it wrote.

Pipelines are described as **directed graphs** in JSON: the nodes are named
**states** (e.g. ``"decompressed"``, ``"compressed"``, ``"encrypted"``) and the
edges are **functions** that convert data from one state to another. Edges run
on a CPU or a GPU, and when both are available the server picks one at runtime.

See :doc:`data_flyway` for building PDC with the optional compression and
encryption libraries and for running the transformation tests, and
:doc:`pdc_an_docs` for deriving *new* objects from existing ones.

.. contents:: On this page
   :local:
   :depth: 1


.. _tf_quick_start:

Quick Start
===========

Compress one region of an integer object with ZFP (requires a build with
``ZFP_ROOT_DIR`` set). The graph, ``tf_client/graphs/zfp.json``:

.. code-block:: json

   {
     "name": "zfp",
     "states": [
       { "name": "decompressed" },
       { "name": "compressed" }
     ],
     "functions": [
       { "name": "zfp_compress",   "device": "CPU", "location": "builtin",
         "input_state": "decompressed", "output_state": "compressed" },
       { "name": "zfp_decompress", "device": "CPU", "location": "builtin",
         "input_state": "compressed",   "output_state": "decompressed" }
     ]
   }

The client:

.. code-block:: c

   #include "pdc.h"

   /* obj was created from a property with its type set (here PDC_INT),
    * e.g. PDCprop_set_obj_type(prop, PDC_INT); the type must be set
    * before a graph can be attached. */
   uint64_t off[1] = {0}, len[1] = {N};
   pdcid_t  reg        = PDCregion_create(1, off, len);   /* local buffer region  */
   pdcid_t  reg_global = PDCregion_create(1, off, len);   /* region in the object */

   /* 1. Load the graph. Returns 0 if the JSON is invalid or a function is not
    *    available in this build (e.g. PDC built without ZFP). */
   pdcid_t dg = PDCtf_dg_json_create("/shared/graphs/zfp.json");

   /* 2. Data in this region is "decompressed" in client memory and must be
    *    stored "compressed". */
   PDCtf_attach_to_region(dg, obj, reg_global, "decompressed", "compressed");

   /* 3. Write as usual. The server runs zfp_compress before storing. */
   pdcid_t t = PDCregion_transfer_create(data, PDC_WRITE, obj, reg, reg_global);
   PDCregion_transfer_start(t);
   PDCregion_transfer_wait(t);
   PDCregion_transfer_close(t);

   /* 4. Read the SAME region as usual. The server runs zfp_decompress and
    *    returns the original integers. */
   t = PDCregion_transfer_create(data_read, PDC_READ, obj, reg, reg_global);
   PDCregion_transfer_start(t);
   PDCregion_transfer_wait(t);
   PDCregion_transfer_close(t);

   /* 5. Release the client-side handle. */
   PDCtf_close_dg(dg);

The full, runnable program is ``src/tests/transformation/compression_transform.c``.


.. _tf_client_guide:

Client Guide
============

Choosing ``client_state`` and ``store_state``
---------------------------------------------

Every attach names two states of the graph:

- ``client_state`` -- the representation in the client's buffers. Writes start
  here and reads end here.
- ``store_state`` -- the representation kept on storage.

On write the server follows the **shortest path** from ``client_state`` to
``store_state`` and runs each edge in order; on read it follows the shortest
path back. So a graph must contain a path in *both* directions, typically a
forward and an inverse function for each step. One graph can serve several
pipelines: with ``tf_client/graphs/zfp_libsod.json``

.. code-block:: text

   decompressed --zfp_compress--> compressed --secret_box_encrypt--> encrypted
   decompressed <-zfp_decompress- compressed <-secret_box_decrypt--- encrypted

attaching ``("decompressed", "compressed")`` only compresses, while
``("decompressed", "encrypted")`` compresses and then encrypts (and on read
decrypts, then decompresses). See
``src/tests/transformation/compression_encryption_transform.c``.

.. note::

   State names are **not** checked when you attach. A name that is not a state
   of the graph, or a pair of states with no path between them, is only
   discovered by the server when it tries to run the pipeline (logged as
   ``No path to desired state``). Double-check names against the JSON and
   check the return value of every transfer call.

Attaching to a region, an object, or many objects
-------------------------------------------------

``PDCtf_attach_to_region(dg, obj, reg_global, client_state, store_state)``
   Only transfers whose object region matches ``reg_global`` **exactly**
   (same ``ndim``, ``offset``, ``size`` and element type) go through the graph.
   Different regions of the same object can use different store states of the
   same graph; a region can have only one graph attached (a second attach to
   the same region returns ``FAIL``).

``PDCtf_attach_to_obj(dg, obj, client_state, store_state)``
   Every region written to ``obj`` goes through the graph. Each newly written
   region is registered with the server on its first write.
   See ``src/tests/transformation/attach_obj_compression_transform.c``.

``PDCtf_attach_to_objs(dg, obj_ids, n, client_state, store_state)``
   Shorthand for ``PDCtf_attach_to_obj`` on each of ``n`` objects.

An object can use only one graph *file* on the server, though different
regions can use different states of it.

Writes, reads, and other processes
----------------------------------

- **Attach before the first write.** The attachment is delivered to the server
  piggybacked on the next write RPC of a matching region; nothing is sent by
  the attach call itself. Data written before attaching is stored as-is.
- **Read exactly the region you wrote.** A transformed region is stored as one
  opaque blob (e.g. a compressed byte stream), so it can only be read back as
  a whole: use the same ``reg_global`` (offset and size) for the read. A read
  of a different or partial region is an ordinary read of the stored bytes.
- **Readers do not need to attach.** The server remembers, per object region,
  which graph and which states were used, and reads are inverted
  automatically -- including from a different process, a later job, or after a
  server restart (the mapping and every function's per-region parameters are
  saved in the server checkpoint).
- **Many regions, many ranks.** Each rank attaches and writes its own
  regions; regions are transformed independently (per-region parameters such
  as the original shape before compression are kept separately).

Element types
-------------

The object's element type (``PDCprop_set_obj_type``) must be set before
attaching, and must be one the graph's functions support -- see each built-in
below. In short: ZFP handles ``PDC_FLOAT``/``PDC_DOUBLE``/``PDC_INT``/
``PDC_INT64``; SZ3 only ``PDC_FLOAT``/``PDC_DOUBLE``; TurboPFor only
``PDC_INT``/``PDC_INT32``/``PDC_INT64``; SecretBox any type (it operates on
bytes). A function that rejects a type fails the transfer.

CPU and GPU edges
-----------------

Listing two functions with the same name, endpoints and different ``device``
creates parallel edges, and the server chooses one per transfer
(:ref:`tf_scheduler`). ``tf_client/graphs/zfp_gpu_or_cpu.json`` does this for
ZFP. GPU edges only exist in builds with ``CUDA_ROOT_DIR``; a graph that names
a GPU edge fails to load otherwise.

Troubleshooting
---------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Symptom
     - Likely cause
   * - ``PDCtf_dg_json_create`` returns ``0``, log says
       ``Failed to link to builtin function``
     - The built-in was not compiled in. Reconfigure with the matching
       ``*_ROOT_DIR``/``*_DIR`` variable (:ref:`flyway_build`).
   * - ``PDCtf_attach_*`` fails with
       ``Data type must be set on object before attaching transformations``
     - Call ``PDCprop_set_obj_type`` on the object property before creating
       the object.
   * - Read returns wrong data
     - The read region differs from the attached/written region.
   * - Server log shows ``No path to desired state``
     - ``client_state``/``store_state`` are misspelled or not connected in
       both directions in the graph.
   * - Server cannot open the graph
     - The JSON (or ``lib_path``) path is not visible on the server node. Use
       an absolute path on a shared file system (:ref:`flyway_graph_paths`).
   * - Lossy results
     - ``sz_compress`` (error bound 0.01) and GPU ``zfp_compress``
       (fixed-rate) are lossy; CPU ``zfp_compress`` and ``turbo_compress``
       are lossless.


-------------------------------------------------------------------------------

.. _tf_architecture:

Architecture Overview
=====================

.. code-block:: text

   Client Write                          Server Store
   ─────────────────────────────────────────────────────────────────
     [raw data]                            [transformed data]
         │                                        ▲
         │  client_state = "raw"                  │  store_state = "compressed"
         ▼                                        │
     PDCtf_attach_to_region/obj          PDCtf_exec_graph
         │                                        │
         └──────── Directed Graph ────────────────┘
                   (JSON file)
                     states: raw ──zfp_compress──▶ compressed
                                 ──sz_compress───▶ compressed

On **write**: data arrives in ``client_state``, the graph is traversed to reach ``store_state``,
and the transformed data is written to disk.

On **read**: data is read from disk in ``store_state``, the graph is traversed in reverse to
reach ``client_state``, and the raw data is returned to the client.

Data Flow
---------

Each edge in the graph is a transformation function. When the server needs to move data
from one state to another, it finds the shortest path through the directed graph and
executes each edge's function in sequence. If multiple edges exist between the same two
nodes (e.g. both a CPU and GPU compressor), the scheduler picks the best one at runtime.

The data buffer (``void **region_data``) and its shape descriptor (``pdc_tf_region_t``) are
updated in place as data flows through the pipeline. After compression, for example, the
region becomes a flat 1D byte array — subsequent functions in the chain see that new shape.


-------------------------------------------------------------------------------

.. _tf_core_concepts:

Core Concepts
=============

States
------

A state is a named node in the directed graph representing a particular data representation.
Examples: ``"raw"``, ``"compressed"``, ``"encrypted"``.

.. code-block:: c

   typedef struct pdc_tf_state_t {
       char       *name;                    // State name, e.g. "raw"
       PDC_VECTOR *pdc_tf_dg_params_vector; // Per-region parameter storage
   } pdc_tf_state_t;

Functions
---------

A function is a directed edge between two states. It wraps a C function pointer and carries
metadata about which device it runs on and where it is located (builtin vs. external shared library).

.. code-block:: c

   typedef struct pdc_tf_func_t {
       pdc_tf_dev_t      dev;        // PDC_TF_CPU_DEVICE or PDC_TF_GPU_DEVICE
       pdc_tf_location_t location;   // PDC_TF_BUILTIN or PDC_TF_EXTERNAL
       char             *name;       // e.g. "zfp_compress"
       char             *params_str; // Optional JSON params string from graph file
       c_func_t          c_func;    // The actual function pointer
       PDC_VECTOR *pdc_tf_dg_params_vector;     // Per-region output params storage
   } pdc_tf_func_t;

Regions
-------

A region describes the shape and element type of a data buffer as it passes through the pipeline.
Each transformation receives the input region descriptor and must populate the output region
descriptor if the shape or type changes.

.. code-block:: c

   typedef struct pdc_tf_region_t {
       size_t   ndim;           // Number of dimensions (1–4)
       uint8_t  pdc_var_type;   // PDC_FLOAT, PDC_DOUBLE, PDC_INT32, etc.
       uint64_t size[DIM_MAX];  // Per-dimension element counts
   } pdc_tf_region_t;

After compression the output region always becomes::

   { ndim=1, pdc_var_type=PDC_CHAR, size[0]=compressed_bytes }

Transform Function Signature
-----------------------------

Every builtin and user-defined external transform function must match this prototype:

.. code-block:: c

   typedef bool (*c_func_t)(
       pdc_tf_internal_param *internal_param, // Graph handle + flat offset + timing
       char                  *params_str,     // "params" string from the JSON, or NULL
       void                 **region_data,    // Pointer to data buffer (may be replaced)
       pdc_tf_region_t        input_region,   // Describes input data shape/type
       pdc_tf_region_t       *output_region   // Must be populated if shape/type changes
   );

**Contract:**

- ``*region_data`` may be replaced with a newly ``malloc``'d buffer.
  The old pointer must **not** be freed by the function — the framework handles that.
- ``output_region`` is pre-initialized to ``input_region``.
  Only update it if the transformation changes the shape or type.
- Return ``true`` on success, ``false`` on failure.

Per-Region Parameters
---------------------

Because multiple data regions may flow through the same graph concurrently, each function
and state node stores parameters keyed by a **flat conceptual offset** — a scalar that
uniquely identifies which region the parameters belong to.

.. code-block:: c

   typedef struct pdc_tf_dg_params_t {
       uint64_t flat_conceptual_offset; // Unique region key
       void    *params;                 // Pointer to parameter struct
       uint64_t params_size;            // Size of params in bytes
   } pdc_tf_dg_params_t;

Functions use the ``SET_FUNC_PARAMS`` / ``GET_FUNC_PARAMS`` macros to store and retrieve these.
For example, ``zfp_compress`` stores the original region descriptor so ``zfp_decompress`` knows
the decompressed shape when it is later called.


-------------------------------------------------------------------------------

.. _tf_json_format:

JSON Graph File Format
======================

Graphs are described in JSON and loaded at runtime via :c:func:`PDCtf_dg_json_create`.

Schema
------

.. code-block:: json

   {
     "name": "my_graph",
     "lib_path": "/optional/path/to/external.so",
     "states": [
       { "name": "raw" },
       { "name": "compressed" }
     ],
     "functions": [
       {
         "name": "zfp_compress",
         "device": "CPU",
         "location": "builtin",
         "input_state": "raw",
         "output_state": "compressed",
         "params": "{\"rate\": 8}"
       },
       {
         "name": "zfp_decompress",
         "device": "CPU",
         "location": "builtin",
         "input_state": "compressed",
         "output_state": "raw"
       }
     ]
   }

Fields
------

.. list-table::
   :header-rows: 1
   :widths: 20 10 70

   * - Field
     - Required
     - Description
   * - ``name``
     - Yes
     - Graph name (informational).
   * - ``lib_path``
     - No
     - Path to a shared library for external functions. Required if any function has ``"location": "external"``.
   * - ``states``
     - Yes
     - Array of named state nodes. Each entry requires a ``"name"`` field.
   * - ``functions``
     - Yes
     - Array of edges. See function fields below.

Function Fields
---------------

.. list-table::
   :header-rows: 1
   :widths: 20 10 70

   * - Field
     - Required
     - Description
   * - ``name``
     - Yes
     - Name of the function. Must match a registered builtin or an exported symbol in ``lib_path``.
   * - ``device``
     - Yes
     - ``"CPU"`` or ``"GPU"``.
   * - ``location``
     - Yes
     - ``"builtin"`` or ``"external"``.
   * - ``input_state``
     - Yes
     - Name of the source state node.
   * - ``output_state``
     - Yes
     - Name of the destination state node.
   * - ``params``
     - No
     - Optional string passed as ``params_str`` to the function at runtime.

Parallel Edges
--------------

Multiple functions between the same two states are fully supported. The scheduler will
select the best candidate at runtime based on device utilization and historical timing.
This allows you to register both a CPU and a GPU version of a compressor and let the
framework decide which to use:

.. code-block:: json

   { "name": "zfp_compress", "device": "CPU", "location": "builtin",
     "input_state": "decompressed", "output_state": "compressed" },
   { "name": "zfp_compress", "device": "GPU", "location": "builtin",
     "input_state": "decompressed", "output_state": "compressed" }

Built-ins are registered per ``(name, device)`` pair, so the CPU and GPU
variants share a name and differ only in ``device``. Remember to provide the
inverse edges (``zfp_decompress``) as well; ``tf_client/graphs/zfp_gpu_or_cpu.json``
is a complete example.


-------------------------------------------------------------------------------

.. _tf_compile_flags:

Compile-Time Feature Flags
==========================

Which built-ins exist is decided at configure time by the optional-dependency
environment variables described in :ref:`flyway_build`. Internally they map to
these preprocessor symbols (set in the top-level ``CMakeLists.txt`` and
``src/server/transform/include/pdc_tf_builtin_common.h``):

.. list-table::
   :header-rows: 1
   :widths: 26 26 48

   * - Environment variable
     - CMake definition
     - Derived TF macro / effect
   * - ``ZFP_ROOT_DIR``
     - ``ZFP_ENABLED``
     - ``ENABLE_TF_ZFP_COMPRESSION``: ZFP CPU (plus GPU with ``CUDA_ENABLED``)
   * - ``SZ_CPU_DIR``
     - ``SZ3_ENABLED``
     - ``ENABLE_TF_SZ_COMPRESSION``: SZ3 CPU
   * - ``SZ_CPU_DIR`` + ``CUDA_ROOT_DIR`` + ``CUSZ_GPU_DIR``
     - ``SZ3_ENABLED`` + ``CUDA_ENABLED``
     - ``ENABLE_TF_SZ_GPU_COMPRESSSION``: cuSZ GPU
   * - ``LIBSODIUM_ROOT_DIR``
     - ``LIBSODIUM_ENABLED``
     - ``ENABLE_TF_SECRET_BOX_ENCRYPTION``: SecretBox encrypt/decrypt
   * - ``TURBO_DIR``
     - ``TURBO_ENABLED``
     - ``ENABLE_TF_TURBO_COMPRESSION``: TurboPFor
   * - ``CUDA_ROOT_DIR``
     - ``CUDA_ENABLED``, ``USE_CUDA``
     - GPU variants, NVML profiling, polynomial GPU scheduler

The same CMake variables (``ZFP_ENABLED``, ``LIBSODIUM_ENABLED``,
``CUDA_ENABLED``) decide which transformation/analysis tests are registered
with CTest (:ref:`flyway_testing`).


-------------------------------------------------------------------------------

.. _tf_client_api:

Client API
==========

Declared in ``pdc_tf.h`` (included by ``pdc.h``). ``PDCinit`` initializes the
framework (it calls ``PDCtf_init`` internally); applications never need to call
``PDCtf_init`` themselves.

Graph Loading
-------------

.. c:function:: pdcid_t PDCtf_dg_json_create(char *json_filepath)

   Parse a JSON graph file (:ref:`tf_json_format`) and return a client-local
   handle. Every function is resolved immediately: built-ins against the
   registry, ``external`` functions by ``dlopen``-ing ``lib_path`` and
   ``dlsym``-ing the function name.

   :param json_filepath: Path to the graph. The server re-opens the same path
      later, so it must be readable there too.
   :returns: Graph handle, or ``0`` on failure (unreadable file, invalid JSON,
      bad ``device``/``location``, missing ``lib_path``, or a function that is
      not compiled into this build).

.. c:function:: perr_t PDCtf_close_dg(pdcid_t dg_id)

   Release the client-local graph handle and its state/function nodes.
   Attachments already delivered to the server, and data already written, are
   unaffected.

   :returns: ``SUCCEED``, or ``FAIL`` for an unknown handle.

.. c:function:: pdc_dg_t *PDCtf_get_dg(pdcid_t dg_id)

   Return the parsed in-memory graph behind a handle (debugging/introspection),
   or ``NULL``.

Attaching Graphs
----------------

All attach calls are client-local: they validate the object and region,
record the mapping, and the mapping is sent to the server piggybacked on the
next **write** of a matching region. Attach before writing.

.. c:function:: perr_t PDCtf_attach_to_region(pdcid_t dg_id, pdcid_t obj_id, pdcid_t remote_reg, char *client_state, char *store_state)

   Route transfers of exactly one object region through the graph. A transfer
   matches only if its object region has the same ``ndim``, ``offset``,
   ``size`` and element type as ``remote_reg``.

   :param dg_id: Handle from :c:func:`PDCtf_dg_json_create`.
   :param obj_id: Object to attach to. Its element type must be set.
   :param remote_reg: The region *in the object* -- the ``reg_global`` you pass
      to ``PDCregion_transfer_create`` -- not the local buffer region.
   :param client_state: State of the data in client memory (e.g. ``"decompressed"``).
   :param store_state: State of the data on storage (e.g. ``"compressed"``).
   :returns: ``SUCCEED``; ``FAIL`` if the object type is not set, the IDs are
      invalid, or this region already has a graph attached.

.. c:function:: perr_t PDCtf_attach_to_obj(pdcid_t dg_id, pdcid_t obj_id, char *client_state, char *store_state)

   Route every region written to ``obj_id`` through the graph. Each distinct
   region is registered with the server on its first write, and must later be
   read back with the same region.

   :returns: ``SUCCEED`` or ``FAIL`` (invalid IDs or object type not set).

.. c:function:: perr_t PDCtf_attach_to_objs(pdcid_t dg_id, pdcid_t *obj_ids, int num_ids, char *client_state, char *store_state)

   Call :c:func:`PDCtf_attach_to_obj` for each of the ``num_ids`` objects in
   ``obj_ids``; stops and returns ``FAIL`` at the first failure.

.. c:function:: perr_t PDCtf_set_output_mode(pdcid_t dg_id, pdc_tf_output_mode_t mode, pdcid_t *obj_ids, char **result_names, int num_ids)

   Reserved for future use: intended to choose between transforming an object
   in place (``PDC_TF_OVERWRITE``, the behavior today) and writing the result
   to a new object (``PDC_TF_CREATE``). **Currently a no-op** that always
   returns ``SUCCEED``. To derive new objects from existing ones, use
   :doc:`pdc_an_docs`.

Diagnostics
-----------

.. c:function:: void PDCtf_print_dg(pdcid_t dg_id, bool write_to_file)

   Prints the graph in Graphviz DOT format. CPU edges are blue, GPU edges are red.
   If ``write_to_file`` is ``true``, output goes to ``graph.txt``; otherwise stdout.

.. c:function:: void PDCtf_print_exec_path(pdcid_t dg_id, char *cur_state, char *desired_state)

   Prints the shortest path through the graph between two named states, showing
   which transformation functions would be executed in order.


-------------------------------------------------------------------------------

.. _tf_server_api:

Server API
==========

Declared in ``pdc_tf_server.h``. These functions are used internally by the PDC
data server and are not part of the client-facing API.

.. c:function:: perr_t PDCtf_store_json_mapping(pdcid_t obj_id, char *json_filepath, char *cur_state, char *client_state, char *store_state, uint64_t *offset, uint64_t *size, uint8_t ndim, pdc_var_type_t pdc_var_type)

   Called on the server when a client attaches a graph to a region. Loads the JSON graph,
   creates the in-memory directed graph if it does not yet exist for this object, and stores
   the conceptual region → actual region mapping.

   If the object already has a graph attached, the filepath is validated to match. If the
   same region offset is attached twice the existing mapping is overwritten.

.. c:function:: perr_t PDCtf_exec_graph(pdc_dg_t *dg, uint64_t flat_conceptual_offset, char *cur_state, char *desired_state, pdc_tf_region_t input_region, pdc_tf_region_t *output_region, void **input, int is_write)

   Runs the shortest path through the graph from ``cur_state`` to ``desired_state``,
   executing each edge's transformation function in sequence. When multiple parallel edges
   exist between the same two nodes, the scheduler selects the best one (see :ref:`tf_scheduler`).

   Updates ``*input`` and ``*output_region`` in place as data flows through the pipeline.
   Intermediate buffers that are replaced during the pipeline are freed automatically,
   except for the very first input buffer when ``is_write`` is true (which is owned by
   the caller).

   :param dg: The directed graph to traverse.
   :param flat_conceptual_offset: Scalar key identifying which region's parameters to use.
   :param cur_state: Starting state name.
   :param desired_state: Target state name.
   :param input_region: Shape/type of the input data.
   :param output_region: Populated with the shape/type of the final output.
   :param input: Double pointer to the data buffer; updated as transforms run.
   :param is_write: Non-zero if this is a write (client→server) operation.


-------------------------------------------------------------------------------

.. _tf_parameter_macros:

Parameter Macros
================

These macros are used **inside transformation functions** to persist state between
compression and decompression passes. For example, ``zfp_compress`` uses
``SET_FUNC_PARAMS`` to save the original region shape so that ``zfp_decompress``
can restore it later.

Each call is automatically scoped to the current region via the ``flat_conceptual_offset``
embedded in ``internal_param``, so concurrent regions flowing through the same graph
do not interfere with each other.

Function Parameter Macros
--------------------------

.. code-block:: c

   // Store parameters for a named function edge
   SET_FUNC_PARAMS("func_name", PDC_TF_CPU_DEVICE, params_ptr, sizeof(params_t));

   // Retrieve parameters previously stored for a named function edge
   GET_FUNC_PARAMS("func_name", PDC_TF_CPU_DEVICE, (void **)&params_ptr, &params_size);

State Parameter Macros
-----------------------

.. code-block:: c

   // Store parameters for a named state node
   SET_STATE_PARAMS("state_name", params_ptr, sizeof(params_t));

   // Retrieve parameters previously stored for a named state node
   GET_STATE_PARAMS("state_name", (void **)&params_ptr, &params_size);

Usage Pattern
-------------

The typical pattern for a compress/decompress pair is:

.. code-block:: c

   typedef struct { pdc_tf_region_t decompressed_region; } my_params_t;

   // In the compress function -- save the original region so decompress can restore it
   my_params_t *out_params        = malloc(sizeof(my_params_t));
   out_params->decompressed_region = input_region;
   SET_FUNC_PARAMS("my_compress", PDC_TF_CPU_DEVICE, out_params, sizeof(my_params_t));

   // In the decompress function -- retrieve the saved region
   my_params_t *in_params      = NULL;
   uint64_t     in_params_size = 0;
   GET_FUNC_PARAMS("my_compress", PDC_TF_CPU_DEVICE, (void **)&in_params, &in_params_size);
   if (in_params == NULL)
       return false;
   *output_region = in_params->decompressed_region;

Parameters are keyed by the name of the function that *stored* them (note
``"my_compress"`` in both calls) and by device. Saved parameters are part of
the server checkpoint, so a region written before a server restart can still
be decompressed after it.

.. note::

   ``SET_FUNC_PARAMS``/``GET_FUNC_PARAMS`` expect a variable named
   ``internal_param`` of type ``pdc_tf_internal_param *`` in scope, which is
   the first parameter of every ``c_func_t``. ``SET_STATE_PARAMS`` /
   ``GET_STATE_PARAMS`` currently dereference it with ``.`` instead of ``->``,
   so they do not compile inside a ``c_func_t``; call
   ``PDCtf_set_state_param``/``PDCtf_get_state_param`` directly if you need
   per-state parameters.


-------------------------------------------------------------------------------

.. _tf_scheduler:

Scheduler
=========

When the path from one state to the next has several parallel edges (e.g. a
CPU and a GPU ``zfp_compress``), :c:func:`PDCtf_exec_graph` picks one per
transfer. Graphs with a single edge per step are never affected.

Selection Algorithm
-------------------

1. **Forced GPU** -- if ``USE_GPU`` is set in the server's environment, or the
   server is in its close/flush phase (``close_time_g``), the GPU edge is used
   on GPU 0.

2. **Choose a GPU** (CUDA builds) -- the server picks which GPU a GPU edge would
   run on. With ``PDC_POLY_COEFF_FILE`` pointing at a coefficients file, a
   degree-3 polynomial model (``pdc_tf_poly_sched.c``) predicts the total
   host→device + compute + device→host time on each GPU from the data size and
   live NVML metrics (utilization, power, memory) and picks the lowest.
   Without it, the least-utilized GPU is used.

3. **Compare edges by expected time** -- for each candidate edge:

   - GPU edge with a polynomial prediction: the predicted time;
   - otherwise: ``avg_past_time / max(0.1, 1 - utilization)`` of that device,
     where ``avg_past_time`` is the rolling average of the edge's last
     ``NUM_TF_FUNC_TIMES = 5`` executions (an edge with no history is assumed
     to take 0.75 s, so each edge is tried early on).

   The edge with the lowest expected time runs.

The CPU utilization fed into this comes from the server's own event loop and
GPU utilization from NVML (:ref:`tf_profiler`).

Logging
-------

When running as server rank 0, the scheduler emits ``LOG_WARNING`` lines prefixed
with ``SCHED:`` describing utilization, candidate edge evaluation, and the final
selection. These can be used to diagnose scheduling behavior in production.


-------------------------------------------------------------------------------

.. _tf_profiler:

Profiler
========

The profiler maintains rolling buffers of recent GPU and CPU utilization samples
that feed into the scheduler's device selection logic.

Declared in ``pdc_tf_profiler.h``, implemented in ``pdc_tf_profiler.c``.

Rolling Buffer
--------------

All samples are stored in a fixed-size rolling buffer of length ``PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE = 3``.
Older samples are overwritten as new ones arrive. Averages are computed over all
non-null entries in the buffer.

GPU Profiling (NVML)
--------------------

Uses the NVIDIA Management Library (NVML) to sample per-device utilization.
On the first call, NVML is initialized and the device count is queried. Subsequent
calls populate a per-device sample array containing:

- ``gpu_utilization`` — GPU compute utilization as a fraction (0.0–1.0).
- ``memory_utilization`` — Memory controller utilization percentage.
- ``memory_total / memory_used / memory_free`` — Absolute memory figures in bytes.

CPU Profiling
-------------

CPU utilization is estimated from the server's own event loop timing:

.. code-block:: text

   cpu_utilization = 1.0 - (elapsed_progress_time / elapsed_total_time)

Where ``elapsed_progress_time`` is the time the server spent doing useful work
and ``elapsed_total_time`` is the total wall time of the loop iteration. This
approximates the fraction of time the CPU was busy.

API
---

.. c:function:: perr_t pdc_tf_update_profiler(double elapsed_total_time_sec, double elapsed_progress_time_sec)

   Call from the server event loop to push a new CPU sample and refresh GPU samples.
   Both the NVML and CPU profilers are updated in a single call.

.. c:function:: double pdc_tf_avg_gpu_utilization(unsigned int device_index)

   Returns the rolling average GPU utilization (0.0–1.0) for the given device index.
   Returns ``-1.0`` if the device index is out of range.

.. c:function:: double pdc_tf_avg_cpu_utilization()

   Returns the rolling average CPU utilization (0.0–1.0) across recent samples.


-------------------------------------------------------------------------------

.. _tf_builtin_registry:

Builtin Function Registry
==========================

The registry is a global ``PDC_VECTOR`` (``pdc_tf_builtin_funcs_vector_g``) of
``pdc_tf_builtin_func_t`` entries, each associating a ``(name, device)`` pair with
a function pointer.

.. code-block:: c

   typedef struct pdc_tf_builtin_func_t {
       char        *name;    // e.g. "zfp_compress"
       pdc_tf_dev_t dev;     // PDC_TF_CPU_DEVICE or PDC_TF_GPU_DEVICE
       c_func_t     c_func;  // Function pointer
   } pdc_tf_builtin_func_t;

Functions are unique by ``(name, device)`` pair. This allows the same logical
operation (e.g. ``"zfp_compress"``) to have both a CPU and a GPU implementation
registered simultaneously, enabling the scheduler to choose between them.

Lifecycle
---------

1. :c:func:`PDCtf_init` calls :c:func:`PDCtf_init_builtin_funcs` at startup.
2. :c:func:`PDCtf_init_builtin_funcs` calls :c:func:`PDCtf_add_builtin_func` for
   each enabled builtin, conditioned on compile-time feature flags.
3. During graph loading (:c:func:`PDCtf_dg_json_create_common`), each JSON function
   entry is bound to its registered function pointer via :c:func:`PDCtf_link_builtin_func`.

API
---

.. c:function:: perr_t PDCtf_add_builtin_func(char *func_name, c_func_t c_func, pdc_tf_dev_t dev)

   Registers a single function pointer in the global builtin registry.

.. c:function:: perr_t PDCtf_link_builtin_func(char *func_name, pdc_tf_dev_t dev, pdc_tf_func_t *f)

   Searches the registry for ``(func_name, dev)`` and sets ``f->c_func`` on success.


-------------------------------------------------------------------------------

.. _tf_builtin_zfp_cpu:

ZFP Compression — CPU
=====================

**Source file:** ``pdc_tf_builtin_zfp_cpu.c``

**Requires:** ``ZFP_ROOT_DIR`` (``ENABLE_TF_ZFP_COMPRESSION``)

ZFP is a floating-point and integer array compression library optimized for
multidimensional scientific data. The CPU implementation uses ZFP's
**reversible (lossless) mode**: decompressed data is bit-for-bit identical to
the input.

Supported Types
---------------

``PDC_FLOAT``, ``PDC_DOUBLE``, ``PDC_INT``/``PDC_INT32``, ``PDC_INT64``.
Supports 1D–4D arrays.

pdc_tf_builtin_zfp_compress
-----------------------------

- Allocates a ZFP field descriptor matching the input region shape and type.
- Opens a bitstream over a ``PDC_malloc``'d buffer sized by ``zfp_stream_maximum_size``.
- Calls ``zfp_compress`` and replaces ``*region_data`` with the compressed buffer.
- Sets output region to ``{ndim=1, pdc_var_type=PDC_CHAR, size[0]=compressed_bytes}``.
- Saves the original input region via ``SET_FUNC_PARAMS("zfp_compress", PDC_TF_CPU_DEVICE, ...)``.

pdc_tf_builtin_zfp_decompress
-------------------------------

- Retrieves the saved region descriptor via ``GET_FUNC_PARAMS``.
  Tries GPU params first, then CPU, to support cross-device round-trips.
- Reconstructs the ZFP stream from ``*region_data``.
- Decompresses into a newly ``malloc``'d buffer.
- Restores ``output_region`` to the original pre-compression shape.


-------------------------------------------------------------------------------

.. _tf_builtin_zfp_gpu:

ZFP Compression — GPU
=====================

**Source file:** ``pdc_tf_builtin_zfp_gpu.c``

**Requires:** ``ZFP_ROOT_DIR`` and ``CUDA_ROOT_DIR``, with ZFP built with CUDA
support (``ENABLE_TF_ZFP_COMPRESSION`` + ``CUDA_ENABLED``)

Same logical operation as :ref:`tf_builtin_zfp_cpu` but executes on the GPU using
``zfp_stream_set_execution(zfp, zfp_exec_cuda)``.

.. warning::

   ZFP's CUDA backend does not support reversible mode, so the GPU variant
   uses **fixed-rate** mode at ``8 * sizeof(element)`` bits per value. This is
   near-lossless but not guaranteed bit-exact. If a graph offers both CPU and
   GPU edges, the result's exactness depends on which one the scheduler
   picked.

pdc_tf_builtin_zfp_compress_cuda
----------------------------------

- Copies host input data to device (``cudaMemcpy`` H→D).
- Allocates a device output buffer sized by ``zfp_stream_maximum_size``.
- Sets ZFP CUDA execution mode and calls ``zfp_compress`` on the GPU.
- Synchronizes the device (``cudaDeviceSynchronize``).
- Copies compressed result back to host, frees device buffers.
- Saves params via ``SET_FUNC_PARAMS("zfp_compress", PDC_TF_GPU_DEVICE, ...)``.

pdc_tf_builtin_zfp_decompress_cuda
------------------------------------

- Retrieves saved params (tries GPU first, then CPU).
- Copies compressed data to device.
- Allocates device output buffer of the correct uncompressed size.
- Sets ZFP CUDA execution mode and calls ``zfp_decompress`` on the GPU.
- Validates that decompression returned a non-zero byte count.
- Copies decompressed result back to host.
- Frees all device buffers and ZFP/bitstream resources.

Error Handling
--------------

All CUDA calls are wrapped in ``CUDA_CHECK`` which prints the CUDA error string,
file, and line number, then calls ``exit(EXIT_FAILURE)`` on failure.


-------------------------------------------------------------------------------

.. _tf_builtin_sz_cpu:

SZ3 Compression — CPU
=====================

**Source file:** ``pdc_tf_builtin_sz_cpu.c``

**Requires:** ``SZ_CPU_DIR`` (``ENABLE_TF_SZ_COMPRESSION``)

SZ3 is an error-bounded lossy scientific data compressor that achieves high
compression ratios on floating-point arrays by exploiting value predictability.

Supported Types
---------------

``PDC_FLOAT`` and ``PDC_DOUBLE`` only. Integer types are rejected (the
transfer fails): SZ3 would reinterpret the integer bits as floating point
rather than converting them. Use ZFP or TurboPFor for integer data.

Error Bound
-----------

Uses ``ABS`` mode with ``absErrBound = 0.01``. Each reconstructed value is
guaranteed to differ from the original by no more than 0.01 in absolute terms.

pdc_tf_builtin_sz_compress
-----------------------------

- Extracts up to 5 dimension sizes from the input region (unused dimensions default to 1).
- Calls ``SZ_compress_args`` with the absolute error bound configuration.
- Replaces ``*region_data`` with the SZ3-allocated compressed buffer.
- Sets output region to ``{ndim=1, pdc_var_type=PDC_CHAR, size[0]=compressed_bytes}``.
- Saves original region via ``SET_FUNC_PARAMS("sz_compress", PDC_TF_CPU_DEVICE, ...)``.

pdc_tf_builtin_sz_decompress
------------------------------

- Retrieves the saved region descriptor.
- Calls ``SZ_decompress`` with the original dimension sizes.
- Restores ``output_region`` to the original pre-compression shape.


-------------------------------------------------------------------------------

.. _tf_builtin_sz_gpu:

SZ (cuSZ) Compression — GPU
============================

**Source file:** ``pdc_tf_builtin_sz_gpu.c``

**Requires:** ``SZ_CPU_DIR``, ``CUDA_ROOT_DIR`` and ``CUSZ_GPU_DIR``
(``ENABLE_TF_SZ_GPU_COMPRESSSION``)

cuSZ is the CUDA GPU port of the SZ compressor. It provides GPU-accelerated
error-bounded lossy compression for scientific floating-point data.

Supported Types
---------------

``PDC_FLOAT`` / ``PDC_INT32`` → cuSZ ``F4``.
``PDC_DOUBLE`` / ``PDC_INT64`` → cuSZ ``F8``.
Supports 1D–3D arrays.

pdc_tf_builtin_sz_compress_cuda
---------------------------------

- Allocates a GPU buffer and copies input data to device.
- Creates a cuSZ compressor with ``psz_create_default`` using the input dtype and dimensions.
- Runs ``psz_compress`` with ``Abs`` error mode and tolerance ``0.01``.
- Copies the compressed output back to a host ``malloc``'d buffer.
- Frees device buffers and releases the compressor.
- Sets output region to ``{ndim=1, pdc_var_type=PDC_CHAR, size[0]=compressed_bytes}``.

pdc_tf_builtin_sz_decompress_cuda
-----------------------------------

- Reconstructs the cuSZ decompressor via ``psz_create_from_header`` from the
  cuSZ header that ``sz_compress`` saved with ``SET_FUNC_PARAMS``.
- Copies compressed data to device.
- Allocates a device output buffer sized from the original element type and shape.
- Runs ``psz_decompress`` and synchronizes the device.
- Copies the decompressed result back to host.
- Cleans up with ``psz_clear_buffer`` and ``psz_release``.

-------------------------------------------------------------------------------

.. _tf_builtin_encrypt:

SecretBox Encryption — CPU
===========================

**Source file:** ``pdc_tf_builtin_encrypt_cpu.c``

**Requires:** ``LIBSODIUM_ROOT_DIR`` (``ENABLE_TF_SECRET_BOX_ENCRYPTION``)

Uses ``libsodium``'s ``crypto_secretbox_easy`` (XSalsa20-Poly1305) for authenticated
symmetric encryption. Provides both confidentiality and integrity verification.

Key and Nonce
-------------

The key and nonce are module-level zero-initialized arrays:

.. code-block:: c

   unsigned char key[crypto_secretbox_KEYBYTES]     = {0};
   unsigned char nonce[crypto_secretbox_NONCEBYTES] = {0};

.. warning::

   These are currently hardcoded to zero and must be configured before production use.
   Using a zero key and nonce provides no real security.

pdc_tf_builtin_encrypt
------------------------

- Allocates a ciphertext buffer of size ``plaintext_len + crypto_secretbox_MACBYTES``.
  The extra bytes hold the Poly1305 authentication MAC.
- Calls ``crypto_secretbox_easy`` and replaces ``*region_data`` with the ciphertext.
- Sets output region to ``{ndim=1, pdc_var_type=PDC_CHAR, size[0]=ciphertext_len}``.
- Saves the original region via ``SET_FUNC_PARAMS("secret_box_encrypt", PDC_TF_CPU_DEVICE, ...)``.

pdc_tf_builtin_decrypt
------------------------

- Retrieves the saved original region descriptor.
- Calls ``crypto_secretbox_open_easy`` to verify the MAC and decrypt.
- Returns ``false`` immediately if MAC verification fails — this indicates either
  data corruption or tampering.
- On success, restores ``output_region`` to the original unencrypted shape.


-------------------------------------------------------------------------------

.. _tf_builtin_turbo:

Turbo Compression — CPU
========================

**Source file:** ``pdc_tf_builtin_turbo_cpu.c``

**Requires:** ``TURBO_DIR`` (``ENABLE_TF_TURBO_COMPRESSION``)

Uses the TurboPFor library (``ic.h``) which provides extremely fast SIMD-accelerated
lossless compression for integer arrays using the P4NENC family of codecs.

Supported Types
---------------

``PDC_INT``/``PDC_INT32`` and ``PDC_INT64`` only. Floating-point types are
rejected (the transfer fails).

pdc_tf_builtin_turbo_compress
-------------------------------

- Calls ``p4nenc32`` for 32-bit integers or ``p4nenc64`` for 64-bit integers.
- The output buffer is pre-allocated at the same size as the input (worst case).
- Sets output region to ``{ndim=1, pdc_var_type=PDC_CHAR, size[0]=compressed_bytes}``
  where ``compressed_bytes`` is the actual encoded size returned by the codec.
- Saves the original region via ``SET_FUNC_PARAMS("turbo_compress", PDC_TF_CPU_DEVICE, ...)``.

pdc_tf_builtin_turbo_decompress
---------------------------------

- Retrieves the saved region descriptor to determine the number of elements and output size.
- Calls ``p4ndec32`` or ``p4ndec64`` to decode into a newly ``malloc``'d buffer.
- Restores ``output_region`` to the original pre-compression shape.

.. note::

   TurboPFor codecs are lossless. Unlike SZ3, there is no error bound — the
   reconstructed values are bit-for-bit identical to the originals.


-------------------------------------------------------------------------------

.. _tf_external_transforms:

Adding External Transformations
================================

You can extend the framework with your own transformation functions without modifying
PDC itself by writing a shared library and referencing it from a JSON graph file.

Step 1 — Write the Function
----------------------------

Your function must match the ``c_func_t`` prototype exactly:

.. code-block:: c

   #include "pdc_tf_user.h"

   bool my_transform(pdc_tf_internal_param *internal_param, char *params_str,
                     void **region_data, pdc_tf_region_t input_region,
                     pdc_tf_region_t *output_region)
   {
       // ... transform *region_data in place or replace it ...
       return true;
   }

Compile it as a shared library against the installed PDC headers::

   gcc -shared -fPIC -I$PDC_DIR/include -o libmytransform.so my_transform.c

Step 2 — Add to the JSON Graph
--------------------------------

.. code-block:: json

   {
     "name": "my_graph",
     "lib_path": "/path/to/libmytransform.so",
     "states": [
       { "name": "raw" },
       { "name": "my_format" }
     ],
     "functions": [
       {
         "name": "my_transform",
         "device": "CPU",
         "location": "external",
         "input_state": "raw",
         "output_state": "my_format"
       }
     ]
   }

Step 3 — Use It
----------------

.. code-block:: c

   pdcid_t dg = PDCtf_dg_json_create("/path/to/my_graph.json");
   PDCtf_attach_to_obj(dg, obj_id, "raw", "my_format");

The framework will ``dlopen`` your library and ``dlsym`` your function at graph-load
time (on the client and again on the server), then register it in the builtin
function registry automatically.

A usable pipeline also needs the inverse function (``my_format`` → ``raw``) so
reads can be served; add it as a second ``external`` edge.

Notes
-----

- The function symbol name in the library must exactly match the ``"name"`` field in the JSON.
- ``lib_path`` must be loadable at the same path on the client and the server.
- ``device`` and the return contract follow :ref:`tf_core_concepts`: return
  ``false`` to fail the transfer, never free the incoming ``*region_data``.
- Only ``pdc_tf_user.h`` is available to external libraries — do not include any other PDC headers.
- If your function needs to persist parameters between compress/decompress calls, use the
  ``SET_FUNC_PARAMS`` / ``GET_FUNC_PARAMS`` macros as described in :ref:`tf_parameter_macros`.
