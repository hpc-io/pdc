PDC Transformation Framework
=============================

The PDC Transformation Framework (PDC TF) is a server-side pipeline system that allows
data regions to be automatically transformed (compressed, encrypted, etc.) as they flow
between clients and the PDC data server. Transformations are described as **directed graphs**
where nodes are named **states** (e.g. ``"raw"``, ``"compressed"``) and edges are **functions**
that convert data from one state to another. The framework supports both CPU and GPU execution
and can dynamically schedule work across available devices based on real-time utilization metrics.

Here's how to navigate:

- **Architecture & Core Concepts**
  Understand how states, functions, regions, and directed graphs fit together.

- **JSON Graph Format**
  Learn how to describe transformation pipelines in JSON.

- **Client & Server API**
  Reference for attaching graphs to objects and executing transformations.

- **Scheduler & Profiler**
  How the framework selects between CPU and GPU edges at runtime.

- **Builtin Transformations**
  Descriptions of every builtin compression and encryption function.

- **Extending the Framework**
  How to add your own external transformation functions.

Use the table of contents below to explore each section in detail.

.. toctree::
   :maxdepth: 2
   :caption: Architecture & Core Concepts

   tf_architecture
   tf_core_concepts

.. toctree::
   :maxdepth: 2
   :caption: Configuration & Graph Format

   tf_json_format
   tf_compile_flags

.. toctree::
   :maxdepth: 2
   :caption: Client & Server API

   tf_client_api
   tf_server_api
   tf_parameter_macros

.. toctree::
   :maxdepth: 2
   :caption: Scheduler & Profiler

   tf_scheduler
   tf_profiler

.. toctree::
   :maxdepth: 2
   :caption: Builtin Transformations

   tf_builtin_registry
   tf_builtin_zfp_cpu
   tf_builtin_zfp_gpu
   tf_builtin_sz_cpu
   tf_builtin_sz_gpu
   tf_builtin_encrypt
   tf_builtin_turbo

.. toctree::
   :maxdepth: 1
   :caption: Extending the Framework

   tf_external_transforms
   tf_contributing


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
       pdc_tf_internal_param internal_param,  // Graph handle + flat offset
       char                 *params_str,      // Optional params string from JSON
       void                **region_data,     // Pointer to data buffer (may be replaced)
       pdc_tf_region_t       input_region,    // Describes input data shape/type
       pdc_tf_region_t      *output_region    // Must be populated if shape/type changes
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

   { "name": "zfp_compress",      "device": "CPU", "location": "builtin",
     "input_state": "raw", "output_state": "compressed" },
   { "name": "zfp_compress_cuda", "device": "GPU", "location": "builtin",
     "input_state": "raw", "output_state": "compressed" }


-------------------------------------------------------------------------------

.. _tf_compile_flags:

Compile-Time Feature Flags
===========================

The following CMake flags control which builtin transformations are compiled in.
They are currently also ``#define``'d at the top of ``pdc_tf_builtin_common.h``
as a temporary workaround (marked ``FIXME`` — these should be driven solely by CMake).

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Flag
     - Effect
   * - ``ENABLE_TF_ZFP_COMPRESSION``
     - Enables ZFP CPU compress/decompress
   * - ``CUDA_ENABLED``
     - Enables ZFP GPU compress/decompress (requires ZFP built with CUDA support)
   * - ``ENABLE_TF_SZ_COMPRESSION``
     - Enables SZ3 CPU compress/decompress
   * - ``ENABLE_TF_SZ_GPU_COMPRESSSION``
     - Enables cuSZ GPU compress/decompress
   * - ``ENABLE_TF_SECRET_BOX_ENCRYPTION``
     - Enables libsodium SecretBox encrypt/decrypt
   * - ``ENABLE_TF_TURBO_COMPRESSION``
     - Enables TurboPFor integer compress/decompress


-------------------------------------------------------------------------------

.. _tf_client_api:

Client API
==========

Declared in ``pdc_tf.h``. These functions are called from client applications to
attach transformation graphs to PDC objects and regions.

Initialization
--------------

.. c:function:: perr_t PDCtf_init()

   Must be called once during PDC initialization. Registers the ``PDC_TF_DG`` type
   and populates the builtin function registry by calling :c:func:`PDCtf_init_builtin_funcs`.

Graph Loading
-------------

.. c:function:: pdcid_t PDCtf_dg_json_create(char *json_filepath)

   Parses a JSON graph file and returns a handle (``pdcid_t``) to the in-memory directed
   graph. External functions referenced in the JSON are loaded via ``dlopen``/``dlsym``
   using the ``lib_path`` field. Returns ``0`` on failure.

.. c:function:: perr_t PDCtf_close_dg(pdcid_t dg_id)

   Frees all resources associated with a directed graph, including all state and function
   nodes and their per-region parameter storage.

Attaching Graphs
----------------

.. c:function:: perr_t PDCtf_attach_to_region(pdcid_t dg_id, pdcid_t obj_id, pdcid_t remote_reg, char *client_state, char *store_state)

   Attaches a transformation graph to a **specific region** of an object. Only transfers
   touching that exact region (matched by ndim, offset, size, and element type) will be
   routed through the graph.

   :param dg_id: Handle to the directed graph returned by :c:func:`PDCtf_dg_json_create`.
   :param obj_id: PDC object to attach the graph to.
   :param remote_reg: Region ID describing the specific region.
   :param client_state: The state name representing data as the client sees it (e.g. ``"raw"``).
   :param store_state: The state name representing data as stored on disk (e.g. ``"compressed"``).

.. c:function:: perr_t PDCtf_attach_to_obj(pdcid_t dg_id, pdcid_t obj_id, char *client_state, char *store_state)

   Attaches a transformation graph to **all regions** of an object. Every read/write on
   the object will be routed through the graph regardless of region.

.. c:function:: perr_t PDCtf_attach_to_objs(pdcid_t dg_id, pdcid_t *obj_ids, int num_ids, char *client_state, char *store_state)

   Convenience wrapper that calls :c:func:`PDCtf_attach_to_obj` for each object in the array.

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

   // In the compress function — save the original region so decompress can restore it
   my_params_t *out_params = malloc(sizeof(my_params_t));
   PDCtf_copy_tf_region_t(&input_region, &out_params->decompressed_region);
   SET_FUNC_PARAMS("my_compress", PDC_TF_CPU_DEVICE, out_params, sizeof(my_params_t));

   // In the decompress function — retrieve the saved region
   my_params_t *in_params = NULL;
   uint64_t     in_params_size;
   GET_FUNC_PARAMS("my_compress", PDC_TF_CPU_DEVICE, (void **)&in_params, &in_params_size);
   PDCtf_copy_tf_region_t(&in_params->decompressed_region, output_region);


-------------------------------------------------------------------------------

.. _tf_scheduler:

Scheduler
=========

When multiple edges exist between the same two states (e.g. both a CPU and a GPU
``zfp_compress``), :c:func:`PDCtf_exec_graph` selects one using the following priority:

Selection Algorithm
-------------------

1. **Force GPU** — If the environment variable ``USE_GPU`` is set, or if ``close_time_g``
   is set (indicating the server is approaching a deadline), always pick the GPU edge
   on device 0.

2. **Skip overloaded GPU** — If the least-loaded GPU has average utilization above 50%,
   skip all GPU edges and fall back to CPU.

3. **Expected time** — For each remaining candidate edge, compute:

   .. math::

      \text{expected\_time} = \frac{\text{avg\_past\_time}}{\max(0.1,\ 1.0 - \text{device\_utilization})}

   The edge with the lowest expected time is selected.

Execution Time History
-----------------------

After each transformation, the actual elapsed wall-clock time is scaled by the
device's free capacity to produce a *projected free-device time*:

.. code-block:: text

   projected_time = transform_time * (1.0 - max(device_utilization, 0.1))

This projected time is stored in a rolling history buffer (``prev_cpu_times`` /
``prev_gpu_times``, length ``NUM_TF_FUNC_TIMES = 5``) and used to compute
``avg_past_time`` in future scheduling decisions.

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

**Requires:** ``ENABLE_TF_ZFP_COMPRESSION``

ZFP is a floating-point and integer array compression library optimized for
multidimensional scientific data. The CPU implementation uses the standard ZFP
bitstream API in fixed-rate mode.

Supported Types
---------------

``PDC_FLOAT``, ``PDC_DOUBLE``, ``PDC_INT32``, ``PDC_INT64``.
Supports 1D–4D arrays.

Rate
----

The rate is set to ``8 * sizeof(element)`` bits/value (``FIXED_CR_RATIO = 1``).
This corresponds to lossless-equivalent throughput at the ZFP bitstream level.

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

**Requires:** ``ENABLE_TF_ZFP_COMPRESSION`` and ``CUDA_ENABLED``

Same logical operation as :ref:`tf_builtin_zfp_cpu` but executes on the GPU using
``zfp_stream_set_execution(zfp, zfp_exec_cuda)``.

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

**Requires:** ``ENABLE_TF_SZ_COMPRESSION``

SZ3 is an error-bounded lossy scientific data compressor that achieves high
compression ratios on floating-point arrays by exploiting value predictability.

Supported Types
---------------

``PDC_FLOAT``, ``PDC_DOUBLE``, ``PDC_INT32``, ``PDC_INT64``.
Integer types are mapped to their nearest float equivalent for SZ3 internally
(``PDC_INT32`` → ``SZ_FLOAT``, ``PDC_INT64`` → ``SZ_DOUBLE``).

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

**Requires:** ``ENABLE_TF_SZ_GPU_COMPRESSSION``

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

- Reconstructs the cuSZ compressor from the compressed data header via ``psz_create_from_header``.
- Copies compressed data to device.
- Allocates a device output buffer sized for the decompressed data (``float`` assumed).
- Runs ``psz_decompress`` and synchronizes the device.
- Copies the decompressed result back to host.
- Cleans up with ``psz_clear_buffer`` and ``psz_release``.

.. note::

   The output buffer is always sized as ``num_elements * sizeof(float)``
   regardless of the original integer type. This is a current limitation of the
   cuSZ integration.


-------------------------------------------------------------------------------

.. _tf_builtin_encrypt:

SecretBox Encryption — CPU
===========================

**Source file:** ``pdc_tf_builtin_encrypt_cpu.c``

**Requires:** ``ENABLE_TF_SECRET_BOX_ENCRYPTION``

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

**Requires:** ``ENABLE_TF_TURBO_COMPRESSION``

Uses the TurboPFor library (``ic.h``) which provides extremely fast SIMD-accelerated
lossless compression for integer arrays using the P4NENC family of codecs.

Supported Types
---------------

``PDC_INT32`` and ``PDC_INT64`` only. Floating-point types are not supported by
this codec.

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

Compile it as a shared library::

   gcc -shared -fPIC -o libmytransform.so my_transform.c

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

   PDCtf_init();
   pdcid_t dg = PDCtf_dg_json_create("/path/to/my_graph.json");
   PDCtf_attach_to_obj(dg, obj_id, "raw", "my_format");

The framework will ``dlopen`` your library and ``dlsym`` your function at graph-load
time, then register it in the builtin function registry automatically.

Notes
-----

- The function symbol name in the library must exactly match the ``"name"`` field in the JSON.
- Only ``pdc_tf_user.h`` is available to external libraries — do not include any other PDC headers.
- If your function needs to persist parameters between compress/decompress calls, use the
  ``SET_FUNC_PARAMS`` / ``GET_FUNC_PARAMS`` macros as described in :ref:`tf_parameter_macros`.
