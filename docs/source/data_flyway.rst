Data Flyway: Transformations and Analysis
==========================================

*Data Flyway* is PDC's server-side data-processing layer. It lets a client
describe, as a small JSON graph, what should happen to an object's data as it
moves between the client and storage, and then leaves the PDC server to do the
work transparently inside ordinary region transfers. There are two
complementary frameworks:

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * -
     - Transformations (PDC TF, ``PDCtf_*``)
     - Region analysis (PDC AN, ``PDCan_*``)
   * - What it does
     - Converts **one region of one object** between representations, e.g.
       compress and/or encrypt on write and invert on read.
     - Computes **new objects from one or more existing objects**, e.g.
       ``magnitude = sqrt(vx² + vy² + vz²)``.
   * - Graph shape
     - States connected by single-input/single-output *functions* (edges).
     - Bipartite: states and multi-input/multi-output *transformations*.
   * - What the client reads back
     - Its own data, exactly as written (modulo lossy codecs).
     - A *derived* object that the client never wrote.
   * - When work happens
     - Forward path on every write, inverse path on every read.
     - On the write of the last input (``eager``) or the first read of the
       output (``lazy``); afterwards the result is served from storage.
   * - Graph files
     - ``tf_client/graphs/*.json``
     - ``an_client/graphs/*.json``
   * - Guide
     - :doc:`pdc_tf_docs`
     - :doc:`pdc_an_docs`

Both frameworks share the same building blocks: named *states*, functions
that run on a ``CPU`` or ``GPU`` *device*, ``builtin`` or ``external``
(``dlopen``'d) function *locations*, and the same ``pdc_tf_region_t`` shape
descriptor. They can be combined: an analysis output object can itself have a
compression graph attached (see :ref:`an_with_tf`).

Neither framework adds a new I/O call. A client loads a graph, *attaches* it
to objects/regions, and then uses the normal
``PDCregion_transfer_create`` / ``start`` / ``wait`` / ``close`` sequence.

.. contents:: On this page
   :local:
   :depth: 2


.. _flyway_build:

Building with optional dependencies
-----------------------------------

The graph engine, the JSON parser (PDC's vendored cJSON) and the analysis
built-ins (``vector_magnitude``, ``curl``) are always compiled in and need no
extra libraries. Each compression/encryption built-in, and every GPU variant,
depends on a third-party library that is **optional**: it is enabled by
pointing an environment variable at that library's install prefix *before
running CMake*.

.. list-table::
   :header-rows: 1
   :widths: 24 30 46

   * - Environment variable
     - Enables
     - CMake looks for
   * - ``ZFP_ROOT_DIR``
     - ``zfp_compress`` / ``zfp_decompress``
     - ``include/zfp.h`` and ``lib{,64}/libzfp``
   * - ``LIBSODIUM_ROOT_DIR``
     - ``secret_box_encrypt`` / ``secret_box_decrypt``
     - ``include/sodium.h`` and ``lib/libsodium``
   * - ``SZ_CPU_DIR``
     - ``sz_compress`` / ``sz_decompress`` (CPU, SZ3)
     - ``include/SZ3c/sz3c.h`` and ``lib{,64}/libSZ3c``
   * - ``TURBO_DIR``
     - ``turbo_compress`` / ``turbo_decompress`` (TurboPFor)
     - ``include/ic.h`` and ``libic`` directly in ``$TURBO_DIR``
   * - ``CUDA_ROOT_DIR``
     - GPU variants of ZFP and ``vector_magnitude``, NVML GPU profiling
     - ``include/cuda_runtime.h``, ``libcudart`` and ``libnvidia-ml``
   * - ``CUSZ_GPU_DIR``
     - ``sz_compress`` / ``sz_decompress`` GPU variant (cuSZ)
     - ``include/cusz/`` and ``lib{,64}/libcusz``

Example:

.. code-block:: bash

   export ZFP_ROOT_DIR=$HOME/opt/zfp
   export LIBSODIUM_ROOT_DIR=$HOME/opt/libsodium
   export LD_LIBRARY_PATH=$ZFP_ROOT_DIR/lib:$LIBSODIUM_ROOT_DIR/lib:$LD_LIBRARY_PATH
   cmake .. -DBUILD_TESTING=ON ...    # as in the normal install instructions
   make -j && make install

Behavior to be aware of:

- **Unset variable → feature compiled out.** CMake prints e.g.
  ``ZFP_ROOT_DIR not set -- zfp_compress/zfp_decompress transformation disabled``
  and the build continues. At runtime, loading a graph that names a
  compiled-out built-in fails: ``PDCtf_dg_json_create`` /
  ``PDCan_dg_json_create`` log ``Failed to link to builtin function`` and
  return ``0``.
- **Set variable, but library not found → configure error.** A variable that
  is set but points at the wrong prefix is a hard ``FATAL_ERROR``, so a typo
  is never silently turned into a disabled feature.
- **CUDA requires cuSZ.** When ``CUDA_ROOT_DIR`` is set, CMake currently also
  requires ``CUSZ_GPU_DIR`` to point at a cuSZ install.
- The libraries are linked into ``libpdc`` and ``pdc_server``, so they must be
  on ``LD_LIBRARY_PATH`` (or in the rpath) for the server **and** the clients.

Which built-ins are available in a build:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Built-in name
     - Device
     - Requires
   * - ``zfp_compress`` / ``zfp_decompress``
     - CPU
     - ``ZFP_ROOT_DIR``
   * - ``zfp_compress`` / ``zfp_decompress``
     - GPU
     - ``ZFP_ROOT_DIR`` + ``CUDA_ROOT_DIR`` (ZFP built with CUDA)
   * - ``sz_compress`` / ``sz_decompress``
     - CPU
     - ``SZ_CPU_DIR``
   * - ``sz_compress`` / ``sz_decompress``
     - GPU
     - ``SZ_CPU_DIR`` + ``CUDA_ROOT_DIR`` + ``CUSZ_GPU_DIR``
   * - ``secret_box_encrypt`` / ``secret_box_decrypt``
     - CPU
     - ``LIBSODIUM_ROOT_DIR``
   * - ``turbo_compress`` / ``turbo_decompress``
     - CPU
     - ``TURBO_DIR``
   * - ``vector_magnitude`` (analysis)
     - CPU
     - always available
   * - ``vector_magnitude`` (analysis)
     - GPU
     - ``CUDA_ROOT_DIR``
   * - ``curl`` (analysis)
     - CPU
     - always available


.. _flyway_graph_paths:

Where graph files live
----------------------

Graph files are plain JSON and can live anywhere, but note that **the graph
is parsed twice**: once by the client in ``PDCtf_dg_json_create`` /
``PDCan_dg_json_create`` (which validates it and links every function), and
again by the server, which receives only the file *path* and opens the file
itself. The graph path -- and the ``lib_path`` of any external function
library -- must therefore be readable at the same path on the client nodes
and on the nodes running ``pdc_server``. Use absolute paths on a shared file
system.

The repository ships example graphs used by the test suite:

- ``tf_client/graphs/`` -- ``zfp.json``, ``zfp_gpu.json``,
  ``zfp_gpu_or_cpu.json``, ``sz.json``, ``sz_gpu.json``, ``turbo.json``,
  ``libsod.json``, ``zfp_libsod.json``, ``zfp_or_libsod.json``,
  ``custom.json``.
- ``an_client/graphs/`` -- ``vector_magnitude.json``,
  ``vector_magnitude_2d.json``, ``vector_magnitude_lazy.json``,
  ``vector_magnitude_dynamic.json``, ``curl_vorticity_magnitude.json``,
  ``curl_vorticity_magnitude_transient.json``.

Inside the PDC build tree the macros ``TF_GRAPHS_DIR`` and ``AN_GRAPHS_DIR``
expand to the absolute paths of those directories (with a trailing ``/``);
they are only defined for code compiled as part of PDC (tests/benchmarks), not
for applications built against an installed PDC.


.. _flyway_server_env:

Server runtime controls
-----------------------

These environment variables are read by ``pdc_server``:

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Variable
     - Effect
   * - ``USE_GPU``
     - If set (to anything), PDC TF always picks a GPU edge on GPU 0 when a
       CPU/GPU choice exists. Useful for benchmarking.
   * - ``PDC_POLY_COEFF_FILE``
     - Path to the coefficients file for the polynomial GPU-selection model
       (CUDA builds only). Without it the scheduler falls back to its
       utilization heuristic. See :ref:`tf_scheduler`.
   * - ``PDC_AN_FORCE_DEVICE``
     - ``CPU`` or ``GPU``: forces which variant of a multi-device analysis
       transformation runs. Intended for tests.

The server logs every scheduling decision as a ``LOG_WARNING`` line prefixed
with ``SCHED:``.


.. _flyway_testing:

Testing
-------

The transformation and analysis tests live in ``src/tests/transformation/``
and ``src/tests/analysis/`` and are registered with CTest under their own
labels, separately from the ``serial`` and ``parallel`` suites:

.. code-block:: bash

   cd build
   ctest -L "transform|analysis" --output-on-failure   # all Data Flyway tests
   ctest -L transform                                  # transformations only
   ctest -L analysis                                   # analysis only

A test is only **registered** when every library its graph needs was found at
configure time, so a build without the optional dependencies runs only the
tests it can actually pass instead of failing the rest. CMake lists the ones it
skipped:

.. code-block:: text

   -- Transformation/analysis tests not registered (optional dependency missing): compression_transform, ...

.. list-table::
   :header-rows: 1
   :widths: 45 25 30

   * - Test(s)
     - Graph(s)
     - Registered when
   * - ``vector_magnitude_test``, ``vector_magnitude_2d_test``
     - ``vector_magnitude*.json``
     - always
   * - ``compression_transform``, ``compression_transform_2D``,
       ``compression_transform_3D``, ``attach_obj_compression_transform``,
       ``all_compression_transform``
     - ``zfp.json``
     - ZFP
   * - ``vector_magnitude_compressed_test``
     - ``vector_magnitude.json`` + ``zfp.json``
     - ZFP
   * - ``compression_encryption_transform``,
       ``compression_encryption_transform_2D``,
       ``compression_encryption_transform_3D``, ``multi_region_transform``
     - ``zfp_libsod.json``
     - ZFP and libsodium
   * - ``compression_transform_gpu`` (labels ``transform;transform_gpu``)
     - ``zfp_gpu.json``
     - ZFP and CUDA
   * - ``cuda_smoke_test``, ``vector_magnitude_gpu_test``
       (labels ``analysis;analysis_gpu``)
     - ``vector_magnitude_dynamic.json``
     - CUDA

When you add a new transformation or analysis test, register it inside the
matching ``if(...)`` block in ``src/tests/CMakeLists.txt`` (and add it to that
block's ``PDC_TF_SKIPPED_TESTS`` list in the ``else()`` branch), so builds
without the dependency keep passing.

.. tip::

   If a freshly built test fails with
   ``symbol lookup error: ... undefined symbol: PDCan_dg_json_create`` (or any
   other ``PDCtf_``/``PDCan_`` symbol), the test picked up an older installed
   ``libpdc`` from ``LD_LIBRARY_PATH``. Put the build tree's ``bin``/``lib``
   directories first or run ``make install`` before testing.
