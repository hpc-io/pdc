.. _client_api: 

**5.** Client API
=================

.. _compile_time_options:

5.1. Compile-Time Options
-------------------------

The following table lists all available compile-time options for PDC, along with a description of each and their current support status:

.. list-table:: Compile-Time Macros
   :header-rows: 1
   :widths: 30 10 50 10

   * - Option Name
     - Default
     - Description
     - Support
   * - BUILD_MPI_TESTING
     - ON
     - Build MPI testing.
     - 🟢
   * - BUILD_SHARED_LIBS
     - ON
     - Build with shared libraries.
     - 🟢
   * - BUILD_TESTING
     - ON
     - Build the testing tree.
     - 🟢
   * - BUILD_TOOLS
     - OFF
     - Build tools.
     - 🟡
   * - PDC_DART_SUFFIX_TREE_MODE
     - ON
     - Enable DART Suffix Tree mode.
     - 🔴
   * - PDC_ENABLE_APP_CLOSE_SERVER
     - OFF
     - Close PDC server at the end of the application.
     - 🟡
   * - PDC_ENABLE_CHECKPOINT
     - ON
     - Enable checkpointing.
     - 🟡
   * - PDC_ENABLE_FASTBIT
     - OFF
     - Enable FastBit.
     - 🔴
   * - PDC_ENABLE_JULIA_SUPPORT
     - OFF
     - Enable Julia support.
     - 🔴
   * - PDC_ENABLE_LUSTRE
     - OFF
     - Enable Lustre.
     - 🟡
   * - PDC_ENABLE_MPI
     - ON
     - Enable MPI.
     - 🟢
   * - PDC_ENABLE_MULTITHREAD
     - OFF
     - Enable multithreading.
     - 🟡
   * - PDC_ENABLE_PROFILING
     - OFF
     - Enable profiling.
     - 🟡
   * - PDC_ENABLE_ROCKSDB
     - OFF
     - Enable RocksDB (experimental).
     - 🔴
   * - PDC_ENABLE_SQLITE3
     - OFF
     - Enable SQLite3 (experimental).
     - 🔴
   * - PDC_ENABLE_TF_ZFP_COMPRESSION
     - ON
     - TensorFlow + ZFP compression (no inline help).
     - 🟡
   * - PDC_ENABLE_WAIT_DATA
     - OFF
     - Wait for data finalized in FS when object unmap is called.
     - 🟡
   * - PDC_ENABLE_ZFP
     - OFF
     - Enable ZFP.
     - 🔴
   * - PDC_HAVE_ATTRIBUTE_UNUSED
     - ON
     - Use compiler attribute for unused variables.
     - 🟢
   * - PDC_SERVER_CACHE
     - OFF
     - Enable server caching.
     - 🟡
   * - PDC_TIMING
     - OFF
     - Enable timing.
     - 🟡
   * - PDC_USE_CRAY_DRC
     - OFF
     - Use Cray DRC to allow multi-job communication.
     - 🔴
   * - PDC_USE_SHARED_SERVER
     - OFF
     - Use shared server with client mode.
     - 🔴

Legend:

- 🟢 = Fully supported
- 🟡 = Partially/experimentally supported
- 🔴 = Not supported or currently disabled


**5.2.** C API Overview
-----------------------
