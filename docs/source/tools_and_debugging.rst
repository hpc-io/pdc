.. _tools_and_debugging:

**7.** Tools and Debugging
==========================

**7.1** Logging
---------------

PDC provides detailed logging capabilities to assist developers in understanding runtime behavior.

* Enabling debugging 
* Enabling output associated with debugging 
* Understanding logs of PDC 
		
Logs include timestamps, thread IDs, and operation context. 
Common log entries cover connection establishment, data transfer initiation/completion, errors, and resource cleanup.

PDC provides detailed logging capabilities to assist developers in understanding runtime behavior.

* Enabling debugging
        Compile with -DPDC_ENABLE_LOG=ON to include internal logging hooks.

* Enabling output associated with debugging
        Set the PDC_LOG_LEVEL environment variable (e.g., DEBUG, INFO, WARN, ERROR) to control verbosity at runtime.

* Understanding logs of PDC
        Logs include timestamps, thread IDs, function call traces, and messages from both client and server sides.

Key logging categories:

* Initialization and shutdown

* Data transfer initiation/completion

* RPC communication events

* Errors, warnings, and assertions

**7.2** Profiling and Tracing
-----------------------------

Profiling and tracing help developers evaluate performance and diagnose bottlenecks.

* Visualizing transferring data
        You can trace object I/O events, region accesses, and metadata lookup sequences. Logging output may be redirected to profiling tools like TAU or Darshan.

* Access patterns of transferring data
        Log regions accessed, data sizes, and the asynchronous/synchronous nature of each I/O. This is useful for tuning and understanding application memory behavior under realistic load.


**7.3** Common Errors
---------------------

Below are frequently encountered errors and how to address them:

* Accessing an Invalid Region
        Ensure that regions defined are within the bounds of the object’s dimensions.

* Putting too many items in one region/container
        Containers and regions may have hard limits; split data into multiple objects or use dynamic allocation strategies.

* Unregistered Object Errors
        Ensure all objects are registered with valid properties before use.

* Network Communication Errors
        Validate server availability, correct port bindings, and network configurations. Logs will usually include connection or RPC timeout details.

* Memory Leaks or Buffer Mismanagement
        Use softwares with debug builds to check for leaks or improper unmap operations.

**7.4** Utilities
-----------------

PDC provides several command-line utilities (when built with tools enabled):

* pdc_server: Launches the PDC metadata/data server.

* pdc_kv_client_test: Example for key-value operations.

* test programs: demonstrate API usage and stress different parts of the system.

These tools help validate installations, debug workflows, and serve as reference implementations.


**7.5** Interfaces 
------------------

PDC supports external interfaces and integration pathways:

* HDF5 VOL Plugin
        Allows applications using the HDF5 API to transparently store data through PDC backends, leveraging PDC's object-based storage model.

* MPI Integration
        PDC works in MPI-based environments, enabling parallel data management and I/O operations compatible with distributed memory systems.

* Custom Interfaces
        The modular structure allows extending PDC to support additional languages or I/O frameworks through bindings or wrappers.

