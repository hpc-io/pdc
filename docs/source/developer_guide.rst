.. _developer_guide:

**7.** Developer Guide
======================

**7.1** Asynchronous Input/Output and Event Handling 
----------------------------------------------------

PDC’s architecture enables efficient asynchronous I/O operations, allowing computation and communication to overlap, which improves application performance especially in HPC environments.

Using asynchronous APIs: 

* PDC provides non-blocking APIs for data transfer such as PDCregion_transfer_start() which initiates a transfer without waiting for completion
* Multiple asynchronous transfers can be launched concurrently to maximize throughputs
* Buffers remain valid until the transfer completes, so the application should avoid modifying memory before transfer completion.

Monitoring events: 

* Applications can query the status of transfers using event monitoring APIs like PDCregion_transfer_wait() or polling mechanisms
* Event callbacks can be registered to handle completion asynchronously, improving responsiveness and resource management.

Waiting for events:
		
* Synchronization can be achieved by explicitly waiting for event completion to ensure data consistency
* Use blocking calls or condition variables to coordinate dependent computations after I/O completion


**7.2** Scalability and Performance
-----------------------------------

Data placement:
		
* PDC supports policy-driven data placement to optimize locality and bandwidth usage
* Object metadata guides data distribution across different storage hierarchies 
* Applications can hint preferred storage classes or tiers to improve I/O performance.

PDC server tuning:

* Server-side parameters such as thread counts, buffer sizes, and cache policies can be tuned for target workloads and for different data types
* Load balancing between servers ensures no single node becomes a bottleneck, and that the data is balanced properly 
* Profiling server behavior helps identify hot spots or resource contention.


**7.3** Integration with MPI and Libraries
------------------------------------------

Using PDC with MPI:

PDC seamlessly integrates with MPI for communication in distributed-memory environments. MPI ranks act as PDC clients issuing data operations concurrently. Also, MPI synchronization primitives can coordinate phases of PDC usage.

Comparison between libraries:

* Unlike traditional MPI-IO, PDC offers object-based APIs with asynchronous data transfers and metadata indexing
* Compared to HDF5 or ADIOS, PDC provides a more flexible abstraction layer, optimized for highly scalable and concurrent workloads
* PDC’s client-server model decouples data access from storage layout, enabling adaptable backends.


PDC Tools 
