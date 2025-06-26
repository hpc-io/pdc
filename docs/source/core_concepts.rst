.. _core_concepts:

**2.** Core Concepts
====================

**2.1.** Architecture of PDC
----------------------------

PDC is built on a distributed client-server architecture that is optimized 
for high-performance computing environments.The client-server model involves 
the clients which are user applications that interact with the PDC API to perform 
data creation, movement, querying, and transformation. On the other hand, the servers 
are the background systems that manage metadata, coordinate data transfer, and optimize 
resource use. The communication layer between the client-server is that it uses MPI and other 
message-passing interfaces for communication between clients and servers. This 
architecture enables scalable, asynchronous, and metadata-rich operations that decouple 
data abstraction from physical data location.

PDC's Client-Server Model
~~~~~~~~~~~~~~~~~~~~~~~~~

The clients focus on initiating operations such as creating objects, issuing I/O, 
and querying metadata. The servers handle back-end processing, such as object placement, 
metadata indexing, and data coordination. This client-server model was used in order to 
support parallel execution, making it suitable for distributed-memory systems. Some key 
design points about this model are that there is minimal client-side memory footprint, 
high concurrency on server side, and that the background transfers are handled independently 
of the client lifecycle.

Data Management and Movement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PDC is able to handle both data and metadata in a way that optimizes movement 
across complex memory hierarchies. The data involved in these containers is 
stored in objects and moved asynchronously using region-based and generated 
APIs. The metadata is indexed and stored distributively, which supports scalable 
searching and querying. Some primary features include asynchronous data transfers, 
region-based memory and data binding, and the automatic coordination that occurs 
between memory locations and object storage containers.

**2.2.** PDC abstractions
-------------------------

PDC provides several core abstractions for modeling and managing data.

Containers
~~~~~~~~~~

- Logical groupings of related data objects, which are similar to folders or directories
- Can be nested to define data hierarchies
- Associated with metadata such as creation time and access rules

Objects 
~~~~~~~

- Fundamental unit of data in PDC
- Contain a combination of raw data and descriptive metadata
- Objects can be queried, updated, and transferred without direct reference to their physical location

Regions 
~~~~~~~

- Define subsections of an object
- Used during data transfer to read/write a portion of the object
- Support multidimensional sub-regions for structured data (e.g., matrices, cubes)

**2.3.** Properties and Descriptions 
------------------------------------

Properties in PDC determine how objects, containers, and transfers behave. 
These are customizable structures associated with entities at creation time. 
Some examples are provided below.

Object Properties
~~~~~~~~~~~~~~~~~

Object Properties can be defined with characteristics like data type (ex. int, float), 
dimensions (1D, 2D), and user ownership. In PDC, they can be specifically created 
with PDCprop_create(PDC_OBJ_CREATE).

Container Properties
~~~~~~~~~~~~~~~~~~~~

Container Properties include some items which are set during creation, 
such as the container's lifetime (temporary or persistent), and the ownership 
and sharing rules. The specific ownership and sharing rules allow inheritance 
of common properties to occur across multiple objects. 

Data Transfer Properties
~~~~~~~~~~~~~~~~~~~~~~~~

Data Transfer Properties decide how data is moved. In the PDC realm, this is 
through access mode (reading and writing directly), buffer alignment, and 
event-handling settings for different async operations. 

**2.4.** Data Acess Lifecycle 
-----------------------------

The typical workflow for interacting with PDC objects is described below.

Object Creation  
~~~~~~~~~~~~~~~

1. Define object properties (datatype, size, etc.).
2. Create or select a container.
3. Call PDCobj_create() to allocate the object with metadata.

Allocation of Regions & Containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Use PDCregion_create() to define the region to write or read from.
2. The region is tied to an object and a memory location.
3. Multiple regions can be created for batch or parallel operations.

Asynchronous & Synchronous Data Transfers  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Asynchronous**

1. Initiated using PDCbuf_obj_map() 
2. Completion checked with PDCregion_transfer_start() and PDCregion_transfer_wait()
3. Enables overlapping of computation and communication

**Synchronous**

1. Blocking operations that ensure data is transferred before proceeding
2. Useful for simple or sequential workflows

Finalization
~~~~~~~~~~~~

Once all of the operations are completed:

1. Call PDCobj_close() and PDCcont_close() to release all handles 
2. Use PDCclose() to clean up the PDC environment 
3. Ensure that all resources have been flushed and deallocated