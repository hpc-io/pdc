================================
Developer Notes
================================


+++++++++++++++++++++++++++++++++++++++++++++
PDC Server Metadata Management
+++++++++++++++++++++++++++++++++++++++++++++

PDC metadata servers, a subset of PDC servers, store metadata for PDC classes such as objects and containers. PDC data server, also a subset of PDC servers (potentially overlapping with PDC metadata server), manages data from users. Such management includes server local caching and I/O to the file system. Both PDC metadata and data servers have some local metadata. 

---------------------------------------------
PDC Metadata Structure
---------------------------------------------

PDC metadata is held in server memories. When servers are closed, metadata will be checkpointed into the local file system. Details about the checkpoint will be discussed in the metadata implementation section.

PDC metadata consists of three major parts at the moment:

- Metadata stored in the hash tables at the metadata server: stores persistent properties for PDC containers and PDC objects. When objects are created, these metadata are registered at the metadata server using mercury RPCs. 

- Metadata query class at the metadata server: maps an object region to a data server, so clients can query for this information to access the corresponding data server. It is only used by dynamic region partition strategy

- Object regions stored at the data server: this includes file names and region chunking information inside the object file on the file system.

---------------------------------------------
Metadata Operations at Client Side
---------------------------------------------

In general, PDC object metadata is initialized when an object is created. The metadata stored at the metadata server is permanent. When clients create the objects, a PDC property is used as one of the arguments for the object creation function. Metadata for the object is set by using PDC property APIs. Most of the metadata are not subject to any changes. Currently, we support setting/getting object dimensions using object API. 

---------------------------------------------
PDC Metadata Management Strategy
---------------------------------------------

This section discusses the metadata management approaches of PDC. First, we briefly summarize how PDC managed metadata in the past. Then, we propose new infrastructures for metadata management.


Managing Metadata and Data by the Same Server
---------------------------------------------

Historically, a PDC server manages both metadata and data for objects it is responsible for. A client forwards I/O requests to the server computed based on MPI ranks statically. If a server is located on the same node as the client, the server will be chosen with a higher priority. This design can achieve high I/O parallelism if the I/O workloads from all clients are well-balanced. In addition, communication contention is minimized because servers are dedicated to serving disjoint subsets of clients.

However, this design has two potential drawbacks. The first disadvantage is supporting general I/O access. For clients served by different PDC servers, accessing overlapping regions is infeasible. Therefore, this design is specialized in applications with a non-overlapping I/O pattern. The second disadvantage is a lack of dynamic load-balancing mechanisms. For example, some applications use a subset of processes for processing I/O. A subset of servers may stay idle because the clients mapped to them are not sending I/O requests.


Separate Metadata Server from Data Server
---------------------------------------------

Metadata service processes are required for distributed I/O applications with a one-sided communication design. When a client attempts to modify or access an object, metadata provides essential information such as object dimensions and the data server rank that contains the regions of interest. A PDC client generally does not have the runtime global metadata information. As a result, the first task is to obtain the essential metadata of the object from the correct metadata server.

Instead of managing metadata and data server together, we can separate the metadata management from the region I/O. A metadata server stores and manages all attributes related to a subset of PDC objects. A PDC server can be both a metadata and data server. However, the metadata and data can refer to different sets of objects.

This approach's main advantage is that the object regions' assignment to data servers becomes flexible. When an object is created, the name of the object maps to a unique metadata server. In our implementation, we adopt string hash values for object names and modulus operations to achieve this goal. The metadata information will be registered at the metadata server. Later, when other clients open the object, they can use the object's name to locate the same metadata server. 

When a client accesses regions of an object, the metadata server informs the client of the corresponding data servers it should transfer its I/O requests. Metadata servers can map object regions to data servers in a few different methods.

---------------------------------------------
PDC Metadata Management Implementation
---------------------------------------------

This section discusses how object metadata is implemented in the PDC production library. The following figure illustrates the flow of object metadata for different object operations. We label the 4 types of metadata in bold.

Create Metadata
---------------------------------------------

Metadata for an object is created by using a PDC property. PDC property is created using client API ``PDCprop_create(pdc_prop_type_t type, pdcid_t pdc_id)``. After a property instance is created, it is possible to set elements in this property using object property APIs. An alternative way is to use ``pdcid_t PDCprop_obj_dup(pdcid_t prop_id)``, which copies all the existing entries in a property to a new object instance.

Binding Metadata to Object
---------------------------------------------

Metadata is attached to an object at the object creation time. ``PDCobj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id)`` is the prototype for binding an object property when an object is created.

Register Object Metadata at Metadata Server
---------------------------------------------

Once an object is created locally at a client, the object metadata is sent to the corresponding metadata server based on the hash value computed from the object name. Internally, search for ``typedef struct pdc_metadata_t {...} pdc_metadata_t;`` in the  ``pdc_client_server_common.h`` file. This data structure contains essential metadata about the object, such as its dimension and name.

Retrieve Metadata from Metadata Server
---------------------------------------------

Object metadata can be obtained from the metadata server when clients open an object using the prototype ``pdcid_t PDCobj_open(const char *obj_name, pdcid_t pdc)``. The client contacts the corresponding metadata server to retrieve data from the data type ``pdc_metadata_t`` stored at the server.

Object Metadata at Client
---------------------------------------------

The current implementation stores metadata at the client in two separate places due to historical reasons. Both places can be accessed from the data type ``struct _pdc_obj_info*``, which is a data type defined in ``pdc_obj_pkg.h``.

We can generally use ``struct _pdc_id_info *PDC_find_id(pdcid_t obj_id)`` to locate the object info pointer ``obj``. Then, ``(struct _pdc_obj_info )(obj->obj_ptr)`` allows use to obtain the ``struct _pdc_obj_info`` structure. We call this pointer ``obj_info_ptr``.
The first piece of local metadata, denoted as metadata buffer, is stored in ``obj_info_ptr->metadata``. This value is a pointer that represents ``pdc_metadata_t``. Its content matches the values stored at the metadata server side exactly. For object create, we copy the data from the pointer to the server memory using mercury RPCs. For object open, we copy from server memory to client memory.

The second piece of local metadata, denoted as object public property, is stored in ``obj_info_ptr->obj_pt``, which has type ``struct pdc_obj_prop`` defined in the ``pdc_prop.h`` file. The values in this data type are copied from the first piece. This metadata data type contains essential information, such as object dims and region partition types. 

Metadata at Data Server
---------------------------------------------

Details about the data server will not be discussed in this section. In general, a data server takes inputs (both metadata and data for an object) from clients and processes them accordingly. It is not supposed to store metadata information for objects. However, it is responsible for storing the locations of data in the file system, including path and offset for regions.

If server cache is enabled, object dimension is stored by the server cache infrastructure when an object is registered for the first time. Object dimension is not used anywhere unless the I/O mode is set to be canonical file order storage. Currently, this mode does not allow clients to change object dimension, so it is not subject to metadata update, which is discussed in the following subsection.

Object Metadata Update
---------------------------------------------

Object metadata is defined before creating an object. At the early stage of PDC, we did not plan to change any of the metadata after an object was created. However, it may be necessary to do this in the future. For example, sometimes applications want to change the sizes of PDC objects along different dimensions. An example is implemented as ``perr_t PDCobj_set_dims(pdcid_t obj_id, int ndim, uint64_t *dims)``. This function can change object dimensions in runtime. As mentioned earlier, we need to update the metadata in three places. Two places are at the client side, and the other place is at the metadata server.

Object Region Metadata
---------------------------------------------

Region metadata is required for dynamic region partitioning. Dynamic region partitioning strategy at the metadata server assigns data server IDs for regions in runtime. The file ``pdc_server_region_transfer_metadata_query.c`` implements the assignments of data server ID for individual regions. For dynamic region partition and local region partition strategies, a metadata server receives client region transfer requests. The metadata server returns a data server ID to the client so the client can send data to the corresponding data server. Details about how the client connects to the metadata server will be discussed in the implementation of the region transfer request.

Metadata Checkpoint
---------------------------------------------

When PDC servers are closed, metadata stored by metadata servers is saved to the file system. Later, when users restart the servers, essential metadata are read back to the memory of the metadata server. In general, client applications should not be aware of any changes if servers are closed and restarted. This subsection layout the data format of PDC metadata when they are checkpointed.

Implementation of server checkpoint is in the function ``PDC_Server_checkpoint``, and the corresponding restart is in the function ``PDC_Server_restart(char *filename)``. The source file is ``pdc_server.c``.

There are four categories of metadata to be checkpointed. One category is concatenated after another seamlessly. We demonstrate the first three categories of metadata in the following figures. Before each bracket, an integer value will indicate the number of repetitions for contents in the brackets. Contents after the bracket will start from the next byte after the last repetition for contents in the bracket. The last category is managed by an independent module ``pdc_server_region_transfer_metadata_query.c``. The content of the metadata is subject to future changes.

!!!!!

Region metadata checkpoint is placed at the end of the server checkpoint file, right after the last byte of data server region. Function ``transfer_request_metadata_query_checkpoint(char **checkpoint, uint64_t *checkpoint_size)`` in  ``pdc_server_region_transfer_metadata_query.c`` file handles the wrapping of region metadata.

---------------------------------------------
Metadata Search and Its Implementation
---------------------------------------------

For Metadata search, we current provide brute-force approaches and index-facilitated approaches. 
For either of these approaches, we consider two types of communication model : point-to-point and collective. 

Point-to-point communication model is for distributed applications where each single client may not follow the exact same workflows, and the timing for them to trigger a metadata search function call can be really random. In this case, each client contacts one or more metadata servers and get the complete result.
Collective communication model applies when a typical application is running. In such an application, each rank follows the exact same workflow and they may trigger a metadata search function call at the same time and the metadata search requests are sent from these clients collectively. In this case, each rank contacts one metadata server and retrieves partial result. Then these clients have to communicate with each other to get the complete result.

Brute-force Approach
---------------------------------------------

For brute-force approach, here are the APIs you can call for different communication models:
    * PDC_Client_query_kvtag (point-to-point)
    * PDC_Client_query_kvtag_mpi (collective)

Index-facilitated Approach
---------------------------------------------

For index-facilitated approach, here are the APIs you can call for different communication models:
    * PDC_Client_search_obj_ref_through_dart (point-to-point)
    * PDC_Client_search_obj_ref_through_dart_mpi (collective)

Before using these APIs, you need to create your index first, so please remember to call `PDC_Client_insert_obj_ref_into_dart` right after a successful function call of `PDCobj_put_tag`.

+++++++++++++++++++++++++++++++++++++++++++++
Object and Region Management
+++++++++++++++++++++++++++++++++++++++++++++

This section discusses how PDC manages objects and regions. 

---------------------------------------------
Static Object Region Mappings
---------------------------------------------

A metadata server can partition the object space evenly among all data servers. For high-dimensional objects, it is possible to define block partitioning methods similar to HDF5s's chunking strategies.

The static object region partitioning can theoretically achieve optimal parallel performance for applications with a balanced workload. In addition, static partitioning determines the mapping from object regions to data servers at object create/open time. No additional metadata management is required.

---------------------------------------------
Dynamic Object Region Mappings
---------------------------------------------

For applications that access a subset of regions for different objects, some data servers can stay idle while the rest are busy fetching or storing data for these regions concentrated around coordinates of interest. Dynamic object partitioning allows metadata servers to balance data server workloads in runtime. The mapping from object regions to the data server is determined at the time of starting region transfer request time.
Partitioning object regions dynamically increases the complexity of metadata management. For example, a read from one client 0 after a write from another client 1 on overlapping regions demands metadata support. Client 0 has to locate the data server to which client 1 writes the region data using information from the metadata server. As a result, metadata servers must maintain up-to-date metadata of the objects they manage. There are a few options we can implement this feature.

*Option 1*: When a client attempts to modify object regions, the client can also send the metadata of this transfer request to the metadata server. Consequently, the metadata server serving for the modified objects always has the most up-to-date metadata.  

Advantage: No need to perform communications between the servers (current strategy)
Disadvantage: The metadata server can be a bottleneck because the number of clients accessing the server may scale up quickly.

*Option 2*: When a data server receives region transfer requests from any client, the data server forwards the corresponding metadata to the metadata server of the object.

Advantage: The number of servers is less than the number of clients, so we are reducing the chance of communication contention
Disadvantage: Server-to-server RPC infrastructures need to be put in place.

*Option 3*: Similar to Option 2, but the data server will modify a metadata file. Later, a metadata server always checks the metadata file for metadata information updates.

Advantage: No communications are required if a metadata file is used.
Disadvantage: Reading metadata files may take some time. If multiple servers are modifying the same metadata file, how should we proceed?

The following table summarizes the communication of the three mapping methods from clients to types of PDC servers when different PDC functions are called.

+-------------------------------+---------------------------------------------+---------------------------------------------------+---------------------------------------------------+
|                               | Static Object Mapping                       | Dynamic Object Mapping & Static Region Mapping    | Dynamic Object Mapping & Dynamic Region Mapping   |
+===============================+=============================================+===================================================+===================================================+
| ``PDC_obj_create``            | Client - Metadata Server                    | Client - Metadata Server                          | Client - Metadata Server                          |
+-------------------------------+---------------------------------------------+---------------------------------------------------+---------------------------------------------------+
| ``PDC_obj_open``              | Client - Metadata Server                    | Client - Metadata Server                          | Client - Metadata Server                          |
+-------------------------------+---------------------------------------------+---------------------------------------------------+---------------------------------------------------+
| ``PDC_region_transfer_start`` | Client - Data Server                        | Client - Data Server                              | Client - Data Server                              |
+-------------------------------+---------------------------------------------+---------------------------------------------------+---------------------------------------------------+
| ``PDC_region_transfer_start`` | Client - Data Server                        | Client - Data Server                              | Client - Metadata Server (Option 1)               |
+-------------------------------+---------------------------------------------+---------------------------------------------------+---------------------------------------------------+
| ``PDC_region_transfer_start`` | Client - Data Server                        | Client - Data Server                              | Data Server - Metadata Server (Option 2)          |
+-------------------------------+---------------------------------------------+---------------------------------------------------+---------------------------------------------------+
| ``PDC_region_transfer_wait``  | Data Server - Client (PDC_READ)             | Data Server - Client (PDC_READ)                   | Data Server - Client (PDC_READ)                   |
+-------------------------------+---------------------------------------------+---------------------------------------------------+---------------------------------------------------+


---------------------------------------------
Region Transfer Request at Client
---------------------------------------------
!!!!!

This section describes how the region transfer request module in PDC works. The region transfer request module is the core of PDC I/O. From the client's point of view, some data is written to regions of objects through transfer request APIs. PDC region transfer request module arranges how data is transferred from clients to servers and how data is stored at servers. 

PDC region: A PDC object abstracts a multi-dimensional array. The current implementation supports up to 3D. A PDC region can be used to access a subarray of the object. A PDC region describes the offsets and lengths to access a multi-dimensional array. Its prototype for creation is ``PDCregion_create(psize_t ndims, uint64_t *offset, uint64_t *size)``. The input values to this create function will be copied into PDC internal memories, so it is safe to free the pointers later.

Region Transfer Request Create and Close
---------------------------------------------

Region transfer request create function has prototype ``PDCregion_transfer_create(void *buf, pdc_access_t access_type, pdcid_t obj_id, pdcid_t local_reg, pdcid_t remote_reg)``. The function takes a contiguous data buffer as input. Content in this data buffer will be stored in the region described by ``remote_reg`` for objects with ``obj_id``. Therefore, ``remote_reg`` has to be contained in the dimension boundaries of the object. The transfer request create function copies the region information into the transfer request's memory, so it is safe to immediately close both ``local_reg`` and ``remote_reg`` after the create function is called.

``local_reg`` describes the shape of the data buffer, aligning to the object's dimensions. For example, if ``"local_reg`` is a 1D region, the start index of the buf to be stored begins at the ``offset[0]`` of the ``local_reg``, with a size of ``size[0]``. Recall that ``offset`` and ``size`` are the input argument. If ``local_reg`` has dimensions larger than 1, then the shape of the data buffer is a subarray described by ``local_reg`` that aligns with the boundaries of object dimensions. In summary, ``local_reg`` is analogous to HDF5's memory space. ``remote_reg`` is parallel to HDF5's data space for data set I/O operations.

``PDCregion_transfer_close(pdcid_t transfer_request_id)`` is used to clean up the internal memories associated with the ``"transfer_request_id``.

Both create and close functions are local memory operations, so no mercury modules will be involved.

Region Transfer Request Start
---------------------------------------------

Starting a region transfer request function will trigger the I/O operation. Data will be transferred from client to server using the        ``pdc_client_connect`` module. ``pdc_client_connect`` module is a middleware layer that transfers client data to a designated server and triggers a corresponding RPC at the server side. In addition, the RPC transfer also allows data transfer by argument. Variables transferred by argument are fixed-sized. For variable-sized variables, mercury bulk transfer is used to transfer a contiguous memory buffer. Region transfer request start APIs: To transfer metadata and data with the pdc_client_connect module, the ``region_transfer_request.c`` file contains mechanisms to wrap request data into a contiguous buffer. There are two ways to start a transfer request. The first prototype is ``PDCregion_transfer_start(pdcid_t transfer_request_id)``. This function starts a single transfer request specified by its ID. The second way is to use the aggregated prototype ``PDCregion_transfer_start_all(pdcid_t *transfer_request_id, int size)``. This function can start multiple transfer requests. It is recommended to use the aggregated version when multiple requests can start together because it allows both client and server to aggregate the requests and achieve better performance.

For the 1D local region, ``PDCregion_transfer_start`` passes the pointer pointing to the ``offset[0] * unit`` location of the input buffer to the ``pdc_client_connect`` module. User data will be copied to a new contiguous buffer for higher dimensions using subregion copy based on local region shape. This implementation is in the static function ``pack_region_buffer``. The new memory buffer will be passed to the ``pdc_client_conenct`` module.

This memory buffer passed to the ``pdc_client_connect`` module is registered with mercury bulk transfer. If it is a read operation, the bulk transfer is a pull operation. Otherwise, it is a push operation. Remote region information and some other relevant metadata are transferred using mercury RPC arguments. Once the ``pdc_client_connect`` module receives a return code and remote transfer request ID from the designated data server, ``PDCregion_transfer_start`` will cache the remote transfer request ID and exit.

``PDCregion_transfer_start`` can be interpreted as ``PDCregion_transfer_start_all`` with the size argument set to 1, though the implementation is optimized. ``PDCregion_transfer_start_all`` performs aggregation of mercury bulk transfer whenever it is possible. Firstly, the function splits the read and write requests. Write requests are handled before the read requests. Wrapping region transfer requests to internal transfer packages: For each of the write requests, it is converted into one or more instances of the structure described by ``pdc_transfer_request_start_all_pkg`` defined in ``pdc_region_transfer.c``. This structure contains the data buffer to be transferred, remote region shapes, and a data server rank to be transferred to. ``PDCregion_transfer_start_all`` implements the package translation in the static function ``prepare_start_all_requests``.

As mentioned earlier in the metadata implementation, an abstract region for an object can be partitioned in different ways. There are four types of partitions: Object static partitioning, region static partitioning, region dynamic partitioning, and node-local region placement. ``PDCprop_set_obj_transfer_region_type(pdcid_t obj_prop, pdc_region_partition_t region_partition)`` allows users to set the partition method before creating an object on the client side. Different partitioning strategies have differences in the target data server rank when a transfer request is packed into ``pdc_transfer_request_start_all_pkg`` (s). We describe them separately.

For the object static partitioning strategy, the input transfer request is directly packed into ``pdc_transfer_request_start_all_pkg`` using a one-to-one mapping. The data server rank is determined at the object create/open time.

For dynamic region partitioning or node-local placement, the static function ``static perr_t register_metadata`` (in ``pdc_region_transfer.c``) contacts the metadata server. The metadata server dynamically selects a data server for the input region transfer request based on the current system status. If local region placement is selected, metadata servers choose the data server on the same node (or as close as possible) of the client rank that transferred this request. If dynamic region partitioning is selected, the metadata server picks the data server currently holding the minimum number of bytes of data. The metadata server holds the region to data server mapping in its metadata region query system ``pdc_server_region_transfer_metadata_query.c``. Metadata held by this module will be permanently stored in the file system as part of the metadata checkpoint file at the PDC server close time. After retrieving the correct data server ID, one ``pdc_transfer_request_start_all_pkg`` is created. The only difference in creating ``pdc_transfer_request_start_all_pkg`` compared with the object static partitioning strategy is how the data server ID is retrieved.

For the static region partitioning strategy, a region is equally partitioned across all data servers. As a result, one region transfer request generates the number of ``pdc_transfer_request_start_all_pkg`` equal to the total number of PDC servers. This implementation is in the static function ``static_region_partition`` in the ``pdc_region_transfer_request.c`` file.

Sending internal transfer request packages from client to server: For an aggregated region transfer request start all function call, two arrays of ``pdc_transfer_request_start_all_pkg`` are created as described in the previous subsection depending on the partitioning strategies. One is for ``PDC_WRITE``, and the other is for ``PDC_READ``. This section describes how ``pdc_region_transfer_request.c`` implements the communication from client to transfer. The core implementation is in the static function ``PDC_Client_start_all_requests``.

Firstly, an array of ``pdc_transfer_request_start_all_pkg`` is sorted based on the target data server ID. Then, dor adjacent ``pdc_transfer_request_start_all_pkg`` that sends to the same data server ID, these packages are packed into a single contiguous memory buffer using the static function ``PDC_Client_pack_all_requests``. This memory buffer is passed to the ``pdc_client_connect`` layer for mercury transfer.

Region transfer request wait: Region transfer request start does not guarantee the finish of data communication or I/O at the server by default. To make sure the input memory buffer is reusable or deletable, a wait function can be used. The wait function is also called implicitly when the object is closed, or special POSIX semantics is set ahead of time when the object is created.

Region Transfer Request Wait
---------------------------------------------

Similar to the start case, the wait API has single and aggregated versions ``PDCregion_transfer_start`` and ``PDCregion_transfer_start_all``. It is possible to wait for more than one request using the aggregated version.

The implementation of the wait all operation is similar to the implementation of the start all request. Firstly, packages defined by the structure ``PDCregion_transfer_wait_all`` are created. ``PDCregion_transfer_wait_all`` only contains the remote region transfer request ID and data server ID. These packages are sorted based on the data server ID. Region transfer requests to the same data server are packed into a contiguous buffer and sent through the PDC client connect module.

Region transfer request wait client control: As mentioned earlier, the region transfer request start all function packs many data packages into the same contiguous buffer, and passes this buffer to the PDC client connect layer for mercury transfer. This buffer can be shared by more than one region transfer request. This buffer can only be freed once wait operations are called on all these requests (not necessarily in a single wait operation call).

When a wait operation is called on a subset of these requests, we reduce the reference counter of the buffer. This reference counter is a pointer stored by the structure ``pdc_transfer_request``. In terms of implementation, ``pdc_transfer_request`` stores an array of reference counter pointers and an array of data buffer pointers. Both arrays have the same size, forming a one-to-one mapping. Each of the data buffer pointers points to an aggregated memory buffer that this region transfer request packs some of its metadata/data into. When the aggregated buffer is created, the corresponding reference counter is set to be the number of region transfer requests that store the reference counter/data buffer pointers. As a result, when all of these region transfer requests have waited, the reference counter becomes zero, and the data buffer can be freed.

---------------------------------------------
Region Transfer Request at Server
---------------------------------------------

The region transfer request module at the server side is implemented in the ``server/pdc_server_region`` directory. This section describes how a data server is implemented at the server side.

Server Region Transfer Request RPC
---------------------------------------------

At the PDC server side, ``pdc_client_server_common.c`` contains all the RPCs' entrances from client calls. ``pdc_server_region_request_handler.h`` contains all the RPCs' related to region transfer requests. The source code is directly included in the ``pdc_client_server_common.c``. ``HG_TEST_RPC_CB(transfer_request, handle)`` and ``HG_TEST_RPC_CB(transfer_request_all, handle)`` are the server RPCs for region transfer request start and region transfer request start all functions called at the client side. ``HG_TEST_RPC_CB(transfer_request_wait, handle)`` and ``HG_TEST_RPC_CB(transfer_request_wait_all, handle)`` are the server RPCs for region transfer request wait and region transfer request wait all.

All functions containing ``cb`` at the end refer to the mercury bulk transfer callback functions. Mercury bulk transfer is used for transferring variable-sized data from client to server. The bulk transfer argument is passed through mercury RPC augment when server RPC is triggered. This argument is used by ``HG_Bulk_create`` and ``HG_Bulk_transfer`` to initiate data transfer from client to server. Once the transfer is finished, the mercury bulk transfer function triggers the call back function (one of the arguments passed to ``HG_Bulk_transfer``) and processes the data received (or sent to the client).

Server Nonblocking Control
---------------------------------------------

By design, the region transfer request start does not guarantee the finish of data transfer or server I/O. In fact, this function should return to the application as soon as possible. Data transfer and server I/O can occur in the background so that client applications can take advantage of overlapping timings between application computations and PDC data management.

Server Region Transfer Request Start
---------------------------------------------

When server RPC for region transfer request start is triggered, it immediately starts the bulk transfer by calling the mercury bulk transfer functions.
In addition, the region transfer request received by the data server triggers a register function ``PDC_transfer_request_id_register`` implemented ``pdc_server_region_transfer.c``. This function returns a unique remote region transfer ID. This remote ID is returned to the client for future reference, so the wait operation can know which region transfer request should be finished on the data server side.

Then, ``PDC_commit_request`` is called for request registration. This operation pushes the metadata for the region transfer request to the end of the data server's linked list for temporary storage.

Finally, the server RPC returns a finished code to the client so that the client can return to the application immediately.

Server Region Transfer Request Wait
---------------------------------------------

The request wait RPC on the server side receives a client's remote region transfer request ID. The RPC returns to the client when this request's local data server I/O is finished.

The implementation uses the ``PDC_check_request`` function in the ``pdc_server_region_transfer.c`` file. This function returns two possibilities. One possible return value is ``PDC_TRANSFER_STATUS_COMPLETE``. In this case, the wait function can immediately return to the client. Another possibility is ``PDC_TRANSFER_STATUS_PENDING``. This flag means that the local server I/O has not finished yet, so this RPC function will not return to the client. Instead, the mercury handle is binded to the structure ``pdc_transfer_request_status`` (defined in ``pdc_server_region_transfer.h``) that stores the metadata of the region transfer request (search by its ID) within the function ``PDC_check_request``.

When the region transfer request callback function for this region transfer is triggered, and the I/O operations are finished, the callback function calls ``PDC_finish_request`` to trigger the return of the wait mercury handle binded to the region transfer request. If a mercury handler is not found, ``PDC_finish_request`` sets the flag of ``pdc_transfer_request_status`` for the region transfer request to be ``PDC_TRANSFER_STATUS_COMPLETE``, so a wait request called later can immediately return as described before. Server region transfer request aggregated mode: the server acquired a contiguous memory buffer through mercury bulk transfer for aggregated region transfer request start and wait calls. This contiguous memory buffer contains packed request metadata/data from the client side. These requests are parsed. For each of the requests, we process them one at a time. The processing method is described in the previous section.

---------------------------------------------
Server Region Storage
---------------------------------------------

PDC is a data management library. I/O is part of its service. Therefore, I/O operation is critical for data persistence. The function ``PDC_Server_transfer_request_io`` in the ``pdc_server_region_transfer.c`` file implements the core I/O function. There are two I/O modes for PDC.
In general, one PDC object is stored in one file per data server.

Storage by File Offset
---------------------------------------------

I/O by file only works for objects with fixed dimensions. Clients are not allowed to modify object dimensions by any means. When a region is written to an object, the region is translated into arrays of offsets and offset lengths based on the region shape using list I/O. Therefore, a region has fixed offsets to be placed on the file.

Storage by Region
---------------------------------------------

I/O by region is a special feature of the PDC I/O management system. Writing a region to an object will append the region to the end of a file. If the same region is read back again sometime later, it only takes a single POSIX ``lseek`` and I/O operation to complete either write or read.

However, when a new region is written to an object, it is necessary to scan all the previously written regions to check for overlapping. The overlapping areas must be updated accordingly. If the new region is fully contained in any previously stored regions, it is unnecessary to append it to the end of the file.

I/O by region will store repeated bytes when write requests contain overlapping parts. In addition, the region update mechanism generates extra I/O operations. This is one of its disadvantages. Optimization for region search (as R trees) in the future can relieve this problem.

+++++++++++++++++++++++++++++++++++++++++++++
Contributing to PDC project
+++++++++++++++++++++++++++++++++++++++++++++

In this section, we will offer you some helpful technical guidance on how to contribute to the PDC project. These 'HowTos' will help you when implementing new features or fixing bugs.


---------------------------------------------
How to set up code formatter for PDC on Mac?
---------------------------------------------

1. PDC project uses clang-format v10 for code formatting and style check.
    1. However, on MacOS, the only available clang-format versions are v8 and v11 if you try to install it via Homebrew. 
    2. To install v10, you need to download it from: https://releases.llvm.org/download.html (https://github.com/llvm/llvm-project/releases/download/llvmorg-10.0.1/llvm-project-10.0.1.tar.xz) 
    3. Then follow instruction here to install clang-format: https://clang.llvm.org/get_started.html. I would suggest you do the following (suppose if you already have homebrew installed)
    
    .. code-block:: Bash
        cd $LLVM_SRC_ROOT
        mkdir build
        cd build
        cmake -G 'Unix Makefiles' -DCMAKE_INSTALL_PREFIX=/opt/llvm/v10 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DLLVM_ENABLE_PROJECTS=clang ../llvm
        make -j 128 
        sudo make install
        sudo ln -s /opt/llvm/v10/bin/clang-format /opt/homebrew/bin/clang-format-v10
    
    
    1. To format all your source code, do the following
    
    .. code-block:: Bash
        cd pdc
        clang-format-v10 -i -style=file src/*
        find src -iname *.h -o -iname *.c | xargs clang-format-v10 -i -style=file
    
    
    1. You can also configure clang-format to be your default C/C++ formatting tool in VSCode, and the automatic code formatter is really convenient to use. 

---------------------------------------------
How to implement an RPC?
---------------------------------------------

This section covers how to implement a simple RPC from client to server. If you call an RPC on the client side, the server should be able to get the argument you passed from the client and execute the corresponding server RPC function.

A concrete example is ``PDC_region_transfer_wait_all``. Mercury transfers at the client side are implemented in ``pdc_client_connect.c``. The name of the function we are using in this example is ``transfer_request_wait_all``. For each component mentioned next, replace ``transfer_request_wait_all`` with your function name. This section will not discuss the design of ``transfer_request_wait_all`` but rather point out where the Mercury components are and how they interact.

Firstly, in ``pdc_client_connect.c``, search for ``transfer_request_wait_all_register_id_g``. Create another variable by replacing ``transfer_request_wait_all`` with your function name. Secondly, search for ``client_send_transfer_request_wait_all_rpc_cb``, and do the same text copy and replacement. This is the callback function on the client side when the RPC is finished on the server side. For most cases, this function loads the server return arguments to a structure and returns the values to the client RPC function. There is also some error checking. Then, search for ``PDC_transfer_request_wait_all_register(*hg_class)`` and ``PDC_Client_transfer_request_wait_all``, and do text copy and replacement for both. This function is the entry point of the mercury RPC call. It contains argument loading, which has the variable name ``in``' This RPC creates a mercury bulk transfer inside it. ``HG_Create`` and ``HG_Bulk_create`` are unnecessary if your mercury transfer does not transfer variable-sized data. ``HG_Forward`` has an argument ``client_send_transfer_request_wait_all_rpc_cb``. The return values from the callback function are placed in ``transfer_args``.

In file ``pdc_client_connect.h``, search for ``_pdc_transfer_request_wait_all_args``, do the text copy and replacement. This structure is the structure for returning values from client call back function ``client_send_transfer_request_wait_all_rpc_cb`` to client RPC function ``PDC_Client_transfer_request_wait_all``. For most cases, an error code is sufficient. For other cases, like creating some object IDs, you must define the structure accordingly. Do not forget to load data in ``_pdc_transfer_request_wait_all_args``. Search for ``PDC_Client_transfer_request_wait_all``, and make sure you register your client connect entry function in the same way.

In file ``pdc_server.c``, search for ``PDC_transfer_request_wait_all_register(hg_class_g);``, make a copy, and replace the ``transfer_request_wait_all`` part with your function name (your function name has to be defined and used consistently throughout all these copy and replacement).
In the file ``pdc_client_server_common.h``, search for ``typedef struct transfer_request_wait_all_in_t``. This is the structure used by a client passing its argument to the server side. You can define whatever you want that is fixed-sized inside this structure. If you have variable-sized data, it can be passed through mercury bulk transfer. The handle is ``hg_bulk_t local_bulk_handle``. ``typedef struct transfer_request_wait_all_out_t`` is the return argument from the server to the client after the server RPC is finished. Next, search for ``hg_proc_transfer_request_wait_all_in_t``. This function defines how arguments are transferred through mercury. 
Similarly, ``hg_proc_transfer_request_wait_all_in_t`` is the other way around. Next, search for ``struct transfer_request_wait_all_local_bulk_args``. This structure is useful when a bulk transfer is used. Using this function, the server passes its variables from the RPC call to the bulk transfer callback function. Finally, search for ``PDC_transfer_request_wait_all_register``. For all these structures and functions, you should copy and replace ``transfer_request_wait_all`` with your own function name.

In file ``pdc_client_server_common.c``, search for ``PDC_FUNC_DECLARE_REGISTER(transfer_request_wait_all)`` and ``HG_TEST_THREAD_CB(transfer_request_wait_all)``, do text copy and function name replacement. ``pdc_server_region_request_handler.h`` is included directly in ``pdc_client_server_common.c``. The server RPC of ``transfer_request_wait_all`` is implemented in ``pdc_server_region_request_handler.h``. However, it is possible to put it directly in the ``pdc_client_server_common.c``. 

Let us open ``pdc_server_region_request_handler.h``. Search for ``HG_TEST_RPC_CB(transfer_request_wait_all, handle)``. This function is the entry point for the server RPC function call. ``transfer_request_wait_all_in_t`` contains the arguments you loaded previously from the client side. If you want to add more arguments, return to ``pdc_client_server_common.h`` and modify it correctly. ``HG_Bulk_create`` and ``HG_Bulk_transfer`` are the mercury bulk function calls. When the bulk transfer is finished, ``transfer_request_wait_all_bulk_transfer_cb`` is called.

After a walk-through of ``transfer_request_wait_all``, you should have learned where different components of a mercury RPC should be placed and how they interact with each other. You can trace other RPC by searching their function names. If you miss things that are not optional, the program will likely hang there forever or run into segmentation faults.


---------------------------------------------
Julia Support for tests
---------------------------------------------
Currently, we add all Julia helper functions to `src/tests/helper/JuliaHelper.jl`

Once you implement your own Julia function, you can use the bridging functions (named with prefix `run_jl_*`) defined in `src/tests/helper/include/julia_helper_loader.h` to call your Julia functions. If the current bridging functions are not sufficient for interacting with your Julia functions, you can add your own bridging functions in `src/tests/helper/include/julia_helper_loader.h` and implement it in `src/tests/helper/include/julia_helper_loader.c`.

When calling your bridging functions, the best example you can follow is `src/tests/dart_attr_dist_test.c`. 

Remember, you must include all your bridging function calls inside the following code blocks, so that the process can have its own Julia runtime loaded. 

.. code-block:: C
    jl_module_list_t modules = {.julia_modules = (char *[]){JULIA_HELPER_NAME}, .num_modules = 1};
    init_julia(&modules);
    ......
    ... call your bridging functions
    ......
    close_julia();

Also, to make sure your code with Julia function calls doesn't get compiled when Julia support is not there, you can add your new test to the list of `ENHANCED_PROGRAMS` in `src/tests/CMakeLists.txt`.

For more info on embedded Julia support, please visit: `Embedded Julia https://docs.julialang.org/en/v1/manual/embedding/`_.