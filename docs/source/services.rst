.. _services:

**3.** Services
===============

**3.1.** Creating Containers and Objects
----------------------------------------

How to use an API for the creation of objects / walkthrough 
		
To perform region transfers within PDC, you must first initialize the PDC layer, create a container and at least one data object.

1. Initialize PDC 

pdcid_t pdc = PDCinit("pdc");

2. Create Container Property

pdcid_t cont_prop = PDCprop_create(PDC_CONT_CREATE);

3. Create Container

pdcid_t cont = PDCcont_create("my_container", cont_prop);

4. Create Object Property

pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE);
PDCprop_set_obj_type(obj_prop, PDC_OBJ_TYPE_USER);
PDCprop_set_obj_buf(obj_prop, data_buffer);
PDCprop_set_obj_dims(obj_prop, 1, &dim_size);  // 1D example

5. Create Object

pdcid_t obj = PDCobj_create("my_object", cont, obj_prop);

Associating properties with objects 

Properties allow fine-tuning of these behaviors: 
        Data type: PDC_INT, PDC_FLOAT, PDC_DOUBLE
        Dimensions: Number and size of dimensions (1D, 2D, etc.)
        Layout: Linear or strided memory layouts
        Buffer: Memory pointer for reading/writing


These are set with functions like:
        PDCprop_set_obj_type();
        PDCprop_set_obj_dims();
        PDCprop_set_obj_time_step();


**3.2.** Writing and Reading Data
---------------------------------

Setup of Region 

Define the memory region and the object region (the logical data range within the object).
PDCid_t region = PDCregion_create(offset, ndim, dims);

Example:

uint64_t offset[1] = {0};

uint64_t dims[1] = {100};

PDCid_t reg = PDCregion_create(1, offset, dims);

Writing to / Reading from regions:

Map buffer to object region:

PDCbuf_obj_map(data, obj, reg, reg, PDC_WRITE, 0);

Start data transfer:

PDCid_t transfer = PDCregion_transfer_create(data, PDC_WRITE, obj, reg, reg);

PDCregion_transfer_start(transfer);

Handling transfer completion ****


**3.3.** Data Types and Layout
------------------------------

Supported data types:
PDC_INT, PDC_UINT
PDC_FLOAT, PDC_DOUBLE
PDC_CHAR, PDC_STRING

Memory Alignment 
For optimal performance, use aligned memory buffers (e.g., 64-byte aligned). PDC does not enforce alignment, but HPC systems benefit from it.

Different Layout Types

Contiguous (default): linear arrays
Strided: spaced elements, multidimensional data
Chunked: block-based layout (planned)


**3.4.** Querying 
-----------------

PDC supports metadata-based querying to locate objects and regions based on user-defined or system-defined attributes. This enables applications to discover data dynamically without prior knowledge of object identifiers.

Creating a Query:
Use functions like PDCquery_create() to define criteria (e.g., object name, data type, tags, dimension bounds).

Combining Queries:
Logical operators (AND, OR) can be used with PDCquery_combine() to build complex query expressions.

Executing a Query:
Results are retrieved via PDCquery_get_next_obj() or similar functions, returning matching object IDs for further interaction.

Use Cases

* Find all objects tagged with a specific simulation timestep
* Retrieve containers created by a specific user
* Select objects within a given spatial or temporal range

This querying model supports scalable, metadata-driven workflows in HPC applications.

**3.5.** Transform and Analysis
-------------------------------

You can apply transformations during transfer (planned features) with compression, filtering, or derived field calculation. 
In the current implementation, use the application-side transform and re-bind memory buffers before writing.

**3.6.** Data Movement
----------------------


PDC abstracts and automates data movement in the following ways: 
    User specifies memory and object regions
    Transfer engines handle movement
    Async transfers allow overlapping compute and I/O.


**3.7.** Metadata Management
----------------------------

Metadata is central to PDC's object model. Each object, container, and region can have associated metadata that describes its structure, access permissions, semantic tags, and custom user-defined fields.

Adding Metadata:
Use functions such as PDCprop_set_*() and PDCobj_set_tag() to attach metadata during object or container creation. User-defined fields like application tags and timestamps can also be added.

Querying Metadata:
PDC supports querying metadata via specific fields. You can search for objects or containers matching certain tags, dimensions, or timestamps using PDCquery_create() and PDCquery_get_next_obj().

Updating Metadata:
Metadata properties can be modified using update APIs, allowing dynamic management (e.g., updating access counts, modifying tags). Note: not all properties are mutable after object creation.

Efficient metadata handling ensures scalable object discovery and enables analytics or visualization workflows to leverage semantic information.