.. _services:

**3.** Services
===============

**3.1.** Creating Containers and Objects
----------------------------------------

**3.2.** Writing and Reading Data
---------------------------------

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
