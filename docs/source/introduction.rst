.. _introduction:

**1.** Introduction
===================

**1.1.** What is PDC
--------------------

Proactive Data Containers (PDC) software provides an object-focused data 
management API, a runtime system with a set of scalable data object 
management services, and tools for managing data objects stored in the PDC 
system. The PDC API allows efficient and transparent data movement in complex 
memory and storage hierarchy. The PDC runtime system performs data movement 
asynchronously and provides scalable metadata operations to find and 
manipulate data objects.

PDC revolutionizes how data is managed and accessed by using object-centric 
abstractions to represent data that moves in the high-performance computing (HPC) 
memory and storage subsystems. PDC manages extensive metadata to describe data 
objects to find desired data efficiently as well as to store information in the data objects.

More information and publications about PDC are available at https://sdm.lbl.gov/pdc.

If you use PDC in your research, please cite the following:

Byna, Suren, Dong, Bin, Tang, Houjun, Koziol, Quincey, Mu, Jingqing, 
Soumagne, Jerome, Vishwanath, Venkat, Warren, Richard, and Tessier, François. 
*Proactive Data Containers (PDC) v0.1*. Computer Software. https://github.com/hpc-io/pdc. 
USDOE. 11 May. 2017. Web. doi:`10.11578/dc.20210325.1 <https://doi.org/10.11578/dc.20210325.1>`_

Key Features
~~~~~~~~~~~~

The key features of PDC include object-based data abstractions, scalable 
metadata indexing, asynchronous I/O, integration with MPI, and support for complex storage hierarchies. 

Key Benefits
~~~~~~~~~~~~

The key benefits of PDC include reducing issues and bottlenecks with I/O,  
optimizing performance in key distributive systems, enhancing scalability and 
flexibility for HPC applications, and simplifying overall data management, movement, and access. 

Comparison with Standardized Data Accessing Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Traditional I/O models like MPI-IO or DeltaV are limited in scalability. 
PDC is able to provide asynchronous, and object-based APIs that separate 
data definition from storage. This makes PDC more scalable and adaptive to data handling.

**1.2.** Installation
---------------------

Prerequisites 
~~~~~~~~~~~~~

Building from Source
~~~~~~~~~~~~~~~~~~~~

Spack Installation
~~~~~~~~~~~~~~~~~~

**1.3.** First PDC Program
--------------------------

This example walks through the essential steps for writing a basic PDC application: 
initializing the PDC layer, creating a container and an object, and performing a 
simple region-based data transfer. It is intended as a starting point for new users.

.. note::

   This example omits detailed error checking for clarity. In practice, always check the return values of PDC API calls. 
   See the section TODO_FIX_REFERENCE for more information on detecting and handling PDC errors.

.. code-block:: c
   :linenos:

   #include <pdc.h>

   int main() {
       // Initialize PDC runtime environment
       pdcid_t pdc_id = PDCinit("pdc");

       // Create container
       pdcid_t cont_id = PDCcont_create(pdc_id, "my_container", PDC_CONT_CREATE_DEFAULT);

       // Define object dimensions and properties
       int region_size = 64;
       uint64_t dims[1] = {region_size};
       pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
       PDCprop_set_obj_type(obj_prop, PDC_INT);
       PDCprop_set_obj_dims(obj_prop, 1, dims);

       // Create object
       pdcid_t obj_id = PDCobj_create(cont_id, "my_object", obj_prop);

       // Prepare data
       int data[64] = {0};

       // Define regions
       uint64_t offset[1] = {0};
       pdcid_t local_region = PDCregion_create(1, offset, dims);
       pdcid_t global_region = PDCregion_create(1, offset, dims);

       // Transfer data
       pdcid_t transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj_id, local_region, global_region);
       PDCregion_transfer_start(transfer_request);
       PDCregion_transfer_wait(transfer_request);

       // Clean up
       PDCregion_transfer_close(transfer_request);
       PDCregion_close(local_region);
       PDCregion_close(global_region);
       PDCobj_close(obj_id);
       PDCcont_close(cont_id);
       PDCclose(pdc_id);

       return 0;
   }

It first initializes the PDC environment and creates a 
container and object with specified properties (lines 7–21). It then 
prepares a data buffer and defines local and global regions representing 
the data range to transfer (lines 23–29). The program performs a region-based 
write transfer of the data to the PDC object, starting and waiting for the 
transfer to complete (lines 31–33). Finally, it cleans up all PDC resources 
by closing the transfer request, regions, object, container, and the 
PDC context itself (lines 35–40). While simplified, this is the typical 
workflow that underlies more advanced PDC programs.

**1.4.** Running the PDC Server and Client Application
------------------------------------------------------