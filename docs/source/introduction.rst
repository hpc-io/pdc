.. _introduction:

Introduction
============

What is PDC
-----------

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

More information and publications about PDC are available at:

  https://sdm.lbl.gov/pdc

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

Installation
------------

Prerequisites 
~~~~~~~~~~~~~

Building from Source
~~~~~~~~~~~~~~~~~~~~

First PDC Program
-----------------

Introductory/Minimal Working Program
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An introductory/minimal working program of PDC will firstly initialize PDC. Then,
the program will move on to create a container and an object. Lastly, the program 
will write and read the data, and then finalize its location.

Walkthrough of Basic Programming Logic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Compiling and Running
~~~~~~~~~~~~~~~~~~~~~