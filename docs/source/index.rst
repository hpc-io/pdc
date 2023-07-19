.. PDC documentation master file, created by
   sphinx-quickstart on Thu Apr 15 14:28:56 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Proactive Data Containers (PDC)
===============================

Proactive Data Containers (PDC) software provides an object-focused data management API, a runtime system with a set of scalable data object management services, and tools for managing data objects stored in the PDC system. The PDC API allows efficient and transparent data movement in complex memory and storage hierarchy. The PDC runtime system performs data movement asynchronously and provides scalable metadata operations to find and manipulate data objects. PDC revolutionizes how data is managed and accessed by using object-centric abstractions to represent data that moves in the high-performance computing (HPC) memory and storage subsystems. PDC manages extensive metadata to describe data objects to find desired data efficiently as well as to store information in the data objects.

PDC API, data types, and developer notes are available in `docs/readme.md  <https://github.com/hpc-io/pdc/blob/kenneth_develop/docs/readme.md>`_

More information and publications of PDC is available at https://sdm.lbl.gov/pdc

If you use PDC in your research, please use the following citation:

Byna, Suren, Dong, Bin, Tang, Houjun, Koziol, Quincey, Mu, Jingqing, Soumagne, Jerome, Vishwanath, Venkat, Warren, Richard, and Tessier, François. Proactive Data Containers (PDC) v0.1. Computer Software. https://github.com/hpc-io/pdc. USDOE. 11 May. 2017. Web. doi:10.11578/dc.20210325.1.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting_started
   .. definitions
   .. assumptions

.. toctree::
   :maxdepth: 2
   :caption: Overview
   
   introduction
   hdf5vol
   .. performance

.. toctree::
   :maxdepth: 2
   :caption: Resources

   hellopdcexample
   api
   tools
   .. inflightanalysis
   .. futurework


.. toctree::
   :maxdepth: 2
   :caption: Developer Documentation

   developer-notes
   documentation/objects
   documentation/regions
   documentation/query
   documentation/analysis
   documentation/transformation


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
