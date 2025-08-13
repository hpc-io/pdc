.. _client_api:

**5.** Client API
=================

This section documents the main Client API for PDC. 
It includes the types, core layer functions, properties, containers, 
objects, and region management functions. Use the links below to 
quickly navigate to each subsection:

- :ref:`client_api_types` — Types used in the Client API
- :ref:`client_api_layer` — Initialization and shutdown functions for the PDC layer
- :ref:`client_api_properties` — Functions for creating and closing properties
- :ref:`client_api_containers` — Functions for creating and managing containers
- :ref:`client_api_objects` — Functions for creating and managing objects
- :ref:`client_api_regions` — Functions for creating and managing regions

.. _client_api_types:

**5.1.** Types
--------------

.. doxygentypedef:: pdcid_t
   :project: PDC

.. doxygentypedef:: perr_t
   :project: PDC

.. doxygentypedef:: pdc_var_type_t
   :project: PDC

.. doxygenenum:: pdc_prop_type_t
   :project: PDC

.. doxygenenum:: pdc_region_partition_t
   :project: PDC

.. doxygenenum:: pdc_lifetime_t
   :project: PDC

.. doxygenenum:: pdc_prop_type_t
   :project: PDC

.. doxygenenum:: pdc_consistency_t
   :project: PDC

.. _client_api_layer:

**5.2.** PDC Layer
------------------

.. doxygenfunction:: PDCinit
   :project: PDC

.. doxygenfunction:: PDCclose
   :project: PDC

.. _client_api_properties:

**5.3.** Properties
-------------------

.. doxygenfunction:: PDCprop_create
   :project: PDC

.. doxygenfunction:: PDCprop_close
   :project: PDC

.. _client_api_containers:

**5.4.** Containers
-------------------

.. doxygenfunction:: PDCcont_create
   :project: PDC

.. doxygenfunction:: PDCcont_close
   :project: PDC

.. _client_api_objects:

**5.5.** Objects
----------------

.. doxygenfunction:: PDCobj_create
   :project: PDC

.. doxygenfunction:: PDCobj_close
   :project: PDC

.. doxygenfunction:: PDCprop_obj_dup
   :project: PDC

.. _client_api_regions:

**5.6.** Regions
----------------

.. doxygenfunction:: PDCregion_create
   :project: PDC

.. doxygenfunction:: PDCregion_close
   :project: PDC
