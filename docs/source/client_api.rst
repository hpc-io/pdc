.. _client_api:

**5.** Client API
=================

This section documents the main Client API for PDC. 
It includes the types, core layer functions, properties, containers, 
objects, and region management functions. Use the links below to 
quickly navigate to each subsection:

- :ref:`client_api_types` - Types used in the Client API
- :ref:`client_api_layer` - Initialization and shutdown functions for the PDC layer
- :ref:`client_api_properties` - Functions for creating and closing properties
- :ref:`client_api_containers` - Functions for creating and managing containers
- :ref:`client_api_objects` - Functions for creating and managing objects
- :ref:`client_api_object_tags` - Functions for creating and managing object tags
- :ref:`client_api_regions` - Functions for creating and managing regions
- :ref:`client_api_object_data_transfers` - Functions for object data transfers

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

.. doxygenfunction:: PDCcont_create_col
   :project: PDC

.. doxygenfunction:: PDCcont_open
   :project: PDC

.. doxygenfunction:: PDCcont_open_col
   :project: PDC

.. doxygenfunction:: PDCcont_close
   :project: PDC

.. doxygenfunction:: PDCcont_persist
   :project: PDC

.. _client_api_objects:

**5.5.** Objects
----------------

.. doxygenfunction:: PDCobj_create
   :project: PDC

.. doxygenfunction:: PDCobj_create_mpi
   :project: PDC

.. doxygenfunction:: PDCobj_open
   :project: PDC

.. doxygenfunction:: PDCobj_open_col
   :project: PDC

.. doxygenfunction:: PDCobj_close
   :project: PDC

.. doxygenfunction:: PDCobj_get_info
   :project: PDC

.. doxygenfunction:: PDCprop_obj_dup
   :project: PDC

.. doxygenfunction:: PDCprop_set_obj_type
   :project: PDC

.. doxygenfunction:: PDCprop_set_obj_dims
   :project: PDC

.. doxygenfunction:: PDCprop_set_obj_user_id
   :project: PDC

.. doxygenfunction:: PDCprop_set_obj_time_step
   :project: PDC

.. doxygenfunction:: PDCprop_set_obj_app_name
   :project: PDC

.. doxygenfunction:: PDCprop_set_obj_tags
   :project: PDC

.. doxygenfunction:: PDCprop_set_obj_transfer_region_type
   :project: PDC

.. doxygenfunction:: PDCobj_iter_start
   :project: PDC

.. doxygenfunction:: PDCobj_iter_null
   :project: PDC

.. doxygenfunction:: PDCobj_iter_get_info
   :project: PDC

.. doxygenfunction:: PDCobj_iter_next
   :project: PDC

.. _client_api_object_tags:

**5.6.** Object Tags
--------------------

.. doxygenfunction:: PDCprop_set_obj_tags
   :project: PDC

.. doxygenfunction:: PDCobj_put_tag
   :project: PDC

.. doxygenfunction:: PDCobj_get_tag
   :project: PDC
.. _client_api_regions:

**5.7.** Regions
----------------

.. doxygenfunction:: PDCregion_create
   :project: PDC

.. doxygenfunction:: PDCregion_close
   :project: PDC

.. _client_api_object_data_transfers:

**5.8.** Object Data Transfers
------------------------------

.. doxygenfunction:: PDCregion_transfer_start
   :project: PDC

.. doxygenfunction:: PDCregion_transfer_wait
   :project: PDC

.. doxygenfunction:: PDCregion_transfer_close
   :project: PDC

.. doxygenfunction:: PDCregion_transfer_start_all
   :project: PDC

.. doxygenfunction:: PDCregion_transfer_wait_all
   :project: PDC

.. doxygenfunction:: PDCobj_put_data
   :project: PDC

.. doxygenfunction:: PDCobj_get_data
   :project: PDC