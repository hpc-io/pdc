.. _client_api:

**4.** Client API
=================

This section documents the main Client API for PDC. 
It includes the types, core layer functions, properties, containers, 
objects, and region management functions. Use the links below to 
quickly navigate to each subsection:

- :ref:`client_api_data_types` - Data types used in the Client API.
- :ref:`client_api_layer` - Initialization and shutdown functions for the PDC layer.
- :ref:`client_api_properties` - Functions for creating and closing properties.
- :ref:`client_api_containers` - Functions for creating and managing containers.
- :ref:`client_api_objects` - Functions for creating and managing objects.
- :ref:`client_api_object_tags` - Functions for creating and managing object tags.
- :ref:`client_api_regions` - Functions for creating and managing regions.
- :ref:`client_api_object_data_transfers` - Functions for object data transfers.
- :ref:`client_api_object_data_querying` - Functions for querying object data.

.. _client_api_data_types:

**4.1.** Data Types
-------------------

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

.. doxygenenum:: pdc_consistency_t
   :project: PDC

.. doxygenenum:: pdc_prop_name_t
   :project: PDC

.. doxygenenum:: pdc_query_op_t
   :project: PDC

.. doxygenenum:: pdc_query_combine_op_t
   :project: PDC

.. doxygenenum:: pdc_query_get_op_t
   :project: PDC

.. doxygentypedef:: pdc_selection_t
   :project: PDC

.. doxygentypedef:: pdc_query_constraint_t
   :project: PDC

.. doxygentypedef:: pdc_query_t
   :project: PDC

.. _client_api_layer:

**4.2.** PDC Layer
------------------

.. doxygenfunction:: PDCinit
   :project: PDC

.. doxygenfunction:: PDCclose
   :project: PDC

.. _client_api_properties:

**4.3.** Properties
-------------------

.. doxygenfunction:: PDCprop_create
   :project: PDC

.. doxygenfunction:: PDCprop_close
   :project: PDC

.. _client_api_containers:

**4.4.** Containers
-------------------

A container can hold objects, other containers, or a mix of both, so
hierarchical structures (e.g. an HDF5-style tree of groups and datasets) can
be represented directly. :code:`PDCcont_put_entries`,
:code:`PDCcont_del_entries`, and :code:`PDCcont_get_entries` operate on
typed entries (see :code:`pdc_entity_type_t` and :code:`pdc_cont_entry_t`
below) and are the preferred way to manage a container's contents;
:code:`PDCcont_put_objids`, :code:`PDCcont_del_objids`, and
:code:`PDCcont_get_objids` remain as an object-only convenience layer on
top of them.

A nested entry's id, as returned by :code:`PDCcont_get_entries`, may name
either a container or a plain object, and may never have been opened
locally by name, so its name and kind are not otherwise known to the
calling process. :code:`PDCcont_get` resolves an entry's name from that id
alone regardless of which kind it is (it checks containers first, then
falls back to object metadata). :code:`PDCcont_put_entries`/
:code:`PDCcont_del_entries`/:code:`PDCcont_get_entries` likewise accept such
a raw, never-locally-opened id directly -- there is no need to open an
entry first just to add, remove, or list its contents.

A nested container cannot always be reached by :code:`PDCcont_open`'s name
lookup: a leaf name may repeat under different parents (ambiguous -- which
one is meant is undefined), and even a unique name requires guessing which
server holds it, since a nested container is keyed by a hash scoped with
its parent's id rather than by its bare name. :code:`PDCcont_open_id`
sidesteps both problems by opening a container directly from its meta id
(e.g. one returned by :code:`PDCcont_get_entries`), giving a normal local
handle usable with any other container function.

Every container function above that talks to the server also has an
:code:`_coll` collective counterpart (:code:`PDCcont_get_coll`,
:code:`PDCcont_put_entries_coll`, :code:`PDCcont_del_entries_coll`,
:code:`PDCcont_get_entries_coll`, :code:`PDCcont_create_at_coll`): all ranks
call these together over a shared :code:`comm`, but only rank 0 talks to the
server, with the result broadcast to every rank of :code:`comm` -- the same
pattern as :code:`PDCcont_create_coll` and :code:`PDCcont_open_coll`.
:code:`PDCcont_create_at_coll` is additionally idempotent by name: calling
it again with a child name that already exists directly under the same
parent returns the existing child instead of creating a duplicate.

.. doxygenenum:: pdc_entity_type_t
   :project: PDC

.. doxygenstruct:: pdc_cont_entry_t
   :project: PDC

.. doxygenfunction:: PDCcont_create
   :project: PDC

.. doxygenfunction:: PDCcont_create_coll
   :project: PDC

.. doxygenfunction:: PDCcont_open
   :project: PDC

.. doxygenfunction:: PDCcont_open_col
   :project: PDC

.. doxygenfunction:: PDCcont_open_id
   :project: PDC

.. doxygenfunction:: PDCcont_close
   :project: PDC

.. doxygenfunction:: PDCcont_persist
   :project: PDC

.. doxygenfunction:: PDCcont_create_at
   :project: PDC

.. doxygenfunction:: PDCcont_create_at_coll
   :project: PDC

.. doxygenfunction:: PDCcont_get
   :project: PDC

.. doxygenfunction:: PDCcont_get_coll
   :project: PDC

.. doxygenfunction:: PDCcont_put_entries
   :project: PDC

.. doxygenfunction:: PDCcont_put_entries_coll
   :project: PDC

.. doxygenfunction:: PDCcont_del_entries
   :project: PDC

.. doxygenfunction:: PDCcont_del_entries_coll
   :project: PDC

.. doxygenfunction:: PDCcont_get_entries
   :project: PDC

.. doxygenfunction:: PDCcont_get_entries_coll
   :project: PDC

.. doxygenfunction:: PDCcont_put_objids
   :project: PDC

.. doxygenfunction:: PDCcont_del_objids
   :project: PDC

.. doxygenfunction:: PDCcont_get_objids
   :project: PDC

.. _client_api_objects:

**4.4.** Objects
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

**4.6.** Object Tags
--------------------

.. doxygenfunction:: PDCprop_set_obj_tags
   :project: PDC

.. doxygenfunction:: PDCobj_put_tag
   :project: PDC

.. doxygenfunction:: PDCobj_get_tag
   :project: PDC
.. _client_api_regions:

**4.7.** Regions
----------------

.. doxygenfunction:: PDCregion_create
   :project: PDC

.. doxygenfunction:: PDCregion_close
   :project: PDC

.. _client_api_object_data_transfers:

**4.8.** Object Data Transfers
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

.. _client_api_object_data_querying:

**4.9** Object Data Querying
----------------------------

.. doxygenfunction:: PDCquery_create
   :project: PDC

.. doxygenfunction:: PDCquery_and
   :project: PDC

.. doxygenfunction:: PDCquery_or
   :project: PDC

.. doxygenfunction:: PDCquery_sel_region
   :project: PDC

.. doxygenfunction:: PDCquery_get_selection
   :project: PDC

.. doxygenfunction:: PDCquery_get_data
   :project: PDC

.. doxygenfunction:: PDCquery_get_histogram
   :project: PDC

.. doxygenfunction:: PDCselection_free
   :project: PDC

.. doxygenfunction:: PDCquery_free
   :project: PDC

.. doxygenfunction:: PDCquery_free_all
   :project: PDC

.. doxygenfunction:: PDCquery_print
   :project: PDC

.. doxygenfunction:: PDCselection_print
   :project: PDC

.. doxygenfunction:: PDCselection_print_all
   :project: PDC