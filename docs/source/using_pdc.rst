.. _using_pdc:

**3.** Using PDC
==================

**3.1.** Overview
-----------------

This section provides a practical overview of how to use the PDC
library to manage and transfer data in high-performance computing environments. 
It walks through the essential steps of initializing PDC, creating containers 
and objects, defining regions, and performing data transfers. 

Basic Usage
~~~~~~~~~~~

- :ref:`3.1. Initializing PDC <initializing-pdc>`
- :ref:`3.2. Container Lifecycle <container-lifecycle>`
- :ref:`3.3. Object Lifecycle <object-lifecycle>`
- :ref:`3.4. Region Transfer Lifecycle <region-lifecycle>`

Complete Examples
~~~~~~~~~~~~~~~~~

- :ref:`Example: 2D Region Transfers <2D-region-transfer>`
- :ref:`Example: 2D Batch Region Transfer <2D-batch-region-transfer>`
- :ref:`Example: Get & Put Object <get-put-object>`
- :ref:`Example: Add & Get KV Tag <add-get-kvtag>`
- :ref:`Example: Querying Object Data <querying-object-data>`

**3.2.** Initializing PDC
-------------------------

.. _initializing-pdc:

Prior to any interaction with PDC, the user needs to initialize it as shown below:

.. code-block:: C

    pdcid_t pdc_id = PDCinit("pdc");

At the end of the application a corresponding deinitialization function should be called:

.. code-block:: C

    PDCclose(pdc_id);

.. note:: 

    Users should check that every PDC API call succeeds.
    In general, if a function returns a ``pdcid_t``, `0` indicates an error.
    If a function returns a ``perr_t``, a negative value indicates an error.


**3.3.** Container Lifecycle
----------------------------

.. _container-lifecycle:

Containers store objects and provide users a way to organize their data.
Before creating a container, a container property must be constructed.
The container property provides users a method for customizing a container's behavior.
For an exhaustive list of container properties, please see FIXME.

This is shown in the example below:

.. code-block:: C

    pdcid_t cont_prop_id = PDCprop_create(PDC_CONT_CREATE, pdc_id);

    // Independent container creation
    pdcid_t cont_id = PDCcont_create("cont", cont_prop_id)

    // Collective container creation
    pdcid_t cont_col_id = PDCcont_create_coll("cont", cont_prop_id, MPI_COMM_WORLD);

To open an existing container:

.. code-block:: C

    pdcid_t cont_id = PDCcont_open("cont");

The following functions should be used to free both the container and its associated property resources:

.. code-block:: C

    PDCprop_close(cont_prop_id);
    PDCcont_close(cont_id);
    PDCcont_close(cont_col_id);

**3.4.** Object Lifecycle
-------------------------

.. _object-lifecycle:

Objects represent user data and are the entities stored within containers in PDC.
Before creating an object, an object property must be defined, which 
specifies metadata such as dimensionality, size, data type, and region partitioning.
For an exhaustive list of object properties, please see FIXME.
This allows for fine-grained control over how data is laid out and accessed.

Below is an example of setting up an object property and creating objects:

.. code-block:: C

    // Create object property
    pdcid_t obj_prop_id = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

    // Set properties: type, dims, etc.
    uint64_t dims[1] = {1024};
    PDCprop_set_obj_dims(obj_prop_id, dims);
    PDCprop_set_obj_type(obj_prop_id, PDC_FLOAT);

    // Independent object creation
    pdcid_t obj_id = PDCobj_create(cont_id, "obj", obj_prop_id);

    // Collective object creation
    pdcid_t obj_col_id = PDCobj_create_coll(cont_id, "obj", obj_prop_id, my_rank, comm);

To open an existing object by name within a container:

.. code-block:: C

    pdcid_t obj_id = PDCobj_open(cont_id, "obj");

When the object and its property are no longer needed, they should be closed to free resources:

.. code-block:: C

    PDCprop_close(obj_prop_id);
    PDCobj_close(obj_id);
    PDCobj_close(obj_col_id);

**3.5.** Region Transfer Lifecycle
----------------------------------

.. _region-lifecycle:

Regions define logical subranges within a PDC object and are used to specify what part of the object's data will be transferred between memory and storage.

Transfers can be performed in three main modes:

- Individually, with ``PDCregion_transfer_start()``
- Collectively, with ``PDCregion_transfer_start_coll()`` across MPI processes
- In batches, with ``PDCregion_transfer_start_all()`` and ``PDCregion_transfer_wait_all()``

Basic Region Transfer
~~~~~~~~~~~~~~~~~~~~~

Create memory and object regions and initiate a transfer:

.. code-block:: C

    uint64_t offset[1] = {0};
    uint64_t size[1] = {1024};

    float *data_buf = malloc(sizeof(float) * size[0]);

    pdcid_t mem_reg_id = PDCregion_create(1, offset, size);
    pdcid_t obj_reg_id = PDCregion_create(1, offset, size);

    pdcid_t xfer = PDCregion_transfer_create(data_buf, PDC_WRITE,
                                             obj_id, obj_reg_id, mem_reg_id);

    PDCregion_transfer_start(xfer);
    PDCregion_transfer_wait(xfer);

    PDCregion_transfer_close(xfer);
    PDCregion_close(mem_reg_id);
    PDCregion_close(obj_reg_id);
    free(data_buf);

Collective Transfer
~~~~~~~~~~~~~~~~~~~

If the transfer is intended to be performed collectively across MPI ranks, use:

.. code-block:: C

    MPI_Comm comm = MPI_COMM_WORLD;
    PDCregion_transfer_start_coll(xfer, comm);

This function should be called by all processes participating in 
the transfer and is useful for coordinated I/O in distributed 
applications. The rest of the transfer workflow (e.g., `PDCregion_transfer_wait()`) 
remains unchanged.

Batch Region Transfer
~~~~~~~~~~~~~~~~~~~~~

For scenarios involving many objects or regions, PDC supports batch transfers to reduce overhead:

.. code-block:: C

    #define OBJ_NUM 10
    #define BUF_LEN 256

    int *data[OBJ_NUM];
    pdcid_t transfer_requests[OBJ_NUM];
    pdcid_t reg = PDCregion_create(1, offset, size);
    pdcid_t reg_global = PDCregion_create(1, offset, size);

    for (int i = 0; i < OBJ_NUM; ++i) {
        data[i] = malloc(sizeof(int) * BUF_LEN);
        for (int j = 0; j < BUF_LEN; ++j)
            data[i][j] = j;

        transfer_requests[i] = PDCregion_transfer_create(
            data[i], PDC_WRITE, obj[i], reg, reg_global);
    }

    // Start all transfers in one batch
    PDCregion_transfer_start_all(transfer_requests, OBJ_NUM);

    // Wait for all to complete
    PDCregion_transfer_wait_all(transfer_requests, OBJ_NUM);

    for (int i = 0; i < OBJ_NUM; ++i) {
        PDCregion_transfer_close(transfer_requests[i]);
        free(data[i]);
    }

    PDCregion_close(reg);
    PDCregion_close(reg_global);

**3.6.** Complete Examples
--------------------------

.. _2D-region-transfer:

2D Region Transfer Example
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C
    :linenos:

        #include <stdio.h>
        #include "pdc.h"

        #define BUF_LEN 128

        int
        main(int argc, char **argv)
        {
            pdcid_t  pdc, cont_prop, cont, obj_prop, memory_region, obj_region;
            pdcid_t  obj;
            char     cont_name[128], obj_name[128];
            pdcid_t  transfer_request;
            int      rank = 0, size = 1, i;
            int      ret_value = 0;
            uint64_t offset[3], offset_length[3];
            uint64_t dims[2];
            int     *data_write = (int *)malloc(sizeof(int) * BUF_LEN);
            int     *data_read  = (int *)malloc(sizeof(int) * BUF_LEN);

        #ifdef ENABLE_MPI
            MPI_Init(&argc, &argv);
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);
        #endif

            // Initialize PDC runtime
            pdc = PDCinit("pdc");

            // Configure and create container
            cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
            sprintf(cont_name, "c%d", rank);
            cont = PDCcont_create(cont_name, cont_prop);

            // Configure and create object
            obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
            PDCprop_set_obj_type(obj_prop, PDC_INT);
            dims[0] = BUF_LEN / 4;
            dims[1] = 4;
            PDCprop_set_obj_dims(obj_prop, 2, dims);
            sprintf(obj_name, "o1_%d", rank);
            obj = PDCobj_create(cont, obj_name, obj_prop);

            // Configure regions
            offset[0]        = 0;
            offset[1]        = 0;
            offset_length[0] = BUF_LEN / 4;
            offset_length[1] = 4;

            // Create local region and object region
            memory_region = PDCregion_create(1, offset, offset_length);
            obj_region    = PDCregion_create(2, offset, offset_length);

            // Initialize memory buffer
            for (i = 0; i < BUF_LEN; ++i)
                data_write[i] = i;

            // Create, start, wait, and close write data transfer
            transfer_request = PDCregion_transfer_create(data_write, PDC_WRITE, obj, memory_region, obj_region);
            PDCregion_transfer_start(transfer_request);
            PDCregion_transfer_wait(transfer_request);
            PDCregion_transfer_close(transfer_request);

            // Create, start, wait, and close read data transfer
            transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj, memory_region, obj_region);
            PDCregion_transfer_start(transfer_request);
            PDCregion_transfer_wait(transfer_request);
            PDCregion_transfer_close(transfer_request);

            // Validate data
            if (memcmp(data_read, data_write, sizeof(int) * BUF_LEN)) {
                printf("Data read was invalid\n");
                ret_value = 1;
            }

            // Close regions
            PDCregion_close(memory_region);
            PDCregion_close(obj_region);

            // Close object
            PDCobj_close(obj);

            // Close container
            PDCcont_close(cont);

            // Close object and container properties
            PDCprop_close(obj_prop);
            PDCprop_close(cont_prop);

            // Close PDC runtime
            PDCclose(pdc);

            // Free memory buffers
            free(data_write);
            free(data_read);

        #ifdef ENABLE_MPI
            MPI_Finalize();
        #endif

            if (ret_value)
                printf("Example had an error\n");
            else
                printf("Example ran successfully\n");

            return ret_value;
        }

.. _2D-batch-region-transfer:

2D Batch Region Transfer Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C
    :linenos:

        #include <stdio.h>
        #include "pdc.h"

        #define BUF_LEN       400
        #define CHUNK_LEN     100
        #define NUM_TRANSFERS (BUF_LEN / CHUNK_LEN)

        int
        main(int argc, char **argv)
        {
            pdcid_t  pdc, cont_prop, cont, obj_prop;
            pdcid_t  obj_id;
            pdcid_t  memory_regions[4], obj_regions[4], transfers[4];
            char     cont_name[128], obj_name[128];
            int      rank = 0, size = 1, i;
            int      ret_value = 0;
            uint64_t dims[2];
            uint64_t offsets[2];
            uint64_t region_size[2];
            int     *data_write;
            int     *data_read;

            // Allocate buffers
            data_write = (int *)malloc(sizeof(int) * BUF_LEN);
            data_read  = (int *)calloc(BUF_LEN, sizeof(int));

        #ifdef ENABLE_MPI
            MPI_Init(&argc, &argv);
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);
        #endif

            // Initialize PDC runtime
            pdc = PDCinit("pdc");

            // Configure and create container
            cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
            sprintf(cont_name, "c%d", rank);
            cont = PDCcont_create(cont_name, cont_prop);

            // Configure and create object
            obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
            dims[0]  = 40; // total height
            dims[1]  = 10; // total width (total = 400 elements)
            PDCprop_set_obj_dims(obj_prop, 2, dims);
            PDCprop_set_obj_type(obj_prop, PDC_INT);
            sprintf(obj_name, "o1_%d", rank);
            obj_id = PDCobj_create(cont, obj_name, obj_prop);

            // Define region size (10x10) and number of transfers
            region_size[0] = 10;
            region_size[1] = 10;

            // Initialize memory buffer
            for (i = 0; i < BUF_LEN; i++)
                data_write[i] = i;

            // Create memory and object regions and start write transfers
            for (i = 0; i < NUM_TRANSFERS; i++) {
                offsets[0] = i * 10; // offset along first dimension (object)
                offsets[1] = 0;      // offset along second dimension

                // Minimal change: memory region always starts at {0,0}
                memory_regions[i] = PDCregion_create(2, (uint64_t[]){0, 0}, region_size);
                obj_regions[i]    = PDCregion_create(2, offsets, region_size);

                // Create region transfer for writing correct slice of buffer
                transfers[i] = PDCregion_transfer_create(data_write + i * CHUNK_LEN, // offset in local memory
                                                        PDC_WRITE, obj_id, memory_regions[i], obj_regions[i]);
            }

            // Start and wait for all writes
            PDCregion_transfer_start_all(transfers, NUM_TRANSFERS);
            PDCregion_transfer_wait_all(transfers, NUM_TRANSFERS);

            // Close write transfers
            for (i = 0; i < NUM_TRANSFERS; i++)
                PDCregion_transfer_close(transfers[i]);

            // Now read back into data_read in four slices
            for (i = 0; i < NUM_TRANSFERS; i++) {
                transfers[i] = PDCregion_transfer_create(data_read + i * CHUNK_LEN, // offset in local memory
                                                        PDC_READ, obj_id, memory_regions[i], obj_regions[i]);
            }

            // Start and wait for all reads
            PDCregion_transfer_start_all(transfers, NUM_TRANSFERS);
            PDCregion_transfer_wait_all(transfers, NUM_TRANSFERS);

            // Close read transfers and regions
            for (i = 0; i < NUM_TRANSFERS; i++) {
                PDCregion_transfer_close(transfers[i]);
                PDCregion_close(memory_regions[i]);
                PDCregion_close(obj_regions[i]);
            }

            // Validate read-back
            if (memcmp(data_read, data_write, sizeof(int) * BUF_LEN) != 0) {
                printf("Data read was invalid\n");
                ret_value = 1;
            }

            // Close object and container
            PDCobj_close(obj_id);
            PDCcont_close(cont);

            // Close object and container properties
            PDCprop_close(obj_prop);
            PDCprop_close(cont_prop);

            // Close PDC runtime
            PDCclose(pdc);

            // Free memory buffers
            free(data_write);
            free(data_read);

        #ifdef ENABLE_MPI
            MPI_Finalize();
        #endif

            if (ret_value)
                printf("Example had an error\n");
            else
                printf("Example ran successfully\n");

            return ret_value;
        }

.. _get-put-object:

Get Put Object Example
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C
    :linenos:

        #include <stdio.h>
        #include <stdlib.h>
        #include <string.h>
        #include "pdc.h"

        #define BUF_LEN 128 // size of data buffer

        int
        main(int argc, char **argv)
        {
            pdcid_t pdc, cont_prop, cont;
            pdcid_t obj1, obj2;
            char    cont_name[128], obj_name1[128], obj_name2[128];
            int    *data_write, *data_read;
            int     rank      = 0, size, i;
            int     ret_value = 0;

        #ifdef ENABLE_MPI
            MPI_Init(&argc, &argv);
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);
        #endif

            // Allocate buffers
            data_write = (int *)malloc(sizeof(int) * BUF_LEN);
            data_read  = (int *)calloc(BUF_LEN, sizeof(int));

            // Initialize PDC runtime
            pdc = PDCinit("pdc");

            // Initialize memory buffer
            for (i = 0; i < BUF_LEN; i++)
                data_write[i] = i;

            // Configure and create container
            cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
            sprintf(cont_name, "c%d", rank);
            cont = PDCcont_create(cont_name, cont_prop);

            // Initialize data and put first object
            sprintf(obj_name1, "o1_%d", rank);
            obj1 = PDCobj_put_data(obj_name1, data_write, BUF_LEN * sizeof(int), cont);

            // Initialize data and put second object
            sprintf(obj_name2, "o2_%d", rank);
            obj2 = PDCobj_put_data(obj_name2, data_write, BUF_LEN * sizeof(int), cont);

            // Get first object
            PDCobj_get_data(obj1, data_read, BUF_LEN * sizeof(int));

            // Validate first object
            if (memcmp(data_write, data_read, BUF_LEN * sizeof(int)) != 0) {
                printf("Data read was invalid for obj1\n");
                ret_value = 1;
            }

            // Get second object
            PDCobj_get_data(obj2, data_read, BUF_LEN * sizeof(int));

            // Validate second object
            if (memcmp(data_write, data_read, BUF_LEN * sizeof(int)) != 0) {
                printf("Data read was invalid for obj2\n");
                ret_value = 1;
            }

            // Close objects and container
            PDCobj_close(obj1);
            PDCobj_close(obj2);
            PDCcont_close(cont);

            // Close container property
            PDCprop_close(cont_prop);

            // Close PDC runtime
            PDCclose(pdc);

            // Free memory buffers
            free(data_write);
            free(data_read);

        #ifdef ENABLE_MPI
            MPI_Finalize();
        #endif

            if (ret_value)
                printf("Example had an error\n");
            else
                printf("Example ran successfully\n");

            return ret_value;
        }

.. _add-get-kvtag:

Add Get KV Tag
~~~~~~~~~~~~~~

.. code-block:: C
    :linenos:

        #include <stdio.h>
        #include <stdlib.h>
        #include <string.h>
        #include "pdc.h"

        int main() {
            pdcid_t pdc, cont_prop, cont, obj_prop1, obj_prop2, obj1, obj2;
            pdc_kvtag_t kvtag1, kvtag2, kvtag3;
            char *v1 = "value1";
            int v2 = 2;
            double v3 = 3.45;
            pdc_var_type_t type1, type2, type3;
            void *value1, *value2, *value3;
            psize_t value_size;

            // create a pdc
            pdc = PDCinit("pdc");

            // create container property and container
            cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
            cont = PDCcont_create("c1", cont_prop);

            // create object properties
            obj_prop1 = PDCprop_create(PDC_OBJ_CREATE, pdc);
            obj_prop2 = PDCprop_create(PDC_OBJ_CREATE, pdc);

            // create objects
            obj1 = PDCobj_create(cont, "o1", obj_prop1);
            obj2 = PDCobj_create(cont, "o2", obj_prop2);

            // define key-value tags
            kvtag1.name = "key1string";
            kvtag1.value = (void *)v1;
            kvtag1.type = PDC_STRING;
            kvtag1.size = strlen(v1) + 1;

            kvtag2.name = "key2int";
            kvtag2.value = (void *)&v2;
            kvtag2.type = PDC_INT;
            kvtag2.size = sizeof(int);

            kvtag3.name = "key3double";
            kvtag3.value = (void *)&v3;
            kvtag3.type = PDC_DOUBLE;
            kvtag3.size = sizeof(double);

            // put tags for obj1
            PDCobj_put_tag(obj1, kvtag1.name, kvtag1.value, kvtag1.type, kvtag1.size);
            PDCobj_put_tag(obj1, kvtag2.name, kvtag2.value, kvtag2.type, kvtag2.size);

            // put tag for obj2
            PDCobj_put_tag(obj2, kvtag3.name, kvtag3.value, kvtag3.type, kvtag3.size);

            // get tags
            PDCobj_get_tag(obj1, kvtag1.name, (void *)&value1, (void *)&type1, (void *)&value_size);
            PDCobj_get_tag(obj2, kvtag1.name, (void *)&value2, (void *)&type2, (void *)&value_size);
            PDCobj_get_tag(obj2, kvtag3.name, (void *)&value3, (void *)&type3, (void *)&value_size);

            // delete and put new tag for obj1
            PDCobj_del_tag(obj1, kvtag1.name);
            v1 = "New Value After Delete";
            kvtag1.value = (void *)v1;
            kvtag1.size = strlen(v1) + 1;
            PDCobj_put_tag(obj1, kvtag1.name, kvtag1.value, kvtag1.type, kvtag1.size);
            PDCobj_get_tag(obj1, kvtag1.name, (void *)&value1, (void *)&type1, (void *)&value_size);

            // close objects, container, properties, and pdc
            PDCobj_close(obj1);
            PDCobj_close(obj2);
            PDCcont_close(cont);
            PDCprop_close(obj_prop1);
            PDCprop_close(obj_prop2);
            PDCprop_close(cont_prop);
            PDCclose(pdc);

            return 0;
        }

.. _querying-object-data:

Querying Object Data
~~~~~~~~~~~~~~~~~~~~

.. code-block:: C

        #include <stdio.h>
        #include <stdlib.h>
        #include <string.h>
        #include <unistd.h>
        #include "pdc.h"

        int main(int argc, char **argv) {
            int rank = 0, size = 1;
            uint64_t size_MB;
            pdcid_t obj_id = -1;
            struct pdc_region_info region;
            uint64_t i, dims[1];
            pdc_selection_t sel;
            char *obj_name;
            int my_data_count;
            pdc_metadata_t *metadata;
            pdcid_t pdc, cont_prop, cont, obj_prop;
            int ndim = 1;
            int *mydata;

        #ifdef ENABLE_MPI
            MPI_Init(&argc, &argv);
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);
        #endif

            if (argc < 3)
                return 1;

            obj_name = argv[1];
            size_MB  = atoi(argv[2]) * 1048576; // convert MB to bytes

            // create a PDC
            pdc = PDCinit("pdc");

            // create container property and container
            cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
            cont = PDCcont_create("c1", cont_prop);

            // create object property
            obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);

            my_data_count = size_MB / size;
            dims[0] = my_data_count;
            PDCprop_set_obj_dims(obj_prop, 1, dims);
            PDCprop_set_obj_user_id(obj_prop, getuid());
            PDCprop_set_obj_time_step(obj_prop, 0);
            PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
            PDCprop_set_obj_tags(obj_prop, "tag0=1");
            PDCprop_set_obj_type(obj_prop, PDC_INT);

            // create object (only rank 0)
            if (rank == 0)
                obj_id = PDCobj_create(cont, obj_name, obj_prop);

        #ifdef ENABLE_MPI
            MPI_Barrier(MPI_COMM_WORLD);
        #endif

            region.ndim = ndim;
            region.offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
            region.size   = (uint64_t *)malloc(sizeof(uint64_t) * ndim);
            region.offset[0] = rank * my_data_count;
            region.size[0]   = my_data_count;

            mydata = (int *)malloc(my_data_count);
            for (i = 0; i < my_data_count / sizeof(int); i++)
                mydata[i] = i + rank * 1000;

            PDC_Client_write(metadata, &region, mydata);

            // construct a simple query example
            int lo0 = 1000;
            pdc_query_t *q0 = PDCquery_create(obj_id, PDC_LT, PDC_INT, &lo0);
            PDCquery_sel_region(q0, &region);

            PDCquery_get_selection(q0, &sel);
            PDCselection_print(&sel);

            // free resources
            PDCquery_free_all(q0);
            PDCregion_free(&region);
            PDCselection_free(&sel);
            free(mydata);

            PDCcont_close(cont);
            PDCprop_close(cont_prop);
            PDCprop_close(obj_prop);
            PDCclose(pdc);

        #ifdef ENABLE_MPI
            MPI_Finalize();
        #endif

            return 0;
        }
