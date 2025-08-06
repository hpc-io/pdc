.. _using_pdc:

**3.0.** Using PDC
==================

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
- :ref:`Example: 3D Region Transfers <3D-region-transfer>`
- :ref:`Example: 2D Batch Region Transfer <2D-batch-region-transfer>`
- :ref:`Example: 3D Batch Region Transfer <3D-batch-region-transfer>`
- :ref:`Example: Get & Put Object <get-put-object>`

3.1. Initializing PDC
---------------------

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


3.2. Container Lifecycle
-------------------------

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
    pdcid_t cont_col_id = PDCcont_create_col("cont", cont_prop_id);

To open an existing container:

.. code-block:: C

    pdcid_t cont_id = PDCcont_open("cont");

The following functions should be used to free both the container and its associated property resources:

.. code-block:: C

    PDCprop_close(cont_prop_id);
    PDCcont_close(cont_id);
    PDCcont_close(cont_col_id);

3.3. Object Lifecycle
---------------------

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
    pdcid_t obj_col_id = PDCobj_create_col(cont_id, "obj", obj_prop_id, my_rank, comm);

To open an existing object by name within a container:

.. code-block:: C

    pdcid_t obj_id = PDCobj_open(cont_id, "obj");

When the object and its property are no longer needed, they should be closed to free resources:

.. code-block:: C

    PDCprop_close(obj_prop_id);
    PDCobj_close(obj_id);
    PDCobj_close(obj_col_id);

3.4. Region Transfer Lifecycle
------------------------------

.. _region-lifecycle:

Regions define logical subranges within a PDC object and are used to specify what part of the object’s data will be transferred between memory and storage.

Transfers can be performed in three main modes:

- Individually, with ``PDCregion_transfer_start()``
- Collectively, with ``PDCregion_transfer_start_col()`` across MPI processes
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

    PDCregion_transfer_start_col(xfer);

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

Full Examples
=============

.. _2D-region-transfer:

2D Region Transfer Example
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C

        #include <stdio.h>
        #include <stdlib.h>
        #include <unistd.h>
        #include <inttypes.h>
        #include "pdc.h"

        #define BUF_LEN 128

        int main(int argc, char **argv)
        {
                pdcid_t pdc, cont_prop, cont, obj_prop, reg, reg_global;
                pdcid_t obj1, obj2;
                char cont_name[128], obj_name1[128], obj_name2[128];
                pdcid_t transfer_request;
                int rank = 0, size = 1, i;
                int ret_value = 0;
                uint64_t offset[3], offset_length[3];
                uint64_t dims[2];
                int *data = (int *)malloc(sizeof(int) * BUF_LEN);
                int *data_read = (int *)malloc(sizeof(int) * BUF_LEN);
                dims[0] = BUF_LEN / 4;
                dims[1] = 4;

                #ifdef ENABLE_MPI
                MPI_Init(&argc, &argv);
                MPI_Comm_rank(MPI_COMM_WORLD, &rank);
                MPI_Comm_size(MPI_COMM_WORLD, &size);
                #endif

                pdc = PDCinit("pdc");
                cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
                sprintf(cont_name, "c%d", rank);
                cont = PDCcont_create(cont_name, cont_prop);
                obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
                PDCprop_set_obj_type(obj_prop, PDC_INT);
                PDCprop_set_obj_dims(obj_prop, 2, dims);
                PDCprop_set_obj_user_id(obj_prop, getuid());
                PDCprop_set_obj_time_step(obj_prop, 0);
                PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
                PDCprop_set_obj_tags(obj_prop, "tag0=1");

                sprintf(obj_name1, "o1_%d", rank);
                obj1 = PDCobj_create(cont, obj_name1, obj_prop);
                sprintf(obj_name2, "o2_%d", rank);
                obj2 = PDCobj_create(cont, obj_name2, obj_prop);

                offset[0] = 0;
                offset_length[0] = BUF_LEN;
                reg = PDCregion_create(1, offset, offset_length);
                offset[0] = 0;
                offset[1] = 0;
                offset_length[0] = BUF_LEN / 4;
                offset_length[1] = 4;
                reg_global = PDCregion_create(2, offset, offset_length);

                for (i = 0; i < BUF_LEN; ++i)
                        data[i] = i;

                transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global);
                PDCregion_transfer_start(transfer_request);
                PDCregion_transfer_wait(transfer_request);
                PDCregion_transfer_close(transfer_request);
                PDCregion_close(reg);
                PDCregion_close(reg_global);

                offset[0] = 0;
                offset_length[0] = BUF_LEN;
                reg = PDCregion_create(1, offset, offset_length);
                offset[0] = 0;
                offset[1] = 0;
                offset_length[0] = BUF_LEN / 4;
                offset_length[1] = 4;
                reg_global = PDCregion_create(2, offset, offset_length);

                transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj1, reg, reg_global);
                PDCregion_transfer_start(transfer_request);
                PDCregion_transfer_wait(transfer_request);
                PDCregion_transfer_close(transfer_request);

                for (i = 0; i < BUF_LEN; ++i)
                        if (data_read[i] != i)
                        ret_value = 1;

                PDCregion_close(reg);
                PDCregion_close(reg_global);
                PDCobj_close(obj1);
                PDCobj_close(obj2);
                PDCcont_close(cont);
                PDCprop_close(obj_prop);
                PDCprop_close(cont_prop);
                free(data);
                free(data_read);
                PDCclose(pdc);

                #ifdef ENABLE_MPI
                MPI_Finalize();
                #endif
                return ret_value;
        }


.. _3D-region-transfer:

3D Region Transfer Example
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C

        #include <stdio.h>
        #include <stdlib.h>
        #include <string.h>
        #include <getopt.h>
        #include <time.h>
        #include <inttypes.h>
        #include <unistd.h>
        #include <sys/time.h>
        #include "pdc.h"

        #define BUF_LEN 256

        int main(int argc, char **argv)
        {
                pdcid_t pdc, cont_prop, cont, obj_prop, reg, reg_global;
                pdcid_t obj1, obj2;
                char cont_name[128], obj_name1[128], obj_name2[128];
                pdcid_t transfer_request;

                int rank = 0, size = 1, i;
                int ret_value = 0;

                uint64_t offset[3], offset_length[3];
                uint64_t dims[3];

                int *data      = (int *)malloc(sizeof(int) * BUF_LEN);
                int *data_read = (int *)malloc(sizeof(int) * BUF_LEN);
                dims[0]        = BUF_LEN / 16;
                dims[1]        = 4;
                dims[2]        = 4;

                #ifdef ENABLE_MPI
                MPI_Init(&argc, &argv);
                MPI_Comm_rank(MPI_COMM_WORLD, &rank);
                MPI_Comm_size(MPI_COMM_WORLD, &size);
                #endif

                pdc = PDCinit("pdc");

                cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
                sprintf(cont_name, "c%d", rank);
                cont = PDCcont_create(cont_name, cont_prop);

                obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
                PDCprop_set_obj_type(obj_prop, PDC_INT);
                PDCprop_set_obj_dims(obj_prop, 3, dims);
                PDCprop_set_obj_user_id(obj_prop, getuid());
                PDCprop_set_obj_time_step(obj_prop, 0);
                PDCprop_set_obj_app_name(obj_prop, "DataServerTest");
                PDCprop_set_obj_tags(obj_prop, "tag0=1");

                sprintf(obj_name1, "o1_%d", rank);
                obj1 = PDCobj_create(cont, obj_name1, obj_prop);
                sprintf(obj_name2, "o2_%d", rank);
                obj2 = PDCobj_create(cont, obj_name2, obj_prop);

                offset[0]        = 0;
                offset_length[0] = BUF_LEN;
                reg              = PDCregion_create(1, offset, offset_length);

                offset[0]        = 0;
                offset[1]        = 0;
                offset[2]        = 0;
                offset_length[0] = BUF_LEN / 16;
                offset_length[1] = 4;
                offset_length[2] = 4;
                reg_global       = PDCregion_create(3, offset, offset_length);

                for (i = 0; i < BUF_LEN; ++i)
                        data[i] = i;

                transfer_request = PDCregion_transfer_create(data, PDC_WRITE, obj1, reg, reg_global);
                PDCregion_transfer_start(transfer_request);
                PDCregion_transfer_wait(transfer_request);
                PDCregion_transfer_close(transfer_request);

                PDCregion_close(reg);
                PDCregion_close(reg_global);

                offset[0]        = 0;
                offset_length[0] = BUF_LEN;
                reg              = PDCregion_create(1, offset, offset_length);

                offset[0]        = 0;
                offset[1]        = 0;
                offset[2]        = 0;
                offset_length[0] = BUF_LEN / 16;
                offset_length[1] = 4;
                offset_length[2] = 4;
                reg_global       = PDCregion_create(3, offset, offset_length);

                transfer_request = PDCregion_transfer_create(data_read, PDC_READ, obj1, reg, reg_global);
                PDCregion_transfer_start(transfer_request);
                PDCregion_transfer_wait(transfer_request);
                PDCregion_transfer_close(transfer_request);

                for (i = 0; i < BUF_LEN; ++i) {
                        if (data_read[i] != i) {
                        ret_value = 1;
                        break;
                        }
                }

                PDCregion_close(reg);
                PDCregion_close(reg_global);

                PDCobj_close(obj1);
                PDCobj_close(obj2);
                PDCcont_close(cont);
                PDCprop_close(obj_prop);
                PDCprop_close(cont_prop);

                free(data);
                free(data_read);

                PDCclose(pdc);

                #ifdef ENABLE_MPI
                MPI_Finalize();
                #endif

                return ret_value;
        }


.. _2D-batch-region-transfer:

2D Batch Region Transfer Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C

        #include <stdio.h>
        #include <stdlib.h>
        #include <string.h>
        #include "pdc.h"

        int main() {
                // Initialize PDC
                pdcid_t pdc_id = PDCinit("pdc");
                pdcid_t prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

                // Set object dimensions
                uint64_t dims[2] = {100, 100};
                PDCprop_set_obj_dims(prop, dims);
                PDCprop_set_obj_type(prop, PDC_INT);

                // Create object
                pdcid_t obj_id = PDCobj_create(pdc_id, "2d_obj", prop);

                // Create memory and object regions for 4 different 10x10 regions
                uint64_t offsets[4][2] = {
                        {0, 0}, {10, 10}, {20, 20}, {30, 30}
                };

                int *buffers[4];
                pdcid_t mem_regions[4], obj_regions[4], transfers[4];

                for (int i = 0; i < 4; i++) {
                        buffers[i] = malloc(sizeof(int) * 10 * 10);
                        for (int j = 0; j < 100; j++) {
                        buffers[i][j] = i * 1000 + j;
                        }

                        mem_regions[i] = PDCregion_create(2, (uint64_t[]){0, 0}, (uint64_t[]){10, 10});
                        obj_regions[i] = PDCregion_create(2, offsets[i], (uint64_t[]){10, 10});
                        transfers[i] = PDCregion_transfer_create(buffers[i], PDC_WRITE, obj_id, obj_regions[i], mem_regions[i]);
                }

                // Start and wait all transfers
                PDCregion_transfer_start_all(4, transfers);
                PDCregion_transfer_wait_all(4, transfers);

                // Cleanup
                for (int i = 0; i < 4; i++) {
                        PDCregion_close(mem_regions[i]);
                        PDCregion_close(obj_regions[i]);
                        PDCregion_transfer_close(transfers[i]);
                        free(buffers[i]);
                }

                PDCobj_close(obj_id);
                PDCprop_close(prop);
                PDCclose(pdc_id);

                return 0;
        }

.. _3D-batch-region-transfer:

3D Batch Region Transfer Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C

        #include <stdio.h>
        #include <stdlib.h>
        #include <string.h>
        #include "pdc.h"

        int main() {
                // Initialize PDC
                pdcid_t pdc_id = PDCinit("pdc");
                pdcid_t prop = PDCprop_create(PDC_OBJ_CREATE, pdc_id);

                // Set object dimensions
                uint64_t dims[3] = {64, 64, 64};
                PDCprop_set_obj_dims(prop, dims);
                PDCprop_set_obj_type(prop, PDC_FLOAT);

                // Create object
                pdcid_t obj_id = PDCobj_create(pdc_id, "3d_obj", prop);

                // Create memory and object regions for 4 different 8x8x8 regions
                uint64_t offsets[4][3] = {
                        {0, 0, 0}, {8, 8, 8}, {16, 16, 16}, {24, 24, 24}
                };

                float *buffers[4];
                pdcid_t mem_regions[4], obj_regions[4], transfers[4];

                for (int i = 0; i < 4; i++) {
                        buffers[i] = malloc(sizeof(float) * 8 * 8 * 8);
                        for (int j = 0; j < 512; j++) {
                        buffers[i][j] = (float)(i * 1000 + j);
                        }

                        mem_regions[i] = PDCregion_create(3, (uint64_t[]){0, 0, 0}, (uint64_t[]){8, 8, 8});
                        obj_regions[i] = PDCregion_create(3, offsets[i], (uint64_t[]){8, 8, 8});
                        transfers[i] = PDCregion_transfer_create(buffers[i], PDC_WRITE, obj_id, obj_regions[i], mem_regions[i]);
                }

                // Start and wait all transfers
                PDCregion_transfer_start_all(4, transfers);
                PDCregion_transfer_wait_all(4, transfers);

                // Cleanup
                for (int i = 0; i < 4; i++) {
                        PDCregion_close(mem_regions[i]);
                        PDCregion_close(obj_regions[i]);
                        PDCregion_transfer_close(transfers[i]);
                        free(buffers[i]);
                }

                PDCobj_close(obj_id);
                PDCprop_close(prop);
                PDCclose(pdc_id);

                return 0;
        }

.. _get-put-object:

Get Put Object Example
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: C

        #include <stdio.h>
        #include <stdlib.h>
        #include <string.h>
        #include "pdc.h"

        int main(int argc, char **argv)
        {
                pdcid_t pdc, cont_prop, cont;
                pdcid_t obj1, obj2;
                char cont_name[128], obj_name1[128], obj_name2[128];
                char *data = (char *)malloc(sizeof(double) * 128);

                #ifdef ENABLE_MPI
                MPI_Init(&argc, &argv);
                #endif

                pdc = PDCinit("pdc");
                cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
                sprintf(cont_name, "c%d", 0);
                cont = PDCcont_create(cont_name, cont_prop);

                memset(data, 1, 128 * sizeof(double));
                sprintf(obj_name1, "o1_%d", 0);
                obj1 = PDCobj_put_data(obj_name1, data, 16 * sizeof(double), cont);

                memset(data, 2, 128 * sizeof(double));
                sprintf(obj_name2, "o2_%d", 0);
                obj2 = PDCobj_put_data(obj_name2, data, 128 * sizeof(double), cont);

                memset(data, 0, 128 * sizeof(double));
                PDCobj_get_data(obj1, data, 16 * sizeof(double));

                memset(data, 0, 128 * sizeof(double));
                PDCobj_get_data(obj2, data, 128 * sizeof(double));

                PDCobj_close(obj1);
                PDCobj_close(obj2);
                PDCcont_close(cont);
                PDCprop_close(cont_prop);
                free(data);
                PDCclose(pdc);

                #ifdef ENABLE_MPI
                MPI_Finalize();
                #endif
                return 0;
        }
