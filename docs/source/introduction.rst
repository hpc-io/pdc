.. _introduction:

**1.** Introduction
===================

**1.1.** What is PDC
--------------------

Proactive Data Containers (PDC) software provides an object-focused data management API, 
a runtime system with a set of scalable data object management services, and tools for 
managing data objects stored in the PDC system. The PDC API allows efficient and 
transparent data movement in complex memory and storage hierarchy. The PDC runtime 
system performs data movement asynchronously and provides scalable metadata operations 
to find and manipulate data objects. PDC revolutionizes how data is managed and accessed 
by using object-centric abstractions to represent data that moves in the high-performance 
computing (HPC) memory and storage subsystems. PDC manages extensive metadata to describe 
data objects to find desired data efficiently as well as to store information in the data objects.

More information and publications about PDC are available at https://sdm.lbl.gov/pdc.

If you use PDC in your research, please cite the following:

Byna, Suren, Dong, Bin, Tang, Houjun, Koziol, Quincey, Mu, Jingqing, 
Soumagne, Jerome, Vishwanath, Venkat, Warren, Richard, and Tessier, François. 
*Proactive Data Containers (PDC) v0.1*. Computer Software. https://github.com/hpc-io/pdc. 
USDOE. 11 May. 2017. Web. doi:`10.11578/dc.20210325.1 <https://doi.org/10.11578/dc.20210325.1>`_

**1.2.** Installation
---------------------

We recommend using GCC 7 or a later version. Intel and Cray compilers also work.

Dependencies
~~~~~~~~~~~~

The following dependencies need to be installed:

1. **MPI** (TODO: PUT_VERSION)
2. **libfabric** (TODO: PUT_VERSION)
3. **Mercury** (TODO: PUT_VERSION)

PDC can use either MPICH or OpenMPI as the MPI library, if your system 
doesn't have one installed, follow `MPICH Installers’ Guide <https://www.mpich.org/documentation/guides>`_ 
or `Installing Open MPI <https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/quickstart.html>`_

We provide detailed instructions for installing libfabric, Mercury, and PDC below.

.. attention:: 

   Following the instructions below will record all the environmental variables 
   needed to run PDC in the ``$WORK_SPACE/pdc_env.sh`` file, which can be used for 
   future PDC runs with ``source $WORK_SPACE/pdc_env.sh``.


Prepare Work Space
~~~~~~~~~~~~~~~~~~

Before installing the dependencies and downloading the code repository, we assume 
there is a directory created for your installation already, e.g. ``$WORK_SPACE`` and 
that you are in the ``$WORK_SPACE`` directory.

.. code-block:: Bash

   export WORK_SPACE=/path/to/your/work/space
   mkdir -p $WORK_SPACE/source
   mkdir -p $WORK_SPACE/install

   cd $WORK_SPACE/source
   git clone https://github.com/ofiwg/libfabric
   git clone https://github.com/mercury-hpc/mercury --recursive
   git clone https://github.com/hpc-io/pdc

   export LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric
   export MERCURY_SRC_DIR=$WORK_SPACE/source/mercury
   export PDC_SRC_DIR=$WORK_SPACE/source/pdc

   export LIBFABRIC_DIR=$WORK_SPACE/install/libfabric
   export MERCURY_DIR=$WORK_SPACE/install/mercury
   export PDC_DIR=$WORK_SPACE/install/pdc

   mkdir -p $LIBFABRIC_SRC_DIR
   mkdir -p $MERCURY_SRC_DIR
   mkdir -p $PDC_SRC_DIR

   mkdir -p $LIBFABRIC_DIR
   mkdir -p $MERCURY_DIR
   mkdir -p $PDC_DIR

   # Save the environment variables to a file
   echo "export LIBFABRIC_SRC_DIR=$LIBFABRIC_SRC_DIR" > $WORK_SPACE/pdc_env.sh
   echo "export MERCURY_SRC_DIR=$MERCURY_SRC_DIR" >> $WORK_SPACE/pdc_env.sh
   echo "export PDC_SRC_DIR=$PDC_SRC_DIR" >> $WORK_SPACE/pdc_env.sh
   echo "export LIBFABRIC_DIR=$LIBFABRIC_DIR" >> $WORK_SPACE/pdc_env.sh
   echo "export MERCURY_DIR=$MERCURY_DIR" >> $WORK_SPACE/pdc_env.sh
   echo "export PDC_DIR=$PDC_DIR" >> $WORK_SPACE/pdc_env.sh

From now on you can simply run the following commands to set the environment variables:

.. code-block:: Bash

   export WORK_SPACE=/path/to/your/work/space
   source $WORK_SPACE/pdc_env.sh

Install libfabric
~~~~~~~~~~~~~~~~~

.. code-block:: Bash

   cd $LIBFABRIC_SRC_DIR
   git checkout v1.18.0
   ./autogen.sh
   ./configure --prefix=$LIBFABRIC_DIR CC=mpicc CFLAG="-O2"
   make -j && make install

   # Test the installation
   make check

   # Set the environment variables
   export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
   export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"
   echo 'export LD_LIBRARY_PATH=$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
   echo 'export PATH=$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh

.. note::

   ``CC=mpicc`` may need to be changed to the corresponding compiler 
   in your system, e.g. ``CC=cc`` or ``CC=gcc``.
   On Perlmutter@NERSC, ``--disable-efa --disable-sockets`` should be 
   added to the ``./configure`` command when compiling on login nodes.

.. attention::

   When installing on MacOS, make sure to enable ``sockets`` with the following configure command:
   ``./configure CFLAG=-O2 --enable-sockets=yes --enable-tcp=yes --enable-udp=yes --enable-rxm=yes``


Install Mercury
~~~~~~~~~~~~~~~

.. code-block:: Bash

   cd $MERCURY_SRC_DIR

   # Checkout a release version
   git checkout v2.2.0
   mkdir build
   cd build
   cmake -DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DBUILD_SHARED_LIBS=ON \
         -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DNA_OFI_TESTING_PROTOCOL=tcp ../
   make -j && make install

   # Test the installation
   ctest

   # Set the environment variables
   export LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
   export PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
   echo 'export LD_LIBRARY_PATH=$MERCURY_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
   echo 'export PATH=$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh

.. note::

   ``CC=mpicc`` may need to be changed to the corresponding compiler in your system, e.g. 
   ``-DCMAKE_C_COMPILER=cc`` or ``-DCMAKE_C_COMPILER=gcc``.
   Make sure the ctest passes. PDC may not work without passing 
   all the tests of Mercury.

.. attention::

   When installing on MacOS, specify the ``sockets`` protocol used by Mercury by replacing 
   the cmake command from ``-DNA_OFI_TESTING_PROTOCOL=tcp`` to ``-DNA_OFI_TESTING_PROTOCOL=sockets``

Install PDC
~~~~~~~~~~~

.. code-block:: Bash

   cd $PDC_SRC_DIR
   git checkout develop
   mkdir build
   cd build
   cmake -DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR \
         -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec ../
   make -j && make install

   # Set the environment variables
   export LD_LIBRARY_PATH="$PDC_DIR/lib:$LD_LIBRARY_PATH"
   export PATH="$PDC_DIR/include:$PDC_DIR/lib:$PATH"	
   echo 'export LD_LIBRARY_PATH=$PDC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
   echo 'export PATH=$PDC_DIR/include:$PDC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh

.. note::

   ``-DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec`` may need to be 
   changed to ``-DCMAKE_C_COMPILER=cc -DMPI_RUN_CMD=srun`` depending on your system environment.

   If you are trying to compile PDC on MacOS, ``LibUUID`` needs to be installed 
   on your MacOS first. Simple use ``brew install ossp-uuid`` to install it.
   If you are trying to compile PDC on Linux, you should also make sure ``LibUUID`` 
   is installed on your system. If not, you can install it with 
   ``sudo apt-get install uuid-dev`` on Ubuntu or ``yum install libuuid-devel`` on CentOS.

   In MacOS you also need to export the following environment variable so PDC 
   (i.e., Mercury) uses the ``socket`` protocol, the only one supported in 
   MacOS: ``export HG_TRANSPORT="sockets"``.


Test Your PDC Installation
~~~~~~~~~~~~~~~~~~~~~~~~~~

PDC's ``ctest`` contains both sequential and parallel (MPI) tests, and can be run 
with the following in the ``build`` directory.

.. code-block:: Bash

   ctest

You can also specify a timeout (e.g., 2 minutes) for the tests by specifying 
the ``timeout`` parameter when calling ``ctest``:

.. code-block:: Bash

   ctest --timeout 120

.. note::

   If you are using PDC on an HPC system, e.g. Perlmutter@NERSC, ``ctest`` should be run 
   on a compute node, you can submit an interactive job on Perlmutter: 
   ``salloc --nodes 1 --qos interactive --time 01:00:00 --constraint cpu --account=mxxxx``

Spack Installation
~~~~~~~~~~~~~~~~~~

Spack is a package manager for supercomputers, Linux, and macOS. 
It makes installing scientific software easy.
More information about Spack can be found at: https://spack.io.
PDC and its dependencies can be installed with spack:

.. code-block:: bash

   # Clone the Spack repository
   git clone -c feature.manyFiles=true https://github.com/spack/spack.git

   # Install PDC and its dependencies
   ./spack/bin/spack install pdc

   # Verify the installation
   pdc --version

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

**1.4.** Running PDC Server(s)
------------------------------

PDC works in a client-server architecture, therefore, before running any PDC
client application, you need to start the PDC server(s) first.
First ensure that the PDC server is built and installed correctly, 
then you can start a single PDC server instance with the following command:

.. code-block:: bash

   ./pdc_server.exe

You can also start multiple PDC server instances on different nodes,
for example, you can start 4 PDC servers using the following command:

.. code-block:: bash

   mpirun -np 4 ./pdc_server.exe

**1.5.** Building & Running PDC Client(s)
-----------------------------------------

Before running a PDC client application, ensure that the PDC server(s) are
running. You can run a single client application using the following command:

.. code-block:: bash

   ./client_app

You can also run multiple client applications in parallel using MPI:

.. code-block:: bash

   mpirun -np 4 ./client_app