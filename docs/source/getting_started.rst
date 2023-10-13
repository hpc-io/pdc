================================
Getting Started
================================

++++++++++++++++++++++++++++++++++
Installing PDC with Spack
++++++++++++++++++++++++++++++++++

Spack is a package manager for supercomputers, Linux, and macOS.
More information about Spack can be found at: https://spack.io
PDC and its dependent libraries can be installed with spack:

.. code-block:: Bash

	# Clone Spack
	git clone -c feature.manyFiles=true https://github.com/spack/spack.git
	# Install the latest PDC release version with Spack
	./spack/bin/spack install pdc

If you run into issues with ``libfabric`` on macOS and some Linux distributions, you can enable all fabrics by installing PDC using:

.. code-block:: Bash

	spack install pdc ^libfabric fabrics=sockets,tcp,udp,rxm

++++++++++++++++++++++++++++++++++
Installing PDC from source code
++++++++++++++++++++++++++++++++++
We recommend using GCC 7 or a later version. Intel and Cray compilers also work.

---------------------------
Dependencies
---------------------------
The following dependencies need to be installed:

* MPI
* libfabric
* Mercury

PDC can use either MPICH or OpenMPI as the MPI library, if your system doesn't have one installed, follow `MPICH Installers’ Guide <https://www.mpich.org/documentation/guides>`_ or `Installing Open MPI <https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/quickstart.html>`_

We provide detailed instructions for installing libfabric, Mercury, and PDC below.

.. attention:: 
	Following the instructions below will record all the environmental variables needed to run PDC in the ``$WORK_SPACE/pdc_env.sh`` file, which can be used for future PDC runs with ``source $WORK_SPACE/pdc_env.sh``.


Prepare Work Space and download source codes
--------------------------------------------
Before installing the dependencies and downloading the code repository, we assume there is a directory created for your installation already, e.g. `$WORK_SPACE` and now you are in `$WORK_SPACE`.

.. code-block:: Bash
	:emphasize-lines: 1

	export WORK_SPACE=/path/to/your/work/space
	mkdir -p $WORK_SPACE/source
	mkdir -p $WORK_SPACE/install

	cd $WORK_SPACE/source
	git clone git@github.com:ofiwg/libfabric.git
	git clone git@github.com:mercury-hpc/mercury.git --recursive
	git clone git@github.com:hpc-io/pdc.git

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
	:emphasize-lines: 1

	export WORK_SPACE=/path/to/your/work/space
	source $WORK_SPACE/pdc_env.sh



Install libfabric
-----------------

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
	``CC=mpicc`` may need to be changed to the corresponding compiler in your system, e.g. ``CC=cc`` or ``CC=gcc``.
	On Perlmutter@NERSC, ``--disable-efa --disable-sockets`` should be added to the ``./configure`` command when compiling on login nodes.


Install Mercury
---------------

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
	``CC=mpicc`` may need to be changed to the corresponding compiler in your system, e.g. ``-DCMAKE_C_COMPILER=cc`` or ``-DCMAKE_C_COMPILER=gcc``.
	Make sure the ctest passes. PDC may not work without passing all the tests of Mercury.


Install PDC
-----------

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
	``-DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec`` may need to be changed to ``-DCMAKE_C_COMPILER=cc -DMPI_RUN_CMD=srun`` depending on your system environment.

.. note::
	If you are trying to compile PDC on your Mac, ``LibUUID`` needs to be installed on your MacOS first. Simple use ``brew install ossp-uuid`` to install it.

Test Your PDC Installation
--------------------------
PDC's ``ctest`` contains both sequential and parallel/MPI tests, and can be run with the following in the `build` directory.

.. code-block:: Bash

	ctest

.. note::
	If you are using PDC on an HPC system, e.g. Perlmutter@NERSC, ``ctest`` should be run on a compute node, you can submit an interactive job on Perlmutter: ``salloc --nodes 1 --qos interactive --time 01:00:00 --constraint cpu --account=mxxxx``


Install/Configure Julia Support for PDC
---------------------------------------
Currently, in ``src/tests``, we experimentally added support for Julia programming language. With this support, you can either hand over the data to a Julia-based function to process, or you can use the result returned by a Julia-based function in which a complex logic is implemented (such as complex algebra operations or connecting to databases/cloud storages, etc).
To know how to add a Julia function, please see Developer Notes. 

But before adding any of your function, we need to enable the Julia support first, and here is how:

Prerequisite
^^^^^^^^^^^^

Make sure you have Julia-lang installed. You can check with your system administrator to see if you already have Julia-lang installed. If not, you can either ask your system administrator to install it for you or you can install it yourself if permitted. On macOS, the best way to install Julia is via `Homebrew https://brew.sh`_. You may also refer to `Julia Download Page https://julialang.org/downloads/`_ for instructions on installing Julia.
Once you installed Julia, you can set `JULIA_HOME` to be where Julia-lang is installed.

.. code-block:: Bash
	export JULIA_HOME=/path/to/julia/install/directory


.. note:: Note on perlmutter:
	You can easily perform `module load julia` to load the Julia-lang environment. Then, you can do the following to set `$JULIA_HOME`:

	.. code-block:: Bash
		export JULIA_HOME=$(dirname $(dirname $(which julia)))


Enabling Julia Support for PDC Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Once the Prerequisite is satisfied, you can enable Julia support by adding `--DPDC_ENABLE_JULIA_SUPPORT=ON` to your cmake command and re-run it. 
Then you can compile your PDC project with Julia support. 

Now, see Developer Notes to know how you can add your own Julia functions to enhance your test cases in PDC. 


Build PDC in a Docker Container
---------------------------------------------------
Simply run the following command from the project root directory to build PDC in a Docker container:
.. code-block:: Bash
	.docker/run_dev_base.sh


Build PDC in Github Codespace
---------------------------------------------------
Simply start a Github Codespace from the targeting branch of your PDC project, and enjoy. 
For more information on how to create Github Codespace, please refer to `Github Codespace Documentation <https://docs.github.com/en/codespaces/developing-in-codespaces/creating-a-codespace>`_


---------------------------
Running PDC
---------------------------

If you have followed all the previous steps, ``$WORK_SPACE/pdc_env.sh`` sets all the environment variables needed to run PDC, and you only need to do the following once in each terminal session before running PDC.

.. code-block:: Bash

	export WORK_SPACE=/path/to/your/work/space
	source $WORK_SPACE/pdc_env.sh

PDC is a typical client-server application.
To run PDC, one needs to start the server processes first, and then the clients can be started and connected to the PDC servers automatically. 

On Linux
--------
Run 2 server processes in the background

.. code-block:: Bash

	mpiexec -np 2 $PDC_DIR/bin/pdc_server.exe &

Run 4 client processes that concurrently create 1000 objects and then create and query 1000 tags:

.. code-block:: Bash

	mpiexec -np 4 $PDC_DIR/share/test/bin/kvtag_add_get_scale 1000 1000 1000

    
On Perlmutter
-------------
Run 4 server processes, each on one compute node in the background:

.. code-block:: Bash

	srun -N 4 -n 4 -c 2 --mem=25600 --cpu_bind=cores $PDC_DIR/bin/pdc_server.exe &

Run 64 client processes that concurrently create 1000 objects and then create and query 100000 tags:

.. code-block:: Bash

	srun -N 4 -n 64 -c 2 --mem=25600 --cpu_bind=cores $PDC_DIR/share/test/bin/kvtag_add_get_scale 100000 100000 100000
