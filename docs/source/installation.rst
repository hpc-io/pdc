================================
PDC Installation
================================

The following instructions are for installing PDC on Linux and Cray machines. GCC version 7 or newer and a version of MPI are needed to install PDC.

Current PDC tests have been verified with MPICH. To install MPICH, follow the documentation in https://www.mpich.org/static/downloads/3.4.1/mpich-3.4.1-installguide.pdf

PDC also depends on libfabric and Mercury. We provide detailed instructions for installing libfabric, Mercury, and PDC below. 

.. attention:: 
	Make sure to record the environmental variables (lines that contains the "export" commands). They are needed for running PDC and to use the libraries again.

---------------------------
Install libfabric
---------------------------

.. code-block:: Bash

	$ wget https://github.com/ofiwg/libfabric/archive/v1.11.2.tar.gz
	$ tar xvzf v1.11.2.tar.gz
	$ cd libfabric-1.11.2
	$ mkdir install
	$ export LIBFABRIC_DIR=$(pwd)/install
	$ ./autogen.sh
	$ ./configure --prefix=$LIBFABRIC_DIR CC=gcc CFLAG="-O2"
	$ make -j8
	$ make install
	$ export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
	$ export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"

---------------------------
Install Mercury
---------------------------

.. attention:: 
	Make sure the ctest passes. PDC may not work without passing all the tests of Mercury.

Step 2 in the following is not required. It is a stable commit that has been used to test when these these instructions were written. One may skip it to use the current master branch of Mercury.

.. code-block:: Bash

	$ git clone https://github.com/mercury-hpc/mercury.git
	$ cd mercury
	$ git checkout e741051fbe6347087171f33119d57c48cb438438
	$ git submodule update --init
	$ export MERCURY_DIR=$(pwd)/install
	$ mkdir install
	$ cd install
	$ cmake ../ -DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=gcc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF
	$ make
	$ make install
	$ ctest
	$ export LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
	$ export PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"

---------------------------
Install PDC
---------------------------

One can replace mpicc to other available MPI compilers. For example, on Cori, cc can be used to replace mpicc. ctest contains both sequential and MPI tests for the PDC settings. These can be used to perform regression tests.

.. code-block:: Bash

	$ git clone https://github.com/hpc-io/pdc.git
	$ cd pdc
	$ git checkout stable
	$ cd src
	$ mkdir install
	$ cd install
	$ export PDC_DIR=$(pwd)
	$ cmake ../ -DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc
	$ make -j8
	$ ctest

---------------------------
Environmental Variables
---------------------------

During installation, we have set some environmental variables. These variables may disappear after the close the current session ends. We recommend adding the following lines to ~/.bashrc. (One may also execute them manually after logging in). The MERCURY_DIR and LIBFABRIC_DIR variables should be identical to the values that were set during the installation of Mercury and libfabric. The install path is the path containing bin and lib directory, instead of the one containing the source code.

.. code-block:: Bash

	$ export PDC_DIR="where/you/installed/your/pdc"
	$ export MERCURY_DIR="where/you/installed/your/mercury"
	$ export LIBFABRIC_DIR="where/you/installed/your/libfabric"
	$ export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
	$ export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"

One can also manage the path with Spack, which is a lot more easier to load and unload these libraries.

---------------------------
Running PDC
---------------------------

The ctest under PDC install folder runs PDC examples using PDC APIs. PDC needs to run at least two applications. The PDC servers need to be started first. The client programs that send I/O request to servers as Mercury RPCs are started next.

We provide a convenient function (mpi_text.sh) to start MPI tests. One needs to change the MPI launching function (mpiexec) with the relevant launcher on a system. On Cori at NERSC, the mpiexec argument needs to be changed to srun. On Theta, it is aprun. On Summit, it is jsrun.

.. code-block:: Bash

	$ cd $PDC_DIR/bin
	$ ./mpi_test.sh ./pdc_init mpiexec 2 4

This is test will start 2 processes for PDC servers. The client program ./pdc_init will start 4 processes. Similarly, one can run any of the client examples in ctest. These source code will provide some knowledge of how to use PDC. For more reference, one may check the documentation folder in this repository.

---------------------------
PDC on Cori
---------------------------

Installation on Cori is not very different from a regular linux machine. Simply replacing all gcc/mpicc with the default cc compiler on Cori would work. Add options -DCMAKE_C_FLAGS="-dynamic" to the cmake line of PDC. Add -DCMAKE_C_FLAGS="-dynamic" -DCMAKE_CXX_FLAGS="-dynamic" at the end of the cmake line for mercury as well. Finally, "-DMPI_RUN_CMD=srun" is needed for ctest command later. In some instances and on some systems, unload darshan before installation may be needed.

For job allocation on Cori it is recommended to add "--gres=craynetwork:2" to the command:

.. code-block:: Bash

	$ salloc -C haswell -N 4 -t 01:00:00 -q interactive --gres=craynetwork:2

And to launch the PDC server and the client, add "--gres=craynetwork:1" before the executables:

Run 4 server processes, each on one node in background:

.. code-block:: Bash

	$ srun -N 4 -n  4 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 ./bin/pdc_server.exe &

Run 64 client processes that concurrently create 1000 objects in total:

.. code-block:: Bash

	$ srun -N 4 -n 64 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 ./bin/create_obj_scale -r 1000
