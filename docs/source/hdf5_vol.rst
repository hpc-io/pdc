.. _hdf5_vol:

**HDF5 VOL for PDC**
====================

The following instructions are for installing PDC on Linux and Cray machines. 
These instructions assume that PDC and its dependencies have all already been 
installed from source (libfabric and Mercury).

Building HDF5
-------------

First set ``HDF5_DIR`` to the directory where you want to install HDF5, e.g. ``$WORK_SPACE/install/hdf5``.

.. code-block:: bash

   wget "https://www.hdfgroup.org/package/hdf5-1-12-1-tar-gz/?wpdmdl=15727&refresh=612559667d6521629837670"
   mv index.html?wpdmdl=15727&refresh=612559667d6521629837670 hdf5-1.12.1.tar.gz
   tar zxf hdf5-1.12.1.tar.gz
   cd hdf5-1.12.1
   ./configure --prefix=$HDF5_DIR
   make
   make check
   make install
   make check-install

Building VOL-PDC
----------------

First set ``HDF5_INCLUDE_DIR``, ``HDF5_LIBRARY``, and ```HDF5_DIR``` to the 
appropriate paths where HDF5 is installed, e.g. ``$HDF5_DIR/include```, 
``$HDF5_DIR/lib```, and ``$HDF5_DIR`` respectively.

.. code-block:: bash

   git clone https://github.com/hpc-io/vol-pdc.git
   cd vol-pdc
   mkdir build
   cd build
   cmake ../ -DHDF5_INCLUDE_DIR=$HDF5_INCLUDE_DIR -DHDF5_LIBRARY=$HDF5_LIBRARY -DBUILD_SHARED_LIBS=ON -DHDF5_DIR=$HDF5_DIR
   make
   make install

Building & Running VOL-PDC Examples
-----------------------------------

The VOL-PDC examples can be built with the following commands:

.. code-block:: bash

   cd vol-pdc/examples
   cmake .
   make

The following assumes ``PDC_BIN_DIR`` is set to the directory 
where the PDC binaries are installed, e.g. ``$PDC_DIR/bin``.
Then, to run the any of the examples you first start the PDC server(s).
You can then launch the example. Finally, you must close the PDC server(s).
For instance, to run the ``h5pdc_vpicio`` example, you can use the following commands:

.. code-block:: bash

   # Start the PDC server(s) in the background
   mpirun -N 1 -n 1 -c 1 ./$PDC_BIN_DIR/pdc_server &
   # Run the example
   mpirun -N 1 -n 1 -c 1 ./h5pdc_vpicio test
   # Close the PDC server(s)
   mpirun -N 1 -n 1 -c 1 ./$PDC_BIN_DIR/close_server