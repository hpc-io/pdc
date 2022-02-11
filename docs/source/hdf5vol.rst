================================
HDF5 VOL for PDC
================================

++++++++++++++++++++++++++++++++++
Installing HDF5 VOL for PDC from source
++++++++++++++++++++++++++++++++++

The following instructions are for installing PDC on Linux and Cray machines. These instructions assume that PDC and its dependencies have all already been installed from source (libfabric and Mercury).

Install HDF5
---------------------------

If a local version of HDF5 is to be used, then the following can be used to install HDF5.

.. code-block:: Bash

	$ wget "https://www.hdfgroup.org/package/hdf5-1-12-1-tar-gz/?wpdmdl=15727&refresh=612559667d6521629837670"
	$ mv index.html?wpdmdl=15727&refresh=612559667d6521629837670 hdf5-1.12.1.tar.gz
	$ tar zxf hdf5-1.12.1.tar.gz
	$ cd hdf5-1.12.1
	$ ./configure --prefix=$HDF5_DIR
	$ make
	$ make check
	$ make install
	$ make check-install

Building VOL-PDC
---------------------------

.. code-block:: Bash

	$ git clone https://github.com/hpc-io/vol-pdc.git
	$ cd vol-pdc
	$ mkdir build
	$ cd build
	$ cmake ../ -DHDF5_INCLUDE_DIR=$HDF5_INCLUDE_DIR -DHDF5_LIBRARY=$HDF5_LIBRARY -DBUILD_SHARED_LIBS=ON -DHDF5_DIR=$HDF5_DIR
	$ make
	$ make install

To compile and run examples:

.. code-block:: Bash

	$ cd vol-pdc/examples
	$ cmake .
	$ make
	$ mpirun -N 1 -n 1 -c 1 <pdc installation directory>/bin/pdc_server.exe &
	$ mpirun -N 1 -n 1 -c 1 ./h5pdc_vpicio test
	$ mpirun -N 1 -n 1 -c 1 <pdc installation directory>/bin/close_server


++++++++++++++++++++++++++++++++++
VOL Function Notes
++++++++++++++++++++++++++++++++++

The following functions have been properly defined:

* H5VL_pdc_info_copy
* H5VL_pdc_info_cmp
* H5VL_pdc_info_free
* H5VL_pdc_info_to_str
* H5VL_pdc_str_to_info

* H5VL_pdc_file_create
* H5VL_pdc_file_open
* H5VL_pdc_file_close
* H5VL_pdc_file_specific

* H5VL_pdc_dataset_create
* H5VL_pdc_dataset_open
* H5VL_pdc_dataset_write
* H5VL_pdc_dataset_read
* H5VL_pdc_dataset_get
* H5VL_pdc_dataset_close

* H5VL_pdc_introspect_get_conn_cls
* H5VL_pdc_introspect_get_cap_flags

* H5VL_pdc_group_create
* H5VL_pdc_group_open

* H5VL_pdc_attr_create
* H5VL_pdc_attr_open
* H5VL_pdc_attr_read
* H5VL_pdc_attr_write
* H5VL_pdc_attr_get

* H5VL_pdc_object_open
* H5VL_pdc_object_get
* H5VL_pdc_object_specific

Any function not listed above is either not currently used by the VOL and therefore does nothing or is called by the VOL, but doesn't need to do anything relevant to the VOL and therefore does nothing.
