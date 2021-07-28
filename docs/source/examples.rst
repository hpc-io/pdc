================================
Examples
================================

* PDC regression tests can be found in https://github.com/hpc-io/pdc/tree/stable/src/tests.
* Please follow the instructions for PDC installations.
* PDC programs start with PDC servers running in the background.
* Client programs uses PDC APIs to forward requests to PDC servers.
* Scripts run_test.sh, mpi_test.sh, and run_multiple_tests.sh automatically run start and close PDC servers

Usage:

.. code-block:: Bash

	./run_test.sh ./pdc_client_application arg1 arg2 .....
	./mpi_test.sh ./pdc_client_application MPIRUN_CMD number_of_servers number_of_clients arg1 arg2 ....
	./run_multiple_test.sh ./pdc_client_application_1 ./pdc_client_application_2 ......


---------------------------
PDC Hello World
---------------------------

* pdc_init.c
* A PDC program starts with PDCinit and finishes with PDCclose.
* To a simple hello world program for PDC, use the following command.

.. code-block:: Bash
	
	make pdc_init
	./run_test.sh ./pdc_init

* The script "run_test.sh" starts a server first. Then program "obj_get_data" is executed. Finally, the PDC servers are closed.
* Alternatively, the following command can be used for multile MPI processes.


.. code-block:: Bash

	make pdc_init
	./mpi_test.sh ./pdc_init mpiexec 2 4

* The above command will start a server with 2 processes. Then it will start the application program with 4 processes. Finally, all servers are closed.
* On supercomputers, "mpiexec" can be replaced with "srun", "jsrun" or "aprun".


---------------------------
Simple I-O
---------------------------

* This example provides a easy way for PDC beginners to write and read data with PDC servers. It can be found in obj_get_data.c
* Functions PDCobj_put_data and PDCobj_get_data are the easist way to write/read data from/to a contiguous memory buffer.
* This example writes different size of data to two objects. It then read back the data to check whether the data is correct or not.
* To run this example, use the following command lines.

.. code-block:: Bash

	make obj_get_data
	./run_test.sh ./obj_get_data
	./mpi_test.sh ./obj_get_data mpiexec 2 4


---------------------------
I-O with region mapping
---------------------------

* The simple I/O can only handles 1D data that is contiguous. PDC supports data dimension up to 3. Simple I/O functions PDCobj_put_data and PDCobj_get_data are wrappers for object create, region mapping, I/O, and object close. The examples in this section breakdowns the wrappers, which allows more flexibility.
* Check region_obj_map_2D.c and region_obj_map_3D.c for how to write 2D and 3D data.
* Generally, PDC perform I/O with the PDCbuf_obj_map, PDCreg_obtain_lock, PDCreg_release_lock, and PDCbuf_obj_unmap. The logic is similar to HDF5 dataspace and memory space. In PDC language, they are remote region and local region. The lock functions for remote regions allow PDC servers to handle concurrent requests from different clients without undefined behaviors.
* To run thie example, use the following command lines.

.. code-block:: Bash

	make
	./run_test.sh ./region_obj_map_2D
	./mpi_test.sh ./region_obj_map_2D mpiexec 2 4
	./run_test.sh ./region_obj_map_3D
	./mpi_test.sh ./region_obj_map_3D mpiexec 2 4


---------------------------
VPIC-IO and BD-CATS-IO
---------------------------

* VPIC is a particle simulation code developed at Los Alamos National Laboratory (LANL). VPIC-IO benchmark is an I/O kernel representing the I/O pattern of a space weather simulation exploring the magnetic reconnection phenomenon. More details of the simulation itself can be found at vpic.pdf.
* BD-CATS is a Big Data clustering (DBSCAN) algorithm that uses HPC systems to analyze trillions of particles. BD-CATS typically analyze data produced by simulations such as VPIC. BD-CATS-IO represents the I/O kernel of the clustering algorithm. More details of BD-CATS can be found at https://sdm.lbl.gov/~sbyna/research/papers/201511-SC15-BD-CATS.pdf 
* To run VPIC-IO and BD-CATS-IO together: Go to the bin folder first after make. Then type 

.. code-block:: Bash

	./run_multiple_test.sh ./vpicio ./bdcats


* VPIC-IO:
	* vpicio.c
	* VPIC I/O is an example for writing multiple objects using PDC, where each object is a variable of particles.
	* We collectively create containers and objects. PDC region map is used to write data to individual objects.
* BD-CATS-IO:
	* bdcats.c
	* BD-CATS-IO is an example for reading data written by VIPIC I/O.
* To run this example

.. code-block:: Bash

	cd make
	./run_multiple_test.sh ./vpicio ./bdcats

---------------------------
HACC-IO
---------------------------


* The purpose of this benchmark is to evaluate the performance of the I/O system for the Hardware Accelerated Cosmology Code (HACC) simulation. The HACC framework uses N-body techniques to simulate the formation of structure in collisionless fluids under the influence of gravity in an expanding universe.
* Each MPI rank writes 9 variables (with different data types) per particle for a total of 38 bytes.

.. code-block:: Bash

	./mpi_test.sh ./haccio mpiexec 1 num-procs num-particles
	

---------------------------
Tile-IO
---------------------------

* In Tile-IO, esach MPI rank writes one tile of a desen 2D data grid. The number of tiles in each dimension and the number of particles hold by each tile can be set through the command line arguments.
* The total data size is: (is x-tiles * y-tiles) * (x-elements-per-tile * y-elements-per-tile).
* Currently, the number of processes should be equal to (x-tiles * y-tiles). i.e., one tile per MPI rank. We plan to lift this restriction later and support overlap between tiles.

.. code-block:: Bash

	./mpi_test.sh ./tileio mpiexec num-servers num-procs x-tiles y-tiles x-elements-per-tile y-elements-per-tile
