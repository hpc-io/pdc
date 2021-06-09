================================
Hello PDC Example
================================

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