================================
PDC Tools
================================

++++++++++++++++++++++++++++++++++
Build Instructions
++++++++++++++++++++++++++++++++++

.. code-block:: Bash

	$ cd tools
	$ cmake .
	$ make

++++++++++++++++++++++++++++++++++
Commands
++++++++++++++++++++++++++++++++++

pdc_ls
---------------------------
Takes in a directory containing PDC metadata checkpoints or an individual metadata checkpoint file and outputs information on objects saved in the checkpoint(s).

Usage: :code:`./pdc_ls <checkpoint> <additional arguments>`:

Arguments:

- :code:`-json <filename>`: save the output to a specified file <filename> in json format.
- :code:`-n <objectname>`: only display objects with a specific object name <objectname>. Regex matching of object names is supported.
- :code:`-i <objectid>`: only display objects with a specific object ID <objectid>. Regex matching of object IDs is supported
- :code:`-ln`: list out all object names as an additional field in the output.
- :code:`-li`: list out all object IDs as an additional field in the output.
- :code:`-s`: display summary statistics (number of objects found, number of containers found, number of regions found) as an additional field in the output.

Examples:

.. code-block:: Bash

	$ ./pdc_ls pdc_tmp -n id.*
	[INFO] File [pdc_tmp/metadata_checkpoint.0] last modified at: Fri Mar 11 14:19:13 2022

	{
		"cont_id: 1000000":	[{
				"obj_id":	1000007,
				"app_name":	"VPICIO",
				"obj_name":	"id11",
				"user_id":	0,
				"tags":	"ag0=1",
				"data_type":	"PDC_INT",
				"num_dims":	1,
				"dims":	[8388608],
				"time_step":	0,
				"region_list_info":	[{
						"storage_loc":	"/user/pdc_data/1000007/server0/s0000.bin",
						"offset":	33554432,
						"num_dims":	1,
						"start":	[0],
						"count":	[8388608],
						"unit_size":	4,
						"data_loc_type":	"PDC_NONE"
					}]
			}, {
				"obj_id":	1000008,
				"app_name":	"VPICIO",
				"obj_name":	"id22",
				"user_id":	0,
				"tags":	"ag0=1",
				"data_type":	"PDC_INT",
				"num_dims":	1,
				"dims":	[8388608],
				"time_step":	0,
				"region_list_info":	[{
						"storage_loc":	"/user/pdc_data/1000008/server0/s0000.bin",
						"offset":	33554432,
						"num_dims":	1,
						"start":	[0],
						"count":	[8388608],
						"unit_size":	4,
						"data_loc_type":	"PDC_NONE"
					}]
			}]
	}

.. code-block:: Bash
	
	$ ./pdc_ls pdc_tmp -n obj-var-p.* -ln -li
	[INFO] File [pdc_tmp/metadata_checkpoint.0] last modified at: Fri Mar 11 14:19:13 2022

	{
		"cont_id: 1000000":	[{
				"obj_id":	1000004,
				"app_name":	"VPICIO",
				"obj_name":	"obj-var-pxx",
				"user_id":	0,
				"tags":	"ag0=1",
				"data_type":	"PDC_FLOAT",
				"num_dims":	1,
				"dims":	[8388608],
				"time_step":	0,
				"region_list_info":	[{
						"storage_loc":	"/user/pdc_data/1000004/server0/s0000.bin",
						"offset":	33554432,
						"num_dims":	1,
						"start":	[0],
						"count":	[8388608],
						"unit_size":	4,
						"data_loc_type":	"PDC_NONE"
					}]
			}, {
				"obj_id":	1000005,
				"app_name":	"VPICIO",
				"obj_name":	"obj-var-pyy",
				"user_id":	0,
				"tags":	"ag0=1",
				"data_type":	"PDC_FLOAT",
				"num_dims":	1,
				"dims":	[8388608],
				"time_step":	0,
				"region_list_info":	[{
						"storage_loc":	"/user/pdc_data/1000005/server0/s0000.bin",
						"offset":	33554432,
						"num_dims":	1,
						"start":	[0],
						"count":	[8388608],
						"unit_size":	4,
						"data_loc_type":	"PDC_NONE"
					}]
			}, {
				"obj_id":	1000006,
				"app_name":	"VPICIO",
				"obj_name":	"obj-var-pzz",
				"user_id":	0,
				"tags":	"ag0=1",
				"data_type":	"PDC_FLOAT",
				"num_dims":	1,
				"dims":	[8388608],
				"time_step":	0,
				"region_list_info":	[{
						"storage_loc":	"/user/pdc_data/1000006/server0/s0000.bin",
						"offset":	33554432,
						"num_dims":	1,
						"start":	[0],
						"count":	[8388608],
						"unit_size":	4,
						"data_loc_type":	"PDC_NONE"
					}]
			}],
		"all_obj_names":	["obj-var-pxx", "obj-var-pyy", "obj-var-pzz"],
		"all_obj_ids":	[1000004, 1000005, 1000006]
	}


pdc_import
---------------------------
Takes in file containing line separated paths to HDF5 files and converts those HDF5 files to a PDC checkpoint.

Usage: :code:`./pdc_ls <file_list> <additional arguments>`:

Arguments:

- :code:`-a <appname>`: Uses the specified <appname> as application name when creating PDC objects.
- :code:`-o`: Specifies whether or not to overwrite pre-existing PDC objects when writing a PDC object that already exists.
Examples:

.. code-block:: Bash

	$ srun -N 1 -n 1 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 --overlap /path/to/pdc_server.exe &
	==PDC_SERVER[0]: using [./pdc_tmp/] as tmp dir, 1 OSTs, 1 OSTs per data file, 0% to BB
	==PDC_SERVER[0]: using ofi+tcp
	==PDC_SERVER[0]: without multi-thread!
	==PDC_SERVER[0]: Read cache enabled!
	==PDC_SERVER[0]: Successfully established connection to 0 other PDC servers
	==PDC_SERVER[0]: Server ready!

	$ srun -N 1 -n 1 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 --overlap ./pdc_import file_names_list
	==PDC_CLIENT: PDC_DEBUG set to 0!
	==PDC_CLIENT[0]: Found 1 PDC Metadata servers, running with 1 PDC clients
	==PDC_CLIENT: using ofi+tcp
	==PDC_CLIENT[0]: Client lookup all servers at start time!
	==PDC_CLIENT[0]: using [./pdc_tmp] as tmp dir, 1 clients per server
	Running with 1 clients, 1 files
	Importer 0: I will import 1 files
	Importer 0: [../../test.h5] 
	Importer 0: processing [../../test.h5]
	Importer 0: Created container [/]

	==PDC_SERVER[0]: Checkpoint file [./pdc_tmp/metadata_checkpoint.0]
	Import 8 datasets with 1 ranks took 0.93 seconds.


pdc_export
---------------------------
Converts PDC metadata checkpoint to a file of specified format. Currently only HDF5 is supported.

Usage: :code:`./pdc_ls <checkpoint> <additional arguments>`:

Arguments:

- :code:`-f <format>`: Uses the specified export <format>. Currently only supports HDF5 exports.

Examples:

.. code-block:: Bash

	$ srun -N 1 -n 1 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 --overlap /path/to/pdc_server.exe &
	==PDC_SERVER[0]: using [./pdc_tmp/] as tmp dir, 1 OSTs, 1 OSTs per data file, 0% to BB
	==PDC_SERVER[0]: using ofi+tcp
	==PDC_SERVER[0]: without multi-thread!
	==PDC_SERVER[0]: Read cache enabled!
	==PDC_SERVER[0]: Successfully established connection to 0 other PDC servers
	==PDC_SERVER[0]: Server ready!

	$ srun -N 1 -n 1 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 --overlap ./pdc_export pdc_tmp
	==PDC_CLIENT: PDC_DEBUG set to 0!
	==PDC_CLIENT[0]: Found 1 PDC Metadata servers, running with 1 PDC clients
	==PDC_CLIENT: using ofi+tcp
	==PDC_CLIENT[0]: Client lookup all servers at start time!
	==PDC_CLIENT[0]: using [./pdc_tmp] as tmp dir, 1 clients per server
	[INFO] File [pdc_tmp/metadata_checkpoint.0] last modified at: Mon May  9 06:17:18 2022

	POSIX read from file offset 117478480, region start = 0, region size = 8388608
	POSIX read from file offset 130024208, region start = 0, region size = 8388608
	POSIX read from file offset 130057104, region start = 0, region size = 8388608
	POSIX read from file offset 130056720, region start = 0, region size = 8388608
	POSIX read from file offset 130023696, region start = 0, region size = 8388608
	POSIX read from file offset 130056592, region start = 0, region size = 8388608
	POSIX read from file offset 130056720, region start = 0, region size = 8388608
	POSIX read from file offset 130023696, region start = 0, region size = 8388608


.. warning::
    PDC tools currently does not support compound data types and will have unexpected behavior when attempting to work with compound data types.




