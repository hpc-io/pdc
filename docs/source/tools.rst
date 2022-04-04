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
