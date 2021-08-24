================================
Ceph Rados PDC Integration
================================

---------------------------
Connection to PDC_Codebase:
---------------------------
We have Used the Librados Library of Ceph to connect with the PDC server in C language.

* To install librados development support files for C/C++ on Debian/Ubuntu distributions, execute the following:

.. code-block:: Bash

	sudo apt-get install librados-dev

* To use Librados, you instantiate a rados_t variable (a cluster handle) and call rados_create() with a pointer to it:

.. code-block:: Bash	

 int retu;
 rados_t cluster;
 retu = rados_create(&cluster, NULL);
 if (retu < 0) {
 fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-err));
 exit(1);
 }

* Then you configure your rados_t to connect to your cluster, either by setting individual values (rados_conf_set()), using a configuration file (rados_conf_read_file()), using command line options (rados_conf_parse_argv()), or an environment variable (rados_conf_parse_env()):

.. code-block:: Bash

 retu = rados_conf_read_file(cluster, NULL);
 if (retu != 0) {
 failed = 1;
 fprintf(stderr, "rados_conf_read_file failed\n");
 goto done;
 }


* Once the cluster handle is configured, you can connect to the cluster with rados_connect():

.. code-block:: Bash

 retu = rados_connect(cluster);
 if (retu < 0) {
 fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
 exit(1);
 }

* Then you open an “IO context”, a rados_ioctx_t, with rados_ioctx_create():

.. code-block:: Bash

 retu = rados_ioctx_create(cluster, poolname, &io);
 if (retu != 0) {
 fprintf(stderr, "rados_ioctx_create failed\n");
 return 1;
 }

* Declare Global variables in pdc_server.c file : 

.. code-block:: Bash

 rados_t       cluster;
 rados_ioctx_t io;
 const char *  poolname = "data";

* Finally, you have a connection to the cluster through PDC.
------------------------------------------------
Functions Used for Storing the PDC Server data and metadata : 
------------------------------------------------
PDC_Server_rados_write() :
------------------

This function reads the data from 'buf' and writes to rados objects whose names are created as "obj_id_reg_id_batch."Depending on the offset and size values of the global region of PDC, it stores the data in Rados objects inside the pool.

* PDC_Server_rados_write(uint64_t obj_id, void *buf, uint64_t write_size, int ndim, uint64_t *offset,uint64_t *size, int o) :
	* Input:
		* obj_id is the id coming from PDC objects.
                * buf is the array buffer data that needs to be written on rados objects.
                * write_size is the total size of buf in bytes.
                * ndim is the dimension of the PDC object, which can be 1 or 2 or 3.
                * offset is the value in bytes from where it should start in buf.
                * size is in bytes.
                * o is '1' only for overlap cases, else it is '0'.
               

	* Output:
		* '0' on Success else negative value on failure.
PDC_Server_rados_read():
------------------
This Function reads back the data from the rados objects for the desired call depending on the offset, size, and ndim Value and finally writes the data in buf, which is passed to the PDC Server.

* PDC_Server_rados_read(uint64_t obj_id, void *buf, uint64_t *offset, uint64_t *size)
       * Input:
		* obj_id is the id coming from PDC objects.
                * buf is the array buffer where data read from rados object to be written.
                * offset is the value in bytes from where it should start reading in buf.
                * size is in bytes.
       * Output:
		* '0' on Success else negative value on failure.

Metadata:
--------
* Extended these  Attributes of ndim , size and offset to rados objects using setxattr() Function of librados. For Key - Value Pairs of pdc metadata, used rados_write_op_omap_set2() function to write these key - value pairs to rados objects and to read them used rados_read_op_omap_get_vals2() function of librados. 

--------------------------------
Sub Functions of Librados Used :
--------------------------------
* int rados_write_full(rados_ioctx_tio, constchar*oid, constchar*buf, size_tlen)
Write len bytes from buf into the oid object. The value of len must be <= UINT_MAX/2.
The object is filled with the provided data. If the object exists, it is atomically truncated and then written.

         * Parameters
               * io – the io context in which the write will occur 
	       * oid – the name of the object
               * buf – data to write
               * len – length of the data, in bytes
              
         * Returns : 
               0 on success, negative error code on failure

* int rados_read(rados_ioctx_tio, constchar*oid, char*buf, size_tlen, uint64_toff)
Read data from an object.
The io context determines the snapshot to read from if any was set by rados_ioctx_snap_set_read().

       * Parameters
              * io – the context in which to perform the read
              * oid – the name of the object to read from
              * buf – where to store the results
              * len – the number of bytes to read
              * off – the offset to start reading from in the object

       * Returns :
            the number of bytes read on success, negative error code on failure 
	    
* int rados_setxattr(rados_ioctx_tio, constchar*o, constchar*name, constchar*buf, size_tlen)
Set an extended attribute on an object.

         * Parameters 
                 * io – the context in which xattr is set
                 * o – name of the object
                 * name – which extended attribute to set
                 * buf – what to store in the xattr
                 * len – the number of bytes in buf
         * Returns
                 * 0 on success, negative error code on failure
		 
* int rados_getxattr(rados_ioctx_tio, constchar*o, constchar*name, char*buf, size_tlen)
Get the value of an extended attribute on an object.
           
	   * Parameters
                  * io – the context in which the attribute is read
                  * o – name of the object
                  * name – which extended attribute to read
                  * buf – where to store the result
                  * len – the size of buf in bytes
           * Returns
                  * length of xattr value on success, negative error code on failure.

* int rados_stat(rados_ioctx_tio, constchar*o, uint64_t*psize, time_t*pmtime)
Get object stats (size/mtime)
	 
	 * Parameters
                  * io – ioctx
                  * o – object name
                  * psize – where to store object size
                  * pmtime – where to store modification time
	   * Returns
                  * 0 on success, negative error code on failure
* void rados_write_op_create(rados_write_op_twrite_op, intexclusive, constchar*category)
Create the object

	 * Parameters
		* write_op – operation to add this action to
		* exclusive – set to either LIBRADOS_CREATE_EXCLUSIVE or LIBRADOS_CREATE_IDEMPOTENT will error if the object already exists.
		* category – category string (DEPRECATED, HAS NO EFFECT)
* void rados_write_op_omap_set2(rados_write_op_twrite_op, charconst*const*keys, charconst*const*vals, constsize_t*key_lens, constsize_t*val_lens, size_tnum)
Set key/value pairs on an object
	 
	 * Parameters
		* write_op – operation to add this action to
		* keys – array of null-terminated char arrays representing keys to set
		* vals – array of pointers to values to set
		* key_lens – array of lengths corresponding to each key
		* val_lens – array of lengths corresponding to each value
		* num – number of key/value pairs to set
* int rados_write_op_operate(rados_write_op_twrite_op, rados_ioctx_tio, constchar*oid, time_t*mtime, intflags)
Perform a write operation synchronously

	 * Parameters 
		write_op – operation to perform
		* io – the ioctx that the object is in
		* oid – the object id
		* mtime – the time to set the mtime to, NULL for the current time
		* flags – flags to apply to the entire operation (LIBRADOS_OPERATION_*)
		
* void rados_read_op_omap_get_vals2(rados_read_op_tread_op, constchar*start_after, constchar*filter_prefix, uint64_tmax_return, rados_omap_iter_t*iter, unsignedchar*pmore, int*prval)
Start iterating over key/value pairs on an object.
They will be returned sorted by key.
	
	* Parameters
		* read_op – operation to add this action to
		* start_after – list keys starting after start_after
		* filter_prefix – list only keys beginning with filter_prefix
		* max_return – list no more than max_return key/value pairs
		* iter – where to store the iterator
		* pmore – flag indicating whether there are more keys to fetch
		* prval – where to store the return value from this action
		
* int rados_read_op_operate(rados_read_op_tread_op, rados_ioctx_tio, constchar*oid, intflags)
Perform a read operation synchronously
	 
	 * Parameters
		* read_op – operation to perform
		* io – the ioctx that the object is in
		* oid – the object id
		* flags – flags to apply to the entire operation (LIBRADOS_OPERATION_*)

* int rados_omap_get_next2(rados_omap_iter_titer, char**key, char**val, size_t*key_len, size_t*val_len)
Get the next omap key/value pair on the object. Note that it’s perfectly safe to mix calls to rados_omap_get_next and rados_omap_get_next2.

	 * Parameters
		* iter – iterator to advance
		* key – where to store the key of the next omap entry
		* val – where to store the value of the next omap entry
		* key_len – where to store the number of bytes in key
		* val_len – where to store the number of bytes in val

* void rados_omap_get_end(rados_omap_iter_titer)
Close the omap iterator.

	 * Parameters
		* iter – the iterator to close

* typedef void *rados_omap_iter_t

		* An iterator for listing omap key/value pairs on an object. Used with rados_read_op_omap_get_keys(), rados_read_op_omap_get_vals(), rados_read_op_omap_get_vals_by_keys(), rados_omap_get_next(), and rados_omap_get_end().


	
