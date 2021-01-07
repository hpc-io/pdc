# PDC user APIs
  ## PDC general APIs
  + pdcid_t PDCinit(const char *pdc_name)
    - Input: 
      + pdc_name is the reference for PDC class. Recommended use "pdc"
    - Output: 
      + PDC class ID used for future reference.
    - All PDC client applications must call PDCinit before using it. This function will setup connections from clients to servers. A valid PDC server must be running.
    - For developers: currently implemented in pdc.c.
  + perr_t PDCclose(pdcid_t pdcid)
    - Input: 
      + PDC class ID returned from PDCinit.
    - Ouput: 
      + SUCCEED if no error, otherwise FAIL.
    - This is a proper way to end a client-server connection for PDC. A PDCinit must correspond to one PDCclose.
    - For developers: currently implemented in pdc.c.
  ## PDC container APIs
  + pdcid_t PDCcont_create(const char *cont_name, pdcid_t cont_prop_id)
    - Input: 
      + cont_name: the name of container. e.g "c1", "c2"
      + cont_prop_id: property ID for inheriting a PDC property for container.
    - Output: pdc_id for future referencing of this container, returned from PDC servers.
    - Create a PDC container for future use. 
    - For developers: currently implemented in pdc_cont.c. This function will send a name to server and receive an container id. This function will allocate necessary memories and initialize properties for a container.
  + pdcid_t PDCcont_create_col(const char *cont_name, pdcid_t cont_prop_id)
    - Input: 
      + cont_name: the name to be assigned to a container. e.g "c1", "c2"
      + cont_prop_id: property ID for inheriting a PDC property for container.
    - Output: pdc_id for future referencing.
    - Exactly the same as PDCcont_create, except all processes must call this function collectively. Create a PDC container for future use collectively.
    - For developers: currently implemented in pdc_cont.c.
  + pdcid_t PDCcont_open(const char *cont_name, pdcid_t pdc)
    - Input: 
      + cont_name: the name of container used for PDCcont_create.
      + pdc: PDC class ID returned from PDCinit.
    - Output:
      + error code. FAIL OR SUCCESS
    - Open a container. Must make sure a container named cont_name is properly created (registered by PDCcont_create at remote servers).
    - For developers: currently implemented in pdc_cont.c. This function will make sure the metadata for a container is returned from servers. For collective operations, rank 0 is going to broadcast this metadata ID to the rest of processes. A struct _pdc_cont_info is created locally for future reference.
  + perr_t PDCcont_close(pdcid_t id)
    - Input: 
      + container ID, returned from PDCcont_create.
    - Output: 
      + error code, SUCCESS or FAIL.
    - Correspond to PDCcont_open. Must be called only once when a container is no longer used in the future.
    - For developers: currently implemented in pdc_cont.c. The reference counter of a container is decremented. When the counter reaches zero, the memory of the container can be freed later.
  + struct pdc_cont_info *PDCcont_get_info(const char *cont_name)
     - Input: 
       + name of the container
     - Output: 
       + Pointer to a new structure that contains the container information
     ```
     struct pdc_cont_info {
          /*Inherited from property*/
          char                   *name;
          /*Registered using PDC_id_register */
          pdcid_t                 local_id;
          /* Need to register at server using function PDC_Client_create_cont_id */
          uint64_t                meta_id;
     };
     ```
     - Correspond to PDCcont_open. Must be called only once when a container is no longer used in the future.
     - For developers: See pdc_cont.c. Use name to search for pdc_id first by linked list lookup. Make a copy of the metadata to the newly malloced structure.
  + perr_t PDCcont_persist(pdcid_t cont_id)
    - Input:
      + cont_id: container ID, returned from PDCcont_create.
    - Output: 
      + error code, SUCCESS or FAIL.
    - Make a PDC container persist.
    - For developers, see pdc_cont.c. Set the container life field PDC_PERSIST.
  + perr_t PDCprop_set_cont_lifetime(pdcid_t cont_prop, pdc_lifetime_t cont_lifetime)
    - Input:
      + cont_prop: Container property pdc_id
      + cont_lifetime: See below
      ```
      typedef enum {
        PDC_PERSIST,
        PDC_TRANSIENT
      } pdc_lifetime_t;
      ```
    - Output: 
      + error code, SUCCESS or FAIL.
    - Set container life time for a property.
    - For developers, see pdc_cont.c.
  + pdcid_t PDCcont_get_id(const char *cont_name, pdcid_t pdc_id)
    - Input:
      + cont_name: Name of the container
      + pdc_id: PDC class ID, returned by PDCinit
    - Output: 
      + container ID created locally
    - Get container ID by name.
    - For developers, see pdc_client_connect.c. It will query the servers for container information and create a container structure locally.
  + perr_t PDCcont_del(pdcid_t cont_id)
    - Input:
      + cont_id: container ID, returned from PDCcont_create.
    - Output: 
      + error code, SUCCESS or FAIL.
    - Deleta a container
    - For developers: see pdc_client_connect.c. Need to send RPCs to servers for metadata update.
  + perr_t PDCcont_put_tag(pdcid_t cont_id, char *tag_name, void *tag_value, psize_t value_size)
    - Input:
      + cont_id: Container ID, returned from PDCcont_create.
      + tag_name: Name of the tag
      + tag_value: Value to be written under the tag
      + value_size: Number of bytes for the tag_value (tag_size may be more informative)
    - Output: 
      + error code, SUCCESS or FAIL.
    - Record a tag_value under the name tag_name for the container referenced by cont_id.
    - For developers: see pdc_client_connect.c. Need to send RPCs to servers for metadata update.
  + perr_t PDCcont_get_tag(pdcid_t cont_id, char *tag_name, void **tag_value, psize_t *value_size)
    - Input:
      + cont_id: Container ID, returned from PDCcont_create.
      + tag_name: Name of the tag
      + value_size: Number of bytes for the tag_value (tag_size may be more informative)
    - Output:
      + tag_value: Pointer to the value to be read under the tag
      + error code, SUCCESS or FAIL.
    - Retrieve a tag value to the memory space pointed by the tag_value under the name tag_name for the container referenced by cont_id.
    - For developers: see pdc_client_connect.c. Need to send RPCs to servers for metadata retrival.
  + perr_t PDCcont_del_tag(pdcid_t cont_id, char *tag_name)
    - Input:
      + cont_id: Container ID, returned from PDCcont_create.
      + tag_name: Name of the tag
    - Output: 
      + error code, SUCCESS or FAIL.
    - Delete a tag for a container by name
    - For developers: see pdc_client_connect.c. Need to send RPCs to servers for metadata update.
  + perr_t PDCcont_put_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids)
    - Input:
      + cont_id: Container ID, returned from PDCcont_create.
      + nobj: Number of objects to be written
      + obj_ids: Pointers to the object IDs
    - Output: 
      + error code, SUCCESS or FAIL.
    - Put an array of objects to a container.
    - For developers: see pdc_client_connect.c. Need to send RPCs to servers for metadata update.
  + perr_t PDCcont_get_objids(pdcid_t cont_id ATTRIBUTE(unused), int *nobj ATTRIBUTE(unused), pdcid_t **obj_ids ATTRIBUTE(unused) )
     TODO:
  + perr_t PDCcont_del_objids(pdcid_t cont_id, int nobj, pdcid_t *obj_ids)
    - Input:
      + cont_id: Container ID, returned from PDCcont_create.
      + nobj: Number of objects to be deleted
      + obj_ids: Pointers to the object IDs
    - Output: 
      + error code, SUCCESS or FAIL.
    - Delete an array of objects to a container.
    - For developers: see pdc_client_connect.c. Need to send RPCs to servers for metadata update.
  ## PDC object APIs
  + pdcid_t PDCobj_create(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id)
    - Input:
      + cont_id: Container ID, returned from PDCcont_create.
      + obj_name: Name of objects to be created
      + obj_prop_id: Property ID to be inherited from.
    - Output: 
      + Local object ID
    - Create a PDC object.
    - For developers: see pdc_obj.c. This process need to send the name of the object to be created to the servers. Then it will receive an object ID. The object structure will inherit attributes from its container and  input object properties.
  + PDCobj_create_mpi(pdcid_t cont_id, const char *obj_name, pdcid_t obj_prop_id, int rank_id, MPI_Comm comm)
    - Input:
      + cont_id: Container ID, returned from PDCcont_create.
      + obj_name: Name of objects to be created
      + rank_id: Which rank ID the object is placed to
      + comm: MPI communicator for the rank_id
    - Output: 
      + Local object ID
    - Create a PDC object at the rank_id in the communicator comm. This function is a colllective operation.
    - For developers: see pdc_mpi.c. If rank_id equals local process rank, then a local object is created. Otherwise we create a global object. The object metadata ID is broadcasted to all processes if a global object is created using MPI_Bcast.
  + pdcid_t PDCobj_open(const char *obj_name, pdcid_t pdc)
    - Input:
      + obj_name: Name of objects to be created
      + pdc: PDC class ID, returned from PDCInit
    - Output: 
      + Local object ID
    - Open a PDC ID created previously by name.
    - For developers: see pdc_obj.c. Need to communicate with servers for metadata of the object.
  + perr_t PDCobj_close(pdcid_t obj_id)
    - Input:
      + obj_id: Local object ID to be closed.
    - Output:
      + error code, SUCCESS or FAIL.
    - Close an object. Must do this after open an object.
    - For developers: see pdc_obj.c. Dereference an object by reducing its reference counter.
  + struct pdc_obj_info *PDCobj_get_info(const char *obj_name)
    - Input:
      + obj_name: Name of object
    - Output:
      + object information
      ```
      struct pdc_obj_info  {
          /* Directly coped from user argument at object creation. */
          char                   *name;
          /* 0 for location = PDC_OBJ_LOAL. 
           * When PDC_OBJ_GLOBAL = 1, use PDC_Client_send_name_recv_id to retrieve ID. */
          pdcid_t                 meta_id;
          /* Registered using PDC_id_register */
          pdcid_t                 local_id;
          /* Set to 0 at creation time. *
          int                     server_id;
          /* Object property. Directly copy from user argument at object creation. */
          struct pdc_obj_prop    *obj_pt;
      };
      ```
    - Get a pointer to a structure that describes the object metadata.
    - For developers: see pdc_obj.c. Local linked list search for object ID first. Then pull out local object metadata by ID.
  + pdcid_t PDCobj_put_data(const char *obj_name, void *data, uint64_t size, pdcid_t cont_id)
    - Input:
      + obj_name: Name of object
      + data: Pointer to data memory
      + size: Size of data
      + cont_id: Container ID of this object
    - Output:
      + error code, SUCCESS or FAIL.
    - Write data to an object.
    - For developers: see pdc_client_connect.c. Nedd to send RPCs to servers for this request.
  + perr_t PDCobj_get_data(pdcid_t obj_id, void **data, uint64_t *size)
    - Input:
      + obj_id: Local object ID
      + size: Size of data
    - Output:
      + data: Pointer to data to be filled
      + error code, SUCCESS or FAIL.
    - Read data from an object.
    - For developers: see pdc_client_connect.c. Use PDC_obj_get_info to retrieve name. Then forward name to servers to fulfill requests.
  + perr_t PDCobj_del_data(pdcid_t obj_id)
    - Input:
      + obj_id: Local object ID
    - Output:
      + error code, SUCCESS or FAIL.
    - Delete data from an object.
    - For developers: see pdc_client_connect.c. Use PDC_obj_get_info to retrieve name. Then forward name to servers to fulfill requests.
  + perr_t PDCobj_put_tag(pdcid_t obj_id, char *tag_name, void *tag_value, psize_t value_size)
    - Input:
      + obj_id: Local object ID
      + tag_name: Name of the tag to be entered
      + tag_value: Value of the tag
      + value_size: Number of bytes for the tag_value
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the tag value for a tag
    - For developers: see pdc_client_connect.c. Need to use PDC_add_kvtag to submit RPCs to the servers for metadata update.
  + perr_t PDCobj_get_tag(pdcid_t obj_id, char *tag_name, void **tag_value, psize_t *value_size)
    - Input:
      + obj_id: Local object ID
      + tag_name: Name of the tag to be entered
      + tag_value: Value of the tag
      + value_size: Number of bytes for the tag_value
    - Output:
      + error code, SUCCESS or FAIL.
    - Get the tag value for a tag
    - For developers: see pdc_client_connect.c. Need to use PDC_get_kvtag to submit RPCs to the servers for metadata update.
  + perr_t PDCobj_del_tag(pdcid_t obj_id, char *tag_name)
    - Input:
      + obj_id: Local object ID
      + tag_name: Name of the tag to be entered
    - Output:
      + error code, SUCCESS or FAIL.
    - Delete a tag.
    - For developers: see pdc_client_connect.c. Need to use PDCtag_delete to submit RPCs to the servers for metadata update.
  ## PDC region APIs
  + pdcid_t PDCregion_create(psize_t ndims, uint64_t *offset, uint64_t *size)
    - Input:
      + ndims: Number of dimensions
      + offset: Array of offsets
      + size: Array of offset length
    - Output:
      + Region ID
    - Create a region with ndims offset/length pairs
    - For developers: see pdc_region.c. Need to use PDC_get_kvtag to submit RPCs to the servers for metadata update.
  + void PDCregion_free(struct pdc_region_info *region)
    - Input:
      + region_id: PDC region info
      ```
      struct pdc_region_info {
        pdcid_t               local_id;
        struct _pdc_obj_info *obj;
        size_t                ndim;
        uint64_t             *offset;
        uint64_t             *size;
        bool                  mapping;
        int                   registered_op;
        void                 *buf;
      };
      ```
    - Output:
      + None
    - Close a PDC region
    - For developers: see pdc_client_server_common.c. Free offset and size arrays.
  + perr_t PDCbuf_obj_map(void *buf, pdc_var_type_t local_type, pdcid_t local_reg, pdcid_t remote_obj, pdcid_t remote_reg)
    - Input:
      + buf: Memory buffer
      + local_type: One of the followings
      ```
      typedef enum {
        PDC_UNKNOWN      = -1, /* error                                      */
        PDC_INT          = 0,  /* integer types                              */
        PDC_FLOAT        = 1,  /* floating-point types                       */
        PDC_DOUBLE       = 2,  /* double types                               */
        PDC_CHAR         = 3,  /* character types                            */
        PDC_COMPOUND     = 4,  /* compound types                             */
        PDC_ENUM         = 5,  /* enumeration types                          */
        PDC_ARRAY        = 6,  /* Array types                                */
        PDC_UINT         = 7,  /* unsigned integer types                     */
        PDC_INT64        = 8,  /* 64-bit integer types                       */
        PDC_UINT64       = 9,  /* 64-bit unsigned integer types              */
        PDC_INT16        = 10, 
        PDC_INT8         = 11,
        NCLASSES         = 12  /* this must be last                          */
      } pdc_var_type_t;
      ```
      + local_reg: Local region ID
      + remote_obj: Remote object ID
      + remote_reg: Remote region ID
    - Output:
      + Region ID
    - Create a region with ndims offset/length pairs
    - For developers: see pdc_region.c. Need to use PDC_get_kvtag to submit RPCs to the servers for metadata update.
## PDC property APIs
  + pdcid_t PDCprop_create(pdc_prop_type_t type, pdcid_t pdcid)
    - Input:
      + type: one of the followings
      ```
      typedef enum {
          PDC_CONT_CREATE = 0,
          PDC_OBJ_CREATE
      } pdc_prop_type_t;
      ```
      - pdcid: PDC class ID, returned by PDCInit.
    - Output:
      + PDC property ID
    - Initialize a property structure.
    - For developers: see pdc_prop.c.
  + perr_t PDCprop_close(pdcid_t id)
    - Input:
      + id: PDC property ID
    - Output:
      + error code, SUCCESS or FAIL.
    - Close a PDC property after openning.
    - For developers: see pdc_prop.c. Decrease reference counter for this property.
  + perr_t PDCprop_set_obj_user_id(pdcid_t obj_prop, uint32_t user_id)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + user_id: PDC user ID
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the user ID of an object.
    - For developers: see pdc_obj.c. Update the user_id field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_data_loc(pdcid_t obj_prop, char *loc) 
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + loc: location
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the location of an object.
    - For developers: see pdc_obj.c. Update the data_loc field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_app_name(pdcid_t obj_prop, char *app_name)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + app_name: application name
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the application name of an object.
    - For developers: see pdc_obj.c. Update the app_name field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_time_step(pdcid_t obj_prop, uint32_t time_step)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + time_step: time step
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the time step of an object.
    - For developers: see pdc_obj.c. Update the time_step field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_tags(pdcid_t obj_prop, char *tags)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + tags: tags
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the tags of an object.
    - For developers: see pdc_obj.c. Update the tags field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + ndim: number of dimensions
      + dims: array of dimensions
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the dimensions of an object.
    - For developers: see pdc_obj.c. Update the obj_prop_pub field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_dims(pdcid_t obj_prop, PDC_int_t ndim, uint64_t *dims)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + ndim: number of dimensions
      + dims: array of dimensions
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the dimensions of an object.
    - For developers: see pdc_obj.c. Update the obj_prop_pub->ndim and obj_prop_pub->dims fields under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_type(pdcid_t obj_prop, pdc_var_type_t type)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + type: one of the followings
      ```
      typedef enum {
        PDC_UNKNOWN      = -1, /* error                                      */
        PDC_INT          = 0,  /* integer types                              */
        PDC_FLOAT        = 1,  /* floating-point types                       */
        PDC_DOUBLE       = 2,  /* double types                               */
        PDC_CHAR         = 3,  /* character types                            */
        PDC_COMPOUND     = 4,  /* compound types                             */
        PDC_ENUM         = 5,  /* enumeration types                          */
        PDC_ARRAY        = 6,  /* Array types                                */
        PDC_UINT         = 7,  /* unsigned integer types                     */
        PDC_INT64        = 8,  /* 64-bit integer types                       */
        PDC_UINT64       = 9,  /* 64-bit unsigned integer types              */
        PDC_INT16        = 10, 
        PDC_INT8         = 11,
        NCLASSES         = 12  /* this must be last                          */
      } pdc_var_type_t;
      ```
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the type of an object.
    - For developers: see pdc_obj.c. Update the obj_prop_pub->type field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + perr_t PDCprop_set_obj_buf(pdcid_t obj_prop, void *buf)
    - Input:
      + obj_prop: PDC property ID (has to be an object)
      + buf: user memory buffer
    - Output:
      + error code, SUCCESS or FAIL.
    - Set the user memory buffer of an object.
    - For developers: see pdc_obj.c. Update the buf field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + pdcid_t PDCprop_obj_dup(pdcid_t prop_id)
    - Input:
      + prop_id: PDC property ID (has to be an object)
    - Output:
      + a new property ID copied.
    - Duplicate an object property
    - For developers: see pdc_prop.c. Update the buf field under struct _pdc_obj_prop. See developer's note for more details about this structure.
  + pdc_query_t *PDCquery_create(pdcid_t obj_id, pdc_query_op_t op, pdc_var_type_t type, void *value)
    - Input:
      + obj_id: local PDC object ID
      + op: one of the followings
      ```
      typedef enum { 
          PDC_OP_NONE = 0, 
          PDC_GT      = 1, 
          PDC_LT      = 2, 
          PDC_GTE     = 3, 
          PDC_LTE     = 4, 
          PDC_EQ      = 5
      } pdc_query_op_t;
      ```
      + type: one of the followings
      ```
      typedef enum {
        PDC_UNKNOWN      = -1, /* error                                      */
        PDC_INT          = 0,  /* integer types                              */
        PDC_FLOAT        = 1,  /* floating-point types                       */
        PDC_DOUBLE       = 2,  /* double types                               */
        PDC_CHAR         = 3,  /* character types                            */
        PDC_COMPOUND     = 4,  /* compound types                             */
        PDC_ENUM         = 5,  /* enumeration types                          */
        PDC_ARRAY        = 6,  /* Array types                                */
        PDC_UINT         = 7,  /* unsigned integer types                     */
        PDC_INT64        = 8,  /* 64-bit integer types                       */
        PDC_UINT64       = 9,  /* 64-bit unsigned integer types              */
        PDC_INT16        = 10, 
        PDC_INT8         = 11,
        NCLASSES         = 12  /* this must be last                          */
      } pdc_var_type_t;
      ```
      + value: constraint value.
    - Output:
      + a new query
      ```
      typedef struct pdc_query_t {
          pdc_query_constraint_t *constraint;
          struct pdc_query_t     *left;
          struct pdc_query_t     *right;
          pdc_query_combine_op_t  combine_op;
          struct pdc_region_info *region;             // used only on client
          void                   *region_constraint;  // used only on server
          pdc_selection_t        *sel;
      } pdc_query_t;
      ```
    - Create a PDC query.
    - For developers, see pdc_query.c. The new query structure is filled. The constraint->value field is copied from the value argument.
  + void PDCquery_free(pdc_query_t *query)
    - Input:
      + query: PDC query from PDCquery_create
    - Free a query structure
# Developers' note for PDC
  + This note is for developers. It helps developers to understand the code structure of PDC code as fast as possible.
  + PDC internal data structure
    - Linkedlist
      * Linkedlist is an important data structure for managing PDC IDs.
      * Overall. An PDC instance after PDC_Init() has a global variable pdc_id_list_g. See pdc_interface.h
      ```
      struct PDC_id_type {
          PDC_free_t                  free_func;         /* Free function for object's of this type    */
          PDC_type_t                  type_id;           /* Class ID for the type                      */
      //    const                     PDCID_class_t *cls;/* Pointer to ID class                        */
          unsigned                    init_count;        /* # of times this type has been initialized  */
          unsigned                    id_count;          /* Current number of IDs held                 */
          pdcid_t                     nextid;            /* ID to use for the next atom                */
          PDC_LIST_HEAD(_pdc_id_info)  ids;               /* Head of list of IDs                        */
      };

      struct pdc_id_list {
          struct PDC_id_type *PDC_id_type_list_g[PDC_MAX_NUM_TYPES];
      };
      struct pdc_id_list *pdc_id_list_g;
      ```
      * pdc_id_list_g is an array that stores the head of linked list for each types.
      * The _pdc_id_info is defined as the followng in pdc_id_pkg.h.
      ```
      struct _pdc_id_info {
          pdcid_t             id;             /* ID for this info                 */
          hg_atomic_int32_t   count;          /* ref. count for this atom         */
          void                *obj_ptr;       /* pointer associated with the atom */
          PDC_LIST_ENTRY(_pdc_id_info) entry;
      };
      ```
      * obj_ptr is the pointer to the item the ID refers to.
      * See pdc_linkedlist.h for implementations of search, insert, remove etc. operations
    - ID
      * ID is important for managing different data structures in PDC.
      * e.g Creating objects or containers will return IDs for them
    - pdcid_t PDC_id_register(PDC_type_t type, void *object)
      * This function maintains a linked list. Entries of the linked list is going to be the pointers to the objects. Every time we create an object ID for object using some magics. Then the linked list entry is going to be put to the beginning of the linked list.
      * type: One of the followings
      ```
      typedef enum {
        PDC_BADID        = -1,  /* invalid Type                                */
        PDC_CLASS        = 1,   /* type ID for PDC                             */
        PDC_CONT_PROP    = 2,   /* type ID for container property              */
        PDC_OBJ_PROP     = 3,   /* type ID for object property                 */
        PDC_CONT         = 4,   /* type ID for container                       */
        PDC_OBJ          = 5,   /* type ID for object                          */
        PDC_REGION       = 6,   /* type ID for region                          */
        PDC_NTYPES       = 7    /* number of library types, MUST BE LAST!      */
      } PDC_type_t;
      ```
      * Object: Pointer to the class instance created ( bad naming, not necessarily a PDC object).
    - struct _pdc_id_info *PDC_find_id(pdcid_t idid);
      * Use ID to get struct _pdc_id_info. For most of the times, we want to locate the object pointer inside the structure. This is linear search in the linked list.
      * idid: ID you want to search.

  + PDC core classes.
    - Property
      * Property in PDC serves as hint and metadata storage purposes.
      * Different types of object has different classes (struct) of properties.
      * See pdc_prop.c, pdc_prop.h and pdc_prop_pkg.h for details.
    - Container
      * Container property
      ```
      struct _pdc_cont_prop {
          /* This class ID is returned from PDC_find_id with an input of ID returned from PDC init. This is true for both object and container. 
           *I think it is referencing the global PDC engine through its ID (or name). */
         struct _pdc_class *pdc{
             /* PDC class instance name*/
             char        *name;
             /* PDC class instance ID. For most of the times, we only have 1 PDC class instance. This is like a global variable everywhere.*/
             pdcid_t     local_id;
          };
          /* This ID is the one returned from PDC_id_register . This is a property ID type. 
           * Some kind of hashing algorithm is used to generate it at property create time*/
          pdcid_t           cont_prop_id;
          /* Not very important */          pdc_lifetime_t    cont_life;
      };
      ```
      * Container structure (pdc_cont_pkg.h and pdc_cont.h)
      ```
      struct _pdc_cont_info {
          struct pdc_cont_info    *cont_info_pub {
              /*Inherited from property*/
              char                   *name;
              /*Registered using PDC_id_register */
              pdcid_t                 local_id;
              /* Need to register at server using function PDC_Client_create_cont_id */
              uint64_t                meta_id;
          };
          /* Pointer to container property.
           * This struct is copied at create time.*/
          struct _pdc_cont_prop   *cont_pt;
      };
      ```
    - Object
      * Object property
      ```
      struct _pdc_obj_prop {
          /* Suffix _pub probably means public attributes to be accessed. */
          struct pdc_obj_prop *obj_prop_pub {
              /* This ID is the one returned from PDC_id_register . This is a property ID*/
              pdcid_t           obj_prop_id;
              /* object dimensions */
              size_t            ndim;
              uint64_t         *dims;
              pdc_var_type_t    type;
          };
          /* This ID is returned from PDC_find_id with an input of ID returned from PDC init. 
           * This is true for both object and container. 
           * I think it is referencing the global PDC engine through its ID (or name). */
          struct _pdc_class   *pdc{
              char        *name;
              pdcid_t     local_id;
          };
          /* The following are created with NULL values in the PDC_obj_create function. */
          uint32_t             user_id;
          char                *app_name;
          uint32_t             time_step;
          char                *data_loc;
          char                *tags;
          void                *buf;
          pdc_kvtag_t         *kvtag;

          /* The following have been added to support of PDC analysis and transforms.
             Will add meanings to them later, they are not critical. */
          size_t            type_extent;
          uint64_t          locus;
          uint32_t          data_state;
          struct _pdc_transform_state transform_prop{
              _pdc_major_type_t storage_order;
              pdc_var_type_t    dtype;
              size_t            ndim;
              uint64_t          dims[4];
              int               meta_index; /* transform to this state */
          };
      };
      ```
      * Object structure (pdc_obj_pkg.h and pdc_obj.h)
      ```
      struct _pdc_obj_info {
          /* Public properties */
          struct pdc_obj_info    *obj_info_pub {
              /* Directly copied from user argument at object creation. */
              char                   *name;
              /* 0 for location = PDC_OBJ_LOAL. 
               * When PDC_OBJ_GLOBAL = 1, use PDC_Client_send_name_recv_id to retrieve ID. */
              pdcid_t                 meta_id;
              /* Registered using PDC_id_register */
              pdcid_t                 local_id;
              /* Set to 0 at creation time. *
              int                     server_id;
              /* Object property. Directly copy from user argument at object creation. */
              struct pdc_obj_prop    *obj_pt;
          };
          /* Argument passed to obj create*/
          _pdc_obj_location_t     location enum {
              /* Either local or global */
              PDC_OBJ_GLOBAL,
              PDC_OBJ_LOCAL
          }
          /* May be used or not used depending on which creation function called. */
          void                   *metadata;
          /* The container pointer this object sits in. Copied*/
          struct _pdc_cont_info  *cont;
          /* Pointer to object property. Copied*/
          struct _pdc_obj_prop   *obj_pt;
          /* Linked list for region, initialized with NULL at create time.*/
          struct region_map_list *region_list_head {
              pdcid_t                orig_reg_id;
              pdcid_t                des_obj_id;
              pdcid_t                des_reg_id;
              /* Double linked list usage*/
              struct region_map_list *prev;
              struct region_map_list *next;
          };
      };
      ```
      
