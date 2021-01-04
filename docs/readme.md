## PDC user APIs
  + pdcid_t PDCinit(const char *pdc_name)
    - Input: pdc_name is the reference for PDC class. Recommended use "pdc"
    - Output: pdc_id used for future reference.
    - All PDC client applications must call PDCinit before using it.
    - This function will setup connections from clients to servers.
    - A valid PDC server must be running.
    - For developers: currently implemented in pdc.c.
  + perr_t PDCclose(pdcid_t pdcid)
    - Input: pdc_id returned from PDCinit.
    - Ouput: SUCCEED if no error.
    - This is a proper way to end a client-server connection for PDC.
    - A PDCinit must correspond to one PDCclose.
    - For developers: currently implemented in pdc.c.
  + pdcid_t PDCcont_create(const char *cont_name, pdcid_t cont_prop_id)
    - Input: 
      + cont_name: the name of container. e.g "c1", "c2"
      + cont_prop_id: property's ID for inheriting a PDC property for container.
    - Output: pdc_id for future referencing.
    - For developers: currently implemented in pdc_cont.c.
    
## Developers' note for PDC
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
      
