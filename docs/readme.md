## Developers' note for PDC
  + This note is for developers. It helps developers to understand the code structure of PDC code as fast as possible.
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
      
