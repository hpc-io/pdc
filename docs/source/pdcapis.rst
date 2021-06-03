================
PDC user APIs
================

---------------------------
PDC general APIs
---------------------------



---------------------------
PDC container APIs
---------------------------


---------------------------
PDC object APIs
---------------------------


---------------------------
PDC region APIs
---------------------------


---------------------------
PDC property APIs
---------------------------


---------------------------
PDC query APIs
---------------------------


---------------------------
PDC hist APIs
---------------------------

---------------------------
PDC Data types
---------------------------

---------------------------
Basic types
---------------------------


---------------------------
Histogram structure
---------------------------


---------------------------
Container info
---------------------------


---------------------------
Container life time
---------------------------


---------------------------
Object property public
---------------------------


---------------------------
Object property
---------------------------


---------------------------
Object info
---------------------------


---------------------------
Object structure
---------------------------


---------------------------
Region info
---------------------------


---------------------------
Access type
---------------------------


---------------------------
Query operators
---------------------------


---------------------------
Query structures
---------------------------


---------------------------
Selection structure
---------------------------


---------------------------
Developers notes
---------------------------

* This note is for developers. It helps developers to understand the code structure of PDC code as fast as possible.
* PDC internal data structure

	* Linkedlist
		* Linkedlist is an important data structure for managing PDC IDs.
		* Overall. An PDC instance after PDC_Init() has a global variable pdc_id_list_g. See pdc_interface.h

		.. code-block:: c

			struct PDC_id_type {
    			PDC_free_t                  free_func;         /* Free function for object's of this type    */
    			PDC_type_t                  type_id;           /* Class ID for the type                      */
				//    const                     PDCID_class_t *cls;/* Pointer to ID class                        */
    			unsigned                    init_count;        /* # of times this type has been initialized  */
    			unsigned                    id_count;          /* Current number of IDs held                 */
    			pdcid_t                     nextid;            /* ID to use for the next atom                */
    			DC_LIST_HEAD(_pdc_id_info)  ids;               /* Head of list of IDs                        */
			};

			struct pdc_id_list {
    			struct PDC_id_type *PDC_id_type_list_g[PDC_MAX_NUM_TYPES];
			};
			struct pdc_id_list *pdc_id_list_g;

		* pdc_id_list_g is an array that stores the head of linked list for each types.
		* The _pdc_id_info is defined as the followng in pdc_id_pkg.h.

		.. code-block:: c

			struct _pdc_id_info {
    			pdcid_t             id;             /* ID for this info                 */
    			hg_atomic_int32_t   count;          /* ref. count for this atom         */
    			void                *obj_ptr;       /* pointer associated with the atom */
    			PDC_LIST_ENTRY(_pdc_id_info) entry;
			};

		* obj_ptr is the pointer to the item the ID refers to.
		* See pdc_linkedlist.h for implementations of search, insert, remove etc. operations

	* ID
		* ID is important for managing different data structures in PDC.
		* e.g Creating objects or containers will return IDs for them

	* pdcid_t PDC_id_register(PDC_type_t type, void *object)
		* This function maintains a linked list. Entries of the linked list is going to be the pointers to the objects. Every time we create an object ID for object using some magics. Then the linked list entry is going to be put to the beginning of the linked list.
		* type: One of the followings

		.. code-block:: c

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

		* Object: Pointer to the class instance created (bad naming, not necessarily a PDC object).


	* struct _pdc_id_info *PDC_find_id(pdcid_t idid);
		* Use ID to get struct _pdc_id_info. For most of the times, we want to locate the object pointer inside the structure. This is linear search in the linked list.
		* idid: ID you want to search.

* PDC core classes.

	* Property
		* Property in PDC serves as hint and metadata storage purposes.
		* Different types of object has different classes (struct) of properties.
		* See pdc_prop.c, pdc_prop.h and pdc_prop_pkg.h for details.
	* Container
		* Container property

		.. code-block:: c

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

		* Container structure (pdc_cont_pkg.h and pdc_cont.h)

		.. code-block:: c

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

	* Object
		* Object property See object property (Add object property link) file:///Users/kenneth/Documents/Berkeley%20Lab/pdc/docs/build/html/pdcapis.html#object-property
		* Object structure (pdc_obj_pkg.h and pdc_obj.h) See Object structure (add object structure link)
