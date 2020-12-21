## Developers' note for PDC
  + This note is for developers. It helps developers to understand the code structure of PDC code as fast as possible.
  + PDC core data structure
    - Property
      * Property in PDC serves as hint and metadata storage purposes.
      * Different types of object has different classes (struct) of properties.
      * See pdc_prop.c, pdc_prop.h and pdc_prop_pkg.h for details.
    - Container
      * Container property
      ```
      struct _pdc_cont_prop {
        /* This ID is returned from PDC_find_id with an input of ID returned from PDC init. This is true for both object and container. I think it is referencing the global PDC engine through its ID (or name). */
        struct _pdc_class *pdc{
            char        *name;
            pdcid_t     local_id;
        };

      /* This ID is the one returned from PDC_id_register . This is a property ID*/
          pdcid_t           cont_prop_id;
          pdc_lifetime_t    cont_life;
      };
      ```
      * Container structure
    - Object
      * Object property
      * Object structure
      
