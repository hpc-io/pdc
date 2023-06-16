#ifndef PDC_GENERIC_H
#define PDC_GENERIC_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

#ifndef __cplusplus
#if __STDC_VERSION__ >= 199901L
/* C99 or later */
#include <stdbool.h>
#else
/* Pre-C99 */
typedef enum { false = 0, true = 1 } bool;
#endif
#endif

typedef enum {
    PDC_UNKNOWN    = -1, /* error                                                          */
    PDC_INT        = 0,  /* integer types     (identical to int32_t)                       */
    PDC_FLOAT      = 1,  /* floating-point types                                           */
    PDC_DOUBLE     = 2,  /* double types                                                   */
    PDC_CHAR       = 3,  /* character types                                                */
    PDC_STRING     = 4,  /* string types                                                   */
    PDC_BOOLEAN    = 5,  /* boolean types                                                  */
    PDC_SHORT      = 6,  /* short types                                                    */
    PDC_UINT       = 7,  /* unsigned integer types (identical to uint32_t)                 */
    PDC_INT64      = 8,  /* 64-bit integer types                                           */
    PDC_UINT64     = 9,  /* 64-bit unsigned integer types                                  */
    PDC_INT16      = 10, /* 16-bit integer types                                           */
    PDC_INT8       = 11, /* 8-bit integer types                                            */
    PDC_UINT8      = 12, /* 8-bit unsigned integer types                                   */
    PDC_UINT16     = 13, /* 16-bit unsigned integer types                                  */
    PDC_LONG       = 14, /* long types                                                     */
    PDC_VOID_PTR   = 15, /* void pointer type                                              */
    PDC_SIZE_T     = 16, /* size_t type                                                    */
    PDC_TYPE_COUNT = 17  /* this is the number of var types and has to be the last         */
} pdc_c_var_type_t;

typedef pdc_c_var_type_t PDC_CType;

typedef enum {
    PDC_CLS_SCALAR,
    PDC_CLS_ARRAY,
    PDC_CLS_ENUM,     // not implemented, users can use PDC_CT_INT
    PDC_CLS_STRUCT,   // not implemented, users can use embedded key value pairs for the members in a struct
    PDC_CLS_UNION,    // not implemented, users can use embedded key value pairs for the only one member value
                      // in a union.
    PDC_CLS_POINTER,  // not implemented, users can use PDC_CT_INT64_T to store the pointer address, but
                      // won't work for distributed memory.
    PDC_CLS_FUNCTION, // not implemented, users can use PDC_CT_INT64_T to store the function address, but
                      // won't work for distributed memory.
    PDC_CLS_COUNT     // just the count of the enum.
} pdc_c_var_class_t;

typedef pdc_c_var_class_t PDC_CType_Class;

const size_t get_size_by_dtype(pdc_c_var_type_t type);

const size_t get_size_by_class_n_type(void *data, size_t item_count, pdc_c_var_class_t pdc_class,
                                      pdc_c_var_type_t pdc_type);

const char *get_name_by_dtype(pdc_c_var_type_t type);

const char *get_enum_name_by_dtype(pdc_c_var_type_t type);

pdc_c_var_type_t get_dtype_by_enum_name(const char *enumName);

#endif /* PDC_GENERIC_H */