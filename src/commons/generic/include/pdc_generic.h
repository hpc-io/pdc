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


/*******************/
/* Public Typedefs */
/*******************/
typedef int                perr_t;
typedef uint64_t           pdcid_t;
typedef unsigned long long psize_t;
typedef bool               pbool_t;

typedef int    PDC_int_t;
typedef float  PDC_float_t;
typedef double PDC_double_t;

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
    PDC_INT32      = 14, /* 32-bit integer types                                           */
    PDC_UINT32     = 15, /* 32-bit unsigned integer types                                  */
    PDC_LONG       = 16, /* long types                                                     */
    PDC_VOID_PTR   = 17, /* void pointer type                                              */
    PDC_SIZE_T     = 18, /* size_t type                                                    */
    PDC_TYPE_COUNT = 19  /* this is the number of var types and has to be the last         */
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

// clang-format off
static const size_t DataTypeSizes[PDC_TYPE_COUNT] = {
    sizeof(int),
    sizeof(float),
    sizeof(double),
    sizeof(char),
    sizeof(char *),
    sizeof(bool),
    sizeof(short),
    sizeof(unsigned int),
    sizeof(int64_t),
    sizeof(uint64_t),
    sizeof(int16_t),
    sizeof(int8_t),
    sizeof(uint8_t),
    sizeof(uint16_t),
    sizeof(int32_t),
    sizeof(uint32_t),
    sizeof(long),
    sizeof(void *),
    sizeof(size_t)
};

static const char *DataTypeNames[PDC_TYPE_COUNT] = {
    "int",
    "float",
    "double",
    "char",
    "char*",
    "bool",
    "short",
    "unsigned int",
    "int64_t",
    "uint64_t",
    "int16_t",
    "int8_t",
    "uint8_t",
    "uint16_t",
    "int32_t",
    "uint32_t",
    "long",
    "void*",
    "size_t"
};

static const char *DataTypeEnumNames[PDC_TYPE_COUNT] = {
    "PDC_INT",
    "PDC_FLOAT",
    "PDC_DOUBLE",
    "PDC_CHAR",
    "PDC_STRING",
    "PDC_BOOLEAN",
    "PDC_SHORT",
    "PDC_UINT",
    "PDC_INT64",
    "PDC_UINT64",
    "PDC_INT16",
    "PDC_INT8",
    "PDC_UINT8",
    "PDC_UINT16",
    "PDC_INT32",
    "PDC_UINT32",
    "PDC_LONG",
    "PDC_VOID_PTR",
    "PDC_SIZE_T"
};

static const char *DataTypeFormat[PDC_TYPE_COUNT] = {
    "%d",     // int
    "%f",     // float
    "%lf",    // double
    "%c",     // char
    "%s",     // char*
    "%d",     // bool (represented as an integer)
    "%hd",    // short
    "%u",     // unsigned int
    "%lld",   // int64_t
    "%llu",   // uint64_t
    "%hd",    // int16_t
    "%hhd",   // int8_t
    "%hhu",   // uint8_t
    "%hu",    // uint16_t
    "%d",     // int32_t
    "%u",     // uint32_t
    "%ld",    // long
    "%p",     // void* (pointer)
    "%zu"     // size_t
};

// clang-format on

inline const char *
get_enum_name_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return NULL;
    }
    return DataTypeEnumNames[type];
}

inline const size_t
get_size_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return 0;
    }
    return DataTypeSizes[type];
}

inline const size_t
get_size_by_class_n_type(void *data, size_t item_count, pdc_c_var_class_t pdc_class,
                         pdc_c_var_type_t pdc_type)
{
    size_t size = 0;
    if (pdc_class == PDC_CLS_SCALAR) {
        if (pdc_type == PDC_STRING) {
            size = (strlen((char *)data) + 1) * sizeof(char);
        }
        else {
            size = get_size_by_dtype(pdc_type);
        }
    }
    else if (pdc_class == PDC_CLS_ARRAY) {
        if (pdc_type == PDC_STRING) {
            char **str_arr = (char **)data;
            int    i       = 0;
            for (i = 0; i < item_count; i++) {
                size = size + (strlen(str_arr[i]) + 1) * sizeof(char);
            }
        } else {
            size = item_count * get_size_by_dtype(pdc_type);
        }
    }
    return size;
}

inline const char *
get_name_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return NULL;
    }
    return DataTypeNames[type];
}

inline pdc_c_var_type_t
get_dtype_by_enum_name(const char *enumName)
{
    for (int i = 0; i < PDC_TYPE_COUNT; i++) {
        if (strcmp(DataTypeEnumNames[i], enumName) == 0) {
            return (pdc_c_var_type_t)i;
        }
    }
    return PDC_UNKNOWN; // assuming PDC_UNKNOWN is the enum value for "unknown"
}

#endif /* PDC_GENERIC_H */