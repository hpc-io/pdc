#ifndef PDC_GENERIC_H
#define PDC_GENERIC_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
    PDC_CT_BOOL,
    PDC_CT_CHAR,
    PDC_CT_SHORT,
    PDC_CT_INT,
    PDC_CT_LONG,
    PDC_CT_FLOAT,
    PDC_CT_DOUBLE,
    PDC_CT_INT8_T,
    PDC_CT_INT16_T,
    PDC_CT_INT32_T,
    PDC_CT_INT64_T,
    PDC_CT_UINT8_T,
    PDC_CT_UINT16_T,
    PDC_CT_UINT32_T,
    PDC_CT_UINT64_T,
    PDC_CT_STRING,
    PDC_CT_VOID_PTR,
    PDC_CT_SIZE_T,
    PDC_CT_COUNT
} PDC_CType;

typedef enum {
    PDC_CT_CLS_SCALAR,
    PDC_CT_CLS_ARRAY,
    PDC_CT_CLS_ENUM,  // not implemented, users can use PDC_CT_INT
    PDC_CT_CLS_STRUCT, // not implemented, users can use embedded key value pairs for the members in a struct
    PDC_CT_CLS_UNION, // not implemented, users can use embedded key value pairs for the only one member value in a union.
    PDC_CT_CLS_POINTER, // not implemented, users can use PDC_CT_INT64_T to store the pointer address, but won't work for distributed memory.
    PDC_CT_CLS_FUNCTION, // not implemented, users can use PDC_CT_INT64_T to store the function address, but won't work for distributed memory.
    PDC_CT_CLS_COUNT // just the count of the enum.
} PDC_CTypeClass;

const size_t get_size_by_dtype(PDC_CType type);

const char * get_name_by_dtype(PDC_CType type);

PDC_CType get_dtype_by_name(const char* typeName);

#endif /* PDC_GENERIC_H */