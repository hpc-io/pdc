#ifndef PDC_GENERIC_H
#define PDC_GENERIC_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
    CHAR,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    INT8_T,
    INT16_T,
    INT32_T,
    INT64_T,
    UINT8_T,
    UINT16_T,
    UINT32_T,
    UINT64_T,
    STRING,
    IARCH,
    UARCH,
    SIZE_COUNT
} PDC_CType;

typedef enum {
    SCALAR,
    ARRAY,
    STRUCT,
    UNION, // not implemented
    ENUM,  // not implemented
    POINTER, // not implemented
    FUNCTION, // not implemented
    TYPE_COUNT // not implemented
} PDC_CTypeClass;

const size_t DataTypeSizes[SIZE_COUNT] = {
    sizeof(char),
    sizeof(short),
    sizeof(int),
    sizeof(long),
    sizeof(float),
    sizeof(double),
    sizeof(int8_t),
    sizeof(int16_t),
    sizeof(int32_t),
    sizeof(int64_t),
    sizeof(uint8_t),
    sizeof(uint16_t),
    sizeof(uint32_t),
    sizeof(uint64_t),
    sizeof(char *),
    sizeof(void *),
    sizeof(size_t)
};

const char *DataTypeNames[SIZE_COUNT] = {
    "char",
    "short",
    "int",
    "long",
    "float",
    "double",
    "int8_t",
    "int16_t",
    "int32_t",
    "int64_t",
    "uint8_t",
    "uint16_t",
    "uint32_t",
    "uint64_t",
    "char *",
    "intptr_t",
    "size_t"
};

PDC_CType getDataTypeByName(const char* typeName);

#endif /* PDC_GENERIC_H */