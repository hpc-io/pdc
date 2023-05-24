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
    LONG_DOUBLE,
    INT8_T,
    INT16_T,
    INT32_T,
    INT64_T,
    UINT8_T,
    UINT16_T,
    UINT32_T,
    UINT64_T,
    IARCH,
    UARCH,
    SIZE_COUNT
} DataType;

const size_t DataTypeSizes[SIZE_COUNT] = {
    sizeof(char),
    sizeof(short),
    sizeof(int),
    sizeof(long),
    sizeof(float),
    sizeof(double),
    sizeof(long double),
    sizeof(int8_t),
    sizeof(int16_t),
    sizeof(int32_t),
    sizeof(int64_t),
    sizeof(uint8_t),
    sizeof(uint16_t),
    sizeof(uint32_t),
    sizeof(uint64_t),
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
    "long double",
    "int8_t",
    "int16_t",
    "int32_t",
    "int64_t",
    "uint8_t",
    "uint16_t",
    "uint32_t",
    "uint64_t",
    "intptr_t",
    "size_t"
};

DataType getDataTypeByName(const char* typeName);

#endif /* PDC_GENERIC_H */