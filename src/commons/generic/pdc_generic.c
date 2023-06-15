#include "pdc_generic.h"

const size_t DataTypeSizes[PDC_CT_COUNT] = {
    sizeof(char),     sizeof(short),    sizeof(int),     sizeof(long),    sizeof(float),   sizeof(double),
    sizeof(int8_t),   sizeof(int16_t),  sizeof(int32_t), sizeof(int64_t), sizeof(uint8_t), sizeof(uint16_t),
    sizeof(uint32_t), sizeof(uint64_t), sizeof(char *),  sizeof(void *),  sizeof(size_t)};

const char *DataTypeNames[PDC_CT_COUNT] = {
    "bool",    "char",    "short",   "int",      "long",     "float",    "double", "int8_t", "int16_t",
    "int32_t", "int64_t", "uint8_t", "uint16_t", "uint32_t", "uint64_t", "char *", "void *", "size_t"};

const char *DataTypeFormat[PDC_CT_COUNT] = {"%d", "%c",  "%hd", "%d", "%ld", "%f",  "%lf", "%d", "%d",
                                            "%d", "%ld", "%u",  "%u", "%u",  "%lu", "%s",  "%p", "%lu"};

const size_t
get_size_by_dtype(PDC_CType type)
{
    return DataTypeSizes[type];
}

const char *
get_name_by_dtype(PDC_CType type)
{
    return DataTypeNames[type];
}

PDC_CType
get_dtype_by_name(const char *typeName)
{
    for (int i = 0; i < PDC_CT_COUNT; i++) {
        if (strcmp(typeName, DataTypeNames[i]) == 0) {
            return (PDC_CType)i;
        }
    }
    return -1; // or some other error value
}
