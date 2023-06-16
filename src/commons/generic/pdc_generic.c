#include "pdc_generic.h"

// clang-format off
const size_t DataTypeSizes[PDC_TYPE_COUNT] = {
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

const char *DataTypeNames[PDC_TYPE_COUNT] = {
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

const char *DataTypeEnumNames[PDC_TYPE_COUNT] = {
    "PDC_UNKNOWN",
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
    "PDC_SIZE_T",
    "PDC_TYPE_COUNT"
};

const char *DataTypeFormat[PDC_TYPE_COUNT] = {
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

const char *
get_enum_name_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return NULL;
    }
    return DataTypeEnumNames[type];
}

const size_t
get_size_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return 0;
    }
    return DataTypeSizes[type];
}

const size_t
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

const char *
get_name_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return NULL;
    }
    return DataTypeNames[type];
}

pdc_c_var_type_t
get_dtype_by_enum_name(const char *enumName)
{
    for (int i = 0; i < PDC_TYPE_COUNT; i++) {
        if (strcmp(DataTypeEnumNames[i], enumName) == 0) {
            return (pdc_c_var_type_t)i;
        }
    }
    return PDC_UNKNOWN; // assuming PDC_UNKNOWN is the enum value for "unknown"
}
