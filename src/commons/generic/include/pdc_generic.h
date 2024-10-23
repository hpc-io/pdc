#ifndef PDC_GENERIC_H
#define PDC_GENERIC_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>

#ifndef __cplusplus
#if __STDC_VERSION__ >= 199901L
/* C99 or later */
#include <stdbool.h>
#else
/* Pre-C99 */
typedef enum { false = 0, true = 1 } bool;
#endif
#endif

typedef enum pdc_c_var_type_t {
    PDC_UNKNOWN    = 0,  /* error */
    PDC_SHORT      = 1,  /* short types */
    PDC_INT        = 2,  /* integer types (identical to int32_t) */
    PDC_UINT       = 3,  /* unsigned integer types (identical to uint32_t) */
    PDC_LONG       = 4,  /* long types */
    PDC_INT8       = 5,  /* 8-bit integer types */
    PDC_UINT8      = 6,  /* 8-bit unsigned integer types */
    PDC_INT16      = 7,  /* 16-bit integer types */
    PDC_UINT16     = 8,  /* 16-bit unsigned integer types */
    PDC_INT32      = 9,  /* 32-bit integer types, already listed as PDC_INT */
    PDC_UINT32     = 10, /* 32-bit unsigned integer types */
    PDC_INT64      = 11, /* 64-bit integer types */
    PDC_UINT64     = 12, /* 64-bit unsigned integer types */
    PDC_FLOAT      = 13, /* floating-point types */
    PDC_DOUBLE     = 14, /* double types */
    PDC_CHAR       = 15, /* character types */
    PDC_STRING     = 16, /* string types */
    PDC_BOOLEAN    = 17, /* boolean types */
    PDC_VOID_PTR   = 18, /* void pointer type */
    PDC_SIZE_T     = 19, /* size_t type */
    PDC_BULKI      = 20, /* BULKI type */
    PDC_BULKI_ENT  = 21, /* BULKI_ENTITY type */
    PDC_TYPE_COUNT = 22  /* this is the number of var types and has to be the last */
} pdc_c_var_type_t;

// typedef pdc_c_var_type_t PDC_CType;

typedef enum pdc_c_var_class_t {
    PDC_CLS_ITEM  = 0,
    PDC_CLS_ARRAY = 1,
    PDC_CLS_COUNT = 2 // just the count of the enum.
} pdc_c_var_class_t;

// typedef pdc_c_var_class_t PDC_CType_Class;

// clang-format off
static size_t DataTypeSizes[PDC_TYPE_COUNT] = {
    0,                      /* PDC_UNKNOWN, error, size 0 as placeholder */
    sizeof(short),          /* PDC_SHORT */
    sizeof(int),            /* PDC_INT */
    sizeof(unsigned int),   /* PDC_UINT */
    sizeof(long),           /* PDC_LONG */
    sizeof(int8_t),         /* PDC_INT8 */
    sizeof(uint8_t),        /* PDC_UINT8 */
    sizeof(int16_t),        /* PDC_INT16 */
    sizeof(uint16_t),       /* PDC_UINT16 */
    sizeof(int32_t),        /* PDC_INT32, already covered by PDC_INT */
    sizeof(uint32_t),       /* PDC_UINT32 */
    sizeof(int64_t),        /* PDC_INT64 */
    sizeof(uint64_t),       /* PDC_UINT64 */
    sizeof(float),          /* PDC_FLOAT */
    sizeof(double),         /* PDC_DOUBLE */
    sizeof(char),           /* PDC_CHAR */
    sizeof(char *),         /* PDC_STRING, assuming pointer to char */
    sizeof(bool),           /* PDC_BOOLEAN, assuming C99 _Bool or C++ bool, typically 1 byte */
    sizeof(void *),         /* PDC_VOID_PTR */
    sizeof(size_t),         /* PDC_SIZE_T */
    sizeof(void *),         /* PDC_BULKI, custom type, size 0 as placeholder */
    sizeof(void *),         /* PDC_BULKI_ENT, custom type, size 0 as placeholder */
};

static char *DataTypeNames[PDC_TYPE_COUNT] = {
    "Unknown",       /* PDC_UNKNOWN */
    "short",         /* PDC_SHORT */
    "int",           /* PDC_INT */
    "unsigned int",  /* PDC_UINT */
    "long",          /* PDC_LONG */
    "int8_t",        /* PDC_INT8 */
    "uint8_t",       /* PDC_UINT8 */
    "int16_t",       /* PDC_INT16 */
    "uint16_t",      /* PDC_UINT16 */
    "int32_t",       /* PDC_INT32, already covered by PDC_INT */
    "uint32_t",      /* PDC_UINT32 */
    "int64_t",       /* PDC_INT64 */
    "uint64_t",      /* PDC_UINT64 */
    "float",         /* PDC_FLOAT */
    "double",        /* PDC_DOUBLE */
    "char",          /* PDC_CHAR */
    "char*",         /* PDC_STRING */
    "bool",          /* PDC_BOOLEAN */
    "void*",         /* PDC_VOID_PTR */
    "size_t",        /* PDC_SIZE_T */
    "BULKI",         /* PDC_BULKI */
    "BULKI_ENTITY"   /* PDC_BULKI_ENT */
};

static char *DataTypeEnumNames[PDC_TYPE_COUNT] = {
    "PDC_UNKNOWN",    /* PDC_UNKNOWN */
    "PDC_SHORT",      /* PDC_SHORT */
    "PDC_INT",        /* PDC_INT */
    "PDC_UINT",       /* PDC_UINT */
    "PDC_LONG",       /* PDC_LONG */
    "PDC_INT8",       /* PDC_INT8 */
    "PDC_UINT8",      /* PDC_UINT8 */
    "PDC_INT16",      /* PDC_INT16 */
    "PDC_UINT16",     /* PDC_UINT16 */
    "PDC_INT32",      /* PDC_INT32, already covered by PDC_INT */
    "PDC_UINT32",     /* PDC_UINT32 */
    "PDC_INT64",      /* PDC_INT64 */
    "PDC_UINT64",     /* PDC_UINT64 */
    "PDC_FLOAT",      /* PDC_FLOAT */
    "PDC_DOUBLE",     /* PDC_DOUBLE */
    "PDC_CHAR",       /* PDC_CHAR */
    "PDC_STRING",     /* PDC_STRING */
    "PDC_BOOLEAN",    /* PDC_BOOLEAN */
    "PDC_VOID_PTR",   /* PDC_VOID_PTR */
    "PDC_SIZE_T",     /* PDC_SIZE_T */
    "PDC_BULKI",      /* PDC_BULKI */
    "PDC_BULKI_ENT"   /* PDC_BULKI_ENT */
};

__attribute__((unused))
static char *DataTypeFormat[PDC_TYPE_COUNT] = {
    "<unknown>",    /* PDC_UNKNOWN, no format as it's an error/unknown type */
    "%hd",          /* PDC_SHORT */
    "%d",           /* PDC_INT */
    "%u",           /* PDC_UINT */
    "%ld",          /* PDC_LONG */
    "%" PRId8,      /* PDC_INT8 */
    "%" PRIu8,      /* PDC_UINT8 */
    "%" PRId16,     /* PDC_INT16 */
    "%" PRIu16,     /* PDC_UINT16 */
    "%d",           /* PDC_INT32, already covered by PDC_INT */
    "%u",           /* PDC_UINT32 */
    "%" PRId64,     /* PDC_INT64 */
    "%" PRIu64,     /* PDC_UINT64 */
    "%f",           /* PDC_FLOAT */
    "%lf",          /* PDC_DOUBLE */
    "%c",           /* PDC_CHAR */
    "%s",           /* PDC_STRING */
    "%d",           /* PDC_BOOLEAN, represented as an integer */
    "%p",           /* PDC_VOID_PTR */
    "%zu",          /* PDC_SIZE_T */
    "%p",           /* PDC_BULKI, assuming pointer or similar for custom type */
    "%p"            /* PDC_BULKI_ENT, assuming pointer or similar for custom type */
};

// clang-format on
__attribute__((unused)) static char *
get_enum_name_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return NULL;
    }
    return DataTypeEnumNames[type];
}
__attribute__((unused)) static size_t
get_size_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return 0;
    }
    return DataTypeSizes[type];
}
__attribute__((unused)) static size_t
get_size_by_class_n_type(void *data, size_t item_count, pdc_c_var_class_t pdc_class,
                         pdc_c_var_type_t pdc_type)
{
    size_t size = 0;
    if (pdc_class == PDC_CLS_ITEM) {
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
            size_t i       = 0;
            for (i = 0; i < item_count; i++) {
                size = size + (strlen(str_arr[i]) + 1) * sizeof(char);
            }
        }
        else {
            size = item_count * get_size_by_dtype(pdc_type);
        }
    }
    return size;
}
__attribute__((unused)) static char *
get_name_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return NULL;
    }
    return DataTypeNames[type];
}
__attribute__((unused)) static pdc_c_var_type_t
get_dtype_by_enum_name(const char *enumName)
{
    for (int i = 0; i < PDC_TYPE_COUNT; i++) {
        if (strcmp(DataTypeEnumNames[i], enumName) == 0) {
            return (pdc_c_var_type_t)i;
        }
    }
    return PDC_UNKNOWN; // assuming PDC_UNKNOWN is the enum value for "unknown"
}

__attribute__((unused)) static char *
get_format_by_dtype(pdc_c_var_type_t type)
{
    if (type < 0 || type >= PDC_TYPE_COUNT) {
        return NULL;
    }
    return DataTypeFormat[type];
}

__attribute__((unused)) static bool
is_PDC_UINT(pdc_c_var_type_t type)
{
    if (type == PDC_UINT || type == PDC_UINT64 || type == PDC_UINT16 || type == PDC_UINT8 ||
        type == PDC_UINT32 || type == PDC_SIZE_T) {
        return true;
    }
    return false;
}

__attribute__((unused)) static bool
is_PDC_INT(pdc_c_var_type_t type)
{
    if (type == PDC_INT || type == PDC_INT64 || type == PDC_INT16 || type == PDC_INT8 || type == PDC_INT32 ||
        type == PDC_LONG) {
        return true;
    }
    return false;
}

__attribute__((unused)) static bool
is_PDC_FLOAT(pdc_c_var_type_t type)
{
    if (type == PDC_FLOAT || type == PDC_DOUBLE) {
        return true;
    }
    return false;
}

__attribute__((unused)) static bool
is_PDC_STRING(pdc_c_var_type_t type)
{
    if (type == PDC_CHAR || type == PDC_STRING) {
        return true;
    }
    return false;
}

__attribute__((unused)) static bool
is_PDC_NUMERIC(pdc_c_var_type_t type)
{
    if (is_PDC_INT(type) || is_PDC_UINT(type) || is_PDC_FLOAT(type)) {
        return true;
    }
    return false;
}

/**
 * get numeric value from a string.
 * @param str
 * @param type
 * @param val_ptr
 * @return the size of the value.
 */
size_t get_number_from_string(char *str, pdc_c_var_type_t type, void **val_ptr);

#endif /* PDC_GENERIC_H */
