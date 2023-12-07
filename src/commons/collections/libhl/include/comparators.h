#ifndef HL_COMPARATORS_H
#define HL_COMPARATORS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "pdc_generic.h"

/**
 * @brief Callback that, if provided, will be used to compare node keys.
 *        If not defined memcmp() will be used in the following way :
 * @param k1     The first key to compare
 * @param k1size The size of the first key to compare
 * @param k2     The second key to compare
 * @param k2size The size of the second key to compare
 * @return The distance between the two keys.
 *         0 will be returned if the keys match (both size and value);\n
 *         "k1size - k2size" will be returned if the two sizes don't match;\n
 *         The difference between the two keys is returned if the two sizes
 *         match but the value doesn't
 * @note By default memcmp() is be used to compare the value, a custom
 *       comparator can be
 *       registered at creation time (as parameter of rbtree_create())
 * @note If integers bigger than 8 bits are going to be used as keys,
 *       an integer comparator should be used instead of the default one
 *       (either a custom comparator or one of the rbtree_cmp_keys_int16(),
 *       rbtree_cmp_keys_int32() and rbtree_cmp_keys_int64() helpers provided
 *       by the library).
 *
 */
typedef int (*libhl_cmp_callback_t)(void *k1, size_t k1size, void *k2, size_t k2size);

#define LIBHL_CAST_KEYS(_type, _k1)                                                                          \
    {                                                                                                        \
        _type _k1i = *((_type *)_k1);                                                                        \
        int   rst  = (int)_k1i;                                                                              \
        return rst >= 0 ? rst : -rst;                                                                        \
    }

#define LIBHL_CMP_KEYS_TYPE(_type, _k1, _k1s, _k2, _k2s)                                                     \
    {                                                                                                        \
        if (_k1s < sizeof(_type) || _k2s < sizeof(_type) || _k1s != _k2s)                                    \
            return _k1s - _k2s;                                                                              \
        _type _k1i = *((_type *)_k1);                                                                        \
        _type _k2i = *((_type *)_k2);                                                                        \
        return _k1i - _k2i;                                                                                  \
    }

static int
libhl_cast_any_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    int rst = *(int *)k1;
    return rst >= 0 ? rst : -rst;
}

static int
libhl_cast_int_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CAST_KEYS(int, k1);
}

static int
libhl_cast_long_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CAST_KEYS(long, k1);
}

static int
libhl_cast_int16_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CAST_KEYS(int16_t, k1);
}

static int
libhl_cast_int32_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CAST_KEYS(int32_t, k1);
}

static int
libhl_cast_int64_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CAST_KEYS(int64_t, k1);
}

static int
libhl_cast_float_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CAST_KEYS(float, k1);
}

static int
libhl_cast_double_to_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CAST_KEYS(double, k1);
}

static int
libhl_cmp_keys_string(void *k1, size_t k1size, void *k2, size_t k2size)
{
    return strcmp((const char *)k1, (const char *)k2);
}

/**
 * @brief int signed integers comparator
 */
static int
libhl_cmp_keys_int(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(int, k1, k1size, k2, k2size);
}

/**
 * @brief long signed integers comparator
 */
static int
libhl_cmp_keys_long(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(long, k1, k1size, k2, k2size);
}

/**
 * @brief 16bit signed integers comparator
 */
static int
libhl_cmp_keys_int16(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(int16_t, k1, k1size, k2, k2size);
}

/**
 * @brief 32bit signed integers comparator
 */
static int
libhl_cmp_keys_int32(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(int32_t, k1, k1size, k2, k2size);
}

/**
 * @brief 64bit signed integers comparator
 */
static int
libhl_cmp_keys_int64(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(int64_t, k1, k1size, k2, k2size);
}

/**
 * @brief 16bit unsigned integers comparator
 */
static int
libhl_cmp_keys_uint16(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(uint16_t, k1, k1size, k2, k2size);
}

/**
 * @brief 32bit unsigned integers comparator
 */
static int
libhl_cmp_keys_uint32(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(uint32_t, k1, k1size, k2, k2size);
}

/**
 * @brief 64bit unsigned integers comparator
 */
static int
libhl_cmp_keys_uint64(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(uint64_t, k1, k1size, k2, k2size);
}

/**
 * @brief float comparator
 */
static int
libhl_cmp_keys_float(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(float, k1, k1size, k2, k2size);
}

/**
 * @brief double comparator
 */
static int
libhl_cmp_keys_double(void *k1, size_t k1size, void *k2, size_t k2size)
{
    LIBHL_CMP_KEYS_TYPE(double, k1, k1size, k2, k2size);
}

static libhl_cmp_callback_t cmp_cb_array[PDC_TYPE_COUNT] = {
    libhl_cmp_keys_int,
    libhl_cmp_keys_float,
    libhl_cmp_keys_double,
    NULL,                  //"PDC_CHAR",
    NULL,                  //"PDC_STRING",
    NULL,                  //"PDC_BOOLEAN",
    NULL,                  //"PDC_SHORT",
    NULL,                  //"PDC_UINT",
    libhl_cmp_keys_int64,  //"PDC_INT64",
    libhl_cmp_keys_uint64, //"PDC_UINT64",
    libhl_cmp_keys_int16,  // "PDC_INT16",
    NULL,                  //"PDC_INT8",
    NULL,                  //"PDC_UINT8",
    libhl_cmp_keys_uint16, // "PDC_UINT16",
    libhl_cmp_keys_int32,  //"PDC_INT32",
    libhl_cmp_keys_uint32, //"PDC_UINT32",
    libhl_cmp_keys_long,   // "PDC_LONG",
    NULL,                  //"PDC_VOID_PTR",
    NULL,                  //"PDC_SIZE_T",
    NULL,                  //"PDC_BULKI",
    NULL,                  //"PDC_BULKI_ENT"
};

static libhl_cmp_callback_t locate_cb_array[PDC_TYPE_COUNT] = {
    libhl_cast_int_to_int,
    libhl_cast_float_to_int,
    libhl_cast_double_to_int,
    NULL,                    //"PDC_CHAR",
    NULL,                    //"PDC_STRING",
    NULL,                    //"PDC_BOOLEAN",
    NULL,                    //"PDC_SHORT",
    NULL,                    //"PDC_UINT",
    libhl_cast_int64_to_int, //"PDC_INT64",
    libhl_cast_any_to_int,   //"PDC_UINT64",
    libhl_cast_int16_to_int, // "PDC_INT16",
    NULL,                    //"PDC_INT8",
    NULL,                    //"PDC_UINT8",
    libhl_cast_any_to_int,   // "PDC_UINT16",
    libhl_cast_int32_to_int, //"PDC_INT32",
    libhl_cast_any_to_int,   //"PDC_UINT32",
    libhl_cast_long_to_int,  // "PDC_LONG",
    NULL,                    //"PDC_VOID_PTR",
    NULL,                    //"PDC_SIZE_T",
    NULL,                    //"PDC_BULKI",
    NULL,                    //"PDC_BULKI_ENT"
};

#define LIBHL_CMP_CB(DT)    cmp_cb_array[DT]
#define LIBHL_LOCATE_CB(DT) locate_cb_array[DT]

#define LIBHL_CHOOSE_CMP_CB(DT)                                                                              \
    libhl_cmp_callback_t cmp_cb;                                                                             \
    libhl_cmp_callback_t locate_cb;                                                                          \
    switch (DT) {                                                                                            \
        case INT:                                                                                            \
            cmp_cb    = libhl_cmp_keys_int;                                                                  \
            locate_cb = libhl_cast_int_to_int;                                                               \
            break;                                                                                           \
        case LONG:                                                                                           \
            cmp_cb    = libhl_cmp_keys_long;                                                                 \
            locate_cb = libhl_cast_long_to_int;                                                              \
            break;                                                                                           \
        case FLOAT:                                                                                          \
            cmp_cb    = libhl_cmp_keys_float;                                                                \
            locate_cb = libhl_cast_float_to_int;                                                             \
            break;                                                                                           \
        case DOUBLE:                                                                                         \
            cmp_cb    = libhl_cmp_keys_double;                                                               \
            locate_cb = libhl_cast_double_to_int;                                                            \
            break;                                                                                           \
        case INT16:                                                                                          \
            cmp_cb    = libhl_cmp_keys_int16;                                                                \
            locate_cb = libhl_cast_int16_to_int;                                                             \
            break;                                                                                           \
        case INT32:                                                                                          \
            cmp_cb    = libhl_cmp_keys_int32;                                                                \
            locate_cb = libhl_cast_int32_to_int;                                                             \
            break;                                                                                           \
        case INT64:                                                                                          \
            cmp_cb    = libhl_cmp_keys_int64;                                                                \
            locate_cb = libhl_cast_int64_to_int;                                                             \
            break;                                                                                           \
        case UINT16:                                                                                         \
            cmp_cb    = libhl_cmp_keys_uint16;                                                               \
            locate_cb = libhl_cast_any_to_int;                                                               \
            break;                                                                                           \
        case UINT32:                                                                                         \
            cmp_cb    = libhl_cmp_keys_uint32;                                                               \
            locate_cb = libhl_cast_any_to_int;                                                               \
            break;                                                                                           \
        case UINT64:                                                                                         \
            cmp_cb    = libhl_cmp_keys_uint64;                                                               \
            locate_cb = libhl_cast_any_to_int;                                                               \
            break;                                                                                           \
        default:                                                                                             \
            cmp_cb    = NULL;                                                                                \
            locate_cb = NULL;                                                                                \
            break;                                                                                           \
    }

#ifdef __cplusplus
}
#endif

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
