/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#ifndef PDC_PRIVATE_H
#define PDC_PRIVATE_H

#include "pdc_config.h"
#include "pdc_public.h"
#include <stdio.h>

/****************************/
/* Library Private Typedefs */
/****************************/
typedef enum {
    UNKNOWN       = 0,
    SERVER_MEMORY = 1,
    CLIENT_MEMORY = 2,
    FLASH         = 3,
    DISK          = 4,
    FILESYSTEM    = 5,
    TAPE          = 6
} _pdc_loci_t;

/* Query type */
typedef enum {
    PDC_Q_TYPE_DATA_ELEM,  /* selects data elements */
    PDC_Q_TYPE_ATTR_VALUE, /* selects attribute values */
    PDC_Q_TYPE_ATTR_NAME,  /* selects attributes */
    PDC_Q_TYPE_LINK_NAME,  /* selects objects */
    PDC_Q_TYPE_MISC        /* (for combine queries) selects misc objects */
} _pdc_query_type_t;

/* Query match conditions */
typedef enum {
    PDC_Q_MATCH_EQUAL,       /* equal */
    PDC_Q_MATCH_NOT_EQUAL,   /* not equal */
    PDC_Q_MATCH_LESS_THAN,   /* less than */
    PDC_Q_MATCH_GREATER_THAN /* greater than */
} _pdc_query_op_t;

typedef enum { ROW_major, COL_major } _pdc_major_type_t;

typedef enum { C_lang = 0, FORTRAN_lang, PYTHON_lang, JULIA_lang, N_LANGUAGES } _pdc_analysis_language_t;

/***************************/
/* Library Private Structs */
/***************************/
struct _pdc_class {
    char *  name;
    pdcid_t local_id;
};

#ifdef __cplusplus
#define ATTRIBUTE(a)
#else /* __cplusplus */
#if defined(HAVE_ATTRIBUTE)
#define ATTRIBUTE(a) __attribute__((a))
#else
#define ATTRIBUTE(a)
#endif
#endif /* __cplusplus */

#ifdef __cplusplus
#define ATTR_UNUSED /*void*/
#else               /* __cplusplus */
#if defined(HAVE_ATTRIBUTE) && !defined(__SUNPRO_C)
#define ATTR_UNUSED __attribute__((unused))
#else
#define ATTR_UNUSED /*void*/
#endif
#endif /* __cplusplus */

#define PDCmemset(X, C, Z) memset((void *)(X), C, Z)

/*
 *  PDC Boolean type.
 */
#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif

extern pbool_t err_occurred;

/*
 * PGOTO_DONE macro. The argument is the return value which is
 * assigned to the `ret_value' variable.  Control branches to
 * the `done' label.
 */
#define PGOTO_DONE(ret_val)                                                                                  \
    do {                                                                                                     \
        ret_value = ret_val;                                                                                 \
        goto done;                                                                                           \
    } while (0)

#define PGOTO_DONE_VOID                                                                                      \
    do {                                                                                                     \
        goto done;                                                                                           \
    } while (0)

/*
 * PGOTO_ERROR macro. The arguments are the return value and an
 * error string.  The return value is assigned to a variable `ret_value' and
 * control branches to the `done' label.
 */
#define PGOTO_ERROR(ret_val, ...)                                                                            \
    do {                                                                                                     \
        fprintf(stderr, "Error in %s:%d\n", __FILE__, __LINE__);                                             \
        fprintf(stderr, " # %s(): ", __func__);                                                              \
        fprintf(stderr, __VA_ARGS__);                                                                        \
        fprintf(stderr, "\n");                                                                               \
        PGOTO_DONE(ret_val);                                                                                 \
    } while (0)

#define PGOTO_ERROR_VOID(...)                                                                                \
    do {                                                                                                     \
        fprintf(stderr, "Error in %s:%d\n", __FILE__, __LINE__);                                             \
        fprintf(stderr, " # %s(): ", __func__);                                                              \
        fprintf(stderr, "\n");                                                                               \
        PGOTO_DONE_VOID;                                                                                     \
    } while (0)

/* Include a basic profiling interface */
#ifdef ENABLE_PROFILING
#include "stack_ops.h"

#define FUNC_ENTER(X)                                                                                        \
    do {                                                                                                     \
        if (enableProfiling)                                                                                 \
            push(__func__, (X));                                                                             \
    } while (0)

#define FUNC_LEAVE(ret_value)                                                                                \
    do {                                                                                                     \
        if (enableProfiling)                                                                                 \
            pop();                                                                                           \
        return (ret_value);                                                                                  \
    } while (0)

#define FUNC_LEAVE_VOID                                                                                      \
    do {                                                                                                     \
        if (enableProfiling)                                                                                 \
            pop();                                                                                           \
        return;                                                                                              \
    } while (0)

#else
/* #define FUNC_ENTER(X) \ */
/*     do { \ */
/*         time_t now; \ */
/*         time(&now); \ */
/*         fprintf(stderr, "%ld enter %s\n", now, __func__); \ */
/*     } while (0) */

/* #define FUNC_LEAVE(ret_value) \ */
/*     do { \ */
/*         time_t now; \ */
/*         time(&now); \ */
/*         fprintf(stderr, "%ld leave %s\n", now, __func__); \ */
/*         return (ret_value); \ */
/*     } while (0) */

#define FUNC_ENTER(X)                                                                                        \
    do {                                                                                                     \
    } while (0)

#define FUNC_LEAVE(ret_value)                                                                                \
    do {                                                                                                     \
        return (ret_value);                                                                                  \
    } while (0)

#define FUNC_LEAVE_VOID                                                                                      \
    do {                                                                                                     \
        return;                                                                                              \
    } while (0)
#endif

#endif /* PDC_PRIVATE_H */
