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

#ifndef _pdc_private_H
#define _pdc_private_H 

#include "stdint.h"

typedef enum {
    PDC_UNKNOWN      = -1, /* error                                      */
    PDC_INT          = 0,  /* integer types                              */
    PDC_FLOAT        = 1,  /* floating-point types                       */
    PDC_DOUBLE       = 2,  /* double types                               */
    PDC_STRING       = 3,  /* character string types                     */
    PDC_COMPOUND     = 4,  /* compound types                             */
    PDC_ENUM         = 5,  /* enumeration types                          */
    PDC_ARRAY        = 6,  /* Array types                                */
 
    NCLASSES         = 7   /* this must be last                          */
} PDC_var_type_t;

typedef enum {
    UNKNOWN = -1,
    MEMORY,
    FLASH,
    DISK,
    FILESYSTEM,
    TAPE
} PDC_loci;

/* Query type */
typedef enum {
    PDC_Q_TYPE_DATA_ELEM,  /* selects data elements */
    PDC_Q_TYPE_ATTR_VALUE, /* selects attribute values */
    PDC_Q_TYPE_ATTR_NAME,  /* selects attributes */
    PDC_Q_TYPE_LINK_NAME,  /* selects objects */
    PDC_Q_TYPE_MISC        /* (for combine queries) selects misc objects */
} PDC_query_type_t;

/* Query match conditions */
typedef enum {
    PDC_Q_MATCH_EQUAL,        /* equal */
    PDC_Q_MATCH_NOT_EQUAL,    /* not equal */
    PDC_Q_MATCH_LESS_THAN,    /* less than */
    PDC_Q_MATCH_GREATER_THAN  /* greater than */
} PDC_query_op_t;

typedef enum {
    ROW_major,
    COL_major
} PDC_major_type;

typedef struct {
} PDC_loci_info_t;

typedef struct {
    PDC_major_type type;
} PDC_transform;

#define SUCCEED    0
#define FAIL    (-1)

#define PDCmemset(X,C,Z)     memset((void*)(X),C,Z)
/* Include a basic profiling interface */
#ifdef ENABLE_PROFILING
#include "stack_ops.h"

#define FUNC_ENTER(X) do {             \
    if (enableProfiling) push(__func__, (X)); \
} while(0)

#define FUNC_LEAVE(ret_value) do {     \
    if (enableProfiling) pop();	       \
    return(ret_value);                 \
} while(0)

#else
#define FUNC_ENTER(X) do {            \
} while(0)

#define FUNC_LEAVE(ret_value) do {              \
        return(ret_value);                      \
} while(0)
#endif

#endif /* end of _pdc_private_H */
