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

#ifndef PDC_PUBLIC_H
#define PDC_PUBLIC_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include "pdc_generic.h"

/*******************/
/* Public Typedefs */
/*******************/

/**
 * @brief PDC error
 *
 * Negative indicates error success otherwise
 */
typedef int perr_t;
/**
 * @brief PDC ID
 *
 * 0 indicates error success otherwise
 */
typedef uint64_t           pdcid_t;
typedef unsigned long long psize_t;
typedef bool               pbool_t;

typedef int    PDC_int_t;
typedef float  PDC_float_t;
typedef double PDC_double_t;

/**
 * @brief PDC variable types
 *
 * List of all variable types:
 *
 * - PDC_UNKNOWN    = 0    : error
 * - PDC_SHORT      = 1    : short types
 * - PDC_INT        = 2    : integer types (identical to int32_t)
 * - PDC_UINT       = 3    : unsigned integer types (identical to uint32_t)
 * - PDC_LONG       = 4    : long types
 * - PDC_INT8       = 5    : 8-bit integer types
 * - PDC_UINT8      = 6    : 8-bit unsigned integer types
 * - PDC_INT16      = 7    : 16-bit integer types
 * - PDC_UINT16     = 8    : 16-bit unsigned integer types
 * - PDC_INT32      = 9    : 32-bit integer types, already listed as PDC_INT
 * - PDC_UINT32     = 10   : 32-bit unsigned integer types
 * - PDC_INT64      = 11   : 64-bit integer types
 * - PDC_UINT64     = 12   : 64-bit unsigned integer types
 * - PDC_FLOAT      = 13   : floating-point types
 * - PDC_DOUBLE     = 14   : double types
 * - PDC_CHAR       = 15   : character types
 * - PDC_STRING     = 16   : string types
 * - PDC_BOOLEAN    = 17   : boolean types
 * - PDC_VOID_PTR   = 18   : void pointer type
 * - PDC_SIZE_T     = 19   : size_t type
 * - PDC_BULKI      = 20   : BULKI type
 * - PDC_BULKI_ENT  = 21   : BULKI_ENTITY type
 * - PDC_TYPE_COUNT = 22   : number of variable types (must be last)
 */
typedef pdc_c_var_type_t pdc_var_type_t;

// FIXME: common data structure should be defined in a group of common header files.
typedef struct pdc_kvtag_t {
    char *   name;
    uint32_t size;
    int8_t   type;
    void *   value;
} pdc_kvtag_t;

/**
 * @brief Lifetime of a PDC container
 *
 * The default is PDC_PERSIST
 */
typedef enum {
    /// @brief The container persists beyond the lifetime of the creating process.
    PDC_PERSIST,
    /// @brief The container exists only for the duration
    /// of the creating process and is deleted when the process exits.
    PDC_TRANSIENT
} pdc_lifetime_t;

typedef enum { PDC_SERVER_DEFAULT = 0, PDC_SERVER_PER_CLIENT = 1 } pdc_server_selection_t;

typedef struct pdc_histogram_t {
    pdc_var_type_t dtype;
    int            nbin;
    double         incr;
    double *       range;
    uint64_t *     bin;
} pdc_histogram_t;

typedef enum pdc_region_writeout_strategy_t {
    /**
     * Store data as multiple regions inside a single file.
     * Overlapping writes that are not fully contained append new regions
     * to the end of the file, with metadata tracking region locations.
     * Supports incremental updates without rewriting large parts of the file.
     */
    STORE_REGION_BY_REGION_SINGLE_FILE = 0,

    /**
     * Store the entire object as a single flat file.
     * Reads and writes operate by seeking directly within the file.
     * No region metadata bookkeeping; simpler but less flexible for partial updates.
     */
    STORE_FLATTENED_SINGLE_FILE,

    /**
     * Store each flattened region in its own separate file.
     * Enables independent file management per region.
     */
    STORE_FLATTENED_REGION_PER_FILE
} pdc_region_writeout_strategy_t;

#define SUCCEED 0
#define FAIL    (-1)

#define PDC_SIZE_UNLIMITED UINT64_MAX

#endif /* PDC_PUBLIC_H */
