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

#ifndef PDC_ID_PKG_H
#define PDC_ID_PKG_H

#include "pdc_private.h"
#include "pdc_linkedlist.h"
#include "mercury_atomic.h"
/*
 * Number of bits to use for ID Type in each atom. Increase if more types
 * are needed (though this will decrease the number of available IDs per
 * type). This is the only number that must be changed since all other bit
 * field sizes and masks are calculated from TYPE_BITS.
 */
#define TYPE_BITS         7
#define TYPE_MASK         (((pdcid_t)1 << TYPE_BITS) - 1)
#define PDC_MAX_NUM_TYPES TYPE_MASK
/*
 * Number of bits to use for the Atom index in each atom (assumes 8-bit
 * bytes). We don't use the sign bit.
 */
#define ID_BITS ((sizeof(pdcid_t) * 8) - (TYPE_BITS + 1))
#define ID_MASK (((pdcid_t)1 << ID_BITS) - 1)

/* Map an atom to an ID type number */
#define PDC_TYPE(a) ((PDC_type_t)(((pdcid_t)(a) >> ID_BITS) & TYPE_MASK))

struct _pdc_id_info {
    pdcid_t           id;      /* ID for this info                 */
    hg_atomic_int32_t count;   /* ref. count for this atom         */
    void *            obj_ptr; /* pointer associated with the atom */
    PDC_LIST_ENTRY(_pdc_id_info) entry;
};

#endif /* PDC_ID_PKG_H */
