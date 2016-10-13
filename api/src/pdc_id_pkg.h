#ifndef _pdc_id_pkg_H
#define _pdc_id_pkg_H

#include "pdc_private.h"
/*
 * Number of bits to use for ID Type in each atom. Increase if more types
 * are needed (though this will decrease the number of available IDs per
 * type). This is the only number that must be changed since all other bit
 * field sizes and masks are calculated from TYPE_BITS.
 */
#define TYPE_BITS       7
#define TYPE_MASK       (((pdcid_t)1 << TYPE_BITS) - 1)
#define PDC_MAX_NUM_TYPES TYPE_MASK
/*
 * Number of bits to use for the Atom index in each atom (assumes 8-bit
 * bytes). We don't use the sign bit.
 */
#define ID_BITS         ((sizeof(pdcid_t) * 8) - (TYPE_BITS + 1))
#define ID_MASK         (((pdcid_t)1 << ID_BITS) - 1)

/* Map an atom to an ID type number */
#define PDC_TYPE(a) ((PDC_type_t)(((pdcid_t)(a) >> ID_BITS) & TYPE_MASK))

#endif /*_pdc_id_pkg_H*/
