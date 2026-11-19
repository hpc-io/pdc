#ifndef PDC_READ_RECORD_TAG_H
#define PDC_READ_RECORD_TAG_H

#include <stdint.h>

/* Give each record a portable identity while preserving its size and payload. */
static inline void
tag_record(uint8_t *record, uint64_t id)
{
    for (unsigned int byte = 0; byte < sizeof(id); ++byte)
        record[byte] = (uint8_t)(id >> (8 * byte));
}

#endif
