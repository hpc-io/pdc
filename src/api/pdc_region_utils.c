#include "pdc_region.h"
#include <string.h>

int
check_overlap(int ndim, uint64_t *offset1, uint64_t *size1, uint64_t *offset2, uint64_t *size2)
{
    int i;
    for (i = 0; i < ndim; ++i) {
        if (!((offset1[i] + size1[i] > offset2[i] && offset2[i] >= offset1[i]) ||
              (offset2[i] + size2[i] > offset1[i] && offset1[i] >= offset2[i]))) {
            return 0;
        }
    }
    return 1;
}

int
PDC_region_overlap_detect(int ndim, uint64_t *offset1, uint64_t *size1, uint64_t *offset2, uint64_t *size2,
                          uint64_t **output_offset, uint64_t **output_size)
{
    int i;
    // First we check if two regions overlaps with each other. If any of the dimensions do not overlap, then
    // we are done.
    if (!check_overlap(ndim, offset1, size1, offset2, size2)) {
        *output_offset = NULL;
        *output_size   = NULL;
        // printf("PDC_region_overlap_detect @ line %d, overlap detect failed\n", __LINE__);
        goto done;
    }
    // Overlapping exist.
    *output_offset = (uint64_t *)malloc(sizeof(uint64_t) * ndim * 2);
    *output_size   = *output_offset + ndim;
    for (i = 0; i < ndim; ++i) {
        output_offset[0][i] = offset2[i] < offset1[i] ? offset1[i] : offset2[i];
        output_size[0][i]   = ((offset2[i] + size2[i] < offset1[i] + size1[i]) ? (offset2[i] + size2[i])
                                                                             : (offset1[i] + size1[i])) -
                            output_offset[0][i];
    }

done:
    return 0;
}

/*
 * For PDC_WRITE, we copy from buf to subregion. Otherwise we reverse copy.
 */
int
memcpy_subregion(int ndim, uint64_t unit, pdc_access_t access_type, char *buf, uint64_t *size, char *sub_buf,
                 uint64_t *sub_offset, uint64_t *sub_size)
{
    uint64_t i, j;
    char *   ptr, *target_buf, *src_buf;

    if (ndim == 1) {
        target_buf = sub_buf;
        src_buf    = buf + sub_offset[0] * unit;
        if (access_type == PDC_WRITE) {
            memcpy(target_buf, src_buf, unit * sub_size[0]);
        }
        else {
            memcpy(src_buf, target_buf, unit * sub_size[0]);
        }
    }
    else if (ndim == 2) {
        ptr = sub_buf;
        for (i = 0; i < sub_size[0]; ++i) {
            target_buf = ptr;
            src_buf    = buf + ((sub_offset[0] + i) * size[1] + sub_offset[1]) * unit;
            if (access_type == PDC_WRITE) {
                memcpy(target_buf, src_buf, sub_size[1] * unit);
            }
            else {
                memcpy(src_buf, target_buf, sub_size[1] * unit);
            }
            ptr += sub_size[1] * unit;
        }
    }
    else if (ndim == 3) {
        ptr = sub_buf;
        for (i = 0; i < sub_size[0]; ++i) {
            for (j = 0; j < sub_size[1]; ++j) {
                target_buf = ptr;
                src_buf    = buf + ((sub_offset[0] + i) * size[1] * size[2] + (sub_offset[1] + j) * size[2] +
                                 sub_offset[2]) *
                                    unit;
                if (access_type == PDC_WRITE) {
                    memcpy(target_buf, src_buf, sub_size[2] * unit);
                }
                else {
                    memcpy(src_buf, target_buf, sub_size[2] * unit);
                }
                ptr += sub_size[2] * unit;
            }
        }
    }

    return 0;
}

/*
 * Copy data from the first region to the second region. Only overlapped parts will be copied.
 */
int
memcpy_overlap_subregion(int ndim, uint64_t unit, char *buf, uint64_t *offset, uint64_t *size, char *buf2,
                         uint64_t *offset2, uint64_t *size2, uint64_t *overlap_offset, uint64_t *overlap_size)
{
    uint64_t i, j;
    char *   target_buf, *src_buf;

    switch (ndim) {
        case 1: {
            src_buf    = buf + (overlap_offset[0] - offset[0]) * unit;
            target_buf = buf2 + (overlap_offset[0] - offset2[0]) * unit;
            memcpy(target_buf, src_buf, unit * overlap_size[0]);
            break;
        }
        case 2: {
            for (i = 0; i < overlap_size[0]; ++i) {
                src_buf =
                    buf +
                    ((overlap_offset[0] - offset[0] + i) * size[1] + overlap_offset[1] - offset[1]) * unit;
                target_buf =
                    buf2 +
                    ((overlap_offset[0] - offset2[0] + i) * size2[1] + overlap_offset[1] - offset2[1]) * unit;
                memcpy(target_buf, src_buf, overlap_size[1] * unit);
            }
            break;
        }
        case 3: {
            for (i = 0; i < overlap_size[0]; ++i) {
                for (j = 0; j < overlap_size[1]; ++j) {
                    src_buf = buf + (((overlap_offset[0] - offset[0] + i) * size[1] +
                                      (overlap_offset[1] - offset[1] + j)) *
                                         size[2] +
                                     overlap_offset[2] - offset[2]) *
                                        unit;
                    target_buf = buf2 + (((overlap_offset[0] - offset2[0] + i) * size2[1] +
                                          (overlap_offset[1] - offset2[1] + j)) *
                                             size2[2] +
                                         overlap_offset[2] - offset2[2]) *
                                            unit;
                    memcpy(target_buf, src_buf, overlap_size[2] * unit);
                }
            }
        }
        default: {
            break;
        }
    }
    return 0;
}

// Check if the first region is fully contained in the second region.
int
detect_region_contained(uint64_t *offset, uint64_t *size, uint64_t *offset2, uint64_t *size2, int ndim)
{
    int i;

    for (i = 0; i < ndim; ++i) {
        if (offset[i] < offset2[i] || offset[i] + size[i] > offset2[i] + size2[i]) {
            return 0;
        }
    }
    return 1;
}
