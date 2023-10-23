#ifndef PARALLELREADTIFF_H
#define PARALLELREADTIFF_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>

typedef struct {
    uint64_t *range;
    size_t    length;
} parallel_tiff_range_t;

typedef struct {
    uint64_t x;
    uint64_t y;
    uint64_t z;
    uint64_t bits;
    uint64_t startSlice;
    uint64_t stripeSize;
    uint64_t is_imageJ;
    uint64_t imageJ_Z;
    void *   tiff_ptr;
    size_t   tiff_size;
} image_info_t;

void parallel_TIFF_load(char *fileName, uint8_t flipXY, parallel_tiff_range_t *strip_range,
                        image_info_t **image_info_t);

#endif // PARALLELREADTIFF_H