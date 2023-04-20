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


#define CREATE_ARRAY(result_var, type, ndim, dim) do { \
    size_t i = 0, dim_prod = 1;                         \
    for (i = 0; i < (ndim); i++) {                      \
        dim_prod *= (dim)[i];                           \
    }                                                   \
    result_var = (void *)malloc(dim_prod * sizeof(type)); \
} while (0)

typedef struct {
    uint64_t *range; 
    size_t length;
} parallel_tiff_range_t;

void parallel_TIFF_load(char *fileName, void **tiff_ptr, uint8_t flipXY, parallel_tiff_range_t *strip_range);

#endif // PARALLELREADTIFF_H