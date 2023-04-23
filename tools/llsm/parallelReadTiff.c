#include "parallelReadTiff.h"
#include "tiffio.h"

// #define ENABLE_OPENMP

#ifdef ENABLE_OPENMP
#include "omp.h"
#endif

#define CREATE_ARRAY(result_var, type, ndim, dim)                                                            \
    do {                                                                                                     \
        size_t i = 0, dim_prod = 1;                                                                          \
        for (i = 0; i < (ndim); i++) {                                                                       \
            dim_prod *= (dim)[i];                                                                            \
        }                                                                                                    \
        result_var = (void *)malloc(dim_prod * sizeof(type));                                                \
    } while (0)

void
DummyHandler(const char *module, const char *fmt, va_list ap)
{
    // ignore errors and warnings
}

// Backup method in case there are errors reading strips
void
readTiffParallelBak(uint64_t x, uint64_t y, uint64_t z, const char *fileName, void *tiff, uint64_t bits,
                    uint64_t startSlice, uint8_t flipXY)
{
    int32_t  numWorkers = omp_get_max_threads();
    int32_t  batchSize  = (z - 1) / numWorkers + 1;
    uint64_t bytes      = bits / 8;

    int32_t w;
#ifdef ENABLE_OPENMP
#pragma omp parallel for
#endif
    for (w = 0; w < numWorkers; w++) {

        TIFF *tif = TIFFOpen(fileName, "r");
        if (!tif)
            printf("tiff:threadError | Thread %d: File \"%s\" cannot be opened\n", w, fileName);

        void *buffer = malloc(x * bytes);
        for (int64_t dir = startSlice + (w * batchSize); dir < startSlice + ((w + 1) * batchSize); dir++) {
            if (dir >= z + startSlice)
                break;

            int counter = 0;
            while (!TIFFSetDirectory(tif, (uint64_t)dir) && counter < 3) {
                printf("Thread %d: File \"%s\" Directory \"%d\" failed to open. Try %d\n", w, fileName, dir,
                       counter + 1);
                counter++;
            }

            for (int64_t i = 0; i < y; i++) {
                TIFFReadScanline(tif, buffer, i, 0);
                if (!flipXY) {
                    memcpy(tiff + ((i * x) * bytes), buffer, x * bytes);
                    continue;
                }
                // loading the data into a buffer
                switch (bits) {
                    case 8:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((uint8_t *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((uint8_t *)buffer)[j];
                        }
                        break;
                    case 16:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((uint16_t *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((uint16_t *)buffer)[j];
                        }
                        break;
                    case 32:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((float *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((float *)buffer)[j];
                        }
                        break;
                    case 64:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((double *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((double *)buffer)[j];
                        }
                        break;
                }
            }
        }
        free(buffer);
        TIFFClose(tif);
    }
}

void
readTiffParallel(uint64_t x, uint64_t y, uint64_t z, const char *fileName, void *tiff, uint64_t bits,
                 uint64_t startSlice, uint64_t stripSize, uint8_t flipXY)
{
    int32_t  numWorkers = omp_get_max_threads();
    int32_t  batchSize  = (z - 1) / numWorkers + 1;
    uint64_t bytes      = bits / 8;

    uint16_t compressed = 1;
    TIFF *   tif        = TIFFOpen(fileName, "r");
    TIFFGetField(tif, TIFFTAG_COMPRESSION, &compressed);

    int32_t w;
    uint8_t errBak = 0;
    uint8_t err    = 0;
    char    errString[10000];
    if (compressed > 1 || z < 32768) {
        TIFFClose(tif);
#ifdef ENABLE_OPENMP
#pragma omp parallel for
#endif
        for (w = 0; w < numWorkers; w++) {

            uint8_t outCounter = 0;
            TIFF *  tif        = TIFFOpen(fileName, "r");
            while (!tif) {
                tif = TIFFOpen(fileName, "r");
                if (outCounter == 3) {
#ifdef ENABLE_OPENMP
#pragma omp critical
#endif
                    {
                        err = 1;
                        sprintf(errString, "Thread %d: File \"%s\" cannot be opened\n", w, fileName);
                    }
                    continue;
                }
                outCounter++;
            }

            void *buffer = malloc(x * stripSize * bytes);
            for (int64_t dir = startSlice + (w * batchSize); dir < startSlice + ((w + 1) * batchSize);
                 dir++) {
                if (dir >= z + startSlice || err)
                    break;

                uint8_t counter = 0;
                while (!TIFFSetDirectory(tif, (uint64_t)dir) && counter < 3) {
                    counter++;
                    if (counter == 3) {
#ifdef ENABLE_OPENMP
#pragma omp critical
#endif
                        {
                            err = 1;
                            sprintf(errString, "Thread %d: File \"%s\" cannot be opened\n", w, fileName);
                        }
                    }
                }
                if (err)
                    break;
                for (int64_t i = 0; i * stripSize < y; i++) {

                    // loading the data into a buffer
                    int64_t cBytes = TIFFReadEncodedStrip(tif, i, buffer, stripSize * x * bytes);
                    if (cBytes < 0) {
#ifdef ENABLE_OPENMP
#pragma omp critical
#endif
                        {
                            errBak = 1;
                            err    = 1;
                            sprintf(errString, "Thread %d: Strip %ld cannot be read\n", w, i);
                        }
                        break;
                    }
                    if (!flipXY) {
                        memcpy(tiff + ((i * stripSize * x) * bytes), buffer, cBytes);
                        continue;
                    }
                    switch (bits) {
                        case 8:
                            // Map Values to flip x and y for MATLAB
                            for (int64_t k = 0; k < stripSize; k++) {
                                if ((k + (i * stripSize)) >= y)
                                    break;
                                for (int64_t j = 0; j < x; j++) {
                                    ((uint8_t *)tiff)[((j * y) + (k + (i * stripSize))) +
                                                      ((dir - startSlice) * (x * y))] =
                                        ((uint8_t *)buffer)[j + (k * x)];
                                }
                            }
                            break;
                        case 16:
                            // Map Values to flip x and y for MATLAB
                            for (int64_t k = 0; k < stripSize; k++) {
                                if ((k + (i * stripSize)) >= y)
                                    break;
                                for (int64_t j = 0; j < x; j++) {
                                    ((uint16_t *)tiff)[((j * y) + (k + (i * stripSize))) +
                                                       ((dir - startSlice) * (x * y))] =
                                        ((uint16_t *)buffer)[j + (k * x)];
                                }
                            }
                            break;
                        case 32:
                            // Map Values to flip x and y for MATLAB
                            for (int64_t k = 0; k < stripSize; k++) {
                                if ((k + (i * stripSize)) >= y)
                                    break;
                                for (int64_t j = 0; j < x; j++) {
                                    ((float *)tiff)[((j * y) + (k + (i * stripSize))) +
                                                    ((dir - startSlice) * (x * y))] =
                                        ((float *)buffer)[j + (k * x)];
                                }
                            }
                            break;
                        case 64:
                            // Map Values to flip x and y for MATLAB
                            for (int64_t k = 0; k < stripSize; k++) {
                                if ((k + (i * stripSize)) >= y)
                                    break;
                                for (int64_t j = 0; j < x; j++) {
                                    ((double *)tiff)[((j * y) + (k + (i * stripSize))) +
                                                     ((dir - startSlice) * (x * y))] =
                                        ((double *)buffer)[j + (k * x)];
                                }
                            }
                            break;
                    }
                }
            }
            free(buffer);
            TIFFClose(tif);
        }
    }
    else {
        uint64_t stripsPerDir = (uint64_t)ceil((double)y / (double)stripSize);
#ifdef _WIN32
        int fd = open(fileName, O_RDONLY | O_BINARY);
#else
        int fd = open(fileName, O_RDONLY);
#endif
        if (fd == -1)
            printf("disk:threadError | File \"%s\" cannot be opened from Disk\n", fileName);

        if (!tif)
            printf("tiff:threadError | File \"%s\" cannot be opened\n", fileName);
        uint64_t  offset  = 0;
        uint64_t *offsets = NULL;
        TIFFGetField(tif, TIFFTAG_STRIPOFFSETS, &offsets);
        uint64_t *byteCounts = NULL;
        TIFFGetField(tif, TIFFTAG_STRIPBYTECOUNTS, &byteCounts);
        if (!offsets || !byteCounts)
            printf("tiff:threadError | Could not get offsets or byte counts from the tiff file\n");
        offset           = offsets[0];
        uint64_t fOffset = offsets[stripsPerDir - 1] + byteCounts[stripsPerDir - 1];
        uint64_t zSize   = fOffset - offset;
        TIFFSetDirectory(tif, 1);
        TIFFGetField(tif, TIFFTAG_STRIPOFFSETS, &offsets);
        uint64_t gap = offsets[0] - fOffset;

        lseek(fd, offset, SEEK_SET);

        TIFFClose(tif);
        uint64_t curr      = 0;
        uint64_t bytesRead = 0;
        // TESTING
        // Not sure if we will need to read in chunks like for ImageJ
        for (uint64_t i = 0; i < z; i++) {
            bytesRead = read(fd, tiff + curr, zSize);
            curr += bytesRead;
            lseek(fd, gap, SEEK_CUR);
        }
        close(fd);
        uint64_t size  = x * y * z * (bits / 8);
        void *   tiffC = malloc(size);
        memcpy(tiffC, tiff, size);
#ifdef ENABLE_OPENMP
#pragma omp parallel for
#endif
        for (uint64_t k = 0; k < z; k++) {
            for (uint64_t j = 0; j < x; j++) {
                for (uint64_t i = 0; i < y; i++) {
                    switch (bits) {
                        case 8:
                            ((uint8_t *)tiff)[i + (j * y) + (k * x * y)] =
                                ((uint8_t *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                        case 16:
                            ((uint16_t *)tiff)[i + (j * y) + (k * x * y)] =
                                ((uint16_t *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                        case 32:
                            ((float *)tiff)[i + (j * y) + (k * x * y)] =
                                ((float *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                        case 64:
                            ((double *)tiff)[i + (j * y) + (k * x * y)] =
                                ((double *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                    }
                }
            }
        }
        free(tiffC);
    }
    if (err) {
        if (errBak)
            readTiffParallelBak(x, y, z, fileName, tiff, bits, startSlice, flipXY);
        else
            printf("tiff:threadError %s\n", errString);
    }
}

// Backup method in case there are errors reading strips
void
readTiffParallel2DBak(uint64_t x, uint64_t y, uint64_t z, const char *fileName, void *tiff, uint64_t bits,
                      uint64_t startSlice, uint8_t flipXY)
{
    int32_t  numWorkers = omp_get_max_threads();
    int32_t  batchSize  = (y - 1) / numWorkers + 1;
    uint64_t bytes      = bits / 8;

    int32_t w;
#ifdef ENABLE_OPENMP
#pragma omp parallel for
#endif
    for (w = 0; w < numWorkers; w++) {

        TIFF *tif = TIFFOpen(fileName, "r");
        if (!tif)
            printf("tiff:threadError | Thread %d: File \"%s\" cannot be opened\n", w, fileName);

        void *buffer = malloc(x * bytes);
        for (int64_t dir = startSlice + (w * batchSize); dir < startSlice + ((w + 1) * batchSize); dir++) {
            if (dir >= z + startSlice)
                break;

            int counter = 0;
            while (!TIFFSetDirectory(tif, (uint64_t)0) && counter < 3) {
                printf("Thread %d: File \"%s\" Directory \"%d\" failed to open. Try %d\n", w, fileName, dir,
                       counter + 1);
                counter++;
            }

            for (int64_t i = (w * batchSize); i < ((w + 1) * batchSize); i++) {
                if (i >= y)
                    break;
                TIFFReadScanline(tif, buffer, i, 0);
                if (!flipXY) {
                    memcpy(tiff + ((i * x) * bytes), buffer, x * bytes);
                    continue;
                }
                // loading the data into a buffer
                switch (bits) {
                    case 8:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((uint8_t *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((uint8_t *)buffer)[j];
                        }
                        break;
                    case 16:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((uint16_t *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((uint16_t *)buffer)[j];
                        }
                        break;
                    case 32:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((float *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((float *)buffer)[j];
                        }
                        break;
                    case 64:
                        // Map Values to flip x and y for MATLAB
                        for (int64_t j = 0; j < x; j++) {
                            ((double *)tiff)[((j * y) + i) + ((dir - startSlice) * (x * y))] =
                                ((double *)buffer)[j];
                        }
                        break;
                }
            }
        }
        free(buffer);
        TIFFClose(tif);
    }
}

void
readTiffParallel2D(uint64_t x, uint64_t y, uint64_t z, const char *fileName, void *tiff, uint64_t bits,
                   uint64_t startSlice, uint64_t stripSize, uint8_t flipXY)
{
    int32_t  numWorkers   = omp_get_max_threads();
    uint64_t stripsPerDir = (uint64_t)ceil((double)y / (double)stripSize);
    int32_t  batchSize    = (stripsPerDir - 1) / numWorkers + 1;
    uint64_t bytes        = bits / 8;

    int32_t w;
    uint8_t err    = 0;
    uint8_t errBak = 0;
    char    errString[10000];

#ifdef ENABLE_OPENMP
#pragma omp parallel for
#endif
    for (w = 0; w < numWorkers; w++) {

        uint8_t outCounter = 0;
        TIFF *  tif        = TIFFOpen(fileName, "r");
        while (!tif) {
            tif = TIFFOpen(fileName, "r");
            if (outCounter == 3) {
#ifdef ENABLE_OPENMP
#pragma omp critical
#endif
                {
                    err = 1;
                    sprintf(errString, "Thread %d: File \"%s\" cannot be opened\n", w, fileName);
                }
                continue;
            }
            outCounter++;
        }

        void *buffer = malloc(x * stripSize * bytes);

        uint8_t counter = 0;
        while (!TIFFSetDirectory(tif, 0) && counter < 3) {
            printf("Thread %d: File \"%s\" Directory \"%d\" failed to open. Try %d\n", w, fileName, 0,
                   counter + 1);
            counter++;
            if (counter == 3) {
#ifdef ENABLE_OPENMP
#pragma omp critical
#endif
                {
                    err = 1;
                    sprintf(errString, "Thread %d: File \"%s\" cannot be opened\n", w, fileName);
                }
            }
        }
        for (int64_t i = (w * batchSize); i < (w + 1) * batchSize; i++) {
            if (i * stripSize >= y || err)
                break;
            // loading the data into a buffer
            int64_t cBytes = TIFFReadEncodedStrip(tif, i, buffer, stripSize * x * bytes);
            if (cBytes < 0) {
#ifdef ENABLE_OPENMP
#pragma omp critical
#endif
                {
                    errBak = 1;
                    err    = 1;
                    sprintf(errString, "Thread %d: Strip %ld cannot be read\n", w, i);
                }
                break;
            }
            if (!flipXY) {
                memcpy(tiff + ((i * stripSize * x) * bytes), buffer, cBytes);
                continue;
            }
            switch (bits) {
                case 8:
                    // Map Values to flip x and y for MATLAB
                    for (int64_t k = 0; k < stripSize; k++) {
                        if ((k + (i * stripSize)) >= y)
                            break;
                        for (int64_t j = 0; j < x; j++) {
                            ((uint8_t *)tiff)[((j * y) + (k + (i * stripSize)))] =
                                ((uint8_t *)buffer)[j + (k * x)];
                        }
                    }
                    break;
                case 16:
                    // Map Values to flip x and y for MATLAB
                    for (int64_t k = 0; k < stripSize; k++) {
                        if ((k + (i * stripSize)) >= y)
                            break;
                        for (int64_t j = 0; j < x; j++) {
                            ((uint16_t *)tiff)[((j * y) + (k + (i * stripSize)))] =
                                ((uint16_t *)buffer)[j + (k * x)];
                        }
                    }
                    break;
                case 32:
                    // Map Values to flip x and y for MATLAB
                    for (int64_t k = 0; k < stripSize; k++) {
                        if ((k + (i * stripSize)) >= y)
                            break;
                        for (int64_t j = 0; j < x; j++) {
                            ((float *)tiff)[((j * y) + (k + (i * stripSize)))] =
                                ((float *)buffer)[j + (k * x)];
                        }
                    }
                    break;
                case 64:
                    // Map Values to flip x and y for MATLAB
                    for (int64_t k = 0; k < stripSize; k++) {
                        if ((k + (i * stripSize)) >= y)
                            break;
                        for (int64_t j = 0; j < x; j++) {
                            ((double *)tiff)[((j * y) + (k + (i * stripSize)))] =
                                ((double *)buffer)[j + (k * x)];
                        }
                    }
                    break;
            }
        }
        free(buffer);
        TIFFClose(tif);
    }

    if (err) {
        if (errBak)
            readTiffParallel2DBak(x, y, z, fileName, tiff, bits, startSlice, flipXY);
        else
            printf("tiff:threadError %s\n", errString);
    }
}

// Reading images saved by ImageJ
void
readTiffParallelImageJ(uint64_t x, uint64_t y, uint64_t z, const char *fileName, void *tiff, uint64_t bits,
                       uint64_t startSlice, uint64_t stripSize, uint8_t flipXY)
{
#ifdef _WIN32
    int fd = open(fileName, O_RDONLY | O_BINARY);
#else
    int fd = open(fileName, O_RDONLY);
#endif
    TIFF *tif = TIFFOpen(fileName, "r");
    if (!tif)
        printf("tiff:threadError | File \"%s\" cannot be opened\n", fileName);
    uint64_t  offset  = 0;
    uint64_t *offsets = NULL;
    TIFFGetField(tif, TIFFTAG_STRIPOFFSETS, &offsets);
    if (offsets)
        offset = offsets[0];

    TIFFClose(tif);
    lseek(fd, offset, SEEK_SET);
    uint64_t bytes = bits / 8;
    //#pragma omp parallel for
    /*
    for(uint64_t i = 0; i < z; i++){
    uint64_t cOffset = x*y*bytes*i;
    //pread(fd,tiff+cOffset,x*y*bytes,offset+cOffset);
    read(fd,tiff+cOffset,x*y*bytes);
    }*/
    uint64_t chunk  = 0;
    uint64_t tBytes = x * y * z * bytes;
    uint64_t bytesRead;
    uint64_t rBytes = tBytes;
    if (tBytes < INT_MAX)
        bytesRead = read(fd, tiff, tBytes);
    else {
        while (chunk < tBytes) {
            rBytes = tBytes - chunk;
            if (rBytes > INT_MAX)
                bytesRead = read(fd, tiff + chunk, INT_MAX);
            else
                bytesRead = read(fd, tiff + chunk, rBytes);
            chunk += bytesRead;
        }
    }
    close(fd);
    // Swap endianess for types greater than 8 bits
    // TODO: May need to change later because we may not always need to swap
    if (bits > 8) {
#ifdef ENABLE_OPENMP
#pragma omp parallel for
#endif
        for (uint64_t i = 0; i < x * y * z; i++) {
            switch (bits) {
                case 16:
                    //((uint16_t*)tiff)[i] = ((((uint16_t*)tiff)[i] & 0xff) >> 8) | (((uint16_t*)tiff)[i] <<
                    // 8);
                    //((uint16_t*)tiff)[i] = bswap_16(((uint16_t*)tiff)[i]);
                    ((uint16_t *)tiff)[i] =
                        ((((uint16_t *)tiff)[i] << 8) & 0xff00) | ((((uint16_t *)tiff)[i] >> 8) & 0x00ff);
                    break;
                case 32:
                    //((num & 0xff000000) >> 24) | ((num & 0x00ff0000) >> 8) | ((num & 0x0000ff00) << 8) |
                    //(num << 24)
                    //((float*)tiff)[i] = bswap_32(((float*)tiff)[i]);
                    ((uint32_t *)tiff)[i] = ((((uint32_t *)tiff)[i] << 24) & 0xff000000) |
                                            ((((uint32_t *)tiff)[i] << 8) & 0x00ff0000) |
                                            ((((uint32_t *)tiff)[i] >> 8) & 0x0000ff00) |
                                            ((((uint32_t *)tiff)[i] >> 24) & 0x000000ff);
                    break;
                case 64:
                    //((double*)tiff)[i] = bswap_64(((double*)tiff)[i]);
                    ((uint64_t *)tiff)[i] = ((((uint64_t *)tiff)[i] << 56) & 0xff00000000000000UL) |
                                            ((((uint64_t *)tiff)[i] << 40) & 0x00ff000000000000UL) |
                                            ((((uint64_t *)tiff)[i] << 24) & 0x0000ff0000000000UL) |
                                            ((((uint64_t *)tiff)[i] << 8) & 0x000000ff00000000UL) |
                                            ((((uint64_t *)tiff)[i] >> 8) & 0x00000000ff000000UL) |
                                            ((((uint64_t *)tiff)[i] >> 24) & 0x0000000000ff0000UL) |
                                            ((((uint64_t *)tiff)[i] >> 40) & 0x000000000000ff00UL) |
                                            ((((uint64_t *)tiff)[i] >> 56) & 0x00000000000000ffUL);
                    break;
            }
        }
    }
    // Find a way to do this in-place without making a copy
    if (flipXY) {
        uint64_t size  = x * y * z * (bits / 8);
        void *   tiffC = malloc(size);
        memcpy(tiffC, tiff, size);
#ifdef ENABLE_OPENMP
#pragma omp parallel for
#endif
        for (uint64_t k = 0; k < z; k++) {
            for (uint64_t j = 0; j < x; j++) {
                for (uint64_t i = 0; i < y; i++) {
                    switch (bits) {
                        case 8:
                            ((uint8_t *)tiff)[i + (j * y) + (k * x * y)] =
                                ((uint8_t *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                        case 16:
                            ((uint16_t *)tiff)[i + (j * y) + (k * x * y)] =
                                ((uint16_t *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                        case 32:
                            ((float *)tiff)[i + (j * y) + (k * x * y)] =
                                ((float *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                        case 64:
                            ((double *)tiff)[i + (j * y) + (k * x * y)] =
                                ((double *)tiffC)[j + (i * x) + (k * x * y)];
                            break;
                    }
                }
            }
        }
        free(tiffC);
    }
}

uint8_t
isImageJIm(TIFF *tif)
{
    if (!tif)
        return 0;
    char *tiffDesc = NULL;
    if (TIFFGetField(tif, TIFFTAG_IMAGEDESCRIPTION, &tiffDesc)) {
        if (strstr(tiffDesc, "ImageJ")) {
            return 1;
        }
    }
    return 0;
}

uint64_t
imageJImGetZ(TIFF *tif)
{
    if (!tif)
        return 0;
    char *tiffDesc = NULL;
    if (TIFFGetField(tif, TIFFTAG_IMAGEDESCRIPTION, &tiffDesc)) {
        if (strstr(tiffDesc, "ImageJ")) {
            char *nZ = strstr(tiffDesc, "images=");
            if (nZ) {
                nZ += 7;
                char *temp;
                return strtol(nZ, &temp, 10);
            }
        }
    }
    return 0;
}

void
get_tiff_info(char *fileName, parallel_tiff_range_t *strip_range, uint64_t *x, uint64_t *y, uint64_t *z,
              uint64_t *bits, uint64_t *startSlice, uint64_t *stripSize, uint64_t *is_imageJ,
              uint64_t *imageJ_Z)
{
    TIFFSetWarningHandler(DummyHandler);
    TIFF *tif = TIFFOpen(fileName, "r");
    if (!tif)
        printf("tiff:inputError | File \"%s\" cannot be opened", fileName);

    TIFFGetField(tif, TIFFTAG_IMAGEWIDTH, x);
    TIFFGetField(tif, TIFFTAG_IMAGELENGTH, y);

    if (strip_range == NULL) {
        uint16_t s = 0, m = 0, t = 1;
        while (TIFFSetDirectory(tif, t)) {
            s = t;
            t *= 8;
            if (s > t) {
                t = 65535;
                printf("Number of slices > 32768\n");
                break;
            }
        }
        while (s != t) {
            m = (s + t + 1) / 2;
            if (TIFFSetDirectory(tif, m)) {
                s = m;
            }
            else {
                if (m > 0)
                    t = m - 1;
                else
                    t = m;
            }
        }
        *z = s + 1;
    }
    else {
        if (strip_range->length != 2) {
            printf("tiff:inputError | Input range is not 2");
        }
        else {
            *startSlice = (uint64_t)(*(strip_range->range)) - 1;
            *z          = (uint64_t)(*(strip_range->range + 1)) - startSlice[0];
            if (!TIFFSetDirectory(tif, startSlice[0] + z[0] - 1) || !TIFFSetDirectory(tif, startSlice[0])) {
                printf("tiff:rangeOutOfBound | Range is out of bounds");
            }
        }
    }

    *is_imageJ = isImageJIm(tif);
    *imageJ_Z  = imageJImGetZ(tif);
    if (*is_imageJ) {
        *is_imageJ = 1;
        *imageJ_Z  = imageJImGetZ(tif);
        if (*imageJ_Z)
            *z = *imageJ_Z;
    }

    TIFFGetField(tif, TIFFTAG_BITSPERSAMPLE, bits);
    TIFFGetField(tif, TIFFTAG_ROWSPERSTRIP, stripSize);
    TIFFClose(tif);
}

void *
_get_tiff_array(int bits, int ndim, size_t *dims)
{
    void *tiff = NULL;
    if (bits == 8) {
        CREATE_ARRAY(tiff, uint8_t, ndim, dims);
    }
    else if (bits == 16) {
        CREATE_ARRAY(tiff, uint16_t, ndim, dims);
    }
    else if (bits == 32) {
        CREATE_ARRAY(tiff, float, ndim, dims);
    }
    else if (bits == 64) {
        CREATE_ARRAY(tiff, double, ndim, dims);
    }
    return tiff;
}

void
_TIFF_load(char *fileName, uint8_t isImageJIm, uint64_t x, uint64_t y, uint64_t z, uint64_t bits,
           uint64_t startSlice, uint64_t stripSize, uint8_t flipXY, int ndim, size_t *dims, void **tiff_ptr)
{
    if (tiff_ptr == NULL) {
        printf("tiff:dataTypeError, Data type not suppported\n");
    }
    *tiff_ptr = _get_tiff_array(bits, ndim, dims);
    // Case for ImageJ
    if (isImageJIm) {
        readTiffParallelImageJ(x, y, z, fileName, *tiff_ptr, bits, startSlice, stripSize, flipXY);
    }
    // Case for 2D
    else if (z <= 1) {
        readTiffParallel2D(x, y, z, fileName, *tiff_ptr, bits, startSlice, stripSize, flipXY);
    }
    // Case for 3D
    else {
        readTiffParallel(x, y, z, fileName, *tiff_ptr, bits, startSlice, stripSize, flipXY);
    }
}

void
parallel_TIFF_load(char *fileName, uint8_t flipXY, parallel_tiff_range_t *strip_range, image_info_t **image_info)
{
    uint64_t x = 1, y = 1, z = 1, bits = 1, startSlice = 0, stripeSize = 0, is_imageJ = 0, imageJ_Z = 0;

    get_tiff_info(fileName, strip_range, &x, &y, &z, &bits, &startSlice, &stripeSize, &is_imageJ, &imageJ_Z);

    int      ndim = 3;
    uint64_t dims[ndim];
    dims[0] = flipXY ? y : x;
    dims[1] = flipXY ? x : y;
    dims[2] = z;

    *image_info = (image_info_t *)malloc(sizeof(image_info_t));
    (*image_info)->x = dims[0];
    (*image_info)->y = dims[1];
    (*image_info)->z = dims[2];
    (*image_info)->bits = bits;
    (*image_info)->startSlice = startSlice;
    (*image_info)->stripeSize = stripeSize;
    (*image_info)->is_imageJ = is_imageJ;
    (*image_info)->imageJ_Z = imageJ_Z;
    (*image_info)->tiff_size = dims[0] * dims[1] * dims[2] * (bits / 8);

    _TIFF_load(fileName, is_imageJ, x, y, z, bits, startSlice, stripeSize, flipXY, ndim, dims, (void **)&((*image_info)->data));
}