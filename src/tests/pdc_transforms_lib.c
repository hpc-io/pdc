#include "pdc.h"
#include <blosc.h>
#include <stdint.h>

/*
 * >> pdc_public.h
 *
 *  typedef enum {
 *      PDC_UNKNOWN      = -1,
 *      PDC_INT          = 0,
 *      PDC_FLOAT        = 1,
 *      PDC_DOUBLE       = 2,
 *      PDC_STRING       = 3,
 *      PDC_COMPOUND     = 4,
 *      PDC_ENUM         = 5,
 *      PDC_ARRAY        = 6,
 *      PDC_UINT         = 7,
 *      PDC_INT64        = 8,
 *      PDC_UINT64       = 9,
 *      PDC_INT16        = 10,
 *      PDC_INT8         = 11,
 *      NCLASSES         = 12
 *  } PDC_var_type_t;
 */

static int
_int_to_float(int *dataIn, float *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (float)dataIn[k];
    return 0;
}

static int
_int_to_double(int *dataIn, double *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (double)dataIn[k];
    return 0;
}

static int
_int_to_int64(int *dataIn, int64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int64_t)dataIn[k];
    return 0;
}

static int
_int_to_int16(int *dataIn, short *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (short)(dataIn[k] & 0x0FFFF);
    return 0;
}

static int
_int_to_int8(int *dataIn, int8_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int8_t)(dataIn[k] & 0x0FF);
    return 0;
}

static int
_uint_to_float(uint32_t *dataIn, float *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (float)dataIn[k];
    return 0;
}

static int
_uint_to_double(uint32_t *dataIn, double *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (double)dataIn[k];
    return 0;
}

static int
_uint_to_int64(uint32_t *dataIn, int64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int64_t)dataIn[k];
    return 0;
}

static int
_uint_to_int16(uint32_t *dataIn, short *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (short)(dataIn[k] & 0x0FFFF);
    return 0;
}

static int
_uint_to_int8(uint32_t *dataIn, int8_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int8_t)(dataIn[k] & 0x0FF);
    return 0;
}

static int
_int64_to_float(int64_t *dataIn, float *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (float)dataIn[k];
    return 0;
}

static int
_int64_to_double(int64_t *dataIn, double *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (double)dataIn[k];
    return 0;
}

static int
_int64_to_int(int64_t *dataIn, int *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int)dataIn[k];
    return 0;
}

static int
_int64_to_int16(int64_t *dataIn, short *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (short)(dataIn[k] & 0x0FFFF);
    return 0;
}

static int
_int64_to_int8(int64_t *dataIn, int8_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int8_t)(dataIn[k] & 0x0FF);
    return 0;
}

static int
_uint64_to_float(uint64_t *dataIn, float *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (float)dataIn[k];
    return 0;
}

static int
_uint64_to_double(uint64_t *dataIn, double *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (double)dataIn[k];
    return 0;
}

static int
_uint64_to_int(uint64_t *dataIn, int *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int)dataIn[k];
    return 0;
}

static int
_uint64_to_int16(uint64_t *dataIn, short *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (short)(dataIn[k] & 0x0FFFF);
    return 0;
}

static int
_uint64_to_int8(uint64_t *dataIn, int8_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int8_t)(dataIn[k] & 0x0FF);
    return 0;
}

static int
_int16_to_float(int16_t *dataIn, float *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (float)dataIn[k];
    return 0;
}

static int
_int16_to_double(int16_t *dataIn, double *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (double)dataIn[k];
    return 0;
}

static int
_int16_to_int64(int16_t *dataIn, int64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int64_t)dataIn[k];
    return 0;
}

static int
_int16_to_uint64(int16_t *dataIn, uint64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (uint64_t)dataIn[k];
    return 0;
}

static int
_int16_to_int(int16_t *dataIn, short *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (short)(dataIn[k] & 0x0FFFF);
    return 0;
}

static int
_int16_to_int8(int16_t *dataIn, int8_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int8_t)(dataIn[k] & 0x0FF);
    return 0;
}

static int
_int8_to_float(int8_t *dataIn, float *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (float)dataIn[k];
    return 0;
}

static int
_int8_to_double(int8_t *dataIn, double *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (double)dataIn[k];
    return 0;
}

static int
_int8_to_int64(int8_t *dataIn, int64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int64_t)dataIn[k];
    return 0;
}

static int
_int8_to_uint64(int8_t *dataIn, uint64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (uint64_t)dataIn[k];
    return 0;
}

static int
_int8_to_int(int8_t *dataIn, int *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (short)(dataIn[k] & 0x0FFFF);
    return 0;
}

static int
_int8_to_int16(int8_t *dataIn, short *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (short)(dataIn[k] & 0x0FFFF);
    return 0;
}

static int
_float_to_int(float *dataIn, int *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int)dataIn[k];
    return 0;
}

static int
_float_to_double(float *dataIn, double *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (double)dataIn[k];
    return 0;
}

static int
_float_to_int64(float *dataIn, int64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int64_t)dataIn[k];
    return 0;
}

static int
_float_to_int16(float *dataIn, int16_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int16_t)dataIn[k];
    return 0;
}

static int
_float_to_int8(float *dataIn, int16_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int8_t)dataIn[k];
    return 0;
}

static int
_double_to_int(double *dataIn, int *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int)dataIn[k];
    return 0;
}

static int
_double_to_float(double *dataIn, float *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (float)dataIn[k];
    return 0;
}

static int
_double_to_int64(double *dataIn, int64_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int64_t)dataIn[k];
    return 0;
}

static int
_double_to_int16(double *dataIn, int16_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int16_t)dataIn[k];
    return 0;
}

static int
_double_to_int8(double *dataIn, int16_t *dataOut, size_t elements)
{
    size_t k;
    for (k = 0; k < elements; k++)
        dataOut[k] = (int8_t)dataIn[k];
    return 0;
}

/*
 * The datatype conversion routines are really only useful
 * when dealing with size changes or type class changes, e.g.
 * integer to floating point or the converse.  All other
 * changes are effectively unsupported, e.g. integer
 * to strings?
 */

size_t
pdc_convert_datatype(void *dataIn, PDC_var_type_t srcType, int ndim, uint64_t *dims, void **dataOut,
                     PDC_var_type_t destType)
{
    int    d;
    size_t elements = dims[0];
    size_t storage_in_bytes;
    for (d = 1; d < ndim; d++)
        elements *= dims[d];
    elements /= 10;
    if (destType >= 0) {
        if ((destType == PDC_INT) || (destType == PDC_UINT) || (destType == PDC_FLOAT))
            storage_in_bytes = elements * sizeof(int);
        else if ((destType == PDC_DOUBLE) || (destType == PDC_INT64) || (destType == PDC_UINT64))
            storage_in_bytes = elements * sizeof(double);
        else if (destType == PDC_INT16)
            storage_in_bytes = elements * sizeof(short);
        else if (destType == PDC_INT8)
            storage_in_bytes = elements;
        else
            storage_in_bytes = elements;
    }
    printf("pdc_convert_datatype: elements = %ld\n", elements);

    switch (srcType) {
        case PDC_INT: {
            if (destType == PDC_FLOAT)
                _int_to_float(dataIn, *dataOut, elements);
            else if (destType == PDC_DOUBLE)
                _int_to_double(dataIn, *dataOut, elements);
            else if (destType == PDC_INT64)
                _int_to_int64(dataIn, *dataOut, elements);
            else if (destType == PDC_INT16)
                _int_to_int16(dataIn, *dataOut, elements);
            else if (destType == PDC_INT8)
                _int_to_int8(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        case PDC_FLOAT: {
            if (destType == PDC_INT)
                _float_to_int(dataIn, *dataOut, elements);
            else if (destType == PDC_DOUBLE)
                _float_to_double(dataIn, *dataOut, elements);
            else if (destType == PDC_INT64)
                _float_to_int64(dataIn, *dataOut, elements);
            else if (destType == PDC_INT16)
                _float_to_int16(dataIn, *dataOut, elements);
            else if (destType == PDC_INT8)
                _float_to_int8(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        case PDC_DOUBLE: {
            if (destType == PDC_INT)
                _double_to_int(dataIn, *dataOut, elements);
            else if (destType == PDC_FLOAT)
                _double_to_float(dataIn, *dataOut, elements);
            else if (destType == PDC_INT64)
                _double_to_int64(dataIn, *dataOut, elements);
            else if (destType == PDC_INT16)
                _double_to_int16(dataIn, *dataOut, elements);
            else if (destType == PDC_INT8)
                _double_to_int8(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        case PDC_UINT: {
            if (destType == PDC_FLOAT)
                _uint_to_float(dataIn, *dataOut, elements);
            else if (destType == PDC_DOUBLE)
                _uint_to_double(dataIn, *dataOut, elements);
            else if (destType == PDC_INT64)
                _uint_to_int64(dataIn, *dataOut, elements);
            else if (destType == PDC_INT16)
                _uint_to_int16(dataIn, *dataOut, elements);
            else if (destType == PDC_INT8)
                _uint_to_int8(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        case PDC_INT64: {
            if (destType == PDC_INT)
                _int64_to_int(dataIn, *dataOut, elements);
            else if (destType == PDC_FLOAT)
                _int64_to_float(dataIn, *dataOut, elements);
            else if (destType == PDC_DOUBLE)
                _int64_to_double(dataIn, *dataOut, elements);
            else if (destType == PDC_INT16)
                _int64_to_int16(dataIn, *dataOut, elements);
            else if (destType == PDC_INT8)
                _int64_to_int8(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        case PDC_UINT64: {
            if (destType == PDC_INT)
                _uint64_to_int(dataIn, *dataOut, elements);
            else if (destType == PDC_FLOAT)
                _uint64_to_float(dataIn, *dataOut, elements);
            else if (destType == PDC_DOUBLE)
                _uint64_to_double(dataIn, *dataOut, elements);
            else if (destType == PDC_INT16)
                _uint64_to_int16(dataIn, *dataOut, elements);
            else if (destType == PDC_INT8)
                _uint64_to_int8(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        case PDC_INT16: {
            if (destType == PDC_INT)
                _int16_to_int(dataIn, *dataOut, elements);
            else if (destType == PDC_FLOAT)
                _int16_to_float(dataIn, *dataOut, elements);
            else if (destType == PDC_DOUBLE)
                _int16_to_double(dataIn, *dataOut, elements);
            else if (destType == PDC_INT64)
                _int16_to_int64(dataIn, *dataOut, elements);
            else if (destType == PDC_UINT64)
                _int16_to_uint64(dataIn, *dataOut, elements);
            else if (destType == PDC_INT8)
                _int16_to_int8(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        case PDC_INT8: {
            if (destType == PDC_INT)
                _int8_to_int(dataIn, *dataOut, elements);
            else if (destType == PDC_FLOAT)
                _int8_to_float(dataIn, *dataOut, elements);
            else if (destType == PDC_DOUBLE)
                _int8_to_double(dataIn, *dataOut, elements);
            else if (destType == PDC_INT16)
                _int8_to_int16(dataIn, *dataOut, elements);
            else if (destType == PDC_INT64)
                _int8_to_int64(dataIn, *dataOut, elements);
            else if (destType == PDC_UINT64)
                _int8_to_uint64(dataIn, *dataOut, elements);
            else
                return -1;
        } break;
        default:
            return -1;
    }
    return storage_in_bytes;
}

int
pdc_transform_increment(void *dataIn, PDC_var_type_t srcType, int ndim, uint64_t *dims, void **dataOut,
                        PDC_var_type_t destType)
{
    int     i;
    int64_t nval, nbytes;
    void *  destBuff;
    size_t  typesize = 1;
    if (srcType >= 0) {
        if ((srcType == PDC_INT) || (srcType == PDC_UINT) || (srcType == PDC_FLOAT))
            typesize = sizeof(int);
        else if ((srcType == PDC_DOUBLE) || (srcType == PDC_INT64) || (srcType == PDC_UINT64))
            typesize = sizeof(double);
        else if (srcType == PDC_INT16)
            typesize = sizeof(short);
    }

    fprintf(stdout, "\n[TRANSFORM] Entering pdc_transform_increment\n");

    nval = 1;
    for (i = 0; i < ndim; i++) {
        nval *= dims[i];
    }

    nbytes   = nval * typesize;
    destBuff = malloc(nbytes);
    memcpy(destBuff, dataIn, nbytes);

    switch (srcType) {
        case PDC_INT:
            for (i = 0; i < nval; i++) {
                ((int *)(destBuff))[i]++;
                ;
            }
            break;
        case PDC_FLOAT:
            for (i = 0; i < nval; i++) {
                ((float *)(destBuff))[i]++;
                ;
            }
            break;
        default:
            fprintf(stdout, "\n[TRANSFORM] Unable to increment values\n");
            return -1;
            break;
    }

    *dataOut = destBuff;

    fprintf(stdout, "\n[TRANSFORM] %ld values successfully incremented\n", nval);
    return nval;
}

/*
 *  https://github.com/Blosc/c-blosc/blob/master/blosc/blosc.h
 */
size_t
pdc_transform_compress(void *dataIn, PDC_var_type_t srcType, int ndim, uint64_t *dims, void **dataOut,
                       PDC_var_type_t destType)
{
    int     clevel    = 9;
    int     doshuffle = BLOSC_BITSHUFFLE;
    int     nval, i, typesize;
    int64_t destsize, nbytes, csize;
    void *  destBuff;

    if ((srcType == PDC_INT) || (srcType == PDC_UINT) || (srcType == PDC_FLOAT))
        typesize = sizeof(int);
    else if ((srcType == PDC_DOUBLE) || (srcType == PDC_INT64) || (srcType == PDC_UINT64))
        typesize = sizeof(double);
    else if (srcType == PDC_INT16)
        typesize = sizeof(short);
    else if (srcType == PDC_INT8)
        typesize = 1;

    fprintf(stdout, "\n[TRANSFORM] Entering pdc_transform_compress\n");
    nval = 1;
    for (i = 0; i < ndim; i++)
        nval *= dims[i];

    nbytes = nval * typesize;
    fprintf(stdout, "[TRANSFORM] Src Buffer size : %d * %d = %ld\n", nval, typesize, nbytes);
    destsize = nbytes + BLOSC_MAX_OVERHEAD;
    fprintf(stdout, "[TRANSFORM] Dest Buffer size : %ld + %d = %ld\n", nbytes, BLOSC_MAX_OVERHEAD, destsize);
    destBuff = malloc(destsize);

    csize = blosc_compress(clevel, doshuffle, typesize, nbytes, dataIn, destBuff, destsize);

    if (csize < 0)
        fprintf(stdout, "[TRANSFORM] Error while compressing data (errcode: %ld)\n", csize);
    if (csize == 0)
        fprintf(stdout, "[TRANSFORM] Unable to compress data (errcode: %ld)\n", csize);

    if (csize > 0) {
        fprintf(stdout, "[TRANSFORM] Data sucessfully compressed from %ld B to %ld B\n", nbytes, csize);
    }
    *dataOut = destBuff;
    return csize;
}

size_t
pdc_transform_decompress(void *dataIn, PDC_var_type_t srcType, int ndim, uint64_t *dims, void **dataOut,
                         PDC_var_type_t destType)
{
    int    i;
    size_t typesize, destsize, dsize;
    void * destBuff = *dataOut;

    fprintf(stdout, "\n[TRANSFORM] Entering pdc_transform_decompress\n");

    if ((destType == PDC_INT) || (destType == PDC_UINT) || (destType == PDC_FLOAT))
        typesize = sizeof(int);
    else if ((destType == PDC_DOUBLE) || (destType == PDC_INT64) || (destType == PDC_UINT64))
        typesize = sizeof(double);
    else if (destType == PDC_INT16)
        typesize = sizeof(short);
    else if (destType == PDC_INT8)
        typesize = 1;

    /* destsize is the byte length of destBuff */
    destsize = typesize;
    for (i = 0; i < ndim; i++)
        destsize *= dims[i];

    dsize = (size_t)blosc_decompress(dataIn, destBuff, destsize);

    if (dsize <= 0)
        fprintf(stdout, "[TRANSFORM] Error while decompressing data (errcode: %zu)\n", dsize);
    else
        fprintf(stdout, "[TRANSFORM] Data sucessfully decompressed!\n");
    return dsize;
}
