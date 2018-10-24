#include "pdc.h"
#include <blosc.h>

/*
 * >> pdc_private.h
 *
 *  typedef enum {
 *      PDC_UNKNOWN      = -1, 
 *      PDC_INT          = 0,  
 *      PDC_FLOAT        = 1,
 *      PDC_DOUBLE       = 2,
 *  } PDC_var_type_t;
 */
int
pdc_transform_increment (void *dataIn, PDC_var_type_t srcType, int typesize, int ndim, uint64_t *dims, void **dataOut)
{
  int i;
  PDC_int_t nval, nbytes;
  void *destBuff;
  
  fprintf ( stdout, "\n[TRANSFORM] Entering pdc_transform_increment\n");

  nval = 1;
  for ( i = 0; i < ndim; i++ ) {
    nval *= dims [i];
  }

  nbytes = nval * typesize;
  destBuff = malloc (nbytes);
  memcpy (destBuff, dataIn, nbytes);

  switch (srcType) {
  case PDC_INT:
    for ( i = 0; i < nval; i++ ) {
      ((int *)(destBuff))[i]++;;
    }
    break;
  case PDC_FLOAT:
    for ( i = 0; i < nval; i++ ) {
      ((float *)(destBuff))[i]++;;
    }
    break;
  default:
    fprintf ( stdout, "\n[TRANSFORM] Unable to increment values\n");
    return -1;
    break;
  }

  *dataOut = destBuff;

  fprintf ( stdout, "\n[TRANSFORM] %d values successfully incremented\n", nval);
  return 0;
}


/*
 *  https://github.com/Blosc/c-blosc/blob/master/blosc/blosc.h
 */
size_t
pdc_transform_compress (void *dataIn, PDC_var_type_t srcType, int typesize, int ndim, uint64_t *dims, void **dataOut)
{
  int clevel = 9;
  int doshuffle = BLOSC_BITSHUFFLE;
  int nval, i;
  size_t destsize, nbytes, csize;
  void *destBuff;

  struct PDC_obj_prop *obj_prop;
  
  fprintf ( stdout, "\n[TRANSFORM] Entering pdc_transform_compress\n");
  nval = 1;
  for ( i = 0; i < ndim; i++ ) 
      nval *= dims [i];

  nbytes = nval * typesize;
  fprintf (stdout, "[TRANSFORM] Src Buffer size : %d * %d = %ld\n", nval, typesize, nbytes);
  destsize = nbytes + BLOSC_MAX_OVERHEAD;
  fprintf (stdout, "[TRANSFORM] Dest Buffer size : %ld + %d = %ld\n", nbytes, BLOSC_MAX_OVERHEAD, destsize);
  destBuff = malloc (destsize);

  csize = blosc_compress (clevel, doshuffle, typesize, nbytes, 
			  dataIn, destBuff, destsize);

  if (csize < 0)
    fprintf (stdout, "[TRANSFORM] Error while compressing data (errcode: %ld)\n", csize);
  if (csize == 0)
    fprintf (stdout, "[TRANSFORM] Unable to compress data (errcode: %ld)\n", csize);

  if (csize > 0) {
    fprintf (stdout, "[TRANSFORM] Data sucessfully compressed from %ld B to %ld B\n", nbytes, csize);
  }
  *dataOut = destBuff;
  return csize;
}

int pdc_transform_decompress (pdcid_t objPropIn)
{
  int ndim, nval, i;
  uint64_t *dims;
  PDC_var_type_t srcType;
  size_t typesize, destsize, dsize;
  void *destBuff;
  
  struct PDC_obj_prop *obj_prop;
  
  fprintf ( stdout, "\n[TRANSFORM] Entering pdc_transform_decompress (%lld)\n", objPropIn);

  obj_prop = PDCobj_prop_get_info (objPropIn);
  srcType = ((struct PDC_obj_prop *)(obj_prop))->type;
  typesize = get_datatype_size (srcType);
  
  ndim = ((struct PDC_obj_prop *)(obj_prop))->ndim;
  dims = (uint64_t *) malloc ( ndim * sizeof (uint64_t));
  nval = 1;
  for ( i = 0; i < ndim; i++ ) {
    dims [i] = ((struct PDC_obj_prop *)(obj_prop))->dims [i];
    nval *= dims [i];
  }

  destsize = nval * typesize;
  destBuff = malloc (destsize);  

  dsize = blosc_decompress (((struct PDC_obj_prop *)(obj_prop))->buf, destBuff, destsize);

  if (dsize <= 0)
    fprintf (stdout, "[TRANSFORM] Error while decompressing data (errcode: %d)\n", dsize);

  if (dsize > 0) {
    fprintf (stdout, "[TRANSFORM] Data sucessfully decompressed!\n");
    PDCprop_set_obj_buf (objPropIn, destBuff);
  }
}
