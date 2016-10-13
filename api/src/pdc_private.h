#ifndef _pdc_private_H
#define _pdc_private_H 
#include <stdint.h>

typedef int 			    perr_t;
typedef int64_t             pdcid_t;
typedef unsigned long long 	psize_t;
typedef unsigned int        pbool_t;

typedef int			        PDC_int_t;
typedef float			    PDC_float_t;
typedef double			    PDC_double_t;

typedef enum {
    PDC_UNKNOWN      = -1, /*error                                      */
    PDC_INT          = 0,  /*integer types                              */
    PDC_FLOAT        = 1,  /*floating-point types                       */
    PDC_DOUBLE       = 2,  /*double types                               */
    PDC_STRING       = 3,  /*character string types                     */
    PDC_COMPOUND     = 4,  /*compound types                             */
    PDC_ENUM         = 5,  /*enumeration types                          */   
    PDC_ARRAY        = 6,  /*Array types                                */
 
    NCLASSES         = 7   /*this must be last                          */
} PDC_var_type_t;

#define SUCCEED    0
#define FAIL    (-1)

#define PDCmemset(X,C,Z)     memset((void*)(X),C,Z)

#define FUNC_ENTER(X) do {            \
} while(0)

#define FUNC_LEAVE(ret_value) do {              \
        return(ret_value);                      \
} while(0)

#endif /* end of _pdc_private_H */
