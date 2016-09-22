#ifndef _PDCprivate_H
#define _PDCprivate_H 

typedef int 			perr_t;
typedef unsigned long long 	psize_t;
typedef unsigned long long	pid_t;     // int64_t in hdf

typedef int			PDC_INT;
typedef float			PDC_FLOAT;
typedef double			PDC_DOUBLE;
struct enum {
    PDC_UNKNOW       = -1, /*error                                      */
    PDC_INT          = 0,  /*integer types                              */
    PDC_FLOAT        = 1,  /*floating-point types                       */
    PDC_DOUBLE       = 2,  /* double types                              */
    PDC_H5T_STRING   = 3,  /*character string types                     */
    PDC_COMPOUND     = 4,  /*compound types                             */
    PDC_ENUM         = 5,  /*enumeration types                          */   
    PDC_ARRAY        = 6,  /*Array types                                */
 
    NCLASSES         = 7   /*this must be last                          */
} PDC_var_type;

typedef struct {
} PDC_CONT;

typedef struct {
} PDC_OBJ;
#endif /* PDCprivate_H */
