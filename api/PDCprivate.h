#ifndef _PDCprivate_H
#define _PDCprivate_H 

typedef int 			perr_t;
typedef unsigned long long 	psize_t;
typedef unsigned long long	pid_t;     // int64_t in hdf

typedef int			PDC_INT;
typedef float			PDC_FLOAT;
typedef double			PDC_DOUBLE;
struct enum {
    PDC_UNKNOW = -1,
    PDC_INT,
    PDC_FLOAT,
    PDC_DOUBLE
// more types
} PDC_var_type;

typedef struct {
} PDC_CONT;

typedef struct {
} PDC_OBJ;
#endif /* PDCprivate_H */
