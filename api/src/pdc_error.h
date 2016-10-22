#ifndef _pdc_error_H
#define _pdc_error_H

#include <stdio.h>
#include <stdarg.h>
#include "pdc_public.h"
#include "pdc_private.h"

/*
 *  PDC Boolean type.
 */
#ifndef FALSE
#   define FALSE 0
#endif
#ifndef TRUE
#   define TRUE 1
#endif

extern pbool_t err_occurred;

/*
 * PGOTO_DONE macro. The argument is the return value which is
 * assigned to the `ret_value' variable.  Control branches to
 * the `done' label.
 */
#define PGOTO_DONE(ret_val)  do {       \
    ret_value = ret_val;                \
    goto done;                          \
} while (0) 

/*
 * PGOTO_ERROR macro. The arguments are the return value and an
 * error string.  The return value is assigned to a variable `ret_value' and
 * control branches to the `done' label.
 */
#define PGOTO_ERROR(ret_val, ...) do {                       \
    fprintf(stderr, "Error in %s:%d\n", __FILE__, __LINE__); \
    fprintf(stderr, " # %s(): ", __func__);                  \
    fprintf(stderr, __VA_ARGS__);                            \
    fprintf(stderr, "\n");                                   \
    PGOTO_DONE(ret_val);                                     \
}  while (0)

#endif /* end of _pdc_error_H */
