/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF5 PDC VOL connector. The full copyright     *
 * notice, including terms governing use, modification, and redistribution,  *
 * is contained in the COPYING file, which can be found at the root of the   *
 * source code distribution tree.                                            *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef H5VLerror_H
#define H5VLerror_H

/* Public headers needed by this file */
#include <H5Epublic.h>

/**
 * Must be defined for each VOL connector.
 */
#define H5VL_ERR_NAME   H5VL_PDC_NAME

/**
 * Boolean types.
 */
#ifndef FALSE
# define FALSE false
#endif
#ifndef TRUE
# define TRUE true
#endif

/**
 * Define __func__ if not defined.
 */
#if defined(__STDC_VERSION__) &&  (__STDC_VERSION__ < 199901L)
# if defined(__GNUC__) && (__GNUC__ >= 2)
#  define __func__ __FUNCTION__
# else
#  define __func__ "<unknown>"
# endif
#elif defined(_WIN32)
# define __func__ __FUNCTION__
#endif

/**
 * Macros to declare and use error stack and class.
 */
#define H5VL_ERR_STACK_NAME(name) __H5VL ## name ## _err_stack_g
#define H5VL_ERR_STACK_NAME_CONC(name) H5VL_ERR_STACK_NAME(name)
#define H5VL_ERR_STACK_g H5VL_ERR_STACK_NAME_CONC(H5VL_ERR_NAME)

#define H5VL_ERR_CLS_NAME(name) __H5VL ## name ## _err_cls_g
#define H5VL_ERR_CLS_NAME_CONC(name) H5VL_ERR_CLS_NAME(name)
#define H5VL_ERR_CLS_g H5VL_ERR_CLS_NAME_CONC(H5VL_ERR_NAME)

/**
 * Macro to print out the VOL connector's current error stack
 * and then clear it for future use
 */
#define HERROR_DUMP_STACK(err_stack) do {                           \
    H5E_auto2_t func;                                               \
    /* Check whether automatic error reporting has been disabled */ \
    H5Eget_auto2(H5E_DEFAULT, &func, NULL);                         \
    if(func && (err_stack > 0) && (H5Eget_num(err_stack) > 0)) {    \
        H5Eprint2(err_stack, NULL);                                 \
        H5Eclear2(err_stack);                                       \
    }                                                               \
} while(0)

/**
 * Use this macro when entering a VOL connector function.
 */
#define FUNC_ENTER_VOL(type, init)                  \
    type __ret_value = init;                        \
    hbool_t __err_occurred = FALSE;

/**
 * Use this macro when leaving a VOL connector function.
 */
#define FUNC_LEAVE_VOL_NAME(name)                       \
    if(__err_occurred)                                  \
        HERROR_DUMP_STACK(H5VL_ERR_STACK_NAME(name));   \
    return __ret_value;
#define FUNC_LEAVE_VOL                                  \
    FUNC_LEAVE_VOL_NAME(H5VL_ERR_NAME)

/**
 * Use this macro to set return value.
 */
#define FUNC_RETURN_SET(ret)                        \
    (__ret_value = ret)

/**
 * Use this macro to test for error.
 */
#define FUNC_ERRORED                                \
    (__err_occurred == TRUE)

/**
 * HERROR macro, used to facilitate error reporting between a FUNC_ENTER()
 * and a FUNC_LEAVE() within a function body.  The arguments are the major
 * error number, the minor error number, and a description of the error.
 */
#define HERROR(err_stack, err_cls, maj_id, min_id, ...) do {            \
    H5E_auto2_t func;                                                   \
    /* Check whether automatic error reporting has been disabled */     \
    H5Eget_auto2(H5E_DEFAULT, &func, NULL);                             \
    if(func && err_stack > 0 && err_cls > 0) {                          \
        H5Epush2(err_stack, __FILE__, __func__, __LINE__,               \
            err_cls, maj_id, min_id, __VA_ARGS__);                      \
    }                                                                   \
} while(0)

/**
 * HCOMMON_ERROR macro, used by HDONE_ERROR and HGOTO_ERROR
 * (Shouldn't need to be used outside this header file)
 */
#define HCOMMON_ERROR_NAME(name, maj, min, ...) do {    \
   HERROR(H5VL_ERR_STACK_NAME(name),                    \
       H5VL_ERR_CLS_NAME(name),                         \
       maj, min, __VA_ARGS__);                          \
   __err_occurred = TRUE;                               \
} while(0)
#define HCOMMON_ERROR(maj, min, ...)                    \
    HCOMMON_ERROR_NAME(H5VL_ERR_NAME, maj, min, __VA_ARGS__)

/**
 * HDONE_ERROR macro, used to facilitate error reporting between a
 * FUNC_ENTER() and a FUNC_LEAVE() within a function body, but _AFTER_ the
 * "done:" label.  The arguments are
 * the major error number, the minor error number, a return value, and a
 * description of the error.
 * (This macro can also be used to push an error and set the return value
 *      without jumping to any labels)
 */
#define HDONE_ERROR(maj, min, ret_val, ...) do {    \
   HCOMMON_ERROR(maj, min, __VA_ARGS__);            \
   __ret_value = ret_val;                           \
} while(0)

/**
 * HGOTO_ERROR macro, used to facilitate error reporting between a
 * FUNC_ENTER() and a FUNC_LEAVE() within a function body.  The arguments are
 * the major error number, the minor error number, the return value, and an
 * error string.  The return value is assigned to a variable `ret_value' and
 * control branches to the `done' label.
 */
#define HGOTO_ERROR(maj, min, ret_val, ...) do {    \
   HCOMMON_ERROR(maj, min, __VA_ARGS__);            \
   HGOTO_DONE(ret_val);                             \
} while(0)

/**
 * HGOTO_DONE macro, used to facilitate normal return between a FUNC_ENTER()
 * and a FUNC_LEAVE() within a function body. The argument is the return
 * value which is assigned to the `ret_value' variable.  Control branches to
 * the `done' label.
 */
#define HGOTO_DONE(ret_val) do {    \
    __ret_value = ret_val;          \
    goto done;                      \
} while(0)

#endif /* H5VLerror_H */
