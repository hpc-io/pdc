#ifndef _STACK_OPS_H
#define _STACK_OPS_H

#include "pdc_config.h"
#include "pdc_private.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>

typedef void *hash_table_t;

typedef struct profileEntry {
    struct profileEntry *next;
    struct profileEntry *prev;
    const char *         ftnkey;
    const char *         tags;
    int64_t              count;
    int64_t              localTotal;
    int64_t              CumTotal;
    int64_t              locmin;
    int64_t              locmax;
    double               usecTotal;
    struct timespec      callTime;
    struct timespec      startTime;
    struct timespec      totalTime;
    struct timespec      selfTime;

    struct profileEntry *parent;
} profileEntry_t;

// typedef enum _boolean {FALSE = 0, TRUE} bool_t;
extern pbool_t enableProfiling;

#ifndef RESET_TIMER
#define RESET_TIMER(x) (x).tv_sec = (x).tv_nsec = 0;
#endif

#ifndef TIMER_DIFF
/* t0 = t1 - t2 */
#define TIMER_DIFF(t0, t1, t2)                                                                               \
    {                                                                                                        \
        if (t2.tv_nsec > (t1).tv_nsec) {                                                                     \
            (t1).tv_nsec += 1000000000;                                                                      \
            (t1).tv_sec -= 1;                                                                                \
        }                                                                                                    \
        (t0).tv_sec  = (t1).tv_sec - (t2).tv_sec;                                                            \
        (t0).tv_nsec = (t1).tv_nsec - (t2).tv_nsec;                                                          \
    }
#endif

#ifndef TIMER_ADD
/* t0 += t1 */
#define TIMER_ADD(t0, t1)                                                                                    \
    {                                                                                                        \
        (t0).tv_sec += (t1).tv_sec;                                                                          \
        if (((t0).tv_nsec += (t1).tv_nsec) > 10000000000) {                                                  \
            (t0).tv_sec += 1;                                                                                \
            (t0).tv_nsec -= 10000000000;                                                                     \
        }                                                                                                    \
    }
#endif

void initialize_profile(void **table, size_t tabsize);
void finalize_profile();
void push(const char *ftnkey, const char *tags);
void pop();

#endif
