/** timer.h
 *  Microsecond timer in C, using `gettimeofday()`.
 *
 *  Note: `gettimeofday()` is Unix, GNU/Linux and
 *        Mac OS X system-specific.
 *
 *        The only portable function is time.h's `clock()`,
 *        but it isn't very precise.
 *
 *        See: http://www.songho.ca/misc/timer/timer.html
 *
 *  Copyright (c) 2011,2013 Alexandre Dantas <eu@alexdantas.net>
 */

#ifndef TIMER_UTILS_H_DEFINED
#define TIMER_UTILS_H_DEFINED

#include <stdio.h>
#include <sys/time.h> /* gettimeofday() */
#include <stdbool.h>  /* bool */
#include <stdint.h>   /* int64_t */

typedef struct stopwatch_t {
    int64_t start_mark; /* Timer start point */
    int64_t pause_mark; /* In case we pause the timer */
    bool    running;    /* Is it running? */
    bool    paused;     /* Is it paused? */

} stopwatch_t;

int64_t timer_us_timestamp();

int64_t timer_ms_timestamp();

int64_t timer_s_timestamp();

/** Starts the timer.
 *
 *  @note If called multiple times, restarts the timer.
 */
void timer_start(stopwatch_t *t);

/** Pauses the timer.
 *
 *  @note If called multiple times, it does nothing.
 */
void timer_pause(stopwatch_t *t);

/** Unpauses the timer.
 *
 *  @note If the timer's not paused or this is called
 *        multiple times, it does nothing.
 */
void timer_unpause(stopwatch_t *t);

/** Returns the time difference in microseconds
 *
 * @note (1/1000000 seconds)
 */
double timer_delta_us(stopwatch_t *t);

/** Returns the time difference in miliseconds.
 *
 * @note (1/1000 seconds)
 */
double timer_delta_ms(stopwatch_t *t);

/** Returns the time difference in seconds. */
double timer_delta_s(stopwatch_t *t);

/** Returns the time difference in minutes (60 seconds). */
double timer_delta_m(stopwatch_t *t);

/** Returns the time difference in hours (3600 seconds). */
double timer_delta_h(stopwatch_t *t);

#endif /* TIMER_UTILS_H_DEFINED */
