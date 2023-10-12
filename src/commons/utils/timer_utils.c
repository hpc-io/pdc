#include "timer_utils.h"

#define MICROSECONDS_IN_SECONDS 1000000

/**
 * Function that returns the current timestamp in microseconds
 * since the Epoch (1970-01-01 00:00:00:000:000 UTC).
 */
int64_t
timer_us_timestamp()
{
    struct timeval tmp;
    gettimeofday(&(tmp), NULL);
    return ((int64_t)tmp.tv_usec) + (int64_t)(tmp.tv_sec * MICROSECONDS_IN_SECONDS);
}

/**
 * Function that returns the current timestamp in miliseconds
 * since the Epoch (1970-01-01 00:00:00:000 UTC).
 */
int64_t
timer_ms_timestamp()
{
    return (timer_us_timestamp() / 1000);
}

/**
 * Function that returns the current timestamp in seconds
 * since the Epoch (1970-01-01 00:00:00 UTC).
 */
int64_t
timer_s_timestamp()
{
    return (timer_ms_timestamp() / 1000);
}

void
timer_start(stopwatch_t *t)
{
    t->start_mark = timer_us_timestamp();
    t->pause_mark = 0;
    t->running    = true;
    t->paused     = false;
}

void
timer_pause(stopwatch_t *t)
{
    if (!(t->running) || (t->paused))
        return;

    t->pause_mark = timer_us_timestamp() - (t->start_mark);
    t->running    = false;
    t->paused     = true;
}

void
timer_unpause(stopwatch_t *t)
{
    if (t->running || !(t->paused))
        return;

    t->start_mark = timer_us_timestamp() - (t->pause_mark);
    t->running    = true;
    t->paused     = false;
}

double
timer_delta_us(stopwatch_t *t)
{
    if (t->running)
        return timer_us_timestamp() - (t->start_mark);

    if (t->paused)
        return t->pause_mark;

    // Will never actually get here
    return (double)((t->pause_mark) - (t->start_mark));
}

double
timer_delta_ms(stopwatch_t *t)
{
    return (timer_delta_us(t) / 1000.0);
}

double
timer_delta_s(stopwatch_t *t)
{
    return (timer_delta_ms(t) / 1000.0);
}

double
timer_delta_m(stopwatch_t *t)
{
    return (timer_delta_s(t) / 60.0);
}

double
timer_delta_h(stopwatch_t *t)
{
    return (timer_delta_m(t) / 60.0);
}
