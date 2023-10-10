#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <uuid/uuid.h>
#include "dart_utils.h"
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

void
random_seed(long int seed)
{
    if (seed > 0)
        srand(seed);
    else if (seed == 0)
        srand(time(NULL));
}

double
random_range(double min, double max)
{
    // return numbers between a given range
    double range = (max - min);
    double div   = RAND_MAX / range;
    return min + (rand() / div);
}

double
random_uniform()
{
    // return numbers between 0 and 1
    return (double)rand() / (RAND_MAX + 1.0);
}

double
random_normal(double mean, double dev)
{
    // returns numbers based on normal distribution given mean and std dev

    // normal distribution centered on 0 with std dev of 1
    // Box-Muller transform
    double randomNum_normal =
        sqrt(-2 * log((rand() + 1.0) / (RAND_MAX + 1.0))) * cos(2 * M_PI * (rand() + 1.0) / (RAND_MAX + 1.0));
    double y = mean + dev * randomNum_normal;

    return y;
}

double
random_exponential(double mean)
{
    // returns numbers based on exp distr given mean
    double x = 1.0 / mean; // inverse lambda

    double u; // this will be my uniform random variable
    double exp_value;

    // Pull a uniform random number (0 < z < 1)
    do {
        u = random_uniform();
    } while ((u == 0) || (u == 1));
    exp_value = -x * log(u);

    return (exp_value);
}

int64_t
get_timestamp_ns()
{
    struct timespec current;
    int64_t         rst = -1;

    if (clock_gettime(CLOCK_REALTIME, &current) == -1) {
        return rst;
    }

    return current.tv_sec * (int64_t)1e9 + current.tv_nsec;
}

int64_t
get_timestamp_us()
{
    return get_timestamp_ns() / 1000;
}

int64_t
get_timestamp()
{
    return get_timestamp_us() / 1000;
}