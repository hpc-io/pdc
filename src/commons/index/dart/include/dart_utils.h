#ifndef DART_UTILS_H
#define DART_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <uuid/uuid.h>
#include <stdint.h>

void   random_seed(long int seed);
double random_uniform();
double random_range(double min, double max);
double random_normal(double mean, double dev);
double random_exponential(double mean);

int64_t get_timestamp_ns();

int64_t get_timestamp_us();

int64_t get_timestamp_ms();

int64_t get_timestamp_s();

#endif // DART_UTILS_H