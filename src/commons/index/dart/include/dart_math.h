#ifndef DART_MATH_H
#define DART_MATH_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <math.h>
#include "dart_utils.h"

double   log_with_base(double base, double x);
uint32_t uint32_pow(uint32_t base, uint32_t pow);
uint64_t uint64_pow(uint64_t base, uint64_t pow);
int      int_abs(int a);

#endif // END DART_MATH_H