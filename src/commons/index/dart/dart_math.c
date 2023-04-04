#include "dart_math.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>

double log_with_base(double base, double x){
    return log(x)/log(base);
}

uint32_t uint32_pow(uint32_t base, uint32_t pow) {
    uint32_t p = 0;
    uint32_t rst = 1;
    for (p = 0; p < pow; p++) {
        rst = base * rst;
    }
    return rst;
}

uint64_t uint64_pow(uint64_t base, uint64_t pow) {
    uint64_t p = 0;
    uint64_t rst = 1;
    for (p = 0; p < pow; p++) {
        rst = base * rst;
    }
    return rst;
}



int int_abs(int a){
    if (a < 0) {
        return 0-a;
    }
    return a;
}