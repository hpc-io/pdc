#ifndef DART_ALGO_H
#define DART_ALGO_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <stdint.h>
#include <uuid/uuid.h>
#include "dart_math.h"

void to_bytes(uint32_t val, uint8_t *bytes);

uint32_t to_int32(const uint8_t *bytes);

void md5(const uint8_t *initial_msg, size_t initial_len, uint8_t *digest);

uint32_t murmur3_32(const uint8_t *key, size_t len, uint32_t seed);

uint32_t md5_hash(int prefix_len, char *word);

uint32_t djb2_hash(char *str, int len);

#endif // DART_ALGO_H