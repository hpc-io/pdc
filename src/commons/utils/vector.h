#ifndef UTILS_VECTOR_H_DEFINED
#define UTILS_VECTOR_H_DEFINED

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define VECTOR_INITIAL_CAPACITY 128

// Define a vector type
typedef struct {
  int size;      // slots used so far
  int capacity;  // total available slots
  uint64_t *data;     // array of integers we're storing
} Vector;

void vector_init(Vector *vector);

void vector_append(Vector *vector, uint64_t value);

int vector_get(Vector *vector, int index);

void vector_set(Vector *vector, int index, uint64_t value);

void vector_double_capacity_if_full(Vector *vector);

void vector_free(Vector *vector);

#endif
