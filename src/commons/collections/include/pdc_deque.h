#ifndef PDC_DEQUE_H
#define PDC_DEQUE_H

#include <stdlib.h>

// Struct for the deque data structure
typedef struct PDC_deque_t {
    void **data;     // Pointer to the data array
    size_t size;     // Current size of the deque
    size_t capacity; // Current capacity of the deque
    size_t head;     // Index of the head of the deque
    size_t tail;     // Index of the tail of the deque
} PDC_deque_t;

// Initialize the deque
void PDC_deque_init(PDC_deque_t *deque);

// Push an element onto the front of the deque
void PDC_deque_push_front(PDC_deque_t *deque, void *value);

// Push an element onto the back of the deque
void PDC_deque_push_back(PDC_deque_t *deque, void *value);

// Pop an element from the front of the deque
void *PDC_deque_pop_front(PDC_deque_t *deque);

// Pop an element from the back of the deque
void *PDC_deque_pop_back(PDC_deque_t *deque);

// Free memory allocated for the deque
void PDC_deque_free(PDC_deque_t *deque);

#endif /* PDC_DEQUE_H */