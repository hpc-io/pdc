#ifndef PDC_STACK_H
#define PDC_STACK_H

#include <stdlib.h>

// Struct for the stack data structure
typedef struct PDC_stack_t {
    void **data;     // Pointer to the data array
    size_t size;     // Current size of the stack
    size_t capacity; // Current capacity of the stack
} PDC_stack_t;

// Initialize the stack
void PDC_stack_init(PDC_stack_t *stack);

// Push an element onto the stack
void PDC_stack_push(PDC_stack_t *stack, void *value);

// Pop an element from the stack
void *PDC_stack_pop(PDC_stack_t *stack);

// Free memory allocated for the stack
void PDC_stack_free(PDC_stack_t *stack);

#endif /* PDC_STACK_H */