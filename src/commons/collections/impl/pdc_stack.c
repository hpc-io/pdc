#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../pdc_stack.h"

#define DEFAULT_CAPACITY 16

void
stack_init(PDC_stack_t *stack)
{
    stack->data     = malloc(sizeof(void *) * DEFAULT_CAPACITY);
    stack->size     = 0;
    stack->capacity = DEFAULT_CAPACITY;
}

void
stack_push(PDC_stack_t *stack, void *value)
{
    if (stack->size == stack->capacity) {
        size_t new_capacity = stack->capacity * 2;
        void **new_data     = realloc(stack->data, sizeof(void *) * new_capacity);
        if (new_data == NULL) {
            printf("Failed to reallocate memory for stack!\n");
            exit(1);
        }
        stack->data     = new_data;
        stack->capacity = new_capacity;
    }
    stack->data[stack->size++] = value;
}

void *
stack_pop(PDC_stack_t *stack)
{
    if (stack->size == 0) {
        printf("Stack underflow!\n");
        exit(1);
    }
    return stack->data[--stack->size];
}

void
stack_free(PDC_stack_t *stack)
{
    free(stack->data);
}