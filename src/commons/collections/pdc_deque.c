#include "pdc_deque.h"
#include <stdio.h>

#define DEFAULT_CAPACITY 16

void
PDC_deque_init(PDC_deque_t *deque)
{
    deque->data     = malloc(sizeof(void *) * DEFAULT_CAPACITY);
    deque->size     = 0;
    deque->capacity = DEFAULT_CAPACITY;
    deque->head     = 0;
    deque->tail     = 0;
}

static void
resize_deque(PDC_deque_t *deque, size_t new_capacity)
{
    void **new_data = malloc(sizeof(void *) * new_capacity);
    if (new_data == NULL) {
        printf("Failed to allocate memory for deque!\n");
        exit(1);
    }

    size_t i = 0;
    while (deque->head != deque->tail) {
        new_data[i++] = deque->data[deque->head];
        deque->head   = (deque->head + 1) % deque->capacity;
    }

    free(deque->data);
    deque->data     = new_data;
    deque->capacity = new_capacity;
    deque->head     = 0;
    deque->tail     = i;
}

void
PDC_deque_push_front(PDC_deque_t *deque, void *value)
{
    if (deque->size == deque->capacity) {
        resize_deque(deque, deque->capacity * 2);
    }
    deque->head              = (deque->head - 1 + deque->capacity) % deque->capacity;
    deque->data[deque->head] = value;
    deque->size++;
}

void
PDC_deque_push_back(PDC_deque_t *deque, void *value)
{
    if (deque->size == deque->capacity) {
        resize_deque(deque, deque->capacity * 2);
    }
    deque->data[deque->tail] = value;
    deque->tail              = (deque->tail + 1) % deque->capacity;
    deque->size++;
}

void *
PDC_deque_pop_front(PDC_deque_t *deque)
{
    if (deque->size == 0) {
        return NULL;
    }
    void *value = deque->data[deque->head];
    deque->head = (deque->head + 1) % deque->capacity;
    deque->size--;
    if (deque->size <= deque->capacity / 4) {
        resize_deque(deque, deque->capacity / 2);
    }
    return value;
}

void *
PDC_deque_pop_back(PDC_deque_t *deque)
{
    if (deque->size == 0) {
        return NULL;
    }
    deque->tail = (deque->tail - 1 + deque->capacity) % deque->capacity;
    void *value = deque->data[deque->tail];
    deque->size--;
    if (deque->size <= deque->capacity / 4) {
        resize_deque(deque, deque->capacity / 2);
    }
    return value;
}

void
PDC_deque_free(PDC_deque_t *deque)
{
    free(deque->data);
}