#include "pdc_vector.h"

PDC_VECTOR *
pdc_vector_new()
{
    return pdc_vector_create(100, 2.0);
}

PDC_VECTOR *
pdc_vector_create(size_t initial_capacity, double expansion_factor)
{
    // Allocate memory for the vector struct.
    PDC_VECTOR *vector = (PDC_VECTOR *)malloc(sizeof(PDC_VECTOR));
    if (vector == NULL) {
        return NULL;
    }

    // Allocate memory for the array of items.
    vector->items = (void **)malloc(initial_capacity * sizeof(void *));
    if (vector->items == NULL) {
        free(vector);
        return NULL;
    }

    // Initialize the vector fields.
    vector->item_count       = 0;
    vector->capacity         = initial_capacity;
    vector->expansion_factor = expansion_factor;

    return vector;
}

void
pdc_vector_destroy(PDC_VECTOR *vector)
{
    if (vector == NULL) {
        return;
    }

    // Free all allocated memory for each item.
    for (size_t i = 0; i < vector->item_count; i++) {
        free(vector->items[i]);
    }

    // Free the array of items and the vector struct.
    free(vector->items);
    free(vector);
}

void
pdc_vector_add(PDC_VECTOR *vector, void *item)
{
    if (vector == NULL || item == NULL) {
        return;
    }

    // Expand the array of items if necessary.
    if (vector->item_count >= vector->capacity) {
        vector->capacity *= vector->expansion_factor;
        vector->items = (void **)realloc(vector->items, vector->capacity * sizeof(void *));
        if (vector->items == NULL) {
            return;
        }
    }

    // Add the new item to the end of the array.
    vector->items[vector->item_count++] = item;
}

void *
pdc_vector_get(PDC_VECTOR *vector, size_t index)
{
    if (vector == NULL || index >= vector->item_count) {
        return NULL;
    }

    // Return a pointer to the item at the given index.
    return vector->items[index];
}

size_t
pdc_vector_size(PDC_VECTOR *vector)
{
    if (vector == NULL) {
        return 0;
    }

    // Return the number of items in the vector.
    return vector->item_count;
}

void
pdc_vector_set_expansion_factor(PDC_VECTOR *vector, double expansion_factor)
{
    if (vector == NULL) {
        return;
    }

    // Set the new expansion factor for the vector.
    vector->expansion_factor = expansion_factor;
}

double
pdc_vector_get_expansion_factor(PDC_VECTOR *vector)
{
    if (vector == NULL) {
        return 0;
    }

    // Return the current expansion factor for the vector.
    return vector->expansion_factor;
}

PDC_VECTOR_ITERATOR *
pdc_vector_iterator_new(PDC_VECTOR *vector)
{
    if (vector == NULL) {
        return NULL;
    }

    // Allocate memory for the iterator struct.
    PDC_VECTOR_ITERATOR *iterator = (PDC_VECTOR_ITERATOR *)malloc(sizeof(PDC_VECTOR_ITERATOR));
    if (iterator == NULL) {
        return NULL;
    }

    // Initialize the iterator fields.
    iterator->vector = vector;
    iterator->index  = 0;

    return iterator;
}

void
pdc_vector_iterator_destroy(PDC_VECTOR_ITERATOR *iterator)
{
    if (iterator == NULL) {
        return;
    }

    // Free the iterator struct.
    free(iterator);
}

void *
pdc_vector_iterator_next(PDC_VECTOR_ITERATOR *iterator)
{
    if (iterator == NULL) {
        return NULL;
    }

    // Return the next item in the vector.
    return pdc_vector_get(iterator->vector, iterator->index++);
}

int
pdc_vector_iterator_has_next(PDC_VECTOR_ITERATOR *iterator)
{
    if (iterator == NULL) {
        return 0;
    }

    // Return true if there are more items in the vector.
    return iterator->index < pdc_vector_size(iterator->vector);
}