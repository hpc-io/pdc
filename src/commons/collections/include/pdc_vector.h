#ifndef PDC_VECTOR_H
#define PDC_VECTOR_H

#include <stdlib.h>

/**
 * A generic vector data structure that stores a variable number of items of any type.
 * A vector is similar to an array, except that it can grow and shrink dynamically.
 */
typedef struct {
    void **items;            // Pointer to the array of items.
    size_t item_count;       // Number of items in the vector.
    size_t capacity;         // Capacity of the array of items.
    double expansion_factor; // Factor by which the capacity is expanded.
} PDC_VECTOR;

/**
 * A generic iterator for iterating over the items in a PDC_VECTOR.
 */
typedef struct {
    PDC_VECTOR *vector;  // The vector being iterated over.
    size_t    index; // The index of the next item to be returned.
} PDC_VECTOR_ITERATOR;

/**
 * Creates a new PDC_VECTOR with default initial capacity 100 and default expansion factor 2.0.
 * @return A pointer to the new PDC_VECTOR.
 */
PDC_VECTOR *pdc_vector_new();

/**
 * Creates a new PDC_VECTOR with the given initial capacity and expansion factor.
 * @param initial_capacity The initial capacity of the vector.
 * @param expansion_factor The factor by which the capacity is expanded when the vector is full.
 *
 * @return A pointer to the new PDC_VECTOR.
 */
PDC_VECTOR *pdc_vector_create(size_t initial_capacity, double expansion_factor);

/**
 * Destroys the given PDC_VECTOR and frees all allocated memory.
 * @param vector The PDC_VECTOR to destroy.
 */
void pdc_vector_destroy(PDC_VECTOR *vector);

/**
 * Adds the given item to the end of the given PDC_VECTOR.
 * @param vector The PDC_VECTOR to add the item to.
 * @param item The item to add to the PDC_VECTOR.
 *
 */
void pdc_vector_add(PDC_VECTOR *vector, void *item);

/**
 * Gets the item at the given index in the given PDC_VECTOR.
 * @param vector The PDC_VECTOR to get the item from.
 * @param index The index of the item to get.
 *
 * @return A pointer to the item at the given index.
 */
void *pdc_vector_get(PDC_VECTOR *vector, size_t index);

/**
 * Sets the item at the given index in the given PDC_VECTOR.
 * @param vector The PDC_VECTOR to set the item in.
 *
 * @return The number of items in the vector.
 */
size_t pdc_vector_size(PDC_VECTOR *vector);

/**
 * Sets the expansion factor for the given PDC_VECTOR.
 * @param vector The PDC_VECTOR to set the expansion factor for.
 * @param expansion_factor The factor by which the capacity is expanded when the vector is full.
 */
void pdc_vector_set_expansion_factor(PDC_VECTOR *vector, double expansion_factor);

/**
 * Gets the expansion factor for the given PDC_VECTOR.
 * @param vector The PDC_VECTOR to get the expansion factor for.
 */
double pdc_vector_get_expansion_factor(PDC_VECTOR *vector);

/**
 * Creates a new PDC_VECTOR_ITERATOR for the given PDC_VECTOR.
 * @param vector The PDC_VECTOR to create the iterator for.
 * @return A pointer to the new PDC_VECTOR_ITERATOR.
 */
PDC_VECTOR_ITERATOR *pdc_vector_iterator_new(PDC_VECTOR *vector);

/**
 * Destroys the given PDC_VECTOR_ITERATOR and frees all allocated memory.
 * @param iterator The PDC_VECTOR_ITERATOR to destroy.
 */
void pdc_vector_iterator_destroy(PDC_VECTOR_ITERATOR *iterator);

/**
 * Returns the next item in the PDC_VECTOR_ITERATOR.
 * @param iterator The PDC_VECTOR_ITERATOR to get the next item from.
 * @return A pointer to the next item in the PDC_VECTOR_ITERATOR.
 */
void *pdc_vector_iterator_next(PDC_VECTOR_ITERATOR *iterator);

/**
 * Returns true if the PDC_VECTOR_ITERATOR has more items.
 * @param iterator The PDC_VECTOR_ITERATOR to check.
 * @return True if the PDC_VECTOR_ITERATOR has more items.
 */
int pdc_vector_iterator_has_next(PDC_VECTOR_ITERATOR *iterator);

#endif // PDC_VECTOR_H