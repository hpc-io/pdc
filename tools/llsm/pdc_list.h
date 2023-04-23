#ifndef PDC_LIST_H
#define PDC_LIST_H

#include <stdlib.h>

/**
 * A generic list data structure that stores a variable number of items of any type.
 */
typedef struct {
    void **items;            // Pointer to the array of items.
    size_t item_count;       // Number of items in the list.
    size_t capacity;         // Capacity of the array of items.
    double expansion_factor; // Factor by which the capacity is expanded.
} PDC_LIST;

/**
 * A generic iterator for iterating over the items in a PDC_LIST.
 */
typedef struct {
    PDC_LIST *list; // The list being iterated over.
    size_t index;   // The index of the next item to be returned.
} PDC_LIST_ITERATOR;


/**
 * Creates a new PDC_LIST with default initial capacity 100 and default expansion factor 2.0.
 * @return A pointer to the new PDC_LIST.
 */
PDC_LIST *pdc_list_new();

/**
 * Creates a new PDC_LIST with the given initial capacity and expansion factor.
 * @param initial_capacity The initial capacity of the list.
 * @param expansion_factor The factor by which the capacity is expanded when the list is full.
 * 
 * @return A pointer to the new PDC_LIST.
 */
PDC_LIST *pdc_list_create(size_t initial_capacity, double expansion_factor);

/**
 * Destroys the given PDC_LIST and frees all allocated memory.
 * @param list The PDC_LIST to destroy.
 */
void pdc_list_destroy(PDC_LIST *list);

/**
 * Adds the given item to the end of the given PDC_LIST.
 * @param list The PDC_LIST to add the item to.
 * @param item The item to add to the PDC_LIST.
 * 
 */
void pdc_list_add(PDC_LIST *list, void *item);

/**
 * Gets the item at the given index in the given PDC_LIST.
 * @param list The PDC_LIST to get the item from.
 * @param index The index of the item to get.
 * 
 * @return A pointer to the item at the given index.
 */
void *pdc_list_get(PDC_LIST *list, size_t index);

/**
 * Sets the item at the given index in the given PDC_LIST.
 * @param list The PDC_LIST to set the item in.
 * 
 * @return The number of items in the list.
 */
size_t pdc_list_size(PDC_LIST *list);

/**
 * Sets the expansion factor for the given PDC_LIST.
 * @param list The PDC_LIST to set the expansion factor for.
 * @param expansion_factor The factor by which the capacity is expanded when the list is full.
 */
void pdc_list_set_expansion_factor(PDC_LIST *list, double expansion_factor);

/**
 * Gets the expansion factor for the given PDC_LIST.
 * @param list The PDC_LIST to get the expansion factor for.
 */
double pdc_list_get_expansion_factor(PDC_LIST *list);

/**
 * Creates a new PDC_LIST_ITERATOR for the given PDC_LIST.
 * @param list The PDC_LIST to create the iterator for.
 * @return A pointer to the new PDC_LIST_ITERATOR.
 */
PDC_LIST_ITERATOR *pdc_list_iterator_new(PDC_LIST *list);

/**
 * Destroys the given PDC_LIST_ITERATOR and frees all allocated memory.
 * @param iterator The PDC_LIST_ITERATOR to destroy.
 */
void pdc_list_iterator_destroy(PDC_LIST_ITERATOR *iterator);

/**
 * Returns the next item in the PDC_LIST_ITERATOR.
 * @param iterator The PDC_LIST_ITERATOR to get the next item from.
 * @return A pointer to the next item in the PDC_LIST_ITERATOR.
 */
void *pdc_list_iterator_next(PDC_LIST_ITERATOR *iterator);

/**
 * Returns true if the PDC_LIST_ITERATOR has more items.
 * @param iterator The PDC_LIST_ITERATOR to check.
 * @return True if the PDC_LIST_ITERATOR has more items.
 */
int pdc_list_iterator_has_next(PDC_LIST_ITERATOR *iterator);

#endif // PDC_LIST_H