#include "pdc_list.h"

PDC_LIST *
pdc_list_new()
{
    return pdc_list_create(100, 2.0);
}

PDC_LIST *
pdc_list_create(size_t initial_capacity, double expansion_factor)
{
    // Allocate memory for the list struct.
    PDC_LIST *list = (PDC_LIST *)malloc(sizeof(PDC_LIST));
    if (list == NULL) {
        return NULL;
    }

    // Allocate memory for the array of items.
    list->items = (void **)malloc(initial_capacity * sizeof(void *));
    if (list->items == NULL) {
        free(list);
        return NULL;
    }

    // Initialize the list fields.
    list->item_count       = 0;
    list->capacity         = initial_capacity;
    list->expansion_factor = expansion_factor;

    return list;
}

void
pdc_list_destroy(PDC_LIST *list)
{
    if (list == NULL) {
        return;
    }

    // Free all allocated memory for each item.
    for (size_t i = 0; i < list->item_count; i++) {
        free(list->items[i]);
    }

    // Free the array of items and the list struct.
    free(list->items);
    free(list);
}

void
pdc_list_add(PDC_LIST *list, void *item)
{
    if (list == NULL || item == NULL) {
        return;
    }

    // Expand the array of items if necessary.
    if (list->item_count >= list->capacity) {
        list->capacity *= list->expansion_factor;
        list->items = (void **)realloc(list->items, list->capacity * sizeof(void *));
        if (list->items == NULL) {
            return;
        }
    }

    // Add the new item to the end of the array.
    list->items[list->item_count++] = item;
}

void *
pdc_list_get(PDC_LIST *list, size_t index)
{
    if (list == NULL || index >= list->item_count) {
        return NULL;
    }

    // Return a pointer to the item at the given index.
    return list->items[index];
}

size_t
pdc_list_size(PDC_LIST *list)
{
    if (list == NULL) {
        return 0;
    }

    // Return the number of items in the list.
    return list->item_count;
}

void
pdc_list_set_expansion_factor(PDC_LIST *list, double expansion_factor)
{
    if (list == NULL) {
        return;
    }

    // Set the new expansion factor for the list.
    list->expansion_factor = expansion_factor;
}

double
pdc_list_get_expansion_factor(PDC_LIST *list)
{
    if (list == NULL) {
        return 0;
    }

    // Return the current expansion factor for the list.
    return list->expansion_factor;
}

PDC_LIST_ITERATOR *
pdc_list_iterator_new(PDC_LIST *list)
{
    if (list == NULL) {
        return NULL;
    }

    // Allocate memory for the iterator struct.
    PDC_LIST_ITERATOR *iterator = (PDC_LIST_ITERATOR *)malloc(sizeof(PDC_LIST_ITERATOR));
    if (iterator == NULL) {
        return NULL;
    }

    // Initialize the iterator fields.
    iterator->list  = list;
    iterator->index = 0;

    return iterator;
}

void
pdc_list_iterator_destroy(PDC_LIST_ITERATOR *iterator)
{
    if (iterator == NULL) {
        return;
    }

    // Free the iterator struct.
    free(iterator);
}

void *
pdc_list_iterator_next(PDC_LIST_ITERATOR *iterator)
{
    if (iterator == NULL) {
        return NULL;
    }

    // Return the next item in the list.
    return pdc_list_get(iterator->list, iterator->index++);
}

int
pdc_list_iterator_has_next(PDC_LIST_ITERATOR *iterator)
{
    if (iterator == NULL) {
        return 0;
    }

    // Return true if there are more items in the list.
    return iterator->index < pdc_list_size(iterator->list);
}