#ifndef PDC_DLLIST_H
#define PDC_DLLIST_H

#include <stdbool.h>
#include "pdc_utlist.h"
#include "pdc_set.h"

typedef unsigned int (*DoublyLinkedListHashFunc)(void *key);
/* A comparator returns zero for equal keys, like strcmp. */
typedef int (*DoublyLinkedListEqualFunc)(void *left, void *right);

typedef struct DoublyLinkedListItem {
    void *                       key;
    Set *                        value;
    struct DoublyLinkedListItem *prev;
    struct DoublyLinkedListItem *next;
} DoublyLinkedListItem;

typedef struct DoublyLinkedList {
    DoublyLinkedListItem *    head;
    DoublyLinkedListHashFunc  hash_func;
    DoublyLinkedListEqualFunc equal_func;
    unsigned int              count;
} DoublyLinkedList;

DoublyLinkedList *dllist_init(DoublyLinkedListHashFunc hash_func, DoublyLinkedListEqualFunc equal_func);
/* On success the list owns the allocated key and value set. */
int  dllist_insert(DoublyLinkedList *list, void *key, Set *value);
Set *dllist_search_key(DoublyLinkedList *list, void *key);
/* Returns a new set of borrowed values, or NULL on allocation failure. */
Set *dllist_search_range(DoublyLinkedList *list, void *start_key, bool include_start, void *end_key,
                         bool include_end);
int  dllist_destroy(void *list);

#endif /* PDC_DLLIST_H */
