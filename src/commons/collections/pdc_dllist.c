#include "pdc_dllist.h"
#include <stdlib.h>

DoublyLinkedList *
dllist_init(DoublyLinkedListHashFunc hash_func, DoublyLinkedListEqualFunc equal_func)
{
    DoublyLinkedList *list = calloc(1, sizeof(*list));
    if (list) {
        list->hash_func  = hash_func;
        list->equal_func = equal_func;
    }
    return list;
}

int
dllist_insert(DoublyLinkedList *list, void *key, Set *value)
{
    if (!list || !key || !value)
        return -1;
    DoublyLinkedListItem *item = malloc(sizeof(*item));
    if (!item)
        return -1;
    item->key   = key;
    item->value = value;
    DL_APPEND(list->head, item);
    list->count++;
    return 0;
}

Set *
dllist_search_key(DoublyLinkedList *list, void *key)
{
    DoublyLinkedListItem *elt;
    if (!list || !key || !list->equal_func)
        return NULL;
    DL_FOREACH(list->head, elt)
    {
        if (list->equal_func(elt->key, key) == 0)
            return elt->value;
    }
    return NULL;
}

Set *
dllist_search_range(DoublyLinkedList *list, void *start_key, bool include_start, void *end_key,
                    bool include_end)
{
    DoublyLinkedListItem *elt;
    Set *                 result = NULL;
    if (!list || !start_key || !end_key || !list->equal_func)
        return NULL;
    DL_FOREACH(list->head, elt)
    {
        int start = list->equal_func(elt->key, start_key);
        int end   = list->equal_func(elt->key, end_key);
        if ((start > 0 || (include_start && start == 0)) && (end < 0 || (include_end && end == 0))) {
            if (!result)
                result = set_union(elt->value, elt->value);
            else if (set_copy(result, elt->value) != 0) {
                set_free(result);
                return NULL;
            }
            if (!result)
                return NULL;
        }
    }
    return result ? result : set_new(list->hash_func, list->equal_func);
}

int
dllist_destroy(void *ptr)
{
    DoublyLinkedList *    list = ptr;
    DoublyLinkedListItem *elt, *tmp;
    if (!list)
        return 0;
    DL_FOREACH_SAFE(list->head, elt, tmp)
    {
        DL_DELETE(list->head, elt);
        set_free(elt->value);
        free(elt->key);
        free(elt);
    }
    free(list);
    return 0;
}
