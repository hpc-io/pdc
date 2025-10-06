#include "pdc_dllist.h"
#include <stdlib.h>

// Now define the actual structures
struct DoublyLinkedListItem {
    void * key;
    void * value;
    struct DoublyLinkedListItem *prev;  // Use 'struct' keyword
    struct DoublyLinkedListItem *next;  // Use 'struct' keyword
};

struct DoublyLinkedList {
    DoublyLinkedListItem * head;
    DoublyLinkedListHashFunc hash_func;
    DoublyLinkedListEqualFunc equal_func;
    unsigned int count;
};
DoublyLinkedList *dllist_init(DoublyLinkedListHashFunc hash_func, DoublyLinkedListEqualFunc equal_func){
    DoublyLinkedList *list = malloc(sizeof(DoublyLinkedList));
    if (!list) {
        return NULL; // Memory allocation failed
    }
    list->hash_func = hash_func;
    list->equal_func = equal_func;
    list->head = NULL;
    list->count = 0;
    return list;
}

int dllist_insert(DoublyLinkedList *list, void *key, void *value){
    DoublyLinkedListItem *item = malloc(sizeof(DoublyLinkedListItem));
    item->key = key;
    item->value = value;
    DL_APPEND(list->head, item);
    list->count++;
    return 0;
}

int *dllist_search_key(DoublyLinkedList *list, void *key){
    DoublyLinkedListItem *elt = NULL;
    DoublyLinkedListEqualFunc key_cmp_func = list->equal_func;
    DL_FOREACH(list->head, elt) {
        if(key_cmp_func(elt->key, key) == 0) break;
    }
    return elt != NULL && key_cmp_func(elt->key, key) == 0 ? elt->value : NULL;
}

int *dllist_search_range(DoublyLinkedList * list, void *start_key, bool include_start, void *end_key, bool include_end){
    DoublyLinkedListItem *elt = NULL;
    Set *value_set = set_new(NULL, NULL);
    DoublyLinkedListEqualFunc key_cmp_func = list->equal_func;
    DL_FOREACH(list->head, elt) {
        int start_dist = key_cmp_func(elt->key, start_key);
        int end_dist = key_cmp_func(elt->key, end_key);
        if(start_dist >= 0 && end_dist <= 0){
            set_copy(value_set, elt->value);
        }
    }
    return value_set;
}

int dllist_destroy(void *ptr){
    DoublyLinkedList *list = (DoublyLinkedList*)ptr;
    DoublyLinkedListItem *elt = NULL;
    DoublyLinkedListItem *tmp = NULL;
    DL_FOREACH_SAFE(list->head, elt, tmp) {
        DL_DELETE(list->head, elt);
        free(elt->value);
        free(elt->key);
        free(elt);
    }
}