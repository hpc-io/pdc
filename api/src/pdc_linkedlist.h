// copied from https://github.com/mercury-hpc/mercury/blob/master/src/util/mercury_list.h

#ifndef _pdc_linkedlist_H
#define _pdc_linkedlist_H

#include "pdc_cont_pkg.h"

#define PDC_LIST_HEAD_INITIALIZER(name)  { NULL }

#define PDC_LIST_HEAD_INIT(struct_head_name, var_name)   \
    struct struct_head_name var_name = PDC_LIST_HEAD_INITIALIZER(var_name)

#define PDC_LIST_HEAD_DECL(struct_head_name, struct_entry_name)  \
    struct struct_head_name {                                   \
        struct struct_entry_name *head;                         \
    }

#define PDC_LIST_HEAD(struct_entry_name)     \
    struct {                                \
        struct struct_entry_name *head;     \
    }

#define PDC_LIST_ENTRY(struct_entry_name)    \
    struct {                                \
        struct struct_entry_name *next;     \
        struct struct_entry_name **prev;    \
    }

#define PDC_LIST_INIT(head_ptr) do {         \
    (head_ptr)->head = NULL;                \
} while (/*CONSTCOND*/0)

#define PDC_LIST_IS_EMPTY(head_ptr)          \
    ((head_ptr)->head == NULL)

#define PDC_LIST_FIRST(head_ptr)             \
    ((head_ptr)->head)

#define PDC_LIST_GET_FIRST(var, head_ptr)             \
    (var = (head_ptr)->head)

#define PDC_LIST_NEXT(entry_ptr, entry_field_name)   \
    ((entry_ptr)->entry_field_name.next)

#define PDC_LIST_TO_NEXT(entry_ptr, entry_field_name)   \
    ((entry_ptr) = (entry_ptr)->entry_field_name.next)

#define PDC_LIST_INSERT_HEAD(head_ptr, entry_ptr, entry_field_name) do {     \
    if (((entry_ptr)->entry_field_name.next = (head_ptr)->head) != NULL)    \
        (head_ptr)->head->entry_field_name.prev =                           \
            &(entry_ptr)->entry_field_name.next;                            \
    (head_ptr)->head = (entry_ptr);                                         \
    (entry_ptr)->entry_field_name.prev = &(head_ptr)->head;                 \
} while (/*CONSTCOND*/0)

/* TODO would be nice to not have any condition */
#define PDC_LIST_REMOVE(entry_ptr, entry_field_name) do {                      \
    if ((entry_ptr)->entry_field_name.next != NULL)                           \
        (entry_ptr)->entry_field_name.next->entry_field_name.prev =           \
            (entry_ptr)->entry_field_name.prev;                               \
    *(entry_ptr)->entry_field_name.prev = (entry_ptr)->entry_field_name.next; \
} while (/*CONSTCOND*/0)

#define PDC_LIST_FOREACH(var, head_ptr, entry_field_name)    \
    for ((var) = ((head_ptr)->head);                        \
        (var);                                              \
        (var) = ((var)->entry_field_name.next))

#define PDC_LIST_SEARCH(var, head_ptr, entry_field_name, item, value)  \
    for ((var) = ((head_ptr)->head);                                  \
        (((var)->item != value) && (var));                             \
        (var) = ((var)->entry_field_name.next))

#define PDC_LIST_SEARCH_CONT_NAME(var, head_ptr, entry_field_name, member, n, name)  \
    for ((var) = ((head_ptr)->head);                                  \
        (strcmp(((struct PDC_cont_info *)((var)->member))->n, name) != 0 && (var));                             \
        (var) = ((var)->entry_field_name.next))

#endif
