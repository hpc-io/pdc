/*

Copyright (c) 2005-2008, Simon Howard

Permission to use, copy, modify, and/or distribute this software
for any purpose with or without fee is hereby granted, provided
that the above copyright notice and this permission notice appear
in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

 */

#include <stdlib.h>
#include <string.h>
#include "pdc_set.h"
#include "pdc_malloc.h"
#include "pdc_timing.h"

/* malloc() / free() testing */

#ifdef ALLOC_TESTING
#include "alloc-testing.h"
#endif

/* A set */

struct _SetEntry {
    SetValue  data;
    SetEntry *next;
};

struct _Set {
    SetEntry **  table;
    unsigned int entries;
    unsigned int table_size;
    unsigned int prime_index;
    SetHashFunc  hash_func;
    SetEqualFunc equal_func;
    SetFreeFunc  free_func;
};

/* This is a set of good hash table prime numbers, from:
 *   http://planetmath.org/encyclopedia/GoodHashTablePrimes.html
 * Each prime is roughly double the previous value, and as far as
 * possible from the nearest powers of two. */

static const unsigned int set_primes[] = {
    193,      389,      769,      1543,      3079,      6151,      12289,     24593,
    49157,    98317,    196613,   393241,    786433,    1572869,   3145739,   6291469,
    12582917, 25165843, 50331653, 100663319, 201326611, 402653189, 805306457, 1610612741,
};

static const unsigned int set_num_primes = sizeof(set_primes) / sizeof(int);

static int
set_allocate_table(Set *set)
{
    FUNC_ENTER(NULL);

    /* Determine the table size based on the current prime index.
     * An attempt is made here to ensure sensible behavior if the
     * maximum prime is exceeded, but in practice other things are
     * likely to break long before that happens. */

    if (set->prime_index < set_num_primes) {
        set->table_size = set_primes[set->prime_index];
    }
    else {
        set->table_size = set->entries * 10;
    }

    /* Allocate the table and initialise to NULL */

    set->table = PDC_calloc(set->table_size, sizeof(SetEntry *));

    FUNC_LEAVE(set->table != NULL);
}

static void
set_free_entry(Set *set, SetEntry *entry)
{
    FUNC_ENTER(NULL);

    /* If there is a free function registered, call it to free the
     * data for this entry first */

    if (set->free_func != NULL) {
        set->free_func(entry->data);
    }

    /* Free the entry structure */

    entry = (SetEntry *)PDC_free(entry);

    FUNC_LEAVE_VOID();
}

Set *
set_new(SetHashFunc hash_func, SetEqualFunc equal_func)
{
    FUNC_ENTER(NULL);

    Set *new_set;

    /* Allocate a new set and fill in the fields */

    new_set = (Set *)PDC_malloc(sizeof(Set));

    if (new_set == NULL) {
        FUNC_LEAVE(NULL);
    }

    new_set->hash_func   = hash_func;
    new_set->equal_func  = equal_func;
    new_set->entries     = 0;
    new_set->prime_index = 0;
    new_set->free_func   = NULL;

    /* Allocate the table */

    if (!set_allocate_table(new_set)) {
        new_set = (Set *)PDC_free(new_set);
        FUNC_LEAVE(NULL);
    }

    FUNC_LEAVE(new_set);
}

void
set_free(Set *set)
{
    FUNC_ENTER(NULL);

    SetEntry *   rover;
    SetEntry *   next;
    unsigned int i;

    /* Free all entries in all chains */

    for (i = 0; i < set->table_size; ++i) {
        rover = set->table[i];

        while (rover != NULL) {
            next = rover->next;

            /* Free this entry */

            set_free_entry(set, rover);

            /* Advance to the next entry in the chain */

            rover = next;
        }
    }

    /* Free the table */
    set->table = (SetEntry **)PDC_free(set->table);

    /* Free the set structure */
    set = (Set *)PDC_free(set);

    FUNC_LEAVE_VOID();
}

void
set_register_free_function(Set *set, SetFreeFunc free_func)
{
    FUNC_ENTER(NULL);

    set->free_func = free_func;

    FUNC_LEAVE_VOID();
}

static int
set_enlarge(Set *set)
{
    FUNC_ENTER(NULL);

    SetEntry *   rover;
    SetEntry *   next;
    SetEntry **  old_table;
    unsigned int old_table_size;
    unsigned int old_prime_index;
    unsigned int index;
    unsigned int i;

    /* Store the old table */

    old_table       = set->table;
    old_table_size  = set->table_size;
    old_prime_index = set->prime_index;

    /* Use the next table size from the prime number array */

    ++set->prime_index;

    /* Allocate the new table */

    if (!set_allocate_table(set)) {
        set->table       = old_table;
        set->table_size  = old_table_size;
        set->prime_index = old_prime_index;

        FUNC_LEAVE(0);
    }

    /* Iterate through all entries in the old table and add them
     * to the new one */

    for (i = 0; i < old_table_size; ++i) {

        /* Walk along this chain */

        rover = old_table[i];

        while (rover != NULL) {

            next = rover->next;

            /* Hook this entry into the new table */

            index             = set->hash_func(rover->data) % set->table_size;
            rover->next       = set->table[index];
            set->table[index] = rover;

            /* Advance to the next entry in the chain */

            rover = next;
        }
    }

    /* Free back the old table */
    old_table = (SetEntry **)PDC_free(old_table);

    /* Resized successfully */
    FUNC_LEAVE(1);
}

int
set_insert(Set *set, SetValue data)
{
    FUNC_ENTER(NULL);

    SetEntry *   newentry;
    SetEntry *   rover;
    unsigned int index;

    /* The hash table becomes less efficient as the number of entries
     * increases. Check if the percentage used becomes large. */

    if ((set->entries * 3) / set->table_size > 0) {

        /* The table is more than 1/3 full and must be increased
         * in size */

        if (!set_enlarge(set)) {
            FUNC_LEAVE(0);
        }
    }

    /* Use the hash of the data to determine an index to insert into the
     * table at. */

    index = set->hash_func(data) % set->table_size;

    /* Walk along this chain and attempt to determine if this data has
     * already been added to the table */

    rover = set->table[index];

    while (rover != NULL) {

        if (set->equal_func(data, rover->data) != 0) {

            /* This data is already in the set */

            FUNC_LEAVE(0);
        }

        rover = rover->next;
    }

    /* Not in the set.  We must add a new entry. */

    /* Make a new entry for this data */

    newentry = (SetEntry *)PDC_malloc(sizeof(SetEntry));

    if (newentry == NULL) {
        FUNC_LEAVE(0);
    }

    newentry->data = data;

    /* Link into chain */

    newentry->next    = set->table[index];
    set->table[index] = newentry;

    /* Keep track of the number of entries in the set */

    ++set->entries;

    /* Added successfully */

    FUNC_LEAVE(1);
}

int
set_remove(Set *set, SetValue data)
{
    FUNC_ENTER(NULL);

    SetEntry **  rover;
    SetEntry *   entry;
    unsigned int index;

    /* Look up the data by its hash key */

    index = set->hash_func(data) % set->table_size;

    /* Search this chain, until the corresponding entry is found */

    rover = &set->table[index];

    while (*rover != NULL) {
        if (set->equal_func(data, (*rover)->data) != 0) {

            /* Found the entry */

            entry = *rover;

            /* Unlink from the linked list */

            *rover = entry->next;

            /* Update counter */

            --set->entries;

            /* Free the entry and return */

            set_free_entry(set, entry);

            FUNC_LEAVE(1);
        }

        /* Advance to the next entry */

        rover = &((*rover)->next);
    }

    /* Not found in set */

    FUNC_LEAVE(0);
}

int
set_query(Set *set, SetValue data)
{
    FUNC_ENTER(NULL);

    SetEntry *   rover;
    unsigned int index;

    /* Look up the data by its hash key */

    index = set->hash_func(data) % set->table_size;

    /* Search this chain, until the corresponding entry is found */

    rover = set->table[index];

    while (rover != NULL) {
        if (set->equal_func(data, rover->data) != 0) {

            /* Found the entry */

            FUNC_LEAVE(1);
        }

        /* Advance to the next entry in the chain */

        rover = rover->next;
    }

    /* Not found */

    FUNC_LEAVE(0);
}

unsigned int
set_num_entries(Set *set)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(set->entries);
}

int set_copy(Set *target, Set* source){
    SetIterator iterator;
    SetValue    value;
    set_iterate(source, &iterator);

    while (set_iter_has_more(&iterator)) {

        /* Read the next value */

        value = set_iter_next(&iterator);
        /* Has this value been put into the new set already?
         * If so, do not insert this again */

        if (set_query(target, value) == 0) {
            if (!set_insert(target, value)) {
                /* Failed to insert */
                set_free(target);
                return -1;
            }
        }
    }
    return 0;
}

SetValue *
set_to_array(Set *set)
{
    FUNC_ENTER(NULL);

    SetValue *   array;
    int          array_counter;
    unsigned int i;
    SetEntry *   rover;

    /* Create an array to hold the set entries */

    array = PDC_malloc(sizeof(SetValue) * set->entries);

    if (array == NULL) {
        FUNC_LEAVE(NULL);
    }

    array_counter = 0;

    /* Iterate over all entries in all chains */

    for (i = 0; i < set->table_size; ++i) {

        rover = set->table[i];

        while (rover != NULL) {

            /* Add this value to the array */

            array[array_counter] = rover->data;
            ++array_counter;

            /* Advance to the next entry */

            rover = rover->next;
        }
    }

    FUNC_LEAVE(array);
}

Set *
set_union(Set *set1, Set *set2)
{
    FUNC_ENTER(NULL);

    SetIterator iterator;
    Set *       new_set;
    SetValue    value;

    new_set = set_new(set1->hash_func, set1->equal_func);

    if (new_set == NULL) {
        FUNC_LEAVE(NULL);
    }

    /* Add all values from the first set */

    set_iterate(set1, &iterator);

    while (set_iter_has_more(&iterator)) {

        /* Read the next value */

        value = set_iter_next(&iterator);

        /* Copy the value into the new set */

        if (!set_insert(new_set, value)) {

            /* Failed to insert */

            set_free(new_set);
            FUNC_LEAVE(NULL);
        }
    }

    /* Add all values from the second set */

    set_iterate(set2, &iterator);

    while (set_iter_has_more(&iterator)) {

        /* Read the next value */

        value = set_iter_next(&iterator);

        /* Has this value been put into the new set already?
         * If so, do not insert this again */

        if (set_query(new_set, value) == 0) {
            if (!set_insert(new_set, value)) {

                /* Failed to insert */

                set_free(new_set);
                FUNC_LEAVE(NULL);
            }
        }
    }

    FUNC_LEAVE(new_set);
}

Set *
set_intersection(Set *set1, Set *set2)
{
    FUNC_ENTER(NULL);

    Set *       new_set;
    SetIterator iterator;
    SetValue    value;

    new_set = set_new(set1->hash_func, set2->equal_func);

    if (new_set == NULL) {
        FUNC_LEAVE(NULL);
    }

    /* Iterate over all values in set 1. */

    set_iterate(set1, &iterator);

    while (set_iter_has_more(&iterator)) {

        /* Get the next value */

        value = set_iter_next(&iterator);

        /* Is this value in set 2 as well?  If so, it should be
         * in the new set. */

        if (set_query(set2, value) != 0) {

            /* Copy the value first before inserting,
             * if necessary */

            if (!set_insert(new_set, value)) {
                set_free(new_set);

                FUNC_LEAVE(NULL);
            }
        }
    }

    FUNC_LEAVE(new_set);
}

void
set_iterate(Set *set, SetIterator *iter)
{
    FUNC_ENTER(NULL);

    unsigned int chain;

    iter->set        = set;
    iter->next_entry = NULL;

    /* Find the first entry */

    for (chain = 0; chain < set->table_size; ++chain) {

        /* There is a value at the start of this chain */

        if (set->table[chain] != NULL) {
            iter->next_entry = set->table[chain];
            break;
        }
    }

    iter->next_chain = chain;

    FUNC_LEAVE_VOID();
}

SetValue
set_iter_next(SetIterator *iterator)
{
    FUNC_ENTER(NULL);

    Set *        set;
    SetValue     result;
    SetEntry *   current_entry;
    unsigned int chain;

    set = iterator->set;

    /* No more entries? */

    if (iterator->next_entry == NULL) {
        FUNC_LEAVE(SET_NULL);
    }

    /* We have the result immediately */

    current_entry = iterator->next_entry;
    result        = current_entry->data;

    /* Advance next_entry to the next SetEntry in the Set. */

    if (current_entry->next != NULL) {

        /* Use the next value in this chain */

        iterator->next_entry = current_entry->next;
    }
    else {

        /* Default value if no valid chain is found */

        iterator->next_entry = NULL;

        /* No more entries in this chain.  Search the next chain */

        chain = iterator->next_chain + 1;

        while (chain < set->table_size) {

            /* Is there a chain at this table entry? */

            if (set->table[chain] != NULL) {

                /* Valid chain found! */

                iterator->next_entry = set->table[chain];

                break;
            }

            /* Keep searching until we find an empty chain */

            ++chain;
        }

        iterator->next_chain = chain;
    }

    FUNC_LEAVE(result);
}

int
set_iter_has_more(SetIterator *iterator)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(iterator->next_entry != NULL);
}
