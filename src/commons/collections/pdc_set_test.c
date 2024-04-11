#include "pdc_set.h"
#include "pdc_compare.h"
#include "pdc_hash.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

void
set_value_free(SetValue value)
{
    free((void *)value);
}

int
main(int argc, char **argv)
{
    // read the max id from the command line
    char *   endptr;
    uint64_t max_id = strtoull(argv[1], &endptr, 10);
    if (*endptr != '\0') {
        fprintf(stderr, "Invalid number: %s\n", argv[1]);
        return 1;
    }

    Set *set = set_new(ui64_hash, ui64_equal);
    set_register_free_function(set, set_value_free);

    for (uint64_t i = 0; i < max_id; i++) {
        uint64_t *value = malloc(sizeof(uint64_t));
        *value          = i;
        set_insert(set, value);
    }

    // test if the size of set is correct

    if (set_num_entries(set) != (unsigned int)max_id) {
        printf("Error: set size is not correct\n");
        return 1;
    }

    // retrieve all values from the set
    for (uint64_t i = 0; i < max_id; i++) {
        uint64_t *value = malloc(sizeof(uint64_t));
        *value          = i;
        if (!set_query(set, value)) {
            printf("Error: value %" PRIu64 " not found in the set\n", i);
            return 1;
        }
    }

    // iterate through all values
    SetIterator *it;
    set_iterate(set, it);
    while (set_iter_has_more(it)) {
        uint64_t *value = set_iter_next(it);
        if (!set_query(set, value)) {
            printf("Error: value %" PRIu64 " not found in the set\n", *value);
            return 1;
        }
    }

    set_free(set);
    return 0;
}