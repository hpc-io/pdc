/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#include <stdio.h>
#include <stdlib.h>

#include "mercury_hash_table.h"

static int
int_equal(hg_hash_table_key_t vlocation1, hg_hash_table_key_t vlocation2)
{
    return *((int *)vlocation1) == *((int *)vlocation2);
}

static unsigned int
int_hash(hg_hash_table_key_t vlocation)
{
    return *((unsigned int *)vlocation);
}

static void
int_hash_key_free(hg_hash_table_key_t key)
{
    free((int *)key);
}

static void
int_hash_value_free(hg_hash_table_value_t value)
{
    free((int *)value);
}

// ADDED
#define MAX_PATH 128
typedef struct hash_value_metadata_t {
    int   obj_id;
    char  obj_name[MAX_PATH];
    char  obj_data_location[MAX_PATH];
    char *test;

} hash_value_metadata_t;

static void
metadata_hash_value_free(hg_hash_table_value_t value)
{
    free((hash_value_metadata_t *)value);
}

/*---------------------------------------------------------------------------*/

int
main(int argc, char *argv[])
{
    hg_hash_table_t *hash_table = NULL;

    int *                  key1, *key2;
    hash_value_metadata_t *value1, *value2;
    const int              num = 100;
    int *                  keys[num];
    int                    i;
    hash_value_metadata_t *values[num];
    hash_value_metadata_t *lookup_value = NULL;
    int                    lookup_key;

    /* int *value1, *value2; */
    int ret = EXIT_SUCCESS;

    (void)argc;
    (void)argv;

    hash_table = hg_hash_table_new(int_hash, int_equal);
    hg_hash_table_register_free_functions(hash_table, int_hash_key_free, int_hash_value_free);

    key1 = (int *)malloc(sizeof(int));
    key2 = (int *)malloc(sizeof(int));

    /* value1 = (int *) malloc(sizeof(int)); */
    /* value2 = (int *) malloc(sizeof(int)); */

    value1 = (hash_value_metadata_t *)malloc(sizeof(hash_value_metadata_t));
    value2 = (hash_value_metadata_t *)malloc(sizeof(hash_value_metadata_t));

    *key1 = 422983929;
    *key2 = 1001;

    value1->obj_id = 10;
    value2->obj_id = 20;

    for (i = 0; i < num; i++) {
        /* printf("Iter[%d]\n", i); */
        keys[i]           = (int *)malloc(sizeof(int));
        *keys[i]          = 1000 + i;
        values[i]         = (hash_value_metadata_t *)malloc(sizeof(hash_value_metadata_t));
        values[i]->obj_id = *keys[i];
        values[i]->test   = "Hello";

        sprintf(values[i]->obj_name, "Obj_%d", *keys[i]);

        hg_hash_table_insert(hash_table, keys[i], values[i]);
    }

    printf("Entries: %d\n", hg_hash_table_num_entries(hash_table));

    lookup_key   = *key2;
    lookup_value = hg_hash_table_lookup(hash_table, &lookup_key);
    if (lookup_value != NULL) {
        printf("Found in hash table with obj_id=%d, obj_name=%s, test=%s\n", lookup_value->obj_id,
               lookup_value->obj_name, lookup_value->test);
        sprintf(lookup_value->obj_name, "%s", "SUCCESSFULLY CHANGED HASH VALUE");
        lookup_value->test = "World";
    }
    else {
        printf("Object with lookup_key=%d not found!\n", lookup_key);
    }

    printf("New value after change!\n");
    lookup_value = hg_hash_table_lookup(hash_table, &lookup_key);
    if (lookup_value != NULL) {
        printf("Found in hash table with obj_id=%d, obj_name=%s, test=%s\n", lookup_value->obj_id,
               lookup_value->obj_name, lookup_value->test);
    }
    else {
        printf("Object with lookup_key=%d not found!\n", lookup_key);
    }

    /* if (*value1 != *((int *)hg_hash_table_lookup(hash_table, key1))) { */
    /*     fprintf(stderr, "Error: values do not match\n"); */
    /*     ret = EXIT_FAILURE; */
    /*     goto done; */
    /* } */
    /* hg_hash_table_remove(hash_table, key1); */

    /* if (1 != hg_hash_table_num_entries(hash_table)) { */
    /*     fprintf(stderr, "Error: was expecting 1 entry, got %u\n", */
    /*             hg_hash_table_num_entries(hash_table)); */
    /*     ret = EXIT_FAILURE; */
    /*     goto done; */
    /* } */

    /* hg_hash_table_iterate(hash_table, &hash_table_iter); */
    /* if (!hg_hash_table_iter_has_more(&hash_table_iter)) { */
    /*     fprintf(stderr, "Error: there should be more values\n"); */
    /*     ret = EXIT_FAILURE; */
    /*     goto done; */
    /* } */
    /* if (*value2 != *((int *)hg_hash_table_iter_next(&hash_table_iter))) { */
    /*     fprintf(stderr, "Error: values do not match\n"); */
    /*     ret = EXIT_FAILURE; */
    /*     goto done; */
    /* } */

    hg_hash_table_free(hash_table);
    return ret;
}
