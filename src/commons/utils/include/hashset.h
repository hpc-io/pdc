// From : 

#ifndef HASHSET_H
#define HASHSET_H 1

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

    struct hashset_st {
        size_t nbits;
        size_t mask;

        size_t capacity;
        size_t *items;
        size_t nitems;
        size_t n_deleted_items;
    };

    typedef struct hashset_st *hashset_t;

    /* create hashset instance */
    hashset_t hashset_create(void);

    /* destroy hashset instance */
    void hashset_destroy(hashset_t set);

    size_t hashset_num_items(hashset_t set);

    /* add item into the hashset.
     *
     * @note 0 and 1 is special values, meaning nil and deleted items. the
     *       function will return -1 indicating error.
     *
     * returns zero if the item already in the set and non-zero otherwise
     */
    int hashset_add(hashset_t set, void *item);

    /* remove item from the hashset
     *
     * returns non-zero if the item was removed and zero if the item wasn't
     * exist
     */
    int hashset_remove(hashset_t set, void *item);

    /* check if existence of the item
     *
     * returns non-zero if the item exists and zero otherwise
     */
    int hashset_is_member(hashset_t set, void *item);


    struct hashset_itr_st {
      hashset_t set;
      size_t index;
    };

    typedef struct hashset_itr_st *hashset_itr_t;

    /* create a hashset iterator, advances to first value is available*/
    hashset_itr_t hashset_iterator(hashset_t set);

    /* returns the value at the current index */
    size_t hashset_iterator_value(hashset_itr_t itr);

    /* return 1 is can advance, 0 otherwise */
    int hashset_iterator_has_next(hashset_itr_t itr);

    /*
     * check if iterator can advance, if so advances
     * returns current index if can advance and -1 otherwise
     */
    int hashset_iterator_next(hashset_itr_t itr);

#ifdef __cplusplus
}
#endif

#endif
