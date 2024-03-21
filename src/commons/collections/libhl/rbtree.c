#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include "rbtree.h"
#include "pdc_malloc.h"

#define IS_BLACK(_n) (!(_n) || (_n)->color == RBTREE_COLOR_BLACK)
#define IS_RED(_n)   ((_n) && (_n)->color == RBTREE_COLOR_RED)

#define PAINT_BLACK(_n)                                                                                      \
    {                                                                                                        \
        if (_n)                                                                                              \
            (_n)->color = RBTREE_COLOR_BLACK;                                                                \
    }
#define PAINT_RED(_n)                                                                                        \
    {                                                                                                        \
        if (_n)                                                                                              \
            (_n)->color = RBTREE_COLOR_RED;                                                                  \
    }

size_t mem_usage_by_all_rbtrees;

typedef enum {
    RBTREE_COLOR_RED = 0,
    RBTREE_COLOR_BLACK,
} rbt_color_t;

typedef struct _rbt_node_s {
    rbt_color_t         color;
    void *              key;
    size_t              klen;
    void *              value;
    struct _rbt_node_s *left;
    struct _rbt_node_s *right;
    struct _rbt_node_s *parent;
} rbt_node_t;

struct _rbt_s {
    rbt_node_t *              root;
    uint64_t                  size;
    libhl_cmp_callback_t      cmp_keys_cb;
    rbt_free_value_callback_t free_value_cb;
    double                    time_for_rotate;
};

rbt_t *
rbt_create(libhl_cmp_callback_t cmp_keys_cb, rbt_free_value_callback_t free_value_cb)
{
    rbt_t *rbt = calloc(1, sizeof(rbt_t));
    // INIT_PERF_INFO_FIELDS(rbt, rbt_t);
    // rbt->time_for_rotate = 0;
    // mem_usage_by_all_rbtrees += sizeof(rbt_t);

    if (!rbt)
        return NULL;
    rbt->free_value_cb = free_value_cb;
    rbt->cmp_keys_cb   = cmp_keys_cb;
    return rbt;
}

static inline void
rbt_destroy_internal(rbt_node_t *node, rbt_free_value_callback_t free_value_cb)
{
    if (!node)
        return;

    rbt_destroy_internal(node->left, free_value_cb);
    rbt_destroy_internal(node->right, free_value_cb);

    free(node->key);
    if (free_value_cb)
        free_value_cb(node->value);

    free(node);
}

void
rbt_destroy(rbt_t *rbt)
{
    if (rbt == NULL) {
        return;
    }
    rbt_destroy_internal(rbt->root, rbt->free_value_cb);
    free(rbt);
}

static int
rbt_range_walk_internal(rbt_t *rbt, rbt_node_t *node, void *begin_key, size_t bgk_size, void *end_key,
                        size_t edk_size, int sorted, rbt_walk_callback cb, void *priv, int beginInclusive,
                        int endInclusive)
{
    if (!node)
        return 0;

    if (begin_key != NULL && end_key != NULL &&
        rbt->cmp_keys_cb(begin_key, bgk_size, end_key, edk_size) >= 0) {
        return 0;
    }

    int rc   = 1;
    int cbrc = 1;

    if (sorted && node->left) {
        int rrc = rbt_range_walk_internal(rbt, node->left, begin_key, bgk_size, end_key, edk_size, sorted, cb,
                                          priv, beginInclusive, endInclusive);
        if (rrc == 0)
            return rc + 1;
        rc += rrc;
    }

    int cmp_begin = begin_key != NULL ? rbt->cmp_keys_cb(node->key, node->klen, begin_key, bgk_size) : 1;
    int cmp_end   = end_key != NULL ? rbt->cmp_keys_cb(node->key, node->klen, end_key, edk_size) : -1;

    // current node = begin : no need for left child, collect
    // current node < begin : no need for left child
    // current node > begin : go for left child, if current node < end, collect
    // current node = end : no need for right child, do not collect
    // current node < end : go for the right child, if current node >= begin, collect
    // current node > end : no need for right child

    // Decide to go left or right based on comparisons and include flags
    int go_for_left  = cmp_begin > 0; // || (beginInclusive && cmp_begin == 0);
    int go_for_right = cmp_end < 0;   // || (endInclusive && cmp_end == 0);

    // If the current node is within the range, or matches begin/end based on include flags, call the callback
    if ((cmp_begin > 0 && cmp_end < 0) || (beginInclusive && cmp_begin == 0) ||
        (endInclusive && cmp_end == 0)) {
        cbrc = cb(rbt, node->key, node->klen, node->value, priv);
    }

    // int go_for_left  = 1;
    // int go_for_right = 1;
    // if (cmp_begin == 0 && beginInclusive) {
    //     cbrc         = cb(rbt, node->key, node->klen, node->value, priv);
    //     go_for_right = 1;
    // }
    // else if (cmp_begin > 0 && (cmp_end < 0 || (endInclusive && cmp_end == 0))) {
    //     cbrc         = cb(rbt, node->key, node->klen, node->value, priv);
    //     go_for_right = 1;
    // }
    // else if (cmp_begin < 0) {
    //     go_for_left = 1;
    // }

    // if (rbt->cmp_keys_cb(node->key, node->klen, begin_key, bgk_size) == 0) {
    //     cbrc         = cb(rbt, node->key, node->klen, node->value, priv);
    //     go_for_right = 1;
    // }
    // else if (rbt->cmp_keys_cb(node->key, node->klen, begin_key, bgk_size) > 0) {
    //     if (rbt->cmp_keys_cb(node->key, node->klen, end_key, edk_size) < 0) {
    //         cbrc         = cb(rbt, node->key, node->klen, node->value, priv);
    //         go_for_right = 1;
    //     }
    //     go_for_left = 1;
    // }

    switch (cbrc) {
        case RBT_WALK_DELETE_AND_STOP:
            rbt_remove(rbt, node->key, node->klen, NULL);
            return 0;
        case RBT_WALK_DELETE_AND_CONTINUE: {
            if (node->left && node->right) {
                rbt_remove(rbt, node->key, node->klen, NULL);
                return rbt_range_walk_internal(rbt, node, begin_key, bgk_size, end_key, edk_size, sorted, cb,
                                               priv, beginInclusive, endInclusive);
            }
            else if (node->left || node->right) {
                return rbt_range_walk_internal(rbt, node->left ? node->left : node->right, begin_key,
                                               bgk_size, end_key, edk_size, sorted, cb, priv, beginInclusive,
                                               endInclusive);
            }
            // this node was a leaf
            return 1;
        }
        case RBT_WALK_STOP:
            return 0;
        case RBT_WALK_CONTINUE:
            break;
        default:
            // TODO - Error Messages
            break;
    }

    if (!sorted && go_for_left && node->left) {
        int rrc = rbt_range_walk_internal(rbt, node->left, begin_key, bgk_size, end_key, edk_size, sorted, cb,
                                          priv, beginInclusive, endInclusive);
        if (rrc == 0)
            return rc + 1;
        rc += rrc;
    }

    if (go_for_right && node->right) {
        int rrc = rbt_range_walk_internal(rbt, node->right, begin_key, bgk_size, end_key, edk_size, sorted,
                                          cb, priv, beginInclusive, endInclusive);
        if (rrc == 0)
            return rc + 1;
        rc += rrc;
    }
    return rc;
}

static int
rbt_walk_internal(rbt_t *rbt, rbt_node_t *node, int sorted, rbt_walk_callback cb, void *priv)
{
    if (!node)
        return 0;

    int rc   = 1;
    int cbrc = 0;

    if (sorted && node->left) {
        int rrc = rbt_walk_internal(rbt, node->left, sorted, cb, priv);
        if (rrc == 0)
            return rc + 1;
        rc += rrc;
    }

    cbrc = cb(rbt, node->key, node->klen, node->value, priv);
    switch (cbrc) {
        case RBT_WALK_DELETE_AND_STOP:
            rbt_remove(rbt, node->key, node->klen, NULL);
            return 0;
        case RBT_WALK_DELETE_AND_CONTINUE: {
            if (node->left && node->right) {
                rbt_remove(rbt, node->key, node->klen, NULL);
                return rbt_walk_internal(rbt, node, sorted, cb, priv);
            }
            else if (node->left || node->right) {
                return rbt_walk_internal(rbt, node->left ? node->left : node->right, sorted, cb, priv);
            }
            // this node was a leaf
            return 1;
        }
        case RBT_WALK_STOP:
            return 0;
        case RBT_WALK_CONTINUE:
            break;
        default:
            // TODO - Error Messages
            break;
    }

    if (!sorted && node->left) {
        int rrc = rbt_walk_internal(rbt, node->left, sorted, cb, priv);
        if (rrc == 0)
            return rc + 1;
        rc += rrc;
    }

    if (node->right) {
        int rrc = rbt_walk_internal(rbt, node->right, sorted, cb, priv);
        if (rrc == 0)
            return rc + 1;
        rc += rrc;
    }

    return rc;
}

int
rbt_walk(rbt_t *rbt, rbt_walk_callback cb, void *priv)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_walk_internal(rbt, rbt->root, 0, cb, priv);

    // rbt->num_of_comparisons += rst;
    return rst;
}

int
rbt_walk_sorted(rbt_t *rbt, rbt_walk_callback cb, void *priv)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_walk_internal(rbt, rbt->root, 1, cb, priv);

    // rbt->num_of_comparisons += rst;
    return rst;
}

int
rbt_range_walk(rbt_t *rbt, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size,
               rbt_walk_callback cb, void *priv, int beginInclusive, int endInclusive)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_range_walk_internal(rbt, rbt->root, begin_key, bgk_size, end_key, edk_size, 0, cb, priv,
                                      beginInclusive, endInclusive);

    // rbt->num_of_comparisons += rst;
    return rst;
}

int
rbt_range_walk_sorted(rbt_t *rbt, void *begin_key, size_t bgk_size, void *end_key, size_t edk_size,
                      rbt_walk_callback cb, void *priv, int beginInclusive, int endInclusive)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_range_walk_internal(rbt, rbt->root, begin_key, bgk_size, end_key, edk_size, 1, cb, priv,
                                      beginInclusive, endInclusive);

    // rbt->num_of_comparisons += rst;
    return rst;
}

int
rbt_range_lt(rbt_t *rbt, void *end_key, size_t edk_size, rbt_walk_callback cb, void *priv, int end_inclusive)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_range_walk_internal(rbt, rbt->root, NULL, 0, end_key, edk_size, 0, cb, priv, 1,
                                      end_inclusive);

    // rbt->num_of_comparisons += rst;
    return rst;
}

int
rbt_range_gt(rbt_t *rbt, void *begin_key, size_t bgk_size, rbt_walk_callback cb, void *priv,
             int begin_inclusive)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_range_walk_internal(rbt, rbt->root, begin_key, bgk_size, NULL, 0, 0, cb, priv,
                                      begin_inclusive, 1);
    // rbt->num_of_comparisons += rst;
    return rst;
}

int
rbt_range_lt_sorted(rbt_t *rbt, void *end_key, size_t edk_size, rbt_walk_callback cb, void *priv,
                    int end_inclusive)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_range_walk_internal(rbt, rbt->root, NULL, 0, end_key, edk_size, 1, cb, priv, 1,
                                      end_inclusive);
    // rbt->num_of_comparisons += rst;
    return rst;
}

int
rbt_range_gt_sorted(rbt_t *rbt, void *begin_key, size_t bgk_size, rbt_walk_callback cb, void *priv,
                    int begin_inclusive)
{
    int rst = 0;
    if (rbt->root)
        rst = rbt_range_walk_internal(rbt, rbt->root, begin_key, bgk_size, NULL, 0, 1, cb, priv,
                                      begin_inclusive, 1);
    // rbt->num_of_comparisons += rst;
    return rst;
}

static inline rbt_node_t *
rbt_grandparent(rbt_node_t *node)
{
    if (node && node->parent)
        return node->parent->parent;
    return NULL;
}

static inline rbt_node_t *
rbt_uncle(rbt_node_t *node)
{
    rbt_node_t *gp = rbt_grandparent(node);
    if (!gp)
        return NULL;
    if (node->parent == gp->left)
        return gp->right;
    else
        return gp->left;
}

static inline int
rbt_compare_keys(rbt_t *rbt, void *k1, size_t k1size, void *k2, size_t k2size)
{
    // stopwatch_t t_locate;
    // timer_start(&t_locate);
    int rc;
    if (rbt->cmp_keys_cb) {
        rc = rbt->cmp_keys_cb(k1, k1size, k2, k2size);
    }
    else {
        if (k1size != k2size) {
            if (k2size > k1size) {
                rc = memcmp(k1, k2, k1size) - (k2size - k1size);
            }
            else {
                rc = memcmp(k1, k2, k2size) + (k1size - k2size);
            }
        }
        else {
            rc = memcmp(k1, k2, k1size);
        }
    }
    // timer_pause(&t_locate);
    // rbt->time_to_locate += timer_delta_ns(&t_locate);
    // rbt->num_of_comparisons++;
    return rc;
}

static int
rbt_add_internal(rbt_t *rbt, rbt_node_t *cur_node, rbt_node_t *new_node)
{
    int rc = rbt_compare_keys(rbt, cur_node->key, cur_node->klen, new_node->key, new_node->klen);

    if (rc == 0) {
        // key matches, just set the new value
        new_node->parent = cur_node->parent;
        new_node->color  = cur_node->color;
        if (new_node->parent) {
            if (new_node->parent->left == cur_node)
                new_node->parent->left = new_node;
            else
                new_node->parent->right = new_node;
        }
        new_node->left  = cur_node->left;
        new_node->right = cur_node->right;

        if (new_node->left)
            new_node->left->parent = new_node;

        if (new_node->right)
            new_node->right->parent = new_node;

        if (new_node->value != cur_node->value && rbt->free_value_cb)
            rbt->free_value_cb(cur_node->value);

        free(cur_node->key);
        free(cur_node);
        return 1;
    }
    else if (rc > 0) {
        if (cur_node->left) {
            return rbt_add_internal(rbt, cur_node->left, new_node);
        }
        else {
            cur_node->left   = new_node;
            new_node->parent = cur_node;
        }
    }
    else {
        if (cur_node->right) {
            return rbt_add_internal(rbt, cur_node->right, new_node);
        }
        else {
            cur_node->right  = new_node;
            new_node->parent = cur_node;
        }
    }
    return 0;
}

static inline void
rbt_rotate_right(rbt_t *rbt, rbt_node_t *node)
{
    rbt_node_t *p = node->left;
    node->left    = p ? p->right : NULL;
    if (p)
        p->right = node;

    if (node->left)
        node->left->parent = node;

    rbt_node_t *parent = node->parent;
    node->parent       = p;
    if (p) {
        p->parent = parent;
        if (p->parent == NULL) {
            rbt->root = p;
        }
        else {
            if (parent->left == node)
                parent->left = p;
            else
                parent->right = p;
        }
    }
    else {
        rbt->root = node;
    }
}

static inline void
rbt_rotate_left(rbt_t *rbt, rbt_node_t *node)
{
    rbt_node_t *p = node->right;
    node->right   = p ? p->left : NULL;
    if (p)
        p->left = node;

    if (node->right)
        node->right->parent = node;

    rbt_node_t *parent = node->parent;
    node->parent       = p;
    if (p) {
        p->parent = parent;
        if (p->parent == NULL) {
            rbt->root = p;
        }
        else {
            if (parent->left == node)
                parent->left = p;
            else
                parent->right = p;
        }
    }
    else {
        rbt->root = node;
    }
}

int
rbt_add(rbt_t *rbt, void *k, size_t klen, void *v)
{
    int rc = 0;
    // stopwatch_t t_expand;
    // timer_start(&t_expand);

    rbt_node_t *node = PDC_calloc(1, sizeof(rbt_node_t));
    mem_usage_by_all_rbtrees += sizeof(rbt_node_t);
    if (!node)
        return -1;

    // rbt->num_of_reallocs++;
    node->key = PDC_malloc(klen);
    mem_usage_by_all_rbtrees += klen;
    if (!node->key) {
        free(node);
        return -1;
    }
    memcpy(node->key, k, klen);
    node->klen  = klen;
    node->value = v;

    // timer_pause(&t_expand);
    // rbt->time_for_expansion += timer_delta_ns(&t_expand);
    // rbt->num_of_comparisons++;
    if (!rbt->root) {
        PAINT_BLACK(node);
        rbt->root = node;
    }
    else {
        rc = rbt_add_internal(rbt, rbt->root, node);
        // rbt->num_of_comparisons++;
        if (IS_BLACK(node)) {
            // if the node just added is now black it means
            // it was already existing and this was only a value update
            if (!node->parent) {
                // we need to check also if the root pointer
                // should be updated as well
                rbt->root = node;
            }
            return 1;
        }
        // rbt->num_of_comparisons++;
        if (!node->parent) {
            // case 1
            PAINT_BLACK(node);
            rbt->root = node;
        }
        else if (IS_BLACK(node->parent)) {
            // case 2
            return rc;
        }
        else {
            // case 3
            rbt_node_t *uncle       = rbt_uncle(node);
            rbt_node_t *grandparent = rbt_grandparent(node);
            // rbt->num_of_comparisons++;
            if (IS_RED(uncle)) {
                PAINT_BLACK(node->parent);
                PAINT_BLACK(uncle);
                if (grandparent) {
                    PAINT_RED(grandparent);
                    rbt_add(rbt, grandparent->key, grandparent->klen, grandparent->value);
                }
            }
            else if (grandparent) {
                // case 4
                // rbt->num_of_comparisons++;
                if (node == node->parent->right && node->parent == grandparent->left) {
                    rbt_rotate_left(rbt, node->parent);
                    node = node->left;
                }
                else if (node == node->parent->left && node->parent == grandparent->right) {
                    rbt_rotate_right(rbt, node->parent);
                    node = node->right;
                }
                // case 5
                grandparent = rbt_grandparent(node);
                if (node->parent) {
                    PAINT_BLACK(node->parent);
                    PAINT_RED(grandparent);
                    // rbt->num_of_comparisons++;
                    if (node == node->parent->left)
                        rbt_rotate_right(rbt, grandparent);
                    else
                        rbt_rotate_left(rbt, grandparent);
                }
                else {
                    fprintf(stderr, "Corrupted tree\n");
                    return -1;
                }
            }
        }
    }
    if (rc == 0) {
        rbt->size = rbt->size + 1;
    }
    return rc;
}

static rbt_node_t *
rbt_find_internal(rbt_t *rbt, rbt_node_t *node, void *key, size_t klen)
{
    if (!node)
        return NULL;

    int rc = rbt_compare_keys(rbt, node->key, node->klen, key, klen);

    if (rc == 0) {
        return node;
    }
    else if (rc > 0) {
        return rbt_find_internal(rbt, node->left, key, klen);
    }
    else {
        return rbt_find_internal(rbt, node->right, key, klen);
    }

    return NULL;
}

int
rbt_find(rbt_t *rbt, void *k, size_t klen, void **v)
{
    rbt_node_t *node = rbt_find_internal(rbt, rbt->root, k, klen);
    if (!node)
        return -1;

    *v = node->value;
    return 0;
}

static inline rbt_node_t *
rbt_sibling(rbt_node_t *node)
{
    return (node == node->parent->left) ? node->parent->right : node->parent->left;
}

static inline rbt_node_t *
rbt_find_next(rbt_node_t *node)
{
    if (!node->right)
        return NULL;

    rbt_node_t *next = node->right;

    while (next->left)
        next = next->left;

    return next;
}

static inline rbt_node_t *
rbt_find_prev(rbt_node_t *node)
{
    if (!node->left)
        return NULL;

    rbt_node_t *prev = node->left;

    while (prev->right)
        prev = prev->right;

    return prev;
}

static void
rbt_paint_onremove(rbt_t *rbt, rbt_node_t *node)
{
    if (!node)
        return;

    // delete case 1
    if (node->parent != NULL) {
        // delete case 2
        rbt_node_t *sibling = rbt_sibling(node);
        if (IS_RED(sibling)) {
            PAINT_RED(node->parent);
            PAINT_BLACK(sibling);
            if (node == node->parent->left) {
                rbt_rotate_left(rbt, node->parent);
            }
            else {
                rbt_rotate_right(rbt, node->parent);
            }
        }

        // delete case 3
        if (IS_BLACK(node->parent) && sibling && IS_BLACK(sibling) && IS_BLACK(sibling->left) &&
            IS_BLACK(sibling->right)) {
            PAINT_RED(sibling);
            rbt_paint_onremove(rbt, node->parent);
        }
        else {
            // delete case 4
            if (IS_RED(node->parent) && sibling && IS_BLACK(sibling) && IS_BLACK(sibling->left) &&
                IS_BLACK(sibling->right)) {
                PAINT_RED(sibling);
                PAINT_BLACK(node->parent);
            }
            else {
                // delete case 5
                if (IS_BLACK(sibling)) {
                    if (node == node->parent->left && sibling && IS_BLACK(sibling->right) &&
                        IS_RED(sibling->left)) {
                        PAINT_RED(sibling);
                        PAINT_BLACK(sibling->left);
                        rbt_rotate_right(rbt, sibling);
                    }
                    else if (node == node->parent->right && sibling && IS_BLACK(sibling->left) &&
                             IS_RED(sibling->right)) {
                        PAINT_RED(sibling);
                        PAINT_BLACK(sibling->right);
                        rbt_rotate_left(rbt, sibling);
                    }
                }
                // delete case 6
                if (sibling)
                    sibling->color = node->parent->color;
                PAINT_BLACK(node->parent);
                if (node == node->parent->left) {
                    if (sibling)
                        PAINT_BLACK(sibling->right);
                    rbt_rotate_left(rbt, node->parent);
                }
                else {
                    if (sibling)
                        PAINT_BLACK(sibling->left);
                    rbt_rotate_right(rbt, node->parent);
                }
            }
        }
    }
}

int
rbt_remove(rbt_t *rbt, void *k, size_t klen, void **v)
{
    rbt_node_t *node = rbt_find_internal(rbt, rbt->root, k, klen);
    if (!node)
        return -1;

    if (node->left || node->right) {
        // the node is not a leaf
        // now check if it has two children or just one
        if (node->left && node->right) {
            // two children case
            rbt_node_t *n        = NULL;
            static int  prevnext = 0;
            int         isprev   = (prevnext++ % 2 == 0);
            if (isprev)
                n = rbt_find_prev(node);
            else
                n = rbt_find_next(node);
            void *new_key = PDC_realloc(node->key, n->klen);
            mem_usage_by_all_rbtrees += n->klen;
            if (!new_key)
                return -1;
            node->key = new_key;
            memcpy(node->key, n->key, n->klen);
            void *prev_value = node->value;
            node->value      = n->value;
            if (isprev) {
                if (n == node->left) {
                    node->left = n->left;
                }
                else {
                    n->parent->right = n->left;
                }
                if (n->left) {
                    n->left->parent = node;
                }
            }
            else {
                if (n == node->right) {
                    node->right = n->right;
                }
                else {
                    n->parent->left = n->right;
                }
                if (n->right) {
                    n->right->parent = node;
                }
            }
            free(n->key);
            if (v)
                *v = prev_value;
            else if (rbt->free_value_cb)
                rbt->free_value_cb(prev_value);

            free(n);
            rbt->size = rbt->size - 1;
            return 0;
        }
        else {
            // one child case
            rbt_node_t *child = node->right ? node->right : node->left;
            // replace node with child
            child->parent = node->parent;
            if (child->parent) {
                if (node == node->parent->left)
                    node->parent->left = child;
                else
                    node->parent->right = child;
            }
            if (IS_BLACK(node)) {
                if (IS_RED(child)) {
                    PAINT_BLACK(child);
                }
                else {
                    rbt_paint_onremove(rbt, child);
                }
            }
            if (v)
                *v = node->value;
            else if (rbt->free_value_cb)
                rbt->free_value_cb(node->value);

            free(node->key);
            free(node);
            rbt->size = rbt->size - 1;
            return 0;
        }
    }

    // if it's not the root node we need to update the parent
    if (node->parent) {
        if (node == node->parent->left)
            node->parent->left = NULL;
        else
            node->parent->right = NULL;
    }
    if (v)
        *v = node->value;
    else if (rbt->free_value_cb && node->value)
        rbt->free_value_cb(node->value);

    free(node->key);
    free(node);
    rbt->size = rbt->size - 1;
    return 0;
}

uint64_t
rbt_size(rbt_t *rbt)
{
    if (rbt == NULL) {
        return 0;
    }
    return rbt->size;
}

#ifdef DEBUG_RBTREE
static int
rbt_print_internal(rbt_node_t *node, int is_left, int offset, int depth, char s[20][255])
{
    char b[20];
    memset(b, 0, sizeof(b));

    if (!node)
        return 0;

    sprintf(b, "(%d %C)", *((int *)node->value), node->color ? 'B' : 'R');
    int width = strlen(b);

    int left  = rbt_print_internal(node->left, 1, offset, depth + 1, s);
    int right = rbt_print_internal(node->right, 0, offset + left + width, depth + 1, s);

    int i;

#ifdef DEBUG_RBTREE_COMPACT
    for (i = 0; i < width; i++)
        s[depth][offset + left + i] = b[i];

    if (depth && is_left) {

        for (i = 0; i < width + right; i++)
            s[depth - 1][offset + left + width / 2 + i] = '-';

        s[depth - 1][offset + left + width / 2] = '.';
    }
    else if (depth && !is_left) {

        for (i = 0; i < left + width; i++)
            s[depth - 1][offset - width / 2 + i] = '-';

        s[depth - 1][offset + left + width / 2] = '.';
    }
#else
    for (i = 0; i < width; i++)
        s[2 * depth][offset + left + i] = b[i];

    if (depth && is_left) {

        for (i = 0; i < width + right; i++)
            s[2 * depth - 1][offset + left + width / 2 + i] = '-';

        s[2 * depth - 1][offset + left + width / 2]                 = '+';
        s[2 * depth - 1][offset + left + width + right + width / 2] = '+';
    }
    else if (depth && !is_left) {

        for (i = 0; i < left + width; i++)
            s[2 * depth - 1][offset - width / 2 + i] = '-';

        s[2 * depth - 1][offset + left + width / 2] = '+';
        s[2 * depth - 1][offset - width / 2 - 1]    = '+';
    }
#endif

    return left + width + right;
}

void
rbt_print(rbt_t *rbt)
{
    int  i;
    char s[20][255];
    memset(s, 0, sizeof(s));

    struct winsize w;
    ioctl(0, TIOCGWINSZ, &w);

    char format[16];
    snprintf(format, sizeof(format), "%%%ds", w.ws_col);
    for (i = 0; i < 20; i++)
        sprintf(s[i], format, " ");

    rbt_print_internal(rbt->root, 0, 0, 0, s);

    for (i = 0; i < 20; i++)
        printf("%s\n", s[i]);
}
#endif

// perf_info_t *
// get_perf_info_rbtree(rbt_t *index_root)
// {
//     GET_PERF_INFO(index_root);
// }

// void
// reset_perf_info_counters_rbtree(rbt_t *rbt)
// {
//     RESET_PERF_INFO_COUNTERS(rbt);
// }

// size_t
// get_mem_usage_by_all_rbtrees()
// {
//     return mem_usage_by_all_rbtrees;
// }

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
