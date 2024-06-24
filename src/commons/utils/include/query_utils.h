//
// Created by Wei Zhang on 7/10/17.
//
#ifndef PDC_QUERY_UTILS_H
#define PDC_QUERY_UTILS_H

#define TAG_DELIMITER ","
#include "string_utils.h"
#include "pdc_public.h"
#include "comparators.h"

typedef struct query_gen_input {
    pdc_kvtag_t *base_tag;
    int          key_query_type;
    int          value_query_type;
    int          affix_len;
    int          range_lo;
    int          range_hi;
} query_gen_input_t;

typedef struct query_gen_output {
    char * key_query;
    size_t key_query_len;
    char * value_query;
    size_t value_query_len;
} query_gen_output_t;

typedef enum { NUM_EXACT, NUM_LT, NUM_GT, NUM_BETWEEN } NUM_QUERY_TYPE;

typedef void (*num_query_action)(void *cond_exact, void *cond_lo, void *cond_hi, int lo_inclusive,
                                 int hi_inclusive, pdc_c_var_type_t num_type, void *input, void **out,
                                 uint64_t *out_len);

typedef struct {
    num_query_action exact_action;
    num_query_action lt_action;
    num_query_action gt_action;
    num_query_action between_action;
} num_query_action_collection_t;

/**
 * Generate query strings for key and value according to the given input.
 * The query strings will be stored in the output.
 * @param input
 * @param output
 */
void gen_query_key_value(query_gen_input_t *input, query_gen_output_t *output);

/**
 * Generate concatenated query strings for key and value according to the given input.
 * @param query_gen_output
 * @return concatenated query string
 */
char *gen_query_str(query_gen_output_t *query_gen_output);

/**
 * Free the memory allocated for the output of query generation
 * This function does not free the memory allocated for the query_gen_output_t struct itself.
 * @param output
 */
void free_query_output(query_gen_output_t *output);

/**
 * Test tag generation in a for loop
 */
void gen_tags_in_loop();
/**
 * Generate tags according to a seed
 * @return
 */
char *gen_tags(int);
/**
 * Get key string from a key value pair
 * @param kv_pair
 * @param delim
 * @return
 */
char *get_key(const char *kv_pair, char delim);
/**
 * Get value string from a key value pair
 * @param kv_pair
 * @param delim
 * @return
 */
char *get_value(const char *kv_pair, char delim);
/**
 * To test if a key value pair matches the given conditions
 * @param tagslist
 * @param key_pattern
 * @param value_pattern
 * @return
 */
char *k_v_matches_p(const char *tagslist, const char *key_pattern, const char *value_pattern);
/**
 * To test if there is the specified tag within a list of tags.
 * @param tagslist
 * @param tagname
 * @return
 */
int has_tag(const char *tagslist, const char *tagname);
/**
 * To test if there is a tag matching the given pattern in the list of tags
 * @param tagslist
 * @param pattern
 * @return
 */
int has_tag_p(const char *tagslist, const char *pattern);
/**
 * To test if there is a tag in the list of tags with its value equals to the given value
 * @param tagslist
 * @param tagname
 * @param val
 * @return
 */
int is_value_match(const char *tagslist, const char *tagname, const char *val);
/**
 * To test if there is a tag with its value matching the given pattern in the list of tags
 * @param tagslist
 * @param tagname
 * @param pattern
 * @return
 */
int is_value_match_p(const char *tagslist, const char *tagname, const char *pattern);
/**
 * To test if there is a tag in the tag list with its value ranges from *from* to *to*
 * @param tagslist
 * @param tagname
 * @param from
 * @param to
 * @return
 */
int is_value_in_range(const char *tagslist, const char *tagname, int from, int to);

/**
 * determine if the value part in the query condition is a string query
 */
int is_string_query(char *value_query);

/**
 * determine if the value part in the query condition is an affix-based query
 */
int is_affix_query(char *value_query);

/**
 * determine if the value part in the query condition is a range query
 */
int is_number_query(char *value_query);

/**
 * parse and run a string query for number value
 *
 * The following queries are what we need to support
 * 1. exact query -> key=|value| (key == value)
 * 5. range query -> key=value~  (key > value)
 * 6. range query -> key=~value (key < value)
 * 7. range query -> key=value|~ (key >= value)
 * 8. range query -> key=~|value (key <= value)
 * 9. range query -> key=value1|~value2 (value1 <= key < value2)
 * 10. range query -> key=value1~|value2 (value1 < key <= value2)
 * 11. range query -> key=value1~value2 (value1 < key < value2)
 * 12. range query -> key=value1|~|value2 (value1 <= key <= value2)
 */
int parse_and_run_number_value_query(char *num_val_query, pdc_c_var_type_t num_type,
                                     num_query_action_collection_t *action_collection, void *cb_input,
                                     uint64_t *cb_out_len, void **cb_out);

#endif // PDC_QUERY_UTILS_H
