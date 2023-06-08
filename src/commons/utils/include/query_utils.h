//
// Created by Wei Zhang on 7/10/17.
//
#ifndef PDC_QUERY_UTILS_H
#define PDC_QUERY_UTILS_H

#define TAG_DELIMITER ","
#include "string_utils.h"



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


#endif //PDC_QUERY_UTILS_H
