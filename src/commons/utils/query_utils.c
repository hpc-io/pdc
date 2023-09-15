//
// Created by Wei Zhang on 7/10/17.
//
#include "query_utils.h"

int
_gen_affix_for_token(char *token_str, int affix_type, size_t affix_len, char **out_str)
{

    size_t token_len = (strlen(token_str) <= affix_len) ? affix_len : strlen(token_str);
    *out_str         = (char *)calloc(token_len + 3, sizeof(char));
    strncpy(*out_str, affix_type <= 1 ? token_str : &(token_str[token_len - affix_len]),
            affix_type <= 1 ? token_len + 1 : affix_len + 1);

    if (affix_type == 0) { // exact
        // nothing to do here.
    }
    else if (affix_type == 1) { // prefix
        // "hello" -> "hell*" or "hell" -> "hell*"
        (*out_str)[affix_len]     = '*';
        (*out_str)[affix_len + 1] = '\0';
    }
    else if (affix_type == 2) { // suffix
        // "hello" -> '*ello' or 'hell' -> '*hell'
        for (int k = token_len; k > (token_len - affix_len); k--) {
            (*out_str)[k] = (*out_str)[k - 1];
        }
        (*out_str)[0]             = '*';
        (*out_str)[affix_len + 1] = '\0';
    }
    else if (affix_type == 3) { // infix
        // "hello" -> '*ello*' or 'hell' -> '*hell*'
        for (int k = token_len; k > (token_len - affix_len); k--) {
            (*out_str)[k] = (*out_str)[k - 1];
        }
        (*out_str)[0]             = '*';
        (*out_str)[affix_len + 1] = '*';
        (*out_str)[affix_len + 2] = '\0';
    }
    else {
        printf("Invalid affix type!\n");
        return 0;
    }
    return strlen(*out_str);
}

void
gen_query_key_value(query_gen_input_t *input, query_gen_output_t *output)
{
    char * key_ptr       = NULL;
    size_t key_ptr_len   = 0;
    char * value_ptr     = NULL;
    size_t value_ptr_len = 0;
    // check base_tag->name length
    if (strlen(input->base_tag->name) < 3) {
        char *new_tag_name = (char *)calloc(4, sizeof(char));
        memset(new_tag_name, input->base_tag->name[0], 3);
        input->base_tag->name = new_tag_name;
        input->affix_len      = 1;
    }
    // check base_tag->value length if it is a string
    if (input->base_tag->type == PDC_STRING && strlen((char *)input->base_tag->value) < 3) {
        char *new_tag_value = (char *)calloc(4, sizeof(char));
        memset(new_tag_value, ((char *)input->base_tag->value)[0], 3);
        input->base_tag->value = (void *)new_tag_value;
        input->affix_len       = 1;
    }
    size_t affix_len = input->affix_len;

    // "hello"
    key_ptr_len = _gen_affix_for_token(input->base_tag->name, input->key_query_type, affix_len, &key_ptr);
    if (key_ptr_len == 0) {
        printf("Failed to generate key query!\n");
        return;
    }

    // process value in base_tag
    if (input->base_tag->type == PDC_STRING) {
        value_ptr_len = _gen_affix_for_token((char *)input->base_tag->value, input->value_query_type,
                                             affix_len, &value_ptr);
        if (value_ptr_len == 0) {
            printf("Failed to generate value query!\n");
            return;
        }
    }
    else if (input->base_tag->type == PDC_INT) {
        if (input->value_query_type == 4) {
            value_ptr_len = snprintf(NULL, 0, "%d", ((int *)input->base_tag->value)[0]);
            value_ptr     = (char *)calloc(value_ptr_len + 1, sizeof(char));
            snprintf(value_ptr, value_ptr_len + 1, "%d", ((int *)input->base_tag->value)[0]);
        }
        else if (input->value_query_type == 5) {
            size_t lo_len = snprintf(NULL, 0, "%d", input->range_lo);
            size_t hi_len = snprintf(NULL, 0, "%d", input->range_hi);
            value_ptr_len = lo_len + hi_len + 1;
            value_ptr     = (char *)calloc(value_ptr_len + 1, sizeof(char));
            snprintf(value_ptr, value_ptr_len + 1, "%d~%d", input->range_lo, input->range_hi);
        }
        else {
            printf("Invalid value query type for integer!\n");
            return;
        }
    }
    else {
        printf("Invalid tag type!\n");
        return;
    }

    output->key_query       = key_ptr;
    output->key_query_len   = key_ptr_len;
    output->value_query     = value_ptr;
    output->value_query_len = value_ptr_len;
}

char *
gen_query_str(query_gen_output_t *query_gen_output)
{
    char *final_query_str =
        (char *)calloc(query_gen_output->key_query_len + query_gen_output->value_query_len + 2, sizeof(char));
    strcat(final_query_str, query_gen_output->key_query);
    strcat(final_query_str, "=");
    strcat(final_query_str, query_gen_output->value_query);
    return final_query_str;
}

void
free_query_output(query_gen_output_t *output)
{
    if (output->key_query != NULL) {
        free(output->key_query);
    }
    if (output->value_query != NULL) {
        free(output->value_query);
    }
}

/**
 *
 * return the key from a kv_pair string connected by delim character.
 *
 * return NULL if key cannot be retrieved, either because the kv_pair is invalid, or
 * because no delimiter is found in kv_pair string.
 *
 * @param kv_pair
 * @param delim
 * @return
 */
char *
get_key(const char *kv_pair, char delim)
{

    char *ret = NULL;
    int   idx = indexOf(kv_pair, delim);

    if (idx < 0) {
        return ret;
    }
    return subrstr(kv_pair, idx);
}

/**
 * return the key from a kv_pair string connected by delim character.
 *
 * return NULL if key cannot be retrieved, either because the kv_pair is invalid, or
 * because no delimiter is found in kv_pair string.
 *
 * @param kv_pair
 * @param delim
 * @return
 */
char *
get_value(const char *kv_pair, char delim)
{

    char *ret = NULL;
    int   idx = indexOf(kv_pair, delim);

    if (idx < 0) {
        return ret;
    }

    return substr(kv_pair, idx + 1);
}

/**
 * Generate tags based on obj_id.
 *
 * It is just for simulation, pretty random.
 *
 * @param obj_id
 * @return
 */
char *
gen_tags(int obj_id)
{
    int   j;
    int   tag_num = obj_id % 20;
    char *ret     = "";
    for (j = 0; j <= tag_num; j++) {
        char *fspace = ret;
        ret          = dsprintf("%stag%d=%d%d,", ret, j, obj_id, j);
        if (strlen(fspace) > 0) {
            free(fspace);
        }
    }
    ret[strlen(ret) - 1] = '\0';
    return ret;
}

/**
 * generate multiple tagslists for many objects.
 *
 * This is just a test.
 */
void
gen_tags_in_loop()
{

    int my_count = 1000;
    int i;
    for (i = 0; i < my_count; i++) {
        int   tag_num = i % 20;
        char *ret     = gen_tags(tag_num);
        println("helloworld, %s", ret);
        if (ret != NULL) {
            free(ret);
        }
    }
}
/**
 * returns 1 if the tag is found, otherwise, returns 0.
 * @param tagslist
 * @param tagname
 * @return
 */
int
has_tag(const char *tagslist, const char *tagname)
{
    /*
    char *pattern = strdup(tagname);
    if (startsWith("*", pattern)) {
        pattern = &pattern[1];
    }
    if (endsWith("*", pattern)) {
        pattern[strlen(pattern)]='\0';
    }
     */
    return has_tag_p(tagslist, tagname);
}
/**
 * Check if there is any tag in the tags list that matches the given pattern.
 *
 * @param tagslist
 * @param pattern
 * @return
 */
int
has_tag_p(const char *tagslist, const char *pattern)
{
    return (k_v_matches_p(tagslist, pattern, NULL) != NULL);
}

char *
k_v_matches_p(const char *tagslist, const char *key_pattern, const char *value_pattern)
{
    char *rst_kv     = NULL;
    char *_tags_list = NULL;

    if (tagslist == NULL || key_pattern == NULL) {
        return rst_kv;
    }
    _tags_list = strdup(tagslist);
    // GO THROUGH EACH KV PAIR
    char *tag_kv = strtok(_tags_list, TAG_DELIMITER);
    while (tag_kv != NULL) {
        /**
         * Check to see if the current key-value pair is valid
         */
        if (strchr(tag_kv, '=') != NULL) {
            // get key and value
            char *key   = NULL;
            key         = get_key(tag_kv, '=');
            char *value = NULL;
            value       = get_value(tag_kv, '=');

            /**
             * if no value pattern is specified, we only match key pattern.
             * otherwise, we match both.
             */
            int is_key_matched = simple_matches(key, key_pattern);

            int is_value_matched = (value_pattern == NULL ? 0 : simple_matches(value, value_pattern));

            int pattern_matches =
                (value_pattern == NULL ? is_key_matched : (is_key_matched && is_value_matched));

            if (key != NULL) {
                free(key);
            }
            if (value != NULL) {
                free(value);
            }

            if (pattern_matches) {
                rst_kv = tag_kv;
                break;
            }
        }
        tag_kv = strtok(NULL, TAG_DELIMITER);
    }

    if (_tags_list != NULL) {
        // free(_tags_list);
    }
    return rst_kv;
}

int
is_value_match(const char *tagslist, const char *tagname, const char *val)
{
    /*
    char *pattern = strdup(val);
    if (startsWith("*", pattern)) {
        pattern = &pattern[1];
    }
    if (endsWith("*", pattern)) {
        pattern[strlen(pattern)]='\0';
    }
     */
    return is_value_match_p(tagslist, tagname, val);
}
int
is_value_match_p(const char *tagslist, const char *tagname, const char *pattern)
{
    return (k_v_matches_p(tagslist, tagname, pattern) != NULL);
}
int
is_value_in_range(const char *tagslist, const char *tagname, int from, int to)
{
    const char *matched_kv = k_v_matches_p(tagslist, tagname, NULL);
    char *      value      = get_value(matched_kv, '=');
    int         v          = atoi(value);
    return (v >= from && v <= to);
}
