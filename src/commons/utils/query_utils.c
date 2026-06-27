#include "query_utils.h"
#include <inttypes.h>
#include <stdint.h>
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

int
_gen_affix_for_token(char *token_str, int affix_type, size_t affix_len, char **out_str)
{
    FUNC_ENTER(NULL);

    size_t token_len = strlen(token_str);

    if (affix_type == 0) {
        *out_str = strdup(token_str);
        FUNC_LEAVE(token_len);
    }

    affix_len        = affix_len < token_len ? affix_len : token_len;
    size_t copy_len  = affix_type == 0 ? token_len : affix_len;
    char * source    = affix_type <= 1 ? token_str : &(token_str[token_len - affix_len]);
    char * affix_str = (char *)PDC_calloc(copy_len + 3, sizeof(char));

    strncpy(affix_str, source, copy_len + 1);

    if (affix_type == 1) { // prefix
        // "hello" -> "hell*" or "hell" -> "hell*"
        affix_str[affix_len]     = '*';
        affix_str[affix_len + 1] = '\0';
    }
    else if (affix_type == 2) { // suffix
        // "hello" -> '*ello' or 'hell' -> '*hell'
        for (int k = affix_len; k > 0; k--) {
            affix_str[k] = affix_str[k - 1];
        }
        affix_str[0]             = '*';
        affix_str[affix_len + 1] = '\0';
    }
    else if (affix_type == 3) { // infix
        // "hello" -> '*ello*' or 'hell' -> '*hell*'
        for (int k = affix_len; k > 0; k--) {
            affix_str[k] = affix_str[k - 1];
        }
        affix_str[0]             = '*';
        affix_str[affix_len + 1] = '*';
        affix_str[affix_len + 2] = '\0';
    }
    else {
        LOG_ERROR("Invalid affix type %d\n", affix_type);
        FUNC_LEAVE(0);
    }

    *out_str = affix_str;

    FUNC_LEAVE(strlen(*out_str));
}

void
gen_query_key_value(query_gen_input_t *input, query_gen_output_t *output)
{
    FUNC_ENTER(NULL);

    char * key_ptr       = NULL;
    size_t key_ptr_len   = 0;
    char * value_ptr     = NULL;
    size_t value_ptr_len = 0;
    // check base_tag->name length
    if (strlen(input->base_tag->name) < 3) {
        char *new_tag_name = (char *)PDC_calloc(4, sizeof(char));
        memset(new_tag_name, input->base_tag->name[0], 3);
        input->base_tag->name = new_tag_name;
        input->affix_len      = 1;
    }
    // check base_tag->value length if it is a string
    if (input->base_tag->type == PDC_STRING && strlen((char *)input->base_tag->value) < 3) {
        char *new_tag_value = (char *)PDC_calloc(4, sizeof(char));
        memset(new_tag_value, ((char *)input->base_tag->value)[0], 3);
        input->base_tag->value = (void *)new_tag_value;
        input->affix_len       = 1;
    }
    size_t affix_len = input->affix_len;

    // "hello"
    key_ptr_len = _gen_affix_for_token(input->base_tag->name, input->key_query_type, affix_len, &key_ptr);
    if (key_ptr_len == 0) {
        LOG_ERROR("Failed to generate key query\n");
        FUNC_LEAVE_VOID();
    }

    // process value in base_tag
    if (is_PDC_STRING(input->base_tag->type)) {
        char *temp_value = NULL;
        value_ptr_len    = _gen_affix_for_token((char *)input->base_tag->value, input->value_query_type,
                                             affix_len, &temp_value);

        if (value_ptr_len == 0) {
            LOG_ERROR("Failed to generate value query\n");
            FUNC_LEAVE_VOID();
        }

        // Allocate space for: '"' + temp_value + '"' + '\0'
        value_ptr = (char *)PDC_calloc(value_ptr_len + 3, sizeof(char));
        if (!value_ptr) {
            LOG_ERROR("Memory allocation failed for value_ptr");
            return;
        }

        int written = snprintf(value_ptr, value_ptr_len + 3, "\"%s\"", temp_value);
        if (written < 0 || written >= value_ptr_len + 3) {
            LOG_ERROR("Value string formatting failed or was truncated");
            value_ptr = (char *)PDC_free(value_ptr);
            value_ptr = NULL;
            return;
        }
    }
    else {
        if (is_PDC_INT(input->base_tag->type)) {
            input->base_tag->type = PDC_INT64;
        }
        else if (is_PDC_UINT(input->base_tag->type)) {
            input->base_tag->type = PDC_UINT64;
        }
        else if (is_PDC_FLOAT(input->base_tag->type)) {
            input->base_tag->type = PDC_DOUBLE;
        }
        else {
            LOG_ERROR("Invalid tag type\n");
            FUNC_LEAVE_VOID();
        }
        char *format_str = get_format_by_dtype(input->base_tag->type);
        if (input->value_query_type == 4) {
            value_ptr_len = snprintf(NULL, 0, format_str, ((int64_t *)input->base_tag->value)[0]);
            value_ptr     = (char *)PDC_calloc(value_ptr_len + 1, sizeof(char));
            snprintf(value_ptr, value_ptr_len + 1, format_str, ((int64_t *)input->base_tag->value)[0]);
        }
        else if (input->value_query_type == 5) {
            size_t lo_len = snprintf(NULL, 0, format_str, input->range_lo);
            size_t hi_len = snprintf(NULL, 0, format_str, input->range_hi);
            value_ptr_len = lo_len + hi_len + 1;
            value_ptr     = (char *)PDC_calloc(value_ptr_len + 1, sizeof(char));
            char fmt_str[20];
            snprintf(fmt_str, 20, "%s~%s", format_str, format_str);
            snprintf(value_ptr, value_ptr_len + 1, fmt_str, input->range_lo, input->range_hi);
        }
        else {
            LOG_ERROR("Invalid value query type for integer\n");
            FUNC_LEAVE_VOID();
        }
    }

    output->key_query       = key_ptr;
    output->key_query_len   = key_ptr_len;
    output->value_query     = value_ptr;
    output->value_query_len = value_ptr_len;

    FUNC_LEAVE_VOID();
}

char *
gen_query_str(query_gen_output_t *query_gen_output)
{
    FUNC_ENTER(NULL);

    char *final_query_str = (char *)PDC_calloc(
        query_gen_output->key_query_len + query_gen_output->value_query_len + 2, sizeof(char));
    snprintf(final_query_str, query_gen_output->key_query_len + query_gen_output->value_query_len + 2,
             "%s=%s", query_gen_output->key_query, query_gen_output->value_query);

    FUNC_LEAVE(final_query_str);
}

void
free_query_output(query_gen_output_t *output)
{
    FUNC_ENTER(NULL);

    if (output->key_query != NULL) {
        output->key_query = (char *)PDC_free(output->key_query);
    }
    if (output->value_query != NULL) {
        output->value_query = (char *)PDC_free(output->value_query);
    }

    FUNC_LEAVE_VOID();
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
    FUNC_ENTER(NULL);

    char *ret = NULL;
    int   idx = indexOf(kv_pair, delim);

    if (idx < 0) {
        FUNC_LEAVE(ret);
    }

    FUNC_LEAVE(subrstr(kv_pair, idx));
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
    FUNC_ENTER(NULL);

    char *ret = NULL;
    int   idx = indexOf(kv_pair, delim);

    if (idx < 0) {
        FUNC_LEAVE(ret);
    }

    FUNC_LEAVE(substr(kv_pair, idx + 1));
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
    FUNC_ENTER(NULL);

    int   j;
    int   tag_num = obj_id % 20;
    char *ret     = "";
    for (j = 0; j <= tag_num; j++) {
        char *fspace = ret;
        ret          = dsprintf("%stag%d=%d%d,", ret, j, obj_id, j);
        if (strlen(fspace) > 0) {
            fspace = (char *)PDC_free(fspace);
        }
    }
    ret[strlen(ret) - 1] = '\0';

    FUNC_LEAVE(ret);
}

/**
 * generate multiple tagslists for many objects.
 *
 * This is just a test.
 */
void
gen_tags_in_loop()
{
    FUNC_ENTER(NULL);

    int my_count = 1000;
    int i;
    for (i = 0; i < my_count; i++) {
        int   tag_num = i % 20;
        char *ret     = gen_tags(tag_num);
        if (ret != NULL) {
            ret = (char *)PDC_free(ret);
        }
    }

    FUNC_LEAVE_VOID();
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
    FUNC_ENTER(NULL);

    /*
    char *pattern = strdup(tagname);
    if (startsWith("*", pattern)) {
        pattern = &pattern[1];
    }
    if (endsWith("*", pattern)) {
        pattern[strlen(pattern)]='\0';
    }
     */
    FUNC_LEAVE(has_tag_p(tagslist, tagname));
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
    FUNC_ENTER(NULL);
    FUNC_LEAVE(k_v_matches_p(tagslist, pattern, NULL) != NULL);
}

char *
k_v_matches_p(const char *tagslist, const char *key_pattern, const char *value_pattern)
{
    FUNC_ENTER(NULL);

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
                key = (char *)PDC_free(key);
            }
            if (value != NULL) {
                value = (char *)PDC_free(value);
            }

            if (pattern_matches) {
                rst_kv = tag_kv;
                break;
            }
        }
        tag_kv = strtok(NULL, TAG_DELIMITER);
    }

    FUNC_LEAVE(rst_kv);
}

int
is_value_match(const char *tagslist, const char *tagname, const char *val)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(is_value_match_p(tagslist, tagname, val));
}

int
is_value_match_p(const char *tagslist, const char *tagname, const char *pattern)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(k_v_matches_p(tagslist, tagname, pattern) != NULL);
}

int
is_value_in_range(const char *tagslist, const char *tagname, int from, int to)
{
    FUNC_ENTER(NULL);

    const char *matched_kv = k_v_matches_p(tagslist, tagname, NULL);
    char *      value      = get_value(matched_kv, '=');
    int         v          = atoi(value);

    FUNC_LEAVE(v >= from && v <= to);
}

int
is_string_query(char *value_query)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(is_quoted_string(value_query));
}

int
is_affix_query(char *value_query)
{
    FUNC_ENTER(NULL);

    if (is_string_query(value_query) && contains(value_query, "*")) {
        FUNC_LEAVE(1);
    }

    FUNC_LEAVE(0);
}

int
is_number_query(char *value_query)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(!is_string_query(value_query));
}

int
parse_and_run_number_value_query(char *num_val_query, pdc_c_var_type_t num_type,
                                 num_query_action_collection_t *action_collection, void *cb_input,
                                 uint64_t *cb_out_len, void **cb_out)
{
    FUNC_ENTER(NULL);

    // allocate memory according to the val_idx_dtype for value 1 and value 2.
    void *val1;
    void *val2;
    if (startsWith(num_val_query, "|") && startsWith(num_val_query, "|")) { // EXACT
        // exact number search
        char * num_str = substring(num_val_query, 1, strlen(num_val_query) - 1);
        size_t klen1   = get_number_from_string(num_str, num_type, &val1);

        action_collection->exact_action(val1, NULL, NULL, 1, 1, num_type, cb_input, cb_out, cb_out_len);
    }
    else if (startsWith(num_val_query, "~")) { // LESS THAN
        int endInclusive = num_val_query[1] == '|';
        // find all numbers that are smaller than the given number
        int    beginPos = endInclusive ? 2 : 1;
        char * numstr   = substring(num_val_query, beginPos, strlen(num_val_query));
        size_t klen1    = get_number_from_string(numstr, num_type, &val1);
        action_collection->lt_action(NULL, NULL, val1, 0, endInclusive, num_type, cb_input, cb_out,
                                     cb_out_len);
    }
    else if (endsWith(num_val_query, "~")) { // GEATER THAN
        int beginInclusive = num_val_query[strlen(num_val_query) - 2] == '|';
        int endPos         = beginInclusive ? strlen(num_val_query) - 2 : strlen(num_val_query) - 1;
        // find all numbers that are greater than the given number
        char * numstr = substring(num_val_query, 0, endPos);
        size_t klen1  = get_number_from_string(numstr, num_type, &val1);

        action_collection->gt_action(NULL, val1, NULL, beginInclusive, 0, num_type, cb_input, cb_out,
                                     cb_out_len);
    }
    else if (contains(num_val_query, "~")) { // BETWEEN
        int    num_tokens = 0;
        char **tokens     = NULL;
        // the string is not ended or started with '~', and if it contains '~', it is a in-between query.
        split_string(num_val_query, "~", &tokens, &num_tokens);
        if (num_tokens != 2) {
            LOG_ERROR("Error invalid range query: %s\n", num_val_query);
            return -1;
        }
        char *lo_tok = tokens[0];
        char *hi_tok = tokens[1];
        // lo_tok might be ended with '|', and hi_tok might be started with '|', to indicate inclusivity.
        int    beginInclusive = endsWith(lo_tok, "|");
        int    endInclusive   = startsWith(hi_tok, "|");
        char * lo_num_str     = beginInclusive ? substring(lo_tok, 0, strlen(lo_tok) - 1) : lo_tok;
        char * hi_num_str     = endInclusive ? substring(hi_tok, 1, strlen(hi_tok)) : hi_tok;
        size_t klen1          = get_number_from_string(lo_num_str, num_type, &val1);
        size_t klen2          = get_number_from_string(hi_num_str, num_type, &val2);

        action_collection->between_action(NULL, val1, val2, beginInclusive, endInclusive, num_type, cb_input,
                                          cb_out, cb_out_len);
    }
    else {
        // exact query by default
        // exact number search
        char * num_str = strdup(num_val_query);
        size_t klen1   = get_number_from_string(num_str, num_type, &val1);

        action_collection->exact_action(val1, NULL, NULL, 1, 1, num_type, cb_input, cb_out, cb_out_len);
    }

    FUNC_LEAVE(0);
}