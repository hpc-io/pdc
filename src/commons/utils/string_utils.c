#include "string_utils.h"
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"
#include <regex.h>

const char *VISIBLE_ALPHABET =
    "abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+[]{}|;:'\",.<>?/`~ABCDEFGHIJKLMNOPQRSTUVWXYZ";

int
startsWith(const char *str, const char *pre)
{
    FUNC_ENTER(NULL);

    if (str == NULL || pre == NULL)
        FUNC_LEAVE(0);

    FUNC_LEAVE(strncmp(str, pre, strlen(pre)) == 0);
}

int
endsWith(const char *str, const char *suf)
{
    FUNC_ENTER(NULL);

    if (str == NULL || suf == NULL)
        FUNC_LEAVE(0);
    size_t lensuf = strlen(suf), lenstr = strlen(str);

    FUNC_LEAVE(lenstr < lensuf ? 0 : (strncmp(str + lenstr - lensuf, suf, lensuf) == 0));
}

int
contains(const char *str, const char *tok)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(strstr(str, tok) != NULL);
}

int
equals(const char *str, const char *tok)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(strcmp(tok, str) == 0);
}

int
simple_matches(const char *str, const char *pattern)
{
    FUNC_ENTER(NULL);

    int result = 0;
    // Ensure both str and pattern cannot be empty.
    if (str == NULL || pattern == NULL) {
        FUNC_LEAVE(result);
    }
    int pattern_type = determine_pattern_type(pattern);

    char *tok = NULL;
    switch (pattern_type) {
        case PATTERN_EXACT:
            result = equals(str, pattern);
            break;
        case PATTERN_PREFIX:
            tok    = subrstr(pattern, strlen(pattern) - 1);
            result = (tok == NULL ? 0 : startsWith(str, tok));
            break;
        case PATTERN_SUFFIX:
            tok    = substr(pattern, 1);
            result = (tok == NULL ? 0 : endsWith(str, tok));
            break;
        case PATTERN_MIDDLE:
            tok    = substring(pattern, 1, strlen(pattern) - 1);
            result = (tok == NULL ? 0 : contains(str, tok));
            break;
        default:
            break;
    }
    if (tok != NULL) {
        tok = (char *)PDC_free(tok);
    }

    FUNC_LEAVE(result);
}

pattern_type_t
determine_pattern_type(const char *pattern)
{
    FUNC_ENTER(NULL);

    if (startsWith(pattern, "*")) {
        if (endsWith(pattern, "*")) {
            FUNC_LEAVE(PATTERN_MIDDLE);
        }
        else {
            FUNC_LEAVE(PATTERN_SUFFIX);
        }
    }
    else {
        if (endsWith(pattern, "*")) {
            FUNC_LEAVE(PATTERN_PREFIX);
        }
        else {
            FUNC_LEAVE(PATTERN_EXACT);
        }
    }
}
char *
substr(const char *str, int start)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(substring(str, start, strlen(str) + 1));
}
char *
subrstr(const char *str, int end)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(substring(str, 0, end));
}

char *
substring(const char *str, int start, int end)
{
    FUNC_ENTER(NULL);

    // Check for invalid parameters
    if (str == NULL || end < start || start < 0 || end < 0) {
        FUNC_LEAVE(NULL);
    }

    // Length of the original string
    int str_len = strlen(str);

    // Adjust end if it is beyond the length of the string
    if (end > str_len) {
        end = str_len;
    }

    // Calculate the length of the substring
    int substr_len = end - start;

    // Allocate memory for the new string (including null-terminator)
    char *substr = (char *)PDC_malloc((substr_len + 1) * sizeof(char));
    if (substr == NULL) { // Check if malloc succeeded
        FUNC_LEAVE(NULL);
    }

    // Copy the substring into the new string
    memcpy(substr, &str[start], substr_len);

    // Null-terminate the new string
    substr[substr_len] = '\0';

    FUNC_LEAVE(substr);
}

int
indexOfStr(const char *str, char *tok)
{
    FUNC_ENTER(NULL);

    const char *p = strstr(str, tok);
    if (p) {
        FUNC_LEAVE(p - str);
    }

    FUNC_LEAVE(-1);
}
int
indexOf(const char *str, char c)
{
    FUNC_ENTER(NULL);

    const char *p = strchr(str, c);
    if (p) {
        FUNC_LEAVE(p - str);
    }

    FUNC_LEAVE(-1);
}

char *
dsprintf(const char *format, ...)
{
    FUNC_ENTER(NULL);

    char *ret;
    // 1. declare argument list
    va_list args;
    // 2. starting argument list
    va_start(args, format);
    // 3. get arguments value
    int numbytes = vsnprintf((char *)NULL, 0, format, args);
    ret          = (char *)PDC_calloc((numbytes + 1), sizeof(char));

    va_start(args, format);
    vsprintf(ret, format, args);
    // 4. ending argument list
    va_end(args);

    FUNC_LEAVE(ret);
}

char *
reverse_str(char *str)
{
    FUNC_ENTER(NULL);

    if (str == NULL) {
        FUNC_LEAVE(NULL);
    }

    int   length   = strlen(str);
    char *reversed = (char *)PDC_malloc(length + 1); // +1 for the null-terminator

    if (reversed == NULL) {
        FUNC_LEAVE(NULL);
    }

    for (int i = 0; i < length; i++) {
        reversed[i] = str[length - 1 - i];
    }

    reversed[length] = '\0'; // Null-terminate the new string

    FUNC_LEAVE(reversed);
}

int
split_string(const char *str, const char *delim, char ***result, int *result_len)
{
    FUNC_ENTER(NULL);

    if (str == NULL || delim == NULL || result == NULL || result_len == NULL) {
        FUNC_LEAVE(-1);
    }

    regex_t regex;
    int     reti;

    // Compile regular expression
    reti = regcomp(&regex, delim, 0);
    if (reti) {
        LOG_ERROR("Could not compile regex\n");
        FUNC_LEAVE(-1);
    }

    const char *tmp   = str;
    int         count = 0;
    regmatch_t  pmatch[1];

    // Count matches
    while (regexec(&regex, tmp, 1, pmatch, 0) != REG_NOMATCH) {
        count++;
        tmp += pmatch[0].rm_eo;
    }

    *result_len = count + 1;
    *result     = (char **)PDC_malloc((*result_len) * sizeof(char *));
    if (!*result) {
        FUNC_LEAVE(-1);
    }

    tmp               = str; // Reset tmp
    const char *start = str;
    int         i     = 0;

    while (i < count && regexec(&regex, tmp, 1, pmatch, 0) != REG_NOMATCH) {
        int len = pmatch[0].rm_so;

        (*result)[i] = (char *)PDC_malloc((len + 1) * sizeof(char));
        if (!(*result)[i]) {
            for (int j = 0; j < i; j++) {
                (*result)[j] = (char *)PDC_free((*result)[j]);
            }
            *result = (char **)PDC_free(*result);
            *result = NULL;
            regfree(&regex);
            FUNC_LEAVE(-1);
        }

        memcpy((*result)[i], start, len);
        (*result)[i][len] = '\0';

        tmp += pmatch[0].rm_eo;
        start = tmp;
        i++;
    }

    (*result)[i] = strdup(start);
    if (!(*result)[i]) {
        for (int j = 0; j < i; j++) {
            (*result)[j] = (char *)PDC_free((*result)[j]);
        }
        *result = (char **)PDC_free(*result);
        *result = NULL;
        regfree(&regex);
        FUNC_LEAVE(-1);
    }

    regfree(&regex);

    FUNC_LEAVE(*result_len);
}

char **
gen_random_strings(int count, int minlen, int maxlen, int alphabet_size)
{
    FUNC_ENTER(NULL);

    int    c        = 0;
    int    i        = 0;
    char **result   = (char **)PDC_calloc(count, sizeof(char *));
    int    abc_size = alphabet_size > strlen(VISIBLE_ALPHABET) ? strlen(VISIBLE_ALPHABET) : alphabet_size;
    abc_size        = abc_size < 1 ? 26 : abc_size; // the minimum alphabet size is 26
    for (c = 0; c < count; c++) {
        int len   = (rand() % maxlen) + 1;
        len       = len < minlen ? minlen : len; // Ensure at least minlen character
        char *str = (char *)PDC_calloc(len + 1, sizeof(char));
        for (i = 0; i < len; i++) {
            unsigned int randnum = (unsigned int)rand();
            char         chr     = VISIBLE_ALPHABET[randnum % abc_size];
            str[i]               = chr;
        }
        str[len]  = '\0'; // Null-terminate the string
        result[c] = str;
    }

    FUNC_LEAVE(result);
}

int
is_quoted_string(char *token)
{
    FUNC_ENTER(NULL);

    if (startsWith(token, "\"") || endsWith(token, "\"")) {
        FUNC_LEAVE(1);
    }

    FUNC_LEAVE(0);
}

/**
 * remove the quotes from a string
 */
char *
stripQuotes(const char *str)
{
    FUNC_ENTER(NULL);

    if (str == NULL) {
        FUNC_LEAVE(NULL);
    }

    int len = strlen(str);
    if (len >= 2 && str[0] == '"' && str[len - 1] == '"') {
        // Call substring to remove the first and last character
        char *stripped = substring(str, 1, len - 1);
        FUNC_LEAVE(stripped);
    }
    else {
        // No quotes to strip, return a copy of the original string
        FUNC_LEAVE(strdup(str)); // strdup allocates memory for the copy
    }
}