//
// Created by Wei Zhang on 7/12/17.
//

#include "string_utils.h"

int
startsWith(const char *str, const char *pre)
{
    char *found = strstr(str, pre);
    return (found && (found - str) == 0);
}

int
endsWith(const char *str, const char *suf)
{
    size_t lensuf = strlen(suf), lenstr = strlen(str);
    return lenstr < lensuf ? 0 : (strncmp(str + lenstr - lensuf, suf, lensuf) == 0);
}

int
contains(const char *str, const char *tok)
{
    return strstr(str, tok) != NULL;
}

int
equals(const char *str, const char *tok)
{
    return strcmp(tok, str) == 0;
}

int
simple_matches(const char *str, const char *pattern)
{
    int result = 0;
    // Ensure both str and pattern cannot be empty.
    if (str == NULL || pattern == NULL) {
        return result;
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
        // free(tok);
    }
    return result;
}

pattern_type_t
determine_pattern_type(const char *pattern)
{

    if (startsWith(pattern, "*")) {
        if (endsWith(pattern, "*")) {
            return PATTERN_MIDDLE;
        }
        else {
            return PATTERN_SUFFIX;
        }
    }
    else {
        if (endsWith(pattern, "*")) {
            return PATTERN_PREFIX;
        }
        else {
            return PATTERN_EXACT;
        }
    }
}
char *
substr(const char *str, int start)
{
    return substring(str, start, -1);
}
char *
subrstr(const char *str, int end)
{
    return substring(str, 0, end);
}

char *
substring(const char *str, int start, int end)
{
    // Check for invalid parameters
    if (str == NULL || end < start || start < 0 || end < 0) {
        return NULL;
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
    char *substr = (char *)malloc((substr_len + 1) * sizeof(char));
    if (substr == NULL) { // Check if malloc succeeded
        return NULL;
    }

    // Copy the substring into the new string
    memcpy(substr, &str[start], substr_len);

    // Null-terminate the new string
    substr[substr_len] = '\0';

    return substr;
}

int
indexOfStr(const char *str, char *tok)
{
    const char *p = strstr(str, tok);
    if (p) {
        return p - str;
    }
    return -1;
}
int
indexOf(const char *str, char c)
{
    const char *p = strchr(str, c);
    if (p) {
        return p - str;
    }
    return -1;
}

char *
dsprintf(const char *format, ...)
{
    char *ret;
    // 1. declare argument list
    va_list args;
    // 2. starting argument list
    va_start(args, format);
    // 3. get arguments value
    int numbytes = vsnprintf((char *)NULL, 0, format, args);
    ret          = (char *)calloc((numbytes + 1), sizeof(char));

    va_start(args, format);
    vsprintf(ret, format, args);
    // 4. ending argument list
    va_end(args);
    return ret;
}

void
println(const char *format, ...)
{
    // 1. declare argument list
    va_list args;
    // 2. starting argument list
    va_start(args, format);
    // 3. get arguments value
    vfprintf(stdout, format, args);
    // 4. ending argument list
    va_end(args);
    fputc('\n', stdout);
    fflush(stdout);
}

void
stderr_println(const char *format, ...)
{
    // 1. declare argument list
    va_list args;
    // 2. starting argument list
    va_start(args, format);
    // 3. get arguments value
    vfprintf(stderr, format, args);
    // 4. ending argument list
    va_end(args);
    fputc('\n', stderr);
    fflush(stderr);
}

char *
reverse_str(char *str)
{
    if (str == NULL) {
        return NULL;
    }

    int   length   = strlen(str);
    char *reversed = (char *)malloc(length + 1); // +1 for the null-terminator

    if (reversed == NULL) {
        return NULL; // Return NULL if memory allocation fails
    }

    for (int i = 0; i < length; i++) {
        reversed[i] = str[length - 1 - i];
    }

    reversed[length] = '\0'; // Null-terminate the new string

    return reversed;
}