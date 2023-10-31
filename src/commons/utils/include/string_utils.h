//
// Created by Wei Zhang on 7/12/17.
//

#ifndef PDC_STRING_UTILS_H
#define PDC_STRING_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

// #define PATTERN_EXACT  0
// #define PATTERN_SUFFIX 1
// #define PATTERN_PREFIX 2
// #define PATTERN_MIDDLE 3

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"

typedef enum { PATTERN_EXACT = 2, PATTERN_PREFIX = 3, PATTERN_SUFFIX = 4, PATTERN_MIDDLE = 5 } pattern_type_t;

/**
 * take the part starting from the start position
 * you need to free str after use.
 * @param str
 * @param start
 * @return
 */
char *substr(const char *str, int start);
/**
 * take the part before end position
 * you need to free str after use.
 * @param str
 * @param end
 * @return
 */
char *subrstr(const char *str, int end);
/**
 * take the substring starting from start, ending at end-1
 * you need to free str after use.
 * @param str
 * @param start
 * @param end
 * @return
 */
char *substring(const char *str, int start, int end);
/**
 * determine the pattern type.
 * Currently, only support for different types:
 *
 * PREFIX : ab*
 * SUFFIX : *ab
 * MIDDLE : *ab*
 * EXACT  : ab
 *
 * @param pattern
 * @return
 */
pattern_type_t determine_pattern_type(const char *pattern);
/**
 * return the index of token tok in given string str.
 * if not found, return -1
 *
 * @param str
 * @param tok
 * @return
 */
int indexOfStr(const char *str, char *tok);
/**
 * return the index of character c in given string str.
 * if not found, return -1
 *
 * @param str
 * @param c
 * @return
 */
int indexOf(const char *str, char c);

/**
 * to determine if a string str starts with prefix pre
 * @param pre
 * @param str
 * @return
 */
int startsWith(const char *str, const char *pre);
/**
 * to determine if a string str ends with suffix suf
 * @param suf
 * @param str
 * @return
 */
int endsWith(const char *str, const char *suf);
/**
 * to determine if a string str contains token tok
 * @param tok
 * @param str
 * @return
 */
int contains(const char *str, const char *tok);
/**
 * to determine if a string str exactly matches a token tok.
 * @param tok
 * @param str
 * @return
 */
int equals(const char *str, const char *tok);

/**
 * dynamically generate string for you according to the format you pass.
 * Since usually, it is hard to predict how much memory should be allocated
 * before generating an arbitrary string.
 *
 * remember to free the generated string after use.
 *
 * @param format
 * @param ...
 * @return
 */
char *dsprintf(const char *format, ...);

/**
 * Print anything on stdout.
 * Always put a line feed after the string you want to print.
 *
 * @param format
 * @param ...
 */
void println(const char *format, ...);

/**
 * Print anything on stderr.
 * Always put a line feed after the string you want to print.
 *
 * @param format
 * @param ...
 */
void stderr_println(const char *format, ...);
/**
 * Only support expressions like:
 *
 * *ab
 * ab*
 * *ab*
 * ab
 *
 * @param str
 * @param pattern
 * @return 1 if matches, 0 if not.
 */
int simple_matches(const char *str, const char *pattern);

/**
 * get the reverse of a given string.
 * @param str
 * @return a reversed string
 * @note remember to free the reversed string after use.
 */
char *reverse_str(char *str);

/**
 * split a string into several parts according to the delimiter.
 * @param str the string to be split
 * @param delim the delimiter, which is a regular expression.
 * @param result the result array
 * @param result_len the length of the result array
 * @return the number of parts, if -1, means error.
 */
int split_string(const char *str, const char *delim, char ***result, int *result_len);

/**
 * Generate given number of random strings with given length.
 * You may need to seed the random number generator before calling this function.
 * The minimum size of alphabet is 26, accordingly, that alphabet will be [a-z].
 * If you would like to have [a-z0-9] , alphabet_size should be 36.
 * If you would like to have [a-z0-9!@#$%^&*()-_=+[]{}|;:'",.<>?/`~], alphabet_size should be 67.
 * If you would like to have [a-z0-9!@#$%^&*()-_=+[]{}|;:'",.<>?/`~A-Z], alphabet_size should be 93.
 * @param count the number of strings to be generated
 * @param minlen the minimum length of the string
 * @param maxlen the maximum length of the string
 * @param alphabet_size the size of the alphabet
 * @return an array of strings
 */
char **gen_random_strings(int count, int minlen, int maxlen, int alphabet_size);

#endif // PDC_STRING_UTILS_H
