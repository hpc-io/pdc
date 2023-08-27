#include "pdc_compare.h"

#include <ctype.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

/* Comparison functions for a pointer to an integer */

int
int_equal(void *vlocation1, void *vlocation2)
{
    int *location1;
    int *location2;

    location1 = (int *)vlocation1;
    location2 = (int *)vlocation2;

    return *location1 == *location2;
}

int
int_compare(void *vlocation1, void *vlocation2)
{
    int *location1;
    int *location2;

    location1 = (int *)vlocation1;
    location2 = (int *)vlocation2;

    if (*location1 < *location2) {
        return -1;
    }
    else if (*location1 > *location2) {
        return 1;
    }
    else {
        return 0;
    }
}

int
ui64_equal(void *vlocation1, void *vlocation2)
{
    uint64_t *location1;
    uint64_t *location2;

    location1 = (uint64_t *)vlocation1;
    location2 = (uint64_t *)vlocation2;

    return *location1 == *location2;
}

int
ui64_compare(void *vlocation1, void *vlocation2)
{
    uint64_t *location1;
    uint64_t *location2;

    location1 = (uint64_t *)vlocation1;
    location2 = (uint64_t *)vlocation2;

    if (*location1 < *location2) {
        return -1;
    }
    else if (*location1 > *location2) {
        return 1;
    }
    else {
        return 0;
    }
}

/* Comparison functions for a generic void pointer */

int
pointer_equal(void *location1, void *location2)
{
    return location1 == location2;
}

int
pointer_compare(void *location1, void *location2)
{
    if (location1 < location2) {
        return -1;
    }
    else if (location1 > location2) {
        return 1;
    }
    else {
        return 0;
    }
}

/* Comparison functions for strings */

int
string_equal(void *string1, void *string2)
{
    return strcmp((char *)string1, (char *)string2) == 0;
}

int
string_compare(void *string1, void *string2)
{
    int result;

    result = strcmp((char *)string1, (char *)string2);

    if (result < 0) {
        return -1;
    }
    else if (result > 0) {
        return 1;
    }
    else {
        return 0;
    }
}

/* Comparison functions for strings, which ignore the case of letters. */

int
string_nocase_equal(void *string1, void *string2)
{
    return string_nocase_compare((char *)string1, (char *)string2) == 0;
}

/* On many systems, strcasecmp or stricmp will give the same functionality
 * as this function.  However, it is non-standard and cannot be relied
 * on to be present. */

int
string_nocase_compare(void *string1, void *string2)
{
    char *p1;
    char *p2;
    int   c1, c2;

    /* Iterate over each character in the strings */

    p1 = (char *)string1;
    p2 = (char *)string2;

    for (;;) {

        c1 = tolower(*p1);
        c2 = tolower(*p2);

        if (c1 != c2) {

            /* Strings are different */

            if (c1 < c2) {
                return -1;
            }
            else {
                return 1;
            }
        }

        /* End of string */

        if (c1 == '\0')
            break;

        /* Advance to the next character */

        ++p1;
        ++p2;
    }

    /* Reached the end of string and no difference found */

    return 0;
}