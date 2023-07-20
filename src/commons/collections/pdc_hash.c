#include "pdc_hash.h"
#include <limits.h>
#include <ctype.h>
#include <inttypes.h>

/* Hash function for a pointer to an integer */

unsigned int
int_hash(void *vlocation)
{
    int *location;

    location = (int *)vlocation;

    return (unsigned int)*location;
}

/* Hash function for a pointer to a uint64_t integer */

unsigned int
ui64_hash(void *vlocation)
{
    uint64_t *location;

    location = (uint64_t *)vlocation;

    return (unsigned int)*location;
}

/* Hash function for a generic pointer */

unsigned int
pointer_hash(void *location)
{
    return (unsigned int)(unsigned long)location;
}

/* String hash function */

unsigned int
string_hash(void *string)
{
    /* This is the djb2 string hash function */

    unsigned int   result = 5381;
    unsigned char *p;

    p = (unsigned char *)string;

    while (*p != '\0') {
        result = (result << 5) + result + *p;
        ++p;
    }

    return result;
}

/* The same function, with a tolower on every character so that
 * case is ignored.  This code is duplicated for performance. */

unsigned int
string_nocase_hash(void *string)
{
    unsigned int   result = 5381;
    unsigned char *p;

    p = (unsigned char *)string;

    while (*p != '\0') {
        result = (result << 5) + result + (unsigned int)tolower(*p);
        ++p;
    }

    return result;
}