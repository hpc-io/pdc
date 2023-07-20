/**
 * @file pdc_hash.h
 *
 * Hash function for integer, pointer, and string.  See @ref xxx_hash.
 */

#ifndef PDC_HASH_H
#define PDC_HASH_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Generate a hash key for a pointer to an integer.  The value pointed
 * at is used to generate the key.
 *
 * @param location        The pointer.
 * @return                A hash key for the value at the location.
 */

unsigned int int_hash(void *location);

/**
 * Generate a hash key for a pointer to an uint64_t integer.  The value pointed
 * at is used to generate the key.
 *
 * @param location        The pointer.
 * @return                A hash key for the value at the location.
 */
unsigned int ui64_hash(void *location);

/**
 * Generate a hash key for a pointer.  The value pointed at by the pointer
 * is not used, only the pointer itself.
 *
 * @param location        The pointer
 * @return                A hash key for the pointer.
 */

unsigned int pointer_hash(void *location);

/**
 * Generate a hash key from a string.
 *
 * @param string           The string.
 * @return                 A hash key for the string.
 */

unsigned int string_hash(void *string);

/**
 * Generate a hash key from a string, ignoring the case of letters.
 *
 * @param string           The string.
 * @return                 A hash key for the string.
 */

unsigned int string_nocase_hash(void *string);

#ifdef __cplusplus
}
#endif

#endif /* #ifndef PDC_HASH_H */