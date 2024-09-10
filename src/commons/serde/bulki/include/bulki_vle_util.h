#ifndef BULKI_VLE_UTIL_H
#define BULKI_VLE_UTIL_H

// Include necessary headers here if needed

// Function declarations for VLE (Variable-Length Encoding) utilities can be added here
#include <stdint.h>
#include <stddef.h>

// Encode an unsigned integer using Variable-Length Encoding
size_t BULKI_vle_encode_uint(uint64_t value, uint8_t *buffer);

// Decode an unsigned integer using Variable-Length Encoding
uint64_t BULKI_vle_decode_uint(const uint8_t *buffer, size_t *bytes_read);

// quickly calculate the encoded size of an unsigned integer
size_t BULKI_vle_encoded_uint_size(uint64_t value);

// Encode a signed integer using Variable-Length Encoding
size_t BULKI_vle_encode_int(int64_t value, uint8_t *buffer);

// Decode a signed integer using Variable-Length Encoding
int64_t BULKI_vle_decode_int(const uint8_t *buffer, size_t *bytes_read);

// quickly calculate the encoded size of a signed integer
size_t BULKI_vle_encoded_int_size(int64_t value);

#endif // BULKI_VLE_UTIL_H
