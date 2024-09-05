#include "bulki_vle_util.h"

// Encode an unsigned integer using Variable-Length Encoding
size_t
BULKI_vle_encode_uint(uint64_t value, uint8_t *buffer)
{
    size_t bytes_written = 0;
    do {
        uint8_t byte = value & 0x7F;
        value >>= 7;
        if (value != 0) {
            byte |= 0x80;
        }
        buffer[bytes_written++] = byte;
    } while (value != 0);
    return bytes_written;
}

// quickly calculate the encoded size of an unsigned integer
size_t
BULKI_vle_encoded_uint_size(uint64_t value)
{
    size_t bytes_written = 1;
    // 0x80 is 128 in decimal, meaning we have more than 7 bits to encode
    while (value >= 0x80) { // this test will help us to save some computation.
        value >>= 7;
        bytes_written++;
    }
    return bytes_written;
}

// Decode an unsigned integer using Variable-Length Encoding
uint64_t
BULKI_vle_decode_uint(const uint8_t *buffer, size_t *bytes_read)
{
    uint64_t result     = 0;
    size_t   shift      = 0;
    size_t   byte_count = 0;

    while (1) {
        uint8_t byte = buffer[byte_count++];
        result |= (uint64_t)(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    if (bytes_read != NULL) {
        *bytes_read = byte_count;
    }

    return result;
}

size_t
BULKI_vle_encode_int(int64_t value, uint8_t *buffer)
{
    // ZigZag encode the signed integer
    uint64_t zigzag_encoded = (value << 1) ^ (value >> 63);
    return BULKI_vle_encode_uint(zigzag_encoded, buffer);
}

int64_t
BULKI_vle_decode_int(const uint8_t *buffer, size_t *bytes_read)
{
    uint64_t zigzag_encoded = BULKI_vle_decode_uint(buffer, bytes_read);
    return (zigzag_encoded >> 1) ^ -(zigzag_encoded & 1);
}

size_t
BULKI_vle_encoded_int_size(int64_t value)
{
    // ZigZag encode the signed integer
    uint64_t zigzag_encoded = (value << 1) ^ (value >> 63);
    return BULKI_vle_encoded_uint_size(zigzag_encoded);
}