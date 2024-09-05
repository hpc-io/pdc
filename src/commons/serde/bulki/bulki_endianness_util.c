#include "bulki_endianness_util.h"

int
BULKI_is_system_le()
{
    uint16_t test = 0x1;
    uint8_t *byte = (uint8_t *)&test;
    return byte[0] == 0x1;
}

int
BULKI_is_system_be()
{
    return !BULKI_is_system_le();
}

uint8_t
BULKI_to_little_endian_u8(uint8_t value)
{
    return value; // 8-bit values don't need conversion
}

uint16_t
BULKI_to_little_endian_u16(uint16_t value)
{
    if (BULKI_is_system_le()) {
        return value;
    }
    else {
        return ((value & 0xFF00) >> 8) | ((value & 0x00FF) << 8);
    }
}

uint32_t
BULKI_to_little_endian_u32(uint32_t value)
{
    if (BULKI_is_system_le()) {
        return value;
    }
    else {
        return ((value & 0xFF000000) >> 24) | ((value & 0x00FF0000) >> 8) | ((value & 0x0000FF00) << 8) |
               ((value & 0x000000FF) << 24);
    }
}

uint64_t
BULKI_to_little_endian_u64(uint64_t value)
{
    if (BULKI_is_system_le()) {
        return value;
    }
    else {
        return ((value & 0xFF00000000000000ULL) >> 56) | ((value & 0x00FF000000000000ULL) >> 40) |
               ((value & 0x0000FF0000000000ULL) >> 24) | ((value & 0x000000FF00000000ULL) >> 8) |
               ((value & 0x00000000FF000000ULL) << 8) | ((value & 0x0000000000FF0000ULL) << 24) |
               ((value & 0x000000000000FF00ULL) << 40) | ((value & 0x00000000000000FFULL) << 56);
    }
}

int8_t
BULKI_to_little_endian_i8(int8_t value)
{
    return value; // 8-bit values don't need conversion
}

int16_t
BULKI_to_little_endian_i16(int16_t value)
{
    return (int16_t)BULKI_to_little_endian_u16((uint16_t)value);
}

int32_t
BULKI_to_little_endian_i32(int32_t value)
{
    return (int32_t)BULKI_to_little_endian_u32((uint32_t)value);
}

int64_t
BULKI_to_little_endian_i64(int64_t value)
{
    return (int64_t)BULKI_to_little_endian_u64((uint64_t)value);
}

float
BULKI_to_little_endian_f32(float value)
{
    union {
        float    f;
        uint32_t i;
    } u;
    u.f = value;
    u.i = BULKI_to_little_endian_u32(u.i);
    return u.f;
}

double
BULKI_to_little_endian_f64(double value)
{
    union {
        double   d;
        uint64_t i;
    } u;
    u.d = value;
    u.i = BULKI_to_little_endian_u64(u.i);
    return u.d;
}

int
BULKI_to_little_endian_int(int value)
{
    return (int)BULKI_to_little_endian_u32((uint32_t)value);
}

long
BULKI_to_little_endian_long(long value)
{
    if (sizeof(long) == 4) {
        return (long)BULKI_to_little_endian_u32((uint32_t)value);
    }
    else if (sizeof(long) == 8) {
        return (long)BULKI_to_little_endian_u64((uint64_t)value);
    }
    else {
        // Handle unexpected long size
        return value;
    }
}

short
BULKI_to_little_endian_short(short value)
{
    return (short)BULKI_to_little_endian_u16((uint16_t)value);
}

char
BULKI_to_little_endian_char(char value)
{
    return value; // char is typically 8-bit, so no conversion needed
}