#ifndef BULKI_ENDIANNESS_UTIL_H
#define BULKI_ENDIANNESS_UTIL_H

#include <stdint.h>

// Function to check if the system is little-endian
int BULKI_is_system_le();

// Function to check if the system is big-endian
int BULKI_is_system_be();

// Functions to convert from big-endian to little-endian
uint8_t  BULKI_to_little_endian_u8(uint8_t value);
uint16_t BULKI_to_little_endian_u16(uint16_t value);
uint32_t BULKI_to_little_endian_u32(uint32_t value);
uint64_t BULKI_to_little_endian_u64(uint64_t value);
int8_t   BULKI_to_little_endian_i8(int8_t value);
int16_t  BULKI_to_little_endian_i16(int16_t value);
int32_t  BULKI_to_little_endian_i32(int32_t value);
int64_t  BULKI_to_little_endian_i64(int64_t value);
float    BULKI_to_little_endian_f32(float value);
double   BULKI_to_little_endian_f64(double value);
int      BULKI_to_little_endian_int(int value);
long     BULKI_to_little_endian_long(long value);
short    BULKI_to_little_endian_short(short value);
char     BULKI_to_little_endian_char(char value);

#endif