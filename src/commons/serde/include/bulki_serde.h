#ifndef BULKI_SERDE_H
#define BULKI_SERDE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include "pdc_generic.h"
#include "bulki.h"

#define MAX_KEYS        10
#define MAX_BUFFER_SIZE 1000

/**
 * @brief Serialize the data in the serialized data structure and return the buffer
 *
 * @param data Pointer to the BULKI structure
 *
 * @return Pointer to the buffer containing the serialized data
 */
void *BULKI_serialize(BULKI *bulki);

/**
 * @brief Deserialize the buffer and return the deserialized data structure
 *
 * @param buffer Pointer to the buffer containing the serialized data
 *
 * @return Pointer to the deserialized BULKI structure
 */
BULKI *BULKI_deserialize(void *buffer);

#endif /* BULKI_SERDE_H */