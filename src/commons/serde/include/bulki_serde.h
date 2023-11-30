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
 * @brief Serialize a BULKI_Entity structure to a buffer
 *
 * @param entity Pointer to the BULKI_Entity structure
 * @param buffer Pointer to the buffer
 * @param offset Pointer to the offset
 *
 * @return Pointer to the buffer
 */
void *BULKI_Entity_serialize_to_buffer(BULKI_Entity *entity, void *buffer, size_t *offset);

/**
 * @brief Serialize a BULKI structure to a buffer
 *
 * @param bulki Pointer to the BULKI structure
 * @param buffer Pointer to the buffer
 * @param offset Pointer to the offset
 *
 * @return Pointer to the buffer
 */
void *BULKI_serialize_to_buffer(BULKI *bulki, void *buffer, size_t *offset);

/**
 * @brief Serialize a BULKI_Entity structure to a buffer
 *
 * @param entity Pointer to the BULKI_Entity structure
 *
 * @return Pointer to the buffer
 */
void *BULKI_Entity_serialize(BULKI_Entity *entity);

/**
 * @brief Serialize a BULKI structure to a buffer
 *
 * @param data Pointer to the BULKI structure
 *
 * @return Pointer to the buffer
 */
void *BULKI_serialize(BULKI *data);

/********************** Deserialize ************************** */

/**
 * @brief Deserialize a BULKI_Entity structure from a buffer
 *
 * @param buffer Pointer to the buffer
 * @param offset Pointer to the offset
 *
 * @return Pointer to the BULKI_Entity structure
 */
BULKI *BULKI_deserialize_from_buffer(void *buffer, size_t *offset);

/**
 * @brief Deserialize a BULKI structure from a buffer
 *
 * @param buffer Pointer to the buffer
 * @param offset Pointer to the offset
 *
 * @return Pointer to the BULKI structure
 */
BULKI_Entity *BULKI_Entity_deserialize_from_buffer(void *buffer, size_t *offset);

#endif /* BULKI_SERDE_H */