
/* File foo.  */
#ifndef BIN_FILE_OPS_H
#define BIN_FILE_OPS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

void bin_append_int(int data, FILE *stream);

void bin_append_double(double data, FILE *stream);

void bin_append_string(char *data, FILE *stream);

void bin_append_string_with_len(char *data, size_t len, FILE *stream);

void bin_append_uint64(uint64_t data, FILE *stream);

void bin_append_size_t(size_t data, FILE *stream);

void bin_append_type(int type, FILE *stream);

int *bin_read_int(FILE *file);

double *bin_read_double(FILE *file);

void *bin_read_index_numeric_value(int *is_float, FILE *file);

char *bin_read_string(FILE *file);

uint64_t *bin_read_uint64(FILE *file);

size_t *bin_read_size_t(FILE *file);

size_t bin_skip_field(FILE *stream);

#endif /* !BIN_FILE_OPS_H */
