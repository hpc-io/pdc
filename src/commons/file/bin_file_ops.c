#include "bin_file_ops.h"

// type 1 int, 2 double, 3 string, 4 uint64, 5 size_t

void
miqs_append_int(int data, FILE *stream)
{
    int    type   = 1;
    size_t length = 1;
    fwrite(&type, sizeof(int), 1, stream);
    fwrite(&length, sizeof(size_t), 1, stream);
    fwrite(&data, sizeof(int), length, stream);
}

void
miqs_append_double(double data, FILE *stream)
{
    int    type   = 2;
    size_t length = 1;
    fwrite(&type, sizeof(int), 1, stream);
    fwrite(&length, sizeof(size_t), 1, stream);
    fwrite(&data, sizeof(double), length, stream);
}

void
miqs_append_string(char *data, FILE *stream)
{
    size_t length = strlen(data);
    miqs_append_string_with_len(data, length, stream);
}

void
miqs_append_string_with_len(char *data, size_t len, FILE *stream)
{
    int type = 3;
    fwrite(&type, sizeof(int), 1, stream);
    fwrite(&len, sizeof(size_t), 1, stream);
    fwrite(data, sizeof(char), len, stream);
}

void
miqs_append_uint64(uint64_t data, FILE *stream)
{
    int    type   = 4;
    size_t length = 1;
    fwrite(&type, sizeof(int), 1, stream);
    fwrite(&length, sizeof(size_t), 1, stream);
    fwrite(&data, sizeof(uint64_t), length, stream);
}

void
miqs_append_size_t(size_t data, FILE *stream)
{
    int    type   = 5;
    size_t length = 1;
    fwrite(&type, sizeof(int), 1, stream);
    fwrite(&length, sizeof(size_t), 1, stream);
    fwrite(&data, sizeof(size_t), length, stream);
}

void
miqs_read_general(int *t, size_t *len, void **data, FILE *stream)
{
    int    type   = -1;
    size_t length = 0;
    fread(&type, sizeof(int), 1, stream);
    fread(&length, sizeof(size_t), 1, stream);
    void *_data;
    if (type == 1) {
        _data = (int *)calloc(length, sizeof(int));
        fread(_data, sizeof(int), length, stream);
    }
    else if (type == 2) {
        _data = (double *)calloc(length, sizeof(double));
        fread(_data, sizeof(double), length, stream);
    }
    else if (type == 3) {
        _data = (char *)calloc(length + 1, sizeof(char));
        fread(_data, sizeof(char), length, stream);
    }
    else if (type == 4) {
        _data = (uint64_t *)calloc(length, sizeof(uint64_t));
        fread(_data, sizeof(uint64_t), length, stream);
    }
    else if (type == 5) {
        _data = (size_t *)calloc(length, sizeof(size_t));
        fread(_data, sizeof(size_t), length, stream);
    }
    data[0] = (void *)_data;
    *t      = type;
    *len    = length;
}

size_t
miqs_skip_field(FILE *stream)
{
    size_t rst    = 0;
    int    type   = -1;
    size_t length = 0;
    fread(&type, sizeof(int), 1, stream);
    if (type == EOF) {
        return rst; // end of file, nothing to skip
    }
    rst += sizeof(int);
    fread(&length, sizeof(size_t), 1, stream);
    rst += sizeof(size_t);
    void *_data;
    if (type == 1) {
        _data = (int *)calloc(length, sizeof(int));
        fread(_data, sizeof(int), length, stream);
        rst += sizeof(int) * length;
    }
    else if (type == 2) {
        _data = (double *)calloc(length, sizeof(double));
        fread(_data, sizeof(double), length, stream);
        rst += sizeof(double) * length;
    }
    else if (type == 3) {
        _data = (char *)calloc(length + 1, sizeof(char));
        fread(_data, sizeof(char), length, stream);
        rst += sizeof(char) * length;
    }
    else if (type == 4) {
        _data = (uint64_t *)calloc(length, sizeof(uint64_t));
        fread(_data, sizeof(uint64_t), length, stream);
        rst += sizeof(uint64_t) * length;
    }
    else if (type == 5) {
        _data = (size_t *)calloc(length, sizeof(size_t));
        fread(_data, sizeof(size_t), length, stream);
        rst += sizeof(size_t) * length;
    }
    free(_data);
    return rst;
}

void *
miqs_read_index_numeric_value(int *is_float, FILE *file)
{
    int    type = 1;
    size_t len  = 1;
    void **data = (void **)calloc(1, sizeof(void *));
    miqs_read_general(&type, &len, data, file);
    if (len == 1) {
        if (type == 1) {
            *is_float = 0;
        }
        else if (type == 2) {
            *is_float = 1;
        }
    }
    return *data;
}

int *
miqs_read_int(FILE *file)
{
    int    type = 1;
    size_t len  = 1;
    void **data = (void **)calloc(1, sizeof(void *));
    miqs_read_general(&type, &len, data, file);
    if (type == 1 && len == 1) {
        return (int *)*data;
    }
    return NULL;
}

double *
miqs_read_double(FILE *file)
{
    int    type = 2;
    size_t len  = 1;
    void **data = (void **)calloc(1, sizeof(void *));
    miqs_read_general(&type, &len, data, file);
    if (type == 2 && len == 1) {
        return (double *)*data;
    }
    return NULL;
}

char *
miqs_read_string(FILE *file)
{
    int    type = 3;
    size_t len  = 1;
    void **data = (void **)calloc(1, sizeof(void *));
    miqs_read_general(&type, &len, data, file);
    if (type == 3) {
        return (char *)*data;
    }
    return NULL;
}

uint64_t *
miqs_read_uint64(FILE *file)
{
    int    type = 4;
    size_t len  = 1;
    void **data = (void **)calloc(1, sizeof(void *));
    miqs_read_general(&type, &len, data, file);
    if (type == 4 && len == 1) {
        return (uint64_t *)*data;
    }
    return NULL;
}

size_t *
miqs_read_size_t(FILE *file)
{
    int    type = 5;
    size_t len  = 1;
    void **data = (void **)calloc(1, sizeof(void *));
    miqs_read_general(&type, &len, data, file);
    if (type == 5 && len == 1) {
        return (size_t *)*data;
    }
    return NULL;
}

// type: 1, int, 2, float, 3. string, 4. uint64 5. size_t
void
miqs_append_type(int type, FILE *stream)
{
    miqs_append_int(type, stream);
}
