#include "common_io.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>

FILE *
open_file(char *filename, char *mode)
{
    FILE *fp = fopen(filename, mode);
    if (fp == NULL) {
        fprintf(stderr, "Error opening file %s: %s\n", filename, strerror(errno));
        exit(1);
    }
    return fp;
}

int
close_file(FILE *fp)
{
    if (fclose(fp) != 0) {
        fprintf(stderr, "Error closing file\n");
        return 1;
    }
    return 0;
}

int
read_file(FILE *fp, io_buffer_t *buffer)
{
    // Determine the file size
    fseek(fp, 0L, SEEK_END);
    buffer->size = ftell(fp);
    rewind(fp);

    // Allocate memory for the buffer
    buffer->buffer = (char *)malloc(buffer->size + 1);
    if (buffer->buffer == NULL) {
        fprintf(stderr, "Error allocating memory for file buffer\n");
        return 1;
    }

    // Read the file into the buffer
    if (fread(buffer->buffer, 1, buffer->size, fp) != buffer->size) {
        fprintf(stderr, "Error reading file\n");
        return 1;
    }
    buffer->buffer[buffer->size] = '\0';

    return 0;
}

int
write_file(FILE *fp, io_buffer_t *buffer)
{
    if (fwrite(buffer->buffer, 1, buffer->size, fp) != buffer->size) {
        fprintf(stderr, "Error writing file\n");
        return 1;
    }
    return 0;
}

void
print_string(char *string)
{
    printf("%s", string);
}

int
read_line(FILE *fp, char *buffer, size_t size)
{
    if (fgets(buffer, size, fp) == NULL) {
        fprintf(stderr, "Error reading line\n");
        return 1;
    }
    // Remove the newline character if present
    if (strchr(buffer, '\n') != NULL) {
        buffer[strcspn(buffer, "\n")] = '\0';
    }
    return 0;
}

int
get_input(char *buffer, size_t size)
{
    if (fgets(buffer, size, stdin) == NULL) {
        fprintf(stderr, "Error getting input\n");
        return 1;
    }
    // Remove the newline character if present
    if (strchr(buffer, '\n') != NULL) {
        buffer[strcspn(buffer, "\n")] = '\0';
    }
    return 0;
}

void
print_error(char *message)
{
    fprintf(stderr, "Error: %s\n", message);
}

int
read_text_file(char *filename, void (*callback)(char *line))
{
    FILE *  fp   = open_file(filename, IO_MODE_READ);
    char *  line = NULL;
    size_t  len  = 0;
    ssize_t read;
    while ((read = getline(&line, &len, fp)) != -1) {
        if (line[read - 1] == '\n') {
            line[read - 1] = '\0';
        }
        callback(line);
    }
    if (ferror(fp)) {
        fprintf(stderr, "Error reading file\n");
        return 1;
    }
    free(line);
    close_file(fp);
    return 0;
}

int
write_text_file(char *filename, char **lines, size_t num_lines)
{
    FILE *fp = open_file(filename, IO_MODE_WRITE);
    for (size_t i = 0; i < num_lines; i++) {
        if (fprintf(fp, "%s\n", lines[i]) < 0) {
            fprintf(stderr, "Error writing to file\n");
            close_file(fp);
            return 1;
        }
    }
    close_file(fp);
    return 0;
}

int
read_binary_file(char *filename, void *buffer, size_t size)
{
    FILE *fp = open_file(filename, IO_MODE_BINARY IO_MODE_READ);
    if (fread(buffer, 1, size, fp) != size) {
        fprintf(stderr, "Error reading file\n");
        close_file(fp);
        return 1;
    }
    close_file(fp);
    return 0;
}

int
write_binary_file(char *filename, void *buffer, size_t size)
{
    FILE *fp = open_file(filename, IO_MODE_BINARY IO_MODE_WRITE);
    if (fwrite(buffer, 1, size, fp) != size) {
        fprintf(stderr, "Error writing file\n");
        close_file(fp);
        return 1;
    }
    close_file(fp);
    return 0;
}

int
update_binary_file(char *filename, void *buffer, size_t size, long start_pos, long length)
{
    FILE *fp = open_file(filename, IO_MODE_BINARY IO_MODE_READ IO_MODE_WRITE);
    if (fseek(fp, start_pos, SEEK_SET) != 0) {
        fprintf(stderr, "Error seeking to starting position\n");
        close_file(fp);
        return 1;
    }
    if (fwrite(buffer, 1, size, fp) != size) {
        fprintf(stderr, "Error writing to file\n");
        close_file(fp);
        return 1;
    }
    if (length != size && ftruncate(fileno(fp), start_pos + size) != 0) {
        fprintf(stderr, "Error truncating file\n");
        close_file(fp);
        return 1;
    }
    close_file(fp);
    return 0;
}