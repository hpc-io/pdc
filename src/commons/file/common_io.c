#include "common_io.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

int
get_file_size(FILE *fp, size_t *file_size)
{

    FUNC_ENTER(NULL);

    long current = ftell(fp);
    errno        = 0;
    if ((*file_size = fseek(fp, 0, SEEK_END)) != 0) {
        LOG_ERROR("%s\n", strerror(errno));
        FUNC_LEAVE(-1);
    }
    errno = 0;
    if (fseek(fp, current, SEEK_SET) != 0) {
        LOG_ERROR("%s\n", strerror(errno));
        FUNC_LEAVE(-1);
    }

    FUNC_LEAVE(0);
}

FILE *
open_file(char *filename, char *mode)
{
    FUNC_ENTER(NULL);

    FILE *fp = fopen(filename, mode);
    if (fp == NULL) {
        LOG_ERROR("Error opening file %s: %s\n", filename, strerror(errno));
        FUNC_LEAVE(NULL);
    }

    FUNC_LEAVE(fp);
}

int
close_file(FILE *fp)
{
    FUNC_ENTER(NULL);

    if (fclose(fp) != 0) {
        LOG_ERROR("Error closing file\n");
        FUNC_LEAVE(1);
    }

    FUNC_LEAVE(0);
}

int
read_file(FILE *fp, io_buffer_t *buffer)
{
    FUNC_ENTER(NULL);

    // Determine the file size
    fseek(fp, 0L, SEEK_END);
    buffer->size = ftell(fp);
    rewind(fp);

    // Allocate memory for the buffer
    buffer->buffer = (char *)PDC_malloc(buffer->size + 1);
    if (buffer->buffer == NULL) {
        LOG_ERROR("Error allocating memory for file buffer\n");
        FUNC_LEAVE(1);
    }

    // Read the file into the buffer
    if (fread(buffer->buffer, 1, buffer->size, fp) != buffer->size) {
        LOG_ERROR("Error reading file\n");
        FUNC_LEAVE(1);
    }
    buffer->buffer[buffer->size] = '\0';

    FUNC_LEAVE(0);
}

int
write_file(FILE *fp, io_buffer_t *buffer)
{
    FUNC_ENTER(NULL);

    if (fwrite(buffer->buffer, 1, buffer->size, fp) != buffer->size) {
        LOG_ERROR("Error writing file\n");
        FUNC_LEAVE(1);
    }

    FUNC_LEAVE(0);
}

void
print_string(char *string)
{
    FUNC_ENTER(NULL);

    LOG_JUST_PRINT("%s", string);

    FUNC_LEAVE_VOID();
}

int
read_line(FILE *fp, char *buffer, size_t size)
{
    FUNC_ENTER(NULL);

    if (fgets(buffer, size, fp) == NULL) {
        LOG_ERROR("Error reading line\n");
        FUNC_LEAVE(1);
    }
    // Remove the newline character if present
    if (strchr(buffer, '\n') != NULL) {
        buffer[strcspn(buffer, "\n")] = '\0';
    }

    FUNC_LEAVE(0);
}

int
get_input(char *buffer, size_t size)
{
    FUNC_ENTER(NULL);

    if (fgets(buffer, size, stdin) == NULL) {
        LOG_ERROR("Error getting input\n");
        FUNC_LEAVE(1);
    }
    // Remove the newline character if present
    if (strchr(buffer, '\n') != NULL) {
        buffer[strcspn(buffer, "\n")] = '\0';
    }

    FUNC_LEAVE(0);
}

void
print_error(char *message)
{
    FUNC_ENTER(NULL);

    LOG_ERROR("Error: %s\n", message);

    FUNC_LEAVE_VOID();
}

int
read_text_file(char *filename, void (*callback)(char *line))
{
    FUNC_ENTER(NULL);

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
        LOG_ERROR("Error reading file\n");
        FUNC_LEAVE(1);
    }
    line = (char *)PDC_free(line);
    close_file(fp);

    FUNC_LEAVE(0);
}

int
write_text_file(char *filename, char **lines, size_t num_lines)
{
    FUNC_ENTER(NULL);

    FILE *fp = open_file(filename, IO_MODE_WRITE);
    for (size_t i = 0; i < num_lines; i++) {
        if (fprintf(fp, "%s\n", lines[i]) < 0) {
            LOG_ERROR("Error writing to file\n");
            close_file(fp);
            FUNC_LEAVE(1);
        }
    }
    close_file(fp);

    FUNC_LEAVE(0);
}

int
read_binary_file(char *filename, void *buffer, size_t size)
{
    FUNC_ENTER(NULL);

    FILE *fp = open_file(filename, IO_MODE_BINARY IO_MODE_READ);
    if (fread(buffer, 1, size, fp) != size) {
        LOG_ERROR("Error reading file\n");
        close_file(fp);
        FUNC_LEAVE(1);
    }
    close_file(fp);

    FUNC_LEAVE(0);
}

int
write_binary_file(char *filename, void *buffer, size_t size)
{
    FUNC_ENTER(NULL);

    FILE *fp = open_file(filename, IO_MODE_BINARY IO_MODE_WRITE);
    if (fwrite(buffer, 1, size, fp) != size) {
        LOG_ERROR("Error writing file\n");
        close_file(fp);
        FUNC_LEAVE(1);
    }
    close_file(fp);

    FUNC_LEAVE(0);
}

int
update_binary_file(char *filename, void *buffer, size_t size, unsigned long start_pos, size_t length)
{
    FUNC_ENTER(NULL);

    FILE *fp = open_file(filename, IO_MODE_BINARY IO_MODE_READ IO_MODE_WRITE);
    if (fseek(fp, start_pos, SEEK_SET) != 0) {
        LOG_ERROR("Error seeking to starting position\n");
        close_file(fp);
        FUNC_LEAVE(1);
    }
    if (fwrite(buffer, 1, size, fp) != size) {
        LOG_ERROR("Error writing to file\n");
        close_file(fp);
        FUNC_LEAVE(1);
    }
    if (length != size && ftruncate(fileno(fp), start_pos + size) != 0) {
        LOG_ERROR("Error truncating file\n");
        close_file(fp);
        FUNC_LEAVE(1);
    }
    close_file(fp);

    FUNC_LEAVE(0);
}