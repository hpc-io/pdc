#ifndef COMMON_IO_H
#define COMMON_IO_H

#include <stdio.h>

// Constants for input/output modes
#define IO_MODE_READ   "r"
#define IO_MODE_WRITE  "w"
#define IO_MODE_APPEND "a"
#define IO_MODE_BINARY "b"

// Data structures for input/output
typedef struct {
    char * buffer;
    size_t size;
} io_buffer_t;

// File handling functions
FILE *open_file(char *filename, char *mode);
int   close_file(FILE *fp);

// Input/output functions
int  read_file(FILE *fp, io_buffer_t *buffer);
int  write_file(FILE *fp, io_buffer_t *buffer);
void print_string(char *string);
int  read_line(FILE *fp, char *buffer, size_t size);
int  get_input(char *buffer, size_t size);
void print_error(char *message);

// Text file handling functions
int read_text_file(char *filename, void (*callback)(char *line));
int write_text_file(char *filename, char **lines, size_t num_lines);

// Binary file handling functions
int read_binary_file(char *filename, void *buffer, size_t size);
int write_binary_file(char *filename, void *buffer, size_t size);
int update_binary_file(char *filename, void *buffer, size_t size, long start_pos, long length);

#endif /* COMMON_IO_H */