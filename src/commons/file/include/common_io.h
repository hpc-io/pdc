#ifndef COMMON_IO_H
#define COMMON_IO_H

#include <stdio.h>

/**
 * \def IO_MODE_READ
 * \brief Constant for read mode.
 */
#define IO_MODE_READ "r"

/**
 * \def IO_MODE_WRITE
 * \brief Constant for write mode.
 */
#define IO_MODE_WRITE "w"

/**
 * \def IO_MODE_APPEND
 * \brief Constant for append mode.
 */
#define IO_MODE_APPEND "a"

/**
 * \def IO_MODE_BINARY
 * \brief Constant for binary mode.
 */
#define IO_MODE_BINARY "b"

/**
 * \struct io_buffer_t
 * \brief Data structure for input/output operations.
 */
typedef struct {
    char * buffer; /**< Pointer to the buffer data */
    size_t size;   /**< Size of the buffer */
} io_buffer_t;

/**
 * \fn FILE *open_file(char *filename, char *mode)
 * \brief Opens a file.
 * \param filename Name of the file to open.
 * \param mode Mode in which to open the file.
 * \return Pointer to the opened file, or NULL if the file couldn't be opened.
 */
FILE *open_file(char *filename, char *mode);

/**
 * \fn int close_file(FILE *fp)
 * \brief Closes a file.
 * \param fp Pointer to the file to close.
 * \return 0 if the file was closed successfully, EOF otherwise.
 */
int close_file(FILE *fp);

/**
 * \fn int read_file(FILE *fp, io_buffer_t *buffer)
 * \brief Reads data from a file into a buffer.
 * \param fp Pointer to the file to read from.
 * \param buffer Pointer to the buffer where the read data will be stored.
 * \return 0 if the data was read successfully, non-zero value otherwise.
 */
int read_file(FILE *fp, io_buffer_t *buffer);

/**
 * \fn int write_file(FILE *fp, io_buffer_t *buffer)
 * \brief Writes data from a buffer to a file.
 * \param fp Pointer to the file to write to.
 * \param buffer Pointer to the buffer from which the data will be written.
 * \return 0 if the data was written successfully, non-zero value otherwise.
 */
int write_file(FILE *fp, io_buffer_t *buffer);

/**
 * \fn void print_string(char *string)
 * \brief Prints a string to the standard output.
 * \param string Pointer to the string to be printed.
 */
void print_string(char *string);

/**
 * \fn int read_line(FILE *fp, char *buffer, size_t size)
 * \brief Reads a line from a file into a buffer.
 * \param fp Pointer to the file to read from.
 * \param buffer Pointer to the buffer where the read line will be stored.
 * \param size The maximum number of characters to be read (including the null character).
 * \return 0 if the line was read successfully, non-zero value otherwise.
 */
int read_line(FILE *fp, char *buffer, size_t size);

/**
 * \fn int get_input(char *buffer, size_t size)
 * \brief Gets user input from the standard input into a buffer.
 * \param buffer Pointer to the buffer where the input will be stored.
 * \param size The maximum number of characters to be read (including the null character).
 * \return 0 if the input was read successfully, non-zero value otherwise.
 */
int get_input(char *buffer, size_t size);

/**
 * \fn void print_error(char *message)
 * \brief Prints an error message to the standard error.
 * \param message Pointer to the error message to be printed.
 */
void print_error(char *message);

/**
 * \fn int read_text_file(char *filename, void (*callback)(char *line))
 * \brief Reads a text file line by line and applies a callback function to each line.
 * \param filename The name of the file to be read.
 * \param callback The function to be applied to each line of the file.
 * \return 0 if the file was read successfully, non-zero value otherwise.
 */
int read_text_file(char *filename, void (*callback)(char *line));

/**
 * \fn int write_text_file(char *filename, char **lines, size_t num_lines)
 * \brief Writes an array of lines to a text file.
 * \param filename The name of the file to be written to.
 * \param lines The array of lines to be written to the file.
 * \param num_lines The number of lines in the array.
 * \return 0 if the file was written successfully, non-zero value otherwise.
 */
int write_text_file(char *filename, char **lines, size_t num_lines);

/**
 * \fn int read_binary_file(char *filename, void *buffer, size_t size)
 * \brief Reads a binary file into a buffer.
 * \param filename The name of the file to be read.
 * \param buffer The buffer where the file content will be stored.
 * \param size The maximum number of bytes to be read.
 * \return 0 if the file was read successfully, non-zero value otherwise.
 */
int read_binary_file(char *filename, void *buffer, size_t size);

/**
 * \fn int write_binary_file(char *filename, void *buffer, size_t size)
 * \brief Writes a buffer to a binary file.
 * \param filename The name of the file to be written to.
 * \param buffer The buffer whose content will be written to the file.
 * \param size The number of bytes to be written from the buffer.
 * \return 0 if the file was written successfully, non-zero value otherwise.
 */
int write_binary_file(char *filename, void *buffer, size_t size);

/**
 * \fn int update_binary_file(char *filename, void *buffer, size_t size, unsigned long start_pos, size_t
 * length)
 * \brief Updates a portion of a binary file with data from a buffer.
 * \param filename The name of the file to be updated.
 * \param buffer The buffer containing the data to be written to the file.
 * \param size The total size of the buffer.
 * \param start_pos The starting position in the file where the update will begin.
 * \param length The number of bytes to be written from the buffer.
 * \return 0 if the file was updated successfully, non-zero value otherwise.
 */
int update_binary_file(char *filename, void *buffer, size_t size, unsigned long start_pos, size_t length);

#endif /* COMMON_IO_H */