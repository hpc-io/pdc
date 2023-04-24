#ifndef CSVREADER_H
#define CSVREADER_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "pdc_list.h"

typedef struct csv_header_t {
    char *               field_name;
    int                  field_index;
    char                 field_type;
    struct csv_header_t *next;
} csv_header_t;

typedef struct csv_cell_t {
    char *             field_value;
    csv_header_t *     header;
    struct csv_cell_t *next;
} csv_cell_t;

typedef struct csv_row_t {
    csv_cell_t *      first_cell;
    struct csv_row_t *next;
} csv_row_t;

typedef struct csv_table_t {
    csv_header_t *first_header;
    csv_row_t *   first_row;
} csv_table_t;

/**
 * @brief This function sets the delimiter for the CSV file. The default is a comma.
 * @param delimiter The delimiter to use.
 */
void csv_set_delimiter(char delimiter);

/**
 * @brief This function sets the quote character for the CSV file. The default is a double quote.
 * @param quote The quote character to use.
 */
void csv_set_quote(char quote);

/**
 * @brief This function sets the escape character for the CSV file. The default is a backslash.
 * @param escape The escape character to use.
 */
void csv_set_escape(char escape);

/**
 * @brief This function sets the newline character for the CSV file. The default is a newline.
 * @param newline The newline character to use.
 */
void csv_set_newline(char newline);


/**
 * @brief This function parses a CSV header line and returns a linked list of csv_header_t structs. The header
 * string may contain quotes and spaces
 * @param line The CSV header line to parse.
 * @param field_types A string of field types. The field types are 's' for string, 'i' for long integer, 'f'
 * for float, and 'd' for double. If this is NULL, all fields are assumed to be strings.
 *
 * @return A pointer to the first csv_header_t struct in the linked list.
 */
csv_header_t *csv_parse_header(char *line, char *field_types);

/**
 * @brief This function parse a CSV row line and returns a linked list of csv_cell_t structs. The row string
 * may contain quotes and spaces
 * @param line The CSV row line to parse.
 * @param header A pointer to the first csv_header_t struct in the linked list.
 *
 * @return A pointer to the csv_row_t struct. The value in the csv_cell should be
 * free of quotes or spaces.
 */
csv_row_t *csv_parse_row(char *line, csv_header_t *header);

/**
 * @brief This function returns the string value of a field for a given row string. The row string may contain
 * quotes and spaces
 * @param row The CSV row to look for.
 * @param header A pointer to the first csv_header_t struct in the linked list.
 * @param field_name The name of the field to get the value for.
 *
 * @return A pointer to the csv_cell struct of the field. The value in the csv_cell should be free of quotes
 * or spaces.
 */
csv_cell_t *csv_get_field_value_by_name(csv_row_t *row, csv_header_t *header, char *field_name);

/**
 * @brief This function returns the string value of a field for a given row string. The row string may contain
 * quotes and spaces
 * @param row The CSV row to look for.
 * @param header A pointer to the first csv_header_t struct in the linked list.
 * @param field_index The index of the field to get the value for.
 *
 * @return A pointer to the csv_cell struct of the field. The value in the csv_cell should be free of quotes
 * or spaces.
 */
csv_cell_t *csv_get_field_value_by_index(csv_row_t *row, csv_header_t *header, int field_index);

/**
 * @brief This function parses a CSV file and returns a csv_table_t struct.
 * @param file_name The name of the CSV file to parse.
 * @param field_types A string of field types. The field types are 's' for string, 'i' for long integer, 'f'
 * for float, and 'd' for double. If this is NULL, all fields are assumed to be strings.
 *
 * @return A pointer to the csv_table_t struct.
 */
csv_table_t *csv_parse_file(char *file_name, char *field_types);

/**
 * @brief This function parses a PDC_LIST of strings as a CSV file and returns a csv_table_t struct.
 * @param list A PDC_LIST of strings to parse.
 * @param field_types A string of field types. The field types are 's' for string, 'i' for long integer, 'f'
 * for float, and 'd' for double. If this is NULL, all fields are assumed to be strings.
 *
 * @return A pointer to the csv_table_t struct.
 */
csv_table_t *csv_parse_list(PDC_LIST *list, char *field_types);

/**
 * @brief This function frees the memory allocated for a csv_table_t struct.
 * @param table A pointer to the csv_table_t struct to free.
 */
void csv_free_table(csv_table_t *table);

/**
 * @brief This function frees the memory allocated for a csv_header_t struct.
 * @param header A pointer to the csv_header_t struct to free.
 */
void csv_free_header(csv_header_t *header);

/**
 * @brief This function frees the memory allocated for a csv_row_t struct.
 * @param row A pointer to the csv_row_t struct to free.
 */
void csv_free_row(csv_row_t *row);

/**
 * @brief This function frees the memory allocated for a csv_cell_t struct.
 * @param cell A pointer to the csv_cell_t struct to free.
 */
void csv_free_cell(csv_cell_t *cell);

/**
 * @brief This function prints the contents of a csv_table_t struct.
 * @param table A pointer to the csv_table_t struct to print.
 */
void csv_print_table(csv_table_t *table);

/**
 * @brief This function prints the contents of a csv_header_t struct.
 * @param header A pointer to the csv_header_t struct to print.
 */
void csv_print_header(csv_header_t *header);

/**
 * @brief This function prints the contents of a csv_row_t struct.
 * @param row A pointer to the csv_row_t struct to print.
 * @param with_key A flag to indicate whether to print the key or not.
 */

void csv_print_row(csv_row_t *row, int with_key);

/**
 * @brief This function prints the contents of a csv_cell_t struct.
 * @param cell A pointer to the csv_cell_t struct to print.
 * @param with_key A flag to indicate whether to print the key or not.
 */
void csv_print_cell(csv_cell_t *cell, int with_key);

/**
 * @brief This function returns the number of rows in a csv_table_t struct.
 * @param table A pointer to the csv_table_t struct.
 *
 * @return The number of rows in the table.
 */
int csv_get_num_rows(csv_table_t *table);

/**
 * @brief This function returns the number of fields in a csv_table_t struct.
 * @param table A pointer to the csv_table_t struct.
 *
 * @return The number of fields in the table.
 */
int csv_get_num_fields(csv_table_t *table);

#endif // CSVREADER_H