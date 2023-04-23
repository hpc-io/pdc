#include "csvReader.h"

csv_header_t *
csv_parse_header(char *line, char *field_types)
{
    csv_header_t *first_header = NULL;
    csv_header_t *last_header  = NULL;
    char *        token        = NULL;
    char *        saveptr      = NULL;
    int           field_index  = 0;
    int           in_quotes    = 0;
    int           value_start  = 0;
    int           i            = 0;

    for (int i = 0; line[i] != '\0'; ++i) {
        if (line[i] == '\"') {
            in_quotes = !in_quotes;
        }
        else if (!in_quotes && (line[i] == ',' || line[i + 1] == '\0')) {
            // Allocate memory for the header struct
            csv_header_t *header = (csv_header_t *)malloc(sizeof(csv_header_t));
            if (header == NULL) {
                return NULL;
            }
            // Remove quotes and spaces from the field name
            header->field_name = strndup(line + value_start, i - value_start + (line[i + 1] == '\0'));

            // Set the field index
            header->field_index = field_index;

            // Set the field type
            if (field_types != NULL) {
                header->field_type = field_types[field_index];
            }
            else {
                header->field_type = 's';
            }

            // Set the next pointer to NULL
            header->next = NULL;

            // Add the header to the linked list
            if (first_header == NULL) {
                first_header = header;
                last_header  = header;
            }
            else {
                last_header->next = header;
                last_header       = header;
            }

            value_start = i + 1;
            field_index++;
        }
    }

    return first_header;
}

csv_cell_t *
csv_parse_row(char *line, csv_header_t *header)
{
    csv_cell_t *  first_cell     = NULL;
    csv_cell_t *  last_cell      = NULL;
    csv_header_t *current_header = header;
    char *        token          = NULL;
    char *        saveptr        = NULL;
    int           field_index    = 0;
    int           in_quotes      = 0;
    int           value_start    = 0;
    int           i              = 0;

    for (int i = 0; line[i] != '\0'; ++i) {
        if (line[i] == '\"') {
            in_quotes = !in_quotes;
        }
        else if (!in_quotes && (line[i] == ',' || line[i + 1] == '\0')) {
            // Allocate memory for the cell struct
            csv_cell_t *cell = (csv_cell_t *)malloc(sizeof(csv_cell_t));
            if (cell == NULL) {
                return NULL;
            }

            // Set the field name
            cell->header = current_header;

            // Set the field value
            cell->field_value = strndup(line + value_start, i - value_start + (line[i + 1] == '\0'));

            // Set the next pointer to NULL
            cell->next = NULL;

            // Add the cell to the linked list
            if (first_cell == NULL) {
                first_cell = cell;
                last_cell  = cell;
            }
            else {
                last_cell->next = cell;
                last_cell       = cell;
            }

            value_start = i + 1;
            field_index++;
            current_header = current_header->next;
        }
    }

    return first_cell;
}

csv_cell_t *
csv_get_field_value_by_name(char *line, csv_header_t *header, char *field_name)
{
    csv_cell_t *cell = csv_parse_row(line, header);
    while (cell != NULL) {
        if (strcmp(cell->header->field_name, field_name) == 0) {
            return cell;
        }
        cell = cell->next;
    }
    return NULL;
}

csv_cell_t *
csv_get_field_value_by_index(char *line, csv_header_t *header, int field_index)
{
    csv_cell_t *cell = csv_parse_row(line, header);
    while (cell != NULL) {
        if (cell->header->field_index == field_index) {
            return cell;
        }
        cell = cell->next;
    }
    return NULL;
}

csv_table_t *
csv_parse_file(char *file_name, char *field_types)
{
    FILE *fp = fopen(file_name, "r");
    if (fp == NULL) {
        return NULL;
    }

    // Allocate memory for the table struct
    csv_table_t *table = (csv_table_t *)malloc(sizeof(csv_table_t));
    if (table == NULL) {
        return NULL;
    }

    // Read the first line of the file
    char *  line = NULL;
    size_t  len  = 0;
    ssize_t read = getline(&line, &len, fp);

    // Parse the header
    table->first_header = csv_parse_header(line, field_types);

    // Parse the rows
    csv_row_t *first_row = NULL;
    csv_row_t *last_row  = NULL;
    while ((read = getline(&line, &len, fp)) != -1) {
        // Allocate memory for the row struct
        csv_row_t *row = (csv_row_t *)malloc(sizeof(csv_row_t));
        if (row == NULL) {
            return NULL;
        }

        // Parse the row
        row->first_cell = csv_parse_row(line, table->first_header);

        // Set the next pointer to NULL
        row->next = NULL;

        // Add the row to the linked list
        if (first_row == NULL) {
            first_row = row;
            last_row  = row;
        }
        else {
            last_row->next = row;
            last_row       = row;
        }
    }

    table->first_row = first_row;

    return table;
}

csv_table_t *csv_parse_list(PDC_LIST *list, char *field_types){
    csv_table_t *table = (csv_table_t *)malloc(sizeof(csv_table_t));
    if (table == NULL) {
        return NULL;
    }
    int num_file_read = 0;
    csv_row_t *first_row = NULL;
    csv_row_t *last_row  = NULL;

    PDC_LIST_ITERATOR *iter = pdc_list_iterator_new(list);
    while (pdc_list_iterator_has_next(iter)) {
        char *line = (char *)pdc_list_iterator_next(iter);
        if (num_file_read == 0) {
            table->first_header = csv_parse_header(line, field_types);
        } else {
            // Allocate memory for the row struct
            csv_row_t *row = (csv_row_t *)malloc(sizeof(csv_row_t));
            if (row == NULL) {
                return NULL;
            }

            // Parse the row
            row->first_cell = csv_parse_row(line, table->first_header);

            // Set the next pointer to NULL
            row->next = NULL;

            // Add the row to the linked list
            if (first_row == NULL) {
                first_row = row;
                last_row  = row;
            }
            else {
                last_row->next = row;
                last_row       = row;
            }
        }
        num_file_read++;
    }

    table->first_row = first_row;

    return table;
}

void
csv_free_header(csv_header_t *header)
{
    csv_header_t *current_header = header;
    csv_header_t *next_header    = NULL;
    while (current_header != NULL) {
        next_header = current_header->next;
        free(current_header->field_name);
        free(current_header);
        current_header = next_header;
    }
}

void
csv_free_row(csv_row_t *row)
{
    csv_row_t *current_row = row;
    csv_row_t *next_row    = NULL;
    while (current_row != NULL) {
        next_row = current_row->next;
        csv_free_cell(current_row->first_cell);
        free(current_row);
        current_row = next_row;
    }
}

void
csv_free_cell(csv_cell_t *cell)
{
    csv_cell_t *current_cell = cell;
    csv_cell_t *next_cell    = NULL;
    while (current_cell != NULL) {
        next_cell = current_cell->next;
        free(current_cell->field_value);
        free(current_cell);
        current_cell = next_cell;
    }
}

void
csv_free_table(csv_table_t *table)
{
    csv_free_header(table->first_header);
    csv_free_row(table->first_row);
    free(table);
}

void
csv_print_header(csv_header_t *header)
{
    csv_header_t *current_header = header;
    while (current_header != NULL) {
        printf("%s", current_header->field_name);
        if (current_header->next != NULL) {
            printf(", ");
        }
        current_header = current_header->next;
    }
    printf("\n");
}

void
csv_print_row(csv_row_t *row, int with_key)
{
    csv_cell_t *current_cell = row->first_cell;
    while (current_cell != NULL) {
        csv_print_cell(current_cell, with_key);
        if (current_cell->next != NULL) {
            printf(", ");
            if (with_key) {
                printf("\n");
            }
        }
        current_cell = current_cell->next;
    }
    printf("\n");
}

void
csv_print_cell(csv_cell_t *cell, int with_key)
{
    if (with_key) {
        printf("%s: ", cell->header->field_name);
    }
    switch (cell->header->field_type)
    {
    case 'i':
        printf("%ld", strtol(cell->field_value, NULL, 10));
        break;

    case 'f':
        printf("%f", strtod(cell->field_value, NULL));
        break;

    case 's':
        printf("%s", cell->field_value);
        break;

    default:
        printf("%s", cell->field_value);
        break;
    }
    
}



void
csv_print_table(csv_table_t *table)
{
    csv_print_header(table->first_header);
    csv_row_t *current_row = table->first_row;
    while (current_row != NULL) {
        csv_print_row(current_row, 0);
        current_row = current_row->next;
    }
}

int
csv_get_num_rows(csv_table_t *table)
{
    int        num_rows    = 0;
    csv_row_t *current_row = table->first_row;
    while (current_row != NULL) {
        num_rows++;
        current_row = current_row->next;
    }
    return num_rows;
}

int
csv_get_num_fields(csv_table_t *table)
{
    int           num_fields     = 0;
    csv_header_t *current_header = table->first_header;
    while (current_header != NULL) {
        num_fields++;
        current_header = current_header->next;
    }
    return num_fields;
}