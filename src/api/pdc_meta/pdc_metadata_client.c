#include "pdc_metadata_client.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "pdc_hash.h"

#define MAX_CONDITIONS   100 // Maximum number of conditions we expect
#define CONDITION_LENGTH 256 // Maximum length of each condition

// Function to trim whitespace from the beginning and end of a string
char *
trimWhitespace(char *str)
{
    char *end;

    // Trim leading space
    while (isspace((unsigned char)*str))
        str++;

    if (*str == 0) // All spaces?
        return str;

    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end))
        end--;

    // Write new null terminator character
    end[1] = '\0';

    return str;
}

void
splitExpression(const char *expression, char conditions[][CONDITION_LENGTH], int *count)
{
    char       tempExpr[1024];
    char *     token;
    const char delimiters[] = "AND OR";
    int        index        = 0;

    // Copy the expression to a temporary buffer and remove parentheses
    for (int i = 0, j = 0; i < strlen(expression); ++i) {
        if (expression[i] != '(' && expression[i] != ')') {
            tempExpr[j++] = expression[i];
            tempExpr[j]   = '\0'; // Ensure string is always null-terminated
        }
    }

    // Split the expression by AND/OR
    token = strtok(tempExpr, delimiters);
    while (token != NULL && index < MAX_CONDITIONS) {
        strcpy(conditions[index], trimWhitespace(token));
        index++;
        token = strtok(NULL, delimiters);
    }

    *count = index; // Update the count of extracted conditions
}

void
parallel_execution_and_merge(char conditions[][CONDITION_LENGTH], int conditionCount,
                             uint64_t **object_id_list, uint64_t *count)
{
    // step 1: send each condition to a separate server for execution, from a different rank
    send_query_condition_get_separate_result(conditions, conditionCount, object_id_list, count, only_count);
    // step 2: merge the results from all servers
}

size_t
PDC_metadata_multi_condition_query(char *queryString, PDC_metadata_query_pe_info pe_info,
                                   uint64_t **object_id_list, uint64_t *count)
{
    char conditions[MAX_CONDITIONS][CONDITION_LENGTH];
    int  conditionCount = 0;

    splitExpression(queryString, conditions, &conditionCount);

    // strategy 1: parallel execution of each condition
    parallel_execution_and_merge(conditions, conditionCount, object_id_list, count);
    // strategy 2: initial execution to pick the most selective condition, then execute the rest in parallel
    // and merge
    // selectivity_based_optimized_execution(conditions, conditionCount, object_id_list, count);

    // For now, we just return a dummy object ID list
    *object_id_list = (uint64_t *)malloc(10 * sizeof(uint64_t));
    for (int i = 0; i < 10; i++) {
        (*object_id_list)[i] = i;
    }
    *count = 10;

    return 10;
}