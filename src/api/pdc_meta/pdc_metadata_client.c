#include "pdc_metadata_client.h"
#include "pdc_client_connect.h"

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

/**
 * the result parameter should be an array of separate_query_result_t, with the size of conditionCount.
 * since conditionCount is known when calling this function, the caller should know how much memory we
 * allocated here in this function.
 */
void
send_query_condition_get_separate_result(char conditions[][CONDITION_LENGTH], int conditionCount,
                                         MPI_Comm world_comm, separate_query_result_t **result)
{
    if (conditionCount <= 0) {
        printf("No conditions to send\n");
        return;
    }
    *result = (separate_query_result_t *)malloc(conditionCount * sizeof(separate_query_result_t));
    for (int i = 0; i < conditionCount; i++) {
        // Send each condition to a separate server for execution
        // The server will execute the condition and return the result to the client
        char *condition = conditions[i];
        // We assume non-collective mode by default, and the caller of this fuction is the sender, and we
        // just send the request.
        int send = 1;
        // if this is collective mode, each client will send a different condition to a different server
        perr_t rst;
        if (world_comm != NULL) {
            rst = PDC_Client_search_obj_ref_through_dart_mpi(DART_HASH, condition, REF_PRIMARY_ID, &n_res,
                                                             &out, world_comm);
        }
        int       n_res;
        uint64_t *out;
        perr_t    rst =
            PDC_Client_search_obj_ref_through_dart(DART_HASH, condition, REF_PRIMARY_ID, &n_res, &out);
        if (rst != SUCCEED) {
            printf("Error with PDC_Client_search_obj_ref_through_dart\n");
            return;
        }
        (*result)[i] = (separate_query_result_t){n_res, out, condition};
    }
}

void
query_execution_and_local_merge(char conditions[][CONDITION_LENGTH], int conditionCount, int isCollective,
                                uint64_t **object_id_list, uint64_t *count)
{
    // step 1: send each condition to a separate server for execution, from a different rank
    separate_query_result_t *separate_result;
    send_query_condition_get_separate_result(conditions, conditionCount, isCollective, &separate_result);
    // step 2: merge the results from all servers
    for (int i = 0; i < conditionCount; i++) {
        if (separate_result[i].n_res > 0) {
            *object_id_list = (uint64_t *)malloc(separate_result[i].n_res * sizeof(uint64_t));
            memcpy(*object_id_list, separate_result[i].out, separate_result[i].n_res * sizeof(uint64_t));
            *count = separate_result[i].n_res;
            break;
        }
    }
}

size_t
PDC_metadata_multi_condition_query(char *queryString, int isCollective, uint64_t **object_id_list,
                                   uint64_t *count)
{
    char conditions[MAX_CONDITIONS][CONDITION_LENGTH];
    int  conditionCount = 0;

    splitExpression(queryString, conditions, &conditionCount);

    // strategy 1: parallel execution of each condition
    query_execution_and_local_merge(conditions, conditionCount, isCollective, object_id_list, count);
    // strategy 2: query_execution_and_parallel_merge(conditions, conditionCount, isCollective,
    // object_id_list, count);
    // strategy 3: initial execution to pick the most selective condition, then
    // execute the rest in parallel and merge selectivity_based_optimized_execution(conditions,
    // conditionCount, object_id_list, count);
    // TODO: implement the above strategy

    // For now, we just return a dummy object ID list
    *object_id_list = (uint64_t *)malloc(10 * sizeof(uint64_t));
    for (int i = 0; i < 10; i++) {
        (*object_id_list)[i] = i;
    }
    *count = 10;

    return 10;
}