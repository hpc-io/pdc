#ifndef QC_PARSER_H
#define QC_PARSER_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "string_utils.h"

typedef enum { NONE = 0, OR = 1, AND = 2, NOT = 3 } logical_op_t;

typedef enum { FACTOR = 0, TERM = 1, EXPRESSION = 2 } condition_type_t;

static const char *logicalOpStr[] = {NULL, "OR", "AND", "NOT"};

typedef struct subClause {
    int               start;
    int               end;
    int               level;
    struct subClause *subClauseArray;
    int               numSubClauses;
} subClause;

/**
 * 1. Factor -> left and right are NULL && op is NONE. if the corresponding string contains 'AND'/'OR' then it
 * is a string of expression.
 * 2. Term -> either left or right must be factor, the other can be a factor or a term
 * 3. Expression -> either left or right must be a term, the other can be a term or an expression
 */
typedef struct Condition {
    int               start;
    int               end;
    int               level;
    condition_type_t  type;
    struct Condition *left;
    struct Condition *right;
    logical_op_t      op;
} Condition;

int isFactor(char *expression, Condition *condition);
int isTerm(char *expression, Condition *condition);
int isExpression(char *expression, Condition *condition);
int isExpressionStringFactor(char *expression, Condition *condition);

int extractExpression(char *expression, Condition *condition);

int extractTerm(char *expression, Condition *condition);

int extractFactor(char *expression, Condition *condition);

int splitCondition(char *expression, Condition *condition);

int subClauseExtractor(char *expression, int start, int end, int level, subClause **subPtr);

void printSubClauses(char *expression, subClause *subPtr, int numSubClauses);

#endif // QC_PARSER_H