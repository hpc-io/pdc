#include "qc_parser.h"

void
printSubExpression(char *expression, Condition *condition)
{
    if (condition == NULL) {
        printf("NULL\n");
        return;
    }
    printf("level %d, logical op: %s, start: %d, end: %d ", condition->level, logicalOpStr[condition->op],
           condition->start, condition->end);
    printf("[");
    for (int i = condition->start; i < condition->end; i++) {
        printf("%c", expression[i]);
    }
    printf("]\n");
    printf("left:");
    printSubExpression(expression, condition->left);
    printf("right:");
    printSubExpression(expression, condition->right);
}

Condition *
createSubCondition(int start, int end, int level)
{
    Condition *condition = (Condition *)malloc(sizeof(Condition));
    condition->start     = start;
    condition->end       = end;
    condition->level     = level;
    return condition;
}

int
isFactor(char *expression, Condition *condition)
{
    if (condition->start == condition->end) {
        return 0;
    }
    // if the string contains matching pairs of '(' and ')' then it is a string of expression
    int pLevel = 0;
    for (int i = condition->start; i < condition->end; i++) {
        if (expression[i] == '(') {
            pLevel++;
        }
        if (expression[i] == ')') {
            pLevel--;
            if (pLevel == 0) {
                return 1;
            }
        }
    }

    // if the string contains 'AND'/'OR' then it is not a factor
    for (int i = condition->start; i < condition->end; i++) {
        if (expression[i] == 'A' && expression[i + 1] == 'N' && expression[i + 2] == 'D') {
            return 0;
        }
        if (expression[i] == 'O' && expression[i + 1] == 'R') {
            return 0;
        }
    }
    return 1;
}

int
isTerm(char *expression, Condition *condition)
{
    if (condition->start == condition->end) {
        return 0;
    }
    // if the string contains matching pairs of '(' and ')' then it is a string of expression
    int pLevel = 0;
    for (int i = condition->start; i < condition->end; i++) {
        if (expression[i] == '(') {
            pLevel++;
        }
        if (expression[i] == ')') {
            pLevel--;
        }
    }
    if (pLevel == 0) {
        return 1;
    }
    // if the string contains 'AND'/'OR' then it is not a factor
    for (int i = condition->start; i < condition->end; i++) {
        if (expression[i] == 'A' && expression[i + 1] == 'N' && expression[i + 2] == 'D') {
            return 1;
        }
    }
    return 0;
}
int isExpression(char *expression, Condition *condition);
int isExpressionStringFactor(char *expression, Condition *condition);

int
extractExpression(char *expression, Condition *condition)
{
    int levelCounter = condition->level;
    int i1           = condition->start;
    int i2           = i1 + 1;
    int rst          = -1;
    while (i2 < condition->end) {
        int          l_start = condition->start;
        int          l_end   = condition->end;
        int          r_start = condition->start;
        int          r_end   = condition->end;
        logical_op_t op;

        if (expression[i1] == '(') {
            levelCounter++;
        }
        if (expression[i1] == ')') {
            levelCounter--;
        }
        // printf("i1: %d, i2: %d, levelCounter: %d\n", i1, i2, levelCounter);
        if (levelCounter == condition->level) {
            int conditionFound = 0;
            if (expression[i1] == 'O' && expression[i2] == 'R') { // FOUND OR on the same level
                l_start        = condition->start;
                l_end          = i1 - 1;
                r_start        = i2 + 1;
                r_end          = condition->end;
                op             = OR;
                conditionFound = 1;
            }
            if (conditionFound) {
                condition->left  = createSubCondition(l_start, l_end, condition->level);
                condition->right = createSubCondition(r_start, r_end, condition->level);
                condition->op    = op;

                printSubExpression(expression, condition);
                splitCondition(expression, condition->left);
                splitCondition(expression, condition->right);
            }
        }
        i1++;
        i2  = i1 + 1;
        rst = 0;
    }
    return rst;
}

int extractTerm(char *expression, Condition *condition);

int extractFactor(char *expression, Condition *condition);

int
splitCondition(char *expression, Condition *condition)
{
    int levelCounter = condition->level;
    int i1           = condition->start;
    int i2           = i1 + 1;
    int rst          = -1;
    while (i2 < condition->end) {
        int          l_start = condition->start;
        int          l_end   = condition->end;
        int          r_start = condition->start;
        int          r_end   = condition->end;
        logical_op_t op;

        if (expression[i1] == '(') {
            levelCounter++;
        }
        if (expression[i1] == ')') {
            levelCounter--;
        }
        // printf("i1: %d, i2: %d, levelCounter: %d\n", i1, i2, levelCounter);
        if (levelCounter == condition->level) {
            int conditionFound = 0;
            if (expression[i1] == 'O' && expression[i2] == 'R') { // FOUND OR on the same level
                l_start        = condition->start;
                l_end          = i1 - 1;
                r_start        = i2 + 1;
                r_end          = condition->end;
                op             = OR;
                conditionFound = 1;
            }
            if (expression[i1] == 'A' && expression[i2] == 'N' &&
                expression[i2 + 1] == 'D') { // FOUND AND on the same level
                l_start        = condition->start;
                l_end          = i1 - 1;
                r_start        = i2 + 2;
                r_end          = condition->end;
                op             = AND;
                conditionFound = 1;
            }
            if (conditionFound) {
                condition->left  = createSubCondition(l_start, l_end, condition->level);
                condition->right = createSubCondition(r_start, r_end, condition->level);
                condition->op    = op;

                printSubExpression(expression, condition);
                splitCondition(expression, condition->left);
                splitCondition(expression, condition->right);
            }
        }
        i1++;
        i2  = i1 + 1;
        rst = 0;
    }
    return rst;
}

int
subClauseExtractor(char *expression, int start, int end, int level, subClause **subPtr)
{
    int levelCounter  = level;
    *subPtr           = NULL; // Initially, there are no subclauses.
    int numSubClauses = 0;

    for (int i = start; i < end; i++) {
        if (expression[i] == '(') {
            levelCounter++;
            if (levelCounter == level + 1) {
                // Allocate or reallocate space for a new subclause.
                *subPtr              = (subClause *)realloc(*subPtr, sizeof(subClause) * (numSubClauses + 1));
                subClause *newClause = &((*subPtr)[numSubClauses]);
                newClause->start     = i + 1;
                newClause->level     = levelCounter;
                newClause->subClauseArray = NULL; // Important to initialize.
            }
        }
        else if (expression[i] == ')') {
            levelCounter--;
            if (levelCounter == level) {
                subClause *newClause     = &((*subPtr)[numSubClauses]);
                newClause->end           = i - 1;
                newClause->numSubClauses = subClauseExtractor(expression, newClause->start, newClause->end,
                                                              newClause->level, &newClause->subClauseArray);
                numSubClauses++;
            }
        }
    }
    return numSubClauses;
}

void
printSubClauses(char *expression, subClause *subPtr, int numSubClauses)
{
    for (int i = 0; i < numSubClauses; i++) {
        printf("SubClause %d: Level: %d, Expression: ", i, subPtr[i].level);
        for (int j = subPtr[i].start; j <= subPtr[i].end; j++) {
            printf("%c", expression[j]);
        }
        printf("\n");
        if (subPtr[i].numSubClauses > 0) {
            printSubClauses(expression, subPtr[i].subClauseArray, subPtr[i].numSubClauses);
        }
    }
}