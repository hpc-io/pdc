#include "qc_parser.h"

int
main()
{
    char *expression =
        "z=\"tsa\" AND t=234234 OR NOT (e=234 AND (a=123 OR b=\"abc\") AND ((NOT c=987) OR d=258))";
    Condition *root = (Condition *)malloc(sizeof(Condition));
    root->start     = 0;
    root->end       = strlen(expression);
    root->level     = 0;
    splitCondition(expression, root);
}