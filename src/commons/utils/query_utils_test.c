#include "query_utils.h"

void
print_query_output(query_gen_output_t *output)
{
    println("key query: %s, len: %lu", output->key_query, output->key_query_len);
    println("value query: %s, len: %lu", output->value_query, output->value_query_len);
    char *final_query_str = gen_query_str(output);
    println("final query: %s, len: %lu", final_query_str, strlen(final_query_str));
    free(final_query_str);
}

int
main(int argc, char *argv[])
{
    pdc_kvtag_t *base_string_tag;
    base_string_tag        = (pdc_kvtag_t *)calloc(1, sizeof(pdc_kvtag_t));
    base_string_tag->name  = "testname";
    base_string_tag->type  = PDC_STRING;
    base_string_tag->value = "testvalue";

    for (int i = 0; i < 4; i++) {
        int                key_query_type   = i;
        int                value_query_type = i;
        query_gen_input_t  input;
        query_gen_output_t output;
        input.base_tag         = base_string_tag;
        input.key_query_type   = key_query_type;
        input.value_query_type = value_query_type;
        input.affix_len        = 3;
        gen_query_key_value(&input, &output);
        print_query_output(&output);
        free_query_output(&output);
    }

    pdc_kvtag_t *base_int_tag;
    int          int_value          = 234;
    base_int_tag                    = (pdc_kvtag_t *)calloc(1, sizeof(pdc_kvtag_t));
    base_int_tag->name              = "testname";
    base_int_tag->type              = PDC_INT;
    base_int_tag->value             = (void *)calloc(1, sizeof(int));
    ((int *)base_int_tag->value)[0] = int_value;

    for (int i = 4; i < 6; i++) {
        int                key_query_type   = 0;
        int                value_query_type = i;
        int                range_offset     = 10;
        int                range_lo         = int_value - range_offset;
        int                range_hi         = int_value + range_offset;
        query_gen_input_t  input;
        query_gen_output_t output;
        input.base_tag         = base_int_tag;
        input.key_query_type   = key_query_type;
        input.value_query_type = value_query_type;
        input.range_lo         = range_lo;
        input.range_hi         = range_hi;
        gen_query_key_value(&input, &output);
        print_query_output(&output);
        free_query_output(&output);
    }

    return 0;
}