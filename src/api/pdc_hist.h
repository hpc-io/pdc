#ifndef PDC_HIST_H
#define PDC_HIST_H

#include "pdc_public.h"
#include "math.h"
#include <inttypes.h>

pdc_histogram_t *PDC_gen_hist(PDC_var_type_t dtype, uint64_t n, void *data);
pdc_histogram_t *PDC_dup_hist(pdc_histogram_t *hist);
pdc_histogram_t *PDC_merge_hist(int n, pdc_histogram_t **hists);
void PDC_free_hist(pdc_histogram_t *hist);
void PDC_print_hist(pdc_histogram_t *hist);
void copy_hist(pdc_histogram_t *to, pdc_histogram_t *from);

#endif /* PDC_HIST_H */
