#include "pdc_hist_pkg.h"
#include "pdc_private.h"
#include <stdio.h>
#include <stdlib.h>

#define MACRO_SAMPLE_MIN_MAX(TYPE, n, data, sample_pct, min, max)                                            \
    ({                                                                                                       \
        uint64_t i, sample_n, iter = 0;                                                                      \
        TYPE *   ldata = (TYPE *)data;                                                                       \
        (min)          = ldata[0];                                                                           \
        (max)          = ldata[0];                                                                           \
        sample_n       = (n) * (sample_pct);                                                                 \
        for (i = 1; i < sample_n; i++) {                                                                     \
            iter = (iter + rand()) % (n);                                                                    \
            if (ldata[iter] > (max))                                                                         \
                (max) = ldata[iter];                                                                         \
            else if (ldata[iter] < (min))                                                                    \
                (min) = ldata[iter];                                                                         \
        }                                                                                                    \
    })

perr_t
PDC_sample_min_max(pdc_var_type_t dtype, uint64_t n, void *data, double sample_pct, double *min, double *max)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (NULL == data || NULL == min || NULL == max)
        PGOTO_DONE(FAIL);

    if (PDC_INT == dtype)
        MACRO_SAMPLE_MIN_MAX(int, n, data, sample_pct, *min, *max);
    else if (PDC_FLOAT == dtype)
        MACRO_SAMPLE_MIN_MAX(float, n, data, sample_pct, *min, *max);
    else if (PDC_DOUBLE == dtype)
        MACRO_SAMPLE_MIN_MAX(double, n, data, sample_pct, *min, *max);
    else if (PDC_INT64 == dtype)
        MACRO_SAMPLE_MIN_MAX(int64_t, n, data, sample_pct, *min, *max);
    else if (PDC_UINT64 == dtype)
        MACRO_SAMPLE_MIN_MAX(uint64_t, n, data, sample_pct, *min, *max);
    else if (PDC_UINT == dtype)
        MACRO_SAMPLE_MIN_MAX(uint32_t, n, data, sample_pct, *min, *max);
    else {
        PGOTO_ERROR(FAIL, "== datatype %d not supported!", dtype);
    }

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

double
ceil_power_of_2(double number)
{
    double ret_value = 0;
    double res       = 1.0;
    double sign      = number > 0 ? 1.0 : -1.0;

    FUNC_ENTER(NULL);

    number *= sign;

    if (number >= res) {
        while (res < number) {
            res *= 2.0;
        }
    }
    else {
        while (res >= number) {
            res /= 2.0;
        }
        res *= 2.0;
    }

    ret_value = res * sign;

    FUNC_LEAVE(ret_value);
}

double
floor_power_of_2(double number)
{
    double ret_value = 0;
    double res       = 1.0;
    double sign      = number > 0 ? 1.0 : -1.0;

    FUNC_ENTER(NULL);

    number *= sign;

    if (number >= res) {
        while (res <= number) {
            res *= 2.0;
        }
        res /= 2.0;
    }
    else {
        while (res > number) {
            res /= 2.0;
        }
    }

    ret_value = res * sign;

    FUNC_LEAVE(ret_value);
}

// Generate histogram with at least nbin bins
pdc_histogram_t *
PDC_create_hist(pdc_var_type_t dtype, int nbin, double min, double max)
{
    pdc_histogram_t *ret_value = NULL;
    pdc_histogram_t *hist;
    int              i;
    double           min_bin, max_bin, bin_incr;

    FUNC_ENTER(NULL);

    if (nbin < 4 || min > max)
        PGOTO_DONE(NULL);

    bin_incr = floor_power_of_2((max - min) / (nbin - 2)); // Excluding first and last bin (open ended)
    nbin     = ceil((max - min) / bin_incr);

    hist        = (pdc_histogram_t *)malloc(sizeof(pdc_histogram_t));
    hist->incr  = bin_incr;
    hist->dtype = dtype;
    hist->range = (double *)calloc(sizeof(double), nbin * 2);
    hist->bin   = (uint64_t *)calloc(sizeof(uint64_t), nbin);
    hist->nbin  = nbin;

    min_bin = floor(min);
    while (min_bin <= min) {
        min_bin += bin_incr;
    }

    max_bin = ceil(max);
    while (max_bin >= max) {
        max_bin -= bin_incr;
    }

    hist->range[0] = min_bin;
    hist->range[1] = min_bin;

    hist->range[nbin * 2 - 1] = max_bin;
    hist->range[nbin * 2 - 2] = max_bin;

    for (i = 2; i < nbin * 2 - 2; i += 2) {
        hist->range[i]     = hist->range[i - 1];
        hist->range[i + 1] = hist->range[i] + bin_incr;
    }

    ret_value = hist;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

#define MACRO_HIST_INCR_ALL(TYPE, hist, n, _data)                                                            \
    ({                                                                                                       \
        uint64_t i;                                                                                          \
        int      lo, mid = 0, hi;                                                                            \
        TYPE *   ldata = (TYPE *)(_data);                                                                    \
        if ((hist)->incr > 0) {                                                                              \
            for (i = 0; i < (n); i++) {                                                                      \
                if (ldata[i] < (hist)->range[1]) {                                                           \
                    (hist)->bin[0]++;                                                                        \
                    if (ldata[i] < (hist)->range[0])                                                         \
                        (hist)->range[0] = ldata[i];                                                         \
                }                                                                                            \
                else if (ldata[i] >= (hist)->range[((hist)->nbin * 2) - 2]) {                                \
                    (hist)->bin[(hist)->nbin - 1]++;                                                         \
                    if (ldata[i] > (hist)->range[((hist)->nbin * 2) - 1])                                    \
                        (hist)->range[((hist)->nbin * 2) - 1] = ldata[i];                                    \
                }                                                                                            \
                else {                                                                                       \
                    (hist)->bin[(int)((ldata[i] - (hist)->range[1]) / (hist)->incr + 1)]++;                  \
                }                                                                                            \
            }                                                                                                \
        }                                                                                                    \
        else {                                                                                               \
            for (i = 0; i < (n); i++) {                                                                      \
                if (ldata[i] < (hist)->range[1]) {                                                           \
                    (hist)->bin[0]++;                                                                        \
                    if (ldata[i] < (hist)->range[0])                                                         \
                        (hist)->range[0] = ldata[i];                                                         \
                }                                                                                            \
                else if (ldata[i] >= (hist)->range[((hist)->nbin * 2) - 2]) {                                \
                    (hist)->bin[(hist)->nbin - 1]++;                                                         \
                    if (ldata[i] > (hist)->range[((hist)->nbin * 2) - 1])                                    \
                        (hist)->range[((hist)->nbin * 2) - 1] = ldata[i];                                    \
                }                                                                                            \
                else {                                                                                       \
                    lo = 1;                                                                                  \
                    hi = (hist)->nbin - 2;                                                                   \
                    while (lo <= hi) {                                                                       \
                        mid = lo + (hi - lo) / 2;                                                            \
                        if (ldata[i] >= (hist)->range[mid * 2]) {                                            \
                            if (ldata[i] < (hist)->range[mid * 2 + 1])                                       \
                                break;                                                                       \
                            lo = mid + 1;                                                                    \
                        }                                                                                    \
                        else                                                                                 \
                            hi = mid - 1;                                                                    \
                    }                                                                                        \
                    (hist)->bin[mid]++;                                                                      \
                }                                                                                            \
            }                                                                                                \
        }                                                                                                    \
    })

perr_t
PDC_hist_incr_all(pdc_histogram_t *hist, pdc_var_type_t dtype, uint64_t n, void *data)
{
    perr_t ret_value = SUCCEED;

    FUNC_ENTER(NULL);

    if (dtype != hist->dtype || 0 == n || NULL == data)
        return FAIL;

    if (PDC_INT == dtype)
        MACRO_HIST_INCR_ALL(int, hist, n, data);
    else if (PDC_FLOAT == dtype)
        MACRO_HIST_INCR_ALL(float, hist, n, data);
    else if (PDC_DOUBLE == dtype)
        MACRO_HIST_INCR_ALL(double, hist, n, data);
    else if (PDC_INT64 == dtype)
        MACRO_HIST_INCR_ALL(int64_t, hist, n, data);
    else if (PDC_UINT64 == dtype)
        MACRO_HIST_INCR_ALL(uint64_t, hist, n, data);
    else if (PDC_UINT == dtype)
        MACRO_HIST_INCR_ALL(uint32_t, hist, n, data);
    else
        PGOTO_ERROR(FAIL, "== datatype %d not supported!", dtype);

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdc_histogram_t *
PDC_gen_hist(pdc_var_type_t dtype, uint64_t n, void *data)
{
    pdc_histogram_t *ret_value = NULL;
    pdc_histogram_t *hist;
    double           min, max;
#ifdef ENABLE_TIMING
    double         gen_hist_time;
    struct timeval pdc_timer_start, pdc_timer_end;
#endif

    FUNC_ENTER(NULL);

    if (0 == n || NULL == data) {
        PGOTO_DONE(NULL);
    }

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_start, 0);
#endif

    PDC_sample_min_max(dtype, n, data, 0.1, &min, &max);

    hist = PDC_create_hist(dtype, 50, min, max);
    if (NULL == hist)
        PGOTO_ERROR(NULL, "== error with PDC_create_hist!");

    PDC_hist_incr_all(hist, dtype, n, data);

#ifdef ENABLE_TIMING
    gettimeofday(&pdc_timer_end, 0);
    gen_hist_time = PDC_get_elapsed_time_double(&pdc_timer_start, &pdc_timer_end);
    printf("== generate histogram of %lu elements with %d bins takes %.2fs\n", n, hist->nbin, gen_hist_time);
#endif

    ret_value = hist;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void
PDC_free_hist(pdc_histogram_t *hist)
{
    FUNC_ENTER(NULL);

    if (NULL == hist)
        PGOTO_DONE_VOID;

    free(hist->range);
    free(hist->bin);
    free(hist);

done:
    fflush(stdout);
    FUNC_LEAVE_VOID;
}

void
PDC_print_hist(pdc_histogram_t *hist)
{
    int i;

    FUNC_ENTER(NULL);

    if (NULL == hist)
        PGOTO_DONE_VOID;

    for (i = 0; i < hist->nbin; i++) {
        if (i != hist->nbin - 1)
            printf("[%.2f, %.2f): %" PRIu64 "\n", hist->range[i * 2], hist->range[i * 2 + 1], hist->bin[i]);
        else
            printf("[%.2f, %.2f]: %" PRIu64 "\n", hist->range[i * 2], hist->range[i * 2 + 1], hist->bin[i]);
    }
    printf("\n\n");

done:
    fflush(stdout);
    FUNC_LEAVE_VOID;
}

pdc_histogram_t *
PDC_dup_hist(pdc_histogram_t *hist)
{
    pdc_histogram_t *ret_value = NULL;
    pdc_histogram_t *res;
    int              nbin;

    FUNC_ENTER(NULL);

    if (NULL == hist)
        PGOTO_DONE(NULL);

    nbin       = hist->nbin;
    res        = (pdc_histogram_t *)malloc(sizeof(pdc_histogram_t));
    res->dtype = hist->dtype;
    res->nbin  = nbin;
    res->range = (double *)calloc(sizeof(double), nbin * 2);
    res->bin   = (uint64_t *)calloc(sizeof(uint64_t), nbin);
    res->incr  = hist->incr;

    memcpy(res->range, hist->range, sizeof(double) * nbin * 2);
    memcpy(res->bin, hist->bin, sizeof(double) * nbin);

    ret_value = res;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

pdc_histogram_t *
PDC_merge_hist(int n, pdc_histogram_t **hists)
{
    pdc_histogram_t *ret_value = NULL;
    int              i, j, incr_max_idx = 0, hi, lo, mid = 0;
    double           tot_min, tot_max, incr_max;
    pdc_histogram_t *res;

    FUNC_ENTER(NULL);

    if (n == 0 || NULL == hists)
        PGOTO_DONE(NULL);

    tot_min  = hists[0]->range[1];
    tot_max  = hists[0]->range[2 * hists[0]->nbin - 2];
    incr_max = hists[0]->incr;

    for (i = 1; i < n; i++) {
        if (hists[i]->dtype != hists[i - 1]->dtype) {
            PGOTO_ERROR(NULL, "== cannot merge histograms of different types!");
        }
        if (hists[i]->incr > incr_max) {
            incr_max     = hists[i]->incr;
            incr_max_idx = i;
        }
        if (hists[i]->range[0] < tot_min)
            tot_min = hists[i]->range[0];
        if (hists[i]->range[2 * hists[i]->nbin - 1] > tot_max)
            tot_max = hists[i]->range[2 * hists[i]->nbin - 1];
    }

    // Merge strategy: Use the histogram with largest incr as base and merge others to it
    //                 TODO?: keep the non-overlapping histograms with smaller incr

    // Duplicate the base hist to result
    res = PDC_dup_hist(hists[incr_max_idx]);
    if (NULL == res)
        PGOTO_ERROR(NULL, "== error with PDC_dup_hist!");

    res->range[0]                   = tot_min;
    res->range[(res->nbin) * 2 - 1] = tot_max;

    for (i = 0; i < n; i++) {  // for each histogram
        if (i == incr_max_idx) // skip the base
            continue;

        for (j = 0; j < hists[i]->nbin; j++) { // for each bin in a non-base hist
            lo = 0;
            hi = res->nbin - 1;
            while (lo <= hi) {
                mid = lo + (hi - lo) / 2;
                if (hists[i]->range[2 * j] >= res->range[2 * mid]) {
                    if (hists[i]->range[2 * j + 1] <= res->range[2 * mid + 1])
                        break;
                    lo = mid + 1;
                }
                else
                    hi = mid - 1;
            }
            res->bin[mid] += hists[i]->bin[j];
        }
    }

    ret_value = res;

done:
    fflush(stdout);
    FUNC_LEAVE(ret_value);
}

void
PDC_copy_hist(pdc_histogram_t *to, pdc_histogram_t *from)
{
    int nbin = from->nbin;

    FUNC_ENTER(NULL);

    to->incr  = from->incr;
    to->dtype = from->dtype;
    if (NULL == to->range)
        to->range = (double *)calloc(sizeof(double), nbin * 2);
    memcpy(to->range, from->range, sizeof(double) * nbin * 2);
    if (NULL == to->bin)
        to->bin = (uint64_t *)calloc(sizeof(uint64_t), nbin);
    memcpy(to->bin, from->bin, sizeof(uint64_t) * nbin);
    to->nbin = from->nbin;

    FUNC_LEAVE_VOID;
}
