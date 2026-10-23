/*
 * pdc_tf_poly_sched.c
 *
 * Model: degree-3 polynomial, 4 features, 35 terms
 * Features: data_size_mb, nvml_gpu_util, prev_h2d_ms, prev_d2h_ms
 * Target:   total_ms (H2D + compression + D2H)
 * R²=0.9723, MAE=2.428 ms
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "pdc_tf_poly_sched.h"
#include "pdc_tf_profiler.h"
#include "pdc_logger.h"

pdc_tf_poly_model_t pdc_tf_poly_model = {.initialized = 0};

int
pdc_tf_poly_sched_init(const char *coeff_file)
{
    FILE *f = fopen(coeff_file, "r");
    if (!f) {
        LOG_ERROR("pdc_tf_poly_sched_init: cannot open %s\n", coeff_file);
        return -1;
    }

    int n_terms = 0;
    if (fscanf(f, "%d", &n_terms) != 1 || n_terms <= 0 || n_terms > PDC_POLY_MAX_TERMS) {
        LOG_ERROR("pdc_tf_poly_sched_init: invalid n_terms=%d in %s\n", n_terms, coeff_file);
        fclose(f);
        return -1;
    }

    pdc_tf_poly_model.n_terms = n_terms;

    for (int t = 0; t < n_terms; t++) {
        int    p[PDC_POLY_N_FEATURES];
        double coeff;
        int    nread = 0;
        for (int i = 0; i < PDC_POLY_N_FEATURES; i++)
            nread += (fscanf(f, "%d", &p[i]) == 1) ? 1 : 0;
        nread += (fscanf(f, "%lf", &coeff) == 1) ? 1 : 0;

        if (nread != PDC_POLY_N_FEATURES + 1) {
            LOG_ERROR("pdc_tf_poly_sched_init: parse error at term %d\n", t);
            fclose(f);
            return -1;
        }
        for (int i = 0; i < PDC_POLY_N_FEATURES; i++)
            pdc_tf_poly_model.powers[t][i] = p[i];
        pdc_tf_poly_model.coefficients[t] = coeff;
    }

    fclose(f);
    pdc_tf_poly_model.initialized = 1;
    LOG_WARNING("pdc_tf_poly_sched_init: loaded %d polynomial terms from %s\n", n_terms, coeff_file);
    return 0;
}

void
pdc_tf_poly_sched_finalize(void)
{
    pdc_tf_poly_model.initialized = 0;
}

double
pdc_tf_poly_predict(const double features[PDC_POLY_N_FEATURES])
{
    if (!pdc_tf_poly_model.initialized)
        return -1.0;

    double f[PDC_POLY_N_FEATURES];
    for (int i = 0; i < PDC_POLY_N_FEATURES; i++)
        f[i] = (features[i] < 0.0) ? 0.0 : features[i];

    double result = 0.0;
    for (int t = 0; t < pdc_tf_poly_model.n_terms; t++) {
        double term = pdc_tf_poly_model.coefficients[t];
        for (int i = 0; i < PDC_POLY_N_FEATURES; i++) {
            int exp = pdc_tf_poly_model.powers[t][i];
            if (exp == 0)
                continue;
            double base = f[i];
            for (int e = 0; e < exp; e++)
                term *= base;
        }
        result += term;
    }
    return (result < 5.0) ? 5.0 : result;
}

int
pdc_tf_poly_select_gpu(double data_size_mb)
{
    unsigned int n_devs = pdc_tf_profiler_nvml_device_count;
    if (n_devs == 0)
        return 0;

    if (!pdc_tf_poly_model.initialized) {
        LOG_WARNING("pdc_tf_poly_select_gpu: model not initialized, "
                    "falling back to lowest utilization\n");
        double       best_util = 2.0;
        unsigned int best_dev  = 0;
        for (unsigned int i = 0; i < n_devs; i++) {
            double u = pdc_tf_avg_gpu_utilization(i);
            if (u < best_util) {
                best_util = u;
                best_dev  = i;
            }
        }
        return (int)best_dev;
    }

    double       best_pred = 1e18;
    unsigned int best_dev  = 0;

    for (unsigned int i = 0; i < n_devs; i++) {
        double prev_h2d_ms, prev_comp_ms, prev_d2h_ms, prev_total_ms;
        pdc_tf_get_device_lag(i, &prev_h2d_ms, &prev_comp_ms, &prev_d2h_ms, &prev_total_ms);

        /* feature order must match export_coefficients.py FEATURES list:
         *   [0] data_size_mb
         *   [1] nvml_gpu_util  (0-100 scale)
         *   [2] prev_h2d_ms
         *   [3] prev_d2h_ms
         */
        double features[PDC_POLY_N_FEATURES];
        features[0] = data_size_mb;
        features[1] = pdc_tf_avg_gpu_utilization(i) * 100.0;
        features[2] = prev_h2d_ms;
        features[3] = prev_d2h_ms;

        double pred = pdc_tf_poly_predict(features);
        /* floor cold-start GPUs (no lag data yet) at 5ms so they
         * don't appear artificially cheaper than warmed-up GPUs */
        if (prev_h2d_ms < 0.0 || prev_d2h_ms < 0.0)
            pred = fmax(pred, 5.0);

        LOG_WARNING("pdc_tf_poly_select_gpu: GPU %u  data=%.1f MB  util=%.1f%%  "
                    "prev_h2d=%.2f ms  prev_d2h=%.2f ms  predicted=%.2f ms\n",
                    i, data_size_mb, features[1], prev_h2d_ms, prev_d2h_ms, pred);

        if (pred < best_pred) {
            best_pred = pred;
            best_dev  = i;
        }
    }

    LOG_WARNING("pdc_tf_poly_select_gpu: selected GPU %u (predicted %.2f ms)\n", best_dev, best_pred);
    return (int)best_dev;
}

void
pdc_tf_poly_update(unsigned int device_index, double h2d_ms, double comp_ms, double d2h_ms, double total_ms)
{
    pdc_tf_update_device_lag(device_index, h2d_ms, comp_ms, d2h_ms, total_ms);
}