#ifndef PDC_TF_PROFILER_H
#define PDC_TF_PROFILER_H

#ifdef CUDA_ENABLED
/* Only pdc_tf_nvml_profiler_update's implementation (pdc_tf_profiler.c)
 * actually needs NVML types; nothing declared below does, so gating just
 * this include keeps the rest of this header (and everyone who includes
 * it -- pdc_tf_server.c, pdc_an_server.c, pdc_tf_poly_sched.c) usable
 * without the CUDA toolkit present at all. */
#include <nvml.h>
#endif
#include "pdc_logger.h"
#include "pdc_timing.h"
#include "pdc_malloc.h"

#define PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE 1 /* single fresh sample */

extern int          pdc_tf_profiler_init;
extern int          pdc_tf_profiler_nvml_init;
extern int          pdc_tf_profiler_cpu_init;
extern unsigned int pdc_tf_profiler_nvml_device_count;

typedef struct {
    double        gpu_utilization;
    int           memory_utilization;
    unsigned long memory_total;
    unsigned long memory_used;
    unsigned long memory_free;
    unsigned int  power_mw;
} pdc_tf_profiler_nvml_sample_t;

typedef struct {
    double cpu_utilization;
} pdc_tf_profiler_cpu_sample_t;

typedef struct {
    pdc_tf_profiler_nvml_sample_t *nvml_samples[PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE];
    int                            nvml_head;
    pdc_tf_profiler_cpu_sample_t * cpu_samples[PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE];
    int                            cpu_head;
    unsigned int                   nvml_device_count;
} pdc_tf_profiler_samples_t;

extern pdc_tf_profiler_samples_t pdc_tf_profiler_samples;

/* main update — called from server loop */
perr_t pdc_tf_update_profiler(double elapsed_total_time_sec, double elapsed_progress_time_sec);

/* fresh NVML sample — called immediately before scheduling decision */
perr_t pdc_tf_nvml_profiler_update(void);

/* utilization queries */
double        pdc_tf_avg_gpu_utilization(unsigned int device_index);
double        pdc_tf_avg_cpu_utilization(void);
double        pdc_tf_avg_gpu_power_mw(unsigned int device_index);
unsigned long pdc_tf_avg_gpu_mem_used(unsigned int device_index);

/* lag storage for polynomial scheduler */
void pdc_tf_update_device_lag(unsigned int device_index, double h2d_ms, double comp_ms, double d2h_ms,
                              double total_ms);
void pdc_tf_get_device_lag(unsigned int device_index, double *prev_h2d_ms, double *prev_comp_ms,
                           double *prev_d2h_ms, double *prev_total_ms);

#endif /* PDC_TF_PROFILER_H */