#include "pdc_tf_profiler.h"
#include <pthread.h>

int                       pdc_tf_profiler_init              = 0;
int                       pdc_tf_profiler_nvml_init         = 0;
int                       pdc_tf_profiler_cpu_init          = 0;
unsigned int              pdc_tf_profiler_nvml_device_count = 0;
pdc_tf_profiler_samples_t pdc_tf_profiler_samples           = {0};
static pthread_mutex_t    profiler_lock                     = PTHREAD_MUTEX_INITIALIZER;

/* ── GPU utilization query ────────────────────────────────────────────────── */
double
pdc_tf_avg_gpu_utilization(unsigned int device_index)
{
    if (device_index >= pdc_tf_profiler_samples.nvml_device_count)
        return -1.0;

    int    count = 0;
    double sum   = 0.0;
    for (int i = 0; i < PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE; i++) {
        pdc_tf_profiler_nvml_sample_t *samples = pdc_tf_profiler_samples.nvml_samples[i];
        if (samples != NULL) {
            sum += samples[device_index].gpu_utilization;
            count++;
        }
    }
    double res = (count > 0) ? (sum / count) : 0.0;
    if (res < 0.0 || res > 1.0)
        LOG_ERROR("Average GPU utilization out of expected range: %f\n", res);
    return res;
}

/* ── CPU utilization query ────────────────────────────────────────────────── */
double
pdc_tf_avg_cpu_utilization(void)
{
    int    count = 0;
    double sum   = 0.0;
    for (int i = 0; i < PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE; i++) {
        pdc_tf_profiler_cpu_sample_t *sample = pdc_tf_profiler_samples.cpu_samples[i];
        if (sample != NULL) {
            sum += sample->cpu_utilization;
            count++;
        }
    }
    double res = (count > 0) ? (sum / count) : 0.0;
    if (res < 0.0 || res > 1.0)
        LOG_ERROR("Average CPU utilization out of expected range: %f\n", res);
    return res;
}

/* ── NVML profiler update — non-static so pdc_tf_server.c can call it ─────── */
#ifndef CUDA_ENABLED
/* No CUDA toolkit (and therefore no NVML) available to link against --
 * pdc_tf_profiler_nvml_device_count stays 0, which every reader above
 * already treats as "no GPU data available" (pdc_tf_avg_gpu_utilization
 * returns -1.0, pdc_tf_get_device_lag returns all -1.0 sentinels, etc.),
 * so every caller works unchanged, just always on the CPU-only path. */
perr_t
pdc_tf_nvml_profiler_update(void)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
#else
perr_t
pdc_tf_nvml_profiler_update(void)
{
    FUNC_ENTER(NULL);
    perr_t       ret_value = SUCCEED;
    nvmlReturn_t nvml_ret;

    if (!pdc_tf_profiler_nvml_init) {
        pdc_tf_profiler_nvml_init = 1;

        nvml_ret = nvmlInit();
        if (nvml_ret != NVML_SUCCESS) {
            LOG_ERROR("Failed to initialize NVML: %s\n", nvmlErrorString(nvml_ret));
            PGOTO_ERROR(FAIL, "Failed to initialize NVML");
        }

        nvml_ret = nvmlDeviceGetCount(&pdc_tf_profiler_nvml_device_count);
        if (nvml_ret != NVML_SUCCESS) {
            LOG_ERROR("Failed to get NVML device count: %s\n", nvmlErrorString(nvml_ret));
            PGOTO_ERROR(FAIL, "Failed to get NVML device count");
        }

        pdc_tf_profiler_samples.nvml_device_count = pdc_tf_profiler_nvml_device_count;
        for (int i = 0; i < PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE; i++)
            pdc_tf_profiler_samples.nvml_samples[i] = NULL;
    }

    if (pdc_tf_profiler_nvml_device_count == 0)
        goto done;

    pdc_tf_profiler_nvml_sample_t *nvml_sample = (pdc_tf_profiler_nvml_sample_t *)PDC_malloc(
        sizeof(pdc_tf_profiler_nvml_sample_t) * pdc_tf_profiler_nvml_device_count);

    for (int i = 0; i < (int)pdc_tf_profiler_nvml_device_count; i++) {
        nvmlDevice_t device;
        nvml_ret = nvmlDeviceGetHandleByIndex(i, &device);
        if (nvml_ret != NVML_SUCCESS)
            continue;

        nvmlMemory_t mem_info;
        nvml_ret = nvmlDeviceGetMemoryInfo(device, &mem_info);
        if (nvml_ret != NVML_SUCCESS)
            continue;

        nvmlUtilization_t util_info;
        nvml_ret = nvmlDeviceGetUtilizationRates(device, &util_info);
        if (nvml_ret != NVML_SUCCESS)
            continue;

        nvml_sample[i].gpu_utilization    = (double)util_info.gpu / 100.0;
        nvml_sample[i].memory_utilization = util_info.memory;
        nvml_sample[i].memory_total       = mem_info.total;
        nvml_sample[i].memory_used        = mem_info.used;
        nvml_sample[i].memory_free        = mem_info.free;

        unsigned int power_mw = 0;
        nvmlDeviceGetPowerUsage(device, &power_mw);
        nvml_sample[i].power_mw = power_mw;

        LOG_DEBUG("NVML device %d: util=%.1f%% power=%u mW\n", i, (double)util_info.gpu, power_mw);
    }

    /* with buffer size 1, always write to slot 0 */
    int idx = pdc_tf_profiler_samples.nvml_head % PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE;
    if (pdc_tf_profiler_samples.nvml_samples[idx] != NULL)
        PDC_free(pdc_tf_profiler_samples.nvml_samples[idx]);
    pdc_tf_profiler_samples.nvml_samples[idx] = nvml_sample;
    pdc_tf_profiler_samples.nvml_head++;

done:
    FUNC_LEAVE(ret_value);
}
#endif /* CUDA_ENABLED */

/* ── CPU profiler update ──────────────────────────────────────────────────── */
static perr_t
pdc_tf_cpu_profiler_update(double elapsed_total_time_sec, double elapsed_progress_time_sec)
{
    FUNC_ENTER(NULL);
    perr_t ret_value = SUCCEED;

    if (!pdc_tf_profiler_cpu_init) {
        pdc_tf_profiler_cpu_init = 1;
        for (int i = 0; i < PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE; i++)
            pdc_tf_profiler_samples.cpu_samples[i] = NULL;
    }

    double cpu_util = 1.0 - (elapsed_progress_time_sec / elapsed_total_time_sec);
    if (cpu_util < 0.0 || cpu_util > 1.0)
        LOG_ERROR("Computed CPU utilization out of expected range: %f\n", cpu_util);

    pdc_tf_profiler_cpu_sample_t *cpu_sample =
        (pdc_tf_profiler_cpu_sample_t *)PDC_malloc(sizeof(pdc_tf_profiler_cpu_sample_t));
    cpu_sample->cpu_utilization = cpu_util;

    int idx = pdc_tf_profiler_samples.cpu_head % PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE;
    if (pdc_tf_profiler_samples.cpu_samples[idx] != NULL)
        PDC_free(pdc_tf_profiler_samples.cpu_samples[idx]);
    pdc_tf_profiler_samples.cpu_samples[idx] = cpu_sample;
    pdc_tf_profiler_samples.cpu_head++;

    FUNC_LEAVE(ret_value);
}

/* ── main update — called from server loop ───────────────────────────────── */
perr_t
pdc_tf_update_profiler(double elapsed_total_time_sec, double elapsed_progress_time_sec)
{
    FUNC_ENTER(NULL);
    perr_t ret_value = SUCCEED;

    if (!pdc_tf_profiler_init) {
        pdc_tf_profiler_init              = 1;
        pdc_tf_profiler_samples.cpu_head  = 0;
        pdc_tf_profiler_samples.nvml_head = 0;
    }

    ret_value = pdc_tf_nvml_profiler_update();
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to update NVML profiler");

    ret_value = pdc_tf_cpu_profiler_update(elapsed_total_time_sec, elapsed_progress_time_sec);
    if (ret_value != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to update CPU profiler");

done:
    FUNC_LEAVE(ret_value);
}

/* ── per-device lag storage ──────────────────────────────────────────────── */
#define PDC_TF_MAX_GPU_DEVICES 8

typedef struct {
    double prev_h2d_ms;
    double prev_comp_ms;
    double prev_d2h_ms;
    double prev_total_ms;
} pdc_tf_device_lag_t;

static pdc_tf_device_lag_t pdc_tf_device_lag[PDC_TF_MAX_GPU_DEVICES];
static int                 pdc_tf_device_lag_initialized = 0;

static void
pdc_tf_lag_init(void)
{
    for (int i = 0; i < PDC_TF_MAX_GPU_DEVICES; i++) {
        pdc_tf_device_lag[i].prev_h2d_ms   = -1.0;
        pdc_tf_device_lag[i].prev_comp_ms  = -1.0;
        pdc_tf_device_lag[i].prev_d2h_ms   = -1.0;
        pdc_tf_device_lag[i].prev_total_ms = -1.0;
    }
    pdc_tf_device_lag_initialized = 1;
}

void
pdc_tf_update_device_lag(unsigned int device_index, double h2d_ms, double comp_ms, double d2h_ms,
                         double total_ms)
{
    if (!pdc_tf_device_lag_initialized)
        pdc_tf_lag_init();
    if (device_index >= PDC_TF_MAX_GPU_DEVICES)
        return;
    pdc_tf_device_lag[device_index].prev_h2d_ms   = h2d_ms;
    pdc_tf_device_lag[device_index].prev_comp_ms  = comp_ms;
    pdc_tf_device_lag[device_index].prev_d2h_ms   = d2h_ms;
    pdc_tf_device_lag[device_index].prev_total_ms = total_ms;
}

void
pdc_tf_get_device_lag(unsigned int device_index, double *prev_h2d_ms, double *prev_comp_ms,
                      double *prev_d2h_ms, double *prev_total_ms)
{
    if (!pdc_tf_device_lag_initialized)
        pdc_tf_lag_init();
    if (device_index >= PDC_TF_MAX_GPU_DEVICES || device_index >= pdc_tf_profiler_nvml_device_count) {
        *prev_h2d_ms = *prev_comp_ms = *prev_d2h_ms = *prev_total_ms = -1.0;
        return;
    }
    *prev_h2d_ms   = pdc_tf_device_lag[device_index].prev_h2d_ms;
    *prev_comp_ms  = pdc_tf_device_lag[device_index].prev_comp_ms;
    *prev_d2h_ms   = pdc_tf_device_lag[device_index].prev_d2h_ms;
    *prev_total_ms = pdc_tf_device_lag[device_index].prev_total_ms;
}

double
pdc_tf_avg_gpu_power_mw(unsigned int device_index)
{
    if (device_index >= pdc_tf_profiler_nvml_device_count)
        return 0.0;
    double sum   = 0.0;
    int    count = 0;
    for (int i = 0; i < PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE; i++) {
        if (pdc_tf_profiler_samples.nvml_samples[i] == NULL)
            continue;
        sum += (double)pdc_tf_profiler_samples.nvml_samples[i][device_index].power_mw;
        count++;
    }
    return (count > 0) ? (sum / count) : 0.0;
}

unsigned long
pdc_tf_avg_gpu_mem_used(unsigned int device_index)
{
    if (device_index >= pdc_tf_profiler_nvml_device_count)
        return 0UL;
    double sum   = 0.0;
    int    count = 0;
    for (int i = 0; i < PDC_TF_PROFILE_SAMPLE_VECTOR_MAX_SIZE; i++) {
        if (pdc_tf_profiler_samples.nvml_samples[i] == NULL)
            continue;
        sum += (double)pdc_tf_profiler_samples.nvml_samples[i][device_index].memory_used;
        count++;
    }
    return (count > 0) ? (unsigned long)(sum / count) : 0UL;
}