#include <assert.h>
#include <string.h>
#include <time.h>
#ifdef CUDA_ENABLED
#include <cuda_runtime.h>
#endif

#include "pdc_tf_server.h"
#include "pdc_malloc.h"
#include "pdc_client_server_common.h"
#include "pdc_vector.h"
#include "pdc_tf_user.h"
#include "pdc_tf_profiler.h"
#include "pdc_server_region_cache.h"
#include "pdc_tf_poly_sched.h"

PDC_VECTOR *tf_obj_id_to_dg_vector_g = NULL;

#ifndef IS_PDC_SERVER
perr_t
PDCtf_store_json_mapping(pdcid_t obj_id, char *json_filepath, char *cur_state, char *client_state,
                         char *store_state, uint64_t *offset, uint64_t *size, uint8_t ndim,
                         pdc_var_type_t pdc_var_type)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
perr_t
PDCtf_exec_graph(pdc_dg_t *dg, uint64_t flat_conceptual_offset, char *cur_state, char *desired_state,
                 pdc_tf_region_t input_region, pdc_tf_region_t *output_region, void **input, int is_write,
                 pdc_tf_sched_mode_t sched_mode)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(SUCCEED);
}
#else
static pdcid_t cur_graph = 1;

perr_t
PDCtf_store_json_mapping(pdcid_t obj_id, char *json_filepath, char *cur_state, char *client_state,
                         char *store_state, uint64_t *offset, uint64_t *size, uint8_t ndim,
                         pdc_var_type_t pdc_var_type)
{
    FUNC_ENTER(NULL);

    LOG_DEBUG("PDCtf_store_json_mapping was called\n");

    perr_t ret_value = SUCCEED;

    if (tf_obj_id_to_dg_vector_g == NULL)
        tf_obj_id_to_dg_vector_g = pdc_vector_create(8, 2.0);
    if (tf_obj_id_to_dg_vector_g == NULL)
        PGOTO_ERROR(FAIL, "tf_obj_id_to_dg_vector_g was NULL");

    // Find object in mapping if it exists
    pdc_tf_obj_id_to_dg_t *obj_id_to_dg                = NULL;
    PDC_VECTOR_ITERATOR *  tf_obj_id_to_dg_vector_iter = pdc_vector_iterator_new(tf_obj_id_to_dg_vector_g);
    while (pdc_vector_iterator_has_next(tf_obj_id_to_dg_vector_iter)) {
        pdc_tf_obj_id_to_dg_t *cur_obj_id_to_dg =
            (pdc_tf_obj_id_to_dg_t *)pdc_vector_iterator_next(tf_obj_id_to_dg_vector_iter);
        if (cur_obj_id_to_dg->obj_id == obj_id) {
            obj_id_to_dg = cur_obj_id_to_dg;
            break;
        }
    }
    pdc_vector_iterator_destroy(tf_obj_id_to_dg_vector_iter);

    // If object has attached graph make sure it is the same as the passed in graph
    if (obj_id_to_dg != NULL) {
        char *graph_json_filepath = (char *)(obj_id_to_dg->dg->data);
        if (strcmp(json_filepath, graph_json_filepath)) {
            PGOTO_ERROR(FAIL, "Passed graph filepath %s didn't match stored filepath %s", json_filepath,
                        graph_json_filepath);
        }
    }

    // Region mappings for passed in region and object
    pdc_tf_region_mapping_t *region_mapping = NULL;

    // If object doesn't have a directed graph create a new one
    if (obj_id_to_dg == NULL) {
        LOG_DEBUG("Creating directed graph for object\n");

        pdc_dg_t *dg = PDCtf_dg_json_create_common(json_filepath);
        if (dg == NULL)
            PGOTO_ERROR(FAIL, "Failed to load JSON\n");

        // Create new obj id to dg and append to vector
        obj_id_to_dg = PDC_malloc(sizeof(pdc_tf_obj_id_to_dg_t));
        pdc_vector_add(tf_obj_id_to_dg_vector_g, obj_id_to_dg);

        obj_id_to_dg->dg     = dg;
        obj_id_to_dg->obj_id = obj_id;

        // Create a new region mapping vector and mapping entry
        obj_id_to_dg->pdc_tf_obj.region_mappings_vector = pdc_vector_create(8, 2.0);
        region_mapping                                  = PDC_calloc(1, sizeof(pdc_tf_region_mapping_t));
        pdc_vector_add(obj_id_to_dg->pdc_tf_obj.region_mappings_vector, region_mapping);
    }

    // Check if this mapping already exists
    if (region_mapping == NULL) {
        PDC_VECTOR_ITERATOR *region_mapping_iter =
            pdc_vector_iterator_new(obj_id_to_dg->pdc_tf_obj.region_mappings_vector);
        while (pdc_vector_iterator_has_next(region_mapping_iter)) {
            pdc_tf_region_mapping_t *cur_region_mapping = pdc_vector_iterator_next(region_mapping_iter);
            if (cur_region_mapping == NULL)
                PGOTO_ERROR(FAIL, "cur_region_mapping was NULL");
            uint64_t *conceptual_offset = cur_region_mapping->conceptual_offset;
            if (memcmp(offset, conceptual_offset, ndim * sizeof(uint64_t)) == 0) {
                region_mapping = cur_region_mapping;
                break;
            }
        }
        pdc_vector_iterator_destroy(region_mapping_iter);
    }

    // If this is null we need to append this mapping
    if (region_mapping == NULL) {
        region_mapping = PDC_calloc(1, sizeof(pdc_tf_region_mapping_t));
        pdc_vector_add(obj_id_to_dg->pdc_tf_obj.region_mappings_vector, region_mapping);
    }

    pdc_tf_region_t *conceptual_region = &region_mapping->conceptual_region;
    uint64_t *       conceptual_offset = region_mapping->conceptual_offset;

    PDC_get_var_type_size(pdc_var_type);

    // copy region information into conceptual region
    conceptual_region->ndim         = ndim;
    conceptual_region->pdc_var_type = pdc_var_type;
    memcpy(conceptual_offset, offset, ndim * sizeof(uint64_t));
    memcpy(conceptual_region->size, size, ndim * sizeof(uint64_t));

    LOG_DEBUG("obj_id=%" PRIu64 " ndim=%u\n", obj_id, ndim);
    for (int i = 0; i < ndim; i++) {
        LOG_DEBUG("  offset[%d]=%" PRIu64 " size[%d]=%" PRIu64 "\n", i, conceptual_offset[i], i,
                  conceptual_region->size[i]);
    }

    // This mapping may be a reused one found above rather than the fresh PDC_calloc'd one, so free
    // any strings it already holds before overwriting them -- PDCtf_store_json_mapping is piggybacked
    // on every region-transfer RPC for an attached graph, not just the initial attach.
    region_mapping->region_state.cur_state    = (char *)PDC_free(region_mapping->region_state.cur_state);
    region_mapping->region_state.client_state = (char *)PDC_free(region_mapping->region_state.client_state);
    region_mapping->region_state.store_state  = (char *)PDC_free(region_mapping->region_state.store_state);

    region_mapping->region_state.cur_state    = strdup(cur_state);
    region_mapping->region_state.client_state = strdup(client_state);
    region_mapping->region_state.store_state  = strdup(store_state);
    region_mapping->region_state.dg_id        = cur_graph;

done:
    FUNC_LEAVE(ret_value);
}

static double
rolling_avg(double *history)
{
    double avg = 0.0;
    for (int i = 0; i < NUM_TF_FUNC_TIMES; i++)
        avg += history[i];
    return avg / NUM_TF_FUNC_TIMES;
}

static double
pure_exec_time(double transform_time, pdc_tf_internal_param internal_params)
{
    double h2d = internal_params.host_to_dev_time >= 0 ? internal_params.host_to_dev_time : 0.0;
    double d2h = internal_params.dev_to_host_time >= 0 ? internal_params.dev_to_host_time : 0.0;
    return fmax(0.0, transform_time - h2d - d2h);
}

static void
update_transfer_time(double measured_time, double *history, uint32_t *index, const char *label,
                     const char *func_name)
{
    if (measured_time < 0)
        return;
    history[*index] = measured_time;
    if (pdc_server_rank_g == 0)
        LOG_WARNING("SCHED: updated %s[%d]=%.4f func=%s\n", label, *index, measured_time, func_name);
    *index = (*index + 1) % NUM_TF_FUNC_TIMES;
}

static void
update_exec_time(pdc_tf_func_t *f, double projected_time)
{
    f->exec_avg_time[f->cur_exec_avg_time_index] = projected_time;
    if (pdc_server_rank_g == 0)
        LOG_WARNING("SCHED: updated %s exec_times[%d]=%.4f new_avg=%.4f func=%s\n",
                    f->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU", f->cur_exec_avg_time_index, projected_time,
                    rolling_avg(f->exec_avg_time), f->name);
    f->cur_exec_avg_time_index = (f->cur_exec_avg_time_index + 1) % NUM_TF_FUNC_TIMES;
}

static void
update_func_history(pdc_tf_func_t *f, pdc_tf_internal_param internal_params, double projected_time)
{
    update_transfer_time(internal_params.host_to_dev_time, f->host_to_dev_avg_time,
                         &f->cur_host_to_dev_avg_time_index, "host_to_dev_times", f->name);
    update_exec_time(f, projected_time);
    update_transfer_time(internal_params.dev_to_host_time, f->dev_to_host_avg_time,
                         &f->cur_dev_to_host_avg_time_index, "dev_to_host_times", f->name);
}

static double
expected_exec_time(pdc_tf_func_t *f, double avg_cpu_utilization, double min_avg_gpu_utilization)
{
    double last_avg = rolling_avg(f->exec_avg_time);
    return (f->dev == PDC_TF_CPU_DEVICE) ? last_avg / fmax(0.1, 1.0 - avg_cpu_utilization)
                                         : last_avg / fmax(0.1, 1.0 - min_avg_gpu_utilization);
}

static uint32_t
select_best_edge(pdc_dg_edge_t *edges_out, uint32_t j, uint32_t cur_edges_between_vertices,
                 double avg_cpu_utilization, double min_avg_gpu_utilization,
                 int *min_gpu_utilization_device_index_out, bool always_use_gpu, bool *force_selected_out,
                 double predicted_gpu_ms)
{
    uint32_t best_edge_idx  = j;
    bool     force_selected = false;

    // Static schedule: always pick GPU edge if available
    if (always_use_gpu || close_time_g) {
        for (uint32_t idx = j; idx < j + cur_edges_between_vertices; idx++) {
            pdc_tf_func_t *f = (pdc_tf_func_t *)edges_out[idx].data;
            if (f->dev == PDC_TF_GPU_DEVICE) {
                best_edge_idx                         = idx;
                *min_gpu_utilization_device_index_out = 0;
                force_selected                        = true;
                if (pdc_server_rank_g == 0 && always_use_gpu)
                    LOG_WARNING("ALWAYS_USE_GPU: selecting edge %u on GPU 0\n", idx);
                if (close_time_g && pdc_server_rank_g == 0)
                    LOG_WARNING("CLOSE_TIME: selecting edge %u on GPU 0\n", idx);
                break;
            }
        }
    }

    // Dynamic schedule: select based on expected time and utilization
    if (!force_selected) {
        double best_expected_time = 1e9;
        for (uint32_t idx = j; idx < j + cur_edges_between_vertices; idx++) {
            pdc_tf_func_t *f = (pdc_tf_func_t *)edges_out[idx].data;

            double exp_time;
            if (f->dev == PDC_TF_GPU_DEVICE && predicted_gpu_ms >= 0.0) {
                exp_time = predicted_gpu_ms;
            }
            else {
                double cpu_rolling = rolling_avg(f->exec_avg_time);
                exp_time           = (cpu_rolling < 0.001)
                               ? 0.750
                               : expected_exec_time(f, avg_cpu_utilization, min_avg_gpu_utilization);
            }
            if (pdc_server_rank_g == 0)
                LOG_WARNING("SCHED: edge %u func=%s dev=%s last_avg_time=%.4f expected_time=%.4f "
                            "best_so_far=%.4f\n",
                            idx, f->name, f->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU",
                            rolling_avg(f->exec_avg_time), exp_time, best_expected_time);

            if (exp_time < best_expected_time) {
                best_expected_time = exp_time;
                best_edge_idx      = idx;
            }
        }

        if (pdc_server_rank_g == 0) {
            pdc_tf_func_t *chosen = (pdc_tf_func_t *)edges_out[best_edge_idx].data;
            LOG_WARNING("SCHED: chose edge %u func=%s dev=%s expected_time=%.4f\n", best_edge_idx,
                        chosen->name, chosen->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU", best_expected_time);
        }
    }

    *force_selected_out = force_selected;
    return best_edge_idx;
}

perr_t
PDCtf_exec_graph(pdc_dg_t *dg, uint64_t flat_conceptual_offset, char *cur_state, char *desired_state,
                 pdc_tf_region_t input_region, pdc_tf_region_t *output_region, void **input, int is_write,
                 pdc_tf_sched_mode_t sched_mode)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    PDC_get_var_type_size(input_region.pdc_var_type);

    pdc_tf_state_t tf_input_state  = {.name = cur_state};
    pdc_tf_state_t tf_output_state = {.name = desired_state};
    void *         input_state     = (void *)&tf_input_state;
    void *         output_state    = (void *)&tf_output_state;

    pdc_dg_edge_t *edges_out = NULL;
    uint32_t       num_edges;

    if (PDCdg_shortest_path(dg, input_state, output_state, &edges_out, &num_edges)) {
        memcpy(output_region, &input_region, sizeof(pdc_tf_region_t));

        for (uint32_t j = 0; j < num_edges;) {
            pdc_dg_vertex_id_t v1_id = edges_out[j].v1_id;
            pdc_dg_vertex_id_t v2_id = edges_out[j].v2_id;

            uint32_t cur_edges_between_vertices = 0;
            uint32_t k                          = j;
            while (k < num_edges && edges_out[k].v1_id == v1_id && edges_out[k].v2_id == v2_id) {
                cur_edges_between_vertices++;
                k++;
            }

            /* hoisted so execute and post-execution blocks can both access */
            int      min_gpu_utilization_device_index = 0;
            double   predicted_total_ms               = -1.0;
            double   avg_cpu_utilization              = 0.0;
            double   min_avg_gpu_utilization          = 0.0;
            uint32_t best_edge_idx                    = j;

            /* ── fast path: single edge, skip profiling and scheduling ── */
            if (cur_edges_between_vertices > 1) {
                double data_size_mb = (double)PDCtf_get_pdc_region_t_bytes(input_region) / 1e6;
                avg_cpu_utilization = pdc_tf_avg_cpu_utilization();

                /* ── GPU selection ────────────────────────────────────────────
                 * DYNAMIC: polynomial predicts total_ms per GPU, pick lowest.
                 * STATIC:  always use GPU 0 (baseline).
                 * ─────────────────────────────────────────────────────────── */
                if (sched_mode == PDC_TF_SCHED_DYNAMIC) {
                    pdc_tf_nvml_profiler_update();
                    min_gpu_utilization_device_index = pdc_tf_poly_select_gpu(data_size_mb);

                    double poly_features[PDC_POLY_N_FEATURES];
                    double prev_h2d_ms_log, prev_comp_ms_log, prev_d2h_ms_log, prev_total_ms_log;
                    pdc_tf_get_device_lag(min_gpu_utilization_device_index, &prev_h2d_ms_log,
                                          &prev_comp_ms_log, &prev_d2h_ms_log, &prev_total_ms_log);
                    poly_features[0]   = data_size_mb;
                    poly_features[1]   = pdc_tf_avg_gpu_utilization(min_gpu_utilization_device_index) * 100.0;
                    poly_features[2]   = prev_h2d_ms_log;
                    poly_features[3]   = prev_d2h_ms_log;
                    predicted_total_ms = pdc_tf_poly_predict(poly_features);
                }
                else {
                    min_gpu_utilization_device_index = 0;
                }

                min_avg_gpu_utilization = pdc_tf_avg_gpu_utilization(min_gpu_utilization_device_index);

                /* ── pre-execution logging ────────────────────────────────── */
                if (true || pdc_server_rank_g == 0) {
                    LOG_WARNING("SCHED: mode=%s avg_cpu_util=%.4f selected_gpu=%d "
                                "nvml_count=%d close_time_g=%d\n",
                                sched_mode == PDC_TF_SCHED_DYNAMIC ? "DYNAMIC" : "STATIC",
                                avg_cpu_utilization, min_gpu_utilization_device_index,
                                pdc_tf_profiler_nvml_device_count, close_time_g);

                    for (int i = 0; i < (int)pdc_tf_profiler_nvml_device_count; i++) {
                        double ph, pc, pd, pt;
                        pdc_tf_get_device_lag(i, &ph, &pc, &pd, &pt);
                        LOG_WARNING("SCHED: GPU[%d] util=%.1f%% power=%.0f mW "
                                    "mem_used=%.0f MB prev_h2d=%.2f ms prev_d2h=%.2f ms "
                                    "prev_total=%.2f ms\n",
                                    i, pdc_tf_avg_gpu_utilization(i) * 100.0,
                                    (double)pdc_tf_avg_gpu_power_mw(i),
                                    (double)(pdc_tf_avg_gpu_mem_used(i) / (1024UL * 1024UL)), ph, pd, pt);
                    }

                    if (sched_mode == PDC_TF_SCHED_DYNAMIC)
                        LOG_WARNING("SCHED: data_size=%.2f MB  predicted_total=%.2f ms  "
                                    "selected_gpu=%d\n",
                                    data_size_mb, predicted_total_ms, min_gpu_utilization_device_index);
                    else
                        LOG_WARNING("SCHED: data_size=%.2f MB  static_gpu=0\n", data_size_mb);
                }

                /* ── edge selection ───────────────────────────────────────── */
                bool force_selected = false;
                best_edge_idx       = select_best_edge(
                    edges_out, j, cur_edges_between_vertices, avg_cpu_utilization, min_avg_gpu_utilization,
                    &min_gpu_utilization_device_index, false, &force_selected, predicted_total_ms / 1000.0);
            }

            /* ── execute transformation ───────────────────────────────── */
            pdc_dg_edge_t  e = edges_out[best_edge_idx];
            pdc_tf_func_t *f = (pdc_tf_func_t *)(e.data);

            pdc_tf_internal_param internal_params  = {0};
            internal_params.dg                     = dg;
            internal_params.flat_conceptual_offset = flat_conceptual_offset;
            internal_params.host_to_dev_time       = 0;
            internal_params.dev_to_host_time       = 0;

#ifdef CUDA_ENABLED
            /* f->dev can only be PDC_TF_GPU_DEVICE here when CUDA_ENABLED
             * registered at least one GPU builtin (see PDCtf_init_builtin_funcs)
             * -- unreachable, not just untested, without it. */
            if (f->dev == PDC_TF_GPU_DEVICE) {
                int gpu_idx = close_time_g ? 0 : min_gpu_utilization_device_index;
                LOG_WARNING("SCHED: setting CUDA device to %d for edge %u func=%s\n", gpu_idx, best_edge_idx,
                            f->name);
                cudaError_t err = cudaSetDevice(gpu_idx);
                if (err != cudaSuccess)
                    PGOTO_ERROR(FAIL, "Failed to set device %d\n", gpu_idx);
            }
#endif

            LOG_WARNING("SCHED: executing edge %u func=%s dev=%s\n", best_edge_idx, f->name,
                        f->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU");

            void *          prev_input = *input;
            struct timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            memcpy(&input_region, output_region, sizeof(pdc_tf_region_t));
            if (!f->c_func(&internal_params, f->params_str, input, input_region, output_region))
                PGOTO_ERROR(FAIL, "Error running transformation %s", f->name);

            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double transform_time =
                (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;
            double transform_time_ms = transform_time * 1000.0;

            /* "decompress" contains "compress" as a substring -- check it
             * first. Anything that's neither (encryption, turbo, ...)
             * isn't one of the three tracked computation metrics and is
             * left unrecorded. */
            if (strstr(f->name, "decompress") != NULL)
                PDC_stats_record(PDC_STAT_DECOMPRESS, transform_time);
            else if (strstr(f->name, "compress") != NULL)
                PDC_stats_record(PDC_STAT_COMPRESS, transform_time);

            /* ── post-execution: only for multi-edge paths ────────────── */
            if (cur_edges_between_vertices > 1) {
                double exec_only_time = pure_exec_time(transform_time, internal_params);
                double projected_time = (f->dev == PDC_TF_CPU_DEVICE)
                                            ? exec_only_time * (1.0 - fmax(avg_cpu_utilization, 0.1))
                                            : exec_only_time * (1.0 - fmax(min_avg_gpu_utilization, 0.1));

                double h2d_ms =
                    internal_params.host_to_dev_time >= 0 ? internal_params.host_to_dev_time * 1000.0 : 0.0;
                double d2h_ms =
                    internal_params.dev_to_host_time >= 0 ? internal_params.dev_to_host_time * 1000.0 : 0.0;
                double comp_ms = exec_only_time * 1000.0;

                if (true || pdc_server_rank_g == 0) {
                    LOG_WARNING("TIMING: mode=%s func=%s dev=%s gpu=%d "
                                "h2d=%.4f ms comp=%.4f ms d2h=%.4f ms "
                                "total=%.4f ms exec_only=%.4f ms projected=%.4f ms",
                                sched_mode == PDC_TF_SCHED_DYNAMIC ? "DYNAMIC" : "STATIC", f->name,
                                f->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU", min_gpu_utilization_device_index,
                                h2d_ms, comp_ms, d2h_ms, transform_time_ms, exec_only_time * 1000.0,
                                projected_time * 1000.0);

                    if (sched_mode == PDC_TF_SCHED_DYNAMIC && f->dev == PDC_TF_GPU_DEVICE)
                        LOG_WARNING("TIMING: predicted=%.4f ms actual=%.4f ms error=%.4f ms\n",
                                    predicted_total_ms, transform_time_ms,
                                    transform_time_ms - predicted_total_ms);
                    else
                        LOG_WARNING("\n");
                }

                update_func_history(f, internal_params, projected_time);

                if (sched_mode == PDC_TF_SCHED_DYNAMIC) {
                    pdc_tf_poly_update(min_gpu_utilization_device_index,
                                       internal_params.host_to_dev_time >= 0 ? h2d_ms : -1.0, comp_ms,
                                       internal_params.dev_to_host_time >= 0 ? d2h_ms : -1.0,
                                       transform_time_ms);
                }
            }

            if (!(is_write && j == 0) && prev_input != *input)
                prev_input = PDC_free(prev_input);

            if (j + cur_edges_between_vertices != num_edges)
                memcpy(&input_region, output_region, sizeof(pdc_tf_region_t));

            j += cur_edges_between_vertices;
        }

        LOG_WARNING("SCHED: done running transformations\n");
    }
    else {
        LOG_ERROR("JSON filepath %s\n", (char *)dg->data);
        LOG_ERROR("Current state %s, desired state %s\n", cur_state, desired_state);
        PGOTO_ERROR(FAIL, "No path to desired state");
    }

done:
    if (edges_out != NULL)
        edges_out = PDC_free(edges_out);

    FUNC_LEAVE(ret_value);
}

perr_t
PDCtf_exec_graph_backup(pdc_dg_t *dg, uint64_t flat_conceptual_offset, char *cur_state, char *desired_state,
                        pdc_tf_region_t input_region, pdc_tf_region_t *output_region, void **input,
                        int is_write, pdc_tf_sched_mode_t sched_mode)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    PDC_get_var_type_size(input_region.pdc_var_type);

    pdc_tf_state_t tf_input_state  = {.name = cur_state};
    pdc_tf_state_t tf_output_state = {.name = desired_state};
    void *         input_state     = (void *)&tf_input_state;
    void *         output_state    = (void *)&tf_output_state;

    pdc_dg_edge_t *edges_out = NULL;
    uint32_t       num_edges;

    const char *value          = getenv("USE_GPU");
    bool        always_use_gpu = (value != NULL);

    if (PDCdg_shortest_path(dg, input_state, output_state, &edges_out, &num_edges)) {
        memcpy(output_region, &input_region, sizeof(pdc_tf_region_t));

        for (uint32_t j = 0; j < num_edges;) {
            pdc_dg_vertex_id_t v1_id = edges_out[j].v1_id;
            pdc_dg_vertex_id_t v2_id = edges_out[j].v2_id;

            uint32_t cur_edges_between_vertices = 0;
            uint32_t k                          = j;
            while (k < num_edges && edges_out[k].v1_id == v1_id && edges_out[k].v2_id == v2_id) {
                cur_edges_between_vertices++;
                k++;
            }

            /* ── device state ─────────────────────────────────────────── */
            double data_size_mb        = (double)PDCtf_get_pdc_region_t_bytes(input_region) / 1e6;
            double avg_cpu_utilization = pdc_tf_avg_cpu_utilization();

            /* ── GPU selection ────────────────────────────────────────────
             * DYNAMIC: polynomial predicts total_ms per GPU, pick lowest.
             * STATIC:  always use GPU 0 (baseline).
             * ─────────────────────────────────────────────────────────── */
            int    min_gpu_utilization_device_index;
            double predicted_total_ms = -1.0;

            if (sched_mode == PDC_TF_SCHED_DYNAMIC) {
                pdc_tf_nvml_profiler_update();
                min_gpu_utilization_device_index = pdc_tf_poly_select_gpu(data_size_mb);

                /* build feature vector for logging predicted vs actual */
                double poly_features[PDC_POLY_N_FEATURES];
                double prev_h2d_ms_log, prev_comp_ms_log, prev_d2h_ms_log, prev_total_ms_log;
                pdc_tf_get_device_lag(min_gpu_utilization_device_index, &prev_h2d_ms_log, &prev_comp_ms_log,
                                      &prev_d2h_ms_log, &prev_total_ms_log);
                poly_features[0]   = data_size_mb;
                poly_features[1]   = pdc_tf_avg_gpu_utilization(min_gpu_utilization_device_index) * 100.0;
                poly_features[2]   = prev_h2d_ms_log;
                poly_features[3]   = prev_d2h_ms_log;
                predicted_total_ms = pdc_tf_poly_predict(poly_features);
            }
            else {
                /* PDC_TF_SCHED_STATIC: always GPU 0 */
                min_gpu_utilization_device_index = 0;
            }

            double min_avg_gpu_utilization = pdc_tf_avg_gpu_utilization(min_gpu_utilization_device_index);

            /* ── pre-execution logging ────────────────────────────────── */
            if (true || pdc_server_rank_g == 0) {
                LOG_WARNING("SCHED: mode=%s avg_cpu_util=%.4f selected_gpu=%d "
                            "nvml_count=%d always_use_gpu=%d close_time_g=%d\n",
                            sched_mode == PDC_TF_SCHED_DYNAMIC ? "DYNAMIC" : "STATIC", avg_cpu_utilization,
                            min_gpu_utilization_device_index, pdc_tf_profiler_nvml_device_count,
                            always_use_gpu, close_time_g);

                /* per-GPU state used by polynomial */
                for (int i = 0; i < (int)pdc_tf_profiler_nvml_device_count; i++) {
                    double ph, pc, pd, pt;
                    pdc_tf_get_device_lag(i, &ph, &pc, &pd, &pt);
                    LOG_WARNING("SCHED: GPU[%d] util=%.1f%% power=%.0f mW "
                                "mem_used=%.0f MB prev_h2d=%.2f ms prev_d2h=%.2f ms "
                                "prev_total=%.2f ms\n",
                                i, pdc_tf_avg_gpu_utilization(i) * 100.0, (double)pdc_tf_avg_gpu_power_mw(i),
                                (double)(pdc_tf_avg_gpu_mem_used(i) / (1024UL * 1024UL)), ph, pd, pt);
                }

                if (sched_mode == PDC_TF_SCHED_DYNAMIC)
                    LOG_WARNING("SCHED: data_size=%.2f MB  predicted_total=%.2f ms  "
                                "selected_gpu=%d\n",
                                data_size_mb, predicted_total_ms, min_gpu_utilization_device_index);
                else
                    LOG_WARNING("SCHED: data_size=%.2f MB  static_gpu=0\n", data_size_mb);
            }

            /* ── edge selection ───────────────────────────────────────── */
            bool     force_selected = false;
            uint32_t best_edge_idx  = select_best_edge(
                edges_out, j, cur_edges_between_vertices, avg_cpu_utilization, min_avg_gpu_utilization,
                &min_gpu_utilization_device_index, always_use_gpu, &force_selected, predicted_total_ms / 1000.0);

            /* ── execute transformation ───────────────────────────────── */
            pdc_dg_edge_t  e = edges_out[best_edge_idx];
            pdc_tf_func_t *f = (pdc_tf_func_t *)(e.data);

            pdc_tf_internal_param internal_params  = {0};
            internal_params.dg                     = dg;
            internal_params.flat_conceptual_offset = flat_conceptual_offset;
            internal_params.host_to_dev_time       = 0;
            internal_params.dev_to_host_time       = 0;

#ifdef CUDA_ENABLED
            /* f->dev can only be PDC_TF_GPU_DEVICE here when CUDA_ENABLED
             * registered at least one GPU builtin (see PDCtf_init_builtin_funcs)
             * -- unreachable, not just untested, without it. */
            if (f->dev == PDC_TF_GPU_DEVICE) {
                LOG_WARNING("SCHED: setting CUDA device to %d for edge %u func=%s\n",
                            (always_use_gpu || close_time_g) ? 0 : min_gpu_utilization_device_index,
                            best_edge_idx, f->name);
                cudaError_t err =
                    cudaSetDevice((always_use_gpu || close_time_g) ? 0 : min_gpu_utilization_device_index);
                if (err != cudaSuccess)
                    PGOTO_ERROR(FAIL, "Failed to set device %d\n",
                                (always_use_gpu || close_time_g) ? 0 : min_gpu_utilization_device_index);
            }
#endif

            LOG_WARNING("SCHED: executing edge %u func=%s dev=%s\n", best_edge_idx, f->name,
                        f->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU");

            void *          prev_input = *input;
            struct timespec start_time, end_time;
            clock_gettime(CLOCK_MONOTONIC, &start_time);

            memcpy(&input_region, output_region, sizeof(pdc_tf_region_t));
            if (!f->c_func(&internal_params, f->params_str, input, input_region, output_region))
                PGOTO_ERROR(FAIL, "Error running transformation %s", f->name);

            clock_gettime(CLOCK_MONOTONIC, &end_time);
            double transform_time =
                (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;
            double transform_time_ms = transform_time * 1000.0;

            double exec_only_time = pure_exec_time(transform_time, internal_params);
            double projected_time = (f->dev == PDC_TF_CPU_DEVICE)
                                        ? exec_only_time * (1.0 - fmax(avg_cpu_utilization, 0.1))
                                        : exec_only_time * (1.0 - fmax(min_avg_gpu_utilization, 0.1));

            double h2d_ms =
                internal_params.host_to_dev_time >= 0 ? internal_params.host_to_dev_time * 1000.0 : 0.0;
            double d2h_ms =
                internal_params.dev_to_host_time >= 0 ? internal_params.dev_to_host_time * 1000.0 : 0.0;
            double comp_ms = exec_only_time * 1000.0;

            /* ── post-execution logging ───────────────────────────────────
             * Log all timing components plus predicted vs actual for
             * dynamic mode so we can evaluate prediction accuracy offline.
             * ─────────────────────────────────────────────────────────── */
            if (true || pdc_server_rank_g == 0) {
                LOG_WARNING("TIMING: mode=%s func=%s dev=%s gpu=%d "
                            "h2d=%.4f ms comp=%.4f ms d2h=%.4f ms "
                            "total=%.4f ms exec_only=%.4f ms projected=%.4f ms",
                            sched_mode == PDC_TF_SCHED_DYNAMIC ? "DYNAMIC" : "STATIC", f->name,
                            f->dev == PDC_TF_CPU_DEVICE ? "CPU" : "GPU", min_gpu_utilization_device_index,
                            h2d_ms, comp_ms, d2h_ms, transform_time_ms, exec_only_time * 1000.0,
                            projected_time * 1000.0);

                if (sched_mode == PDC_TF_SCHED_DYNAMIC && f->dev == PDC_TF_GPU_DEVICE)
                    LOG_WARNING("TIMING: predicted=%.4f ms actual=%.4f ms error=%.4f ms\n",
                                predicted_total_ms, transform_time_ms,
                                transform_time_ms - predicted_total_ms);
                else
                    LOG_WARNING("\n");
            }

            update_func_history(f, internal_params, projected_time);

            /* ── update lag features ──────────────────────────────────────
             * Record actual timings for this GPU so they become lag
             * features for the next scheduling decision on this device.
             * Only updated in DYNAMIC mode — STATIC has no model to feed.
             * ─────────────────────────────────────────────────────────── */
            if (sched_mode == PDC_TF_SCHED_DYNAMIC) {
                pdc_tf_poly_update(min_gpu_utilization_device_index,
                                   internal_params.host_to_dev_time >= 0 ? h2d_ms : -1.0, comp_ms,
                                   internal_params.dev_to_host_time >= 0 ? d2h_ms : -1.0, transform_time_ms);
            }

            if (!(is_write && j == 0) && prev_input != *input)
                prev_input = PDC_free(prev_input);

            if (j + cur_edges_between_vertices != num_edges)
                memcpy(&input_region, output_region, sizeof(pdc_tf_region_t));

            j += cur_edges_between_vertices;
        }

        LOG_WARNING("SCHED: done running transformations\n");
    }
    else {
        LOG_ERROR("JSON filepath %s\n", (char *)dg->data);
        LOG_ERROR("Current state %s, desired state %s\n", cur_state, desired_state);
        PGOTO_ERROR(FAIL, "No path to desired state");
    }

done:
    if (edges_out != NULL)
        edges_out = PDC_free(edges_out);

    FUNC_LEAVE(ret_value);
}

#endif // IS_PDC_SERVER
