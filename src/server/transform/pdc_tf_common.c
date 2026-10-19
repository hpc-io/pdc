#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include <dlfcn.h>

#include "common_io.h"
#include "pdc_malloc.h"
#include "pdc_tf_common.h"
#include "pdc_tf_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_tf.h"
#include "pdc_timing.h"
#include "pdc_interface.h"
#include "pdc_tf_poly_sched.h"
#ifdef JSONC_ENABLED
#include "json-c/json.h"
#endif

PDC_VECTOR *pdc_tf_builtin_funcs_vector_g = NULL;

void
append_host_to_dev_time(pdc_tf_builtin_func_t *func, double value)
{
    func->host_to_dev_avg_time[func->cur_host_to_dev_avg_time_index] = value;
    func->cur_host_to_dev_avg_time_index = (func->cur_host_to_dev_avg_time_index + 1) % NUM_TF_FUNC_TIMES;
}

void
append_dev_to_host_time(pdc_tf_builtin_func_t *func, double value)
{
    func->dev_to_host_avg_time[func->cur_dev_to_host_avg_time_index] = value;
    func->cur_dev_to_host_avg_time_index = (func->cur_dev_to_host_avg_time_index + 1) % NUM_TF_FUNC_TIMES;
}

void
append_exec_time(pdc_tf_builtin_func_t *func, double value)
{
    func->exec_avg_time[func->cur_exec_avg_time_index] = value;
    func->cur_exec_avg_time_index = (func->cur_exec_avg_time_index + 1) % NUM_TF_FUNC_TIMES;
}

double
get_host_to_dev_avg(const pdc_tf_builtin_func_t *func)
{
    double sum = 0.0;
    for (int i = 0; i < NUM_TF_FUNC_TIMES; i++)
        sum += func->host_to_dev_avg_time[i];
    return sum / NUM_TF_FUNC_TIMES;
}

double
get_dev_to_host_avg(const pdc_tf_builtin_func_t *func)
{
    double sum = 0.0;
    for (int i = 0; i < NUM_TF_FUNC_TIMES; i++)
        sum += func->dev_to_host_avg_time[i];
    return sum / NUM_TF_FUNC_TIMES;
}

double
get_exec_avg(const pdc_tf_builtin_func_t *func)
{
    double sum = 0.0;
    for (int i = 0; i < NUM_TF_FUNC_TIMES; i++)
        sum += func->exec_avg_time[i];
    return sum / NUM_TF_FUNC_TIMES;
}

perr_t
PDCtf_set_tf_region_t(pdc_tf_region_t *dest, uint8_t ndim, pdc_var_type_t pdc_var_type, uint64_t *size)
{
    FUNC_ENTER(NULL);

    PDC_get_var_type_size(pdc_var_type);

    dest->ndim         = ndim;
    dest->pdc_var_type = pdc_var_type;
    for (int i = 0; i < ndim; i++)
        dest->size[i] = size[i];

    FUNC_LEAVE(SUCCEED);
}

perr_t
PDCtf_copy_tf_region_t(pdc_tf_region_t *src, pdc_tf_region_t *dest)
{
    FUNC_ENTER(NULL);

    dest->ndim         = src->ndim;
    dest->pdc_var_type = src->pdc_var_type;
    for (int i = 0; i < src->ndim; i++)
        dest->size[i] = src->size[i];

    FUNC_LEAVE(SUCCEED);
}

perr_t
PDCtf_add_builtin_func(char *func_name, c_func_t c_func, pdc_tf_dev_t dev)
{
    FUNC_ENTER(NULL);

    int ret_value = SUCCEED;

    if (func_name == NULL)
        PGOTO_ERROR(FAIL, "func_name was NULL");
    if (c_func == NULL)
        PGOTO_ERROR(FAIL, "c_func was NULL");

    pdc_tf_builtin_func_t *builtin_func = PDC_malloc(sizeof(pdc_tf_builtin_func_t));
    pdc_vector_add(pdc_tf_builtin_funcs_vector_g, builtin_func);

    builtin_func->name   = strdup(func_name);
    builtin_func->c_func = c_func;
    builtin_func->dev    = dev;

    /* Initialize rolling history indices */
    builtin_func->cur_host_to_dev_avg_time_index = 0;
    builtin_func->cur_dev_to_host_avg_time_index = 0;
    builtin_func->cur_exec_avg_time_index        = 0;

    /* Initialize timing histories */
    for (int i = 0; i < NUM_TF_FUNC_TIMES; i++) {
        builtin_func->host_to_dev_avg_time[i] = 0.0;
        builtin_func->dev_to_host_avg_time[i] = 0.0;
        builtin_func->exec_avg_time[i]        = (builtin_func->dev == PDC_TF_CPU_DEVICE) ? 0.750 : 0.0;
    }

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCtf_link_builtin_func(char *func_name, pdc_tf_dev_t dev, pdc_tf_func_t *f)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;
    bool   found     = false;

    if (func_name == NULL)
        PGOTO_ERROR(FAIL, "func_name was NULL");
    if (f == NULL)
        PGOTO_ERROR(FAIL, "f was NULL");

    PDC_VECTOR_ITERATOR *builtin_func_iter = pdc_vector_iterator_new(pdc_tf_builtin_funcs_vector_g);
    while (pdc_vector_iterator_has_next(builtin_func_iter)) {
        pdc_tf_builtin_func_t *builtin_func = pdc_vector_iterator_next(builtin_func_iter);
        if (builtin_func == NULL)
            PGOTO_ERROR(FAIL, "builtin_func was NULL");
        if (strcmp(builtin_func->name, func_name) == 0 && builtin_func->dev == dev) {
            found     = true;
            f->c_func = builtin_func->c_func;
        }
    }
    pdc_vector_iterator_destroy(builtin_func_iter);

    if (!found)
        PGOTO_ERROR(FAIL, "Builtin function not found");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCtf_init_builtin_funcs()
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

#ifdef CUDA_ENABLED
    /* The polynomial GPU cost model is entirely optional: the only
     * production call site (pdc_server_region_transfer.c) hardcodes
     * PDC_TF_SCHED_STATIC, which never consults it at all, and even the
     * DYNAMIC path (not currently used in production) falls back to a
     * simple lowest-utilization heuristic whenever the model isn't
     * initialized -- see pdc_tf_poly_select_gpu/pdc_tf_poly_predict,
     * pdc_tf_poly_sched.c. There's no sensible system-wide default
     * coefficients file (it's fitted offline, per-cluster, per-kernel), so
     * only attempt to load one if the user explicitly opts in. */
    const char *coeff_file = getenv("PDC_POLY_COEFF_FILE");
    if (coeff_file != NULL && pdc_tf_poly_sched_init(coeff_file) != 0)
        LOG_WARNING("Failed to initialize polynomial scheduler from %s (PDC_POLY_COEFF_FILE); GPU "
                    "scheduling will use the fallback heuristic\n",
                    coeff_file);
#endif

    if (pdc_tf_builtin_funcs_vector_g == NULL)
        pdc_tf_builtin_funcs_vector_g = pdc_vector_create(16, 2.0);
    if (pdc_tf_builtin_funcs_vector_g == NULL)
        PGOTO_ERROR(FAIL, "pdc_tf_builtin_funcs_vector_g was NULL");
#ifdef ENABLE_TF_SZ_GPU_COMPRESSSION
    if (PDCtf_add_builtin_func("sz_compress", pdc_tf_builtin_sz_compress_cuda, PDC_TF_GPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func sz_compress GPU");
    if (PDCtf_add_builtin_func("sz_decompress", pdc_tf_builtin_sz_decompress_cuda, PDC_TF_GPU_DEVICE) !=
        SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func sz_decompress GPU");
#endif
#ifdef ENABLE_TF_SZ_COMPRESSION
    if (PDCtf_add_builtin_func("sz_compress", pdc_tf_builtin_sz_compress, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func sz_compress CPU");
    if (PDCtf_add_builtin_func("sz_decompress", pdc_tf_builtin_sz_decompress, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func sz_decompress CPU");
#endif
#ifdef ENABLE_TF_ZFP_COMPRESSION
    if (PDCtf_add_builtin_func("zfp_compress", pdc_tf_builtin_zfp_compress, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func zfp_compress CPU");
    if (PDCtf_add_builtin_func("zfp_decompress", pdc_tf_builtin_zfp_decompress, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func zfp_decompress CPU");
#ifdef CUDA_ENABLED
    if (PDCtf_add_builtin_func("zfp_compress", pdc_tf_builtin_zfp_compress_cuda, PDC_TF_GPU_DEVICE) !=
        SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func zfp_compress GPU");
    if (PDCtf_add_builtin_func("zfp_decompress", pdc_tf_builtin_zfp_decompress_cuda, PDC_TF_GPU_DEVICE) !=
        SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func zfp_decompress GPU");
#endif
#endif
#ifdef ENABLE_TF_SECRET_BOX_ENCRYPTION
    if (PDCtf_add_builtin_func("secret_box_encrypt", pdc_tf_builtin_encrypt, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func secret_box_encrypt CPU");
    if (PDCtf_add_builtin_func("secret_box_decrypt", pdc_tf_builtin_decrypt, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func secret_box_decrypt CPU");
#endif
#ifdef ENABLE_TF_TURBO_COMPRESSION
    if (PDCtf_add_builtin_func("turbo_compress", pdc_tf_builtin_turbo_compress, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func turbo_compress CPU");
    if (PDCtf_add_builtin_func("turbo_decompress", pdc_tf_builtin_turbo_decompress, PDC_TF_CPU_DEVICE) !=
        SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin func decompress CPU");
#endif

done:
    FUNC_LEAVE(ret_value);
}

bool
PDCtf_region_has_attached_graph(struct pdc_tf_obj_t *tf_obj, int ndim, size_t unit, uint64_t *offset,
                                uint64_t *size, pdc_tf_region_mapping_t **region_mapping)
{
    FUNC_ENTER(NULL);

    bool                 ret_value            = false;
    PDC_VECTOR_ITERATOR *region_mappings_iter = NULL;

    if (tf_obj == NULL) {
        LOG_DEBUG("tf_obj is NULL\n");
        PGOTO_DONE(false);
    }
    if (tf_obj->region_mappings_vector == NULL) {
        LOG_DEBUG("region_mappings_vector is NULL\n");
        PGOTO_DONE(false);
    }

    region_mappings_iter = pdc_vector_iterator_new(tf_obj->region_mappings_vector);
    while (pdc_vector_iterator_has_next(region_mappings_iter)) {
        *region_mapping                    = pdc_vector_iterator_next(region_mappings_iter);
        pdc_tf_region_t *conceptual_region = &((*region_mapping)->conceptual_region);
        uint64_t *       conceptual_offset = (*region_mapping)->conceptual_offset;

        bool ndim_matches   = conceptual_region->ndim == ndim;
        bool unit_matches   = PDC_get_var_type_size(conceptual_region->pdc_var_type) == unit;
        bool offset_matches = true;
        bool size_matches   = true;

        for (int i = 0; i < ndim; i++) {
            bool offset_i_match = (conceptual_offset[i] == offset[i]);
            bool size_i_match   = (conceptual_region->size[i] == size[i]);
            offset_matches &= offset_i_match;
            size_matches &= size_i_match;
        }

        if (ndim_matches && offset_matches && size_matches && unit_matches) {
            PGOTO_DONE(true);
        }
    }

done:
    if (region_mappings_iter != NULL)
        pdc_vector_iterator_destroy(region_mappings_iter);
    FUNC_LEAVE(ret_value);
}

#ifdef JSONC_ENABLED
static struct array_list *
get_json_array(struct json_object *json_obj, char *arr_name)
{
    FUNC_ENTER(NULL);

    struct json_object *ret_value = NULL;

    if (json_object_object_get_ex(json_obj, arr_name, &ret_value) == 0)
        PGOTO_ERROR(NULL, "%s was not found", arr_name);
    if (json_object_get_type(ret_value) != json_type_array)
        PGOTO_ERROR(NULL, "%s was not an array", arr_name);

done:
    if (ret_value == NULL)
        FUNC_LEAVE(NULL);
    FUNC_LEAVE(json_object_get_array(ret_value));
}

static const char *
get_json_string(struct json_object *json_obj, char *str_name, bool expect_string)
{
    struct json_object *str_json_obj = NULL;
    const char *        ret_value    = NULL;

    if (!json_object_object_get_ex(json_obj, str_name, &str_json_obj)) {
        if (expect_string)
            PGOTO_ERROR(NULL, "%s was not found", str_name);
        else
            PGOTO_DONE(NULL);
    }
    if (json_object_get_type(str_json_obj) != json_type_string)
        PGOTO_ERROR(NULL, "%s was not a string\n", str_json_obj);
    ret_value = json_object_get_string(str_json_obj);

done:
    if (ret_value == NULL)
        FUNC_LEAVE(ret_value);
    FUNC_LEAVE(json_object_get_string(str_json_obj));
}
#endif /* JSONC_ENABLED */

char *pdc_tf_dev_strs[]      = {"CPU", "GPU"};
char *pdc_tf_location_strs[] = {"builtin", "external"};

bool
vertices_are_equal(void *v1, void *v2)
{
    pdc_tf_state_t *s1 = (pdc_tf_state_t *)v1;
    pdc_tf_state_t *s2 = (pdc_tf_state_t *)v2;

    if (s1 == NULL || s2 == NULL)
        return false;

    return !strcmp(s1->name, s2->name);
}

static void
graph_free(void *data)
{
    FUNC_ENTER(NULL);
    char *json_filepath = (char *)data;
    json_filepath       = PDC_free(data);
    FUNC_LEAVE_VOID();
}

static void
edge_free(void *data)
{
    FUNC_ENTER(NULL);

    pdc_tf_func_t *      f              = (pdc_tf_func_t *)data;
    PDC_VECTOR_ITERATOR *dg_params_iter = pdc_vector_iterator_new(f->pdc_tf_dg_params_vector);
    while (pdc_vector_iterator_has_next(dg_params_iter)) {
        pdc_tf_dg_params_t *cur_param = pdc_vector_iterator_next(dg_params_iter);
        if (cur_param != NULL)
            cur_param = PDC_free(cur_param);
    }
    pdc_vector_iterator_destroy(dg_params_iter);
    if (f->params_str != NULL)
        f->params_str = PDC_free(f->params_str);
    f->name = PDC_free(f->name);
    f       = PDC_free(f);

    FUNC_LEAVE_VOID();
}

static void
vertex_free(void *data)
{
    FUNC_ENTER(NULL);

    pdc_tf_state_t *     s              = (pdc_tf_state_t *)data;
    PDC_VECTOR_ITERATOR *dg_params_iter = pdc_vector_iterator_new(s->pdc_tf_dg_params_vector);
    while (pdc_vector_iterator_has_next(dg_params_iter)) {
        pdc_tf_dg_params_t *cur_param = pdc_vector_iterator_next(dg_params_iter);
        if (cur_param != NULL)
            cur_param = PDC_free(cur_param);
    }
    pdc_vector_iterator_destroy(dg_params_iter);
    s->name = PDC_free(s->name);
    s       = PDC_free(s);

    FUNC_LEAVE_VOID();
}

#ifndef JSONC_ENABLED
/* JSON-C wasn't found at configure time (see root CMakeLists.txt) -- the
 * only thing that's actually unavailable is attaching a graph; every
 * other PDC operation (object/region create, read, write, ...) works
 * unaffected. Callers already treat a NULL return the same as any other
 * graph-load failure (see PDCtf_dg_json_create, src/api/pdc_tf/pdc_tf.c). */
pdc_dg_t *
PDCtf_dg_json_create_common(char *filepath)
{
    LOG_ERROR("PDCtf_dg_json_create_common: JSON-C support was not compiled in (JSONC_ROOT_DIR was not "
              "set at build time) -- cannot parse transformation graph \"%s\"\n",
              filepath);
    return NULL;
}
#else
pdc_dg_t *
PDCtf_dg_json_create_common(char *filepath)
{
    FUNC_ENTER(NULL);

    pdc_dg_t *          ret_value = NULL;
    pdc_dg_t *          dg_cpy    = NULL;
    FILE *              fp        = NULL;
    struct json_object *json_obj  = NULL;
    io_buffer_t         io_buffer;
    memset(&io_buffer, 0, sizeof(io_buffer_t));

    if ((fp = open_file(filepath, IO_MODE_READ)) == NULL)
        PGOTO_ERROR(0, "Failed to open_file: %s\n", filepath);
    if (read_file(fp, &io_buffer) != 0)
        PGOTO_ERROR(NULL, "Failed to read_file");

    if ((json_obj = json_tokener_parse(io_buffer.buffer)) == NULL)
        PGOTO_ERROR(NULL, "Failed to parse JSON");

    const char *dg_name = get_json_string(json_obj, "name", true);
    if (dg_name == NULL)
        PGOTO_ERROR(NULL, "Failed to find graph name");
    const char *lib_path = NULL;
    if ((lib_path = get_json_string(json_obj, "lib_path", false)) != NULL)
        LOG_DEBUG("Library path: %s\n", lib_path);

    ret_value         = PDCdg_create(graph_free, vertices_are_equal, NULL, edge_free, vertex_free);
    dg_cpy            = ret_value;
    (ret_value)->data = strdup(filepath);

    struct array_list *states    = get_json_array(json_obj, "states");
    struct array_list *functions = get_json_array(json_obj, "functions");
    if (states == NULL || functions == NULL)
        PGOTO_DONE(NULL);

    int states_length = array_list_length(states);
    for (int i = 0; i < states_length; i++) {
        struct json_object *s      = array_list_get_idx(states, i);
        char *              s_name = strdup(get_json_string(s, "name", true));

        if (s_name == NULL)
            PGOTO_DONE(NULL);

        pdc_tf_state_t *dg_state = PDC_calloc(1, sizeof(pdc_tf_state_t));
        dg_state->name           = s_name;

        if (PDCdg_add_vertex(ret_value, dg_state) == PDC_DG_INVALID_VERTEX)
            PGOTO_ERROR(NULL, "Failed to add vertex to directed graph");
    }

    int functions_length = array_list_length(functions);
    for (int i = 0; i < functions_length; i++) {
        struct json_object *f = array_list_get_idx(functions, i);

        char *f_name         = strdup(get_json_string(f, "name", true));
        char *f_input_state  = strdup(get_json_string(f, "input_state", true));
        char *f_output_state = strdup(get_json_string(f, "output_state", true));
        char *f_params_str   = NULL;
        if (get_json_string(f, "params", false) != NULL)
            f_params_str = strdup(get_json_string(f, "params", false));
        const char *f_device   = get_json_string(f, "device", true);
        const char *f_location = get_json_string(f, "location", true);

        if (f_name == NULL || f_input_state == NULL || f_output_state == NULL || f_location == NULL)
            PGOTO_DONE(NULL);

        pdc_tf_func_t *dg_func = PDC_calloc(1, sizeof(pdc_tf_func_t));

        pdc_tf_dev_t dev;
        bool         found_device = false;
        for (int j = 0; j < PDC_TF_NUM_DEVICES; j++) {
            if (!strcmp(f_device, pdc_tf_dev_strs[j])) {
                found_device = true;
                dev          = j;
                break;
            }
        }
        if (!found_device)
            PGOTO_ERROR(NULL, "Invalid device %s\n", f_device);

        pdc_tf_location_t location;
        bool              found_location = false;
        for (int j = 0; j < PDC_TF_NUM_LOCATIONS; j++) {
            if (!strcmp(f_location, pdc_tf_location_strs[j])) {
                found_location = true;
                location       = j;
                break;
            }
        }
        if (!found_location)
            PGOTO_ERROR(NULL, "Invalid location %s\n", f_location);

        if (location == PDC_TF_EXTERNAL) {
            if (lib_path == NULL)
                PGOTO_ERROR(NULL, "Function %s is external but no lib_path was provided\n", f_name);

            void *handle = dlopen(lib_path, RTLD_LAZY);
            if (!handle)
                PGOTO_ERROR(NULL, "Failed to dlopen library at path %s: %s\n", lib_path, dlerror());

            dlerror();
            void *func_ptr = dlsym(handle, f_name);
            char *error;
            if ((error = dlerror()) != NULL)
                PGOTO_ERROR(NULL, "Failed to find symbol %s in library %s: %s\n", f_name, lib_path, error);

            dg_func->c_func = func_ptr;

            if (PDCtf_add_builtin_func(f_name, dg_func->c_func, dev) != SUCCEED)
                PGOTO_ERROR(NULL, "Failed to add external function %s to builtin functions vector\n", f_name);
        }

        dg_func->dev      = dev;
        dg_func->location = location;
        if (PDCtf_link_builtin_func(f_name, dev, dg_func) != SUCCEED)
            PGOTO_ERROR(NULL, "Failed to link to builtin function\n");
        dg_func->name       = f_name;
        dg_func->params_str = (f_params_str) ? f_params_str : NULL;

        pdc_tf_state_t i_state = {.name = (char *)f_input_state};
        pdc_tf_state_t o_state = {.name = (char *)f_output_state};

        if (PDCdg_add_edge(ret_value, &i_state, &o_state, dg_func) == PDC_DG_INVALID_EDGE)
            PGOTO_ERROR(NULL, "Failed to add edge to directed graph\n");
    }

done:
    if (fp != NULL)
        close_file(fp);
    if (io_buffer.buffer != NULL)
        PDC_free(io_buffer.buffer);
    if (json_obj != NULL)
        json_object_put(json_obj);
    if (ret_value == NULL && dg_cpy != NULL) {
        LOG_ERROR("Failed load JSON freeing graph\n");
        PDCdg_destroy(dg_cpy);
    }

    FUNC_LEAVE(ret_value);
}
#endif /* JSONC_ENABLED */

size_t
PDCtf_get_pdc_region_t_elements(pdc_tf_region_t reg)
{
    FUNC_ENTER(NULL);

    size_t num_elements = 1;
    for (int i = 0; i < reg.ndim; ++i)
        num_elements *= reg.size[i];

    FUNC_LEAVE(num_elements);
}

size_t
PDCtf_get_flat_conceptual_offset(int ndim, uint64_t offset[4], const uint64_t *dims)
{
    FUNC_ENTER(NULL);

    assert(ndim > 0);

    size_t flat_offset = 0;
    size_t stride      = 1;

    for (int i = 0; i < ndim; i++) {
        flat_offset += offset[i] * stride;
        stride *= dims[i];
    }

    FUNC_LEAVE(flat_offset);
}

size_t
PDCtf_get_pdc_region_t_bytes(pdc_tf_region_t reg)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE(PDCtf_get_pdc_region_t_elements(reg) * PDC_get_var_type_size(reg.pdc_var_type));
}

void
PDCtf_log_pdc_region_t(pdc_tf_region_t reg)
{
    FUNC_ENTER(NULL);
    FUNC_LEAVE_VOID();
}

void
PDCtf_print_exec_path_common(pdc_dg_t *dg, char *cur_state, char *desired_state)
{
    FUNC_ENTER(NULL);

    assert(dg != NULL);
    assert(cur_state != NULL);
    assert(desired_state != NULL);

    pdc_tf_state_t tf_cur_state     = {.name = cur_state};
    pdc_tf_state_t tf_desired_state = {.name = desired_state};
    pdc_dg_edge_t *edges_out;
    uint32_t       num_edges;

    if (PDCdg_shortest_path(dg, &tf_cur_state, &tf_desired_state, &edges_out, &num_edges)) {
        for (uint32_t j = 0; j < num_edges; j++) {
            pdc_dg_edge_t   e  = edges_out[j];
            pdc_tf_state_t *v1 = (pdc_tf_state_t *)(dg->vertices[e.v1_id]->data);
            pdc_tf_state_t *v2 = (pdc_tf_state_t *)(dg->vertices[e.v2_id]->data);
        }
    }
    else
        LOG_DEBUG("No path found\n");

    FUNC_LEAVE_VOID();
}

void
PDCtf_print_dg_common(pdc_dg_t *dg, bool write_to_file)
{
    int stdout_fd;
    int file_fd;

    if (write_to_file) {
        stdout_fd = dup(STDOUT_FILENO);
        if (stdout_fd == -1) {
            perror("dup");
            return;
        }

        file_fd = open("graph.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (file_fd == -1) {
            perror("open");
            close(stdout_fd);
            return;
        }

        if (dup2(file_fd, STDOUT_FILENO) == -1) {
            perror("dup2");
            close(stdout_fd);
            close(file_fd);
            return;
        }

        close(file_fd);
    }

    LOG_JUST_PRINT("\tdigraph G {\n");
    LOG_JUST_PRINT("legend [shape=none, margin=0, label=<\n");
    LOG_JUST_PRINT("  <TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"0\">\n");
    LOG_JUST_PRINT("    <TR><TD><FONT COLOR=\"blue\">&#8594;</FONT></TD><TD>CPU</TD></TR>\n");
    LOG_JUST_PRINT("    <TR><TD><FONT COLOR=\"red\">&#8594;</FONT></TD><TD>GPU</TD></TR>\n");
    LOG_JUST_PRINT("  </TABLE>\n");
    LOG_JUST_PRINT(">];\n");
    LOG_JUST_PRINT("\tlabel=\"Transformation Graph\";\n");
    LOG_JUST_PRINT("\tlabelloc=top;\n");
    LOG_JUST_PRINT("\tfontsize=8;\n");
    LOG_JUST_PRINT("\tfontname=\"Helvetica\";\n");
    LOG_JUST_PRINT("\tnodesep=1.0;\n");
    LOG_JUST_PRINT("\tranksep=1.0;\n");
    LOG_JUST_PRINT("\tsplines=true;\n");
    LOG_JUST_PRINT("\toverlap=false;\n");
    LOG_JUST_PRINT("\trankdir=LR;\n");
    LOG_JUST_PRINT(
        "\tnode [fontsize=10, shape=box, style=filled, fillcolor=lightgray, fontname=\"Helvetica\"];\n");
    LOG_JUST_PRINT("\tedge [fontsize=10];\n");

    for (int i = 0; i < dg->edge_count; i++) {
        pdc_dg_edge_t *edge = dg->edges[i];

        pdc_tf_state_t *input_state  = (pdc_tf_state_t *)PDCdg_get_vertex_data(dg, edge->v1_id);
        pdc_tf_state_t *output_state = (pdc_tf_state_t *)PDCdg_get_vertex_data(dg, edge->v2_id);
        pdc_tf_func_t * edge_func    = (pdc_tf_func_t *)edge->data;

        const char *color = (edge_func->dev == PDC_TF_CPU_DEVICE) ? "blue" : "red";

        LOG_JUST_PRINT("\t\"%s\" -> \"%s\" [label=\"%s\", color=%s];\n", input_state->name,
                       output_state->name, edge_func->name, color);
    }

    LOG_JUST_PRINT("}\n");

    if (write_to_file) {
        fflush(stdout);
        if (dup2(stdout_fd, STDOUT_FILENO) == -1)
            perror("dup2 restore");
        close(stdout_fd);
    }
}
