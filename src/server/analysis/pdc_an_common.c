#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <dlfcn.h>

#include "common_io.h"
#include "pdc_malloc.h"
#include "pdc_an_common.h"
#include "pdc_an_builtin_common.h"
#include "pdc_client_server_common.h"
#include "pdc_timing.h"
#ifdef JSONC_ENABLED
#include "json-c/json.h"
#endif

PDC_VECTOR *pdc_an_builtin_funcs_vector_g = NULL;

/* One registered (name, device) builtin implementation. Distinct from
 * pdc_an_func_t (the graph vertex payload, pdc_an_user.h), which holds an
 * array of variants gathered from possibly-several of these entries by
 * PDCan_link_builtin_func. */
typedef struct pdc_an_builtin_entry_t {
    char *                name;
    pdc_an_func_variant_t variant;
} pdc_an_builtin_entry_t;

char *pdc_an_persistence_strs[] = {"transient", "persistent"};
char *pdc_an_trigger_strs[]     = {"eager", "lazy"};

/* Client-local: vector of {dg_id, PDC_VECTOR *mappings} pairs. See
 * PDCan_get_client_dg_mappings/PDCan_add_client_dg_mapping in pdc_an_common.h. */
typedef struct pdc_an_client_dg_entry_t {
    pdcid_t     dg_id;
    PDC_VECTOR *mappings; /* vector of pdc_an_region_mapping_t* (borrowed) */
} pdc_an_client_dg_entry_t;

static PDC_VECTOR *an_client_dg_registry_g = NULL;

PDC_VECTOR *
PDCan_get_client_dg_mappings(pdcid_t dg_id)
{
    if (an_client_dg_registry_g == NULL)
        return NULL;

    PDC_VECTOR_ITERATOR *iter = pdc_vector_iterator_new(an_client_dg_registry_g);
    while (pdc_vector_iterator_has_next(iter)) {
        pdc_an_client_dg_entry_t *e = (pdc_an_client_dg_entry_t *)pdc_vector_iterator_next(iter);
        if (e != NULL && e->dg_id == dg_id) {
            pdc_vector_iterator_destroy(iter);
            return e->mappings;
        }
    }
    pdc_vector_iterator_destroy(iter);
    return NULL;
}

/* Deep-copies src's fields into dst, freeing dst's previously-owned strings
 * first. Used to supersede a stale per-dg_id mapping in place (see below)
 * without touching src or its own owner (the target object's own
 * pdc_an_obj->region_mappings_vector, set up by the same
 * PDCan_attach_to_region call and independently long-lived) -- pdc_vector
 * has no by-index replace/remove, so mutating the existing struct's content
 * is the only way to "replace" an entry, and each vector needs its own
 * copy of the owned string fields to avoid a double free. */
static void
an_region_mapping_copy_into(pdc_an_region_mapping_t *dst, const pdc_an_region_mapping_t *src)
{
    PDC_free(dst->region_state.state_name);
    PDC_free(dst->region_state.tf_json_filepath);
    PDC_free(dst->region_state.tf_client_state);
    PDC_free(dst->region_state.tf_store_state);

    dst->region_state.dg_id      = src->region_state.dg_id;
    dst->region_state.state_name = strdup(src->region_state.state_name);
    dst->region_state.tf_json_filepath =
        src->region_state.tf_json_filepath ? strdup(src->region_state.tf_json_filepath) : NULL;
    dst->region_state.tf_client_state =
        src->region_state.tf_client_state ? strdup(src->region_state.tf_client_state) : NULL;
    dst->region_state.tf_store_state =
        src->region_state.tf_store_state ? strdup(src->region_state.tf_store_state) : NULL;

    dst->ndim = src->ndim;
    memcpy(dst->offset, src->offset, sizeof(dst->offset));
    memcpy(dst->size, src->size, sizeof(dst->size));
    dst->obj_id   = src->obj_id;
    dst->obj_ndim = src->obj_ndim;
    memcpy(dst->obj_dims, src->obj_dims, sizeof(dst->obj_dims));
    dst->pdc_var_type = src->pdc_var_type;
}

perr_t
PDCan_add_client_dg_mapping(pdcid_t dg_id, pdc_an_region_mapping_t *mapping)
{
    if (an_client_dg_registry_g == NULL)
        an_client_dg_registry_g = pdc_vector_create(4, 2.0);

    PDC_VECTOR *mappings = PDCan_get_client_dg_mappings(dg_id);
    if (mappings == NULL) {
        mappings                        = pdc_vector_create(8, 2.0);
        pdc_an_client_dg_entry_t *entry = PDC_malloc(sizeof(pdc_an_client_dg_entry_t));
        entry->dg_id                    = dg_id;
        entry->mappings                 = mappings;
        pdc_vector_add(an_client_dg_registry_g, entry);
    }

    /* A later attach for a state_name already recorded under this dg_id
     * supersedes the earlier one (e.g. a multi-timestep workload
     * re-attaching "vx" to a fresh per-timestep object under the same,
     * reused dg_id) -- each timestep's object is its own distinct object,
     * so only its current attachment should ever be piggybacked to the
     * server; nothing should keep resending a previous timestep's now-stale
     * (state_name -> obj_id/region) mapping alongside it every subsequent
     * I/O. Since dg_id is typically created once and reused across every
     * timestep of a run, without this the per-dg_id list here would grow
     * without bound and the piggyback below would carry every past
     * timestep's bindings forever. */
    size_t n = pdc_vector_size(mappings);
    for (size_t i = 0; i < n; i++) {
        pdc_an_region_mapping_t *existing = (pdc_an_region_mapping_t *)pdc_vector_get(mappings, i);
        if (existing == NULL || strcmp(existing->region_state.state_name, mapping->region_state.state_name))
            continue;
        an_region_mapping_copy_into(existing, mapping);
        return SUCCEED;
    }

    pdc_vector_add(mappings, mapping);
    return SUCCEED;
}

bool
PDCan_region_has_attached_graph(pdc_an_obj_t *obj_an, uint8_t ndim, const uint64_t *offset,
                                const uint64_t *size, pdc_an_region_mapping_t **region_mapping)
{
    FUNC_ENTER(NULL);

    bool                 ret_value            = false;
    PDC_VECTOR_ITERATOR *region_mappings_iter = NULL;

    if (obj_an == NULL || obj_an->region_mappings_vector == NULL)
        PGOTO_DONE(false);

    region_mappings_iter = pdc_vector_iterator_new(obj_an->region_mappings_vector);
    while (pdc_vector_iterator_has_next(region_mappings_iter)) {
        *region_mapping = (pdc_an_region_mapping_t *)pdc_vector_iterator_next(region_mappings_iter);

        if ((*region_mapping)->ndim != ndim)
            continue;

        bool match = true;
        for (int i = 0; i < ndim; i++) {
            if ((*region_mapping)->offset[i] != offset[i] || (*region_mapping)->size[i] != size[i]) {
                match = false;
                break;
            }
        }
        if (match)
            PGOTO_DONE(true);
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
        PGOTO_ERROR(NULL, "%s was not a string\n", str_name);
    ret_value = json_object_get_string(str_json_obj);

done:
    if (ret_value == NULL)
        FUNC_LEAVE(ret_value);
    FUNC_LEAVE(json_object_get_string(str_json_obj));
}
#endif /* JSONC_ENABLED */

static bool
an_vertices_are_equal(void *v1, void *v2)
{
    pdc_an_node_t *n1 = (pdc_an_node_t *)v1;
    pdc_an_node_t *n2 = (pdc_an_node_t *)v2;

    if (n1 == NULL || n2 == NULL)
        return false;

    return !strcmp(n1->name, n2->name);
}

static void
an_graph_free(void *data)
{
    FUNC_ENTER(NULL);
    data = PDC_free(data);
    FUNC_LEAVE_VOID();
}

static void
an_edge_free(void *data)
{
    /* Edges only encode connectivity in the analysis graph; every real
     * field (device/location/params/etc) lives on the function vertex. */
    (void)data;
}

static void
an_vertex_free(void *data)
{
    FUNC_ENTER(NULL);

    pdc_an_node_t *node = (pdc_an_node_t *)data;

    if (node->kind == PDC_AN_NODE_FUNC) {
        pdc_an_func_t *f = &node->u.func;
        for (int i = 0; i < f->num_inputs; i++)
            f->input_names[i] = PDC_free(f->input_names[i]);
        f->input_names = PDC_free(f->input_names);
        for (int i = 0; i < f->num_outputs; i++)
            f->output_names[i] = PDC_free(f->output_names[i]);
        f->output_names = PDC_free(f->output_names);
        for (int i = 0; i < f->num_variants; i++)
            if (f->variants[i].params_str != NULL)
                f->variants[i].params_str = PDC_free(f->variants[i].params_str);
        f->variants = PDC_free(f->variants);
        f->name     = PDC_free(f->name);
    }
    /* For state vertices, u.state.name aliases node->name (a single
     * allocation); for function vertices node->name is the separately
     * allocated "fn:"-prefixed vertex key. Either way, free it here. */
    node->name = PDC_free(node->name);
    node       = PDC_free(node);

    FUNC_LEAVE_VOID();
}

perr_t
PDCan_init_builtin_funcs(void)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (pdc_an_builtin_funcs_vector_g == NULL)
        pdc_an_builtin_funcs_vector_g = pdc_vector_create(16, 2.0);
    if (pdc_an_builtin_funcs_vector_g == NULL)
        PGOTO_ERROR(FAIL, "pdc_an_builtin_funcs_vector_g was NULL");

    /* External ("location": "external") functions register themselves into
     * this same vector via PDCan_add_builtin_func at graph-parse time. */
    if (PDCan_add_builtin_func("vector_magnitude", pdc_an_builtin_vector_magnitude, PDC_TF_CPU_DEVICE) !=
        SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin analysis func vector_magnitude CPU");
#ifdef CUDA_ENABLED
    if (PDCan_add_builtin_func("vector_magnitude", pdc_an_builtin_vector_magnitude_gpu, PDC_TF_GPU_DEVICE) !=
        SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin analysis func vector_magnitude GPU");
#endif

    if (PDCan_add_builtin_func("curl", pdc_an_builtin_curl, PDC_TF_CPU_DEVICE) != SUCCEED)
        PGOTO_ERROR(FAIL, "Failed to add builtin analysis func curl CPU");

done:
    FUNC_LEAVE(ret_value);
}

perr_t
PDCan_add_builtin_func(char *func_name, a_func_t a_func, pdc_tf_dev_t dev)
{
    FUNC_ENTER(NULL);

    perr_t ret_value = SUCCEED;

    if (func_name == NULL)
        PGOTO_ERROR(FAIL, "func_name was NULL");
    if (a_func == NULL)
        PGOTO_ERROR(FAIL, "a_func was NULL");

    pdc_an_builtin_entry_t *builtin_func = PDC_calloc(1, sizeof(pdc_an_builtin_entry_t));
    builtin_func->name                   = strdup(func_name);
    builtin_func->variant.a_func         = a_func;
    builtin_func->variant.dev            = dev;

    pdc_vector_add(pdc_an_builtin_funcs_vector_g, builtin_func);

done:
    FUNC_LEAVE(ret_value);
}

/**
 * Populates f->variants/num_variants from the builtin registry.
 *
 * dev == PDC_TF_NUM_DEVICES means "any device": every registry entry whose
 * name matches contributes one variant, regardless of device -- this is how
 * a transformation JSON entry that omits "device" ends up with a variant
 * per device the name is registered under, letting PDCan_exec_graph choose
 * dynamically the same way PDCtf_exec_graph's select_best_edge does.
 * Otherwise, exactly the one (name, dev) match is used, matching this
 * function's original single-device behavior.
 *
 * location/params_str come from the transformation's JSON entry (declared
 * once per transformation, not per device) and are duplicated into every
 * resulting variant so an_vertex_free can free each independently.
 */
perr_t
PDCan_link_builtin_func(char *func_name, pdc_tf_dev_t dev, pdc_tf_location_t location, const char *params_str,
                        pdc_an_func_t *f)
{
    FUNC_ENTER(NULL);

    perr_t ret_value   = SUCCEED;
    int    num_matches = 0;

    if (func_name == NULL)
        PGOTO_ERROR(FAIL, "func_name was NULL");
    if (f == NULL)
        PGOTO_ERROR(FAIL, "f was NULL");

    size_t registry_size = pdc_vector_size(pdc_an_builtin_funcs_vector_g);
    for (size_t i = 0; i < registry_size; i++) {
        pdc_an_builtin_entry_t *entry = pdc_vector_get(pdc_an_builtin_funcs_vector_g, i);
        if (entry == NULL)
            PGOTO_ERROR(FAIL, "builtin registry entry was NULL");
        if (strcmp(entry->name, func_name) == 0 && (dev == PDC_TF_NUM_DEVICES || entry->variant.dev == dev))
            num_matches++;
    }

    if (num_matches == 0)
        PGOTO_ERROR(FAIL, "Builtin analysis function \"%s\" not found for device %s", func_name,
                    dev == PDC_TF_NUM_DEVICES ? "ANY" : pdc_tf_dev_strs[dev]);

    f->variants     = PDC_calloc((size_t)num_matches, sizeof(pdc_an_func_variant_t));
    f->num_variants = num_matches;

    int v = 0;
    for (size_t i = 0; i < registry_size; i++) {
        pdc_an_builtin_entry_t *entry = pdc_vector_get(pdc_an_builtin_funcs_vector_g, i);
        if (strcmp(entry->name, func_name) != 0)
            continue;
        if (dev != PDC_TF_NUM_DEVICES && entry->variant.dev != dev)
            continue;

        f->variants[v].dev        = entry->variant.dev;
        f->variants[v].location   = location;
        f->variants[v].a_func     = entry->variant.a_func;
        f->variants[v].params_str = (params_str != NULL) ? strdup(params_str) : NULL;
        v++;
    }

done:
    FUNC_LEAVE(ret_value);
}

pdc_an_node_t *
PDCan_dg_get_node(pdc_dg_t *dg, const char *name)
{
    pdc_an_node_t *ret_value = NULL;

    if (dg == NULL || name == NULL)
        goto done;

    for (uint32_t i = 0; i < dg->vertex_count; i++) {
        pdc_an_node_t *node = (pdc_an_node_t *)dg->vertices[i]->data;
        if (node != NULL && node->name != NULL && !strcmp(node->name, name)) {
            ret_value = node;
            goto done;
        }
    }

done:
    return ret_value;
}

pdc_an_state_t *
PDCan_dg_get_state(pdc_dg_t *dg, const char *name)
{
    pdc_an_node_t *node = PDCan_dg_get_node(dg, name);

    if (node == NULL || node->kind != PDC_AN_NODE_STATE)
        return NULL;

    return &node->u.state;
}

#ifndef JSONC_ENABLED
/* JSON-C wasn't found at configure time (see root CMakeLists.txt) -- the
 * only thing that's actually unavailable is attaching a graph; every
 * other PDC operation (object/region create, read, write, ...) works
 * unaffected. Callers already treat a NULL return the same as any other
 * graph-load failure (see PDCan_dg_json_create, src/api/pdc_an/pdc_an.c). */
pdc_dg_t *
PDCan_dg_json_create_common(char *filepath)
{
    LOG_ERROR("PDCan_dg_json_create_common: JSON-C support was not compiled in (JSONC_ROOT_DIR was not "
              "set at build time) -- cannot parse analysis graph \"%s\"\n",
              filepath);
    return NULL;
}
#else
pdc_dg_t *
PDCan_dg_json_create_common(char *filepath)
{
    FUNC_ENTER(NULL);

    pdc_dg_t *          ret_value = NULL;
    pdc_dg_t *          dg_cpy    = NULL;
    FILE *              fp        = NULL;
    struct json_object *json_obj  = NULL;
    io_buffer_t         io_buffer;
    memset(&io_buffer, 0, sizeof(io_buffer_t));

    if ((fp = open_file(filepath, IO_MODE_READ)) == NULL)
        PGOTO_ERROR(NULL, "Failed to open_file: %s\n", filepath);
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

    ret_value = PDCdg_create(NULL, an_vertices_are_equal, an_graph_free, an_edge_free, an_vertex_free);
    if (ret_value == NULL)
        PGOTO_ERROR(NULL, "Failed to create directed graph");
    dg_cpy          = ret_value;
    ret_value->data = strdup(filepath);

    struct array_list *states          = get_json_array(json_obj, "states");
    struct array_list *transformations = get_json_array(json_obj, "transformations");
    if (states == NULL || transformations == NULL)
        PGOTO_DONE(NULL);

    /* Pass 1: declare every state as a vertex up front, so transformations
     * below can only ever reference states that truly exist. pdc_dg's
     * add_edge silently creates a new vertex from whatever lookup key it's
     * given when a name isn't found yet; declaring states first (and
     * validating below) means every lookup key we hand it for a state
     * always resolves to this real, heap-allocated vertex. */
    int states_length = array_list_length(states);
    for (int i = 0; i < states_length; i++) {
        struct json_object *s = array_list_get_idx(states, i);

        const char *s_name_str = get_json_string(s, "name", true);
        if (s_name_str == NULL)
            PGOTO_DONE(NULL);
        char *s_name = strdup(s_name_str);

        const char *s_persistence_str = get_json_string(s, "persistence", true);
        if (s_persistence_str == NULL)
            PGOTO_DONE(NULL);

        pdc_an_persistence_t persistence       = PDC_AN_TRANSIENT;
        bool                 found_persistence = false;
        for (int j = 0; j < PDC_AN_NUM_PERSISTENCE; j++) {
            if (!strcmp(s_persistence_str, pdc_an_persistence_strs[j])) {
                found_persistence = true;
                persistence       = (pdc_an_persistence_t)j;
                break;
            }
        }
        if (!found_persistence)
            PGOTO_ERROR(NULL, "Invalid persistence \"%s\" for state \"%s\"\n", s_persistence_str, s_name);

        /* Optional -- only meaningful for states that turn out to be an
         * output (checked in pass 2 below, since is_output isn't known
         * yet here); defaults to eager, preserving this framework's
         * original attach-order-determined behavior for any graph JSON
         * that doesn't declare it. */
        const char *     s_trigger_str = get_json_string(s, "trigger", false);
        pdc_an_trigger_t trigger       = PDC_AN_TRIGGER_EAGER;
        if (s_trigger_str != NULL) {
            bool found_trigger = false;
            for (int j = 0; j < PDC_AN_NUM_TRIGGER; j++) {
                if (!strcmp(s_trigger_str, pdc_an_trigger_strs[j])) {
                    found_trigger = true;
                    trigger       = (pdc_an_trigger_t)j;
                    break;
                }
            }
            if (!found_trigger)
                PGOTO_ERROR(NULL, "Invalid trigger \"%s\" for state \"%s\"\n", s_trigger_str, s_name);
        }

        pdc_an_node_t *node       = PDC_calloc(1, sizeof(pdc_an_node_t));
        node->kind                = PDC_AN_NODE_STATE;
        node->name                = s_name;
        node->u.state.name        = s_name;
        node->u.state.persistence = persistence;
        node->u.state.trigger     = trigger;
        node->u.state.is_output   = false;

        if (PDCdg_add_vertex(ret_value, node) == PDC_DG_INVALID_VERTEX)
            PGOTO_ERROR(NULL, "Failed to add state vertex \"%s\" to directed graph\n", s_name);
    }

    /* Pass 2: transformations. Each becomes its own vertex (namespaced with
     * PDC_AN_FUNC_NODE_PREFIX so it can never collide with a state name),
     * with an edge from every declared input state and an edge to every
     * declared output state. */
    int transformations_length = array_list_length(transformations);
    for (int i = 0; i < transformations_length; i++) {
        struct json_object *t = array_list_get_idx(transformations, i);

        const char *t_name_str = get_json_string(t, "name", true);
        if (t_name_str == NULL)
            PGOTO_DONE(NULL);
        char *t_name = strdup(t_name_str);

        /* Optional: a transformation that omits "device" gets one variant
         * per device its name is registered under (see PDCan_link_builtin_func),
         * letting PDCan_exec_graph pick dynamically at execution time
         * instead of statically at parse time. */
        const char *t_device   = get_json_string(t, "device", false);
        const char *t_location = get_json_string(t, "location", true);
        if (t_location == NULL)
            PGOTO_DONE(NULL);

        const char *t_params_str = get_json_string(t, "params", false);

        struct array_list *t_inputs  = get_json_array(t, "inputs");
        struct array_list *t_outputs = get_json_array(t, "outputs");
        if (t_inputs == NULL || t_outputs == NULL)
            PGOTO_ERROR(NULL, "Transformation \"%s\" must specify \"inputs\" and \"outputs\" arrays\n",
                        t_name);

        int t_num_inputs  = array_list_length(t_inputs);
        int t_num_outputs = array_list_length(t_outputs);
        if (t_num_inputs == 0 || t_num_outputs == 0)
            PGOTO_ERROR(NULL, "Transformation \"%s\" must have at least one input and one output\n", t_name);

        /* PDC_TF_NUM_DEVICES doubles as the "any device" sentinel when
         * "device" was omitted from the JSON -- see PDCan_link_builtin_func. */
        pdc_tf_dev_t dev = PDC_TF_NUM_DEVICES;
        if (t_device != NULL) {
            bool found_device = false;
            for (int j = 0; j < PDC_TF_NUM_DEVICES; j++) {
                if (!strcmp(t_device, pdc_tf_dev_strs[j])) {
                    found_device = true;
                    dev          = (pdc_tf_dev_t)j;
                    break;
                }
            }
            if (!found_device)
                PGOTO_ERROR(NULL, "Invalid device \"%s\" for transformation \"%s\"\n", t_device, t_name);
        }

        pdc_tf_location_t location       = PDC_TF_BUILTIN;
        bool              found_location = false;
        for (int j = 0; j < PDC_TF_NUM_LOCATIONS; j++) {
            if (!strcmp(t_location, pdc_tf_location_strs[j])) {
                found_location = true;
                location       = (pdc_tf_location_t)j;
                break;
            }
        }
        if (!found_location)
            PGOTO_ERROR(NULL, "Invalid location \"%s\" for transformation \"%s\"\n", t_location, t_name);

        if (location == PDC_TF_EXTERNAL) {
            if (t_device == NULL)
                PGOTO_ERROR(NULL,
                            "Transformation \"%s\" is external but omits \"device\"; an external symbol "
                            "resolves to exactly one function pointer for exactly one device, so "
                            "\"device\" must be specified\n",
                            t_name);
            if (lib_path == NULL)
                PGOTO_ERROR(NULL, "Transformation \"%s\" is external but no \"lib_path\" was provided\n",
                            t_name);

            void *handle = dlopen(lib_path, RTLD_LAZY);
            if (!handle)
                PGOTO_ERROR(NULL, "Failed to dlopen library at path %s: %s\n", lib_path, dlerror());

            dlerror();
            void *func_ptr = dlsym(handle, t_name);
            char *error;
            if ((error = dlerror()) != NULL)
                PGOTO_ERROR(NULL, "Failed to find symbol %s in library %s: %s\n", t_name, lib_path, error);

            if (PDCan_add_builtin_func(t_name, (a_func_t)func_ptr, dev) != SUCCEED)
                PGOTO_ERROR(NULL, "Failed to add external function \"%s\" to builtin functions vector\n",
                            t_name);
        }

        pdc_an_func_t func_data;
        memset(&func_data, 0, sizeof(func_data));
        func_data.name = t_name;

        if (PDCan_link_builtin_func(t_name, dev, location, t_params_str, &func_data) != SUCCEED)
            PGOTO_ERROR(NULL, "Failed to link transformation \"%s\" to a builtin function\n", t_name);

        func_data.num_inputs   = t_num_inputs;
        func_data.input_names  = PDC_calloc(t_num_inputs, sizeof(char *));
        func_data.num_outputs  = t_num_outputs;
        func_data.output_names = PDC_calloc(t_num_outputs, sizeof(char *));

        for (int j = 0; j < t_num_inputs; j++) {
            struct json_object *s_obj = array_list_get_idx(t_inputs, j);
            if (json_object_get_type(s_obj) != json_type_string)
                PGOTO_ERROR(NULL, "Input %d of transformation \"%s\" was not a string\n", j, t_name);
            func_data.input_names[j] = strdup(json_object_get_string(s_obj));
        }
        for (int j = 0; j < t_num_outputs; j++) {
            struct json_object *s_obj = array_list_get_idx(t_outputs, j);
            if (json_object_get_type(s_obj) != json_type_string)
                PGOTO_ERROR(NULL, "Output %d of transformation \"%s\" was not a string\n", j, t_name);
            func_data.output_names[j] = strdup(json_object_get_string(s_obj));
        }

        char *fn_node_name = PDC_malloc(strlen(PDC_AN_FUNC_NODE_PREFIX) + strlen(t_name) + 1);
        sprintf(fn_node_name, "%s%s", PDC_AN_FUNC_NODE_PREFIX, t_name);

        /* PDCdg_add_vertex does not dedup by name -- a second transformation
         * entry sharing this name would otherwise silently attach all of
         * its edges to whichever vertex PDCan_dg_get_node finds first
         * (first-match linear scan), leaving this one permanently
         * unreachable with no error. Catch it here instead. */
        if (PDCan_dg_get_node(ret_value, fn_node_name) != NULL)
            PGOTO_ERROR(NULL,
                        "Transformation \"%s\" is declared more than once; to give it multiple device "
                        "variants, omit \"device\" from a single entry instead\n",
                        t_name);

        pdc_an_node_t *fn_node = PDC_calloc(1, sizeof(pdc_an_node_t));
        fn_node->kind          = PDC_AN_NODE_FUNC;
        fn_node->name          = fn_node_name;
        fn_node->u.func        = func_data;

        if (PDCdg_add_vertex(ret_value, fn_node) == PDC_DG_INVALID_VERTEX)
            PGOTO_ERROR(NULL, "Failed to add transformation vertex \"%s\" to directed graph\n", t_name);

        pdc_an_node_t fn_lookup;
        memset(&fn_lookup, 0, sizeof(fn_lookup));
        fn_lookup.name = fn_node_name;

        for (int j = 0; j < t_num_inputs; j++) {
            char *in_name = func_data.input_names[j];

            pdc_an_node_t *in_node = PDCan_dg_get_node(ret_value, in_name);
            if (in_node == NULL || in_node->kind != PDC_AN_NODE_STATE)
                PGOTO_ERROR(NULL,
                            "Transformation \"%s\" references undeclared input state \"%s\"; every "
                            "input/output must appear in \"states\"\n",
                            t_name, in_name);

            pdc_an_node_t in_lookup;
            memset(&in_lookup, 0, sizeof(in_lookup));
            in_lookup.name = in_name;
            if (PDCdg_add_edge(ret_value, &in_lookup, &fn_lookup, NULL) == PDC_DG_INVALID_EDGE)
                PGOTO_ERROR(NULL, "Failed to add edge \"%s\" -> \"%s\"\n", in_name, fn_node_name);
        }

        for (int j = 0; j < t_num_outputs; j++) {
            char *out_name = func_data.output_names[j];

            pdc_an_node_t *out_node = PDCan_dg_get_node(ret_value, out_name);
            if (out_node == NULL || out_node->kind != PDC_AN_NODE_STATE)
                PGOTO_ERROR(NULL,
                            "Transformation \"%s\" references undeclared output state \"%s\"; every "
                            "input/output must appear in \"states\"\n",
                            t_name, out_name);
            if (out_node->u.state.is_output)
                PGOTO_ERROR(NULL,
                            "State \"%s\" is produced by more than one transformation; every state may "
                            "have at most one producer\n",
                            out_name);
            out_node->u.state.is_output = true;

            pdc_an_node_t out_lookup;
            memset(&out_lookup, 0, sizeof(out_lookup));
            out_lookup.name = out_name;
            if (PDCdg_add_edge(ret_value, &fn_lookup, &out_lookup, NULL) == PDC_DG_INVALID_EDGE)
                PGOTO_ERROR(NULL, "Failed to add edge \"%s\" -> \"%s\"\n", fn_node_name, out_name);
        }
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
