#ifndef PDC_TF_COMMON_H
#define PDC_TF_COMMON_H

#include "mercury_proc_string.h"

#include "pdc_public.h"
#include "pdc_dg.h"
#include "pdc_region.h"
#include "pdc_obj_pkg.h"
#include "pdc_vector.h"
#include "pdc_tf_user.h"

typedef struct pdc_tf_region_state_t {
    pdcid_t dg_id;
    char *  cur_state;
    char *  client_state;
    char *  store_state;
} pdc_tf_region_state_t;

typedef struct pdc_tf_region_mapping_t {
    pdc_tf_region_state_t region_state;
    pdc_tf_region_t       conceptual_region;
    uint64_t              conceptual_offset[DIM_MAX];

    // This is the region information for storing on disk
    pdc_tf_region_t actual_region;
} pdc_tf_region_mapping_t;

/**
 * This is a field in _pdc_obj_info that enables transformations
 * This is a mapping between conceptual and actual regions
 * Example: Conceptual region is the region before compression,
 *          actual region is the region after compression.
 *          The conceptual region is used to define the transformation,
 *          while the actual region is used to store the data.
 * The num_region is the number of regions with attached graphs.
 * The attach_to_all_regions indicates whether all region transfers
 * should go through the directed graph.
 */
typedef struct pdc_tf_obj_t {
    /**
     * These fields are for attaching graphs to region transfers
     * after the graph has been attached to the entire object
     */
    bool                  attach_to_all_regions;
    pdc_tf_region_state_t all_regions_state;

    // This field is used to attach graphs to individual regions
    PDC_VECTOR *region_mappings_vector;
} pdc_obj_tf_t;

extern char *pdc_tf_dev_strs[];
extern char *pdc_tf_location_strs[];

/**
 * Strings needed by server to run transformation
 */
typedef struct pdc_tf_pkg_t {
    uint32_t    pdc_var_type;
    hg_string_t json_filepath;
    hg_string_t cur_state;
    hg_string_t client_state;
    hg_string_t store_state;
} pdc_tf_pkg_t;

void append_host_to_dev_time(pdc_tf_builtin_func_t *func, double value);
void append_dev_to_host_time(pdc_tf_builtin_func_t *func, double value);
void append_exec_time(pdc_tf_builtin_func_t *func, double value);

double get_host_to_dev_avg(const pdc_tf_builtin_func_t *func);
double get_dev_to_host_avg(const pdc_tf_builtin_func_t *func);
double get_exec_avg(const pdc_tf_builtin_func_t *func);

// this is our global array of builtin functions
extern PDC_VECTOR *pdc_tf_builtin_funcs_vector_g;

/**
 * @brief Initializes a pdc_tf_region_t with the given dimensionality, type, and sizes.
 *
 * @param dest         Pointer to the region struct to populate.
 * @param ndim         Number of dimensions.
 * @param pdc_var_type Element type (e.g. PDC_FLOAT, PDC_DOUBLE).
 * @param size         Array of per-dimension sizes (must have at least ndim elements).
 * @return SUCCEED on success, FAIL otherwise.
 */
perr_t PDCtf_set_tf_region_t(pdc_tf_region_t *dest, uint8_t ndim, pdc_var_type_t pdc_var_type,
                             uint64_t *size);

/**
 * @brief Deep-copies a pdc_tf_region_t from src into dest.
 *
 * Copies ndim, pdc_var_type, and all size values.
 *
 * @param src  Source region to copy from.
 * @param dest Destination region to copy into.
 * @return SUCCEED on success, FAIL otherwise.
 */
perr_t PDCtf_copy_tf_region_t(pdc_tf_region_t *src, pdc_tf_region_t *dest);

/**
 * @brief Parses a JSON file and constructs a transformation directed graph (DG).
 *
 * The JSON file must describe a set of named states and functions connecting them,
 * with each function specifying its device (CPU/GPU), location (builtin/external),
 * input state, output state, and optional parameters. External functions are loaded
 * via dlopen/dlsym using the lib_path field.
 *
 * @param filepath Path to the JSON file describing the transformation graph.
 * @return Pointer to the newly created pdc_dg_t, or NULL on failure.
 */
pdc_dg_t *PDCtf_dg_json_create_common(char *filepath);

/**
 * @brief Registers all compiled-in builtin transform functions into the global function registry.
 *
 * Which functions are registered depends on compile-time feature flags:
 *   - ENABLE_TF_SZ_GPU_COMPRESSSION  → SZ GPU compress/decompress
 *   - ENABLE_TF_SZ_COMPRESSION       → SZ CPU compress/decompress
 *   - ENABLE_TF_ZFP_COMPRESSION      → ZFP CPU compress/decompress
 *   - CUDA_ENABLED                   → ZFP GPU compress/decompress
 *   - ENABLE_TF_SECRET_BOX_ENCRYPTION→ SecretBox encrypt/decrypt
 *   - ENABLE_TF_TURBO_COMPRESSION    → Turbo compress/decompress
 *
 * Must be called before any transform graph is used.
 *
 * @return SUCCEED on success, FAIL otherwise.
 */
perr_t PDCtf_init_builtin_funcs();

/**
 * @brief Registers a single function pointer in the global builtin function registry.
 *
 * Associates a name and device type with a C function pointer so it can later
 * be looked up by PDCtf_link_builtin_func().
 *
 * @param func_name Name to register the function under (e.g. "zfp_compress").
 * @param c_func    The function pointer to register.
 * @param dev       Device the function runs on (PDC_TF_CPU_DEVICE or PDC_TF_GPU_DEVICE).
 * @return SUCCEED on success, FAIL otherwise.
 */
perr_t PDCtf_add_builtin_func(char *func_name, c_func_t c_func, pdc_tf_dev_t dev);

/**
 * @brief Resolves a named builtin function and binds it to a pdc_tf_func_t.
 *
 * Searches the global builtin registry for a function matching both the given
 * name and device, and sets f->c_func to the matching function pointer.
 *
 * @param func_name Name of the function to look up (e.g. "sz_decompress").
 * @param dev       Device to match against (PDC_TF_CPU_DEVICE or PDC_TF_GPU_DEVICE).
 * @param f         Output pdc_tf_func_t whose c_func field will be set on success.
 * @return SUCCEED if found and linked, FAIL otherwise.
 */
perr_t PDCtf_link_builtin_func(char *func_name, pdc_tf_dev_t dev, pdc_tf_func_t *f);

/**
 * @brief Checks whether a region of a PDC transform object has an attached transformation graph.
 *
 * Iterates over all region mappings on the given transform object and checks whether
 * any conceptual region matches the provided ndim, unit, offset, and size. If a match
 * is found, region_mapping is set to point to it.
 *
 * @param tf_obj         The transform object to search.
 * @param ndim           Number of dimensions of the query region.
 * @param unit           Element size in bytes (used to match pdc_var_type).
 * @param offset         Per-dimension offsets of the query region.
 * @param size           Per-dimension sizes of the query region.
 * @param region_mapping Output pointer set to the matching region mapping if found.
 * @return true if a matching region mapping was found, false otherwise.
 */
bool PDCtf_region_has_attached_graph(struct pdc_tf_obj_t *tf_obj, int ndim, size_t unit, uint64_t *offset,
                                     uint64_t *size, pdc_tf_region_mapping_t **region_mapping);

/**
 * @brief Returns the total number of elements in a region (product of all dimension sizes).
 *
 * @param reg The region descriptor.
 * @return Total element count across all dimensions.
 */
size_t PDCtf_get_pdc_region_t_elements(pdc_tf_region_t reg);

/**
 * @brief Computes a flat (linear) offset from a multi-dimensional offset array.
 *
 * Uses row-major (C-order) stride calculation over the provided dimension sizes.
 *
 * @param ndim   Number of dimensions (must be > 0).
 * @param offset Per-dimension offset values.
 * @param dims   Per-dimension sizes used to compute strides.
 * @return Flat linear offset corresponding to the multi-dimensional offset.
 */
size_t PDCtf_get_flat_conceptual_offset(int ndim, uint64_t offset[4], const uint64_t *dims);

/**
 * @brief Returns the total size in bytes of all elements in a region.
 *
 * Equivalent to PDCtf_get_pdc_region_t_elements(reg) * PDC_get_var_type_size(reg.pdc_var_type).
 *
 * @param reg The region descriptor.
 * @return Total byte size of the region's data.
 */
size_t PDCtf_get_pdc_region_t_bytes(pdc_tf_region_t reg);

/**
 * @brief Logs the contents of a pdc_tf_region_t for debugging purposes.
 *
 * Currently a no-op stub; intended to print ndim, type, and size array.
 *
 * @param reg The region to log.
 */
void PDCtf_log_pdc_region_t(pdc_tf_region_t reg);

/**
 * @brief Prints the sequence of transform functions along the shortest path between two states.
 *
 * Runs Dijkstra/BFS on the directed graph from cur_state to desired_state and
 * logs each edge (function) along the resulting path.
 *
 * @param dg            The transformation directed graph to search.
 * @param cur_state     Name of the starting state.
 * @param desired_state Name of the target state.
 */
void PDCtf_print_exec_path_common(pdc_dg_t *dg, char *cur_state, char *desired_state);

/**
 * @brief Prints the full transformation graph in Graphviz DOT format.
 *
 * Outputs all edges with their function names and device (CPU=blue, GPU=red).
 * If write_to_file is true, output is redirected to "graph.txt"; otherwise
 * it goes to stdout.
 *
 * @param dg            The transformation directed graph to print.
 * @param write_to_file If true, write DOT output to "graph.txt" instead of stdout.
 */
void PDCtf_print_dg_common(pdc_dg_t *dg, bool write_to_file);

#endif // PDC_TF_COMMON_H
