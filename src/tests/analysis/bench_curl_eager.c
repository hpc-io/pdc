/**
 * E3SM-shaped curl/vorticity-magnitude benchmark, eager mode: writes a
 * local (nx, ny, nz_per_rank) block of a synthetic wind-velocity field
 * (u, v, w) per rank through the region-analysis framework (DataFlyway)
 * with the curl_vorticity_magnitude graph attached -- curl_x/y/z and
 * vorticity_magnitude are computed server-side, eagerly, in the write
 * path (see an_client/graphs/curl_vorticity_magnitude.json and the
 * builtin "curl"/"vector_magnitude" functions in src/server/analysis/).
 * A confirmation read of vorticity_magnitude follows.
 *
 * Every state in the graph is persistent, matching the source paper's
 * "Store" strategy (curl and magnitude are both real, queryable objects,
 * not just transient intermediates) -- see
 * pdc_helper_scripts/curl_analysis_scripts/README.md for how this
 * benchmark's shape/sizing was derived from that paper's reported data
 * volumes.
 *
 * With --compress, vorticity_magnitude is additionally composed with the
 * existing GPU ZFP compression transform (tf_client/graphs/zfp_gpu.json)
 * before the analysis graph is attached, so the server transparently
 * stores it compressed and decompresses transparently on read -- no new
 * compression code, just reusing PDC's existing transformation
 * framework, composed onto an analysis output exactly as the poster's
 * "Composes with transformations" design intends.
 *
 * Like analysis_scripts/'s magnitude benchmark, runs N_TIMESTEPS
 * repetitions of write(+compute) within one continuous client/server
 * session, each timestep producing a distinct, uniquely named set of
 * u/v/w/curl_x/y/z/vorticity_magnitude objects (see the loop below for
 * why per-timestep names are required rather than a shared name).
 *
 * Usage: bench_curl_eager <nx> <ny> <nz_per_rank> <compress:0|1>
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,nx,ny,nz_per_rank,compress,setup_s,write_s,confirm_read_s,step_total_s,bad
 */

#define N_TIMESTEPS 3

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <mpi.h>

#include "pdc.h"
#include "curl_math.h"

#define EPSILON 1e-3

static void
do_transfer(void *buf, pdc_access_t access, pdcid_t obj, pdcid_t reg, pdcid_t reg_global, const char *what)
{
    pdcid_t tr = PDCregion_transfer_create(buf, access, obj, reg, reg_global);
    if (tr == 0) {
        fprintf(stderr, "PDCregion_transfer_create failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (PDCregion_transfer_start(tr) < 0) {
        fprintf(stderr, "PDCregion_transfer_start failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (PDCregion_transfer_wait(tr) < 0) {
        fprintf(stderr, "PDCregion_transfer_wait failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (PDCregion_transfer_close(tr) < 0) {
        fprintf(stderr, "PDCregion_transfer_close failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int
main(int argc, char **argv)
{
    int    rank, nranks;
    long   nx, ny, nz_per_rank;
    int    compress;
    int    transient;
    int    step;
    size_t i;

    double t_setup0, t_setup1, t_write0, t_write1, t_read0, t_read1;

    if (argc < 5) {
        fprintf(stderr, "Usage: %s <nx> <ny> <nz_per_rank> <compress:0|1> [transient:0|1]\n", argv[0]);
        return 1;
    }
    nx          = atol(argv[1]);
    ny          = atol(argv[2]);
    nz_per_rank = atol(argv[3]);
    compress    = atoi(argv[4]);
    /* transient=1 declares curl_x/y/z as transient graph states (see
     * an_client/graphs/curl_vorticity_magnitude_transient.json): they are
     * computed server-side within the write call and never persisted, so
     * they get no PDC object and no region attachment here. */
    transient = (argc >= 6) ? atoi(argv[5]) : 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    size_t n_elem = (size_t)nx * (size_t)ny * (size_t)nz_per_rank;

    float * u            = (float *)malloc(sizeof(float) * n_elem);
    float * v            = (float *)malloc(sizeof(float) * n_elem);
    float * w            = (float *)malloc(sizeof(float) * n_elem);
    double *mag_expected = (double *)malloc(sizeof(double) * n_elem);
    double *mag_read     = (double *)malloc(sizeof(double) * n_elem);

    for (i = 0; i < n_elem; ++i) {
        u[i] = (float)((i % 1000) + 1);
        v[i] = (float)(((i + 137) % 1000) + 1);
        w[i] = (float)(((i + 613) % 1000) + 1);
    }

    /* Recomputed once outside the loop: the correctness check regenerates
     * the same deterministic u/v/w every timestep, so expected curl and
     * magnitude are identical across timesteps too. */
    double *curl_x_expected = (double *)malloc(sizeof(double) * n_elem);
    double *curl_y_expected = (double *)malloc(sizeof(double) * n_elem);
    double *curl_z_expected = (double *)malloc(sizeof(double) * n_elem);
    curl_math_compute(u, v, w, (size_t)nx, (size_t)ny, (size_t)nz_per_rank, curl_x_expected, curl_y_expected,
                      curl_z_expected);
    for (i = 0; i < n_elem; ++i) {
        double cx = curl_x_expected[i], cy = curl_y_expected[i], cz = curl_z_expected[i];
        mag_expected[i] = sqrt(cx * cx + cy * cy + cz * cz);
    }

    uint64_t local_offset[3], global_offset[3], region_len[3], dims[3];
    local_offset[0]  = 0;
    local_offset[1]  = 0;
    local_offset[2]  = 0;
    global_offset[0] = 0;
    global_offset[1] = 0;
    global_offset[2] = (uint64_t)rank * (uint64_t)nz_per_rank;
    region_len[0]    = (uint64_t)nx;
    region_len[1]    = (uint64_t)ny;
    region_len[2]    = (uint64_t)nz_per_rank;
    dims[0]          = (uint64_t)nx;
    dims[1]          = (uint64_t)ny;
    dims[2]          = (uint64_t)nranks * (uint64_t)nz_per_rank;

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup0 = MPI_Wtime();

    pdcid_t pdc = PDCinit("pdc");
    if (pdc == 0) {
        fprintf(stderr, "PDCinit failed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    const char *cont_name = "curl_bench_shared";
    pdcid_t     cont_prop = 0, cont = 0;
    pdcid_t     prop_float = 0, prop_double = 0;
    pdcid_t     u_obj = 0, v_obj = 0, w_obj = 0;
    pdcid_t     curl_x_obj = 0, curl_y_obj = 0, curl_z_obj = 0, mag_obj = 0;

    /* Every rank calls the collective _col/_mpi variants (see
     * bench_magnitude.c's comment for why plain create+barrier+open is
     * unsafe -- confirmed on Perlmutter to race when a non-creating
     * rank's PDCobj_open can observe "not found" while the server is
     * still processing a prior timestep's create). */
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    cont      = PDCcont_create_col(cont_name, cont_prop);

    prop_float = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_type(prop_float, PDC_FLOAT);
    PDCprop_set_obj_dims(prop_float, 3, dims);
    PDCprop_set_obj_user_id(prop_float, getuid());
    PDCprop_set_obj_app_name(prop_float, "BenchCurlVorticity");
    PDCprop_set_obj_tags(prop_float, "tag0=1");
    PDCprop_set_obj_transfer_region_type(prop_float, PDC_REGION_STATIC);

    prop_double = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_type(prop_double, PDC_DOUBLE);
    PDCprop_set_obj_dims(prop_double, 3, dims);
    PDCprop_set_obj_user_id(prop_double, getuid());
    PDCprop_set_obj_app_name(prop_double, "BenchCurlVorticity");
    PDCprop_set_obj_tags(prop_double, "tag0=1");
    PDCprop_set_obj_transfer_region_type(prop_double, PDC_REGION_STATIC);

    /* Same region geometry (offset/length) applies to every state here --
     * they're all colocated blocks of the same per-rank domain slice, just
     * different objects/dtypes, so one reg/reg_global pair is reused for
     * every create/open/attach/transfer call below (same pattern as
     * bench_magnitude.c). */
    pdcid_t reg        = PDCregion_create(3, local_offset, region_len);
    pdcid_t reg_global = PDCregion_create(3, global_offset, region_len);

    /* Both graphs are created once, outside the loop, and reattached to a
     * fresh set of per-timestep objects on every iteration below -- same
     * pattern bench_magnitude.c uses for its vector_magnitude graph. */
    pdcid_t tf_dg_id = 0;
    if (compress) {
        tf_dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "zfp_gpu.json");
        if (tf_dg_id == 0) {
            fprintf(stderr, "PDCtf_dg_json_create (zfp_gpu) failed\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    char an_graph_path[512];
    snprintf(an_graph_path, sizeof(an_graph_path), "%s%s", AN_GRAPHS_DIR,
             transient ? "curl_vorticity_magnitude_transient.json" : "curl_vorticity_magnitude.json");
    pdcid_t an_dg_id = PDCan_dg_json_create(an_graph_path);
    if (an_dg_id == 0) {
        fprintf(stderr, "PDCan_dg_json_create failed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();

    double local_setup = t_setup1 - t_setup0;
    double max_setup;
    MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    /* Per-timestep-unique names ("u_0", "u_1", ...) rather than a shared
     * name with an incrementing time_step property -- see
     * bench_magnitude.c's comment for the full reasoning (irrelevant here
     * in practice since this benchmark never reopens objects by name, but
     * kept consistent with every other object-creating loop in this
     * evaluation). */
    int global_bad = 0;
    for (step = 0; step < N_TIMESTEPS; ++step) {
        char u_name[32], v_name[32], w_name[32];
        char curl_x_name[32], curl_y_name[32], curl_z_name[32], mag_name[32];
        snprintf(u_name, sizeof(u_name), "u_%d", step);
        snprintf(v_name, sizeof(v_name), "v_%d", step);
        snprintf(w_name, sizeof(w_name), "w_%d", step);
        snprintf(curl_x_name, sizeof(curl_x_name), "curl_x_%d", step);
        snprintf(curl_y_name, sizeof(curl_y_name), "curl_y_%d", step);
        snprintf(curl_z_name, sizeof(curl_z_name), "curl_z_%d", step);
        snprintf(mag_name, sizeof(mag_name), "vorticity_magnitude_%d", step);

        u_obj = PDCobj_create_mpi(cont, u_name, prop_float, 0, MPI_COMM_WORLD);
        v_obj = PDCobj_create_mpi(cont, v_name, prop_float, 0, MPI_COMM_WORLD);
        w_obj = PDCobj_create_mpi(cont, w_name, prop_float, 0, MPI_COMM_WORLD);
        if (!transient) {
            curl_x_obj = PDCobj_create_mpi(cont, curl_x_name, prop_double, 0, MPI_COMM_WORLD);
            curl_y_obj = PDCobj_create_mpi(cont, curl_y_name, prop_double, 0, MPI_COMM_WORLD);
            curl_z_obj = PDCobj_create_mpi(cont, curl_z_name, prop_double, 0, MPI_COMM_WORLD);
        }
        mag_obj = PDCobj_create_mpi(cont, mag_name, prop_double, 0, MPI_COMM_WORLD);
        if (u_obj == 0 || v_obj == 0 || w_obj == 0 ||
            (!transient && (curl_x_obj == 0 || curl_y_obj == 0 || curl_z_obj == 0)) || mag_obj == 0) {
            fprintf(stderr, "Failed to create one or more step-%d objects\n", step);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        if (compress) {
            /* Composed onto vorticity_magnitude BEFORE the analysis graph
             * is attached below, so PDCan_attach_to_region's
             * find_attached_tf_info picks it up and piggybacks it
             * alongside the analysis binding -- see src/api/pdc_an/pdc_an.c. */
            PDCtf_attach_to_region(tf_dg_id, mag_obj, reg_global, "decompressed", "compressed");
        }

        PDCan_attach_to_region(an_dg_id, "u", u_obj, reg_global);
        PDCan_attach_to_region(an_dg_id, "v", v_obj, reg_global);
        PDCan_attach_to_region(an_dg_id, "w", w_obj, reg_global);
        if (!transient) {
            PDCan_attach_to_region(an_dg_id, "curl_x", curl_x_obj, reg_global);
            PDCan_attach_to_region(an_dg_id, "curl_y", curl_y_obj, reg_global);
            PDCan_attach_to_region(an_dg_id, "curl_z", curl_z_obj, reg_global);
        }
        PDCan_attach_to_region(an_dg_id, "vorticity_magnitude", mag_obj, reg_global);

        /* Write u, v, w -- the last write transparently triggers
         * server-side curl + vector_magnitude computation and
         * persistence of every downstream state in the graph before this
         * phase's wait/close return. */
        t_write0 = MPI_Wtime();
        do_transfer(u, PDC_WRITE, u_obj, reg, reg_global, "write u");
        do_transfer(v, PDC_WRITE, v_obj, reg, reg_global, "write v");
        do_transfer(w, PDC_WRITE, w_obj, reg, reg_global, "write w");
        MPI_Barrier(MPI_COMM_WORLD);
        t_write1 = MPI_Wtime();

        /* Confirmation read of the final output only -- matches
         * bench_magnitude.c's eager confirm-read convention (curl_x/y/z
         * being persistent affects write-side storage cost, not what
         * this confirmation step checks). Transparently decompressed on
         * the way back if --compress was used. */
        t_read0 = MPI_Wtime();
        do_transfer(mag_read, PDC_READ, mag_obj, reg, reg_global, "read vorticity_magnitude");
        MPI_Barrier(MPI_COMM_WORLD);
        t_read1 = MPI_Wtime();

        /* Correctness check (not timed): u/v/w are the same deterministic
         * pattern every timestep, so mag_expected (computed once above)
         * applies unchanged. */
        int local_bad = 0;
        for (i = 0; i < n_elem; ++i) {
            if (fabs(mag_read[i] - mag_expected[i]) > EPSILON)
                local_bad++;
        }
        int step_bad = 0;
        MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        global_bad += step_bad;

        double local_write = t_write1 - t_write0;
        double local_read  = t_read1 - t_read0;

        double max_write, max_read;
        MPI_Reduce(&local_write, &max_write, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            double step_total = max_setup + max_write + max_read;
            printf("curl_eager,%d,%d,%ld,%ld,%ld,%d,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks, nx, ny,
                   nz_per_rank, compress, max_setup, max_write, max_read, step_total, step_bad);
            fflush(stdout);
        }

        PDCobj_close(u_obj);
        PDCobj_close(v_obj);
        PDCobj_close(w_obj);
        if (!transient) {
            PDCobj_close(curl_x_obj);
            PDCobj_close(curl_y_obj);
            PDCobj_close(curl_z_obj);
        }
        PDCobj_close(mag_obj);
    }

    PDCan_close_dg(an_dg_id);
    if (compress)
        PDCtf_close_dg(tf_dg_id);
    PDCregion_close(reg);
    PDCregion_close(reg_global);
    PDCcont_close(cont);
    PDCprop_close(prop_float);
    PDCprop_close(prop_double);
    PDCprop_close(cont_prop);
    PDCclose(pdc);

    free(u);
    free(v);
    free(w);
    free(mag_expected);
    free(mag_read);
    free(curl_x_expected);
    free(curl_y_expected);
    free(curl_z_expected);

    MPI_Finalize();
    return global_bad != 0;
}
