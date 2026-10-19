/**
 * E3SM-shaped curl/vorticity-magnitude benchmark, posthoc phase 2 of 2:
 * opens the u/v/w objects a prior, already-exited bench_curl_write run
 * created (against a PDC server that's since been closed and restarted
 * with the `restart` argument to reload that data from its checkpoint),
 * reads them back, computes curl and then vorticity magnitude
 * client-side -- both via the shared kernel in curl_math.h / a plain
 * sqrt(x^2+y^2+z^2) reduction -- and writes both curl_x/y/z and
 * vorticity_magnitude out as plain PDC objects, all in this one process.
 *
 * This intentionally matches the two-binary (write, then analyze) shape
 * of analysis_scripts/'s magnitude benchmark (bench_write_components.c /
 * bench_posthoc_analyze.c) rather than splitting "compute curl" and
 * "compute magnitude" into their own separately relaunched phases: a
 * real posthoc analysis workload reads the data someone else wrote once
 * and derives whatever downstream quantities it needs in that same pass,
 * it doesn't artificially reopen the server between two computations it
 * could just as well do back to back. curl is still written out here
 * (not just magnitude) so the data volume this mode persists stays
 * comparable to eager's "Store" strategy (see README.md) -- it's simply
 * computed and written in the same pass as magnitude instead of forcing
 * its own intervening server close/restart the way a genuinely separate
 * "compute curl" job would.
 *
 * Like bench_curl_write.c, runs N_TIMESTEPS repetitions within this one
 * session, reading back N_TIMESTEPS distinct sets of u/v/w
 * (per-timestep-unique names "u_0", "u_1", ... -- see
 * bench_write_components.c's comment on why PDCobj_open() can't
 * disambiguate timesteps by a shared name) and producing N_TIMESTEPS
 * distinct sets of curl_x/y/z/vorticity_magnitude objects.
 *
 * With --compress, vorticity_magnitude is created with the existing GPU
 * ZFP compression transform (tf_client/graphs/zfp_gpu.json) composed onto
 * it before the write -- the transformation framework already applies
 * transparently to a plain object write, no analysis graph involved here
 * at all (posthoc never uses one).
 *
 * Usage: bench_curl_analyze <nx> <ny> <nz_per_rank> <compress:0|1>
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,nx,ny,nz_per_rank,compress,setup_s,readback_s,curl_compute_s,curl_writeback_s,magnitude_compute_s,magnitude_writeback_s,step_total_s,bad
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
    int    step;
    size_t i;

    double t_setup0, t_setup1, t_readback0, t_readback1;
    double t_curl_compute0, t_curl_compute1, t_curl_wb0, t_curl_wb1;
    double t_mag_compute0, t_mag_compute1, t_mag_wb0, t_mag_wb1;

    if (argc < 5) {
        fprintf(stderr, "Usage: %s <nx> <ny> <nz_per_rank> <compress:0|1>\n", argv[0]);
        return 1;
    }
    nx          = atol(argv[1]);
    ny          = atol(argv[2]);
    nz_per_rank = atol(argv[3]);
    compress    = atoi(argv[4]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    size_t n_elem = (size_t)nx * (size_t)ny * (size_t)nz_per_rank;

    float * u_rb   = (float *)malloc(sizeof(float) * n_elem);
    float * v_rb   = (float *)malloc(sizeof(float) * n_elem);
    float * w_rb   = (float *)malloc(sizeof(float) * n_elem);
    double *curl_x = (double *)malloc(sizeof(double) * n_elem);
    double *curl_y = (double *)malloc(sizeof(double) * n_elem);
    double *curl_z = (double *)malloc(sizeof(double) * n_elem);
    double *mag    = (double *)malloc(sizeof(double) * n_elem);

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

    const char *cont_name   = "curl_bench_shared";
    pdcid_t     prop_double = 0;
    pdcid_t     cont = 0, u_obj = 0, v_obj = 0, w_obj = 0;
    pdcid_t     curl_x_obj = 0, curl_y_obj = 0, curl_z_obj = 0, mag_obj = 0;

    /* Container already exists from the prior write-phase job. */
    cont = PDCcont_open(cont_name, pdc);

    prop_double = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_type(prop_double, PDC_DOUBLE);
    PDCprop_set_obj_dims(prop_double, 3, dims);
    PDCprop_set_obj_user_id(prop_double, getuid());
    PDCprop_set_obj_app_name(prop_double, "BenchCurlVorticity");
    PDCprop_set_obj_tags(prop_double, "tag0=1");
    PDCprop_set_obj_transfer_region_type(prop_double, PDC_REGION_STATIC);

    pdcid_t reg        = PDCregion_create(3, local_offset, region_len);
    pdcid_t reg_global = PDCregion_create(3, global_offset, region_len);

    /* Created once, reused every timestep -- PDCtf_attach_to_region is
     * called fresh below for each timestep's own mag_obj, the same way
     * bench_curl_eager.c reattaches its analysis graph to a fresh set of
     * objects every timestep. */
    pdcid_t tf_dg_id = 0;
    if (compress) {
        tf_dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "zfp_gpu.json");
        if (tf_dg_id == 0) {
            fprintf(stderr, "PDCtf_dg_json_create (zfp_gpu) failed\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();

    double local_setup = t_setup1 - t_setup0;
    double max_setup;
    MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    /* Like bench_curl_write.c, u/v/w/curl/magnitude use per-timestep-
     * unique names ("u_0", "u_1", ...) -- see that file's comment on why
     * PDCobj_open() can't disambiguate timesteps by a shared name. u/v/w
     * are already durable (written by a completely different,
     * already-exited process, with a full server close+restart in
     * between), so plain PDCobj_open is safe for those; curl_x/y/z and
     * vorticity_magnitude are created fresh here each iteration via the
     * collective PDCobj_create_mpi (see bench_magnitude.c for why plain
     * create+barrier+open is unsafe for objects this same process just
     * created). */
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

        u_obj = PDCobj_open(u_name, pdc);
        v_obj = PDCobj_open(v_name, pdc);
        w_obj = PDCobj_open(w_name, pdc);
        if (u_obj == 0 || v_obj == 0 || w_obj == 0) {
            fprintf(stderr, "Failed to open one or more step-%d input objects\n", step);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        curl_x_obj = PDCobj_create_mpi(cont, curl_x_name, prop_double, 0, MPI_COMM_WORLD);
        curl_y_obj = PDCobj_create_mpi(cont, curl_y_name, prop_double, 0, MPI_COMM_WORLD);
        curl_z_obj = PDCobj_create_mpi(cont, curl_z_name, prop_double, 0, MPI_COMM_WORLD);
        mag_obj    = PDCobj_create_mpi(cont, mag_name, prop_double, 0, MPI_COMM_WORLD);
        if (curl_x_obj == 0 || curl_y_obj == 0 || curl_z_obj == 0 || mag_obj == 0) {
            fprintf(stderr, "Failed to create one or more step-%d output objects\n", step);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        if (compress)
            PDCtf_attach_to_region(tf_dg_id, mag_obj, reg_global, "decompressed", "compressed");

        t_readback0 = MPI_Wtime();
        do_transfer(u_rb, PDC_READ, u_obj, reg, reg_global, "readback u");
        do_transfer(v_rb, PDC_READ, v_obj, reg, reg_global, "readback v");
        do_transfer(w_rb, PDC_READ, w_obj, reg, reg_global, "readback w");
        MPI_Barrier(MPI_COMM_WORLD);
        t_readback1 = MPI_Wtime();

        t_curl_compute0 = MPI_Wtime();
        curl_math_compute(u_rb, v_rb, w_rb, (size_t)nx, (size_t)ny, (size_t)nz_per_rank, curl_x, curl_y,
                          curl_z);
        MPI_Barrier(MPI_COMM_WORLD);
        t_curl_compute1 = MPI_Wtime();

        t_curl_wb0 = MPI_Wtime();
        do_transfer(curl_x, PDC_WRITE, curl_x_obj, reg, reg_global, "writeback curl_x");
        do_transfer(curl_y, PDC_WRITE, curl_y_obj, reg, reg_global, "writeback curl_y");
        do_transfer(curl_z, PDC_WRITE, curl_z_obj, reg, reg_global, "writeback curl_z");
        MPI_Barrier(MPI_COMM_WORLD);
        t_curl_wb1 = MPI_Wtime();

        /* magnitude is derived from curl_x/y/z already sitting in memory
         * from the compute step above -- no need to read curl back from
         * PDC (that would only be necessary if this were a genuinely
         * separate later process, which is exactly the split this
         * benchmark deliberately avoids -- see file header comment). */
        t_mag_compute0 = MPI_Wtime();
        for (i = 0; i < n_elem; ++i) {
            double cx = curl_x[i], cy = curl_y[i], cz = curl_z[i];
            mag[i] = sqrt(cx * cx + cy * cy + cz * cz);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        t_mag_compute1 = MPI_Wtime();

        t_mag_wb0 = MPI_Wtime();
        do_transfer(mag, PDC_WRITE, mag_obj, reg, reg_global, "writeback vorticity_magnitude");
        MPI_Barrier(MPI_COMM_WORLD);
        t_mag_wb1 = MPI_Wtime();

        /* Correctness check (not timed): u/v/w were generated with the
         * same deterministic pattern by bench_curl_write -- regenerate
         * them locally and recompute the expected curl/magnitude rather
         * than reading anything back a second time. */
        float *u = (float *)malloc(sizeof(float) * n_elem);
        float *v = (float *)malloc(sizeof(float) * n_elem);
        float *w = (float *)malloc(sizeof(float) * n_elem);
        for (i = 0; i < n_elem; ++i) {
            u[i] = (float)((i % 1000) + 1);
            v[i] = (float)(((i + 137) % 1000) + 1);
            w[i] = (float)(((i + 613) % 1000) + 1);
        }
        double *curl_x_expected = (double *)malloc(sizeof(double) * n_elem);
        double *curl_y_expected = (double *)malloc(sizeof(double) * n_elem);
        double *curl_z_expected = (double *)malloc(sizeof(double) * n_elem);
        curl_math_compute(u, v, w, (size_t)nx, (size_t)ny, (size_t)nz_per_rank, curl_x_expected,
                          curl_y_expected, curl_z_expected);

        int local_bad = 0;
        for (i = 0; i < n_elem; ++i) {
            double expected =
                sqrt(curl_x_expected[i] * curl_x_expected[i] + curl_y_expected[i] * curl_y_expected[i] +
                     curl_z_expected[i] * curl_z_expected[i]);
            if (fabs(mag[i] - expected) > EPSILON)
                local_bad++;
        }
        int step_bad = 0;
        MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        global_bad += step_bad;

        free(u);
        free(v);
        free(w);
        free(curl_x_expected);
        free(curl_y_expected);
        free(curl_z_expected);

        double local_readback  = t_readback1 - t_readback0;
        double local_curl_comp = t_curl_compute1 - t_curl_compute0;
        double local_curl_wb   = t_curl_wb1 - t_curl_wb0;
        double local_mag_comp  = t_mag_compute1 - t_mag_compute0;
        double local_mag_wb    = t_mag_wb1 - t_mag_wb0;

        double max_readback, max_curl_comp, max_curl_wb, max_mag_comp, max_mag_wb;
        MPI_Reduce(&local_readback, &max_readback, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_curl_comp, &max_curl_comp, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_curl_wb, &max_curl_wb, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_mag_comp, &max_mag_comp, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_mag_wb, &max_mag_wb, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            double step_total =
                max_setup + max_readback + max_curl_comp + max_curl_wb + max_mag_comp + max_mag_wb;
            printf("curl_posthoc_analyze,%d,%d,%ld,%ld,%ld,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step,
                   nranks, nx, ny, nz_per_rank, compress, max_setup, max_readback, max_curl_comp, max_curl_wb,
                   max_mag_comp, max_mag_wb, step_total, step_bad);
            fflush(stdout);
        }

        PDCobj_close(u_obj);
        PDCobj_close(v_obj);
        PDCobj_close(w_obj);
        PDCobj_close(curl_x_obj);
        PDCobj_close(curl_y_obj);
        PDCobj_close(curl_z_obj);
        PDCobj_close(mag_obj);
    }

    if (compress)
        PDCtf_close_dg(tf_dg_id);
    PDCregion_close(reg);
    PDCregion_close(reg_global);
    PDCcont_close(cont);
    PDCprop_close(prop_double);
    PDCclose(pdc);

    free(u_rb);
    free(v_rb);
    free(w_rb);
    free(curl_x);
    free(curl_y);
    free(curl_z);
    free(mag);

    MPI_Finalize();
    return global_bad != 0;
}
