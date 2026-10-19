/**
 * Posthoc benchmark, phase 2 of 2, EAGER-GRAPH variant ("eager_posthoc"):
 * opens the vx/vy/vz objects a prior, already-exited bench_write_components
 * run wrote (against a PDC server that has since been closed and restarted
 * with the `restart` argument to reload that data from its checkpoint),
 * attaches the DataFlyway vector_magnitude graph to them AND a freshly
 * created magnitude object -- in THIS process, not the one that wrote them
 * -- then reads magnitude back.
 *
 * Unlike bench_posthoc_analyze.c (which computes magnitude with a
 * hand-rolled client-side sqrt loop, entirely outside the analysis
 * framework), the read below is what triggers the *server* to
 * compute+persist magnitude via the same DataFlyway graph engine
 * bench_magnitude.c's eager mode uses. The difference from that eager mode
 * is where and when the graph gets attached: bench_magnitude.c attaches it
 * in the same session that writes vx/vy/vz, so the last component write
 * triggers computation immediately (PDCan_notify_input_written's
 * write-side hook). Here, vx/vy/vz were already written and made durable
 * by a completely different, already-exited process before this one even
 * started -- that write-time trigger never fires, because the graph
 * didn't exist yet when the write happened. Instead, this exercises
 * PDC_Server_data_io_region_analysis's read-side hook (see
 * src/server/analysis/pdc_an_server.c): a read of an unmaterialized
 * analysis output transparently triggers the minimal computation needed
 * to produce it first. That path doesn't care whether magnitude's
 * *inputs* were attached to this graph a millisecond or a full server
 * restart ago -- PDCan_exec_graph reads a leaf input's bytes straight from
 * its underlying PDC object (PDC_Server_transfer_request_io) regardless of
 * that leaf's own `materialized` flag, since `materialized` only gates
 * whether a state needs its *producer function* re-run, and leaf inputs
 * have no producer. So the only thing this benchmark needs to get right is
 * attaching vx/vy/vz/magnitude to a fresh graph instance before touching
 * magnitude at all, then reading it.
 *
 * This measures a real, different use case from both eager and plain
 * posthoc: attaching an analysis pipeline to data after the fact, in a
 * later session, without having planned for it (or paid any per-write
 * cost) when that data was originally produced.
 *
 * Like bench_posthoc_analyze.c, reads back N_TIMESTEPS distinct sets of
 * vx/vy/vz (per-timestep-unique names "vx_0", "vx_1", ... -- see
 * bench_write_components.c's comment on why PDCobj_open() can't
 * disambiguate timesteps by a shared name) and produces N_TIMESTEPS
 * distinct magnitude objects.
 *
 * Usage: bench_posthoc_analyze_eager <n_elem_per_rank>
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,readback_s,compute_s,writeback_s,step_total_s,bad
 * (compute_s/writeback_s are always 0 here -- the server does both,
 * inseparably, inside the timed read -- kept as always-0 columns so this
 * combines with the write phase's log via the exact same line-pairing
 * shape posthoc_pdc.sbatch already uses for bench_posthoc_analyze.c.)
 */

#define N_TIMESTEPS 3

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <mpi.h>

#include "pdc.h"

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
    long   n_elem;
    int    step;
    size_t i;

    double t_setup0, t_setup1, t_read0, t_read1;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <n_elem_per_rank>\n", argv[0]);
        return 1;
    }
    n_elem = atol(argv[1]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    double *mag = (double *)malloc(sizeof(double) * n_elem);
    memset(mag, 0, sizeof(double) * n_elem);

    uint64_t local_offset[1], global_offset[1], region_len[1], dims[1];
    local_offset[0]  = 0;
    global_offset[0] = (uint64_t)rank * (uint64_t)n_elem;
    region_len[0]    = (uint64_t)n_elem;
    dims[0]          = (uint64_t)nranks * (uint64_t)n_elem;

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup0 = MPI_Wtime();

    pdcid_t pdc = PDCinit("pdc");
    if (pdc == 0) {
        fprintf(stderr, "PDCinit failed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    const char *cont_name    = "bench_shared";
    pdcid_t     obj_prop_out = 0;
    pdcid_t     cont = 0, vx_obj = 0, vy_obj = 0, vz_obj = 0, mag_obj = 0;

    /* Container and vx/vy/vz already exist from the prior write-phase job
     * (durable across the server close+restart between that job and this
     * one) -- see bench_posthoc_analyze.c for why plain PDCcont_open /
     * PDCobj_open is safe for those here. magnitude_N is created fresh by
     * this process each iteration via the collective PDCobj_create_mpi,
     * same as bench_magnitude.c's eager path. */
    cont = PDCcont_open(cont_name, pdc);

    obj_prop_out = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_type(obj_prop_out, PDC_DOUBLE);
    PDCprop_set_obj_dims(obj_prop_out, 1, dims);
    PDCprop_set_obj_user_id(obj_prop_out, getuid());
    PDCprop_set_obj_app_name(obj_prop_out, "BenchMagnitude");
    PDCprop_set_obj_tags(obj_prop_out, "tag0=1");
    PDCprop_set_obj_transfer_region_type(obj_prop_out, PDC_REGION_STATIC);

    pdcid_t reg        = PDCregion_create(1, local_offset, region_len);
    pdcid_t reg_global = PDCregion_create(1, global_offset, region_len);

    /* Unlike bench_magnitude.c's eager path (which attaches this same
     * graph to objects it just created, in the same session that writes
     * them), this graph is attached fresh here, in a brand new process, to
     * vx/vy/vz objects a completely different, already-exited process
     * wrote. See the file header comment for why that's still expected to
     * work: PDCan_attach_to_region is client-local-only, so nothing about
     * this attach reaches the server until the timed magnitude read below
     * -- deliberately this benchmark's only I/O call per timestep. */
    pdcid_t dg_id = PDCan_dg_json_create(AN_GRAPHS_DIR "vector_magnitude.json");
    if (dg_id == 0) {
        fprintf(stderr, "PDCan_dg_json_create failed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();

    double local_setup = t_setup1 - t_setup0;
    double max_setup;
    MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    /* Like bench_posthoc_analyze.c, vx/vy/vz/magnitude use per-timestep-
     * unique names ("vx_0", "vx_1", ...). */
    int global_bad = 0;
    for (step = 0; step < N_TIMESTEPS; ++step) {
        char vx_name[32], vy_name[32], vz_name[32], mag_name[32];
        snprintf(vx_name, sizeof(vx_name), "vx_%d", step);
        snprintf(vy_name, sizeof(vy_name), "vy_%d", step);
        snprintf(vz_name, sizeof(vz_name), "vz_%d", step);
        snprintf(mag_name, sizeof(mag_name), "magnitude_%d", step);

        vx_obj = PDCobj_open(vx_name, pdc);
        vy_obj = PDCobj_open(vy_name, pdc);
        vz_obj = PDCobj_open(vz_name, pdc);
        if (vx_obj == 0 || vy_obj == 0 || vz_obj == 0) {
            fprintf(stderr, "Failed to open one or more step-%d input objects\n", step);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        mag_obj = PDCobj_create_mpi(cont, mag_name, obj_prop_out, 0, MPI_COMM_WORLD);
        if (mag_obj == 0) {
            fprintf(stderr, "Failed to create step-%d magnitude object\n", step);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        PDCan_attach_to_region(dg_id, "vx", vx_obj, reg_global);
        PDCan_attach_to_region(dg_id, "vy", vy_obj, reg_global);
        PDCan_attach_to_region(dg_id, "vz", vz_obj, reg_global);
        PDCan_attach_to_region(dg_id, "magnitude", mag_obj, reg_global);

        /* The only I/O this process does per timestep: reading magnitude
         * is what triggers the server-side eager computation (see file
         * header comment). */
        t_read0 = MPI_Wtime();
        do_transfer(mag, PDC_READ, mag_obj, reg, reg_global, "read magnitude");
        MPI_Barrier(MPI_COMM_WORLD);
        t_read1 = MPI_Wtime();

        /* Correctness check (not timed): vx/vy/vz were generated with the
         * same deterministic pattern by bench_write_components, so it's
         * regenerated locally here rather than read back a second time. */
        int local_bad = 0;
        for (i = 0; i < (size_t)n_elem; ++i) {
            float  ex       = (float)((i % 1000) + 1);
            float  ey       = (float)(((i + 137) % 1000) + 1);
            float  ez       = (float)(((i + 613) % 1000) + 1);
            double expected = sqrt((double)ex * ex + (double)ey * ey + (double)ez * ez);
            if (fabs(mag[i] - expected) > EPSILON)
                local_bad++;
        }

        double local_read = t_read1 - t_read0;
        double max_read;
        int    step_bad = 0;
        MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        global_bad += step_bad;

        if (rank == 0) {
            /* compute_s/writeback_s always 0 -- see file header comment. */
            double step_total = max_read;
            printf("posthoc_analyze,%d,%d,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks, n_elem, max_setup,
                   max_read, 0.0, 0.0, step_total, step_bad);
            fflush(stdout);
        }

        PDCobj_close(vx_obj);
        PDCobj_close(vy_obj);
        PDCobj_close(vz_obj);
        PDCobj_close(mag_obj);
    }

    PDCan_close_dg(dg_id);
    PDCregion_close(reg);
    PDCregion_close(reg_global);
    PDCcont_close(cont);
    PDCprop_close(obj_prop_out);
    PDCclose(pdc);

    free(mag);

    MPI_Finalize();
    return global_bad != 0;
}
