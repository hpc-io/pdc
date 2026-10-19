/**
 * Scale benchmark comparing two strategies for producing a "magnitude"
 * object from three vector-component objects (vx, vy, vz) -- both fully
 * server-side via the region-analysis framework (DataFlyway); neither
 * mode ever computes magnitude client-side, since DataFlyway's whole
 * point is that in-flight processing happens on the server, not in the
 * client process. Both modes attach their graph to vx/vy/vz/magnitude
 * identically, before writing -- the *graph itself* now declares
 * whether its output is eager or lazy (the "trigger" field on the
 * "magnitude" state -- see pdc_an_trigger_t in
 * src/server/analysis/include/pdc_an_user.h), so client attach order no
 * longer has to encode that decision:
 *
 *   eager   - an_client/graphs/vector_magnitude.json, whose "magnitude"
 *             state declares "trigger": "eager" (the default). The last
 *             component write transparently triggers server-side eager
 *             computation of magnitude in the write path
 *             (PDCan_notify_input_written's write-triggered hook). A
 *             confirmation read of magnitude follows.
 *
 *   lazy    - an_client/graphs/vector_magnitude_lazy.json, identical
 *             graph shape but "magnitude" declares "trigger": "lazy" --
 *             PDCan_notify_input_written deliberately skips eagerly
 *             computing it even once vx/vy/vz are all written. The read
 *             of magnitude that follows is what triggers server-side
 *             computation: PDC_Server_data_io_region_analysis (see
 *             src/server/analysis/pdc_an_server.c) transparently
 *             materializes an unmaterialized bound output the first time
 *             it's read. This is genuine lazy evaluation -- computed
 *             once, on first need, and persisted -- not "recomputed from
 *             scratch on every access": the framework has no notion of a
 *             non-materializing view, and reintroducing one client-side
 *             would just be eager's write-triggered case with the
 *             trigger moved to a hand-rolled client read, still
 *             in-flight processing on the client either way.
 *
 * The third strategy, posthoc (write, then in a separate later job read
 * back, compute, and write the result), needs a real client-close +
 * server-restart + client-relaunch between the write and the
 * read/compute/write to be representative -- that can't happen inside one
 * continuous binary, so it lives in the two-binary pair
 * bench_write_components / bench_posthoc_analyze instead. See
 * pdc_helper_scripts/analysis_scripts/posthoc_analysis.sbatch.
 *
 * Like VPIC-IO and DLIO elsewhere in this evaluation, the workload runs
 * N_TIMESTEPS repetitions of write(+compute) within one continuous
 * client/server session, each timestep producing a distinct, uniquely
 * named set of vx/vy/vz/magnitude objects (see the loop below for why
 * per-timestep names are required rather than a shared name).
 *
 * Usage: bench_magnitude <eager|lazy> <n_elem_per_rank>
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,step_total_s,bad
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
    int    is_eager, is_lazy;
    int    step;
    size_t i;

    double t_setup0, t_setup1, t_write0, t_write1;
    double t_readback0 = 0, t_readback1 = 0, t_compute0 = 0, t_compute1 = 0;
    double t_writeback0 = 0, t_writeback1 = 0, t_read0 = 0, t_read1 = 0;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <eager|lazy> <n_elem_per_rank>\n", argv[0]);
        return 1;
    }
    is_eager = (strcmp(argv[1], "eager") == 0);
    is_lazy  = (strcmp(argv[1], "lazy") == 0);
    if (!is_eager && !is_lazy) {
        fprintf(stderr, "Usage: %s <eager|lazy> <n_elem_per_rank>\n", argv[0]);
        return 1;
    }
    n_elem = atol(argv[2]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    float * vx  = (float *)malloc(sizeof(float) * n_elem);
    float * vy  = (float *)malloc(sizeof(float) * n_elem);
    float * vz  = (float *)malloc(sizeof(float) * n_elem);
    double *mag = (double *)malloc(sizeof(double) * n_elem);

    for (i = 0; i < (size_t)n_elem; ++i) {
        vx[i] = (float)((i % 1000) + 1);
        vy[i] = (float)(((i + 137) % 1000) + 1);
        vz[i] = (float)(((i + 613) % 1000) + 1);
    }
    memset(mag, 0, sizeof(double) * n_elem);

    /* One shared object per variable (vx, vy, vz, magnitude), spanning the
     * whole nranks*n_elem problem domain -- every rank takes a distinct,
     * disjoint subset [i*n_elem, (i+1)*n_elem) to run analysis on. Use
     * PDC_REGION_STATIC (region-offset-based server routing, splitting the
     * object across all data servers) rather than PDC_OBJ_STATIC
     * (creator-rank-based routing): with a shared object, only rank 0
     * creates it, so OBJ_STATIC would pin every rank's I/O onto rank 0's
     * one server regardless of how many servers exist. REGION_STATIC lets
     * each rank's own region route to its own server when server count
     * matches client count. This works safely with PDCan_attach_to_region's
     * piggyback design (see PDC_Client_transfer_request): the attach info
     * rides along on the very same region-routed RPC, so it always lands
     * wherever that RPC's region actually goes. */
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

    const char *cont_name = "bench_shared";
    pdcid_t     cont_prop = 0, cont = 0;
    pdcid_t     obj_prop_in = 0, obj_prop_out = 0;
    pdcid_t     vx_obj = 0, vy_obj = 0, vz_obj = 0, mag_obj = 0;

    /* Every rank calls the collective _col/_mpi variants (see
     * src/tests/misc/vpicio.c for the same pattern): the designated rank
     * (0) performs the real server-side create, and the call internally
     * MPI_Bcasts the resulting metadata (object id, server id, etc.) to
     * every other rank, which builds a local-only handle from that
     * broadcast data -- no RPC-based query from non-creating ranks at
     * all. This avoids a real race with plain PDCobj_create +
     * MPI_Barrier + PDCobj_open: PDCobj_create returning on the creating
     * rank does not guarantee the object is yet visible to another rank's
     * PDCobj_open, which queries the server directly and can observe
     * "not found" if the server is still processing the create (confirmed
     * on Perlmutter: a non-creating rank's open failed this way while the
     * server was still flushing the previous timestep). Property values
     * must still be set identically on every rank, since PDCobj_create_mpi
     * still reads dims/type/etc. off the local property handle it's given
     * even on the non-creating ranks. */
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    cont      = PDCcont_create_col(cont_name, cont_prop);

    obj_prop_in = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_type(obj_prop_in, PDC_FLOAT);
    PDCprop_set_obj_dims(obj_prop_in, 1, dims);
    PDCprop_set_obj_user_id(obj_prop_in, getuid());
    PDCprop_set_obj_app_name(obj_prop_in, "BenchMagnitude");
    PDCprop_set_obj_tags(obj_prop_in, "tag0=1");
    PDCprop_set_obj_transfer_region_type(obj_prop_in, PDC_REGION_STATIC);

    obj_prop_out = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_type(obj_prop_out, PDC_DOUBLE);
    PDCprop_set_obj_dims(obj_prop_out, 1, dims);
    PDCprop_set_obj_user_id(obj_prop_out, getuid());
    PDCprop_set_obj_app_name(obj_prop_out, "BenchMagnitude");
    PDCprop_set_obj_tags(obj_prop_out, "tag0=1");
    PDCprop_set_obj_transfer_region_type(obj_prop_out, PDC_REGION_STATIC);

    pdcid_t reg        = PDCregion_create(1, local_offset, region_len);
    pdcid_t reg_global = PDCregion_create(1, global_offset, region_len);

    /* Only the graph file differs between modes now -- vector_magnitude.json's
     * "magnitude" state declares "trigger": "eager", vector_magnitude_lazy.json's
     * declares "lazy" (see file header comment); both modes attach and
     * write identically below. */
    char graph_path[256];
    snprintf(graph_path, sizeof(graph_path), "%s%s", AN_GRAPHS_DIR,
             is_eager ? "vector_magnitude.json" : "vector_magnitude_lazy.json");
    pdcid_t dg_id = PDCan_dg_json_create(graph_path);
    if (dg_id == 0) {
        fprintf(stderr, "PDCan_dg_json_create failed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();

    double local_setup = t_setup1 - t_setup0;
    double max_setup;
    MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    const char *mode_name = is_eager ? "eager" : "lazy";

    /* Like VPIC-IO/DLIO, repeat the write(+compute) cycle across
     * N_TIMESTEPS timesteps within this one session, each producing a
     * distinct set of vx/vy/vz/magnitude objects, and print one CSV row
     * per timestep so per-step timing is visible rather than only an
     * aggregate.
     *
     * Objects use per-timestep-unique names ("vx_0", "vx_1", ...) rather
     * than a shared name with an incrementing time_step property: on the
     * non-creating ranks, PDCobj_open() resolves purely by name and
     * internally queries the server with time_step hardcoded to 0 (see
     * PDCobj_open_common), and PDC_metadata_cmp (pdc_client_server_common.c)
     * compares time_step first, treating any mismatch as a non-match
     * regardless of whether the name also matches -- so the time_step
     * property is never set here at all (left at its default, 0, for every
     * step): setting it to `step` would make steps 1+ permanently
     * unreachable by name to any later plain PDCobj_open, exactly as it did
     * in bench_write_components.c/bench_posthoc_analyze.c before that was
     * fixed. The object name is the sole thing that disambiguates
     * timesteps here. The PDCan_attach_to_region role labels ("vx", "vy",
     * "vz", "magnitude") are independent of the object's PDC name -- they
     * identify which state in the JSON graph this object fulfills -- so
     * they stay fixed across timesteps. */
    char name_buf[4][32];
    int  global_bad = 0;
    for (step = 0; step < N_TIMESTEPS; ++step) {
        snprintf(name_buf[0], sizeof(name_buf[0]), "vx_%d", step);
        snprintf(name_buf[1], sizeof(name_buf[1]), "vy_%d", step);
        snprintf(name_buf[2], sizeof(name_buf[2]), "vz_%d", step);
        snprintf(name_buf[3], sizeof(name_buf[3]), "magnitude_%d", step);

        vx_obj  = PDCobj_create_mpi(cont, name_buf[0], obj_prop_in, 0, MPI_COMM_WORLD);
        vy_obj  = PDCobj_create_mpi(cont, name_buf[1], obj_prop_in, 0, MPI_COMM_WORLD);
        vz_obj  = PDCobj_create_mpi(cont, name_buf[2], obj_prop_in, 0, MPI_COMM_WORLD);
        mag_obj = PDCobj_create_mpi(cont, name_buf[3], obj_prop_out, 0, MPI_COMM_WORLD);
        if (vx_obj == 0 || vy_obj == 0 || vz_obj == 0 || mag_obj == 0) {
            fprintf(stderr, "Failed to create one or more step-%d objects\n", step);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* Same attach call for both modes now -- whether this write ends
         * up eagerly triggering computation or not is entirely up to the
         * loaded graph's own "trigger" declaration on "magnitude" (see
         * file header comment), not client attach order. */
        PDCan_attach_to_region(dg_id, "vx", vx_obj, reg_global);
        PDCan_attach_to_region(dg_id, "vy", vy_obj, reg_global);
        PDCan_attach_to_region(dg_id, "vz", vz_obj, reg_global);
        PDCan_attach_to_region(dg_id, "magnitude", mag_obj, reg_global);

        /* Write vector components -- for eager mode, the final component
         * write transparently triggers server-side computation+persistence
         * of magnitude before this phase's wait/close return
         * (PDCan_notify_input_written). For lazy, that same hook sees
         * "magnitude" declared lazy and deliberately skips it -- this
         * write completes without any computation happening yet. */
        t_write0 = MPI_Wtime();
        do_transfer(vx, PDC_WRITE, vx_obj, reg, reg_global, "write vx");
        do_transfer(vy, PDC_WRITE, vy_obj, reg, reg_global, "write vy");
        do_transfer(vz, PDC_WRITE, vz_obj, reg, reg_global, "write vz");
        MPI_Barrier(MPI_COMM_WORLD);
        t_write1 = MPI_Wtime();

        /* Read magnitude -- for eager this just confirms the write-time
         * computation already happened; for lazy, this read is what
         * transparently triggers the server to compute and persist
         * magnitude for the first time (PDC_Server_data_io_region_analysis's
         * read-triggered hook -- see file header comment). Either way,
         * the result comes back from the server, never computed here. */
        t_read0 = MPI_Wtime();
        /* Re-zero every timestep, not just once before the loop -- vx/vy/vz
         * (and therefore the expected magnitude) are identical every
         * timestep, so without this a silently no-op read on step 1/2
         * would leave the previous timestep's correct-looking values in
         * place and the correctness check below would pass without this
         * step's read having actually happened. Matches
         * adios2_bench_magnitude.cpp's std::fill before every Get(). */
        memset(mag, 0, sizeof(double) * n_elem);
        do_transfer(mag, PDC_READ, mag_obj, reg, reg_global, "read magnitude");
        MPI_Barrier(MPI_COMM_WORLD);
        t_read1 = MPI_Wtime();

        /* Correctness check (not timed). */
        int local_bad = 0;
        for (i = 0; i < (size_t)n_elem; ++i) {
            double x = (double)vx[i], y = (double)vy[i], z = (double)vz[i];
            double expected = sqrt(x * x + y * y + z * z);
            if (fabs(mag[i] - expected) > EPSILON)
                local_bad++;
        }

        double local_write     = t_write1 - t_write0;
        double local_readback  = t_readback1 - t_readback0;
        double local_compute   = t_compute1 - t_compute0;
        double local_writeback = 0;
        double local_read      = t_read1 - t_read0;

        double max_write, max_readback, max_compute, max_writeback, max_read;
        int    step_bad = 0;
        MPI_Reduce(&local_write, &max_write, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_readback, &max_readback, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_compute, &max_compute, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_writeback, &max_writeback, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        global_bad += step_bad;

        if (rank == 0) {
            /* Same formula for both modes now: readback_s/compute_s/
             * writeback_s are always 0 (neither mode ever reads
             * components back or computes client-side -- see file header
             * comment), so the total is just write + the read that
             * confirms (eager) or triggers (lazy) server-side
             * materialization. */
            double step_total = max_write + max_read;
            printf("%s,%d,%d,%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d\n", mode_name, step, nranks, n_elem,
                   max_setup, max_write, max_readback, max_compute, max_writeback, max_read, step_total,
                   step_bad);
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
    PDCprop_close(obj_prop_in);
    PDCprop_close(obj_prop_out);
    PDCprop_close(cont_prop);
    PDCclose(pdc);

    free(vx);
    free(vy);
    free(vz);
    free(mag);

    MPI_Finalize();
    return global_bad != 0;
}
