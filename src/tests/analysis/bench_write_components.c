/**
 * Posthoc benchmark, phase 1 of 2: write the three vector-component
 * objects (vx, vy, vz) as plain PDC objects (no graph attached) and exit.
 *
 * A real posthoc workflow writes data in one job, then some time later a
 * separate job comes back to read it, compute a derived product, and
 * persist that too -- unlike eager/lazy, which stay in one continuous
 * client session. This binary is only the first half: run it, let every
 * client rank fully exit, restart the PDC server with the `restart`
 * argument so it reloads the checkpointed metadata, then run
 * bench_posthoc_analyze against the same container/objects.
 *
 * Like bench_magnitude.c, writes N_TIMESTEPS distinct sets of vx/vy/vz
 * using per-timestep-unique names ("vx_0", "vx_1", ...) so the posthoc
 * comparison covers the same multi-timestep workload as eager/lazy.
 *
 * Usage: bench_write_components <n_elem_per_rank>
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,write_s
 */

#define N_TIMESTEPS 3

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <mpi.h>

#include "pdc.h"

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

    double t_setup0, t_setup1, t_write0, t_write1;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <n_elem_per_rank>\n", argv[0]);
        return 1;
    }
    n_elem = atol(argv[1]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    float *vx = (float *)malloc(sizeof(float) * n_elem);
    float *vy = (float *)malloc(sizeof(float) * n_elem);
    float *vz = (float *)malloc(sizeof(float) * n_elem);

    /* Same deterministic pattern bench_posthoc_analyze uses to
     * independently regenerate expected values for its correctness
     * check. */
    for (i = 0; i < (size_t)n_elem; ++i) {
        vx[i] = (float)((i % 1000) + 1);
        vy[i] = (float)(((i + 137) % 1000) + 1);
        vz[i] = (float)(((i + 613) % 1000) + 1);
    }

    /* See bench_magnitude.c's comment on REGION_STATIC vs OBJ_STATIC for
     * why a shared object with region-offset-based routing is used here
     * instead of one object per rank. */
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
    pdcid_t     obj_prop_in = 0;
    pdcid_t     vx_obj = 0, vy_obj = 0, vz_obj = 0;

    /* Every rank calls the collective _col/_mpi variants (see
     * src/tests/misc/vpicio.c for the same pattern, and bench_magnitude.c
     * for why plain create+barrier+open is unsafe): the designated rank
     * (0) performs the real server-side create, and the call internally
     * MPI_Bcasts the resulting metadata to every other rank, which builds
     * a local-only handle from that broadcast data -- no RPC-based query
     * from non-creating ranks at all. */
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    cont      = PDCcont_create_col(cont_name, cont_prop);

    obj_prop_in = PDCprop_create(PDC_OBJ_CREATE, pdc);
    PDCprop_set_obj_type(obj_prop_in, PDC_FLOAT);
    PDCprop_set_obj_dims(obj_prop_in, 1, dims);
    PDCprop_set_obj_user_id(obj_prop_in, getuid());
    PDCprop_set_obj_app_name(obj_prop_in, "BenchMagnitude");
    PDCprop_set_obj_tags(obj_prop_in, "tag0=1");
    PDCprop_set_obj_transfer_region_type(obj_prop_in, PDC_REGION_STATIC);

    pdcid_t reg        = PDCregion_create(1, local_offset, region_len);
    pdcid_t reg_global = PDCregion_create(1, global_offset, region_len);

    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();

    double local_setup = t_setup1 - t_setup0;
    double max_setup;
    MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    /* Like bench_magnitude.c, write N_TIMESTEPS distinct sets of vx/vy/vz
     * using per-timestep-unique names ("vx_0", "vx_1", ...) rather than a
     * shared name with an incrementing time_step property -- PDCobj_open()
     * resolves purely by name and hardcodes time_step=0 in its server
     * query (see PDCobj_open_common), so a shared name would always
     * reopen timestep 0's object on the non-creating ranks. These names
     * must match what bench_posthoc_analyze.c opens in the later,
     * separately launched analyze phase.
     *
     * Unlike bench_magnitude.c, obj_prop_in's time_step property is left
     * at its default (0) for every step rather than set to match `step`:
     * bench_magnitude.c never reopens its objects by name (every handle it
     * needs stays in-process, from the same PDCobj_create_mpi call), so a
     * nonzero time_step there is inert. Here, though, vx_N/vy_N/vz_N are
     * reopened by name in a *separate* process (bench_posthoc_analyze.c,
     * after a full server restart) via plain PDCobj_open, which queries
     * time_step=0 unconditionally -- and PDC_metadata_cmp (see
     * pdc_client_server_common.c) compares time_step first and treats any
     * mismatch as a non-match regardless of whether the name also matches.
     * Setting time_step=step here would make vx_1/vx_2's stored metadata
     * permanently unreachable by that later PDCobj_open, even though the
     * name alone already disambiguates the timestep. */
    for (step = 0; step < N_TIMESTEPS; ++step) {
        char vx_name[32], vy_name[32], vz_name[32];
        snprintf(vx_name, sizeof(vx_name), "vx_%d", step);
        snprintf(vy_name, sizeof(vy_name), "vy_%d", step);
        snprintf(vz_name, sizeof(vz_name), "vz_%d", step);

        vx_obj = PDCobj_create_mpi(cont, vx_name, obj_prop_in, 0, MPI_COMM_WORLD);
        vy_obj = PDCobj_create_mpi(cont, vy_name, obj_prop_in, 0, MPI_COMM_WORLD);
        vz_obj = PDCobj_create_mpi(cont, vz_name, obj_prop_in, 0, MPI_COMM_WORLD);
        if (vx_obj == 0 || vy_obj == 0 || vz_obj == 0) {
            fprintf(stderr, "Failed to create one or more step-%d objects\n", step);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        t_write0 = MPI_Wtime();
        do_transfer(vx, PDC_WRITE, vx_obj, reg, reg_global, "write vx");
        do_transfer(vy, PDC_WRITE, vy_obj, reg, reg_global, "write vy");
        do_transfer(vz, PDC_WRITE, vz_obj, reg, reg_global, "write vz");
        MPI_Barrier(MPI_COMM_WORLD);
        t_write1 = MPI_Wtime();

        double local_write = t_write1 - t_write0;
        double max_write;
        MPI_Reduce(&local_write, &max_write, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            printf("posthoc_write,%d,%d,%ld,%.6f,%.6f\n", step, nranks, n_elem, max_setup, max_write);
            fflush(stdout);
        }

        PDCobj_close(vx_obj);
        PDCobj_close(vy_obj);
        PDCobj_close(vz_obj);
    }

    PDCregion_close(reg);
    PDCregion_close(reg_global);
    PDCcont_close(cont);
    PDCprop_close(obj_prop_in);
    PDCprop_close(cont_prop);
    PDCclose(pdc);

    free(vx);
    free(vy);
    free(vz);

    MPI_Finalize();
    return 0;
}
