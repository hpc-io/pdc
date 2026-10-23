/**
 * Combined vpicio (write) + bdcats (read) benchmark, in one process: writes
 * `steps` timesteps of the standard 8-array VPIC-shaped particle dataset
 * (dX,dY,dZ,Ux,Uy,Uz,q,i), closing each timestep's objects as it goes (as
 * the original vpicio.c does), then -- still in the SAME process, so the
 * server's region cache from the writes is still warm rather than having
 * been dropped by a server restart between two separate job steps --
 * reopens every one of those same per-timestep objects by name (as the
 * original bdcats.c does) and reads them back, verifying the values match
 * what was written.
 *
 * Every distinct PDC client API call is timed with PDC_TIMED
 * (pdc_call_stats.h) and pooled into a mean/stdev/count reported per call
 * name, across every rank, at the end. One-time setup/teardown calls
 * (PDCinit, container/property create/close, ...) go in one pool; the
 * write-phase loop and read-phase loop each get their OWN pool, even for
 * call names both loops share (PDCregion_transfer_create/
 * start_all_mpi/wait_all/close, PDCobj_close) -- a single shared pool
 * would otherwise merge write-phase and read-phase timings for those
 * names into one indistinguishable mean.
 *
 * Two transfer modes, selected by argv[3]:
 *   sync  -- PDCregion_transfer_start_all_mpi immediately followed by
 *            PDCregion_transfer_wait_all, no overlap.
 *   async -- sleep(sleeptime) between start and wait, standing in for
 *            compute overlapped with in-flight I/O (mirrors vpicio.c's
 *            original behavior; bdcats.c's own copy of this had hardcoded
 *            sleep(40) instead of honoring its own sleeptime argument --
 *            fixed here).
 *
 * Usage: vpic_bdcats <numparticles_per_rank> <steps> <sync|async> [sleeptime_s]
 *
 * Prints a CSV to stdout from rank 0 on completion:
 *   record_type,name,step,mean_s,stdev_s,count,value
 * with record_type one of:
 *   api_call               -- one row per distinct one-time setup/teardown
 *                             call name (PDCinit, container/property
 *                             create/close, ...), not phase-specific
 *   api_call_write          -- one row per distinct call name made inside
 *                             the write-phase loop only
 *   api_call_read           -- same, for the read-phase loop only
 *   throughput_write_MBps  -- one row per write-phase timestep. Data size
 *                             over the observed I/O time: the whole
 *                             step's wall-clock time (object create/close,
 *                             transfer create/close, transfer start/wait)
 *                             minus sleep(sleeptime) in async mode, since
 *                             that sleep stands in for compute the client
 *                             does while the transfer is in flight, not
 *                             time spent waiting on I/O -- so async
 *                             throughput is expected to come out HIGHER
 *                             than sync's, not lower.
 *   throughput_read_MBps   -- one row per read-phase timestep, same basis
 *   total_data_size_bytes  -- one row, whole run, one direction, all ranks
 *   data_size_per_rank_bytes -- one row, whole run, one direction, one rank
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <inttypes.h>
#include <mpi.h>

#include "pdc.h"
#include "pdc_timing.h"
#include "pdc_call_stats.h"

#define N_OBJS         8
#define DEFAULT_SLEEP  2
#define NPARTICLES_DEF 8388608

static const char *obj_names[N_OBJS] = {"dX", "dY", "dZ", "Ux", "Uy", "Uz", "q", "i"};

typedef enum { XFER_SYNC, XFER_ASYNC } xfer_mode_t;

static void
print_usage(void)
{
    LOG_JUST_PRINT("Usage: vpic_bdcats <numparticles_per_rank> <steps> <sync|async> [sleeptime_s]\n");
}

int
main(int argc, char **argv)
{
    int rank = 0, nranks = 1;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (argc < 4) {
        if (rank == 0)
            print_usage();
        MPI_Finalize();
        return 1;
    }

    uint64_t    numparticles = atoll(argv[1]);
    int         steps        = atoi(argv[2]);
    xfer_mode_t mode         = (strcmp(argv[3], "sync") == 0) ? XFER_SYNC : XFER_ASYNC;
    int         sleeptime    = (argc >= 5) ? atoi(argv[4]) : DEFAULT_SLEEP;

    if (numparticles == 0)
        numparticles = NPARTICLES_DEF;
    if (steps <= 0)
        steps = 1;

    if (rank == 0)
        LOG_WARNING("vpic_bdcats: %" PRIu64 " particles/rank, %d steps, mode=%s%s\n", numparticles, steps,
                    mode == XFER_SYNC ? "sync" : "async",
                    mode == XFER_SYNC ? "" : " (sleep between start/wait)");

    /* `stats` times one-time setup/teardown calls that happen once for
     * the whole run (PDCinit, container/property create/close, ...) --
     * neither write-phase nor read-phase specific. `write_stats` and
     * `read_stats` time everything inside each phase's loop separately,
     * even calls with the same name in both loops (PDCregion_transfer_
     * create/start_all_mpi/wait_all/close, PDCobj_close) -- a single
     * shared stats object would otherwise pool those two phases' timings
     * into one indistinguishable mean, which is exactly the gap that
     * motivated splitting this out. */
    pdc_call_stats_t stats, write_stats, read_stats;
    pdc_call_stats_init(&stats);
    pdc_call_stats_init(&write_stats);
    pdc_call_stats_init(&read_stats);

    /* Reference data: identical content written under every timestep's
     * distinct object names, so read-back verification just compares
     * against this one buffer regardless of which step is being checked. */
    float *ref_dx = (float *)malloc(numparticles * sizeof(float));
    float *ref_dy = (float *)malloc(numparticles * sizeof(float));
    float *ref_dz = (float *)malloc(numparticles * sizeof(float));
    float *ref_ux = (float *)malloc(numparticles * sizeof(float));
    float *ref_uy = (float *)malloc(numparticles * sizeof(float));
    float *ref_uz = (float *)malloc(numparticles * sizeof(float));
    float *ref_q  = (float *)malloc(numparticles * sizeof(float));
    int *  ref_id = (int *)malloc(numparticles * sizeof(int));

    srand(42 + rank);
    for (uint64_t i = 0; i < numparticles; i++) {
        ref_id[i] = (int)i;
        ref_q[i]  = (float)(i * 2);
        ref_dx[i] = (float)rand() / (float)RAND_MAX * 64.0f;
        ref_dy[i] = (float)rand() / (float)RAND_MAX * 64.0f;
        ref_dz[i] = ((float)ref_id[i] / (float)numparticles) * 64.0f;
        ref_ux[i] = (float)rand() / (float)RAND_MAX * 64.0f;
        ref_uy[i] = (float)rand() / (float)RAND_MAX * 64.0f;
        ref_uz[i] = (ref_q[i] / (float)numparticles) * 64.0f;
    }
    void *write_ptrs[N_OBJS] = {ref_dx, ref_dy, ref_dz, ref_ux, ref_uy, ref_uz, ref_q, ref_id};

    float *read_dx           = (float *)malloc(numparticles * sizeof(float));
    float *read_dy           = (float *)malloc(numparticles * sizeof(float));
    float *read_dz           = (float *)malloc(numparticles * sizeof(float));
    float *read_ux           = (float *)malloc(numparticles * sizeof(float));
    float *read_uy           = (float *)malloc(numparticles * sizeof(float));
    float *read_uz           = (float *)malloc(numparticles * sizeof(float));
    float *read_q            = (float *)malloc(numparticles * sizeof(float));
    int *  read_id           = (int *)malloc(numparticles * sizeof(int));
    void * read_ptrs[N_OBJS] = {read_dx, read_dy, read_dz, read_ux, read_uy, read_uz, read_q, read_id};

    const size_t bytes_per_particle    = 7 * sizeof(float) + sizeof(int); /* == 32 */
    const double local_bytes_per_step  = (double)numparticles * (double)bytes_per_particle;
    const double global_bytes_per_step = local_bytes_per_step * (double)nranks;

    double *write_throughput_mbps = (double *)malloc(sizeof(double) * (size_t)steps);
    double *read_throughput_mbps  = (double *)malloc(sizeof(double) * (size_t)steps);

    pdcid_t  pdc_id, cont_prop, cont_id, region_local, region_remote;
    pdcid_t  obj_prop_float, obj_prop_int;
    pdcid_t  obj_ids[N_OBJS];
    pdcid_t  transfer_requests[N_OBJS];
    uint64_t dims[1], offset_local[1], offset_remote[1], mysize[1];
    char     obj_name[64];
    int      global_bad = 0;

    PDC_TIMED(&stats, "PDCinit", pdc_id = PDCinit("pdc"));
    if (pdc_id == 0) {
        LOG_ERROR("Failed to initialize PDC\n");
        return 1;
    }

    PDC_TIMED(&stats, "PDCprop_create_cont", cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id));
    PDC_TIMED(&stats, "PDCcont_create_col", cont_id = PDCcont_create_col("c1", cont_prop));
    if (cont_id <= 0) {
        LOG_ERROR("Failed to create container\n");
        return 1;
    }

    dims[0] = numparticles * (uint64_t)nranks;

    PDC_TIMED(&stats, "PDCprop_create_obj", obj_prop_float = PDCprop_create(PDC_OBJ_CREATE, pdc_id));
    PDCprop_set_obj_dims(obj_prop_float, 1, dims);
    PDCprop_set_obj_type(obj_prop_float, PDC_FLOAT);
    PDCprop_set_obj_user_id(obj_prop_float, getuid());
    PDCprop_set_obj_app_name(obj_prop_float, "VPIC_BDCATS");
    PDCprop_set_obj_transfer_region_type(obj_prop_float, PDC_REGION_STATIC);

    obj_prop_int = PDCprop_obj_dup(obj_prop_float);
    PDCprop_set_obj_type(obj_prop_int, PDC_INT);

    offset_local[0]  = 0;
    offset_remote[0] = (uint64_t)rank * numparticles;
    mysize[0]        = numparticles;
    PDC_TIMED(&stats, "PDCregion_create", region_local = PDCregion_create(1, offset_local, mysize));
    PDC_TIMED(&stats, "PDCregion_create", region_remote = PDCregion_create(1, offset_remote, mysize));

    /* ---- write phase (vpicio-style) ---- */
    for (int step = 0; step < steps; step++) {
        /* Throughput is data size over the OBSERVED I/O time for the
         * whole step, including metadata operations (object
         * create/close, transfer create/close) -- those are real,
         * unavoidable per-timestep cost, not incidental setup to be
         * excluded. sleep(sleeptime) below is excluded, though: it's
         * standing in for compute the client does WHILE the transfer is
         * in flight, not time spent waiting on I/O, so it's subtracted
         * back out of the elapsed time below -- this is also why async
         * throughput is expected to come out higher than sync's, not
         * lower, despite the extra wall-clock time async takes overall. */
        MPI_Barrier(MPI_COMM_WORLD);
        double step_t0 = MPI_Wtime();

        for (int i = 0; i < N_OBJS; i++) {
            sprintf(obj_name, "%s-%d", obj_names[i], step);
            pdcid_t prop = (i < 7) ? obj_prop_float : obj_prop_int;
            PDC_TIMED(&write_stats, "PDCobj_create_mpi",
                      obj_ids[i] = PDCobj_create_mpi(cont_id, obj_name, prop, 0, MPI_COMM_WORLD));
            if (obj_ids[i] == 0) {
                LOG_ERROR("Failed to create object %s\n", obj_name);
                return 1;
            }
        }

        for (int i = 0; i < N_OBJS; i++)
            PDC_TIMED(&write_stats, "PDCregion_transfer_create",
                      transfer_requests[i] = PDCregion_transfer_create(write_ptrs[i], PDC_WRITE, obj_ids[i],
                                                                       region_local, region_remote));

        PDC_TIMED(&write_stats, "PDCregion_transfer_start_all_mpi",
                  PDCregion_transfer_start_all_mpi(transfer_requests, N_OBJS, MPI_COMM_WORLD));

        if (mode == XFER_ASYNC)
            sleep((unsigned int)sleeptime);

        PDC_TIMED(&write_stats, "PDCregion_transfer_wait_all",
                  PDCregion_transfer_wait_all(transfer_requests, N_OBJS));

        for (int i = 0; i < N_OBJS; i++)
            PDC_TIMED(&write_stats, "PDCregion_transfer_close",
                      PDCregion_transfer_close(transfer_requests[i]));
        for (int i = 0; i < N_OBJS; i++)
            PDC_TIMED(&write_stats, "PDCobj_close", PDCobj_close(obj_ids[i]));

        MPI_Barrier(MPI_COMM_WORLD);
        double step_t1         = MPI_Wtime();
        double step_io_elapsed = step_t1 - step_t0;
        if (mode == XFER_ASYNC)
            step_io_elapsed -= (double)sleeptime;
        write_throughput_mbps[step] = global_bytes_per_step / step_io_elapsed / 1e6;

        if (rank == 0)
            LOG_WARNING("write step %d: %.2f MB/s\n", step, write_throughput_mbps[step]);
    }

    /* ---- read phase (bdcats-style), same process: server region cache
     * from the writes above is still warm. ---- */
    for (int step = 0; step < steps; step++) {
        /* See the write phase above: whole-step elapsed time including
         * metadata ops, minus sleep(sleeptime) (compute overlapped with
         * in-flight I/O, not time spent waiting on it) for async. */
        MPI_Barrier(MPI_COMM_WORLD);
        double step_t0 = MPI_Wtime();

        for (int i = 0; i < N_OBJS; i++) {
            sprintf(obj_name, "%s-%d", obj_names[i], step);
            PDC_TIMED(&read_stats, "PDCobj_open_col", obj_ids[i] = PDCobj_open_col(obj_name, pdc_id));
            if (obj_ids[i] == 0) {
                LOG_ERROR("Failed to open object %s\n", obj_name);
                return 1;
            }
        }

        for (int i = 0; i < N_OBJS; i++)
            PDC_TIMED(&read_stats, "PDCregion_transfer_create",
                      transfer_requests[i] = PDCregion_transfer_create(read_ptrs[i], PDC_READ, obj_ids[i],
                                                                       region_local, region_remote));

        PDC_TIMED(&read_stats, "PDCregion_transfer_start_all_mpi",
                  PDCregion_transfer_start_all_mpi(transfer_requests, N_OBJS, MPI_COMM_WORLD));

        if (mode == XFER_ASYNC)
            sleep((unsigned int)sleeptime);

        PDC_TIMED(&read_stats, "PDCregion_transfer_wait_all",
                  PDCregion_transfer_wait_all(transfer_requests, N_OBJS));

        for (int i = 0; i < N_OBJS; i++)
            PDC_TIMED(&read_stats, "PDCregion_transfer_close",
                      PDCregion_transfer_close(transfer_requests[i]));
        for (int i = 0; i < N_OBJS; i++)
            PDC_TIMED(&read_stats, "PDCobj_close", PDCobj_close(obj_ids[i]));

        MPI_Barrier(MPI_COMM_WORLD);
        double step_t1         = MPI_Wtime();
        double step_io_elapsed = step_t1 - step_t0;
        if (mode == XFER_ASYNC)
            step_io_elapsed -= (double)sleeptime;
        read_throughput_mbps[step] = global_bytes_per_step / step_io_elapsed / 1e6;

        int step_bad = 0;
        for (uint64_t i = 0; i < numparticles; i++) {
            if (read_dx[i] != ref_dx[i] || read_dy[i] != ref_dy[i] || read_dz[i] != ref_dz[i] ||
                read_ux[i] != ref_ux[i] || read_uy[i] != ref_uy[i] || read_uz[i] != ref_uz[i] ||
                read_q[i] != ref_q[i] || read_id[i] != ref_id[i]) {
                step_bad++;
            }
        }
        int global_step_bad = 0;
        MPI_Reduce(&step_bad, &global_step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
        global_bad += global_step_bad;

        if (rank == 0)
            LOG_WARNING("read step %d: %.2f MB/s, mismatches=%d\n", step, read_throughput_mbps[step],
                        global_step_bad);
    }

    PDC_TIMED(&stats, "PDCregion_close", PDCregion_close(region_local));
    PDC_TIMED(&stats, "PDCregion_close", PDCregion_close(region_remote));
    PDC_TIMED(&stats, "PDCcont_close", PDCcont_close(cont_id));
    PDC_TIMED(&stats, "PDCprop_close", PDCprop_close(cont_prop));
    PDC_TIMED(&stats, "PDCprop_close", PDCprop_close(obj_prop_float));
    PDC_TIMED(&stats, "PDCprop_close", PDCprop_close(obj_prop_int));
    PDC_TIMED(&stats, "PDCclose", PDCclose(pdc_id));

    if (rank == 0) {
        printf("record_type,name,step,mean_s,stdev_s,count,value\n");
    }
    pdc_call_stats_print_csv(&stats, stdout, rank, MPI_COMM_WORLD, "api_call");
    pdc_call_stats_print_csv(&write_stats, stdout, rank, MPI_COMM_WORLD, "api_call_write");
    pdc_call_stats_print_csv(&read_stats, stdout, rank, MPI_COMM_WORLD, "api_call_read");
    if (rank == 0) {
        for (int step = 0; step < steps; step++)
            printf("throughput_write_MBps,,%d,,,,%.6f\n", step, write_throughput_mbps[step]);
        for (int step = 0; step < steps; step++)
            printf("throughput_read_MBps,,%d,,,,%.6f\n", step, read_throughput_mbps[step]);
        printf("total_data_size_bytes,,,,,,%.0f\n", local_bytes_per_step * (double)nranks * (double)steps);
        printf("data_size_per_rank_bytes,,,,,,%.0f\n", local_bytes_per_step * (double)steps);
        if (global_bad != 0)
            LOG_ERROR("vpic_bdcats: %d total value mismatches across all steps/ranks\n", global_bad);
        else
            LOG_WARNING("vpic_bdcats: all read-back values matched\n");
        fflush(stdout);
    }

    free(ref_dx);
    free(ref_dy);
    free(ref_dz);
    free(ref_ux);
    free(ref_uy);
    free(ref_uz);
    free(ref_q);
    free(ref_id);
    free(read_dx);
    free(read_dy);
    free(read_dz);
    free(read_ux);
    free(read_uy);
    free(read_uz);
    free(read_q);
    free(read_id);
    free(write_throughput_mbps);
    free(read_throughput_mbps);

    MPI_Finalize();
    return global_bad != 0;
}
