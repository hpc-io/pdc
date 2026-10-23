/**
 * Standalone read-only verification client for vpicio.c -- deliberately a
 * SEPARATE process from the writer, run as its own srun step after
 * vpicio has fully exited, so read/verification time is never folded
 * into the write-workload CSV (see vpicio.c's own header comment and
 * vpicio_scripts/common.sh). Opens the dX/dY/dZ/Ux/Uy/Uz/q/i objects
 * vpicio already wrote against a still-running server (same session, no
 * restart needed), reads each back, and checks it against the same
 * deterministic formula vpicio.c uses to generate them.
 *
 * vpicio.c had no correctness check at all before this was added --
 * this is the first one, and it lives entirely in this separate binary
 * rather than inside vpicio.c itself, exactly so its cost is never
 * counted as part of vpicio's own write-throughput measurement.
 *
 * Usage: vpicio_verify <numparticles> <steps>
 *
 * Prints one CSV line per (step, object) pair from rank 0:
 *   mode,step,n_ranks,object,open_s,read_s,bad
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <math.h>
#include "pdc.h"

static void
do_read(void *buf, pdcid_t obj, pdcid_t reg, pdcid_t reg_global, const char *what)
{
    pdcid_t tr = PDCregion_transfer_create(buf, PDC_READ, obj, reg, reg_global);
    if (tr == 0) {
        fprintf(stderr, "create failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (PDCregion_transfer_start(tr) < 0) {
        fprintf(stderr, "start failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (PDCregion_transfer_wait(tr) < 0) {
        fprintf(stderr, "wait failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (PDCregion_transfer_close(tr) < 0) {
        fprintf(stderr, "close failed for %s\n", what);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int
main(int argc, char **argv)
{
    int      rank, nranks;
    uint64_t numparticles;
    int      steps;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <numparticles> <steps>\n", argv[0]);
        return 1;
    }
    numparticles = (uint64_t)atoll(argv[1]);
    steps        = atoi(argv[2]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    const char *obj_names[] = {"dX", "dY", "dZ", "Ux", "Uy", "Uz", "q", "i"};

    float *fbuf = (float *)malloc(sizeof(float) * numparticles);
    int *  ibuf = (int *)malloc(sizeof(int) * numparticles);

    uint64_t local_offset[1] = {0}, global_offset[1], region_len[1] = {numparticles};
    global_offset[0] = (uint64_t)rank * numparticles;

    pdcid_t pdc = PDCinit("pdc");
    if (pdc == 0) {
        fprintf(stderr, "PDCinit failed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    pdcid_t cont = PDCcont_open("c1", pdc);
    if (cont == 0) {
        fprintf(stderr, "PDCcont_open failed\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    pdcid_t reg        = PDCregion_create(1, local_offset, region_len);
    pdcid_t reg_global = PDCregion_create(1, global_offset, region_len);

    int global_bad = 0;
    for (int iter = 0; iter < steps; iter++) {
        for (int i = 0; i < 8; i++) {
            char obj_name[64];
            snprintf(obj_name, sizeof(obj_name), "%s-%d", obj_names[i], iter);

            double  t_open0 = MPI_Wtime();
            pdcid_t obj     = PDCobj_open(obj_name, pdc);
            if (obj == 0) {
                fprintf(stderr, "open failed for %s\n", obj_name);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            double t_open1 = MPI_Wtime();

            int is_int = (i == 7); /* "i" (particle id) is the only PDC_INT object */

            double t_read0 = MPI_Wtime();
            if (is_int)
                do_read(ibuf, obj, reg, reg_global, obj_name);
            else
                do_read(fbuf, obj, reg, reg_global, obj_name);
            MPI_Barrier(MPI_COMM_WORLD);
            double t_read1 = MPI_Wtime();

            double local_open = t_open1 - t_open0, local_read = t_read1 - t_read0;
            double max_open, max_read;
            MPI_Reduce(&local_open, &max_open, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            /* Same deterministic formula as vpicio.c's data generation,
             * including the same per-step boundary overrides on q/i at
             * index 0 and numparticles-1 (see vpicio.c's step loop). */
            int local_bad = 0;
            for (uint64_t j = 0; j < numparticles; j++) {
                uint64_t global_j = (uint64_t)rank * numparticles + j;
                double   expected;
                switch (i) {
                    case 0:
                        expected = (double)((global_j % 1000) + 1);
                        break; /* dX */
                    case 1:
                        expected = (double)(((global_j + 137) % 1000) + 1);
                        break; /* dY */
                    case 2:
                        expected = (double)(((global_j + 271) % 1000) + 1);
                        break; /* dZ */
                    case 3:
                        expected = (double)(((global_j + 613) % 1000) + 1);
                        break; /* Ux */
                    case 4:
                        expected = (double)(((global_j + 911) % 1000) + 1);
                        break; /* Uy */
                    case 5:
                        expected = (double)(((global_j + 1301) % 1000) + 1);
                        break; /* Uz */
                    case 6:    /* q */
                        if (j == 0)
                            expected = (double)(rank + iter * 2);
                        else if (j == numparticles - 1)
                            expected = (double)(rank - iter * 2);
                        else
                            expected = (double)((global_j % 1000) + 1);
                        break;
                    default: /* i (particle id) */
                        if (j == 0)
                            expected = (double)(rank + iter);
                        else if (j == numparticles - 1)
                            expected = (double)(rank - iter);
                        else
                            expected = (double)global_j;
                        break;
                }
                double actual = is_int ? (double)ibuf[j] : (double)fbuf[j];
                if (fabs(actual - expected) > 1e-3)
                    local_bad++;
            }
            int step_bad = 0;
            MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            global_bad += step_bad;

            if (rank == 0) {
                printf("vpicio_verify,%d,%d,%s,%.6f,%.6f,%d\n", iter, nranks, obj_name, max_open, max_read,
                       step_bad);
                fflush(stdout);
            }
            PDCobj_close(obj);
        }
    }

    PDCregion_close(reg);
    PDCregion_close(reg_global);
    PDCcont_close(cont);
    PDCclose(pdc);
    free(fbuf);
    free(ibuf);

    MPI_Finalize();
    return global_bad != 0;
}
