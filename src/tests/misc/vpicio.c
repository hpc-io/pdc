/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <inttypes.h>
#include "pdc.h"
#include "pdc_timing.h"

#define dLEAP          2
#define PRECISION_INIT 2
#define PRECISION_HEX  20000000

#define WARP_SIZE         32
#define WARPS_PER_BLOCK   2
#define THREADS_PER_BLOCK (WARPS_PER_BLOCK * WARP_SIZE)
#define BLOCKS_NUM        ((PRECISION_HEX / dLEAP / THREADS_PER_BLOCK) + 1)

#define NUM_ITERATIONS 1
#define NPARTICLES     8388608

void
print_usage()
{
    LOG_JUST_PRINT("Usage: srun -n ./vpicio #particles #steps sleep_time(s) [transform]\n");
}

/* In addition to the existing per-operation LOG_WARNING lines (left
 * unchanged, still useful for fine-grained debugging), rank 0 now also
 * prints one CSV line per step:
 *   mode,step,n_ranks,transform,setup_s,write_s,step_total_s
 *
 * setup_s (container/property creation, once) and write_s (object
 * create + transfer create/start/wait/close + object close, summed) are
 * real MPI_Wtime brackets around a barrier, matching the timing
 * convention used throughout pdc_helper_scripts/analysis_scripts/ and
 * curl_analysis_scripts/. write_s deliberately EXCLUDES the sleep(3)
 * call between transfer_start and transfer_wait (the synthetic
 * "emulate compute" delay, up to 100s/step in the real sweep scripts)
 * -- that's an injected artificial gap, not real I/O cost, and folding
 * it into write_s would make every transform look identically,
 * fictitiously slow regardless of what it actually costs.
 *
 * There is no confirmation read or correctness check anywhere in this
 * benchmark (none existed before this change either) -- it measures
 * write throughput under a given transform, nothing else -- so there is
 * deliberately no trailing "bad" column the way the analysis_scripts
 * benchmarks have; printing a fake always-0 value here would imply a
 * check that never happens. Server-side close cost (PDC-specific
 * durability/checkpoint cost, no client-side equivalent) is not printed
 * by this binary at all -- it's captured the same way analysis_scripts
 * does it, by grepping close_server's own "total close time" log lines
 * after the fact (see vpicio_scripts/common.sh and the *.sbatch files). */

int
main(int argc, char **argv)
{
    int         rank = 0, size = 1;
    pdcid_t     pdc_id, cont_prop, cont_id, region_local, region_remote;
    pdcid_t     obj_prop_float, obj_prop_int;
    pdcid_t     obj_ids[8];
    float *     dx, *dy, *dz, *ux, *uy, *uz, *q;
    int *       id;
    int         ndim = 1, steps = 1, sleeptime = 0;
    uint64_t    numparticles, dims[1], offset_local[1], offset_remote[1], mysize[1];
    double      t0, t1, t_setup0 = 0, t_setup1 = 0, setup_s = 0, step_write_s;
    const char *obj_names[] = {"dX", "dY", "dZ", "Ux", "Uy", "Uz", "q", "i"};
    char        obj_name[64];
    const char *transformation_str = "raw";

    pdcid_t transfer_requests[8];

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
#endif

    numparticles = NPARTICLES;
    if (argc >= 2)
        numparticles = atoll(argv[1]);
    if (argc >= 3)
        steps = atoi(argv[2]);
    if (argc >= 4)
        sleeptime = atoi(argv[3]);
    if (argc >= 5)
        transformation_str = argv[4];

    if (rank == 0)
        LOG_WARNING("Writing %" PRIu64 " particles per rank for %d steps with %d sec sleep time.\n",
                    numparticles, steps, sleeptime);

    dims[0] = numparticles * size;

    dx = (float *)malloc(numparticles * sizeof(float));
    dy = (float *)malloc(numparticles * sizeof(float));
    dz = (float *)malloc(numparticles * sizeof(float));
    ux = (float *)malloc(numparticles * sizeof(float));
    uy = (float *)malloc(numparticles * sizeof(float));
    uz = (float *)malloc(numparticles * sizeof(float));
    q  = (float *)malloc(numparticles * sizeof(float));

    id = (int *)malloc(numparticles * sizeof(int));

    void *data_ptrs[] = {&dx[0], &dy[0], &dz[0], &ux[0], &uy[0], &uz[0], &q[0], &id[0]};

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t_setup0 = MPI_Wtime();
#endif

    // create a pdc
    pdc_id = PDCinit("pdc");
    if (pdc_id == 0) {
        LOG_ERROR("Failed to initialize PDC\n");
        return FAIL;
    }

    // create a container property
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc_id);
    if (cont_prop <= 0) {
        LOG_ERROR("Failed to create container property\n");
        return FAIL;
    }
    // create a container
#ifdef ENABLE_MPI
    cont_id = PDCcont_create_col("c1", cont_prop);
#else
    cont_id = PDCcont_create("c1", cont_prop);
#endif
    if (cont_id <= 0) {
        LOG_ERROR("Failed to create container\n");
        return FAIL;
    }
    // create an object property
    obj_prop_float = PDCprop_create(PDC_OBJ_CREATE, pdc_id);
    PDCprop_set_obj_dims(obj_prop_float, 1, dims);
    PDCprop_set_obj_type(obj_prop_float, PDC_FLOAT);
    PDCprop_set_obj_user_id(obj_prop_float, getuid());
    PDCprop_set_obj_app_name(obj_prop_float, "VPICIO");
    PDCprop_set_obj_transfer_region_type(obj_prop_float, PDC_REGION_STATIC);

    obj_prop_int = PDCprop_obj_dup(obj_prop_float);
    PDCprop_set_obj_type(obj_prop_int, PDC_INT);

#ifdef ENABLE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    t_setup1 = MPI_Wtime();
    setup_s  = t_setup1 - t_setup0;
#endif

    /* Deterministic, index-based generation (same convention as
     * bench_magnitude.c / bench_curl_eager.c / hdf5_bench_write.c etc.)
     * instead of the previous uniform_random_number()-based values --
     * this benchmark never had a correctness check at all before, and
     * an rand()-derived sequence would be fragile to regenerate
     * independently from vpicio_verify's own separate process (any
     * difference in how many times something upstream happens to call
     * rand() would silently desync the two processes' sequences).
     * Deterministic and per-rank via global_i means vpicio_verify can
     * reconstruct exactly these values with nothing more than
     * numparticles and rank. */
    for (uint64_t i = 0; i < numparticles; i++) {
        uint64_t global_i = (uint64_t)rank * numparticles + i;
        id[i]             = (int)global_i;
        q[i]              = (float)((global_i % 1000) + 1);
        dx[i]             = (float)((global_i % 1000) + 1);
        dy[i]             = (float)(((global_i + 137) % 1000) + 1);
        dz[i]             = (float)(((global_i + 271) % 1000) + 1);
        ux[i]             = (float)(((global_i + 613) % 1000) + 1);
        uy[i]             = (float)(((global_i + 911) % 1000) + 1);
        uz[i]             = (float)(((global_i + 1301) % 1000) + 1);
    }

    offset_local[0]  = 0;
    offset_remote[0] = rank * numparticles;
    mysize[0]        = numparticles;

    // create local and remote region
    region_local  = PDCregion_create(ndim, offset_local, mysize);
    region_remote = PDCregion_create(ndim, offset_remote, mysize);

    for (int iter = 0; iter < steps; iter++) {
        step_write_s = 0;
        // Change data for different steps for verification
        id[0]                = rank + iter;
        q[0]                 = rank + iter * 2;
        id[numparticles - 1] = rank - iter;
        q[numparticles - 1]  = rank - iter * 2;

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        if (rank == 0)
            LOG_WARNING("\n#Step  %d\n", iter);
        t0 = MPI_Wtime();
#endif

        for (int i = 0; i < 8; i++) {
            sprintf(obj_name, "%s-%d", obj_names[i], iter);
            pdcid_t obj_prop = (i < 7) ? obj_prop_float : obj_prop_int;
#ifdef ENABLE_MPI
            obj_ids[i] = PDCobj_create_mpi(cont_id, obj_name, obj_prop, 0, MPI_COMM_WORLD);
#else
            obj_ids[i] = PDCobj_create(cont_id, obj_name, obj_prop);
#endif
            if (obj_ids[i] == 0) {
                LOG_ERROR("Error getting an object id of %s from server\n", obj_name);
                return FAIL;
            }
            if (!strcmp(transformation_str, "turbo") && obj_prop == obj_prop_int) {
                if (rank == 0)
                    LOG_WARNING("Attaching turbo to index: %d\n", obj_ids[i]);
                pdcid_t dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "turbo.json");
                PDCtf_attach_to_obj(dg_id, obj_ids[i], "decompressed", "compressed");
            }
            else if (!strcmp(transformation_str, "zfp") && obj_prop == obj_prop_float) {
                if (rank == 0)
                    LOG_WARNING("Attaching zfp to index: %d\n", obj_ids[i]);
                pdcid_t dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "zfp.json");
                PDCtf_attach_to_obj(dg_id, obj_ids[i], "decompressed", "compressed");
            }
            else if (!strcmp(transformation_str, "zfp_gpu") && obj_prop == obj_prop_float) {
                if (rank == 0)
                    LOG_WARNING("Attaching zfp gpu to index: %d\n", obj_ids[i]);
                pdcid_t dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "zfp_gpu.json");
                PDCtf_attach_to_obj(dg_id, obj_ids[i], "decompressed", "compressed");
            }
            else if (!strcmp(transformation_str, "zfp_gpu_or_cpu") && obj_prop == obj_prop_float) {
                if (rank == 0)
                    LOG_WARNING("Attaching zfp gpu to index: %d\n", obj_ids[i]);
                pdcid_t dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "zfp_gpu_or_cpu.json");
                PDCtf_attach_to_obj(dg_id, obj_ids[i], "decompressed", "compressed");
            }
            else if (!strcmp(transformation_str, "sz") && obj_prop == obj_prop_float) {
                if (rank == 0)
                    LOG_WARNING("Attaching sz to index: %d\n", obj_ids[i]);
                pdcid_t dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "sz.json");
                PDCtf_attach_to_obj(dg_id, obj_ids[i], "decompressed", "compressed");
            }
            else if (!strcmp(transformation_str, "zfp_libsod") && obj_prop == obj_prop_float) {
                if (rank == 0)
                    LOG_WARNING("Attaching zfp_libsod to index: %d\n", obj_ids[i]);
                pdcid_t dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "zfp_libsod.json");
                PDCtf_attach_to_obj(dg_id, obj_ids[i], "decompressed", "encrypted");
                if (rank == 0)
                    PDCtf_print_dg(dg_id, true);
            }
            else if (!strcmp(transformation_str, "custom") && obj_prop == obj_prop_float) {
                if (rank == 0)
                    LOG_WARNING("Attaching custom to index: %d\n", obj_ids[i]);
                pdcid_t dg_id = PDCtf_dg_json_create(TF_GRAPHS_DIR "custom.json");
                PDCtf_attach_to_obj(dg_id, obj_ids[i], "client", "server");
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        step_write_s += (t1 - t0);
        if (rank == 0)
            LOG_WARNING("Obj create time: %.5e\n", t1 - t0);
#endif

        for (int i = 0; i < 8; i++) {
            transfer_requests[i] =
                PDCregion_transfer_create(data_ptrs[i], PDC_WRITE, obj_ids[i], region_local, region_remote);
            if (transfer_requests[i] == 0) {
                LOG_ERROR("%s transfer request creation failed\n", obj_names[i]);
                return FAIL;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
        step_write_s += (t0 - t1);
        if (rank == 0)
            LOG_WARNING("Transfer create time: %.5e\n", t0 - t1);
#endif

#ifdef ENABLE_MPI
        if (PDCregion_transfer_start_all_mpi(transfer_requests, 8, MPI_COMM_WORLD) != SUCCEED) {
#else
        if (PDCregion_transfer_start_all(transfer_requests, 8) != SUCCEED) {
#endif
            LOG_ERROR("Failed to start transfer requests\n");
            return FAIL;
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        step_write_s += (t1 - t0);
        if (rank == 0)
            LOG_WARNING("Transfer start time: %.5e\n", t1 - t0);
#endif
        // Emulate compute with sleep
        if (iter != steps - 1) {
            if (rank == 0)
                LOG_WARNING("Sleep start: %llu.00\n", sleeptime);
            double loop_start = MPI_Wtime();
            // Call C function which launches kernel here
            sleep(sleeptime);
            double loop_end = MPI_Wtime();
            if (rank == 0) {
                LOG_WARNING("\nTotal time for %d iterations: %f s\n", NUM_ITERATIONS, loop_end - loop_start);
            }
            if (rank == 0)
                LOG_WARNING("Sleep end: %llu.00\n", sleeptime);
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
#endif

        if (PDCregion_transfer_wait_all(transfer_requests, 8) != SUCCEED) {
            LOG_ERROR("Failed to transfer wait all\n");
            return FAIL;
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        step_write_s += (t1 - t0);
        if (rank == 0)
            LOG_WARNING("Transfer wait time: %.5e\n", t1 - t0);
#endif

        for (int j = 0; j < 8; j++) {
            if (PDCregion_transfer_close(transfer_requests[j]) != SUCCEED) {
                LOG_ERROR("region transfer close failed\n");
                return FAIL;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t0 = MPI_Wtime();
        step_write_s += (t0 - t1);
        if (rank == 0)
            LOG_WARNING("Transfer close time: %.5e\n", t0 - t1);
#endif

        for (int i = 0; i < 8; i++) {
            if (PDCobj_close(obj_ids[i]) != SUCCEED) {
                LOG_ERROR("Failed to close object #%d\n", i);
                return FAIL;
            }
        }

#ifdef ENABLE_MPI
        MPI_Barrier(MPI_COMM_WORLD);
        t1 = MPI_Wtime();
        step_write_s += (t1 - t0);
        if (rank == 0)
            LOG_WARNING("Obj close time: %.5e\n", t1 - t0);
#endif

        if (rank == 0) {
            printf("vpicio,%d,%d,%s,%.6f,%.6f,%.6f\n", iter, size, transformation_str, setup_s, step_write_s,
                   setup_s + step_write_s);
            fflush(stdout);
        }
    } // End for steps

    if (PDCprop_close(obj_prop_float) != SUCCEED) {
        LOG_ERROR("Failed to close obj_prop_float\n");
        return FAIL;
    }
    if (PDCprop_close(obj_prop_int) != SUCCEED) {
        LOG_ERROR("Failed to close obj_prop_int\n");
        return FAIL;
    }
    if (PDCregion_close(region_local) != SUCCEED) {
        LOG_ERROR("Failed to close local region \n");
        return FAIL;
    }
    if (PDCregion_close(region_remote) != SUCCEED) {
        LOG_ERROR("Failed to close remote region\n");
        return FAIL;
    }
    if (PDCcont_close(cont_id) != SUCCEED) {
        LOG_ERROR("Failed to close container\n");
        return FAIL;
    }
    if (PDCprop_close(cont_prop) != SUCCEED) {
        LOG_ERROR("Failed to close property\n");
        return FAIL;
    }
    if (PDCclose(pdc_id) != SUCCEED) {
        LOG_ERROR("Failed to close PDC\n");
        return FAIL;
    }
    free(dx);
    free(dy);
    free(dz);
    free(ux);
    free(uy);
    free(uz);
    free(id);
    free(q);
#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
