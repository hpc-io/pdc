/* Portable public-API experiment: bounded reads in the original per-rank shuffle order. */
#include <mpi.h>
#include <pdc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef DIM1
#define DIM1 10240
#endif
#ifndef DIM2
#define DIM2 10240
#endif
#define MAX_DEPTH    16
#define RECORD_BYTES ((size_t)DIM1 * (size_t)DIM2)
#define REQUIRE(x)                                                                                           \
    do {                                                                                                     \
        if (!(x)) {                                                                                          \
            fprintf(stderr, "rank %d: %s failed at %d\n", rank, #x, __LINE__);                               \
            MPI_Abort(MPI_COMM_WORLD, 20);                                                                   \
        }                                                                                                    \
    } while (0)
static uint64_t shuffle_seed;
static uint32_t
next_random(void)
{
    shuffle_seed = (shuffle_seed * 6364136223846793005ULL + 1);
    return (uint32_t)(shuffle_seed >> 32);
}
int
main(int argc, char **argv)
{
    int rank, ranks;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);
    if (argc != 7) {
        if (!rank)
            fprintf(stderr, "Usage: %s files depth each|all verify(0|1) open_container(0|1) seed\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    int files = atoi(argv[1]), depth = atoi(argv[2]), verify = atoi(argv[4]), open_container = atoi(argv[5]);
    int aggregate = strcmp(argv[3], "all") == 0;
    REQUIRE(files > 0 && depth > 0 && depth <= MAX_DEPTH);
    REQUIRE(aggregate || strcmp(argv[3], "each") == 0);
    REQUIRE((verify == 0 || verify == 1) && (open_container == 0 || open_container == 1));
    const char *prefix = getenv("DATA_DIR");
    REQUIRE(prefix);
    pdcid_t pdc = PDCinit("pdc");
    REQUIRE(pdc);
    int *indices = malloc((size_t)files * sizeof(*indices));
    REQUIRE(indices);
    for (int i = 0; i < files; i++)
        indices[i] = i;
    shuffle_seed = strtoull(argv[6], NULL, 10);
    for (int i = files - 1; i > 0; i--) {
        int j = next_random() % (i + 1), t = indices[i];
        indices[i] = indices[j];
        indices[j] = t;
    }
    uint8_t *buffers[MAX_DEPTH], **expected = NULL;
    for (int j = 0; j < depth; j++) {
        buffers[j] = malloc(RECORD_BYTES);
        REQUIRE(buffers[j]);
    }
    /* Verification is a separate, untimed invocation. It uses the SAME shuffle
     * and batching as the reader, including mixed generator seeds in a batch. */
    if (verify) {
        expected = calloc((size_t)ranks, sizeof(*expected));
        REQUIRE(expected);
        for (int r = 0; r < ranks; r++) {
            expected[r] = malloc(RECORD_BYTES);
            REQUIRE(expected[r]);
            srand(10 + r);
            for (size_t k = 0; k < RECORD_BYTES; k++)
                expected[r][k] = (uint8_t)(rand() % 255);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (!rank) {
        printf("--- PDC public-API bounded reader ---\nRanks: %d | Epochs: 1 | Total Files: %d\n", ranks,
               files);
        printf("depth=%d mode=%s verify=%d open_container=%d bytes_per_file=%zu\n", depth, argv[3], verify,
               open_container, RECORD_BYTES);
    }
    int    local_count = (files > rank) ? (files - rank + ranks - 1) / ranks : 0;
    double epoch_start = MPI_Wtime();
    for (int base = 0; base < local_count; base += depth) {
        int     n = local_count - base < depth ? local_count - base : depth;
        pdcid_t objects[MAX_DEPTH], containers[MAX_DEPTH] = {0}, local[MAX_DEPTH], remote[MAX_DEPTH],
                                    requests[MAX_DEPTH];
        int    samples[MAX_DEPTH];
        double sample_start[MAX_DEPTH];
        for (int j = 0; j < n; j++) {
            samples[j]      = indices[rank + (base + j) * ranks];
            sample_start[j] = MPI_Wtime();
            char name[1024];
            int  len = snprintf(name, sizeof(name), "%s/dataset_%d", prefix, samples[j]);
            REQUIRE(len > 0 && (size_t)len < sizeof(name));
            if (open_container) {
                containers[j] = PDCcont_open(name, pdc);
                REQUIRE(containers[j]);
            }
            objects[j] = PDCobj_open(name, pdc);
            REQUIRE(objects[j]);
            uint64_t offset[3] = {0, 0, 0}, dims[3] = {1, DIM1, DIM2};
            local[j]  = PDCregion_create(3, offset, dims);
            remote[j] = PDCregion_create(3, offset, dims);
            REQUIRE(local[j] && remote[j]);
            requests[j] = PDCregion_transfer_create(buffers[j], PDC_READ, objects[j], local[j], remote[j]);
            REQUIRE(requests[j]);
        }
        if (aggregate)
            REQUIRE(PDCregion_transfer_start_all(requests, n) >= 0);
        else
            for (int j = 0; j < n; j++)
                REQUIRE(PDCregion_transfer_start(requests[j]) >= 0);
        if (n == 1)
            REQUIRE(PDCregion_transfer_wait(requests[0]) >= 0);
        else
            REQUIRE(PDCregion_transfer_wait_all(requests, n) >= 0);
        for (int j = 0; j < n; j++) {
            if (verify && memcmp(buffers[j], expected[samples[j] % ranks], RECORD_BYTES) != 0) {
                fprintf(stderr, "rank %d sample %d: full record mismatch\n", rank, samples[j]);
                MPI_Abort(MPI_COMM_WORLD, 21);
            }
            REQUIRE(PDCregion_transfer_close(requests[j]) >= 0);
            printf("[RANK %d | EPOCH 1] Read Sample %d in %.4f sec\n", rank, samples[j],
                   MPI_Wtime() - sample_start[j]);
            memset(buffers[j], 0, RECORD_BYTES);
            REQUIRE(PDCregion_close(local[j]) >= 0 && PDCregion_close(remote[j]) >= 0);
            REQUIRE(PDCobj_close(objects[j]) >= 0);
            if (open_container)
                REQUIRE(PDCcont_close(containers[j]) >= 0);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    double elapsed = MPI_Wtime() - epoch_start;
    if (!rank)
        printf("Epoch 1 Complete | Time: %.4f sec\n", elapsed);
    int total;
    MPI_Reduce(&local_count, &total, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    if (verify && !rank)
        printf("VERIFIED backend=pdc-pipeline files=%d bytes_per_file=%zu generator_ranks=%d depth=%d "
               "mode=%s container_open=%d\n",
               total, RECORD_BYTES, ranks, depth, argv[3], open_container);
    REQUIRE(PDCclose(pdc) >= 0);
    for (int j = 0; j < depth; j++)
        free(buffers[j]);
    if (verify) {
        for (int r = 0; r < ranks; r++)
            free(expected[r]);
        free(expected);
    }
    free(indices);
    MPI_Finalize();
    return 0;
}
