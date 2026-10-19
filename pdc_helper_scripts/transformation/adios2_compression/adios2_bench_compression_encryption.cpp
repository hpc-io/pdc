/**
 * ADIOS2 analog of Data Flyway's chained ZFP-compression +
 * libsodium-encryption transform (tf_client/graphs/zfp_libsod.json,
 * src/tests/transformation/compression_encryption_transform.c, and the
 * "ZFP chained with a libsodium encryption stage" VPIC-IO configuration
 * in evaluation.tex).
 *
 * ADIOS2 has no built-in encryption operator (unlike ZFP/SZ/etc., which
 * ship in source/adios2/operator/compress/) -- but it does ship a
 * complete, ready-to-use *plugin* operator implementation,
 * plugins/operators/EncryptionOperator.cpp, built whenever libsodium is
 * found (ADIOS2_HAVE_Sodium), using libsodium's crypto_secretbox_easy
 * -- XSalsa20-Poly1305, the exact same algorithm Data Flyway's own
 * encryption transform uses (see table.tex's shared experimental
 * config: "Encryption: XSalsa20-Poly1305"). No custom code was written
 * for this benchmark; it's the same reference implementation ADIOS2's
 * own examples/plugins/operator/examplePluginOperatorWrite.cpp uses.
 *
 * Two AddOperation calls on the same variable chain compression then
 * encryption, applied in that order on write and inverted in reverse
 * order on read -- the direct ADIOS2 analog of Data Flyway composing
 * zfp_compress -> encrypt in one transform graph:
 *
 *   var.AddOperation("zfp", {{"rate", "16"}});
 *   var.AddOperation("plugin", {{"PluginName", "MyEncryptOp"},
 *                                {"PluginLibrary", "EncryptionOperator"},
 *                                {"SecretKeyFile", keyFile}});
 *
 * Where this differs from Data Flyway, beyond what
 * adios2_bench_compression.cpp already documents for the ZFP half:
 *   - BP5, ADIOS2's current default file engine, flatly refuses more
 *     than one operator per variable: attaching both zfp and
 *     EncryptionOperator under BP5 throws "BP5 does not support
 *     multiple operators" (toolkit/format/bp5/BP5Serializer.cpp) --
 *     confirmed by hitting that exact exception with this workload.
 *     Data Flyway composes an arbitrary chain of transforms on one
 *     region without this restriction (table.tex's "Composable
 *     transforms" row: Full for Data Flyway, None for everything
 *     else). This workload only works at all because it falls back to
 *     BP4 (io.SetEngine("BP4")), ADIOS2's older file format, which has
 *     no such check -- itself further, concrete evidence for that same
 *     "None" rating, not a workaround that erases the finding.
 *   - The encryption key is a plain file on local disk
 *     (EncryptionOperator::GenerateOrReadKey: written in cleartext the
 *     first time it's needed, read back thereafter) -- this benchmark
 *     writes it under the same directory as the .bp output for
 *     convenience. This is a benchmark, not a real key-management
 *     deployment either way (Data Flyway's own libsodium transform is
 *     no different in that respect), so this isn't a meaningfully
 *     unfair simplification.
 *   - InverseOperate() (decrypt) reads the key back from that same
 *     file via ADIOS2's own plugin loading, independent of the
 *     "SecretKeyFile" param only being needed on the *write* side per
 *     EncryptionOperator's own constructor comment ("If
 *     'secretkeyfile' is not found, the operator should be calling
 *     InverseOperate(), due to ADIOS not allowing Parameters to be
 *     passed then") -- the key file's *path* still has to match
 *     between write and read for this to work at all, which this
 *     benchmark's single continuous process trivially satisfies.
 *
 * Usage: adios2_bench_compression_encryption <n_elem_per_rank> [out_file]
 *
 * Prints one CSV line per timestep from rank 0:
 *   mode,step,n_client_ranks,n_elem,setup_s,write_s,confirm_read_s,step_total_s,bad
 */

#define N_TIMESTEPS 3

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <string>
#include <vector>
#include <mpi.h>
#include <adios2.h>

/* Same tolerance rationale as adios2_bench_compression.cpp: ZFP is
 * lossy at a fixed rate; encryption on top doesn't add error, it just
 * wraps ZFP's already-lossy output losslessly. */
#define EPSILON 1e-2

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank, nranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (argc < 2) {
        if (rank == 0)
            fprintf(stderr, "Usage: %s <n_elem_per_rank> [out_file]\n", argv[0]);
        MPI_Finalize();
        return 1;
    }
    long        n_elem   = atol(argv[1]);
    const char *out_file = (argc >= 3) ? argv[2] : "adios2_bench_compression_encryption.bp";
    std::string key_file = std::string(out_file) + ".key";

    std::vector<float> data(n_elem), data_read(n_elem);
    for (long i = 0; i < n_elem; ++i)
        data[i] = (float)((i % 1000) + 1) * 1.000123f;

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup0 = MPI_Wtime();

    adios2::ADIOS adios(MPI_COMM_WORLD);
    adios2::IO    io = adios.DeclareIO("BenchCompressionEncryption");
    /* BP5 (ADIOS2's current default engine) explicitly rejects more
     * than one operator per variable ("BP5 does not support multiple
     * operators", toolkit/format/bp5/BP5Serializer.cpp) -- confirmed by
     * hitting that exact exception with this workload's two chained
     * operators (zfp, then encryption). BP4, the predecessor format,
     * has no such restriction, so this workload specifically. */
    io.SetEngine("BP4");

    size_t global = (size_t)nranks * (size_t)n_elem;
    size_t offset = (size_t)rank * (size_t)n_elem;
    size_t count  = (size_t)n_elem;

    auto var = io.DefineVariable<float>("data", {global}, {offset}, {count});
    var.AddOperation("zfp", {{"rate", "16"}});
    var.AddOperation("plugin", {{"PluginName", "AdiosBenchEncrypt"},
                                {"PluginLibrary", "EncryptionOperator"},
                                {"SecretKeyFile", key_file}});

    MPI_Barrier(MPI_COMM_WORLD);
    double t_setup1  = MPI_Wtime();
    double max_setup = 0;
    {
        double local_setup = t_setup1 - t_setup0;
        MPI_Reduce(&local_setup, &max_setup, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    }

    double step_write_s[N_TIMESTEPS];
    {
        adios2::Engine writer = io.Open(out_file, adios2::Mode::Write);
        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_write0 = MPI_Wtime();

            /* Both operators run transparently inside this bracket, in
             * the order attached (compress, then encrypt) -- no
             * explicit client-side compress/encrypt calls. */
            writer.BeginStep();
            writer.Put(var, data.data());
            writer.EndStep();

            MPI_Barrier(MPI_COMM_WORLD);
            double t_write1    = MPI_Wtime();
            double local_write = t_write1 - t_write0;
            MPI_Reduce(&local_write, &step_write_s[step], 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        }
        writer.Close();
    }

    int global_bad = 0;
    {
        adios2::IO     rio    = adios.DeclareIO("BenchCompressionEncryptionRead");
        adios2::Engine reader = rio.Open(out_file, adios2::Mode::ReadRandomAccess);
        auto           rvar   = rio.InquireVariable<float>("data");

        for (int step = 0; step < N_TIMESTEPS; ++step) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t_read0 = MPI_Wtime();

            rvar.SetStepSelection({(size_t)step, 1});
            rvar.SetSelection({{offset}, {count}});
            std::fill(data_read.begin(), data_read.end(), 0.0f);
            /* Both inverse operators (decrypt, then decompress) run
             * transparently inside this Get(). */
            reader.Get(rvar, data_read.data(), adios2::Mode::Sync);

            MPI_Barrier(MPI_COMM_WORLD);
            double t_read1    = MPI_Wtime();
            double local_read = t_read1 - t_read0;
            double max_read   = 0;
            MPI_Reduce(&local_read, &max_read, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

            int local_bad = 0;
            for (long i = 0; i < n_elem; ++i)
                if (fabs((double)data_read[i] - (double)data[i]) > EPSILON)
                    local_bad++;
            int step_bad = 0;
            MPI_Reduce(&local_bad, &step_bad, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
            global_bad += step_bad;

            if (rank == 0) {
                double step_total = max_setup + step_write_s[step] + max_read;
                printf("adios2_zfp_encrypt,%d,%d,%ld,%.6f,%.6f,%.6f,%.6f,%d\n", step, nranks, n_elem,
                       max_setup, step_write_s[step], max_read, step_total, step_bad);
                fflush(stdout);
            }
        }
        reader.Close();
    }

    MPI_Finalize();
    return global_bad != 0;
}
