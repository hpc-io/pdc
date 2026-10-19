# curl_analysis_scripts

Slurm jobs and supporting bash scripts for an E3SM-shaped curl +
vorticity-magnitude analysis benchmark, comparing PDC eager
(DataFlyway), a GPU-ZFP-compressed variant of eager, a genuinely
single-restart PDC posthoc workflow, and a plain parallel-HDF5
baseline -- modeled after the tropical-cyclone-track use case in the
paper this compares against (curl of wind velocity / magnitude of
curl, used to detect atmospheric rotation in E3SM output on Frontier).

## The pipeline

`an_client/graphs/curl_vorticity_magnitude.json` chains two builtin,
server-side analysis functions:

```
u, v, w  --curl-->  curl_x, curl_y, curl_z  --vector_magnitude-->  vorticity_magnitude
```

Every state is **persistent** (not just the endpoints) -- the source
paper's "Store" strategy treats curl itself as a real, queryable, stored
quantity (the extra ~4GB it reports going from 24GB primary data to
28GB total), not a throwaway intermediate. `curl` and `vector_magnitude`
are both compiled directly into the server as builtins (see
`src/server/analysis/pdc_an_builtin_curl_cpu.c` and
`pdc_an_builtin_magnitude_cpu.c`), registered in
`PDCan_init_builtin_funcs` -- no dlopen, no external library, since both
are general enough to ship as part of PDC itself.

`curl`'s math lives in `src/tests/analysis/curl_math.h`, shared between
the server-side builtin and the posthoc client compute binary so both
modes compute the identical result: central differences in the interior
of each rank's local (nx, ny, nz) block, one-sided at the block's own
edges (a stand-in for a halo exchange with neighboring ranks, not a real
one), unit grid spacing. Like `vector_magnitude`'s plain
`sqrt(x^2+y^2+z^2)`, this is a benchmark stand-in for curl's cost and
data shape, not a scientifically exact discretization of E3SM's actual
grid.

Like `analysis_scripts/`'s magnitude benchmark, every mode here repeats
its write(+compute) cycle for `N_TIMESTEPS = 3` timesteps within one
session (or, for posthoc/HDF5, across its write and analyze phases),
each producing a distinct, uniquely named set of objects/datasets
(`u_0`/`u_1`/`u_2`, `curl_x_0`/`curl_x_1`/`curl_x_2`,
`vorticity_magnitude_0`/..., etc.) rather than a single shot -- see
`bench_magnitude.c`'s comment on why per-timestep names are required
instead of a shared name with an incrementing `time_step` property.
Each timestep gets its own CSV row (see "Result CSV schemas" below).

### Domain decomposition

Each rank owns a local `NX x NY x NZ_PER_RANK` block of `u`, `v`, `w`
(float32) and, correspondingly, of `curl_x/y/z` and
`vorticity_magnitude` (float64). The global object spans
`[NX, NY, nranks*NZ_PER_RANK]`, split along the z axis (vertical
levels): rank `r` owns `global_offset = [0, 0, r*NZ_PER_RANK]`. This is
the same `PDC_REGION_STATIC` region-offset routing pattern as
`analysis_scripts/`' magnitude benchmark, just 3D instead of 1D.

### Eager vs. posthoc

- **Eager** (`bench_curl_eager`, `curl_eager_analysis.sbatch`): one
  continuous client session. Writing `u`, `v`, `w` with the graph
  attached transparently triggers server-side `curl` then
  `vector_magnitude` in the write path; a confirmation read of
  `vorticity_magnitude` follows.
- **Posthoc** (`bench_curl_write` / `bench_curl_analyze`,
  `curl_posthoc_analysis.sbatch`): two genuinely separate client
  processes, matching the real posthoc workflow this is supposed to
  model -- a write phase that persists `u`, `v`, `w` and exits, and a
  later analyze phase that reopens them, computes `curl` then
  `vorticity_magnitude`, and writes both out, all in that one analyze
  process (deriving curl and then magnitude from it doesn't need its
  own separately relaunched job any more than eager needs one -- both
  are just downstream computation once the analyze phase already has
  the inputs in hand). The PDC server is fully closed (which
  checkpoints its metadata) and restarted (`./pdc_server restart`,
  which reloads that checkpoint) **between the two phases**, so the
  analyze phase is really talking to a server that had to reload
  persisted data, not one that kept it in memory the whole time. See
  `analysis_scripts/README.md`'s "Result CSV schema" section for why
  this relaunch cost matters.
- **HDF5 baseline** (`hdf5_bench_curl_write` / `hdf5_bench_curl_analyze`
  in `hdf5_analysis_test/`, `curl_hdf5_analysis.sbatch`): the same
  two-phase shape as PDC posthoc (write u,v,w; then read them back and
  compute curl then magnitude), each phase a separate srun step against
  a plain HDF5 file -- no PDC server, so no restart cycle, just the file
  being reopened fresh for the analyze phase. Uses the identical
  `curl_math.h` kernel as the PDC side (see `hdf5_analysis_test/Makefile`'s
  `-I../src/tests/analysis`), so results are directly comparable.

### Compression (optional)

Set `COMPRESS=1` (default `0`) to additionally compose the existing,
already-GPU-backed ZFP compression transform
(`tf_client/graphs/zfp_gpu.json`, `zfp_compress`/`zfp_decompress`
builtins) onto `vorticity_magnitude` before it's written -- via
`PDCtf_attach_to_region(tf_dg_id, mag_obj, reg_global, "decompressed",
"compressed")` called *before* the analysis graph (eager) or the plain
write (posthoc's final phase). `PDCan_attach_to_region`'s
`find_attached_tf_info` (see `src/api/pdc_an/pdc_an.c`) automatically
picks up and piggybacks that composition, so this reuses PDC's existing
transformation framework end to end -- no new compression code. This is
exactly the poster's "an analysis output can itself be compressed or
transformed" claim, exercised for real.

- **Eager, compressed** (`bench_curl_eager --compress`,
  `curl_eager_compress_analysis.sbatch`): mechanically identical to
  plain eager -- same binary, same one-continuous-session shape -- with
  `COMPRESS` fixed to `1` instead of an ad hoc env-var override, and its
  own dedicated results file
  (`results_curl_eager_compress_<jobid>.csv`) and `curl_eager_compress`
  mode label. Kept as its own test type (rather than folding compressed
  runs into `results_curl_eager_*.csv` distinguished only by the CSV's
  `compress` column) so it shows up in `plot_curl_totals.py` as its own
  directly comparable workload next to plain eager, posthoc, and the
  HDF5 baseline. Posthoc's analyze phase also accepts `COMPRESS=1` (see
  `curl_posthoc_analysis.sbatch`) since `bench_curl_analyze.c` composes
  the identical transform onto its own `vorticity_magnitude` write, but
  that variant doesn't get its own dedicated test type here -- only
  eager does, per this section. There's no HDF5-compressed variant at
  all: compression is a PDC transformation-framework feature with
  nothing analogous on the plain-HDF5 side.

## E3SM data-shape derivation

The source paper doesn't state a grid resolution, so there's no single
authoritative shape to copy. What it does give is internally consistent
with this benchmark's element-wise convention (float32 in/out, float64
for magnitude): `curl_x/y/z` mirror `u/v/w`'s shape exactly (the compute
is element-wise per rank, not resolution-reducing), so if curl totals
~4GB across 3 components, magnitude (1 field, double-precision) comes
out to `(4GB/3) * 2 ≈ 2.67GB` -- matching the paper's reported "~3GB"
closely. So `NX=256, NY=256, NZ_PER_RANK=64` is sized to total ~4GB of
`u+v+w` at a **96-rank reference point** (matching the paper's primary
analysis rank count): `256*256*64 = 4,194,304` elements/rank/variable,
`*4 bytes = 16MiB/rank/variable`, `*3 variables *96 ranks ≈ 4.6GB`. The
other 6 of the paper's "9 primary variables" (~20GB) aren't part of
this curl/magnitude chain and aren't synthesized here.

This benchmark sweeps node/rank count with these dimensions held fixed
per rank (weak scaling), consistent with `analysis_scripts/`'s
convention, rather than replicating the paper's own strong-scaling
study (fixed total problem size, 96 up to 900 processes) -- the 96-rank
sizing above is a calibration reference point, not a hard scale limit.
Override `NX`/`NY`/`NZ_PER_RANK` at submit time the same way `N_ELEM` is
overridden in `analysis_scripts/`, e.g.:

```
NZ_PER_RANK=128 sbatch --nodes=4 curl_eager_analysis.sbatch
```

## Layout

| File | Role |
|---|---|
| `srun_server.sh` | Starts a fresh `pdc_server` (no prior data) in the background |
| `srun_server_restart.sh` | Restarts `pdc_server` with `restart`, reloading the metadata checkpoint |
| `srun_close_server.sh` | Gracefully shuts the server down via `close_server` (checkpoints first) |
| `srun_client_curl_eager.sh` | Runs `bench_curl_eager`, appends a CSV row |
| `srun_client_curl_write.sh` | Posthoc phase 1: runs `bench_curl_write` |
| `srun_client_curl_analyze.sh` | Posthoc phase 2: runs `bench_curl_analyze` (reads u/v/w, computes curl then magnitude, writes both) |
| `srun_hdf5_curl_write.sh` | HDF5 baseline phase 1: runs `hdf5_bench_curl_write` |
| `srun_hdf5_curl_analyze.sh` | HDF5 baseline phase 2: runs `hdf5_bench_curl_analyze` (reads u/v/w, computes curl then magnitude, writes both) |
| `curl_eager_analysis.sbatch` | Single-node-count job: eager curl+magnitude |
| `curl_eager_compress_analysis.sbatch` | Single-node-count job: eager curl+magnitude, GPU-ZFP-compressed `vorticity_magnitude` (`COMPRESS` fixed to `1`) |
| `curl_posthoc_analysis.sbatch` | Single-node-count job: 2-phase posthoc curl+magnitude |
| `curl_hdf5_analysis.sbatch` | Single-node-count job: 2-phase HDF5 baseline curl+magnitude |
| `curl_eager_analysis_run.sh` | Submits chained `curl_eager_analysis.sbatch` jobs, one per node count |
| `curl_eager_compress_analysis_run.sh` | Submits chained `curl_eager_compress_analysis.sbatch` jobs, one per node count |
| `curl_posthoc_analysis_run.sh` | Submits chained `curl_posthoc_analysis.sbatch` jobs, one per node count |
| `curl_hdf5_analysis_run.sh` | Submits chained `curl_hdf5_analysis.sbatch` jobs, one per node count |
| `plot_curl_totals.py` | Stacked-bar + line-graph comparison of total workload time across eager / eager (compressed) / posthoc / HDF5, reconstructed from this directory's `results_curl_<mode>_*.csv` files |

## Result CSV schemas

Eager (`results_curl_eager_<jobid>.csv`) and eager-compressed
(`results_curl_eager_compress_<jobid>.csv`), one row per timestep,
straight from `bench_curl_eager`'s own output (the compressed variant's
`mode` field is relabeled `curl_eager_compress` by
`curl_eager_compress_analysis.sbatch`, everything else identical):
```
mode,step,n_ranks,nx,ny,nz_per_rank,compress,setup_s,write_s,confirm_read_s,total_s,bad
```
`setup_s` is the one-time session setup cost, repeated on every row.
`total_s` as printed includes `confirm_read_s` (the post-write
confirmation read -- see `bench_curl_eager.c`); `plot_curl_totals.py`
deliberately excludes it from the reconstructed total it plots, since
it's a correctness check for this benchmark, not part of the workload
being timed (same reasoning as `analysis_scripts/plot_totals.py`'s
`confirm_read_s` exclusion).

Posthoc (`results_curl_posthoc_<jobid>.csv`), one row per timestep,
pairing write-phase step *N* with analyze-phase step *N* and folding in
the measured relaunch cost:
```
mode,step,n_ranks,nx,ny,nz_per_rank,compress,write_setup_s,write_s,relaunch_s,analyze_setup_s,readback_s,curl_compute_s,curl_writeback_s,magnitude_compute_s,magnitude_writeback_s,total_s,bad
```
`relaunch_s` is wall-clock time across the close+restart cycle
(`srun_close_server.sh` + `srun_server_restart.sh`) between the write
and analyze phases -- since the relaunch happens once per job rather
than once per timestep, the same `relaunch_s` value is repeated on
every timestep's row, same as `write_setup_s`/`analyze_setup_s`.
`total_s` sums every phase's cost including the relaunch.
`curl_compute_s`/`curl_writeback_s` and
`magnitude_compute_s`/`magnitude_writeback_s` are both measured within
the single analyze phase (see `bench_curl_analyze.c`) -- there's no
separate relaunch between computing curl and computing magnitude from
it, since both happen in the same already-running process.

HDF5 baseline (`results_curl_hdf5_<jobid>.csv`), one row per timestep,
pairing write-phase step *N* with analyze-phase step *N* -- no relaunch
column, since there's no server to close/restart, just the file
reopened fresh for the analyze phase:
```
mode,step,n_ranks,nx,ny,nz_per_rank,write_setup_s,write_s,analyze_setup_s,readback_s,curl_compute_s,curl_writeback_s,magnitude_compute_s,magnitude_writeback_s,total_s,bad
```

All four `.sbatch` scripts append two more columns after their own
schema above: `avg_close_s,total_with_close_s`, the same convention
`analysis_scripts/`'s magnitude benchmark uses (see that README's
"Result CSV schema" section for the full rationale). `close_server`
checkpoints every server's in-memory metadata to disk before exiting,
which is also what actually flushes PDC's server-side region cache (up
to 64GB by default -- see `src/server/pdc_server_region/pdc_server_region_cache.c`)
to the storage backend: a write can return once its bytes land in that
cache, well before they're durably persisted, so `total_s` alone can
make eager/posthoc look artificially cheap for any run whose data
fits in cache. Each PDC-backed script greps its own
`close_server_<tag>_<N>.log` for "total close time", averages across
server ranks, and appends it as `avg_close_s` plus `total_with_close_s`
(`total_s` + `avg_close_s`). `results_curl_hdf5_*.csv` always has
`avg_close_s = 0` (no server to close, and HDF5's `H5Dwrite` here goes
straight through MPI-IO with no comparable cache to hide behind), so
`total_with_close_s` there equals `total_s` -- and all curl CSVs end up
directly comparable on that last column.

### Data size validation

Each `.sbatch` script also sanity-checks the data volume that actually
reached storage against what `N_TIMESTEPS x NX x NY x NZ_PER_RANK x`
(client ranks) should produce -- u/v/w (float32) plus curl_x/y/z and
vorticity_magnitude (float64), all persisted once per timestep, the
same formula `plot_curl_totals.py`'s `aggregate()` uses for the x-axis
data-size label. For the PDC-backed scripts this measures
`$PDC_DATA_LOC/pdc_data` (where object data actually lives on disk --
see `pdc_server_data.c`'s `storage_location`) with `du -sb`, taken
*after* the job's last `close_server` call so the region cache has
actually flushed; for the HDF5 baseline it reads the output file's size
directly. This exists because `bad=0` alone only checks values read
back *through* PDC/HDF5 -- it can't catch a truncated write, a stale
data directory left over from a previous run, or a region that was
never flushed at all. Uncompressed runs get a hard `[0.95, 1.05]`
tolerance check (`[0.95, 1.15]` for HDF5, to allow for its own
per-dataset/chunk metadata overhead) printed as `OK`/`WARNING` in the
job log; `COMPRESS=1` runs just report the expected-vs-actual numbers
without a tolerance check, since a smaller actual size is the whole
point of compression.

## Usage on Perlmutter

Build the binaries first (see `hdf5_analysis_test/README.md` for the
HDF5 side -- needs `module load cray-hdf5-parallel` before `make`):

```
cd build && make bench_curl_eager bench_curl_write bench_curl_analyze
cd ../hdf5_analysis_test && module load cray-hdf5-parallel && make
```

Then submit:

```
cd pdc_helper_scripts/curl_analysis_scripts
./curl_eager_analysis_run.sh           # submits chained jobs, one per node count
./curl_eager_compress_analysis_run.sh  # same, for the dedicated GPU-compressed eager variant
./curl_posthoc_analysis_run.sh         # same, for the 2-phase PDC posthoc variant
./curl_hdf5_analysis_run.sh            # same, for the 2-phase HDF5 baseline

COMPRESS=1 ./curl_posthoc_analysis_run.sh  # ad hoc compressed posthoc sweep (no HDF5 equivalent)
```

Each job defaults to `--account=m2621`; edit the `#SBATCH` header, or
export `SBATCH_ACCOUNT=<yours>`, if that allocation isn't yours.

Once results exist for whichever modes you ran, plot them:

```
python3 plot_curl_totals.py --results-dir . --out curl_totals_comparison.png
```
