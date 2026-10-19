# analysis_scripts

Slurm jobs and supporting bash scripts for the magnitude-analysis scale
study (eager DataFlyway vs. lazy vs. posthoc vs. eager_posthoc vs. plain
parallel HDF5), modeled after
`pdc_helper_scripts/vpicio_scripts/` (background server srun step, foreground
client srun step, graceful `close_server` shutdown, per-node-count job
chaining via `vpicio_scale_run.sh`).

Both posthoc benchmarks (PDC and HDF5) are genuinely two-phase: a write
job runs to completion and every client rank fully exits, then a separate
job relaunches to read the data back, compute magnitude, and write it
back. For PDC this also closes and restarts `pdc_server` in between (with
the `restart` argument, so it reloads its metadata checkpoint) rather than
leaving one server process running throughout. That relaunch is real
posthoc-workflow cost -- someone coming back later, in a new job, to
derive and persist a product from data someone else already wrote -- and
is why posthoc is expected to come out slower than eager, not something
the benchmark tries to hide by keeping one continuous session open the
way eager and lazy do.

`eager_posthoc` is a third variant of that same two-phase shape: phase 1
is byte-for-byte the same `bench_write_components` plain write as regular
posthoc (no DataFlyway graph at write time at all), but phase 2
(`bench_posthoc_analyze_eager`) attaches the eager `vector_magnitude`
graph fresh, in that new process, to the already-durable vx/vy/vz objects
plus a newly created magnitude object, and its one read of magnitude is
what triggers the server to compute+persist it -- via
`PDC_Server_data_io_region_analysis`'s read-triggered path (see
`src/server/analysis/pdc_an_server.c`), not the write-triggered path
`bench_magnitude.c`'s eager mode relies on (which never fires here, since
the graph didn't exist yet when the writes happened). This measures a
genuinely different use case from all three other PDC modes: attaching an
analysis pipeline to data after the fact, in a later session that had no
part in writing it, rather than planning for the pipeline (eager, lazy)
or hand-rolling the derived-value compute client-side (plain posthoc).
See `bench_posthoc_analyze_eager.c`'s header comment for why a leaf
input's own `materialized` flag (always freshly `false` when attached in
a new process) doesn't block this: `PDCan_exec_graph` reads a leaf's
bytes straight from its underlying PDC object regardless of that flag --
`materialized` only gates whether a state needs its *producer function*
re-run, and leaf inputs have none.

## Layout

| File | Role |
|---|---|
| `srun_server.sh` | Starts a fresh `pdc_server` (no prior data) in the background for one node-count step |
| `srun_server_restart.sh` | Restarts `pdc_server` with the `restart` argument, reloading the metadata checkpoint written on close |
| `srun_close_server.sh` | Gracefully shuts the server down via `close_server` (checkpoints metadata first) |
| `srun_client_eager.sh` | Runs `bench_magnitude eager`, appends a CSV row |
| `srun_client_lazy.sh` | Runs `bench_magnitude lazy`, appends a CSV row |
| `srun_client_posthoc_write.sh` | Posthoc phase 1 (shared by posthoc and eager_posthoc): runs `bench_write_components` (writes vx/vy/vz, exits) |
| `srun_client_posthoc_analyze.sh` | Posthoc phase 2: runs `bench_posthoc_analyze` (reads back, computes, writes back magnitude client-side) |
| `srun_client_posthoc_analyze_eager.sh` | eager_posthoc phase 2: runs `bench_posthoc_analyze_eager` (attaches the eager graph fresh, reads magnitude -- server computes+persists it) |
| `srun_hdf5_write.sh` | HDF5 posthoc phase 1: runs `hdf5_bench_write` (no PDC server) |
| `srun_hdf5_posthoc_analyze.sh` | HDF5 posthoc phase 2: runs `hdf5_bench_posthoc_analyze` (no PDC server) |
| `eager_pdc.sbatch` | Single-node-count job: PDC DataFlyway (eager) |
| `lazy_pdc.sbatch` | Single-node-count job: PDC lazy (read-triggered, in-session) |
| `posthoc_pdc.sbatch` | Single-node-count job: PDC post-hoc (write -> close/restart server -> analyze, hand-rolled client compute) |
| `eager_posthoc_pdc.sbatch` | Single-node-count job: PDC post-hoc with the eager graph attached fresh in the analyze phase instead of client-side compute |
| `posthoc_hdf5.sbatch` | Single-node-count job: plain parallel HDF5 baseline (write -> analyze) |
| `eager_pdc_run.sh` | Submits chained `eager_pdc.sbatch` jobs, one per node count |
| `lazy_pdc_run.sh` | Submits chained `lazy_pdc.sbatch` jobs, one per node count |
| `posthoc_pdc_run.sh` | Submits chained `posthoc_pdc.sbatch` jobs, one per node count |
| `eager_posthoc_pdc_run.sh` | Submits chained `eager_posthoc_pdc.sbatch` jobs, one per node count |
| `posthoc_hdf5_run.sh` | Submits chained `posthoc_hdf5.sbatch` jobs, one per node count |

Each `.sbatch` job runs **one** node count with 8 data servers/node and 32
client ranks/node (see "Server count vs. client count" below), and writes
one CSV row **per timestep** to `results_<mode>_<jobid>.csv` in this
directory: `bench_magnitude.c`, `bench_write_components.c` /
`bench_posthoc_analyze.c`, and `hdf5_bench_write.c` /
`hdf5_bench_posthoc_analyze.c` all repeat their write(+compute) cycle for
`N_TIMESTEPS = 3` timesteps within one session, matching the VPIC-IO/DLIO
convention elsewhere in this evaluation, and each timestep gets its own
row rather than only an aggregate. Each `_run.sh` wrapper submits its
`.sbatch` once per node count (see the `for nodes in ...` line at the top
of each `_run.sh`), chained with `--dependency=afterok` so they run one
after another rather than all competing for the account's allocation at
once.

### Result CSV schema

`eager_pdc.sbatch` and `lazy_pdc.sbatch` write the schema
`bench_magnitude.c` prints directly, one row per timestep:
`mode,step,n_ranks,n_elem,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,step_total_s,bad`
(eager leaves `readback_s`/`compute_s`/`writeback_s` at 0 -- that cost is
folded into `write_s`, since eager materializes magnitude during the
write itself; lazy's `write_s` covers only the plain vx/vy/vz write, with
`readback_s`/`compute_s` covering the first, computing read of magnitude
-- `writeback_s` stays 0 for lazy too, since that computed magnitude is
persisted server-side as part of the read, with no separate client write
call; `setup_s` is the one-time session setup cost, repeated on every
row).

`posthoc_pdc.sbatch`, `eager_posthoc_pdc.sbatch`, and
`posthoc_hdf5.sbatch` combine their two phases' own per-timestep CSV
lines into one row per timestep, pairing write-phase step *N* with
analyze-phase step *N*, with a different schema that makes the relaunch
cost visible instead of burying it:
`mode,step,n_ranks,n_elem,write_setup_s,write_s,relaunch_s,analyze_setup_s,readback_s,compute_s,writeback_s,total_s,bad`.
`relaunch_s` is the wall-clock time between the write phase's client
srun step returning and the analyze phase's client srun step starting --
for PDC that spans `srun_close_server.sh` + `srun_server_restart.sh`; for
HDF5 there's no server to restart, so it's always `0`. Since the relaunch
happens once per job rather than once per timestep, the same `relaunch_s`
value is repeated on every timestep's row. `total_s` sums every phase's
cost including `relaunch_s`. For `eager_posthoc_pdc.sbatch` specifically,
`compute_s` and `writeback_s` are always `0` -- the server does both,
inseparably, inside the timed `readback_s` (there's no separate
client-side compute step or client-issued write call the way plain
posthoc has -- see `bench_posthoc_analyze_eager.c`).

All five `.sbatch` scripts append two more columns after their own schema
above: `avg_close_s,total_with_close_s`. `close_server` (the
`PDC_Client_close_all_server` RPC each script's final
`srun_close_server.sh` call runs) checkpoints every server's in-memory
metadata to disk before exiting, and each server rank prints its own
"total close time = ..." line to that step's
`$BIN_DIR/close_server_<LOG_TAG>_<NUM_NODES>.log`. That checkpoint is a
real, PDC-specific durability cost with no equivalent in the HDF5
baseline (`H5Fclose` in `hdf5_bench_write.c` /
`hdf5_bench_posthoc_analyze.c` isn't even separately timed -- it comes
after the last per-timestep line is already logged, since closing a file
backed by already-durable collective MPI-IO writes is comparatively
free), and until this was added it was invisible in every results CSV --
so `step_total_s` / `total_s` alone made eager/lazy/posthoc look
artificially cheaper than the HDF5 baseline rather than genuinely
faster. Each script greps its own close_server log, averages the
per-server-rank close time, and appends it as `avg_close_s` plus
`total_with_close_s` (`step_total_s`/`total_s` + `avg_close_s`) on every
well-formed row -- `results_hdf5_*.csv` always has `avg_close_s = 0`
(no server to close) so `total_with_close_s` there equals `total_s`,
and all four CSVs end up directly comparable on that last column. Rows
using the `FAILED` sentinel format (5 fields for eager/lazy, `mode,step,
n_ranks,n_elem,FAILED` for posthoc/hdf5) are left untouched -- there's no
`step_total_s`/`total_s` to add to.

## Usage on Perlmutter

```
git clone <repo> && cd pdc
# build PDC (produces build/bin/pdc_server, bench_magnitude,
# bench_write_components, bench_posthoc_analyze,
# bench_posthoc_analyze_eager, close_server)
...
# build the HDF5 baseline (produces hdf5_bench_write, hdf5_bench_posthoc_analyze)
module load cray-hdf5-parallel
cd hdf5_analysis_test && make && cd ..

cd pdc_helper_scripts/analysis_scripts
./eager_pdc_run.sh          # submits chained jobs, one per node count
./lazy_pdc_run.sh           # submits chained jobs, one per node count
./posthoc_pdc_run.sh        # submits chained jobs, one per node count
./eager_posthoc_pdc_run.sh  # submits chained jobs, one per node count
./posthoc_hdf5_run.sh       # submits chained jobs, one per node count
```

Each job defaults to `--account=m2621`; edit the `#SBATCH` header, or export
`SBATCH_ACCOUNT=<yours>`, if that allocation isn't yours. `BIN_DIR` and
`N_ELEM` are overridable via the environment at submit time, e.g.:

```
N_ELEM=33554432 ./eager_pdc_run.sh
```

To run a single node count directly instead of the full sweep:

```
N_ELEM=33554432 sbatch --nodes=4 eager_pdc.sbatch
```

The default `N_ELEM=16777216` (64 MiB/rank of float32) is sized so that at
128 nodes * 32 ranks/node (4096 ranks) -- the largest scale point these
benchmarks are meant to reach -- total data written stays under 2 TB
(~1.25 TiB aggregate: 192 MiB/rank input + 128 MiB/rank magnitude output).
Scale `N_ELEM` up with care; it applies per rank, so aggregate write grows
with both `N_ELEM` and node count.

## Server count vs. client count

The magnitude benchmarks (`src/tests/analysis/bench_magnitude.c`,
`bench_write_components.c`, `bench_posthoc_analyze.c`,
`bench_posthoc_analyze_eager.c`) all use
`PDC_REGION_STATIC`, which splits each object's region across however many
data servers are running (`static_region_partition`), independent of how
many client ranks exist. Server count and client count don't need to
match for correctness -- 8 data servers/node and 32 client ranks/node is
just the deployment shape used here, same asymmetry as
`vpicio_scripts` (`DATA_SERVERS_PER_NODE=1`, `CLIENTS_PER_NODE=32`).
