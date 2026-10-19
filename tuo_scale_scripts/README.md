# tuo_scale_scripts

Slurm-scheduled scaling study for `vpic_bdcats` (`src/tests/misc/vpic_bdcats.c`)
on Tuolumne, at 64 and 128 nodes.

## What `vpic_bdcats` does

Combines the existing `vpicio` (write) and `bdcats` (read) benchmarks into
one process: writes 5 timesteps of the standard 8-array VPIC-shaped
particle dataset (dX, dY, dZ, Ux, Uy, Uz, q, i), closing each timestep's
objects as it goes, then -- still in the same process, so the server's
region cache from the writes is still warm rather than dropped by a
server restart between two separate job steps -- reopens every one of
those same per-timestep objects by name and reads them back, verifying
every value matches what was written.

Two transfer modes (`argv[3]`):
- `sync` -- `PDCregion_transfer_start_all_mpi` immediately followed by
  `PDCregion_transfer_wait_all`, no overlap.
- `async` -- `sleep(sleeptime)` between start and wait, standing in for
  compute overlapped with in-flight I/O.

Every distinct PDC client API call is timed (`pdc_call_stats.h`'s
`PDC_TIMED` macro) and pooled into a mean/stdev/count across every rank.
One-time setup/teardown calls (`PDCinit`, container/property
create/close, ...) go in one pool (`api_call`); the write-phase loop and
read-phase loop each get their OWN pool (`api_call_write` /
`api_call_read`), even for call names both loops share
(`PDCregion_transfer_create`/`start_all_mpi`/`wait_all`/`close`,
`PDCobj_close`) -- a single shared pool would otherwise merge
write-phase and read-phase timings for those names into one
indistinguishable mean. Output is a CSV to stdout:

```
record_type,name,step,mean_s,stdev_s,count,value
api_call,PDCinit,,0.136790256,0.005833318,4,
api_call_write,PDCobj_create_mpi,,0.000043734,0.000028744,160,
api_call_write,PDCregion_transfer_wait_all,,0.000101202,0.000034552,20,
api_call_read,PDCobj_open_col,,0.000037066,0.000026051,160,
api_call_read,PDCregion_transfer_wait_all,,0.006377127,0.024424627,20,
...
throughput_write_MBps,,0,,,,134.091060
...
throughput_read_MBps,,0,,,,131.664722
...
total_data_size_bytes,,,,,,2621440
data_size_per_rank_bytes,,,,,,655360
data_dir_size_bytes,,,,,,2621440
```

- `api_call` rows: one per distinct one-time setup/teardown call name, not
  phase-specific.
- `api_call_write` / `api_call_read` rows: one per distinct call name made
  inside that phase's loop only, pooled across every rank and every
  occurrence within that phase (e.g. `api_call_write,PDCobj_create_mpi`
  has `count = nranks * 8 objects * steps`).
- `throughput_write_MBps` / `throughput_read_MBps`: one row per timestep,
  computed from the total bytes moved across all ranks that timestep
  divided by the observed I/O time for that timestep -- the whole step's
  wall-clock time, including metadata operations (object create/close,
  transfer create/close), since those are real per-timestep cost, not
  incidental setup to exclude. In async mode, `sleep(sleeptime)` between
  transfer start and wait is subtracted back out of that elapsed time:
  the client isn't waiting on I/O during that sleep, it's doing (simulated)
  compute while the transfer runs in the background, so counting that
  time against I/O throughput would be wrong. This means async throughput
  is expected to come out *higher* than sync's, not lower, despite async
  taking more wall-clock time per step overall.
- `total_data_size_bytes` / `data_size_per_rank_bytes`: what the client
  itself counted it moved, the whole run, one direction (write and read
  move the identical volume), across all ranks vs. one rank.
- `data_dir_size_bytes`: what actually landed on disk under
  `$PDC_DATA_LOC/pdc_data` (`du -sb`, checked in `vpic_bdcats.sbatch`
  after `close_server` flushes the region cache) -- an independent check
  against `total_data_size_bytes` that the client's own byte counting
  matches real storage, not just what PDC's in-memory cache reported.
  Expect this to run well above `total_data_size_bytes` at
  `run_small_scale.sh`'s tiny sizing (1024 particles/rank): each object
  is partitioned across all `SERVERS_PER_NODE * NUM_NODES` servers, and
  each per-server shard file carries a fixed storage overhead on top of
  its actual data (confirmed on a real 2-node run: ~4 KB of real data
  per shard file, ~42 KB actual file size) -- that overhead is roughly
  fixed per file, not proportional to data size, so it dominates at tiny
  particle counts and should become negligible at real sweep scale. The
  ratio-tolerance WARNING in `vpic_bdcats.sbatch` is non-fatal for
  exactly this reason; only an actually-empty directory fails the job.
  `vpic_bdcats.sbatch` fails the job outright if this is 0.

## Scheduler: Slurm (`sbatch`/`srun`), not Flux

This directory originally targeted Flux (`flux batch`/`flux run`/`flux
submit`), but real testing on Tuolumne showed the client's inner `flux
run` reliably hitting "waiting for resources" and never getting
co-scheduled alongside the backgrounded server job, even after reserving
a whole `--exclusive` node -- see git history (`vpic_bdcats_job.flux.sh`,
now removed) for the sequence of Flux-specific fixes that were tried.

`pdc_helper_scripts/curl_analysis_scripts` already runs the identical
concurrent-server-plus-client pattern via plain Slurm and has for a long
time: one `sbatch` allocation sized to the *combined* server + client
task count per node, a server `srun` step backgrounded with `&`, and a
client `srun` step in the foreground -- no special flag (no `--overlap`)
needed, because Slurm's default job-step semantics let multiple `srun`
steps within one `sbatch` allocation run concurrently as long as their
combined per-node task request doesn't exceed `--ntasks-per-node`. This
directory now mirrors that exact pattern instead of Flux's sub-instance
model, which doesn't have an equivalent default.

## Files

- `common.sh` -- shared config sourced by every script below: node-count
  list (64, 128 -- the top two points of the original 1..128
  progression), `SERVERS_PER_NODE` (4), `CLIENTS_PER_NODE` (32), `STEPS`
  (5), `NUMPARTICLES` -- computed once, held constant across the whole
  sweep (weak scaling), sized against a 128-node/4 TiB target -- since
  the sweep's top node count now matches `TOP_NODES`, **the 128-node
  step writes the full 4 TiB target**, and 64 nodes writes half that
  (~2 TiB) -- and
  `TASKS_PER_NODE`/`CPUS_PER_TASK`, derived from `CORES_PER_NODE`
  (default 96, matching Tuolumne's pbatch queue) so the single `sbatch`
  allocation is sized to fit server + client tasks without oversubscribing
  cores. Requires `PDC_DATA_LOC` to already be set to real parallel
  scratch -- see below.
- `vpic_bdcats.sbatch` -- the actual per-node-count, per-mode Slurm job:
  cleans out `PDC_DATA_LOC`/`PDC_TMPDIR` from any previous run first,
  starts `pdc_server` (backgrounded via `&`), runs `vpic_bdcats` in the
  foreground, closes the server, extracts that run's CSV. Delegates to
  `srun_server.sh` / `srun_client_vpic_bdcats.sh` / `srun_close_server.sh`
  (mirroring `pdc_helper_scripts/curl_analysis_scripts`'s file layout).
  Not invoked directly -- `--nodes`/`--ntasks-per-node`/`--cpus-per-task`
  are always overridden at submit time by the `run_*.sh` scripts below.
- `run_sync.sh` / `run_async.sh` -- sweep drivers, one per transfer mode,
  submitting `vpic_bdcats.sbatch` once per node count via `sbatch
  --dependency=afterok`, chained so each node count starts only after the
  previous one finishes (mirrors
  `pdc_helper_scripts/curl_analysis_scripts/curl_eager_analysis_run.sh`).
  These submit the whole chain and return immediately -- check progress
  with `squeue -u $USER`. **`run_async.sh`'s sleep duration
  (`SLEEP_TIME`) is declared at the very top of the file** -- edit it
  there.
- `run_small_scale.sh` -- a fast, 2-node, tiny-particle-count (1024/rank,
  2 steps) sanity check of the whole pipeline (sbatch submission, server
  startup, write+read-back, CSV extraction) for both modes, blocking via
  `sbatch --wait` so it can print each CSV immediately. 2 nodes rather
  than 1 specifically so it exercises cross-node communication -- some
  bugs (e.g. Tuolumne's ofi+tcp vs ofi+cxi transport mismatch) only show
  up once server and client actually have to talk across a node
  boundary. **Run this first**, before either sweep, to catch a bad
  `PDC_DATA_LOC` or a partition/account mismatch without waiting on a
  real sweep step or writing anywhere near the sweep's 4 TiB top.

## Required: `PDC_DATA_LOC`

At up to 4 TiB for a single run (the 128-node step), this **must** point
at real parallel scratch, not wherever `BIN_DIR` happens to sit.
`common.sh` requires it to already be set (it will refuse to run
otherwise):

```
export PDC_DATA_LOC=/p/lustre1/$USER/pdc_vpic_bdcats_scale
```

Every job first removes everything under `PDC_DATA_LOC` (and
`PDC_TMPDIR`, which defaults to the same path) before starting its own
server, so stale data from a previous run never lingers into the next.

## Build

Built as part of the normal PDC test suite (`-DBUILD_TESTING=ON`); the
binary lands at `<build>/bin/vpic_bdcats` alongside `pdc_server` and
`close_server`. `common.sh` auto-detects `BIN_DIR` as
`tuo_scale_scripts/../build/bin` -- override it if your build lives
elsewhere.

## Run

```
export PDC_DATA_LOC=/p/lustre1/$USER/pdc_vpic_bdcats_scale

./run_small_scale.sh   # sanity check first -- 2 nodes, seconds, both modes
./run_sync.sh          # full sweep, 64 and 128 nodes, sync mode
./run_async.sh         # full sweep, 64 and 128 nodes, async mode
```

`vpic_bdcats.sbatch`'s `--partition=pbatch` default matches Tuolumne's
queue name observed via `flux resource list`; add `--account=<bank>` to
the `sbatch` calls in `run_*.sh` if your site requires one.

Override any of `SERVERS_PER_NODE`, `CLIENTS_PER_NODE`, `STEPS`,
`NUMPARTICLES`, `CORES_PER_NODE` as env vars before running (see
`common.sh` for defaults).

Each node count writes `vpic_bdcats_<mode>_<nodes>.csv` to `$RESULTS_DIR`,
tagged with a trailing `# n_nodes=...,servers_per_node=...,...` comment
line recording that run's shape.

The benchmark binary and CSV pipeline (`vpic_bdcats`, `pdc_call_stats.h`)
are fully tested locally against a real `pdc_server` over plain MPI.
`run_small_scale.sh` getting a clean CSV on your first try is the signal
that the Slurm layer itself is working end to end on Tuolumne -- run it
again after any Slurm-related change before trusting the full sweep.

## Plotting: `plot_vpic_bdcats.py`

Reads every `vpic_bdcats_<mode>_<nodes>.csv` under the given directories
(one directory per sweep, e.g. `results_sync_<timestamp>/` and
`results_async_<timestamp>/`) and produces six PNGs. write and read are
ALWAYS separate figures -- nothing combines the two directions:

```
python3 plot_vpic_bdcats.py results_sync_<timestamp> results_async_<timestamp> -o plots/
```

- `write_stacked_bar.png` / `read_stacked_bar.png` -- sync/async bars per
  node count, each stacked by PDC operation (object create/open, transfer
  create, transfer start_all_mpi, transfer wait_all, transfer close,
  object close) -- fill = operation, edge = mode. Requires the
  `api_call_write`/`api_call_read` CSV rows (`vpic_bdcats.c`); older CSVs
  with only pooled `api_call` rows can't produce these two and are
  skipped with a message rather than erroring. Each segment is `mean_s *
  count / ranks`, an ESTIMATE of one rank's time in that operation, not
  a wall-clock measurement -- it won't exactly sum to
  `write_total_time.png`/`read_total_time.png`'s values, since those come
  from an actual barrier-bounded window and this doesn't capture
  cross-rank synchronization/imbalance.
- `write_total_time.png` / `read_total_time.png` -- total observed I/O
  time vs scale, one line per mode -- the actual barrier-bounded
  wall-clock measurement (the whole step's time including metadata ops,
  minus `sleep(sleeptime)` for async), not the per-operation estimate the
  stacked bars above are built from.
- `throughput_write.png` / `throughput_read.png` -- aggregate throughput
  (total bytes moved / total observed time, not a mean of per-step
  rates) vs scale, one line per mode, one figure per direction.

Every value is read from each CSV's own trailing `# key=value,...`
metadata comment rather than parsed from the filename or recomputed from
a hardcoded bytes-per-particle constant, so it stays correct even if
`common.sh`'s sizing changes. Requires `numpy` and `matplotlib` (no
pandas), matching `pdc_helper_scripts/*/plot_*totals.py`'s convention.
