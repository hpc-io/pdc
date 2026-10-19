# io/adios2_vpicio

ADIOS2 analogs of the raw VPIC-IO write/read-back throughput benchmarks
(`src/tests/misc/vpicio.c` / `bdcats.c`) -- no analysis or transformation
step, just the 8-field particle write/read shape, for a plain I/O
comparison point.

Build (needs ADIOS2 already installed -- see `../../adios2_build/README.md`):

```
ADIOS2_PREFIX=/path/to/adios2/install make
```

Run: `./adios2_vpicio` writes, then `./adios2_bdcats` (a separate
process, after the writer exits) reads it back -- see each `.cpp`'s own
header comment for the exact CSV schema.

Slurm (Perlmutter): `./vpicio_bdcats_run.sh` runs both as chained job
steps per node count. `common.sh` here is local to this directory only
(no PDC server involved, so no `SERVERS_PER_NODE`) -- override at submit
time, e.g. `SLEEPTIME=40 sbatch --nodes=4 vpicio_bdcats.sbatch`.
