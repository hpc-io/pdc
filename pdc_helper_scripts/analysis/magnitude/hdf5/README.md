# analysis/magnitude/hdf5

Parallel-HDF5 baseline for the magnitude analysis benchmark, standing in for
the "posthoc" strategy compared against in `src/tests/analysis/` (there is no
in-flight transform framework in plain HDF5, so posthoc -- write, read back,
compute client-side, write the result back -- is the only strategy that
applies). Same problem size and per-rank hyperslab decomposition as the PDC
benchmark, so results are directly comparable.

Two separate binaries, meant to run as two separate job steps with every
rank from the first fully exiting before the second launches -- a real
posthoc workflow writes data in one job and comes back to analyze it in a
later one, so measuring that relaunch cost matters as much as measuring
the I/O itself:

- `hdf5_bench_write` -- write vx/vy/vz into a fresh file, exit.
- `hdf5_bench_posthoc_analyze` -- reopen that file, read vx/vy/vz back,
  compute magnitude client-side, write it back as a fourth dataset.

## Build

Needs a parallel-HDF5-enabled MPI environment (`h5pcc` on `$PATH`, or MPI +
`libhdf5` reachable via `$HDF5_CFLAGS`/`$HDF5_LDFLAGS`):

```
module load cray-hdf5-parallel   # Perlmutter
make
```

## Run

```
./hdf5_bench_write <n_elem_per_rank> [out_file]
./hdf5_bench_posthoc_analyze <n_elem_per_rank> [out_file]
```

Each prints one CSV line from rank 0:

```
posthoc_write,n_client_ranks,n_elem,setup_s,write_s
posthoc_analyze,n_client_ranks,n_elem,setup_s,readback_s,compute_s,writeback_s,total_s,bad
```

See `../pdc/posthoc_hdf5.sbatch` for the Slurm job that runs both phases
as separate srun steps, combines their CSV lines into one row, and
sweeps rank counts at scale on Perlmutter.
