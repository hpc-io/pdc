# analysis/curl/hdf5

Parallel-HDF5 baseline for the curl+vorticity-magnitude benchmark, the
same two-phase posthoc shape as `../../magnitude/hdf5/`: no PDC server,
so the file is just reopened fresh for the analyze phase instead of a
server being closed and restarted. Uses the identical `curl_math.h`
kernel as the PDC and ADIOS2 sides (see this directory's own Makefile),
so results are directly comparable.

- `hdf5_bench_curl_write` -- write u/v/w into a fresh file, exit.
- `hdf5_bench_curl_analyze` -- reopen that file, read u/v/w back,
  compute curl then vorticity magnitude client-side, write both back.

## Build

```
module load cray-hdf5-parallel   # Perlmutter
make
```

## Run

```
./hdf5_bench_curl_write <nx> <ny> <nz_per_rank> [out_file]
./hdf5_bench_curl_analyze <nx> <ny> <nz_per_rank> [out_file]
```

See `../pdc/curl_hdf5_analysis.sbatch` for the Slurm job that runs both
phases as separate srun steps and combines their CSV lines into one row.
