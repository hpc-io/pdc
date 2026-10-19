# analysis/magnitude/highfive

Single-session (not posthoc-split) magnitude benchmark using HighFive, a
C++ header-only wrapper over the same `libhdf5` `../hdf5/`'s plain-C
posthoc pair benchmarks. Exists to answer "does the wrapper cost
anything over the raw C API", not to introduce a different storage
engine -- HighFive calls into the identical `libhdf5`, same file format,
same MPI-IO path. Any measurable difference from `../hdf5/`'s combined
numbers is wrapper overhead, not a different I/O path.

Write vx/vy/vz, read them back, compute magnitude, write it back,
confirm-read it -- all in one process, matching
`../adios2/adios2_bench_magnitude.cpp`'s and PDC's own eager/lazy
benchmarks' single-session shape.

## Build

```
module load cray-hdf5-parallel   # Perlmutter
make
```

Clones HighFive's headers on demand into `.highfive_src/` (header-only,
no build step of its own).

## Run

```
./magnitude_highfive <n_elem_per_rank> [out_file]
```

Prints one CSV line per timestep (N_TIMESTEPS=3) from rank 0:

```
magnitude_highfive,step,n_ranks,n_elem,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,close_s,step_total_s,bad
```

See `./magnitude_highfive.sbatch` for the Slurm job (single srun
step -- no write/analyze split needed).
