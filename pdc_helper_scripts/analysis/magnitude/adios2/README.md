# analysis/magnitude/adios2

ADIOS2 analog of Data Flyway's eager magnitude benchmark, via a genuine
ADIOS2 derived variable (`MAGNITUDE(vx,vy,vz)`, write-triggered inside
`EndStep()`). Works correctly (`bad=0`).

Build (needs ADIOS2 already installed -- see `../../../adios2_build/README.md`):

```
ADIOS2_PREFIX=/path/to/adios2/install make
```

Run:

```
./adios2_bench_magnitude <n_elem_per_rank> [out_file]
./adios2_verify_magnitude <n_elem_per_rank> [out_file]   # separate-process correctness check
```

Prints one CSV line per timestep (N_TIMESTEPS=3) from rank 0:

```
adios2_derived,step,n_client_ranks,n_elem,setup_s,write_s,confirm_read_s,step_total_s,bad
```

Slurm (Perlmutter): `./magnitude_analysis_run.sh` (1-128 nodes, mirrors
`../pdc/eager_pdc.sbatch`'s N_ELEM=16,777,216 default), or submit a
single node count directly, e.g. `sbatch --nodes=4
magnitude_analysis.sbatch`. `magnitude_analysis_everyonewrites.sbatch`
is the same benchmark with `ADIOS2_AGGREGATION_TYPE=EveryoneWrites`
forced (BP5's default is a "fewer ranks aggregate" strategy) -- see that
file's own comment.
