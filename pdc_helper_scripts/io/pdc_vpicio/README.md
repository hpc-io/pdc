# io/pdc_vpicio

Plain VPIC-IO write against PDC, no TF transform attached (`"raw"` in
`vpicio.c`'s `transformation_str` -- literally that value's own default).
The PDC half of a no-transformations PDC-vs-HDF5 comparison point;
`io/adios2_vpicio/` is the ADIOS2 analog (no server, no PDC involvement
at all), and `transformation/vpicio_zfp/` covers the ZFP-compressed
variants. `vpicio.c`/`vpicio_verify.c` live in `src/tests/misc/` (core
PDC source, built as part of the main PDC build -- nothing to build
locally here).

Run (Perlmutter): `sbatch vpicio_raw.sbatch` runs the full
`SERVER_SWEEP` (2/4/8 servers/node by default, see `common.sh`) as three
back-to-back workload runs within one job, PDC's data directory wiped
between them; `./vpicio_raw_run.sh` chains one job per node count (1-128)
on top of that. Override sizing or the sweep at submit time, e.g.:

```
NPARTICLES=16777216 sbatch --nodes=8 vpicio_raw.sbatch
SERVER_SWEEP="2 8" sbatch --nodes=8 vpicio_raw.sbatch
```

Output -- the combined `results_vpicio_raw_<jobid>.csv`, every srun log,
and copies of the per-step `build/bin` logs -- lands in
`./<MM_DD_YYYY>-<jobid>/`.

A matching plain-write HDF5 vpicio benchmark does not exist yet -- this
directory currently only covers the PDC side of that comparison.
