# transformation/adios2_compression

ADIOS2 Operators analog of Data Flyway's TF (transformation framework)
compression/encryption benchmarks.

- `adios2_bench_compression` -- ZFP fixed-rate compression
  (`Variable::AddOperation("zfp", {{"rate","16"}})`), transparent on
  write and read, matching this project's own ZFP configuration
  (fixed-rate, 16 bits/value) and Data Flyway's TF attach-once model.
  Works correctly (`bad=0`).
- `adios2_bench_compression_encryption` -- ZFP chained with encryption
  via two `AddOperation` calls, using ADIOS2's own shipped
  `EncryptionOperator` plugin (`crypto_secretbox_easy`,
  XSalsa20-Poly1305 -- the same algorithm Data Flyway's own encryption
  transform uses; no custom code written). Real finding: BP5 (ADIOS2's
  default file engine) refuses more than one operator per variable
  ("BP5 does not support multiple operators"), so this only works with
  the older BP4 engine explicitly set (`io.SetEngine("BP4")`) -- itself
  evidence for "no composable transforms" in ADIOS2, not a workaround
  that erases the finding. Works correctly (`bad=0`) under BP4.

Build (needs ADIOS2 already installed -- see `../../adios2_build/README.md` --
plus ZFP; `./build_zfp.sh` here builds ZFP itself into `.zfp_build/` if
you don't already have it):

```
ADIOS2_PREFIX=/path/to/adios2/install make
```

Run:

```
./adios2_bench_compression <n_elem_per_rank> [out_file]
./adios2_bench_compression_encryption <n_elem_per_rank> [out_file]
```

Both print, per timestep: `adios2_zfp[_encrypt],step,n_client_ranks,n_elem,setup_s,write_s,confirm_read_s,step_total_s,bad`

Slurm (Perlmutter): `./compression_analysis_run.sh` /
`./compression_encryption_analysis_run.sh` (1-128 nodes,
N_ELEM=67,108,864 -- matches VPIC-IO's real per-rank byte count, see the
sbatch files' own comments).
