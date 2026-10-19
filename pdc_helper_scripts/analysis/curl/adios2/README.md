# analysis/curl/adios2

ADIOS2 analog of Data Flyway's curl+vorticity-magnitude benchmark. ADIOS2
does have a native `CURL` expression operator (same central-difference
scheme as `curl_math.h`), and `MAGNITUDE(CURL(u,v,w))` is a valid nested
expression that *should* reproduce Data Flyway's two-stage
curl_vorticity_magnitude graph in one derived-variable declaration -- but
it doesn't work in the ADIOS2 build this was developed against (v2.12.1):
the written `CURL` output is silently all-zero (confirmed via `bpls` and
a minimal, hand-verifiable single-rank reproduction), while the
shape-preserving `MAGNITUDE` operator works fine. Looks like a real
upstream bug specific to derived variables whose output shape differs
from their inputs' (`CURL` appends a trailing size-3 dimension;
`MAGNITUDE` doesn't) -- see `adios2_bench_curl.cpp`'s own header comment
for the full writeup. This workload therefore falls back to client-side
compute (read u/v/w back, `curl_math.h`, write vorticity_magnitude back)
-- Data Flyway's own *posthoc* strategy, still a meaningful comparison
point, just not the eager one originally intended.

Build (needs ADIOS2 already installed -- see `../../../adios2_build/README.md`):

```
ADIOS2_PREFIX=/path/to/adios2/install make
```

Run: `./adios2_bench_curl <nx> <ny> <nz_per_rank> [out_file]`

```
adios2_posthoc_curl,step,n_client_ranks,nx,ny,nz_per_rank,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,step_total_s,bad
```

Slurm (Perlmutter): `./curl_analysis_run.sh` (1-128 nodes, mirrors
`../pdc/curl_eager_analysis.sbatch`'s NX=256/NY=256/NZ_PER_RANK=64
default).
