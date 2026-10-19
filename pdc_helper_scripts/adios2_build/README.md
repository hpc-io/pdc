# pdc_helper_scripts/adios2_build

Shared build scripts for third-party dependencies used by the ADIOS2
benchmarks under `pdc_helper_scripts/analysis/*/adios2/`,
`pdc_helper_scripts/transformation/adios2_compression/`, and
`pdc_helper_scripts/io/adios2_vpicio/` -- all four of those install
locations point at the *same* `ADIOS2_PREFIX`, so this only needs to run
once regardless of which category you're working on.

`./build_adios2.sh` clones ADIOS2 v2.12.1 (into `.adios2_src/`, skipped
if already present), configures it with exactly the feature set these
workloads need (MPI, Derived Variables, ZFP, Sodium; SST/UCX explicitly
off -- they caused an unrelated hang during development), builds,
installs, then builds all the `adios2_bench_*`/`adios2_vpicio`/
`adios2_bdcats` programs across every category above via each one's own
`make`. Needs zfp and libsodium already built and installed somewhere
findable by CMake -- `ZFP_PREFIX`/`SODIUM_PREFIX` env vars if not at this
script's defaults (`/mnt/fast/nlewis/workspace/install/{zfp,libsodium}`).
`ADIOS2_PREFIX` controls the install location (default
`/mnt/fast/nlewis/workspace/install/adios2`).

```
./build_adios2.sh
# or, to install somewhere else:
ADIOS2_PREFIX=$HOME/adios2-install ./build_adios2.sh
```

To rebuild just one category's programs against an ADIOS2 already
installed elsewhere, skip this script and run that category's own
Makefile directly:

```
cd ../analysis/magnitude/adios2 && ADIOS2_PREFIX=/path/to/adios2/install make
```

ZFP itself (needed only by `transformation/adios2_compression/`) has its
own `build_zfp.sh` living in that directory, not here.
