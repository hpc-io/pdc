# PDC first-epoch read benchmark

[Performance results](PERFORMANCE.md) covers batch-16 PDC with batched reads on
server workers and the measured core allocations. The workload is a synthetic
C reader over 100 MiB records with deterministic data and shuffle order. It
tags each payload with its record index so verification detects a record
returned to the wrong request. It measures the first reader epoch, including
PDC object opens, transfer starts,
waits and request cleanup. Dataset generation, writeback, and the separate
full-byte verification run outside that timer.

## Build on Perlmutter

Use a compatible Cray MPI environment, a shared Mercury 2.4.1 installation
with the OFI CXI provider, and the Cray HDF5 module. The measurements used
Cray `cc` 2.7.36, libfabric 2.3.1, HDF5 1.14.3.9, and a `RelWithDebInfo` PDC
build with MPI, server cache, and server multithreading enabled. The Mercury
build enabled `NA_USE_OFI` and disabled `NA_USE_SM` and Mercury checksums.
Set `MERCURY_LIBDIR` or `HDF5_LIBDIR` if the installations use `lib64`.

Build from the repository root.

```bash
module load cray-hdf5
export CRAYPE_LINK_TYPE=dynamic
export MERCURY_PREFIX=/path/to/mercury-2.4.1
export LD_LIBRARY_PATH="$MERCURY_PREFIX/lib:${LD_LIBRARY_PATH:-}"
benchmark/pdc_read/build.sh "$PWD/build-pdc-read"
```

`CC` and `CMAKE` select the compiler wrapper and CMake. Set `CC` to the
Cray MPI compiler wrapper if your shell resolves `cc` to a system compiler.
Additional arguments to `build.sh` are PDC CMake options. For example, a serial server build can
pass `-DPDC_ENABLE_MULTITHREAD=OFF` and use worker count 0 in the run layout.
For a small functional check, set `DIM1=1024 DIM2=1024` at build time and run
with `--files 64`. Use the default dimensions for the performance measurements.

The generator and HDF5 reference workloads were adapted from the [SC26 I/O
artifact](https://github.com/hyunjuooh/sc26-artifacts/tree/4b485f75381ef1b9dc10b68fac5029b10a4be1bf/pytorch_training_io_simulation).
The sources here retain the [artifact license](LICENSE.artifact). The
reproducer builds all clients from the selected checkout. In the measured
runs, dataset generation used the previously built PDC client with the same
generator source and object layout. Generation and the writeback barrier were
outside the read timer.

## Use CXI for PDC tests and debugging

The benchmark uses `ofi+cxi` for all PDC ranks. Use the same Perlmutter rank
launcher for MPI CTests and manual runs. Build with the CXI-compatible Mercury
installation described above.

For MPI CTests, configure PDC with these additional CMake options. Set
`MERCURY_HOME` to the Mercury installation and load Cray MPI before configuration.

```bash
export MERCURY_HOME="$MERCURY_PREFIX"
cmake -S . -B build-pdc-tests -DCMAKE_C_COMPILER=cc \
    -DBUILD_TESTING=ON -DBUILD_MPI_TESTING=ON -DPDC_ENABLE_MPI=ON \
    -DPDC_ENABLE_MULTITHREAD=ON \
    -DMPI_RUN_CMD="$PWD/scripts/perlmutter/pdc-mpi-step.sh"
cmake --build build-pdc-tests --parallel 2
```

Use a single-node Perlmutter compute allocation with
`--network=job_vni,single_node_vni` and enough cores for the overlapping server
and client steps. The MPI launcher reserves separate cores for each step and
requests four cores per server rank and two per client rank. Set `PDC_TEST_SERVER_CPUS` for a different server CPU layout.
Leave `SUPERCOMPUTER` unset. Run tests sequentially because they reuse the same
endpoint ranges and working files.

```bash
export PDC_SERVER_NTHREAD=4
SUPERCOMPUTER= ctest --test-dir build-pdc-tests --parallel 1 --output-on-failure \
    -R '^(cont_tags_mpi|region_transfer_write_read_mpi)$'
```

For a manual run, invoke the rank launcher inside each `srun` step. Use the
`server` role for `pdc_server` and the `client` role for the application, a
debugger running the application, or `close_server`. Start those steps in the
same job with `--network=job_vni,single_node_vni` on each step. The single-node
option requests a Slingshot service even when a step runs on only one node.
The launcher derives the endpoint and authorization from the rank's Slurm
environment. For example, start a single server and run a one-rank client under
a debugger inside the same allocation.

```bash
rank_launcher="$PWD/scripts/perlmutter/pdc-rank.sh"
srun --exclusive --exact --network=job_vni,single_node_vni -N1 -n1 -c4 \
    "$rank_launcher" server "$PDC_BIN/pdc_server" &
srun --exclusive --exact --network=job_vni,single_node_vni -N1 -n1 -c2 --pty \
    "$rank_launcher" client gdb --args "$PDC_BIN/pdc_init"
srun --exclusive --exact --network=job_vni,single_node_vni -N1 -n1 -c2 \
    "$rank_launcher" client "$PDC_BIN/close_server"
wait
```

Here `PDC_BIN` is the directory containing the server, `close_server`, and the
built test client. Run all three steps from the same working directory.

To isolate a transport problem on one node, explicitly set
`HG_TRANSPORT=ofi+tcp_rxm HG_HOST=127.0.0.1` when starting CTest. The same
MPI launcher will select TCP for its PDC ranks. Loopback is only usable when
all ranks are on the same node. Slurm must be able to provision CXI for
concurrent server and client steps. An `Error configuring interconnect` from
`srun` means the affected rank never started. Record the job ID and step error
when diagnosing that failure.

## Reproduce the core sweep

Request a four-node CPU allocation. This example uses one shared job VNI and
four ranks per node with access to a full CPU node for explicit binding. Set
an account eligible for your allocation and keep the nodes dedicated to the benchmark.

```bash
salloc --account="$PDC_ACCOUNT" --constraint=cpu --qos=interactive \
    --nodes=4 --ntasks-per-node=4 --cpus-per-task=32 --hint=nomultithread \
    --network=job_vni --time=03:00:00
```

Inside that allocation, use fresh output and scratch directories. One full-size
dataset uses 328.125 GiB for record data. Allow headroom for metadata and verify
your scratch quota before launching.

```bash
python3 benchmark/pdc_read/run.py --build "$PWD/build-pdc-read" \
    --scratch "$SCRATCH/pdc-core-sweep" --output "$PWD/pdc-core-sweep" \
    --layout 4:2:1 --layout 1:4:2 --layout 2:2:1 \
    --layout 4:4:2 --layout 1:2:1 --layout 2:4:2 --passes 3 --flush-timeout 1800
```

A layout is `SERVERS_PER_NODE:RPC_WORKERS_PER_SERVER:PHYSICAL_CORES_PER_SERVER`.
`hdf5` selects the HDF5 reader. The default file count is 840 records per node.
Use `--files` for a different total, divisible by the number of reader ranks.
There are four reader ranks per node. Each case starts with a new dataset,
and each pass starts a new reader process. Later passes may benefit from OS
cache state. The runner uses the same physical reader cores for every case,
pins server ranks to separate cores, and verifies the resulting affinity after
MPI initialization. A server retains both hardware threads of each assigned
physical core. Per-node PDC cache capacity is held at 64 GiB. The threaded
test datasets are disposable and skip restart checkpoint creation at shutdown.
The read timing ends before the close step. After full-byte verification, the
runner attempts an orderly close. If that step reaches its deadline, the
runner stops its own benchmark steps, verifies they ended, and records
the shutdown status separately from the read time.

Verified trial datasets are removed on success. Use `--keep-data` to retain
them. The output directory includes the exact commands, CPU topology, logs,
per-case acceptance, and `summary.json`. The runner requires an existing job
owned by the caller. It submits no new allocations.

## Reproduce the full-core comparison

Use fixed binding for a matched comparison with the same reader cores in
both modes. Three fresh rounds give one timed pass per independent dataset.
Use `--binding slurm` to reproduce historical automatic placement.

```bash
python3 benchmark/pdc_read/run.py --build "$PWD/build-pdc-read" \
    --scratch "$SCRATCH/pdc-full-cores" --output "$PWD/pdc-full-cores" \
    --binding fixed --layout 4:4:8 --layout hdf5 --rounds 3
```

For threaded depth one, run the same `4:4:8` layout with
`--depth 1 --mode each` and fresh directories. For the batch-16 Mercury
dispatch control, build the pinned PDC revision with the benchmark scripts in
this checkout. Use the same dependencies and record dimensions for both builds.

```bash
git worktree add --detach "$PWD/pdc-dispatch" d34881d5c07877da513ce45a0fac232235aa831c
PDC_SOURCE="$PWD/pdc-dispatch" benchmark/pdc_read/build.sh "$PWD/build-pdc-dispatch"

# Run inside the same allocation with fresh scratch and output directories.
python3 benchmark/pdc_read/run.py --build "$PWD/build-pdc-dispatch" \
    --scratch "$SCRATCH/pdc-dispatch-control" --output "$PWD/pdc-dispatch-control" \
    --binding fixed --layout 4:4:8 --rounds 3
```

Keep the PDC cache, VNI, client layout, and file count fixed across controls.
This comparison uses the historical PDC revision, so it includes other
transfer changes made since that revision.

For scaling runs, use a homogeneous Perlmutter CPU allocation with four
readers per node and the site-supported VNI mode. The default 840 records per
node gives a weak-scaling workload. Use a fixed `--files` across allocations
to compare time to read the same dataset.
