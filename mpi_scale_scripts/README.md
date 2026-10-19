# mpi_scale_scripts

Isolates the MPI collective communication pattern of
`bench_curl_eager`'s object-setup phase (container + per-timestep object
creation) so its own scaling can be measured directly, without a PDC
server, Mercury RPCs, or any storage I/O in the way.

## Why

`src/tests/analysis/bench_curl_eager.c` (the curl/vorticity-magnitude
eager benchmark, see `../pdc_helper_scripts/curl_analysis_scripts/`)
creates one shared container and, per timestep, 7 objects (u, v, w,
curl_x, curl_y, curl_z, vorticity_magnitude) via `PDCcont_create_col` /
`PDCobj_create_mpi`. Both are collective: one rank performs the actual
create RPC against the PDC server, then broadcasts the result to every
other rank. As client rank counts grow (this repo's PDC benchmarks scale
up to 128 nodes x 32 ranks/node), it's useful to know how much of that
setup cost is inherent to the MPI broadcast pattern itself, independent
of server/network behavior.

## What's reproduced

Read directly off the client library, not guessed:

- `PDCcont_create_col` (`src/api/pdc_obj/pdc_cont.c`) ->
  `PDC_Client_create_cont_id_mpi` (`src/api/pdc_client_connect.c`): rank 0
  does the RPC, then `MPI_Bcast(&cont_id, 1, MPI_LONG_LONG, 0, comm)`.
  Once, before the per-timestep loop.
- `PDCobj_create_mpi` (`src/api/pdc_obj/pdc_mpi.c`), called once per
  object with `rank_id=0` (matching `bench_curl_eager.c`'s call sites):
  rank 0 does the RPC, then four broadcasts --
  `meta_id` (`MPI_LONG_LONG`), `metadata_server_id` (`MPI_UINT32_T`),
  `data_server_id` (`MPI_UINT32_T`), `region_partition` (`MPI_UINT8_T`).
  7 objects/timestep x 4 broadcasts = 28 broadcasts/timestep.

The RPC itself isn't reproduced (there's no server here) -- only the MPI
collectives surrounding it.

## Build

```
make            # portable default (mpicc)
make CC=cc      # Perlmutter -- Cray compiler wrapper, cray-mpich's OFI
                # netmod picks the cxi provider automatically over
                # Slingshot-11; a plain mpicc build may instead pick up a
                # generic MPICH with no cxi support
```

Standalone -- no dependency on the rest of the PDC build tree.
`mpi_object_setup_scale.sbatch` also sets `FI_PROVIDER=cxi` before the
srun step, so a run fails loudly instead of silently falling back to a
slower provider if cxi isn't picked up for some reason.

## Run

Single node count:

```
sbatch --nodes=4 mpi_object_setup_scale.sbatch
```

Full sweep, 1 to 128 nodes (32 ranks/node, doubling: 1, 2, 4, 8, 16, 32,
64, 128 -- so 32 up to 4096 ranks), chained one job at a time via
`--dependency=afterok`, mirroring `curl_analysis_scripts/*_run.sh`:

```
./run_scale.sh
```

Each job writes `results_mpi_object_setup_scale_<jobid>.csv`
(`phase,step,n_ranks,setup_s` -- one `cont_create` row plus one
`object_create` row per timestep); `cat` them together to see the full
sweep.
