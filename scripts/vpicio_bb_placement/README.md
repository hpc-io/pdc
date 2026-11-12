# VPICIO server-placement comparison (El Capitan / Tuolumne, Flux)

Two Flux job scripts for the same VPICIO workload, differing only in
where the PDC server rank runs, to compare the traditional
server-colocated-with-compute deployment against moving the server onto
a Rabbit's own CPU:

- `job1_compute_oversubscribed.flux.sh` -- one compute node. One VPICIO
  client rank per user core, plus one PDC server rank sharing a core
  with a client (the node runs one slot oversubscribed).
- `job2_compute_plus_rabbit.flux.sh` -- one compute node plus one Rabbit
  attached to its chassis. Same client layout, but the PDC server runs
  on the Rabbit's own CPU instead of the compute node, so it never
  contends with a client for a core.
- `small_job2_compute_plus_rabbit.flux.sh` -- same placement as job 2,
  but only 2 client ranks instead of 84 (still 8 objects, so 16 regions
  / ~512 MB instead of 672 regions / ~21 GB). For quickly iterating on
  the close-time flush hang seen on the full-size job 2 without waiting
  on an 84-rank run each time.
- `job1_2node_oversubscribed.flux.sh` / `job2_2node_rabbit.flux.sh`,
  `job1_4node_oversubscribed.flux.sh` / `job2_4node_rabbit.flux.sh`,
  `job1_8node_oversubscribed.flux.sh` / `job2_8node_rabbit.flux.sh`,
  `job1_16node_oversubscribed.flux.sh` / `job2_16node_rabbit.flux.sh` --
  N-node scale-outs of job 1/job 2 for N in {2, 4, 8, 16}: one
  distributed PDC server (N ranks total) either spread 1-per-node
  across the same N compute nodes as the clients (oversubscribed,
  scaled-out job 1), or all N ranks consolidated onto a single Rabbit
  chassis (2*N reserved cores, 2 cores/rank; scaled-out job 2).
  **Unverified** -- see the caveats at the top of each script before
  running; in particular, the NPARTICLES/STEPS defaults imply
  proportionally large total writes at these node counts (~3.3 TB at
  16 nodes, scaling down to ~0.4 TB at 2 nodes), so test with much
  smaller NPARTICLES first.

All of job1/job2 assume `pdc_server`, `vpicio`, and `close_server` are
already built (edit `EXECPATH` near the top of each script) and submit
with:

```
flux batch job1_compute_oversubscribed.flux.sh
flux batch job2_compute_plus_rabbit.flux.sh
flux batch small_job2_compute_plus_rabbit.flux.sh
flux batch job1_2node_oversubscribed.flux.sh
flux batch job2_2node_rabbit.flux.sh
flux batch job1_4node_oversubscribed.flux.sh
flux batch job2_4node_rabbit.flux.sh
flux batch job1_8node_oversubscribed.flux.sh
flux batch job2_8node_rabbit.flux.sh
flux batch job1_16node_oversubscribed.flux.sh
flux batch job2_16node_rabbit.flux.sh
```

All of job1/job2 now also run VPICIO with 10 steps and 5s of real CPU
work (not `sleep()`) between non-final steps, via `vpicio.c`'s
`hard_compute()` -- see its usage comment for the `#particles #steps
compute_time` CLI args. This is what actually lets job 1's oversubscribed
core show contention: sleeping never contends for a core no matter how a
rank is placed.

## What's confirmed (two real runs on Tuolumne)

All of the following comes from two actual `flux batch` runs of
`explore_rabbit_cores.flux.sh` on Tuolumne, 2026-10-30 (jobs
`f3h9kPZRAK7u` and `f3h9rpjGmLEK`):

- `#DW jobdw` Rabbit storage works exactly as documented: mounted at
  `/mnt/nnf/...`, 1.3-1.4 GiB/s for a 1GiB write+fsync, served by a
  rabbit host genuinely separate from the compute node.
- A `flux batch` job runs in its own private Flux sub-instance relative
  to the system instance -- `flux getrabbit` and other chassis/rabbit
  queries need `flux --parent ...` to reach the system instance and
  see anything. True of every batch job, not specific to nesting one
  inside another.
- `--coral2-rabbit-cores` **alone is rejected** (`[Errno 19]
  Unsatisfiable request`); it's only accepted bundled with a `#DW
  jobdw` directive in the *same* request.
- **Decisive fingerprint check, run for real (not submitted-and-
  cancelled) in the second run**: a job submitted with
  `--coral2-chassis=1 --coral2-rabbit-cores=1` plus a `#DW` directive
  got both a compute node AND a rabbit core, and running a plain
  command against that allocation executed it on *both*: the compute
  node (`AMD Instinct MI300A Accelerator`, 168 threads = 84 user cores
  x2 HW threads, matching job 1's node layout) and, on a different
  host entirely, the rabbit's own reserved core (`AMD EPYC 7713
  64-Core Processor`, 2 threads). Different host, different CPU vendor
  and model -- this is a real, separate piece of hardware, not a
  resource-accounting fiction.

`job2_compute_plus_rabbit.flux.sh` is written against this confirmed
behavior: `--coral2-chassis`/`--coral2-rabbit-cores` live directly in
its own `#FLUX:` header next to `-S dw=...` (so the chassis+rabbit-cores
+DW bundling requirement above is satisfied by the job's own top-level
request, no nested reservation needed), and it uses `flux --parent
getrabbit` to learn which host that reserved core landed on before
targeting the server there with `--requires=host:`.

## Fabric transport (ofi+cxi) and per-rank NIC pinning

`job2_2node_rabbit` run `f3hXYoGSHsoV` showed all 168 client ranks
failing `NA_Initialize_opt()` immediately with the default `ofi+tcp`
("`cxi`" printed as the available provider), so every multi-node
script (`job1`/`job2` at 2/4/8/16 nodes) now forces `HG_TRANSPORT=ofi+cxi`.

The next run (`f3hXbV6Hdegw`) then had both rabbit-side server ranks
fail `HG_Init()` immediately under `ofi+cxi`. `probe_fabric.flux.sh`
(see below) showed the rabbit and compute nodes actually have
identical libfabric provider sets (`cxi`, `tcp`, etc. on both) with the
rabbit chassis exposing 2 Cassini NICs -- so it isn't a missing
provider. `pdc_server.c`/`pdc_client_connect.c` give every rank an
independent port (`rank % 32 + 7000`) but build an identical
`HG_HOST`/`HG_TRANSPORT` address string across ranks, with no
NIC-selection logic at all -- so multiple server ranks sharing one
rabbit host (`job2`'s `--cores-per-task`) can default to the same
libfabric device.

`pin_cxi_nic.sh` works around this: it's a wrapper each server rank
execs through instead of running `pdc_server` directly, reading the
launcher's pre-exec local-rank env var (`FLUX_TASK_LOCAL_ID`/
`PMI_LOCAL_RANK`/etc. -- available before the real binary's own
`MPI_Init` even runs) to set `FI_CXI_DEVICE_NAME=cxi<local_rank % 2>`
per rank before exec'ing. Wired into the rabbit server launch in all
four `job2_{2,4,8,16}node_rabbit.flux.sh` scripts (`job1` only ever
runs 1 server rank per node, so there's no collision risk there).
**Still unverified against a real run** -- the local-rank env var
guess hasn't been confirmed to actually be set by this Flux version;
check the `[pin_cxi_nic]` line each rank logs to `server.out` for
which source variable it found (or `none`, meaning no pinning
happened and every rank fell back to device `cxi0`).

- `probe_fabric.flux.sh` -- one-off diagnostic (not part of the
  job1/job2 comparison): runs `fi_info` and `lspci`/`lsmod`/`ip link`
  on both a rabbit host and a compute node side by side. Submit with
  `flux batch probe_fabric.flux.sh`.
