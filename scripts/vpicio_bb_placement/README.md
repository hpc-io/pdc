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

Both job1/job2 assume `pdc_server`, `vpicio`, and `close_server` are
already built (edit `EXECPATH` near the top of each script) and submit
with:

```
flux batch job1_compute_oversubscribed.flux.sh
flux batch job2_compute_plus_rabbit.flux.sh
flux batch small_job2_compute_plus_rabbit.flux.sh
```

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
