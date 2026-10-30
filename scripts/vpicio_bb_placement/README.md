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

Both assume `pdc_server`, `vpicio`, and `close_server` are already built
(edit `EXECPATH` near the top of each script) and submit with:

```
flux batch job1_compute_oversubscribed.flux.sh
flux batch job2_compute_plus_rabbit.flux.sh
```

## What's confirmed vs. still open (updated from a real run on Tuolumne)

Confirmed from an actual `flux batch` run of `explore_rabbit_cores.flux.sh`
on Tuolumne (job `f3h9kPZRAK7u`, 2026-10-30):

- `#DW jobdw` Rabbit storage works exactly as documented: mounted at
  `/mnt/nnf/...`, 1325 MiB/s for a 1GiB write+fsync, served by a rabbit
  host (`tuolumne201`) genuinely separate from the compute node
  (`tuolumne1009`).
- A `flux batch` job runs in its own private Flux sub-instance that
  does **not** see chassis/rabbit resources directly -- anything
  chassis/rabbit-related has to go through `flux --parent ...` to reach
  the system instance. (This bit us on the first run: the original
  in-job `--coral2-rabbit-cores` submission failed immediately because
  of this, not because the flag doesn't work.)
- `--coral2-rabbit-cores` **alone is rejected** (`[Errno 19]
  Unsatisfiable request`), in both `pdebug` and `pbatch`. It's only
  accepted when bundled with a `#DW jobdw` directive in the *same*
  submission -- reserving compute on a rabbit you're not also using for
  storage isn't supported. **`job2_compute_plus_rabbit.flux.sh` as
  currently written doesn't do this** (it requests rabbit-cores on its
  own, separately from the `#FLUX: -S dw=...` in its header) and needs
  fixing before it will work.
- Flux directive syntax: `-t 30m` (not `0:30:00`), `--job-name=` /
  `--output=` / `--error=` with a `{{id}}` placeholder (not `-J` / `-o
  %J`).

Still open, because the first run's feasibility checks only
submitted-then-cancelled an accepted `chassis + rabbit-cores + DW`
combo rather than letting it run: whether a process submitted that way
actually *executes* on the rabbit's own CPU, or is just accounted
against the rabbit's resource pool while still running on compute-node
hardware. `explore_rabbit_cores.flux.sh` now includes a fingerprint
check that runs (not cancels) that exact combo and compares its
host/CPU model against the compute node's -- rerun it to close this
gap before trusting job 2's design. Once that comes back, job 2 still
needs the rabbit-cores reservation reworked to go through `flux
--parent` with the `#DW` bundled in, the same way the explore script
had to be reworked.
