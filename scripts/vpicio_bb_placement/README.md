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

## What's confirmed vs. best-effort here

Everything in job 1, and most of job 2, is built on mechanisms directly
confirmed from LLNL's own Aug 2025 CORAL2 roadshow slides and the
flux-coral2 docs: node layout (96 cores/node, 84 for user processes),
Flux's Slurm-equivalent commands, `#DW jobdw` for Rabbit storage, and
`--requires=host:<name>` for targeting a specific node.

The one piece that isn't: actually scheduling a *compute process* onto a
Rabbit's own CPU (job 2's whole point). That depends on
`--coral2-rabbit-cores`/`--coral2-chassis`, which the flux-coral2 docs
themselves call experimental, and which is absent from LLNL's own
mainstream user-facing slide deck -- i.e. it exists, but it isn't part
of the documented, everyday workflow the way storage requests are.
Job 2 chains that flag together with `flux getrabbit` + host-targeting
as a best-effort way to get a rank physically onto the Rabbit; run
`flux alloc --help=coral2-rabbit-cores` on the actual target system and
sanity-check job 2's rabbit-targeting block against it before trusting
the comparison it's set up to produce.

`explore_rabbit_cores.flux.sh` is the script to run first, before either
job: it allocates rabbit cores the same way job 2 does, then dumps the
flag's own help text, the actual resource set Flux assigned (`flux job
info ... R`), and a hostname + CPU-model fingerprint of a process
targeted at the rabbit vs. one on the compute node -- a differing CPU
model between those two confirms the server would really land on
separate hardware; a match means job 2's `--requires=host:` targeting
isn't doing what it's assumed to do and needs rework. Submit it with
`flux batch explore_rabbit_cores.flux.sh` and read its single output
file for everything needed to correct job 2 if it's wrong.
