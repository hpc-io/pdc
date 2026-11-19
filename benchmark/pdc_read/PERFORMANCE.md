# PDC read performance on Perlmutter

**Threaded PDC batch 16 finished the matched first read in 13.74 s
at the median, 40.4% less time than HDF5 (23.05 s).**
The best core-capped layout reserved **8 physical cores per node** for PDC
and reached a **15.32 s median** in its three-pass sweep.

## Matched first reads

The benchmark used four Perlmutter CPU nodes, 16 MPI readers, and
3,360 × 100 MiB records (328.125 GiB). Each repeat started with a fresh dataset.
PDC and HDF5 used the same physical reader cores. PDC used four servers per
node, four RPC workers per server, eight physical cores per server, and batch
size 16. PDC cache capacity was 64 GiB per node.

| Reader | Repeat 1 (s) | Repeat 2 (s) | Repeat 3 (s) | Median (s) |
| --- | ---: | ---: | ---: | ---: |
| Threaded PDC batch 16 on server workers | 13.7447 | 13.5702 | 14.3605 | **13.74** |
| HDF5 | 24.5729 | 22.7522 | 23.0485 | **23.05** |

The earlier throughput comparison used the same workload and automatic
Slurm placement. The worker-pool build reached 13.36 s versus 20.07 s for
the Mercury-dispatch build, a 33.4% difference for those revisions. Threaded
depth one reached 18.02 s in the same campaign.

## Core-capped configurations

Four servers per node with four RPC workers and two physical cores per
server was the fastest capped layout. Its three passes took
14.8294 s, 16.5762 s, 15.3221 s. At a budget of four physical
cores per node, four one-core servers and two two-core servers gave similar
medians of 24.80 and 25.00 s in the earlier sweep.

| Servers/node | RPC workers/server | Physical cores/server | PDC cores/node | First read (s) | Median (s) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 1 | 1 | 144.27 | 164.35 |
| 1 | 4 | 2 | 2 | 100.85 | 112.53 |
| 2 | 2 | 1 | 2 | 41.88 | 41.88 |
| 2 | 4 | 2 | 4 | 24.55 | 25.00 |
| 4 | 2 | 1 | 4 | 24.80 | 24.80 |
| 4 | 4 | 2 | 8 | 14.83 | **15.32** |

The core sweep used batch size 16, 64 GiB of PDC cache per node, and three
separate reader processes over one dataset per layout. Later passes may
benefit from OS cache state. The 15.32 s row was rerun with the
reviewed build. The other rows are from the earlier sweep. The 1-server,
2-worker and 2-server, 4-worker layouts used a second four-node set with
the same CPU topology.

## Measurement scope

The matched first reads and the repeated 8-core run used source revision
`46d9d0c9906d25dfd7c1a0f5e9e4affa6053d945`. All seven full-size datasets passed byte and
record-identity verification. The subsequent failure-handling changes
passed separate local checks. Timing excludes generation, writeback,
verification, and shutdown. These measurements cover a synthetic read
workload and its core reservations. Application time to solution with PDC
alongside computation remains to be measured.

[Reproduce the comparisons or run additional layouts](README.md).
