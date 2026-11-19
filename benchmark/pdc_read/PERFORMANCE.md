# PDC read performance on Perlmutter

**Four servers per node with four RPC workers and two physical cores per server** was the fastest capped layout. It read the workload in 14.65 s at the median with 8 PDC cores per node, one quarter of the 32 cores reserved by the throughput configuration.

## Throughput configuration

The benchmark used four Perlmutter CPU nodes, 16 MPI readers, and 3,360 × 100 MiB records (328.125 GiB). Each row summarizes three first reads of independently generated datasets. The threaded PDC runs used four server ranks per node, four RPC workers per rank, and eight physical cores per rank.

| Reader | Median read (s) |
| --- | ---: |
| Threaded PDC batch 16 on server workers | **13.36** |
| Threaded PDC depth one | 18.02 |
| Threaded PDC batch 16 on Mercury dispatch | 20.07 |
| HDF5 | 22.84 |

Moving batch reads onto the workers reduced the batch-16 median by **33.4%**. The worker-pool result took 25.9% less time than depth one and 41.5% less than HDF5.

## Core-capped configurations

| Servers/node | RPC workers/server | Physical cores/server | PDC cores/node | First read (s) | Median read (s) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 1 | 1 | 144.27 | 164.35 |
| 1 | 4 | 2 | 2 | 100.85 | 112.53 |
| 2 | 2 | 1 | 2 | 41.88 | 41.88 |
| 2 | 4 | 2 | 4 | 24.55 | 25.00 |
| 4 | 2 | 1 | 4 | 24.80 | 24.80 |
| 4 | 4 | 2 | 8 | 14.37 | 14.65 |

At **four cores per node**, four one-core servers and two two-core servers gave similar medians of 24.80 and 25.00 s. At **two cores per node**, two one-core servers took 62.8% less time than one two-core server on the same nodes.

Each layout used batch size 16, 64 GiB of PDC cache capacity per node, a fresh dataset, and three consecutive reads from separate reader invocations. Later passes reused the dataset and may reflect OS cache state. The reader core placement stayed fixed and disjoint from the server cores. The first-read times are provided separately for comparison.

The one-server/two-worker and two-server/four-worker layouts ran on a second four-node CPU set with the same topology. The 8-core control reproduced at 14.35 s median there, compared with 14.65 s on the original nodes.

Every reported case passed full-byte verification. Read times exclude generation, writeback, verification, and teardown. The two-server/two-worker case reached its close deadline after verification. The measurements cover synthetic read performance and core reservations. Application time to solution with PDC alongside computation remains to be measured.

[Reproduce the comparisons or run additional layouts](README.md).
