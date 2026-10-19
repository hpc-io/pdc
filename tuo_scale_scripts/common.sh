#!/bin/bash
# Shared config, sourced by run_sync.sh, run_async.sh, run_small_scale.sh,
# and vpic_bdcats.sbatch, so the node-count list and the weak-scaling
# particle count are computed in exactly one place instead of drifting
# between the sync/async drivers.

export SERVERS_PER_NODE=${SERVERS_PER_NODE:-4}
export CLIENTS_PER_NODE=${CLIENTS_PER_NODE:-32}
export STEPS=${STEPS:-5}

# Combined per-node task count and per-task core count for the single
# sbatch allocation both the server and client srun steps share. 96
# matches Tuolumne's pbatch queue (confirmed via `flux resource list`);
# override CORES_PER_NODE if running this elsewhere.
export CORES_PER_NODE=${CORES_PER_NODE:-96}
export TASKS_PER_NODE=$((SERVERS_PER_NODE + CLIENTS_PER_NODE))
export CPUS_PER_TASK=${CPUS_PER_TASK:-$((CORES_PER_NODE / TASKS_PER_NODE))}
if [ "$CPUS_PER_TASK" -lt 1 ]; then
    export CPUS_PER_TASK=1
fi

# Weak scaling: NUMPARTICLES (per rank) is held constant across every node
# count in the sweep, sized against a TOP_NODES-node target of 4 TiB
# total across all STEPS timesteps. TOP_NODES is a pure sizing anchor,
# independent of NODE_COUNTS below -- with NODE_COUNTS topping out at 128
# (same as TOP_NODES), the 128-node step writes the full 4 TiB target and
# 64 nodes writes 64/128 = 1/2 of that (~2 TiB). Change TOP_NODES if you
# want the top of NODE_COUNTS to hit a different target instead.
# bytes_per_particle=32 == 7 floats + 1 int (dX,dY,dZ,Ux,Uy,Uz,q,i),
# matching src/tests/misc/vpic_bdcats.c exactly.
TOP_NODES=128
BYTES_PER_PARTICLE=32
TOTAL_BYTES_TARGET=$((4 * 1024 * 1024 * 1024 * 1024)) # 4 TiB
TOP_TOTAL_RANKS=$((TOP_NODES * CLIENTS_PER_NODE))
export NUMPARTICLES=${NUMPARTICLES:-$((TOTAL_BYTES_TARGET / (TOP_TOTAL_RANKS * BYTES_PER_PARTICLE * STEPS)))}

# Required, not defaulted to a relative path: at 4 TiB total, this MUST
# point at real parallel scratch storage, not whatever filesystem BIN_DIR
# happens to sit on. Set this once in your environment (or export it
# before invoking run_sync.sh/run_async.sh/run_small_scale.sh) --
# e.g. export PDC_DATA_LOC=/p/lustre1/$USER/pdc_vpic_bdcats_scale
: "${PDC_DATA_LOC:?PDC_DATA_LOC must be set to a real parallel-filesystem path -- this sweep writes up to 4 TiB}"
export PDC_DATA_LOC
export PDC_TMPDIR=${PDC_TMPDIR:-$PDC_DATA_LOC}

export BIN_DIR=${BIN_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../build/bin" && pwd)}

# 64, 128 -- just the top two points of the original 1..128 progression
# (see TOP_NODES above for how that interacts with the weak-scaling
# data-volume target).
NODE_COUNTS=(64 128)
