#!/bin/bash
# Shared configuration for every PDC magnitude/curl analysis sbatch script
# (analysis_scripts/*.sbatch and curl_analysis_scripts/*.sbatch) -- sourced
# near the top of each one so server/client topology and workload sizing
# come from ONE place instead of being copy-pasted (and drifting) across
# nine nearly-identical files. Every value here stays override-able at
# submit time exactly like before, e.g.:
#   SERVERS_PER_NODE=4 sbatch --nodes=4 eager_pdc.sbatch
#   N_ELEM=33554432 sbatch --nodes=4 lazy_pdc.sbatch
#
# CAVEAT -- this does NOT control everything: Slurm's sbatch scans
# #SBATCH pragma lines (--account, --time, --constraint, --qos,
# --ntasks-per-node, --gpus-per-node) as static text directly in the
# submitted file, before any shell code -- including this source line --
# ever executes. Those still have to be edited per-file; sourcing this
# script only affects values used in the script BODY (plain `export`
# lines below and after). In particular, every PDC-server sbatch file's
# own "#SBATCH --ntasks-per-node=N" line is meant to equal
# SERVERS_PER_NODE + CLIENTS_PER_NODE from here -- changing either
# default below without also updating that line in each file will
# silently under/over-allocate tasks per node.

# ── PDC server/client topology (analysis_scripts/, curl_analysis_scripts/,
#    every mode except the HDF5 baselines, which have no PDC server) ──────
export SERVERS_PER_NODE=${SERVERS_PER_NODE:-4}
export CLIENTS_PER_NODE=${CLIENTS_PER_NODE:-32}

# ── Servers-per-node strong-scaling sweep -- every PDC-backed sbatch
#    script in analysis/{magnitude,curl}/pdc/ loops over this list within
#    ONE job submission (one full workload run per value, PDC data
#    directory wiped between steps by srun_server.sh's own startup), so a
#    single job produces the whole sweep instead of three separately
#    submitted, separately dated jobs. Each script's own
#    #SBATCH --ntasks-per-node must equal the LARGEST value here plus
#    CLIENTS_PER_NODE (see the caveat above -- #SBATCH pragmas are static
#    and can't read this file). Override at submit time the same way,
#    e.g. SERVER_SWEEP="2 8" sbatch --nodes=8 eager_pdc.sbatch.
export SERVER_SWEEP=${SERVER_SWEEP:-"2 4 8"}

# ── Magnitude workload sizing (analysis_scripts/) ─────────────────────────
# 64 MiB/rank of float32 -- at the max scale point (128 nodes * 32
# ranks/node = 4096 ranks), this writes 320 MiB/rank per timestep (192 MiB
# input + 128 MiB magnitude out), and with N_TIMESTEPS=3 in
# bench_magnitude.c / bench_write_components.c / bench_posthoc_analyze.c /
# hdf5_bench_write.c that's 960 MiB/rank, ~3.75 TiB (~4.12 TB) aggregate
# against a 4 TB budget -- just over on paper but within rounding of
# typical scratch quotas; reduce N_ELEM if that budget is a hard
# constraint.
export N_ELEM=${N_ELEM:-16777216}

# ── Curl workload sizing (curl_analysis_scripts/) ─────────────────────────
# Per-rank 3D block dimensions -- see curl_analysis_scripts/README.md for
# how these were derived from the source paper's reported data volumes
# (u,v,w ~4GB total at a 96-rank reference point, so that curl comes out
# to ~4GB and vorticity_magnitude to ~2.7GB, matching the paper's reported
# ~4GB/~3GB).
export NX=${NX:-256}
export NY=${NY:-256}
export NZ_PER_RANK=${NZ_PER_RANK:-64}
export COMPRESS=${COMPRESS:-0}
