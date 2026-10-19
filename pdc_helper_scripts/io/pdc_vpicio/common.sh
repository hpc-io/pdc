#!/bin/bash
# Shared configuration for vpicio_raw.sbatch -- sourced near the top of
# that sbatch script and its matching vpicio_raw_run.sh, the same
# convention as transformation/vpicio_zfp/common.sh. Every value here
# stays override-able at submit time, e.g.:
#   SERVERS_PER_NODE=4 sbatch --nodes=4 vpicio_raw.sbatch
#
# CAVEAT -- same as every other common.sh in this repo: this does NOT
# control #SBATCH pragma lines (--account, --time, --constraint, --qos,
# --ntasks-per-node) -- those are static text Slurm scans in the
# submitted file before any shell code runs. vpicio_raw_run.sh computes
# and passes --ntasks-per-node on the sbatch command line to handle this
# correctly; submitting vpicio_raw.sbatch directly does not.

export SERVERS_PER_NODE=${SERVERS_PER_NODE:-2}
export CLIENTS_PER_NODE=${CLIENTS_PER_NODE:-32}

# ── Servers-per-node strong-scaling sweep -- same convention as
#    transformation/vpicio_zfp/common.sh: vpicio_raw.sbatch loops over
#    this list within one job submission (PDC data directory wiped
#    between steps by srun_server_vpicio.sh's own startup), so one job
#    produces the whole 2/4/8 sweep. vpicio_raw.sbatch's own #SBATCH
#    --ntasks-per-node must equal the LARGEST value here plus
#    CLIENTS_PER_NODE.
export SERVER_SWEEP=${SERVER_SWEEP:-"2 4 8"}

# vpicio.c workload sizing -- 8388608 particles/rank * 7 float fields
# (dX,dY,dZ,Ux,Uy,Uz,q) + 1 int field (i) = 32 bytes/particle = 256
# MiB/rank/step, matching this project's own established VPIC-IO sizing
# convention (see evaluation.tex / transformation/vpicio_zfp/common.sh's
# NPARTICLES). STEPS=5 matches that same convention.
# SLEEPTIME defaults to 40s -- the paper's own compute-overlap
# convention (evaluation.tex: "40s/100s GEMM" per step; 40s is the
# standard point, 100s only an independent one-off comparison). vpicio.c's
# write_s already correctly excludes whatever sleep time is configured
# here from its timing regardless -- this is purely the emulated-compute
# phase the write is meant to overlap with, not padding on the measured
# cost itself.
export NPARTICLES=${NPARTICLES:-8388608}
export STEPS=${STEPS:-5}
export SLEEPTIME=${SLEEPTIME:-40}
