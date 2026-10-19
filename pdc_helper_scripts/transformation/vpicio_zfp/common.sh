#!/bin/bash
# Shared configuration for the zfp_compress_cpu / zfp_compress_gpu /
# zfp_gpu_then_encrypt sbatch scripts in this directory -- sourced near
# the top of each one (and of each matching *_run.sh) so server/client
# topology and workload sizing come from ONE place, the same convention
# as pdc_helper_scripts/analysis_scripts/common.sh and
# curl_analysis_scripts/. Every value here stays override-able at submit
# time, e.g.:
#   SERVERS_PER_NODE=4 sbatch --nodes=4 zfp_compress_cpu.sbatch
#
# CAVEAT -- same as the shared pdc_helper_scripts/common.sh: this does
# NOT control #SBATCH pragma lines (--account, --time, --constraint,
# --qos, --ntasks-per-node, --gpus-per-node) -- those are static text
# Slurm scans in the submitted file before any shell code runs. The
# *_run.sh wrapper scripts handle this correctly (they compute and pass
# --ntasks-per-node on the sbatch command line, same as
# analysis_scripts/*_run.sh); submitting a .sbatch file here directly
# does not.

export SERVERS_PER_NODE=${SERVERS_PER_NODE:-2}
export CLIENTS_PER_NODE=${CLIENTS_PER_NODE:-32}

# ── Servers-per-node strong-scaling sweep -- same convention as
#    analysis/common.sh: each zfp_*.sbatch script here loops over this
#    list within one job submission (PDC data directory wiped between
#    steps by srun_server_vpicio.sh's own startup), so one job produces
#    the whole 2/4/8 sweep. Each script's own #SBATCH --ntasks-per-node
#    must equal the LARGEST value here plus CLIENTS_PER_NODE.
export SERVER_SWEEP=${SERVER_SWEEP:-"2 4 8"}

# vpicio.c workload sizing -- 8388608 particles/rank * 7 float fields
# (dX,dY,dZ,Ux,Uy,Uz,q) + 1 int field (i) = 32 bytes/particle = 256
# MiB/rank/step, matching this project's own established VPIC-IO sizing
# convention (see evaluation.tex / vpicio_scripts/vpicio_scale.sh's
# original NPARTICLES). STEPS=5 matches that same convention.
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
