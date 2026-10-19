#!/bin/bash
# Shared configuration for vpicio_bdcats.sbatch (adios2_vpicio +
# adios2_bdcats) -- sourced near the top of that sbatch script and its
# matching vpicio_bdcats_run.sh, the same convention as
# pdc_helper_scripts/vpicio_scripts/common.sh on the PDC side. Every
# value here stays override-able at submit time, e.g.:
#   SLEEPTIME=40 sbatch --nodes=4 vpicio_bdcats.sbatch
#
# No SERVERS_PER_NODE here -- ADIOS2 has no server process at all, so
# there's nothing analogous to configure. CAVEAT (same as the PDC-side
# common.sh files): this does NOT control #SBATCH pragma lines
# (--account, --time, --constraint, --qos, --ntasks-per-node) -- those
# are static text Slurm scans before any shell code runs.
# vpicio_bdcats_run.sh computes and passes --ntasks-per-node on the
# sbatch command line to handle this correctly; submitting
# vpicio_bdcats.sbatch directly does not.

export CLIENTS_PER_NODE=${CLIENTS_PER_NODE:-32}

# Same VPIC-IO sizing convention as vpicio_scripts/common.sh -- 8388608
# particles/rank * 7 float fields + 1 int field = 256 MiB/rank/step.
export NPARTICLES=${NPARTICLES:-8388608}
export STEPS=${STEPS:-5}

# How long (seconds) adios2_vpicio sleeps between steps to emulate
# compute -- see adios2_vpicio.cpp's own header comment for exactly
# where this sits relative to the timed write bracket. Defaults to 40s,
# matching io/pdc_vpicio/'s and transformation/vpicio_zfp/'s own
# SLEEPTIME convention (the paper's standard compute-overlap point) --
# both sides of the PDC-vs-ADIOS2 comparison need the same compute
# overlap to be comparable.
export SLEEPTIME=${SLEEPTIME:-40}
