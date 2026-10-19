#!/bin/bash
# Scaling of bench_curl_eager's object-setup MPI collective pattern alone
# (see mpi_object_setup_scale.c) -- single node-count step. No PDC server
# involved. 32 ranks/node, matching bench_curl_eager's own client rank
# density (see ../pdc_helper_scripts/curl_analysis_scripts/).
#
# Submit one of these per node count via run_scale.sh, or directly, e.g.:
#   sbatch --nodes=4 mpi_object_setup_scale.sbatch
# Override N_TIMESTEPS at submit time, e.g.:
#   N_TIMESTEPS=10 sbatch --nodes=4 mpi_object_setup_scale.sbatch

#SBATCH --job-name=mpi-object-setup-scale
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=32
#SBATCH --cpus-per-task=4
#SBATCH --account=m2621
#SBATCH --time=00:15:00
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --output=%x_%j.log

set -xeu
cd "$SLURM_SUBMIT_DIR"

export NUM_NODES=$SLURM_JOB_NUM_NODES
export RANKS_PER_NODE=32
export TOTAL_TASKS=$((NUM_NODES * RANKS_PER_NODE))
export N_TIMESTEPS=${N_TIMESTEPS:-3}

# Cray MPICH's OFI netmod already targets cxi automatically over
# Slingshot-11 (provided the binary was built with `make CC=cc`, not
# generic mpicc -- see Makefile), so this isn't strictly required. Setting
# it explicitly makes the MPI collectives below fail loudly if cxi isn't
# picked up for some reason, instead of silently falling back to a
# slower provider.
export FI_PROVIDER=cxi

# One CSV per job (== per node count, since run_scale.sh submits one job
# per node count), matching curl_analysis_scripts/*.sbatch's convention --
# concatenate the per-node-count files afterward to see the full sweep.
export RESULTS=$PWD/results_mpi_object_setup_scale_${SLURM_JOB_ID}.csv
LOG="mpi_object_setup_scale_${NUM_NODES}.log"

srun \
  -N "$NUM_NODES" \
  -n "$TOTAL_TASKS" \
  --ntasks-per-node="$RANKS_PER_NODE" \
  --output="$LOG" \
  ./mpi_object_setup_scale "$N_TIMESTEPS"

# LOG has a "phase,step,n_ranks,setup_s" header line followed by one row
# for the container-create broadcast and one per timestep.
cp "$LOG" "$RESULTS"

echo "Done. Results in $RESULTS"
