#!/bin/bash
# Pin each rank launched under `flux run` to its own CXI NIC device, so
# multiple ranks sharing one host (e.g. several PDC server ranks
# consolidated onto one Rabbit chassis via --cores-per-task) don't all
# default to the same libfabric device. No code in pdc_server.c/
# pdc_client_connect.c sets FI_CXI_DEVICE_NAME itself -- HG_HOST/
# HG_TRANSPORT are identical across ranks, so without this every rank on
# a host picks libfabric's default device.
#
# Reads the launcher's pre-exec local-rank env var (set before this
# script execs, so it works even though the real rank is only known
# inside the target binary's own MPI_Init). Tries, in order:
#   FLUX_TASK_LOCAL_ID, PMI_LOCAL_RANK, OMPI_COMM_WORLD_LOCAL_RANK,
#   FLUX_TASK_RANK, PMI_RANK
# and falls back to 0 (logged) if none are set, so a run still proceeds
# (just without pinning) rather than failing outright.
#
# Usage: pin_cxi_nic.sh <num_nics> <real_binary> [args...]

set -euo pipefail

NUM_NICS=$1
shift

if [ -n "${FLUX_TASK_LOCAL_ID:-}" ]; then
    LOCAL_RANK=$FLUX_TASK_LOCAL_ID
    SRC=FLUX_TASK_LOCAL_ID
elif [ -n "${PMI_LOCAL_RANK:-}" ]; then
    LOCAL_RANK=$PMI_LOCAL_RANK
    SRC=PMI_LOCAL_RANK
elif [ -n "${OMPI_COMM_WORLD_LOCAL_RANK:-}" ]; then
    LOCAL_RANK=$OMPI_COMM_WORLD_LOCAL_RANK
    SRC=OMPI_COMM_WORLD_LOCAL_RANK
elif [ -n "${FLUX_TASK_RANK:-}" ]; then
    LOCAL_RANK=$FLUX_TASK_RANK
    SRC=FLUX_TASK_RANK
elif [ -n "${PMI_RANK:-}" ]; then
    LOCAL_RANK=$PMI_RANK
    SRC=PMI_RANK
else
    LOCAL_RANK=0
    SRC=none
fi

DEV_IDX=$((LOCAL_RANK % NUM_NICS))
export FI_CXI_DEVICE_NAME="cxi${DEV_IDX}"

echo "[pin_cxi_nic] host=$(hostname) local_rank=$LOCAL_RANK (source=$SRC) num_nics=$NUM_NICS -> FI_CXI_DEVICE_NAME=$FI_CXI_DEVICE_NAME" >&2

exec "$@"
