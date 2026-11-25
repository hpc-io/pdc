#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != -n || ! "${2:-}" =~ ^[1-9][0-9]*$ || $# -lt 3 ]]; then
    echo "Usage: $0 -n ranks command [args...]" >&2
    exit 2
fi

ranks=$2
shift 2
rank_launcher="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/pdc-rank.sh"
executable=${1##*/}
case "$executable" in
    pdc_server) role=server; cores=${PDC_TEST_SERVER_CPUS:-4} ;;
    *) role=client; cores=2 ;;
esac

network=()
if [[ "${HG_TRANSPORT:-ofi+cxi}" == ofi+cxi ]]; then
    # Single-node steps also need a Slingshot service, including close_server.
    network=(--network=job_vni,single_node_vni)
fi
nodes=${SLURM_JOB_NUM_NODES:?Run the MPI CTests within a Slurm allocation}
if ((nodes > ranks)); then
    nodes=$ranks
fi

# Each CTest starts its server step in the background before starting the client.
status=0
srun --immediate=15 --mpi=cray_shasta --exclusive --exact --cpu-bind=cores \
    --distribution=cyclic -N "$nodes" -n "$ranks" -c "$cores" "${network[@]}" \
    "$rank_launcher" "$role" "$@" || status=$?
printf 'PDC_MPI_STEP ranks=%s executable=%s status=%s\n' "$ranks" "$executable" "$status"
exit "$status"
