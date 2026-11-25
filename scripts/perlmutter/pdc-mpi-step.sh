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
    pdc_server)
        role=server
        cores=${PDC_TEST_SERVER_CPUS:-4}
        memory=8192M
        ;;
    *)
        role=client
        cores=2
        # The 16-rank VPIC test needs more memory than the small client tests.
        memory=32768M
        ;;
esac

nodes=${SLURM_JOB_NUM_NODES:?Run the MPI CTests within a Slurm allocation}
: "${SLURM_JOB_ID:?Run the MPI CTests within a Slurm allocation}"
placement=()
if [[ "${HG_TRANSPORT:-ofi+cxi}" == ofi+cxi ]]; then
    if ((nodes < 2)); then
        echo "Allocate two nodes for CXI CTests so server and client steps have separate NICs." >&2
        exit 2
    fi
    # A single-node step needs its own service while sharing the allocation's job VNI.
    relative=0
    if [[ "$role" == client ]]; then
        relative=1
    fi
    placement=(--nodes=1 --relative="$relative" --network=job_vni,single_node_vni)
else
    if ((nodes > ranks)); then
        nodes=$ranks
    fi
    placement=(--nodes="$nodes")
fi

# Reserve only this step's memory and relay termination to the owned srun process.
srun --immediate=15 --mpi=cray_shasta --exact --cpu-bind=cores \
    --mem="$memory" --distribution=cyclic -n "$ranks" -c "$cores" "${placement[@]}" \
    "$rank_launcher" "$role" "$@" &
step_pid=$!
trap 'kill -TERM "$step_pid" 2>/dev/null || :; wait "$step_pid" || :; exit 143' TERM
status=0
wait "$step_pid" || status=$?
printf 'PDC_MPI_STEP ranks=%s executable=%s status=%s\n' "$ranks" "$executable" "$status"
exit "$status"
