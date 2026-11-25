#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
    server) base=128 ;;
    client) base=256 ;;
    *) echo "Usage: $0 {server|client} command [args...]" >&2; exit 2 ;;
esac
shift

export HG_TRANSPORT="${HG_TRANSPORT:-ofi+cxi}"
if [[ "$HG_TRANSPORT" == ofi+cxi ]]; then
    if [[ ! "${SLURM_LOCALID:-}" =~ ^[0-9]+$ ]]; then
        echo "Run each CXI rank in an srun step within a Perlmutter allocation." >&2
        exit 2
    fi
    # Reserve disjoint PID ranges for servers and clients on each NIC.
    if ((SLURM_LOCALID >= 128)); then
        echo "The CXI rank launcher supports up to 128 ranks per role per node." >&2
        exit 2
    fi
    export HG_HOST="cxi0:$((base + SLURM_LOCALID))"

    # Slurm lists the step VNI first and the job VNI second when it lists both.
    service_ids=${SLINGSHOT_SVC_IDS:-}
    vnis=${SLINGSHOT_VNIS:-}
    service=${service_ids%%,*}
    vnis=${vnis#*,}
    vni=${vnis%%,*}
    if [[ ! "$service" =~ ^[0-9]+$ || ! "$vni" =~ ^[0-9]+$ ]]; then
        echo "CXI requires Slingshot service and VNI information from srun --network=job_vni,single_node_vni." >&2
        exit 2
    fi
    export PDC_HG_AUTH_KEY="$service:$vni"
fi

exec "$@"
