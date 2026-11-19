#!/usr/bin/env bash
set -euo pipefail

role=$1
shift
if [[ "$role" != hdf5 ]]; then
    export HG_TRANSPORT=ofi+cxi
    base=256
    if [[ "$role" == server ]]; then
        base=128
    fi
    export HG_HOST="cxi0:$((base + SLURM_LOCALID))"
    service=${SLINGSHOT_SVC_IDS%%,*}
    vnis=${SLINGSHOT_VNIS#*,}
    vni=${vnis%%,*}
    export PDC_HG_AUTH_KEY="${service:?}:${vni:?}"
fi
if [[ -n ${PDC_BENCH_CPUSETS:-} ]]; then
    IFS=';' read -r -a masks <<< "$PDC_BENCH_CPUSETS"
    mask=${masks[${SLURM_LOCALID:?}]:?}
    exec /usr/bin/taskset --cpu-list "$mask" "$@"
fi
exec "$@"
