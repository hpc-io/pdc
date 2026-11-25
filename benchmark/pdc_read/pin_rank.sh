#!/usr/bin/env bash
set -euo pipefail

role=$1
shift
if [[ "$role" != hdf5 ]]; then
    rank_launcher="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/perlmutter/pdc-rank.sh"
    export HG_TRANSPORT=ofi+cxi
    case "$role" in
        server) set -- "$rank_launcher" server "$@" ;;
        *) set -- "$rank_launcher" client "$@" ;;
    esac
fi
if [[ -n ${PDC_BENCH_CPUSETS:-} ]]; then
    IFS=';' read -r -a masks <<< "$PDC_BENCH_CPUSETS"
    mask=${masks[${SLURM_LOCALID:?}]:?}
    exec /usr/bin/taskset --cpu-list "$mask" "$@"
fi
exec "$@"
