#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 15m
#FLUX: --job-name=pdc_rabbit_explore
#FLUX: --output=pdc_rabbit_explore.{{id}}.out
#FLUX: --error=pdc_rabbit_explore.{{id}}.out
#FLUX: --setattr=dw="#DW jobdw type=xfs capacity=10GiB name=pdcexplore"

# Diagnostic-only script: runs no PDC code. Collects what's needed to
# decide whether job2_compute_plus_rabbit.flux.sh can put the PDC server
# on a Rabbit's CPU, and confirms the Rabbit storage path either way.
#
# What changed from the first version and why:
#   - The first version submitted the rabbit-cores request from INSIDE this
#     batch job. A batch job is its own Flux instance that only knows about
#     its one compute node (no chassis, no rabbits), so that inner job
#     failed immediately and every later probe saw nothing. All rabbit
#     requests below go to the SYSTEM instance via `flux --parent`.
#   - This job requests a Rabbit volume itself (#DW above), so it can
#     confirm the storage mount and ask the system which Rabbit served it.
#   - The rabbit-cores feasibility checks submit, record accepted vs.
#     rejected (with Flux's reason), and cancel any accepted job right away
#     instead of waiting on it.
#   - Parsing guards: "No rabbits found..." is no longer treated as a
#     hostname.
#
# Submit with: flux batch scripts/vpicio_bb_placement/explore_rabbit_cores.flux.sh
# Then read the single output file configured above.

set -uo pipefail # not -e: we want every probe below to run even if an earlier one fails

section() { echo; echo "=================================================="; echo "$1"; echo "=================================================="; }

JOBID=$(flux getattr jobid)

section "flux version"
flux version

section "flux alloc --help=coral2-rabbit-cores (authoritative flag semantics)"
flux alloc --help=coral2-rabbit-cores 2>&1

section "This job (as seen by the system instance)"
echo "JOBID=$JOBID"
flux --parent jobs -o "{id} {queue} {state} {nnodes} {nodelist}" "$JOBID" 2>&1

section "flux resource list (inside this job's own instance)"
flux resource list 2>&1

section "Rabbit storage: this job's #DW jobdw volume"
env | grep '^DW_' || echo "No DW_* variables set -- the Rabbit volume was not created."
if [ -n "${DW_JOB_pdcexplore:-}" ]; then
    df -h "$DW_JOB_pdcexplore"
    T0=$(date +%s.%N)
    dd if=/dev/zero of="$DW_JOB_pdcexplore/ddtest" bs=1M count=1024 conv=fsync status=none
    T1=$(date +%s.%N)
    awk -v a="$T0" -v b="$T1" 'BEGIN { printf "1 GiB write+fsync to Rabbit volume: %.2f s (%.0f MiB/s)\n", b-a, 1024/(b-a) }'
    rm -f "$DW_JOB_pdcexplore/ddtest"
fi

section "flux getrabbit -j \$JOBID (which Rabbit serves this job's storage)"
GETRABBIT_OUT=$(flux --parent getrabbit -j "$JOBID" 2>&1)
echo "$GETRABBIT_OUT"
if echo "$GETRABBIT_OUT" | grep -qi -e "no rabbits" -e "error" -e "usage"; then
    RABBIT_HOST=""
else
    RABBIT_HOST=$(echo "$GETRABBIT_OUT" | tr -d '[] ' | cut -d',' -f1)
fi
echo "Parsed RABBIT_HOST=${RABBIT_HOST:-<none>}"

section "Rabbit-cores feasibility (submitted to the SYSTEM instance, cancelled immediately)"
try_submit() {
    local label=$1; shift
    local out
    out=$(flux --parent submit -t 1m "$@" hostname 2>&1)
    if [[ "$out" =~ ^f[0-9A-Za-z]+$ ]]; then
        echo "ACCEPTED  $label   (job $out, cancelling)"
        flux --parent cancel "$out" 2>/dev/null || true
    else
        echo "REJECTED  $label   -> $out"
    fi
}
for Q in pdebug pbatch; do
    try_submit "$Q: chassis only"                -q "$Q" -N1 --coral2-chassis=1
    try_submit "$Q: chassis + rabbit-cores=1"    -q "$Q" -N1 --coral2-chassis=1 --coral2-rabbit-cores=1
    try_submit "$Q: chassis + rabbit-cores + DW" -q "$Q" -N1 --coral2-chassis=1 --coral2-rabbit-cores=1 \
        --setattr=dw="#DW jobdw type=xfs capacity=10GiB name=pdcprobe"
done
echo
echo "If every 'rabbit-cores' line is REJECTED, user processes can't be scheduled on a"
echo "Rabbit's CPU on this system -- job 2's server-on-Rabbit design needs LC support"
echo "(ask whether --coral2-rabbit-cores is enabled on Tuolumne)."

section "Hardware fingerprint: this job's compute node"
flux run -N1 -n1 bash -c 'echo "host: $(hostname)"; grep -m1 "model name" /proc/cpuinfo; nproc'

section "Done"
