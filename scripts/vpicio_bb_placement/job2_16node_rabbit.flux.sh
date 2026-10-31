#!/bin/bash
#FLUX: -N 16
#FLUX: -q pbatch
#FLUX: -t 60m
#FLUX: --job-name=pdc_vpicio_16node_rabbit
#FLUX: --output=pdc_vpicio_16node_rabbit.{{id}}.out
#FLUX: --error=pdc_vpicio_16node_rabbit.{{id}}.out
#FLUX: --coral2-chassis=1
#FLUX: --coral2-rabbit-cores=32
#FLUX: -S dw="#DW jobdw type=xfs capacity=10GiB name=pdcbb16"

# 16-node scale-out of job2_compute_plus_rabbit.flux.sh: the same 16
# compute nodes and client population as job1_16node_oversubscribed.flux.sh,
# but all 16 PDC server ranks run together on a single Rabbit chassis (32
# reserved cores, 2 cores per rank, so each rank gets its own pair of
# cores rather than sharing) instead of being spread 1-per-node across the
# compute nodes. This is the actual comparison job1/job2 exists to make,
# scaled up from the single-node version.
#
# UNVERIFIED: this has not been run. Specific things that haven't been
# tried anywhere in this debugging history and could need adjusting:
#   - --coral2-rabbit-cores=32 (only 1 has been confirmed to work)
#   - --cores-per-task=2 on the server launch, to get 2 cores/rank -- the
#     exact flag name/behavior on this flux version is unconfirmed; check
#     `flux run --help` if it's rejected.
#   - A 16-rank distributed PDC server all landing on ONE host via
#     --requires=host: (the single-rabbit-core job2 only ever ran 1
#     server rank there).
# See job1_16node_oversubscribed.flux.sh for the data-volume/walltime/
# queue caveats at these NPARTICLES/STEPS values -- same concern applies
# here.
#
# Submit with: flux batch job2_16node_rabbit.flux.sh

set -euo pipefail

N_NODES=16
N_USER_CORES_PER_NODE=84
N_SERVER_RANKS=16
CORES_PER_SERVER_RANK=2
N_CLIENT_RANKS=$((N_NODES * N_USER_CORES_PER_NODE))

# See job1_16node_oversubscribed.flux.sh for the data-volume caveat before
# changing these for a real run.
NPARTICLES=8388608
STEPS=10
COMPUTE_TIME=5

# See job1_compute_oversubscribed.flux.sh for why this is needed; needed
# here too since the server launch below runs on a different host (the
# rabbit) than this script itself.
export LD_LIBRARY_PATH=/opt/cray/pe/cce/20.0.0/cce/x86_64/lib:${LD_LIBRARY_PATH:-}

EXECPATH=/p/lustre5/lewis153/src/workspace/source/pdc/build/bin
SERVER=$EXECPATH/pdc_server
CLIENT=$EXECPATH/vpicio
CLOSE=$EXECPATH/close_server

export PDC_TMPDIR=${PDC_TMPDIR:-$HOME/pdc_tmp_job2_16node_rabbit}
export PDC_NCLIENT_PER_SERVER=$N_USER_CORES_PER_NODE
rm -rf "$PDC_TMPDIR"
mkdir -p "$PDC_TMPDIR"

echo "=== Job 2 (16 nodes): $N_SERVER_RANKS server ranks on 1 Rabbit chassis ($CORES_PER_SERVER_RANK cores/rank) + $N_CLIENT_RANKS client ranks across $N_NODES compute nodes ==="
date

JOBID=$(flux getattr jobid)

# The --coral2-chassis/--coral2-rabbit-cores/#DW in the header above
# already got us a chassis reservation -- find out which rabbit host it
# landed on (same approach as job2_compute_plus_rabbit.flux.sh).
GETRABBIT_OUT=$(flux --parent getrabbit -j "$JOBID" 2>&1)
if echo "$GETRABBIT_OUT" | grep -qi -e "no rabbits" -e "error" -e "usage"; then
    echo "ERROR: 'flux --parent getrabbit -j $JOBID' didn't return a rabbit host:" >&2
    echo "$GETRABBIT_OUT" >&2
    exit 1
fi
RABBIT_HOST=$(echo "$GETRABBIT_OUT" | tr -d '[] ' | cut -d',' -f1)
echo "--- targeting PDC server ranks at rabbit host: $RABBIT_HOST ---"

echo "--- starting $N_SERVER_RANKS-rank distributed PDC server on the rabbit, $CORES_PER_SERVER_RANK cores/rank (stdout/stderr -> $PDC_TMPDIR/server.out) ---"
flux run --requires=host:"$RABBIT_HOST" -n"$N_SERVER_RANKS" --cores-per-task="$CORES_PER_SERVER_RANK" \
    "$SERVER" > "$PDC_TMPDIR/server.out" 2>&1 &
SERVER_PID=$!
sleep 2

echo "--- starting $N_CLIENT_RANKS VPICIO client ranks across $N_NODES compute nodes (stdout/stderr -> $PDC_TMPDIR/vpicio.out) ---"
flux run -N"$N_NODES" -n"$N_CLIENT_RANKS" --exclusive --requires=-host:"$RABBIT_HOST" "$CLIENT" \
    "$NPARTICLES" "$STEPS" "$COMPUTE_TIME" > "$PDC_TMPDIR/vpicio.out" 2>&1

echo "--- closing PDC server (stdout/stderr -> $PDC_TMPDIR/close_server.out) ---"
# See job2_compute_plus_rabbit.flux.sh for why this runs on the compute
# node rather than targeting the rabbit host.
flux run -N1 -n1 --requires=-host:"$RABBIT_HOST" "$CLOSE" > "$PDC_TMPDIR/close_server.out" 2>&1

wait "$SERVER_PID" 2>/dev/null || true

# Copy the per-binary logs out of $PDC_TMPDIR (which gets rm -rf'd at the
# start of the NEXT run) into this job's own output directory, tagged with
# the job id so repeated runs don't clobber each other.
echo "--- copying per-binary logs into job dir as pdc_vpicio_16node_rabbit.$JOBID.{server,vpicio,close_server}.out ---"
cp "$PDC_TMPDIR/server.out" "./pdc_vpicio_16node_rabbit.$JOBID.server.out" || true
cp "$PDC_TMPDIR/vpicio.out" "./pdc_vpicio_16node_rabbit.$JOBID.vpicio.out" || true
cp "$PDC_TMPDIR/close_server.out" "./pdc_vpicio_16node_rabbit.$JOBID.close_server.out" || true

date
echo "=== Job 2 (16 nodes) done ==="
