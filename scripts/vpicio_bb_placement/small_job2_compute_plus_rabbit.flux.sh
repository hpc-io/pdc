#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 10m
#FLUX: --job-name=small_pdc_vpicio_rabbit
#FLUX: --output=small_pdc_vpicio_rabbit.{{id}}.out
#FLUX: --error=small_pdc_vpicio_rabbit.{{id}}.out
#FLUX: --coral2-chassis=1
#FLUX: --coral2-rabbit-cores=1
#FLUX: -S dw="#DW jobdw type=xfs capacity=10GiB name=pdcbbsmall"

# Small-scale copy of job2_compute_plus_rabbit.flux.sh -- same server/client
# placement (PDC server on a Rabbit's own CPU, VPICIO clients on the compute
# node), but only 2 client ranks instead of 84. VPICIO still writes 8
# objects (dX, dY, dZ, Ux, Uy, Uz, q, i) regardless of rank count, so this
# is 2 ranks x 8 objects = 16 regions (~512 MB) instead of 84 x 8 = 672
# regions (~21 GB), for a fast iteration cycle while debugging the
# close-time flush hang seen on the full-size job2 runs.
#
# See job2_compute_plus_rabbit.flux.sh and README.md in this directory for
# the full placement rationale; this file only trims N_USER_CORES and
# renames the job/output/tmpdir so it doesn't collide with that one.
#
# Submit with: flux batch small_job2_compute_plus_rabbit.flux.sh

set -euo pipefail

N_USER_CORES=2

# See job1_compute_oversubscribed.flux.sh for what these are for.
NPARTICLES=8388608
STEPS=10
COMPUTE_TIME=5

# See job1_compute_oversubscribed.flux.sh for why this is needed: the
# build links against CCE runtime libs (libfi.so.1 etc.) that the loaded
# cce module doesn't put on LD_LIBRARY_PATH itself. This has to be set
# here too since the server launch below runs on a different host (the
# rabbit) than this script itself -- it needs its own copy of this, not
# just whatever's in this job's own environment.
export LD_LIBRARY_PATH=/opt/cray/pe/cce/20.0.0/cce/x86_64/lib:${LD_LIBRARY_PATH:-}

EXECPATH=/p/lustre5/lewis153/src/workspace/source/pdc/build/bin
SERVER=$EXECPATH/pdc_server
CLIENT=$EXECPATH/vpicio
CLOSE=$EXECPATH/close_server

export PDC_TMPDIR=${PDC_TMPDIR:-$HOME/pdc_tmp_job2_rabbit_small}
export PDC_NCLIENT_PER_SERVER=$N_USER_CORES
rm -rf "$PDC_TMPDIR"
mkdir -p "$PDC_TMPDIR"

echo "=== Small Job 2: server on a Rabbit's CPU, $N_USER_CORES clients on the compute node ==="
date

JOBID=$(flux getattr jobid)

# The --coral2-chassis/--coral2-rabbit-cores/#DW in the header above already
# got us both a compute node AND a rabbit core in this one job -- find out
# which rabbit host that reserved core is actually on.
GETRABBIT_OUT=$(flux --parent getrabbit -j "$JOBID" 2>&1)
if echo "$GETRABBIT_OUT" | grep -qi -e "no rabbits" -e "error" -e "usage"; then
    echo "ERROR: 'flux --parent getrabbit -j $JOBID' didn't return a rabbit host:" >&2
    echo "$GETRABBIT_OUT" >&2
    exit 1
fi
RABBIT_HOST=$(echo "$GETRABBIT_OUT" | tr -d '[] ' | cut -d',' -f1)
echo "--- targeting PDC server at rabbit host: $RABBIT_HOST ---"

echo "--- starting PDC server on the rabbit (stdout/stderr -> $PDC_TMPDIR/server.out) ---"
flux run --requires=host:"$RABBIT_HOST" -n1 "$SERVER" > "$PDC_TMPDIR/server.out" 2>&1 &
SERVER_PID=$!
sleep 2

echo "--- starting $N_USER_CORES VPICIO client ranks on the compute node (stdout/stderr -> $PDC_TMPDIR/vpicio.out) ---"
flux run -N1 -n "$N_USER_CORES" --exclusive --requires=-host:"$RABBIT_HOST" "$CLIENT" \
    "$NPARTICLES" "$STEPS" "$COMPUTE_TIME" > "$PDC_TMPDIR/vpicio.out" 2>&1

echo "--- closing PDC server (stdout/stderr -> $PDC_TMPDIR/close_server.out) ---"
# See job2_compute_plus_rabbit.flux.sh for why this runs on the compute
# node rather than targeting the rabbit host.
flux run -N1 -n1 --requires=-host:"$RABBIT_HOST" "$CLOSE" > "$PDC_TMPDIR/close_server.out" 2>&1

wait "$SERVER_PID" 2>/dev/null || true

# Copy the per-binary logs out of $PDC_TMPDIR (which gets rm -rf'd at the
# start of the NEXT run) into this job's own output directory, tagged with
# the job id so repeated runs don't clobber each other.
echo "--- copying per-binary logs into job dir as small_pdc_vpicio_rabbit.$JOBID.{server,vpicio,close_server}.out ---"
cp "$PDC_TMPDIR/server.out" "./small_pdc_vpicio_rabbit.$JOBID.server.out" || true
cp "$PDC_TMPDIR/vpicio.out" "./small_pdc_vpicio_rabbit.$JOBID.vpicio.out" || true
cp "$PDC_TMPDIR/close_server.out" "./small_pdc_vpicio_rabbit.$JOBID.close_server.out" || true

date
echo "=== Small Job 2 done ==="
