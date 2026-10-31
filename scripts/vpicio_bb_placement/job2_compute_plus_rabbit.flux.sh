#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 30m
#FLUX: --job-name=pdc_vpicio_rabbit
#FLUX: --output=pdc_vpicio_rabbit.{{id}}.out
#FLUX: --error=pdc_vpicio_rabbit.{{id}}.out
#FLUX: --coral2-chassis=1
#FLUX: --coral2-rabbit-cores=1
#FLUX: -S dw="#DW jobdw type=xfs capacity=10GiB name=pdcbb"

# Job 2 of 2 -- see README.md in this directory for the comparison this
# pair of jobs is for.
#
# Same VPICIO workload as job 1, but this time the PDC server runs on a
# Rabbit's own CPU instead of sharing a core with a client. One compute
# node for all the client ranks (one per user core, same as job 1, and
# NOT oversubscribed this time), plus one Rabbit core reserved on that
# node's chassis, with the server placed there specifically.
#
# CONFIRMED on a real run on Tuolumne (job f3h9rpjGmLEK, 2026-10-30), via
# scripts/vpicio_bb_placement/explore_rabbit_cores.flux.sh's fingerprint
# check: a process submitted with --coral2-chassis=1 --coral2-rabbit-cores=1
# bundled with a #DW directive genuinely lands on a different host with a
# different CPU model (the rabbit's AMD EPYC 7713, vs. the compute node's
# AMD Instinct MI300A) -- this is real, not just resource accounting. Two
# things that submission had to work around, both reflected below:
#   - --coral2-rabbit-cores alone is REJECTED ([Errno 19] Unsatisfiable
#     request); it's only accepted bundled with a #DW directive in the
#     SAME request, hence --coral2-chassis/--coral2-rabbit-cores living
#     in this job's own #FLUX: header right next to -S dw=, rather than
#     as a separate reservation submitted from inside the script body.
#   - Querying which rabbit a job landed on (flux getrabbit) has to go
#     through `flux --parent`, since this batch job is itself a Flux
#     sub-instance relative to the system instance -- true of every
#     flux batch job, not something specific to nesting inside another
#     one.
#
# Submit with: flux batch job2_compute_plus_rabbit.flux.sh

set -euo pipefail

N_USER_CORES=84 # see job1_compute_oversubscribed.flux.sh for where this comes from

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

export PDC_TMPDIR=${PDC_TMPDIR:-$HOME/pdc_tmp_job2_rabbit}
export PDC_NCLIENT_PER_SERVER=$N_USER_CORES
rm -rf "$PDC_TMPDIR"
mkdir -p "$PDC_TMPDIR"

echo "=== Job 2: server on a Rabbit's CPU, $N_USER_CORES clients on the compute node ==="
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
# --exclusive here (unlike job 1): this is the normal, best-practice
# flag for a real run -- each client rank gets its own core, and since
# the server isn't competing for any of these cores (it's over on the
# rabbit), there's no need to deliberately avoid --exclusive the way
# job 1 does.
flux run -N1 -n "$N_USER_CORES" --exclusive --requires=-host:"$RABBIT_HOST" "$CLIENT" > "$PDC_TMPDIR/vpicio.out" 2>&1

echo "--- closing PDC server (stdout/stderr -> $PDC_TMPDIR/close_server.out) ---"
flux run --requires=host:"$RABBIT_HOST" -n1 "$CLOSE" > "$PDC_TMPDIR/close_server.out" 2>&1

wait "$SERVER_PID" 2>/dev/null || true
date
echo "=== Job 2 done ==="
