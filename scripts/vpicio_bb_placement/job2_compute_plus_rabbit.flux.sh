#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 0:30:00
#FLUX: -J pdc_vpicio_rabbit
#FLUX: -o pdc_vpicio_rabbit.%J.out
#FLUX: -e pdc_vpicio_rabbit.%J.out
#FLUX: -S dw="#DW jobdw type=xfs capacity=10GiB name=pdcbb"

# Job 2 of 2 -- see README.md in this directory for the comparison this
# pair of jobs is for.
#
# Same VPICIO workload as job 1, but this time the PDC server runs on a
# Rabbit's own CPU instead of sharing a core with a client. One compute
# node for all the client ranks (one per user core, same as job 1, and
# NOT oversubscribed this time), plus one Rabbit attached to that node's
# chassis, with the server placed on the Rabbit's processor specifically.
#
# IMPORTANT CAVEAT: scheduling a user process onto a Rabbit's own CPU
# (as opposed to just using the Rabbit for storage, which is the well
# documented, everyday use of #DW jobdw) relies on --coral2-rabbit-cores
# / --coral2-chassis, which flux-coral2's own docs mark EXPERIMENTAL and
# which doesn't appear anywhere in LLNL's Aug 2025 CORAL2 roadshow slide
# deck -- i.e. it isn't part of the mainstream, confirmed-stable user
# workflow the way #DW jobdw storage requests are. Before relying on
# this for anything that matters, run `flux alloc --help=coral2-rabbit-
# cores` on the actual target system and compare against what's below;
# the flag names are confirmed to exist, but the exact placement/
# targeting mechanism here (reserve cores on the rabbit, then target a
# flux run at that rabbit's hostname) is this script's own best-effort
# chaining of two independently-documented pieces, not a worked example
# taken from LLNL's own docs.
#
# Submit with: flux batch job2_compute_plus_rabbit.flux.sh

set -euo pipefail

N_USER_CORES=84 # see job1_compute_oversubscribed.flux.sh for where this comes from

# EDIT ME: wherever this build's pdc_server/vpicio/close_server live.
EXECPATH=/path/to/pdc/install/bin
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

# Reserve 1 compute core on the Rabbit attached to this job's chassis.
# This is the experimental part flagged above -- if it errors out on
# your system, that's the signal the flag names/semantics have moved on
# from what's documented here.
RABBIT_CORE_JOBID=$(flux submit --coral2-chassis=1 --coral2-rabbit-cores=1 -N1 -n1 sleep inf)
sleep 2

# flux getrabbit is the confirmed, documented way to list which rabbit(s)
# are attached to a job (`flux getrabbit -j $JOBID`); we use it here to
# learn the rabbit's hostname so the server launch below can be targeted
# at it specifically via --requires=host:<name>, the same host-targeting
# mechanism LLNL's own docs show for steering jobs onto/away from named
# compute nodes.
RABBIT_HOST=$(flux getrabbit -j "$JOBID" | tr -d '[]' | cut -d',' -f1)
if [ -z "$RABBIT_HOST" ]; then
    echo "ERROR: could not determine a rabbit hostname from 'flux getrabbit -j $JOBID' -- see the" >&2
    echo "caveat at the top of this script; the rabbit-cores reservation above may not have landed." >&2
    flux cancel "$RABBIT_CORE_JOBID" 2>/dev/null || true
    exit 1
fi
echo "--- targeting PDC server at rabbit host: $RABBIT_HOST ---"

echo "--- starting PDC server on the rabbit ---"
flux run --requires=host:"$RABBIT_HOST" -n1 "$SERVER" &
SERVER_PID=$!
sleep 2

echo "--- starting $N_USER_CORES VPICIO client ranks on the compute node ---"
# --exclusive here (unlike job 1): this is the normal, best-practice
# flag for a real run -- each client rank gets its own core, and since
# the server isn't competing for any of these cores (it's over on the
# rabbit), there's no need to deliberately avoid --exclusive the way
# job 1 does.
flux run -N1 -n "$N_USER_CORES" --exclusive --requires=-host:"$RABBIT_HOST" "$CLIENT"

echo "--- closing PDC server ---"
flux run --requires=host:"$RABBIT_HOST" -n1 "$CLOSE"

wait "$SERVER_PID" 2>/dev/null || true
flux cancel "$RABBIT_CORE_JOBID" 2>/dev/null || true
date
echo "=== Job 2 done ==="
