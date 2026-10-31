#!/bin/bash
#FLUX: -N 16
#FLUX: -q pbatch
#FLUX: -t 60m
#FLUX: --job-name=pdc_vpicio_16node_oversub
#FLUX: --output=pdc_vpicio_16node_oversub.{{id}}.out
#FLUX: --error=pdc_vpicio_16node_oversub.{{id}}.out

# 16-node scale-out of job1_compute_oversubscribed.flux.sh: one distributed
# PDC server (16 ranks, one per compute node, launched as a single MPI job
# spanning all 16 nodes) colocated with that node's own client ranks,
# sharing a core the same way job1 does on a single node. Compare against
# job2_16node_rabbit.flux.sh, which moves all 16 server ranks onto a
# single Rabbit chassis instead.
#
# UNVERIFIED: this has not been run. It's a direct scale-out of
# job1_compute_oversubscribed.flux.sh (same flux run flags, same per-node
# layout), on the assumption that a multi-node "-N16 -n16" pdc_server
# launch forms one 16-rank distributed server the same way a single-node
# multi-rank launch would (PDC's server code is MPI-based and designed for
# this; it just hasn't been exercised across nodes in this debugging
# session). Also check whether "pbatch"/60m actually fit this system's
# queue limits for a 16-node job -- these are guesses.
#
# DATA VOLUME: with NPARTICLES/STEPS below unchanged from the single-node
# jobs, this is ~16x the single-node job's data (~3.3 TB total across all
# 10 steps) and will likely need well over 60m. Test with a much smaller
# NPARTICLES (and/or fewer STEPS) first -- see small_job2_compute_plus_
# rabbit.flux.sh for the equivalent fast-iteration pattern -- before
# running this at the size below.
#
# Submit with: flux batch job1_16node_oversubscribed.flux.sh

set -euo pipefail

N_NODES=16
N_USER_CORES_PER_NODE=84 # see job1_compute_oversubscribed.flux.sh
N_SERVER_RANKS=$N_NODES  # one server rank per node, colocated with that node's clients
N_CLIENT_RANKS=$((N_NODES * N_USER_CORES_PER_NODE))

# See job1_compute_oversubscribed.flux.sh for what these are for. Read the
# DATA VOLUME note above before changing these for a real run.
NPARTICLES=8388608
STEPS=10
COMPUTE_TIME=5

# See job1_compute_oversubscribed.flux.sh for why this is needed.
export LD_LIBRARY_PATH=/opt/cray/pe/cce/20.0.0/cce/x86_64/lib:${LD_LIBRARY_PATH:-}

EXECPATH=/p/lustre5/lewis153/src/workspace/source/pdc/build/bin
SERVER=$EXECPATH/pdc_server
CLIENT=$EXECPATH/vpicio
CLOSE=$EXECPATH/close_server

export PDC_TMPDIR=${PDC_TMPDIR:-$HOME/pdc_tmp_job1_16node}
export PDC_NCLIENT_PER_SERVER=$N_USER_CORES_PER_NODE
rm -rf "$PDC_TMPDIR"
mkdir -p "$PDC_TMPDIR"

echo "=== Job 1 (16 nodes): $N_SERVER_RANKS server ranks (1/node, oversubscribed) + $N_CLIENT_RANKS client ranks ==="
date

JOBID=$(flux getattr jobid)

echo "--- starting $N_SERVER_RANKS-rank distributed PDC server, 1 per node (stdout/stderr -> $PDC_TMPDIR/server.out) ---"
flux run -N"$N_NODES" -n"$N_SERVER_RANKS" -o cpu-affinity=off "$SERVER" > "$PDC_TMPDIR/server.out" 2>&1 &
SERVER_PID=$!
sleep 2

echo "--- starting $N_CLIENT_RANKS VPICIO client ranks across $N_NODES nodes (stdout/stderr -> $PDC_TMPDIR/vpicio.out) ---"
flux run -N"$N_NODES" -n"$N_CLIENT_RANKS" -o cpu-affinity=off "$CLIENT" "$NPARTICLES" "$STEPS" "$COMPUTE_TIME" \
    > "$PDC_TMPDIR/vpicio.out" 2>&1

echo "--- closing PDC server (stdout/stderr -> $PDC_TMPDIR/close_server.out) ---"
flux run -N1 -n1 -o cpu-affinity=off "$CLOSE" > "$PDC_TMPDIR/close_server.out" 2>&1

wait "$SERVER_PID" 2>/dev/null || true

# Copy the per-binary logs out of $PDC_TMPDIR (which gets rm -rf'd at the
# start of the NEXT run) into this job's own output directory, tagged with
# the job id so repeated runs don't clobber each other.
echo "--- copying per-binary logs into job dir as pdc_vpicio_16node_oversub.$JOBID.{server,vpicio,close_server}.out ---"
cp "$PDC_TMPDIR/server.out" "./pdc_vpicio_16node_oversub.$JOBID.server.out" || true
cp "$PDC_TMPDIR/vpicio.out" "./pdc_vpicio_16node_oversub.$JOBID.vpicio.out" || true
cp "$PDC_TMPDIR/close_server.out" "./pdc_vpicio_16node_oversub.$JOBID.close_server.out" || true

date
echo "=== Job 1 (16 nodes) done ==="
