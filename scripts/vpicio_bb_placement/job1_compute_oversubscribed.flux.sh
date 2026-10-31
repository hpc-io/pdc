#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 30m
#FLUX: --job-name=pdc_vpicio_oversub
#FLUX: --output=pdc_vpicio_oversub.{{id}}.out
#FLUX: --error=pdc_vpicio_oversub.{{id}}.out

# Job 1 of 2 -- see README.md in this directory for the comparison this
# pair of jobs is for.
#
# Single compute node, no Rabbit involved: one VPICIO client rank per
# user core, plus one PDC server rank, all on that same node. The server
# is deliberately NOT given a core of its own -- it shares a core with
# a client rank, so this node runs oversubscribed by one slot. This is
# the traditional PDC deployment model (server colocated with compute,
# competing with it for cycles), as a baseline to compare against job 2
# (server moved onto a Rabbit's own CPU, so it never contends with the
# clients for a core).
#
# Submit with: flux batch job1_compute_oversubscribed.flux.sh

set -euo pipefail

# El Capitan/Tuolumne MI300A node layout, per LLNL's Aug 2025 CORAL2
# roadshow slides (4 sockets/node, 21 user cores + 3 system cores per
# socket): 96 cores/node total, 84 dedicated to user processes (the
# other 12 are reserved for system/Lustre and aren't schedulable here
# anyway). Adjust if running this on a different node type.
N_USER_CORES=84

# STEPS/COMPUTE_TIME: run 10 steps with 5s of real CPU work (not sleep)
# between each non-final step, so a client sharing a core with the server
# actually contends for it instead of idling -- see hard_compute() in
# src/tests/misc/vpicio.c.
NPARTICLES=8388608
STEPS=10
COMPUTE_TIME=5

# The build links against CCE runtime libs (libfi.so.1 etc.) that the
# loaded cce module doesn't put on LD_LIBRARY_PATH itself (it only sets
# PATH/MODULEPATH for the compiler, not its runtime libs) -- confirmed
# via `ldd pdc_server` showing these "not found" until this is added.
export LD_LIBRARY_PATH=/opt/cray/pe/cce/20.0.0/cce/x86_64/lib:${LD_LIBRARY_PATH:-}

EXECPATH=/p/lustre5/lewis153/src/workspace/source/pdc/build/bin
SERVER=$EXECPATH/pdc_server
CLIENT=$EXECPATH/vpicio
CLOSE=$EXECPATH/close_server

export PDC_TMPDIR=${PDC_TMPDIR:-$HOME/pdc_tmp_job1_oversub}
export PDC_NCLIENT_PER_SERVER=$N_USER_CORES
rm -rf "$PDC_TMPDIR"
mkdir -p "$PDC_TMPDIR"

echo "=== Job 1: server + $N_USER_CORES clients packed onto 1 node (oversubscribed) ==="
date

JOBID=$(flux getattr jobid)

# -o cpu-affinity=off on both launches below, and no --exclusive on
# either: that's what makes the overlap happen. --exclusive is flux's
# "give each task a disjoint slice of the node" mode (the right default
# for a real run, per LLNL's own guidance), which is exactly what we do
# NOT want to demonstrate here. With affinity pinning turned off and no
# exclusivity, demand on this node is $N_USER_CORES clients + 1 server =
# N_USER_CORES+1 tasks against only $N_USER_CORES cores, so the kernel's
# own scheduler has to time-slice at least one core between two of them
# -- the thing this job is testing for. Which specific core ends up
# shared isn't pinned down deliberately; the point is that the node as a
# whole is oversubscribed, matching what was asked for.
echo "--- starting PDC server (stdout/stderr -> $PDC_TMPDIR/server.out) ---"
flux run -N1 -n1 -o cpu-affinity=off "$SERVER" > "$PDC_TMPDIR/server.out" 2>&1 &
SERVER_PID=$!
sleep 2

echo "--- starting $N_USER_CORES VPICIO client ranks (stdout/stderr -> $PDC_TMPDIR/vpicio.out) ---"
flux run -N1 -n "$N_USER_CORES" -o cpu-affinity=off "$CLIENT" "$NPARTICLES" "$STEPS" "$COMPUTE_TIME" \
    > "$PDC_TMPDIR/vpicio.out" 2>&1

echo "--- closing PDC server (stdout/stderr -> $PDC_TMPDIR/close_server.out) ---"
flux run -N1 -n1 -o cpu-affinity=off "$CLOSE" > "$PDC_TMPDIR/close_server.out" 2>&1

wait "$SERVER_PID" 2>/dev/null || true

# Copy the per-binary logs out of $PDC_TMPDIR (which gets rm -rf'd at the
# start of the NEXT run) into this job's own output directory, tagged with
# the job id so repeated runs don't clobber each other.
echo "--- copying per-binary logs into job dir as pdc_vpicio_oversub.$JOBID.{server,vpicio,close_server}.out ---"
cp "$PDC_TMPDIR/server.out" "./pdc_vpicio_oversub.$JOBID.server.out" || true
cp "$PDC_TMPDIR/vpicio.out" "./pdc_vpicio_oversub.$JOBID.vpicio.out" || true
cp "$PDC_TMPDIR/close_server.out" "./pdc_vpicio_oversub.$JOBID.close_server.out" || true

date
echo "=== Job 1 done ==="
