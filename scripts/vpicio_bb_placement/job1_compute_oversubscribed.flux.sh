#!/bin/bash
#FLUX: -N 1
#FLUX: -q pdebug
#FLUX: -t 0:30:00
#FLUX: -J pdc_vpicio_oversub
#FLUX: -o pdc_vpicio_oversub.%J.out
#FLUX: -e pdc_vpicio_oversub.%J.out

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

# EDIT ME: wherever this build's pdc_server/vpicio/close_server live.
EXECPATH=/path/to/pdc/install/bin
SERVER=$EXECPATH/pdc_server
CLIENT=$EXECPATH/vpicio
CLOSE=$EXECPATH/close_server

export PDC_TMPDIR=${PDC_TMPDIR:-$HOME/pdc_tmp_job1_oversub}
export PDC_NCLIENT_PER_SERVER=$N_USER_CORES
rm -rf "$PDC_TMPDIR"
mkdir -p "$PDC_TMPDIR"

echo "=== Job 1: server + $N_USER_CORES clients packed onto 1 node (oversubscribed) ==="
date

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
echo "--- starting PDC server ---"
flux run -N1 -n1 -o cpu-affinity=off "$SERVER" &
SERVER_PID=$!
sleep 2

echo "--- starting $N_USER_CORES VPICIO client ranks ---"
flux run -N1 -n "$N_USER_CORES" -o cpu-affinity=off "$CLIENT"

echo "--- closing PDC server ---"
flux run -N1 -n1 -o cpu-affinity=off "$CLOSE"

wait "$SERVER_PID" 2>/dev/null || true
date
echo "=== Job 1 done ==="
