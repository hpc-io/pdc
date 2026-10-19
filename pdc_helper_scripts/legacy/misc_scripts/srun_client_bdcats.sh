set -x
set -u

NUM_NODES=4
TASKS_PER_NODE=32
TOTAL_TASKS=$((NUM_NODES * TASKS_PER_NODE))

pushd ./build/bin
rm -f client_output_bdcats.txt
rm -f client_output.txt
srun -N"$NUM_NODES" -n"$TOTAL_TASKS" --ntasks-per-node="$TASKS_PER_NODE" ./vpicio > client_output.txt 2>&1
srun -N"$NUM_NODES" -n"$TOTAL_TASKS" --ntasks-per-node="$TASKS_PER_NODE" ./bdcats > client_output_bdcats.txt 2>&1
popd

